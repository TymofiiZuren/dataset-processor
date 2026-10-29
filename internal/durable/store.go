// Package durable implements the private local distributed runtime. PostgreSQL
// owns transitions; messages are hints and object versions are immutable inputs.
package durable

import (
	"context"
	"crypto/rand"
	"dataset-processor/internal/cluster"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"dataset-processor/internal/processor"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrStale     = errors.New("stale claim")
	ErrNotFound  = errors.New("job not found")
	ErrConflict  = errors.New("request conflicts with existing job")
	ErrCapacity  = errors.New("local capacity full")
	ErrIntegrity = errors.New("committed object integrity error")
	ErrExpired   = errors.New("result expired")
)

const MaxInput = 8 << 20
const maxDerived = 64 << 20

func ID() string {
	var b [24]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic("secure randomness unavailable")
	}
	return hex.EncodeToString(b[:])
}

type Ref struct {
	Key     string `json:"key"`
	Version string `json:"version"`
	SHA256  string `json:"sha256"`
	Size    int64  `json:"size"`
}
type Manifest struct {
	processor.Manifest
	Parts int `json:"parts"`
}
type Job struct {
	ID              string    `json:"id"`
	Owner           string    `json:"-"`
	State           string    `json:"state"`
	Descriptor      string    `json:"descriptor"`
	Version         string    `json:"transformation_version"`
	Generation      int64     `json:"generation"`
	Revision        int64     `json:"revision"`
	Token           string    `json:"stage_token"`
	Until           time.Time `json:"stage_until"`
	StageAttempts   int       `json:"stage_attempts"`
	Preparation     string    `json:"preparation"`
	Input           Ref       `json:"input"`
	Total           int64     `json:"records"`
	Parts           int       `json:"parts"`
	Committed       int       `json:"committed_chunks"`
	Accepted        int64     `json:"accepted"`
	Rejected        int64     `json:"rejected"`
	Error           string    `json:"error,omitempty"`
	Created         time.Time `json:"created_at"`
	Updated         time.Time `json:"updated_at"`
	Deadline        time.Time `json:"deadline"`
	Expires         time.Time `json:"expires_at"`
	MetadataExpires time.Time `json:"metadata_expires_at"`
	Derived         int64     `json:"derived_bytes"`
	Manifest        *Manifest `json:"manifest,omitempty"`
}
type Chunk struct {
	Job           string    `json:"job"`
	Index         int       `json:"index"`
	Preparation   string    `json:"preparation"`
	Generation    int64     `json:"generation"`
	First         int64     `json:"first"`
	Records       int64     `json:"records"`
	Input         Ref       `json:"input"`
	Clean         Ref       `json:"clean"`
	Rejected      Ref       `json:"rejected"`
	Accepted      int64     `json:"accepted"`
	RejectedCount int64     `json:"rejected_count"`
	State         string    `json:"state"`
	Token         string    `json:"token"`
	Until         time.Time `json:"until"`
	Attempts      int       `json:"attempts"`
}

// Owner is persisted separately from JSON, so serialization never exposes it.
type Store struct{ pool *pgxpool.Pool }

func Open(ctx context.Context, uri string) (*Store, error) { return OpenProfile(ctx, uri, "local") }
func databaseConfig(uri, profile string) (*pgxpool.Config, error) {
	c, e := pgxpool.ParseConfig(uri)
	if e != nil {
		return nil, errors.New("invalid database configuration")
	}
	ip := net.ParseIP(c.ConnConfig.Host)
	loopback := c.ConnConfig.Host == "localhost" || ip != nil && ip.IsLoopback()
	if profile != "local" && profile != "aws" && profile != "cluster" {
		return nil, errors.New("invalid database profile")
	}
	if profile == "local" && !loopback {
		return nil, errors.New("local database must use a loopback host")
	}
	if profile == "cluster" && (c.ConnConfig.Host != cluster.DatabaseHost || c.ConnConfig.Port != 5432 || c.ConnConfig.User != "processor" || c.ConnConfig.Database != "processor") {
		return nil, errors.New("cluster database identity is fixed")
	}
	if profile == "cluster" || profile == "aws" && !loopback {
		tls := c.ConnConfig.TLSConfig
		if tls == nil || tls.InsecureSkipVerify || tls.ServerName != c.ConnConfig.Host || len(c.ConnConfig.Fallbacks) > 0 {
			return nil, errors.New("remote PostgreSQL requires sslmode=verify-full with a trusted certificate authority")
		}
	}
	return c, nil
}
func OpenProfile(ctx context.Context, uri, profile string) (*Store, error) {
	c, e := databaseConfig(uri, profile)
	if e != nil {
		return nil, e
	}
	c.ConnConfig.ConnectTimeout = 10 * time.Second
	c.MaxConns = 4
	c.ConnConfig.RuntimeParams["lock_timeout"] = "3s"
	c.ConnConfig.RuntimeParams["statement_timeout"] = "10s"
	p, e := pgxpool.NewWithConfig(ctx, c)
	if e != nil {
		return nil, errors.New("database unavailable")
	}
	if e = p.Ping(ctx); e != nil {
		p.Close()
		return nil, errors.New("database unavailable")
	}
	return &Store{p}, nil
}
func (s *Store) Close() { s.pool.Close() }

const schema = `CREATE TABLE IF NOT EXISTS processor_schema(version integer PRIMARY KEY, checksum text NOT NULL);
CREATE TABLE IF NOT EXISTS processor_capacity(id integer PRIMARY KEY, input_bytes bigint NOT NULL, derived_bytes bigint NOT NULL);
INSERT INTO processor_capacity VALUES(1,0,0) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS processor_jobs(id text PRIMARY KEY,owner text NOT NULL,request_key text NOT NULL,descriptor text NOT NULL,doc jsonb NOT NULL, UNIQUE(owner,request_key));
CREATE INDEX IF NOT EXISTS processor_jobs_state ON processor_jobs((doc->>'state'));
CREATE TABLE IF NOT EXISTS processor_chunks(job text REFERENCES processor_jobs(id), preparation text NOT NULL, idx integer NOT NULL,doc jsonb NOT NULL,PRIMARY KEY(job,preparation,idx));
CREATE TABLE IF NOT EXISTS processor_outbox(job text REFERENCES processor_jobs(id),preparation text NOT NULL,idx integer NOT NULL,sent_at timestamptz,PRIMARY KEY(job,preparation,idx));
CREATE TABLE IF NOT EXISTS processor_events(seq bigserial PRIMARY KEY,job text REFERENCES processor_jobs(id),revision bigint NOT NULL,state text NOT NULL,code text NOT NULL,at timestamptz NOT NULL DEFAULT clock_timestamp());`

func (s *Store) Migrate(ctx context.Context) error {
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(72103114)"); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, schema); e != nil {
		return e
	}
	hash := Hash([]byte(schema))
	var old string
	e = tx.QueryRow(ctx, "SELECT checksum FROM processor_schema WHERE version=1").Scan(&old)
	if e == nil && old != hash {
		return errors.New("migration checksum mismatch")
	}
	if e != nil && !errors.Is(e, pgx.ErrNoRows) {
		return e
	}
	_, e = tx.Exec(ctx, "INSERT INTO processor_schema VALUES(1,$1) ON CONFLICT DO NOTHING", hash)
	if e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, schema2); e != nil {
		return e
	}
	var second string
	e = tx.QueryRow(ctx, "SELECT checksum FROM processor_schema WHERE version=2").Scan(&second)
	if e == nil && second != Hash([]byte(schema2)) {
		return errors.New("migration checksum mismatch")
	}
	if e != nil && !errors.Is(e, pgx.ErrNoRows) {
		return e
	}
	if _, e = tx.Exec(ctx, "INSERT INTO processor_schema VALUES(2,$1) ON CONFLICT DO NOTHING", Hash([]byte(schema2))); e != nil {
		return e
	}
	return tx.Commit(ctx)
}

const schema2 = `CREATE TABLE IF NOT EXISTS processor_backends(owner text PRIMARY KEY, identity text NOT NULL);`

var ErrBackend = errors.New("backend binding mismatch; use a dedicated owner/database and run explicit migrate")

func (s *Store) BindBackend(ctx context.Context, owner, identity string, adoptLocal bool) error {
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = compatible(ctx, tx); e != nil {
		return e
	}
	if e = bindBackend(ctx, tx, owner, identity, adoptLocal); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func bindBackend(ctx context.Context, tx pgx.Tx, owner, identity string, adoptLocal bool) error {
	if owner == "" || identity == "" || adoptLocal && !strings.HasPrefix(identity, "local:") {
		return ErrBackend
	}
	var version int
	if e := tx.QueryRow(ctx, "SELECT max(version) FROM processor_schema").Scan(&version); e != nil || version != 2 {
		return ErrBackend
	}
	if _, e := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1)::bigint)", owner); e != nil {
		return e
	}
	var old string
	e := tx.QueryRow(ctx, "SELECT identity FROM processor_backends WHERE owner=$1", owner).Scan(&old)
	if e == nil {
		if old != identity {
			return ErrBackend
		}
		return nil
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return e
	}
	if !adoptLocal {
		var count int
		if e = tx.QueryRow(ctx, "SELECT count(*) FROM processor_jobs WHERE owner=$1", owner).Scan(&count); e != nil {
			return e
		}
		if count > 0 {
			return ErrBackend
		}
	}
	_, e = tx.Exec(ctx, "INSERT INTO processor_backends VALUES($1,$2)", owner, identity)
	return e
}
func compatible(ctx context.Context, tx pgx.Tx) error {
	var version int
	var checksum string
	e := tx.QueryRow(ctx, "SELECT version,checksum FROM processor_schema ORDER BY version DESC LIMIT 1").Scan(&version, &checksum)
	if e != nil || (version != 1 && version != 2) || version == 1 && checksum != Hash([]byte(schema)) || version == 2 && checksum != Hash([]byte(schema2)) {
		return errors.New("unsupported database schema; run migrate with compatible roles")
	}
	var initial string
	if e = tx.QueryRow(ctx, "SELECT checksum FROM processor_schema WHERE version=1").Scan(&initial); e != nil || initial != Hash([]byte(schema)) {
		return errors.New("database migration checksum mismatch")
	}
	return nil
}
func (s *Store) Check(ctx context.Context) error {
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	return compatible(ctx, tx)
}
func save(ctx context.Context, tx pgx.Tx, j *Job, code string) error {
	j.Revision++
	if e := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&j.Updated); e != nil {
		return e
	}
	b, e := json.Marshal(j)
	if e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, "UPDATE processor_jobs SET doc=$2 WHERE id=$1", j.ID, b); e != nil {
		return e
	}
	_, e = tx.Exec(ctx, "INSERT INTO processor_events(job,revision,state,code) VALUES($1,$2,$3,$4)", j.ID, j.Revision, j.State, code)
	return e
}
func load(ctx context.Context, tx pgx.Tx, id string) (Job, time.Time, error) {
	var j Job
	var b []byte
	var owner string
	e := tx.QueryRow(ctx, "SELECT doc,owner FROM processor_jobs WHERE id=$1 FOR UPDATE", id).Scan(&b, &owner)
	if errors.Is(e, pgx.ErrNoRows) {
		return j, time.Time{}, ErrNotFound
	}
	if e != nil {
		return j, time.Time{}, e
	}
	if e = json.Unmarshal(b, &j); e != nil {
		return j, time.Time{}, e
	}
	j.Owner = owner
	var now time.Time
	e = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now)
	return j, now, e
}
func terminal(state string) bool {
	return state == "succeeded" || state == "failed" || state == "cancelled"
}
func (s *Store) mutate(ctx context.Context, id string, fn func(pgx.Tx, *Job, time.Time) error) error {
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = compatible(ctx, tx); e != nil {
		return e
	}
	j, now, e := load(ctx, tx, id)
	if e != nil {
		return e
	}
	if e = fn(tx, &j, now); e != nil {
		return e
	}
	return tx.Commit(ctx)
}

// Capacity is always locked before a job row, including bounded claims.
func (s *Store) mutateCapacity(ctx context.Context, id string, fn func(pgx.Tx, *Job, time.Time) error) error {
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = compatible(ctx, tx); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, "SELECT id FROM processor_capacity WHERE id=1 FOR UPDATE"); e != nil {
		return e
	}
	j, now, e := load(ctx, tx, id)
	if e != nil {
		return e
	}
	if e = fn(tx, &j, now); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (s *Store) Admit(ctx context.Context, owner, key, descriptor, backend string) (Job, error) {
	var j Job
	if owner == "" || key == "" || len(key) > 128 || len(descriptor) > 256 {
		return j, ErrConflict
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return j, e
	}
	defer tx.Rollback(ctx)
	if e = compatible(ctx, tx); e != nil {
		return j, e
	}
	var used int64
	if e = tx.QueryRow(ctx, "SELECT input_bytes FROM processor_capacity WHERE id=1 FOR UPDATE").Scan(&used); e != nil {
		return j, e
	}
	// Capacity -> owner binding -> job is the common admission lock order.
	var version int
	if e = tx.QueryRow(ctx, "SELECT max(version) FROM processor_schema").Scan(&version); e != nil {
		return j, e
	}
	local := strings.HasPrefix(backend, "local:")
	if version == 2 {
		if e = bindBackend(ctx, tx, owner, backend, local); e != nil {
			return j, e
		}
	} else if !local {
		return j, ErrBackend
	}
	var b []byte
	e = tx.QueryRow(ctx, "SELECT doc,descriptor FROM processor_jobs WHERE owner=$1 AND request_key=$2", owner, key).Scan(&b, &j.Descriptor)
	if e == nil {
		if j.Descriptor != descriptor {
			return j, ErrConflict
		}
		e = json.Unmarshal(b, &j)
		j.Owner = owner
		if e != nil {
			return j, e
		}
		e = tx.Commit(ctx)
		return j, e
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return j, e
	}
	var active int
	if e = tx.QueryRow(ctx, "SELECT count(*) FROM processor_jobs WHERE owner=$1 AND doc->>'state' NOT IN ('succeeded','failed','cancelled')", owner).Scan(&active); e != nil {
		return j, e
	}
	if active >= 2 || used+MaxInput > 2<<30 {
		return j, ErrCapacity
	}
	var now time.Time
	if e = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); e != nil {
		return j, e
	}
	j = Job{ID: ID(), Owner: owner, Descriptor: descriptor, State: "uploading", Version: processor.Version, Generation: 1, Created: now, Updated: now, Deadline: now.Add(time.Hour), MetadataExpires: now.Add(7 * 24 * time.Hour)}
	j.Input.Key = "jobs/" + j.ID + "/input"
	b, _ = json.Marshal(j)
	if _, e = tx.Exec(ctx, "INSERT INTO processor_jobs VALUES($1,$2,$3,$4,$5)", j.ID, owner, key, descriptor, b); e != nil {
		return j, e
	}
	if _, e = tx.Exec(ctx, "UPDATE processor_capacity SET input_bytes=input_bytes+$1 WHERE id=1", MaxInput); e != nil {
		return j, e
	}
	e = tx.Commit(ctx)
	return j, e
}
func (s *Store) Get(ctx context.Context, owner, id string) (Job, error) {
	var j Job
	var b []byte
	e := s.pool.QueryRow(ctx, "SELECT doc FROM processor_jobs WHERE id=$1 AND owner=$2 AND (doc->>'metadata_expires_at')::timestamptz>clock_timestamp()", id, owner).Scan(&b)
	if errors.Is(e, pgx.ErrNoRows) {
		return j, ErrNotFound
	}
	if e == nil {
		e = json.Unmarshal(b, &j)
	}
	j.Owner = owner
	return j, e
}
func (s *Store) InputReady(ctx context.Context, id string, input Ref) error {
	return s.mutate(ctx, id, func(tx pgx.Tx, j *Job, now time.Time) error {
		if j.State != "uploading" || !now.Before(j.Deadline) {
			return ErrStale
		}
		if input.Key != j.Input.Key || input.Version == "" || input.Version == "null" || input.Size > MaxInput {
			return ErrIntegrity
		}
		j.Input = input
		j.State = "preparing"
		return save(ctx, tx, j, "input_pinned")
	})
}
func (s *Store) Reserve(ctx context.Context, id string, n int64) error {
	if n < 0 {
		return ErrCapacity
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = compatible(ctx, tx); e != nil {
		return e
	}
	var used int64
	if e = tx.QueryRow(ctx, "SELECT derived_bytes FROM processor_capacity WHERE id=1 FOR UPDATE").Scan(&used); e != nil {
		return e
	}
	j, _, e := load(ctx, tx, id)
	if e != nil {
		return e
	}
	if terminal(j.State) {
		return ErrStale
	}
	if used+n > 8<<30 || j.Derived+n > maxDerived {
		return ErrCapacity
	}
	j.Derived += n
	if _, e = tx.Exec(ctx, "UPDATE processor_capacity SET derived_bytes=derived_bytes+$1 WHERE id=1", n); e != nil {
		return e
	}
	if e = save(ctx, tx, &j, "storage_reserved"); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func validStage(j *Job, claim Job, now time.Time) bool {
	return j.Generation == claim.Generation && j.Token == claim.Token && j.State == claim.State && now.Before(j.Until) && now.Before(j.Deadline)
}
func (s *Store) ClaimStage(ctx context.Context, id, state string, lease time.Duration) (Job, error) {
	var out Job
	e := s.mutateCapacity(ctx, id, func(tx pgx.Tx, j *Job, now time.Time) error {
		if terminal(j.State) || !now.Before(j.Deadline) || j.Version != processor.Version {
			return ErrStale
		}
		if state == "preparing" {
			var busy int
			if e := tx.QueryRow(ctx, "SELECT count(*) FROM processor_jobs WHERE id<>$1 AND doc->>'state'='preparing' AND (doc->>'stage_until')::timestamptz>clock_timestamp()", id).Scan(&busy); e != nil {
				return e
			}
			if busy >= 1 {
				return ErrStale
			}

			if j.State != "preparing" {
				return ErrStale
			}
		} else if state == "finalizing" {
			if j.State != "processing" && j.State != "finalizing" {
				return ErrStale
			}
			if j.Committed != j.Parts {
				return ErrStale
			}
		} else {
			return ErrStale
		}
		if now.Before(j.Until) {
			return ErrStale
		}
		if j.StageAttempts >= 5 {
			j.State = "failed"
			j.Error = "RECOVERY_EXHAUSTED"
			return save(ctx, tx, j, j.Error)
		}
		j.State = state
		j.Token = ID()
		j.Until = now.Add(lease)
		j.StageAttempts++
		if e := save(ctx, tx, j, "stage_claimed"); e != nil {
			return e
		}
		out = *j
		return nil
	})
	if e == nil && out.ID == "" {
		e = ErrStale
	}
	return out, e
}
func (s *Store) RenewStage(ctx context.Context, claim Job, lease time.Duration) error {
	return s.mutate(ctx, claim.ID, func(tx pgx.Tx, j *Job, now time.Time) error {
		if !validStage(j, claim, now) {
			return ErrStale
		}
		j.Until = now.Add(lease)
		return save(ctx, tx, j, "stage_renewed")
	})
}
func (s *Store) StageChunk(ctx context.Context, p Job, c Chunk) error {
	return s.mutate(ctx, p.ID, func(tx pgx.Tx, j *Job, now time.Time) error {
		if !validStage(j, p, now) || c.Index < 0 || c.Index >= 1024 || c.Records <= 0 {
			return ErrStale
		}
		c.Job = j.ID
		c.Generation = j.Generation
		c.Preparation = p.Token
		c.State = "pending"
		b, _ := json.Marshal(c)
		_, e := tx.Exec(ctx, "INSERT INTO processor_chunks VALUES($1,$2,$3,$4)", j.ID, p.Token, c.Index, b)
		if e != nil {
			return e
		}
		_, e = tx.Exec(ctx, "INSERT INTO processor_outbox(job,preparation,idx) VALUES($1,$2,$3)", j.ID, p.Token, c.Index)
		return e
	})
}
func (s *Store) SelectPreparation(ctx context.Context, p Job, parts int, records int64) error {
	return s.mutate(ctx, p.ID, func(tx pgx.Tx, j *Job, now time.Time) error {
		if !validStage(j, p, now) {
			return ErrStale
		}
		var count int
		var sum int64
		e := tx.QueryRow(ctx, "SELECT count(*),COALESCE(sum((doc->>'records')::bigint),0) FROM processor_chunks WHERE job=$1 AND preparation=$2", j.ID, p.Token).Scan(&count, &sum)
		if e != nil {
			return e
		}
		if count != parts || sum != records {
			return ErrIntegrity
		}
		j.Preparation = p.Token
		j.Parts = parts
		j.Total = records
		j.State = "processing"
		j.Until = time.Time{}
		j.Token = ""
		j.StageAttempts = 0
		return save(ctx, tx, j, "preparation_selected")
	})
}
func (s *Store) Chunks(ctx context.Context, j Job) ([]Chunk, error) {
	rows, e := s.pool.Query(ctx, "SELECT doc FROM processor_chunks WHERE job=$1 AND preparation=$2 ORDER BY idx", j.ID, j.Preparation)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Chunk{}
	for rows.Next() {
		var b []byte
		var c Chunk
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(b, &c); e != nil {
			return nil, e
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
func chunk(ctx context.Context, tx pgx.Tx, j *Job, idx int) (Chunk, error) {
	var c Chunk
	var b []byte
	e := tx.QueryRow(ctx, "SELECT doc FROM processor_chunks WHERE job=$1 AND preparation=$2 AND idx=$3", j.ID, j.Preparation, idx).Scan(&b)
	if errors.Is(e, pgx.ErrNoRows) {
		return c, ErrStale
	}
	if e == nil {
		e = json.Unmarshal(b, &c)
	}
	return c, e
}
func saveChunk(ctx context.Context, tx pgx.Tx, c Chunk) error {
	b, e := json.Marshal(c)
	if e != nil {
		return e
	}
	_, e = tx.Exec(ctx, "UPDATE processor_chunks SET doc=$4 WHERE job=$1 AND preparation=$2 AND idx=$3", c.Job, c.Preparation, c.Index, b)
	return e
}
func (s *Store) ClaimChunk(ctx context.Context, id string, idx int, lease time.Duration) (Chunk, error) {
	var out Chunk
	e := s.mutateCapacity(ctx, id, func(tx pgx.Tx, j *Job, now time.Time) error {
		if j.State != "processing" || j.Version != processor.Version || !now.Before(j.Deadline) {
			return ErrStale
		}
		c, e := chunk(ctx, tx, j, idx)
		if e != nil {
			return e
		}
		if c.State == "done" || now.Before(c.Until) {
			return ErrStale
		}
		if c.Attempts >= 5 {
			j.State = "failed"
			j.Error = "RECOVERY_EXHAUSTED"
			return save(ctx, tx, j, j.Error)
		}
		var active int
		if e = tx.QueryRow(ctx, `SELECT count(*) FROM processor_chunks c JOIN processor_jobs j ON j.id=c.job WHERE j.doc->>'state'='processing' AND j.doc->>'preparation'=c.preparation AND c.doc->>'state'='running' AND (c.doc->>'until')::timestamptz>clock_timestamp()`).Scan(&active); e != nil {
			return e
		}
		if active >= 4 {
			return ErrStale
		}
		c.Attempts++
		c.Token = ID()
		c.State = "running"
		c.Until = now.Add(lease)
		if e = saveChunk(ctx, tx, c); e != nil {
			return e
		}
		out = c
		return save(ctx, tx, j, "chunk_claimed")
	})
	if e == nil && out.Token == "" {
		e = ErrStale
	}
	return out, e
}
func validChunk(j *Job, c, claim Chunk, now time.Time) bool {
	return j.State == "processing" && j.Generation == claim.Generation && j.Preparation == claim.Preparation && c.Token == claim.Token && c.State == "running" && now.Before(c.Until) && now.Before(j.Deadline)
}
func (s *Store) RenewChunk(ctx context.Context, claim Chunk, lease time.Duration) error {
	return s.mutate(ctx, claim.Job, func(tx pgx.Tx, j *Job, now time.Time) error {
		c, e := chunk(ctx, tx, j, claim.Index)
		if e != nil {
			return e
		}
		if !validChunk(j, c, claim, now) {
			return ErrStale
		}
		c.Until = now.Add(lease)
		return saveChunk(ctx, tx, c)
	})
}
func (s *Store) CommitChunk(ctx context.Context, claim Chunk, clean, rejected Ref, accepted, bad int64) error {
	return s.mutate(ctx, claim.Job, func(tx pgx.Tx, j *Job, now time.Time) error {
		c, e := chunk(ctx, tx, j, claim.Index)
		if e != nil {
			return e
		}
		if !validChunk(j, c, claim, now) {
			return ErrStale
		}
		if accepted < 0 || bad < 0 || accepted+bad != c.Records {
			return ErrIntegrity
		}
		c.State = "done"
		c.Clean = clean
		c.Rejected = rejected
		c.Accepted = accepted
		c.RejectedCount = bad
		if e = saveChunk(ctx, tx, c); e != nil {
			return e
		}
		j.Committed++
		j.Accepted += accepted
		j.Rejected += bad
		return save(ctx, tx, j, "chunk_committed")
	})
}
func (s *Store) FailChunk(ctx context.Context, claim Chunk, code string, retry bool) error {
	return s.mutate(ctx, claim.Job, func(tx pgx.Tx, j *Job, now time.Time) error {
		c, e := chunk(ctx, tx, j, claim.Index)
		if e != nil {
			return e
		}
		if !validChunk(j, c, claim, now) {
			return ErrStale
		}
		if retry && c.Attempts < 5 {
			c.State = "pending"
			c.Until = time.Time{}
			if e = saveChunk(ctx, tx, c); e != nil {
				return e
			}
		} else {
			j.State = "failed"
			j.Error = code
		}
		return save(ctx, tx, j, code)
	})
}
func (s *Store) FailStage(ctx context.Context, claim Job, code string, retry bool) error {
	return s.mutate(ctx, claim.ID, func(tx pgx.Tx, j *Job, now time.Time) error {
		if !validStage(j, claim, now) {
			return ErrStale
		}
		if retry && j.StageAttempts < 5 {
			j.Until = time.Time{}
		} else {
			j.State = "failed"
			j.Error = code
		}
		return save(ctx, tx, j, code)
	})
}
func (s *Store) Publish(ctx context.Context, p Job, m Manifest) error {
	return s.mutate(ctx, p.ID, func(tx pgx.Tx, j *Job, now time.Time) error {
		if !validStage(j, p, now) {
			return ErrStale
		}
		if j.Committed != j.Parts || j.Accepted+j.Rejected != j.Total || m.Records != j.Total || m.Accepted != j.Accepted || m.Rejected != j.Rejected {
			return ErrIntegrity
		}
		j.Manifest = &m
		j.Expires = now.Add(24 * time.Hour)
		j.State = "succeeded"
		return save(ctx, tx, j, "result_published")
	})
}
func (s *Store) Cancel(ctx context.Context, owner, id string) error {
	return s.mutate(ctx, id, func(tx pgx.Tx, j *Job, _ time.Time) error {
		if j.Owner != owner {
			return ErrNotFound
		}
		if j.State == "cancelled" {
			return nil
		}
		if terminal(j.State) {
			return ErrConflict
		}
		j.State = "cancelled"
		j.Generation++
		j.Token = ""
		return save(ctx, tx, j, "cancelled")
	})
}
func (s *Store) Active(ctx context.Context, owner string) ([]Job, error) {
	rows, e := s.pool.Query(ctx, "SELECT doc,owner FROM processor_jobs WHERE owner=$1 AND doc->>'state' NOT IN ('succeeded','failed','cancelled') ORDER BY id LIMIT 128", owner)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Job{}
	for rows.Next() {
		var b []byte
		var j Job
		var owner string
		if e = rows.Scan(&b, &owner); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(b, &j); e != nil {
			return nil, e
		}
		j.Owner = owner
		out = append(out, j)
	}
	return out, rows.Err()
}
func (s *Store) Expire(ctx context.Context, id string) error {
	return s.mutate(ctx, id, func(tx pgx.Tx, j *Job, now time.Time) error {
		if terminal(j.State) || now.Before(j.Deadline) {
			return nil
		}
		j.State = "failed"
		j.Generation++
		j.Error = "RECOVERY_DEADLINE"
		return save(ctx, tx, j, j.Error)
	})
}

type Dispatch struct {
	Job         string `json:"job"`
	Preparation string `json:"preparation"`
	Index       int    `json:"index"`
}

func (s *Store) Pending(ctx context.Context, owner string) ([]Dispatch, error) {
	rows, e := s.pool.Query(ctx, `SELECT o.job,o.preparation,o.idx FROM processor_outbox o JOIN processor_jobs j ON j.id=o.job JOIN processor_chunks c ON c.job=o.job AND c.preparation=o.preparation AND c.idx=o.idx WHERE j.owner=$1 AND j.doc->>'state'='processing' AND j.doc->>'preparation'=o.preparation AND c.doc->>'state'<>'done' AND (o.sent_at IS NULL OR o.sent_at<clock_timestamp()-interval '30 seconds') LIMIT 128`, owner)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Dispatch{}
	for rows.Next() {
		var d Dispatch
		if e = rows.Scan(&d.Job, &d.Preparation, &d.Index); e != nil {
			return nil, e
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
func (s *Store) Sent(ctx context.Context, d Dispatch) error {
	_, e := s.pool.Exec(ctx, "UPDATE processor_outbox SET sent_at=clock_timestamp() WHERE job=$1 AND preparation=$2 AND idx=$3", d.Job, d.Preparation, d.Index)
	return e
}
func (s *Store) Diagnostic(ctx context.Context, owner, id string) (map[string]any, error) {
	j, e := s.Get(ctx, owner, id)
	if e != nil {
		return nil, e
	}
	c, e := s.Chunks(ctx, j)
	if e != nil {
		return nil, e
	}
	return map[string]any{"job": j, "chunks": c}, nil
}
func safeError(stage string) error {
	return fmt.Errorf("%s unavailable; check the selected profile doctor and role diagnostics", stage)
}

// CheckBackend is read-only. Legacy unbound owners can continue in local mode;
// a cloud owner requires an explicit binding made by migrate --profile aws.
func (s *Store) CheckBackend(ctx context.Context, owner, identity string, allowLocal bool) error {
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = compatible(ctx, tx); e != nil {
		return e
	}
	var version int
	if e = tx.QueryRow(ctx, "SELECT max(version) FROM processor_schema").Scan(&version); e != nil {
		return e
	}
	if version == 1 {
		if allowLocal {
			return nil
		}
		return ErrBackend
	}
	var target string
	e = tx.QueryRow(ctx, "SELECT identity FROM processor_backends WHERE owner=$1", owner).Scan(&target)
	if errors.Is(e, pgx.ErrNoRows) && allowLocal {
		return nil
	}
	if e != nil || target != identity {
		return ErrBackend
	}
	return nil
}
