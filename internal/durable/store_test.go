package durable

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("PROCESSOR_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("explicit integration suite requires PROCESSOR_TEST_DATABASE_URL")
	}
	s, e := Open(context.Background(), url)
	if e != nil {
		t.Fatal("test database unavailable")
	}
	t.Cleanup(s.Close)
	if e = s.Migrate(context.Background()); e != nil {
		t.Fatal(e)
	}
	return s
}
func TestStoreFencesStaleCompletionAndFailure(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	owner := ID()
	j, e := s.Admit(ctx, owner, ID(), "fixture", "local:fixture")
	if e != nil {
		t.Fatal(e)
	}
	if e = s.InputReady(ctx, j.ID, Ref{Key: j.Input.Key, Version: "v1", SHA256: "hash", Size: 100}); e != nil {
		t.Fatal(e)
	}
	p, e := claimPreparation(t, s, ctx, j.ID, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	c := Chunk{Index: 0, First: 1, Records: 1, Input: Ref{Key: "part", Version: "v1", Size: 10}, Preparation: p.Token}
	if e = s.StageChunk(ctx, p, c); e != nil {
		t.Fatal(e)
	}
	if e = s.SelectPreparation(ctx, p, 1, 1); e != nil {
		t.Fatal(e)
	}
	a, e := s.ClaimChunk(ctx, j.ID, 0, time.Millisecond)
	if e != nil {
		t.Fatal(e)
	}
	time.Sleep(5 * time.Millisecond)
	b, e := s.ClaimChunk(ctx, j.ID, 0, time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	if b.Token == a.Token || b.Attempts != 2 {
		t.Fatal("no replacement attempt")
	}
	if e = s.CommitChunk(ctx, a, Ref{}, Ref{}, 1, 0); e != ErrStale {
		t.Fatalf("stale commit: %v", e)
	}
	if e = s.FailChunk(ctx, a, "stale", false); e != ErrStale {
		t.Fatalf("stale failure: %v", e)
	}
	if e = s.CommitChunk(ctx, b, Ref{Version: "v"}, Ref{Version: "v"}, 1, 0); e != nil {
		t.Fatal(e)
	}
	f, e := s.ClaimStage(ctx, j.ID, "finalizing", time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Cancel(ctx, owner, j.ID); e != nil {
		t.Fatal(e)
	}
	if e = s.Publish(ctx, f, Manifest{}); e != ErrStale {
		t.Fatal("cancelled publication")
	}
	got, e := s.Get(ctx, owner, j.ID)
	if e != nil || got.State != "cancelled" {
		t.Fatal("cancel lost")
	}
}
func TestAdmissionIdempotencyAndCapacity(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	owner, key := ID(), ID()
	a, e := s.Admit(ctx, owner, key, "same", "local:fixture")
	if e != nil {
		t.Fatal(e)
	}
	b, e := s.Admit(ctx, owner, key, "same", "local:fixture")
	if e != nil || a.ID != b.ID {
		t.Fatal("identity changed")
	}
	if _, e = s.Admit(ctx, owner, key, "different", "local:fixture"); e != ErrConflict {
		t.Fatal("conflict missing")
	}
	if _, e = s.Get(ctx, ID(), a.ID); e != ErrNotFound {
		t.Fatal("ownership leak")
	}
	second, e := s.Admit(ctx, owner, ID(), "second", "local:fixture")
	if e != nil {
		t.Fatal(e)
	}
	defer s.Cancel(ctx, owner, second.ID)
	if _, e = s.Admit(ctx, owner, ID(), "third", "local:fixture"); e != ErrCapacity {
		t.Fatal("active admission limit not enforced")
	}
	if e = s.Cancel(ctx, owner, a.ID); e != nil {
		t.Fatal(e)
	}
	third, e := s.Admit(ctx, owner, ID(), "third", "local:fixture")
	if e != nil {
		t.Fatal("cancel did not release active job slot", e)
	}
	defer s.Cancel(ctx, owner, third.ID)
}

func readyToFinalize(t *testing.T, s *Store) (string, Job) {
	t.Helper()
	ctx := context.Background()
	owner := ID()
	j, e := s.Admit(ctx, owner, ID(), "fixture", "local:fixture")
	if e != nil {
		t.Fatal(e)
	}
	if e = s.InputReady(ctx, j.ID, Ref{Key: j.Input.Key, Version: "v1", SHA256: "hash", Size: 1}); e != nil {
		t.Fatal(e)
	}
	p, e := claimPreparation(t, s, ctx, j.ID, time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.SelectPreparation(ctx, p, 0, 0); e != nil {
		t.Fatal(e)
	}
	f, e := s.ClaimStage(ctx, j.ID, "finalizing", time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	return owner, f
}
func TestCancelPublishBothLockOrders(t *testing.T) {
	s := testStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, cancelFirst := range []bool{true, false} {
		owner, f := readyToFinalize(t, s)
		tx, e := s.pool.Begin(ctx)
		if e != nil {
			t.Fatal(e)
		}
		if _, _, e = load(ctx, tx, f.ID); e != nil {
			t.Fatal(e)
		}
		first, second := make(chan error, 1), make(chan error, 1)
		cancelJob := func() error { return s.Cancel(ctx, owner, f.ID) }
		publish := func() error { return s.Publish(ctx, f, Manifest{}) }
		a, b := publish, cancelJob
		if cancelFirst {
			a, b = cancelJob, publish
		}
		go func() { first <- a() }()
		// Observe a real PostgreSQL lock wait before starting the opposing transition.
		waitForLocks := func(want int) {
			t.Helper()
			for {
				var n int
				e := s.pool.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT doc,owner FROM processor_jobs%'").Scan(&n)
				if e != nil {
					t.Fatal(e)
				}
				if n >= want {
					return
				}
				select {
				case <-ctx.Done():
					t.Fatal("lock barrier timeout")
				case <-time.After(time.Millisecond):
				}
			}
		}
		waitForLocks(1)
		go func() { second <- b() }()
		waitForLocks(2)
		if e = tx.Commit(ctx); e != nil {
			t.Fatal(e)
		}
		if e = <-first; e != nil {
			t.Fatal("first transition failed", e)
		}
		e = <-second
		if cancelFirst && e != ErrStale {
			t.Fatal("cancel did not fence publication", e)
		}
		if !cancelFirst && e != ErrConflict {
			t.Fatal("publication did not win", e)
		}
		j, e := s.Get(ctx, owner, f.ID)
		if e != nil {
			t.Fatal(e)
		}
		want := "succeeded"
		if cancelFirst {
			want = "cancelled"
		}
		if j.State != want {
			t.Fatal("wrong terminal state")
		}
	}
}

func TestSchemaRangeAndChecksumCheckedInsideTransaction(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "UPDATE processor_schema SET checksum='incompatible-test' WHERE version=1"); e != nil {
		t.Fatal(e)
	}
	if e = compatible(ctx, tx); e == nil {
		t.Fatal("running transition accepted incompatible schema")
	}
	// This transaction rolls back; other runtime roles see unchanged committed schema.
}
func TestPartialPreparationAndStaleFailureAreFenced(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	owner := ID()
	j, e := s.Admit(ctx, owner, ID(), "fixture", "local:fixture")
	if e != nil {
		t.Fatal(e)
	}
	defer s.Cancel(ctx, owner, j.ID)
	if e = s.InputReady(ctx, j.ID, Ref{Key: j.Input.Key, Version: "v", Size: 1}); e != nil {
		t.Fatal(e)
	}
	old, e := claimPreparation(t, s, ctx, j.ID, time.Millisecond)
	if e != nil {
		t.Fatal(e)
	}
	// No selected preparation generation: its outbox cannot become runnable.
	pending, e := s.Pending(ctx, owner)
	if e != nil || len(pending) != 0 {
		t.Fatal("unselected dispatch")
	}
	time.Sleep(5 * time.Millisecond)
	current, e := claimPreparation(t, s, ctx, j.ID, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.FailStage(ctx, old, "STALE", false); e != ErrStale {
		t.Fatal("stale stage failure accepted")
	}
	if e = s.SelectPreparation(ctx, current, 0, 0); e != nil {
		t.Fatal(e)
	}
	if e = s.FailStage(ctx, old, "STALE", false); e != ErrStale {
		t.Fatal("old failure changed selected generation")
	}
}

func claimPreparation(t *testing.T, s *Store, ctx context.Context, id string, lease time.Duration) (Job, error) {
	t.Helper()
	until := time.Now().Add(10 * time.Second)
	for {
		j, e := s.ClaimStage(ctx, id, "preparing", lease)
		if e != ErrStale || time.Now().After(until) {
			return j, e
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestBackendBindingCannotSwitchProfilesOrAdoptCloudJobs(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	owner := ID()
	if e := s.BindBackend(ctx, owner, "local:fixture", true); e != nil {
		t.Fatal(e)
	}
	if e := s.BindBackend(ctx, owner, "local:fixture", true); e != nil {
		t.Fatal("idempotent binding", e)
	}
	if e := s.BindBackend(ctx, owner, "aws:other", false); e != ErrBackend {
		t.Fatal("backend switch accepted")
	}
	oldOwner := ID()
	legacy := Job{ID: ID(), State: "uploading", Generation: 1, MetadataExpires: time.Now().Add(time.Hour)}
	data, _ := json.Marshal(legacy)
	_, e := s.pool.Exec(ctx, "INSERT INTO processor_jobs VALUES($1,$2,$3,$4,$5)", legacy.ID, oldOwner, ID(), "legacy", data)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Cancel(ctx, oldOwner, legacy.ID)
	if e = s.BindBackend(ctx, oldOwner, "aws:fixture", false); e != ErrBackend {
		t.Fatal("unbound legacy jobs adopted by cloud")
	}
}
func TestRemoteDatabaseRequiresVerifiedTLS(t *testing.T) {
	for _, dsn := range []string{"postgres://db.example.test/db?sslmode=disable", "postgres://db.example.test/db?sslmode=require", "postgres://db.example.test/db?sslmode=verify-ca"} {
		if _, e := databaseConfig(dsn, "aws"); e == nil {
			t.Fatal("unverified remote TLS accepted")
		}
	}
	if _, e := databaseConfig("postgres://db.example.test/db?sslmode=verify-full", "aws"); e != nil {
		t.Fatal(e)
	}
	if _, e := databaseConfig("postgres://db.example.test/db?sslmode=verify-full", "local"); e == nil {
		t.Fatal("local profile lost loopback restriction")
	}
}

func TestCloudBindingBlocksReverseSwitchAndPrematureIO(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	owner := ID()
	cloud := &Runtime{Store: s, Owner: owner, Objects: &Objects{Cloud: true, Backend: "aws:target"}}
	// Nil SDK clients are intentional: any attempted object IO would panic.
	if _, e := cloud.Submit(ctx, ID(), []byte("synthetic")); e != ErrBackend {
		t.Fatal("unbound cloud owner reached storage")
	}
	if e := s.BindBackend(ctx, owner, "aws:target", false); e != nil {
		t.Fatal(e)
	}
	if e := cloud.CheckBackend(ctx); e != nil {
		t.Fatal(e)
	}
	local := &Runtime{Store: s, Owner: owner, Objects: &Objects{Backend: "local:target"}}
	if _, e := local.Submit(ctx, ID(), []byte("synthetic")); e != ErrBackend {
		t.Fatal("cloud jobs could switch to local backend")
	}
	if e := s.Migrate(ctx); e != nil {
		t.Fatal("idempotent migration failed")
	}
	if e := cloud.CheckBackend(ctx); e != nil {
		t.Fatal("migration lost binding")
	}
}

func TestAdmissionAndCloudBindingSerializeOnOwner(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	for i := 0; i < 8; i++ {
		owner := ID()
		start := make(chan struct{})
		cloudDone := make(chan error, 1)
		localDone := make(chan error, 1)
		go func() { <-start; cloudDone <- s.BindBackend(ctx, owner, "aws:race", false) }()
		go func() {
			<-start
			j, e := s.Admit(ctx, owner, ID(), "race", "local:race")
			if e == nil {
				defer s.Cancel(ctx, owner, j.ID)
			}
			localDone <- e
		}()
		close(start)
		cloudErr, localErr := <-cloudDone, <-localDone
		if cloudErr == nil && localErr == nil {
			t.Fatal("local admission raced into cloud namespace")
		}
		if cloudErr != nil && cloudErr != ErrBackend {
			t.Fatal(cloudErr)
		}
		if localErr != nil && localErr != ErrBackend {
			t.Fatal(localErr)
		}
		if cloudErr != nil && localErr != nil {
			t.Fatal("neither serialized action won")
		}
	}
}
