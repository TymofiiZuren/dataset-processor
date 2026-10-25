package durable

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"time"

	"dataset-processor/internal/processor"
)

type Runtime struct {
	Store      *Store
	Objects    *Objects
	Owner      string
	ChunkBytes int
	Lease      time.Duration
}

func (r *Runtime) lease() time.Duration {
	if r.Lease > 0 {
		return r.Lease
	}
	return time.Minute
}
func (r *Runtime) target() int {
	if r.ChunkBytes > 0 {
		return r.ChunkBytes
	}
	return 16 << 20
}
func descriptor(b []byte) string { return Hash(b) + ":" + strconv.Itoa(len(b)) }
func (r *Runtime) Submit(ctx context.Context, key string, b []byte) (Job, error) {
	if e := r.CheckBackend(ctx); e != nil {
		return Job{}, e
	}
	if len(b) > MaxInput {
		return Job{}, &processor.Error{Code: "file_too_large"}
	}
	j, e := r.Store.Admit(ctx, r.Owner, key, descriptor(b), r.Objects.Backend)
	if e != nil {
		return j, e
	}
	if j.State != "uploading" {
		return j, nil
	}
	// Each write, including a duplicate or ambiguous upload, is reserved before IO.
	if e = r.Store.Reserve(ctx, j.ID, int64(len(b))); e != nil {
		return j, e
	}
	ref, e := r.Objects.Put(ctx, j.Input.Key, b)
	if e != nil {
		return j, e
	}
	if e = r.Store.InputReady(ctx, j.ID, ref); e != nil {
		if errors.Is(e, ErrStale) {
			return r.Store.Get(ctx, r.Owner, j.ID)
		}
		return j, e
	}
	return r.Store.Get(ctx, r.Owner, j.ID)
}
func renewed(ctx context.Context, lease time.Duration, renew func(context.Context) error, fn func(context.Context) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		tick := time.NewTicker(max(lease/4, time.Millisecond))
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-tick.C:
				if renew(ctx) != nil {
					cancel()
					return
				}
			}
		}
	}()
	e := fn(ctx)
	close(done)
	<-stopped
	return e
}
func (r *Runtime) Coordinate(ctx context.Context) error {
	if e := r.CheckBackend(ctx); e != nil {
		return e
	}
	if e := r.Store.Check(ctx); e != nil {
		return e
	}
	jobs, e := r.Store.Active(ctx, r.Owner)
	if e != nil {
		return e
	}
	for _, j := range jobs {
		if j.Owner != r.Owner {
			continue
		}
		if e = r.Store.Expire(ctx, j.ID); e != nil {
			return e
		}
		switch j.State {
		case "uploading":
			ref, e := r.Objects.RecoverInput(ctx, j.Input.Key)
			if e == nil && j.Descriptor == ref.SHA256+":"+strconv.FormatInt(ref.Size, 10) {
				_ = r.Store.InputReady(ctx, j.ID, ref)
			}
		case "preparing":
			p, e := r.Store.ClaimStage(ctx, j.ID, "preparing", r.lease())
			if errors.Is(e, ErrStale) {
				continue
			}
			if e != nil {
				return e
			}
			e = renewed(ctx, r.lease(), func(c context.Context) error { return r.Store.RenewStage(c, p, r.lease()) }, func(c context.Context) error { return r.prepare(c, p) })
			if e != nil {
				var invalid *processor.Error
				retry := !errors.As(e, &invalid) && !errors.Is(e, ErrIntegrity) && !errors.Is(e, ErrCapacity)
				code := "PREPARATION_UNAVAILABLE"
				if invalid != nil {
					code = invalid.Code
				}
				if errors.Is(e, ErrIntegrity) {
					code = "INTEGRITY_ERROR"
				}
				if errors.Is(e, ErrCapacity) {
					code = "CAPACITY_EXHAUSTED"
				}
				_ = r.Store.FailStage(ctx, p, code, retry)
				log.Printf("role=coordinator job=%s code=%s", j.ID, code)
			}
		case "processing", "finalizing":
			p, e := r.Store.ClaimStage(ctx, j.ID, "finalizing", r.lease())
			if errors.Is(e, ErrStale) {
				continue
			}
			if e != nil {
				return e
			}
			e = renewed(ctx, r.lease(), func(c context.Context) error { return r.Store.RenewStage(c, p, r.lease()) }, func(c context.Context) error { return r.finalize(c, p) })
			if e != nil {
				code := "FINALIZATION_UNAVAILABLE"
				retry := !errors.Is(e, ErrIntegrity)
				if !retry {
					code = "INTEGRITY_ERROR"
				}
				_ = r.Store.FailStage(ctx, p, code, retry)
				log.Printf("role=coordinator job=%s code=%s", j.ID, code)
			}
		}
	}
	dispatch, e := r.Store.Pending(ctx, r.Owner)
	if e != nil {
		return e
	}
	for _, d := range dispatch {
		if e = r.Objects.Send(ctx, d); e != nil {
			return e
		}
		if e = r.Store.Sent(ctx, d); e != nil {
			return e
		}
	}
	return nil
}
func (r *Runtime) put(ctx context.Context, job, key string, b []byte) (Ref, error) {
	if e := r.Store.Reserve(ctx, job, int64(len(b))); e != nil {
		return Ref{}, e
	}
	return r.Objects.Put(ctx, key, b)
}
func (r *Runtime) prepare(ctx context.Context, p Job) error {
	data, e := r.Objects.Read(ctx, p.Input)
	if e != nil {
		return e
	}
	parts := 0
	limits := processor.DefaultLimits()
	limits.MaxFileBytes = MaxInput
	m, e := processor.Prepare(bytes.NewReader(data), limits, r.target(), func(part processor.Part) error {
		if parts >= 1024 {
			return ErrCapacity
		}
		ref, e := r.put(ctx, p.ID, fmt.Sprintf("jobs/%s/prepare/%s/%d", p.ID, p.Token, parts), part.Data)
		if e != nil {
			return e
		}
		c := Chunk{Index: parts, First: part.FirstOrdinal, Records: part.Records, Input: ref, Preparation: p.Token}
		if e = r.Store.StageChunk(ctx, p, c); e != nil {
			return e
		}
		parts++
		return nil
	})
	if e != nil {
		return e
	}
	if m.InputSHA256 != p.Input.SHA256 || m.InputBytes != p.Input.Size {
		return ErrIntegrity
	}
	return r.Store.SelectPreparation(ctx, p, parts, m.Records)
}
func (r *Runtime) Work(ctx context.Context, d Dispatch) error {
	if e := r.CheckBackend(ctx); e != nil {
		return e
	}
	j, e := r.Store.Get(ctx, r.Owner, d.Job)
	if e != nil {
		return e
	}
	if d.Preparation != j.Preparation {
		return ErrStale
	}
	c, e := r.Store.ClaimChunk(ctx, d.Job, d.Index, r.lease())
	if e != nil {
		return e
	}
	e = renewed(ctx, r.lease(), func(ctx context.Context) error { return r.Store.RenewChunk(ctx, c, r.lease()) }, func(ctx context.Context) error {
		data, e := r.Objects.Read(ctx, c.Input)
		if e != nil {
			return e
		}
		var clean, rejected bytes.Buffer
		budget := int64(maxDerived)
		m, e := processor.ProcessPart(bytes.NewReader(data), &boundedOutput{&clean, &budget}, &boundedOutput{&rejected, &budget}, processor.DefaultLimits(), c.First)
		if e != nil {
			return e
		}
		if m.Records != c.Records || m.InputSHA256 != c.Input.SHA256 {
			return ErrIntegrity
		}
		prefix := fmt.Sprintf("jobs/%s/attempt/%s/", c.Job, c.Token)
		cr, e := r.put(ctx, c.Job, prefix+"clean", clean.Bytes())
		if e != nil {
			return e
		}
		rr, e := r.put(ctx, c.Job, prefix+"rejected", rejected.Bytes())
		if e != nil {
			return e
		}
		// Verify exact written versions before selecting the attempt.
		if _, e = r.Objects.Read(ctx, cr); e != nil {
			return e
		}
		if _, e = r.Objects.Read(ctx, rr); e != nil {
			return e
		}
		return r.Store.CommitChunk(ctx, c, cr, rr, m.Accepted, m.Rejected)
	})
	if e != nil {
		retry := !errors.Is(e, ErrIntegrity) && !errors.Is(e, ErrCapacity)
		code := "WORKER_UNAVAILABLE"
		if !retry {
			code = "INTEGRITY_ERROR"
		}
		if errors.Is(e, ErrCapacity) {
			code = "CAPACITY_EXHAUSTED"
		}
		_ = r.Store.FailChunk(ctx, c, code, retry)
	}
	return e
}
func (r *Runtime) finalize(ctx context.Context, p Job) error {
	c, rh := sha256.New(), sha256.New()
	if e := r.assemble(ctx, p, "clean.csv", c); e != nil {
		return e
	}
	if e := r.assemble(ctx, p, "rejected.csv", rh); e != nil {
		return e
	}
	m := Manifest{Manifest: processor.Manifest{Version: p.Version, Records: p.Total, Accepted: p.Accepted, Rejected: p.Rejected, InputBytes: p.Input.Size, InputSHA256: p.Input.SHA256, CleanSHA256: hex.EncodeToString(c.Sum(nil)), RejectedSHA256: hex.EncodeToString(rh.Sum(nil))}, Parts: p.Parts}
	return r.Store.Publish(ctx, p, m)
}
func (r *Runtime) assemble(ctx context.Context, j Job, name string, w io.Writer) error {
	chunks, e := r.Store.Chunks(ctx, j)
	if e != nil {
		return e
	}
	if len(chunks) != j.Parts {
		return ErrIntegrity
	}
	c, rh := processor.OutputHeaders()
	header := c
	if name == "rejected.csv" {
		header = rh
	}
	if _, e = w.Write(header); e != nil {
		return e
	}
	var next int64 = 1
	for idx, c := range chunks {
		if c.Index != idx || c.State != "done" || c.First != next || c.Records <= 0 || c.Accepted+c.RejectedCount != c.Records {
			return ErrIntegrity
		}
		next += c.Records
		ref := c.Clean
		if name == "rejected.csv" {
			ref = c.Rejected
		}
		b, e := r.Objects.Read(ctx, ref)
		if e != nil {
			return e
		}
		if _, e = w.Write(b); e != nil {
			return e
		}
	}
	if next-1 != j.Total {
		return ErrIntegrity
	}
	return nil
}
func (r *Runtime) Download(ctx context.Context, j Job, name string, w io.Writer) error {
	if e := r.CheckBackend(ctx); e != nil {
		return e
	}
	if j.State != "succeeded" || j.Manifest == nil {
		return ErrConflict
	}
	if !time.Now().Before(j.Expires) {
		return ErrExpired
	}
	if name == "manifest.json" {
		return json.NewEncoder(w).Encode(j.Manifest)
	}
	if name != "clean.csv" && name != "rejected.csv" {
		return ErrNotFound
	}
	h := sha256.New()
	if e := r.assemble(ctx, j, name, io.MultiWriter(w, h)); e != nil {
		return e
	}
	want := j.Manifest.CleanSHA256
	if name == "rejected.csv" {
		want = j.Manifest.RejectedSHA256
	}
	if hex.EncodeToString(h.Sum(nil)) != want {
		return ErrIntegrity
	}
	return nil
}
func (r *Runtime) Run(ctx context.Context, role string) error {
	if role != "coordinator" && role != "worker" {
		return errors.New("unknown runtime role")
	}
	for ctx.Err() == nil {
		var e error
		if role == "coordinator" {
			e = r.Coordinate(ctx)
		} else {
			var m *Message
			m, e = r.Objects.Receive(ctx)
			if e == nil && m != nil {
				if m.Job == "" {
					e = r.Objects.Ack(ctx, *m)
				} else {
					done := make(chan struct{})
					go func() {
						tick := time.NewTicker(15 * time.Second)
						defer tick.Stop()
						for {
							select {
							case <-done:
								return
							case <-ctx.Done():
								return
							case <-tick.C:
								_ = r.Objects.Visibility(ctx, *m)
							}
						}
					}()
					e = r.Work(ctx, m.Dispatch)
					close(done)
					// Duplicate hints can be removed: durable outbox reconciliation will
					// wake pending or expired work again even if another worker is active.
					if e == nil || errors.Is(e, ErrStale) || errors.Is(e, ErrNotFound) {
						e = r.Objects.Ack(ctx, *m)
					}
				}
			}
		}
		if ctx.Err() != nil {
			break
		}
		if e != nil {
			log.Printf("role=%s code=DEPENDENCY_OR_WORK_UNAVAILABLE", role)
		}
		if role == "coordinator" || e != nil {
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
	return nil
}

// A shared byte budget bounds both outputs before buffer growth and storage IO.
type boundedOutput struct {
	writer    io.Writer
	remaining *int64
}

func (w *boundedOutput) Write(p []byte) (int, error) {
	if int64(len(p)) > *w.remaining {
		return 0, ErrCapacity
	}
	n, e := w.writer.Write(p)
	*w.remaining -= int64(n)
	return n, e
}

func (r *Runtime) CheckBackend(ctx context.Context) error {
	if r.Objects == nil {
		return ErrBackend
	}
	return r.Store.CheckBackend(ctx, r.Owner, r.Objects.Backend, !r.Objects.Cloud)
}
