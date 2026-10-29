package durable

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestOverviewOwnerExpiryAndCurrentGeneration(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	owner := ID()
	empty, e := s.Overview(ctx, owner)
	if e != nil || empty.Total != 0 || empty.ObservedAt.IsZero() {
		t.Fatal("empty overview incorrect")
	}
	add := func(who, state string, expired bool) Job {
		t.Helper()
		j, e := s.Admit(ctx, who, ID(), "fixture", "local:fixture")
		if e != nil {
			t.Fatal(e)
		}
		if e = s.Cancel(ctx, who, j.ID); e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { s.Cancel(ctx, who, j.ID) })
		j.State = state
		j.Preparation = "selected"
		if expired {
			j.MetadataExpires = time.Now().Add(-time.Hour)
		}
		b, _ := json.Marshal(j)
		if _, e = s.pool.Exec(ctx, `UPDATE processor_jobs SET doc=$2 WHERE id=$1`, j.ID, b); e != nil {
			t.Fatal(e)
		}
		return j
	}
	active := add(owner, "processing", false)
	add(owner, "succeeded", false)
	add(owner, "failed", false)
	add(owner, "cancelled", false)
	foreign := add(ID(), "processing", false)
	expired := add(owner, "processing", true)
	chunk := func(j Job, index int, prep, state string, until time.Time) {
		t.Helper()
		c := Chunk{Index: index, State: state, Until: until}
		b, _ := json.Marshal(c)
		if _, e = s.pool.Exec(ctx, `INSERT INTO processor_chunks(job,preparation,idx,doc) VALUES($1,$2,$3,$4)`, j.ID, prep, index, b); e != nil {
			t.Fatal(e)
		}
	}
	past := time.Now().Add(-time.Hour)
	future := time.Now().Add(time.Hour)
	chunk(active, 0, "selected", "running", future)
	chunk(active, 1, "selected", "running", past)
	chunk(active, 2, "old", "running", past)
	chunk(active, 3, "selected", "done", past)
	chunk(foreign, 0, "selected", "running", past)
	chunk(expired, 0, "selected", "running", past)
	got, e := s.Overview(ctx, owner)
	if e != nil {
		t.Fatal(e)
	}
	if got.Total != 4 || got.Active != 1 || got.Succeeded != 1 || got.Failed != 1 || got.Cancelled != 1 || got.LiveLeases != 1 || got.ExpiredLeases != 1 {
		t.Fatal("overview includes foreign, expired, obsolete or completed chunk state")
	}
}
