package durable

import (
	"context"
	"encoding/json"
	"testing"
)

func TestRecentJobsOwnerExpiryOrderAndLimit(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	owner := ID()
	other := ID()
	add := func(who string) Job {
		t.Helper()
		j, e := s.Admit(ctx, who, ID(), "fixture", "local:fixture")
		if e != nil {
			t.Fatal(e)
		}
		if e = s.Cancel(ctx, who, j.ID); e != nil {
			t.Fatal(e)
		}
		return j
	}
	older := add(owner)
	newer := add(owner)
	foreign := add(other)
	expired := add(owner)
	if _, e := s.pool.Exec(ctx, `UPDATE processor_jobs SET doc=jsonb_set(doc,'{metadata_expires_at}',to_jsonb((clock_timestamp()-interval '1 day')::text)) WHERE id=$1`, expired.ID); e != nil {
		t.Fatal(e)
	}
	jobs, e := s.RecentJobs(ctx, owner, 20)
	if e != nil {
		t.Fatal(e)
	}
	if len(jobs) != 2 || jobs[0].ID != newer.ID || jobs[1].ID != older.ID {
		t.Fatal("history owner, expiry or newest-first contract failed")
	}
	b, _ := json.Marshal(jobs)
	for _, j := range jobs {
		if j.ID == foreign.ID || j.ID == expired.ID {
			t.Fatal("history includes inaccessible job")
		}
	}
	if len(b) == 0 {
		t.Fatal("empty serialization")
	}
	jobs, e = s.RecentJobs(ctx, owner, 1)
	if e != nil || len(jobs) != 1 || jobs[0].ID != newer.ID {
		t.Fatal("limit ignored")
	}
	for _, limit := range []int{0, -1, 101} {
		if _, e = s.RecentJobs(ctx, owner, limit); e == nil {
			t.Fatal("unbounded history accepted")
		}
	}
}
