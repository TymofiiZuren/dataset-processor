package durable

import (
	"context"
	"errors"
	"testing"
)

func TestActivityOwnerExpiryBoundAndRevision(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	owner := ID()
	j, e := s.Admit(ctx, owner, ID(), "fixture", "local:fixture")
	if e != nil {
		t.Fatal(e)
	}
	defer s.Cancel(ctx, owner, j.ID)
	if _, e = s.Activity(ctx, ID(), j.ID, 100); !errors.Is(e, ErrNotFound) {
		t.Fatal("foreign activity exposed")
	}
	empty, e := s.Activity(ctx, owner, j.ID, 0)
	if e != nil || len(empty.Events) != 0 || empty.Truncated {
		t.Fatal("new job invents events")
	}
	// Persist a long audit trail to test truncation without running 60 workers.
	for i := 1; i <= 60; i++ {
		if _, e = s.pool.Exec(ctx, `INSERT INTO processor_events(job,revision,state,code) VALUES($1,$2,'processing','chunk_committed')`, j.ID, i); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = s.pool.Exec(ctx, `UPDATE processor_jobs SET doc=jsonb_set(doc,'{revision}','60'::jsonb) WHERE id=$1`, j.ID); e != nil {
		t.Fatal(e)
	}
	activity, e := s.Activity(ctx, owner, j.ID, 60)
	if e != nil {
		t.Fatal(e)
	}
	if !activity.Truncated || len(activity.Events) != 50 || activity.Events[0].Revision != 11 || activity.Events[49].Revision != 60 {
		t.Fatal("latest 50 events not chronological")
	}
	activity, e = s.Activity(ctx, owner, j.ID, 3)
	if e != nil || len(activity.Events) != 3 || activity.Events[2].Revision != 3 {
		t.Fatal("events exceed visible job revision")
	}
	if _, e = s.pool.Exec(ctx, `UPDATE processor_jobs SET doc=jsonb_set(doc,'{metadata_expires_at}',to_jsonb((clock_timestamp()-interval '1 day')::text)) WHERE id=$1`, j.ID); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Activity(ctx, owner, j.ID, 60); !errors.Is(e, ErrNotFound) {
		t.Fatal("expired activity exposed")
	}
}
