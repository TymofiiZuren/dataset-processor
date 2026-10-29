package durable

import (
	"context"
	"errors"
	"time"
)

type Event struct {
	Revision    int64
	State, Code string
	At          time.Time
}
type Activity struct {
	Events    []Event
	Truncated bool
}

// Activity returns persisted transitions through the caller's visible revision.
// Authorization and metadata expiry are checked independently of the caller.
func (s *Store) Activity(ctx context.Context, owner, id string, revision int64) (Activity, error) {
	result := Activity{Events: []Event{}}
	if revision < 0 {
		return result, errors.New("activity revision cannot be negative")
	}
	j, e := s.Get(ctx, owner, id)
	if e != nil {
		return result, e
	}
	if revision > j.Revision {
		revision = j.Revision
	}
	rows, e := s.pool.Query(ctx, `SELECT e.revision,e.state,e.code,e.at FROM processor_events e JOIN processor_jobs j ON j.id=e.job WHERE j.id=$1 AND j.owner=$2 AND (j.doc->>'metadata_expires_at')::timestamptz>clock_timestamp() AND e.revision<=$3 ORDER BY e.revision DESC,e.seq DESC LIMIT 51`, id, owner, revision)
	if e != nil {
		return result, e
	}
	defer rows.Close()
	for rows.Next() {
		var event Event
		if e = rows.Scan(&event.Revision, &event.State, &event.Code, &event.At); e != nil {
			return result, e
		}
		result.Events = append(result.Events, event)
	}
	if e = rows.Err(); e != nil {
		return result, e
	}
	if len(result.Events) > 50 {
		result.Events = result.Events[:50]
		result.Truncated = true
	}
	for i, k := 0, len(result.Events)-1; i < k; i, k = i+1, k-1 {
		result.Events[i], result.Events[k] = result.Events[k], result.Events[i]
	}
	return result, nil
}
