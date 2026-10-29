package durable

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// RecentJob contains only owner-visible history fields, never lease tokens,
// credentials, storage keys, or input contents.
type RecentJob struct {
	ID        string    `json:"id"`
	State     string    `json:"state"`
	Created   time.Time `json:"created_at"`
	Updated   time.Time `json:"updated_at"`
	Accepted  int64     `json:"accepted"`
	Rejected  int64     `json:"rejected"`
	Committed int       `json:"committed_chunks"`
	Parts     int       `json:"parts"`
}

// RecentJobs uses the same metadata expiry boundary as Get. The list is bounded
// and deterministic; it does not change job state or extend retention.
func (s *Store) RecentJobs(ctx context.Context, owner string, limit int) ([]RecentJob, error) {
	if limit < 1 || limit > 100 {
		return nil, errors.New("history limit must be between 1 and 100")
	}
	rows, e := s.pool.Query(ctx, `SELECT doc FROM processor_jobs WHERE owner=$1 AND (doc->>'metadata_expires_at')::timestamptz>clock_timestamp() ORDER BY (doc->>'created_at')::timestamptz DESC,id DESC LIMIT $2`, owner, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	jobs := []RecentJob{}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		var j RecentJob
		if e = json.Unmarshal(b, &j); e != nil {
			return nil, e
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}
