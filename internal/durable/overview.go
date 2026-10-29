package durable

import (
	"context"
	"time"
)

// Overview is an owner-scoped database snapshot, not a pod health or queue-depth
// assertion. Metadata expiry and lease comparisons use one database timestamp.
type Overview struct {
	ObservedAt                                  time.Time
	Total, Active, Succeeded, Failed, Cancelled int64
	LiveLeases, ExpiredLeases                   int64
}

func (s *Store) Overview(ctx context.Context, owner string) (Overview, error) {
	var v Overview
	e := s.pool.QueryRow(ctx, `WITH visible AS MATERIALIZED (
 SELECT id,doc FROM processor_jobs WHERE owner=$1 AND (doc->>'metadata_expires_at')::timestamptz>statement_timestamp()
 ), leases AS (
 SELECT (c.doc->>'until')::timestamptz AS until FROM processor_chunks c JOIN visible j ON j.id=c.job
 WHERE j.doc->>'state'='processing' AND j.doc->>'preparation'=c.preparation AND c.doc->>'state'='running'
 ) SELECT statement_timestamp(),count(*),
 count(*) FILTER(WHERE doc->>'state' NOT IN ('succeeded','failed','cancelled')),
 count(*) FILTER(WHERE doc->>'state'='succeeded'),count(*) FILTER(WHERE doc->>'state'='failed'),count(*) FILTER(WHERE doc->>'state'='cancelled'),
 (SELECT count(*) FROM leases WHERE until>statement_timestamp()),
 (SELECT count(*) FROM leases WHERE until<=statement_timestamp()) FROM visible`, owner).Scan(&v.ObservedAt, &v.Total, &v.Active, &v.Succeeded, &v.Failed, &v.Cancelled, &v.LiveLeases, &v.ExpiredLeases)
	return v, e
}
