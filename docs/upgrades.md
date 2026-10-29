# Schema and runtime upgrades

Current schema/transformation: schema 2 / transactions-v1. Migration 1 source/checksum is preserved; migration 2 adds processor_backends with an immutable owner-to-backend identity. New code reads schema 1 for legacy local operation and schema 2 for bound profiles; AWS requires schema 2. `migrate` takes a transaction-scoped advisory lock, creates the schema transactionally and records its source checksum. Interrupted uncommitted migration rolls back and can be rerun. A checksum mismatch or unsupported schema version is rejected. There is no destructive downgrade/reset tool.

All state-changing role transactions recheck the supported schema/checksum. An already-running incompatible role cannot make new durable claims/commits after an incompatible schema change. Transforms are immutable: a worker rejects a job with an unsupported transformation version before incrementing attempts. An unsupported job cannot silently change semantics; its one-hour recovery deadline remains bounded. This milestone contains only transactions-v1; rolling multi-version deployments have not been exercised.

Future upgrade order: stop new admission, drain active claims or allow their leases to expire, apply an additive compatible migration, start compatible coordinators/workers, then update API admission. Incompatible schema changes require explicit scope/rollout review before execution. Retain the existing owner files and selected versioned objects. Never reinterpret old outputs with a new transformation implementation.

Reference `serve` results remain ephemeral and are not migrated into fictional durable jobs. CLI `process`/`verify` output contracts remain compatible. No CI/CD pipeline or deployment automation is included.

## Schema 1 to 2

Drain/stop only the application roles being upgraded before running the explicit migration, then restart all three with the new binary and unchanged local configuration. Older binaries support only schema 1 and will reject schema 2 transitions; they must not remain active during migration. Do not destructively downgrade. This implementation was tested in a separate local database; the running workspace database was not migrated.

`migrate --profile local` applies schema 2 and adopts the owner's existing local namespace. Keep the same endpoint/bucket/queue/owner when adopting legacy local jobs, whose object references did not store a bucket. `migrate --profile aws` binds only a new owner without preexisting unbound jobs, or verifies an already matching binding. AWS defaults to different owner/password files. Neither direction can change an existing binding. Admission checks/creates the local binding atomically with quota and job admission under capacity → owner-binding → job lock order, so concurrent local admission cannot race a first cloud binding. Doctor and runtime checks are read-only; no automatic namespace switching is available.
