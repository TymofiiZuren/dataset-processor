# Implementation status — 2026-10-29

Complete: retained deterministic streaming reference and independent expectations; approved pgx/AWS SDK dependencies; native Go API/coordinator/worker roles in one non-root image; local PostgreSQL/Moto profile; persistent owned/idempotent jobs; record-safe preparation; selected-generation outbox; exact object versions; lease/attempt fencing for success/failure; shared-lock cancel/finalize; ordered integrity publication/download; stable owner job pages; persistent private owner config; local preflight, migration and read-only diagnostics; real worker-process recovery experiment.

Primary signal met for the bounded local recovery slice: multi-chunk replacement processing produces the independent expected clean/rejected bytes and stale success/failure cannot publish. Both cancellation/publication lock orderings are tested with real PostgreSQL barriers. Normal browser processing uses local SQS and matches both independent expected outputs. See validation for exact checks and limitations.

Implementation refinements: 8 MiB upload buffering before API-to-object relay; expired preparation retries the full bounded input with a fresh token rather than a saved per-batch cursor; two-pass verified download serving; no automatic cleanup/read leases because no deletion is performed. Download expiry and seven-day metadata access do not physically remove data or release quotas. Larger/direct uploads and coordinated cleanup remain later work.

Not implemented/validated: AWS provisioning/permissions/service semantics, larger-file browser uploads, performance/memory capacity benchmarks, full object-emulator replacement recovery, mixed-version rolling upgrades, full assistive-technology audit, public publication. No CI/CD or hosted automation added. Local Git history was subsequently reconstructed at the owner's request; see ../HISTORY.md. No push or file deletion was performed.

## AWS connection milestone — 2026-10-25

Implemented: explicit AWS profile, SDK workload credential chain, standard signed HTTPS service calls, expected bucket owner, region/account/queue validation, read-only resource preflight, verified remote PostgreSQL TLS, schema-2 backend bindings with atomic admission, distinct AWS UI/CLI mode labels, and a container CA bundle. Added a declarative S3/SQS/DLQ plus per-role IAM policy foundation; it contains no compute/database/credentials or pipeline.

Validated: mocked AWS HTTP contracts and unsafe-target/resource rejection, real PostgreSQL namespace/concurrency contracts, local Moto regressions, race tests, vet/build, and cfn-lint. The isolated test database has schema 2; the running local workspace remains on its existing schema. No live AWS request, resource creation, IAM attachment, migration to remote PostgreSQL, budget setup, workload deployment or real AWS recovery experiment was executed. AWS runtime acceptance remains partially validated until the reviewed environment exists and passes end-to-end tests. See docs/aws.md.

## Local Kubernetes and owner history

Implemented and validated: a dedicated kind workspace, private bootstrap identity and PostgreSQL TLS, separate restricted role pods, loopback-only API port-forward, worker abandoned-lease recovery after pod replacement, API/coordinator replacement with persisted state and exact downloads. PostgreSQL and Moto remain running during this proof; whole-cluster/emulator replacement durability is not claimed. See kubernetes.md.

The authenticated workspace now lists up to 20 recent jobs for the current owner, excludes expired metadata, and distinguishes no jobs from unavailable history. Links open the existing owner-authorized job pages. This changes discoverability only; retention, downloads, cancellation and processing contracts remain unchanged.

## Job activity timeline — 2026-10-29

Added a read-only timeline to owner job pages and status polling, using existing persisted state-transition events. It excludes expired/foreign jobs, shows the latest 50 transitions in chronological order, labels truncation, and caps events at the displayed job revision. Empty and unavailable activity are distinct. Friendly labels omit internal diagnostic details; processing, fencing, retention and schema remain unchanged.

## Owner operations overview — 2026-10-29

Added a private, read-only home-page snapshot of visible job outcomes and selected-preparation running chunk leases, split by live/expired. Owner boundaries, metadata expiry and obsolete/terminal chunk exclusion are tested against PostgreSQL. The UI identifies the observation time, manual refresh, and the distinction between lease state and runtime health. Failures display unavailable state rather than zero metrics. No pod health, queue depth, storage usage, new schema, or hosted monitoring is claimed.
