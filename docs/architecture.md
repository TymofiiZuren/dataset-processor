# Reference and durable architecture

```text
Reference CLI / serve -> shared bounded CSV reader -> transform -> canonical CSV

Owner browser -> api -> PostgreSQL upload intent / owner + idempotency
                    -> version-pinned input in local S3 emulation
                           |
                 coordinator discovers preparing jobs
                           |
        original record-safe chunks + fenced staging/outbox transactions
                           |
             select complete preparation -> SQS wake-up hints
                           |
              workers claim chunks under the job-row lock
                           |
            exact-version input -> shared transform -> private attempt outputs
                           |
             fenced commit -> coordinator ordered integrity checks
                           |
      job-row-serialized success OR cancellation -> owner-authorized downloads
```

One Go binary/image has `api`, `coordinator`, `worker` modes. `serve/process/verify` retain reference semantics. `processor.scan` owns bounded encoded framing, UTF-8, header and four-field validation; preparation preserves original encoded records without headers. Workers use the same transformer/serializer with global ordinal offsets. Outputs are headerless canonical parts; finalization and download add one header and read selected parts in source order. Header-only input succeeds with zero chunks. All preparation must succeed before any staged chunk is dispatchable.

PostgreSQL tables: schema/checksum, capacity reservations, owned/idempotent jobs, generation-specific chunks, transactional outbox, and revisioned events. Job/chunk documents hold explicit lease tokens, attempt counts, selected versions and immutable transformation version. Capacity is locked before job rows; transitions then update chunk/outbox/events. Every mutation checks compatible schema inside its transaction. Lease validity uses `clock_timestamp()` after the row lock, not transaction-start time. Both successful and failed stale reports are fenced. Duplicate delivery cannot spend another attempt while a live claim exists. Cancel/publication share the job lock; terminal state cannot be overwritten.

Outbox sends happen after commit, with sent bookkeeping after successful send. Unfinished selected chunks receive another hint after 30 seconds, including abandoned leases. Queue receive counts and DLQ contents are not application authority. A committed chunk can be redelivered and acknowledged without acceptance twice. PostgreSQL remains discoverable after API/coordinator/worker restart. Expired preparation restarts the bounded input with a new token; staged objects from older attempts remain private and counted, rather than resuming a per-batch cursor.

Inputs and outputs pin bucket-configured key + non-null version ID + byte count + SHA-256. Admission persists the unique upload key before object IO. Interrupted ambiguous uploads reconcile from that key and expected input identity. Each attempted write reserves bytes first, including duplicates/orphans. Successful objects are verified before commit, ordered objects are checked before final publication, and downloads preflight exact selected versions before headers, then stream them again. A midstream failure aborts the HTTP response. No aggregate result object is stored. No cleanup races exist in the shipped runtime because it performs no deletion; coordinated read leases are required before adding cleanup.

The first durable API buffers at most 8 MiB per upload before storing it; this is a documented bounded implementation of the initial API relay. Each worker reads a bounded part and shares a 64 MiB output budget between accepted/rejected buffers. Admission/storage limits reject amplification or retained-store saturation. They are not throughput or resident-memory benchmarks. Each runtime pool is bounded to four PostgreSQL connections, with 3-second lock/10-second statement limits. Downloads have two slots per API process. Status and cancellation do not use the upload/download slots.

Security: exact loopback Host/Origin, owner password, session expiry, constant-time comparison, CSRF, five-login-attempt/minute limiter, HttpOnly SameSiteStrict cookies remain. Durable owner ID/password persist in mode-0600 files; logout clears access, not jobs. The local profile requires loopback SDK/database endpoints; emulator-only credentials prevent AWS fallback. The explicit AWS profile uses the SDK credential chain, pinned HTTPS service endpoints and account/region checks; remote PostgreSQL requires verified TLS. No credentials, CSV contents or signed URLs are logged. Local PostgreSQL/Moto are private development dependencies; no production deployment, AWS identities or cloud resources are claimed. The explicit cluster profile uses fixed service DNS, verified PostgreSQL TLS and local Moto emulation; see kubernetes.md for pod recovery evidence and its limits.

## Explicit AWS boundary

`NewAWSObjects` selects standard commercial AWS endpoints and refuses emulator overrides. Exact-version object paths, outbox/chunk semantics and job-row fencing are shared with local processing. Cloud init cannot create resources; configuration checks are read-only. Per-owner backend bindings persist profile/region/account/bucket/queue identity. Runtime checks and atomic admission reject profile switching; legacy local jobs cannot be adopted into AWS. Migration 2 adds only the binding table, with migration 1 checksum unchanged.

A declarative CloudFormation source supplies private versioned/encrypted storage, encrypted standard work/DLQ queues, TLS-only policies, and separate scoped role capabilities. It creates no compute, database, credentials, role trust or pipeline. The image carries system CA certificates for AWS HTTPS. See aws.md for operator configuration and limitations. Signed SDK/mock contracts plus local Moto/PostgreSQL tests are code evidence, not real AWS service validation.

## Owner job history

The authenticated home page queries up to 20 of the current owner's jobs, ordered by creation time descending with ID as a deterministic tie-breaker. It applies the same database-clock metadata-expiry predicate as direct job reads. A dedicated summary type exposes only job identity, state, timestamps and committed counts; lease/fencing values, storage references and CSV contents are not rendered. No new schema or retention policy is introduced. Database failure returns HTTP 503 with a retry link rather than pretending the history is empty. Direct job links remain independently usable.

## Job activity timeline

Owner-authorized job pages and status polling include up to 50 persisted `processor_events`, shown chronologically. The query rechecks job ownership and metadata expiry and excludes events beyond the job revision accompanying the response. A truncation notice identifies omitted earlier transitions. No new audit events or schema changes are introduced; this is a read-only projection of the existing state-transition trail, not a per-chunk trace or a complete infrastructure audit.

The UI maps known event codes to plain language and uses a generic label for unknown internal diagnostics. Polling builds text nodes, retains the last confirmed view on request failure, and updates the timeline alongside the returned progress snapshot. No event details include lease tokens, object references, credentials, or CSV contents. A new job can correctly have no events until its first persisted state change. Initial activity-query failure is presented explicitly with HTTP 503 and a retry link rather than a false empty timeline.

## Owner operations overview

The authenticated home page includes a read-only PostgreSQL snapshot of all the owner's jobs whose metadata remains available, independent of the recent-history limit. One SQL statement uses `statement_timestamp()` for the observation time, metadata expiry, and lease expiry. Counts distinguish active, succeeded, failed and cancelled jobs. Running chunk leases are counted only for processing jobs and their currently selected preparation; obsolete preparations and terminal jobs cannot inflate recovery indicators.

The page displays the observation time and a manual refresh link. Expired chunk leases indicate persisted lease state, not proof that a pod has failed or that recovery will succeed: deadlines, attempt limits and dispatch still apply. No pod health, queue depth, capacity, or physical storage metric is inferred. Database errors return HTTP 503 with explicit unavailable text rather than zero counts. The overview exposes no job contents, credentials, lease tokens or object keys; it introduces no writes, dependencies or schema changes.
