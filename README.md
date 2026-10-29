# Dataset Processor

A Go portfolio application for deterministic transaction CSV processing. It now has a streaming reference processor and a separate **local durable runtime**: PostgreSQL jobs/leases/fences, version-pinned S3-compatible objects, SQS-compatible dispatch, persistent owner job pages, and cancellation. Local integration uses Moto; this is not validation against AWS. An explicit real-AWS backend is now implemented, with a reviewable infrastructure template; it has not been deployed or validated against AWS.

## Real AWS backend

Use `--profile aws` only with explicit account/region/bucket/queue/database configuration and workload credentials. See the [AWS runbook](docs/aws.md) and [CloudFormation foundation](infra/aws/template.json). AWS mode checks existing resources, pins expected ownership, and cannot run local provisioning/recovery helpers. Each owner is bound to its storage namespace to prevent mixing local and AWS jobs. No AWS resources or cloud acceptance run were created during implementation.

## Local Kubernetes and pod recovery

Run the three Go roles in a dedicated kind cluster and prove worker lease takeover after pod replacement:

```sh
python3 scripts/kubernetes.py up
python3 scripts/kubernetes.py forward
# In another terminal:
python3 scripts/kubernetes.py recovery
```

Open `http://127.0.0.1:8089`; the private password file is `.scratch/k8s/private/password`. See the [Kubernetes runbook](docs/kubernetes.md) for prerequisites, isolation, evidence, and limits. The recovery proof also replaces the API and coordinator, then verifies persisted job state and identical downloads. PostgreSQL and Moto stay running; emulator/whole-cluster restart durability is not claimed.

## Run durable processing locally

Requires Go 1.27+, Docker with its daemon running, and Docker Compose. No AWS account or credentials. The pinned local PostgreSQL profile uses trust authentication **only on a loopback port**; Moto is a development emulator. Keep both private. This is not a production deployment profile.

```sh
docker compose -f compose.local.yaml up -d
go run ./cmd/processor local init
go run ./cmd/processor doctor --profile local
```

`local init` applies the transactional schema and idempotently initializes a versioned bucket, queue/DLQ and private owner configuration. Repeating it preserves jobs, volumes, ownership and password. It prints only the password file path; open the file locally to sign in. Do not paste its contents into chat, screenshots or logs.

Run these in three separate terminals from the project root:

```sh
go run ./cmd/processor coordinator
```

```sh
go run ./cmd/processor worker
```

```sh
go run ./cmd/processor api --addr 127.0.0.1:8087
```

Choose another free port if needed; no process needs to be stopped to free one. The API prints its workspace URL. Upload the synthetic sample, retain `/jobs/{id}`, and download results after completion. Browser disconnect, logout, or role restart does not cancel a durable job. Sessions remain ephemeral; sign in again to find your 20 most recent jobs on the home page, or reopen a saved job link. History excludes jobs whose seven-day metadata access has expired. The home page also shows an owner-scoped operations snapshot: job outcomes, live chunk leases and expired chunk leases, with an observation time and refresh link. Each job page includes the latest 50 persisted state changes, with live updates and an explicit truncation notice. Explicit cancellation serializes with publication: the first committed terminal transition wins.

Limits: 8 MiB input; 64 KiB encoded record; two unfinished jobs per owner; one active preparation and four active chunk claims globally; 1-hour recovery deadline, five execution claims per chunk/stage. Ordinary duplicate messages do not spend an execution attempt. Default chunks target 16 MiB; the bounded first workspace therefore normally uses one chunk. Recovery tests force smaller chunks. API uploads use a bounded in-memory buffer before versioned storage; direct browser multipart remains later work. Worker output buffers have a shared 64 MiB ceiling; large rejection amplification can fail with `CAPACITY_EXHAUSTED` rather than exceed the budget. These are configured limits, not measured capacity claims.

Successful download access lasts 24 hours; metadata remains accessible seven days. Expiry does not delete objects. All input reservations and attempted writes remain quota-accounted, including orphans: global input 2 GiB, global derived 8 GiB, per-job derived 64 MiB (including upload attempts). There is **no automatic deletion or quota reclamation**; a full retained store refuses more work. Never remove active input or selected output versions manually. Cleanup with read-lease coordination is deferred.

## Reproduce worker recovery

```sh
go run ./cmd/processor demo recovery
```

The experiment uses a separate owner and queue, forces multiple chunks, launches a real worker subprocess, interrupts only that worker after a persisted claim and before commit, waits for its lease to expire, and launches a replacement. It completes chunks in reverse order, rejects stale success/failure reports, and compares both downloaded outputs with independently authored expectations. A failed checkpoint, missing replacement attempt, or output mismatch exits nonzero. Evidence and synthetic outputs are saved in a new `.scratch/recovery-*` directory. It leaves retained test data quota-accounted; it never purges existing state.

```sh
./scripts/test-integration.sh
go vet ./...
go build -o .scratch/processor ./cmd/processor
```

The explicit integration script creates/uses the isolated processor_aws_test database in the local Compose service; it never defaults to migrating the running workspace database. It requires real local PostgreSQL and Moto and fails when unavailable. `go test ./...` without integration variables runs zero-cloud tests and skips service-dependent cases. See [local setup](docs/local-development.md), [recovery](docs/recovery.md), [architecture](docs/architecture.md), [errors](docs/errors.md), [upgrades](docs/upgrades.md), and [validation](docs/validation.md).

## Container image

One image contains `api`, `coordinator`, `worker`, and reference utility modes. The final image is a non-root static binary without a shell.

```sh
docker build -t dataset-processor:local .
docker run --rm dataset-processor:local doctor
```

The local durable profile requires loopback endpoints and private configuration files. The image also includes a CA trust bundle for the explicit AWS profile. Native Go roles are the tested local setup; container-network service wiring and Kubernetes manifests are later work. Do not substitute public binds or cloud credentials into the local emulator profile to make a container connect.

## Run the reference web app

Requires Go 1.27 or newer. The reference path needs no external services or cloud credentials. The module also includes the approved pgx/AWS SDK dependencies for durable modes.

```sh
go run ./cmd/processor serve
```

Open the loopback URL printed at startup. Sign in using the private password file whose **path**, not contents, is printed. Open that file locally; do not paste its contents into chat, screenshots, source control, or logs. Each server start generates a fresh password unless `PROCESSOR_OWNER_PASSWORD` is supplied through your environment. Do not place credentials in command-line flags or URLs.

The app chooses a free port. For a stable port, use `serve --addr 127.0.0.1:8087` if available. Public binds are refused. Local HTTP is restricted to loopback: do not expose this milestone through a public reverse proxy.

1. Download the synthetic sample from the workspace.
2. Choose that CSV and select **Validate and process**.
3. Inspect counts, download cleaned/rejected records, and inspect the SHA-256 manifest.

Web uploads are limited to 8 MiB, with one processing request at a time. This smaller reference-workspace limit is deliberate; the streaming CLI permits 1 GiB. A result is held in memory, replaced by the next successful result, expires for download after 15 minutes, and is lost on restart. There is no durable job history or independent verification of arbitrary uploaded datasets. Download hashes identify bytes; they do not prove business correctness.

## Verify the known sample

```sh
go run ./cmd/processor verify \
  --input testdata/transactions.csv \
  --expected testdata/expected \
  --output .scratch/verified-run
```

Expected: **6 records, 3 accepted, 3 rejected**, and `Verification: PASS`. The accepted/rejected fixture files were authored independently. Verification compares both full output file hashes, not counts alone. Choose a new output directory for every run: existing paths are never overwritten.

To process another file without an expected-result comparison:

```sh
go run ./cmd/processor process --input /path/to/input.csv --output .scratch/new-run
```

Outputs: `clean.csv`, `rejected.csv`, and `manifest.json`. The manifest is the completion marker and is published last. Failed processing or expected-output mismatch publishes no result directory. A filesystem failure during publication can leave an incomplete directory; do not use it without its completion manifest. Private temporary staging remains under `.scratch/`; cleanup is manual. No automatic file deletion occurs.

## Dataset contract

Exact UTF-8 CSV header:

```csv
transaction_id,occurred_at,currency,amount
```

- ID: 1–128 Unicode code points; no control characters. Duplicate IDs remain separate source records.
- Timestamp: explicit RFC3339 timezone, seconds required, up to nine fractional digits. Normalized to UTC.
- Currency: case-sensitive `EUR`, `GBP`, `USD`.
- Amount: optional minus, digits, optional dot and 1–2 fractional digits. No plus sign, exponent, whitespace, rounding, or floating point. Checked signed 64-bit integer minor-unit range. Negative zero becomes `0.00`.
- No implicit trimming, BOM, empty physical records, malformed quotes, or wrong field counts. Header-only succeeds with zero records; an empty file fails.
- Quoted multiline fields are parsed correctly before field validation; a newline in an ID is a control-character rejection.
- Maximum encoded record: 64 KiB **including delimiters and record terminator**. The lexical framing guard runs before CSV field allocation.

Clean output includes source ordinal and normalized fields. Rejected output includes ordinal, original parsed fields, and stable reason codes in column order. Source ordinals count data records, excluding the header, rather than physical lines. Both outputs preserve source order within their category.

Machine-readable rejection data preserves input values. Inspect untrusted CSV as text: spreadsheet programs can interpret cells as formulas.

## Validate

```sh
go test ./...
go test -race ./...
go vet ./...
go build -o .scratch/processor ./cmd/processor
go test ./internal/processor -run '^$' -fuzz FuzzProcess -fuzztime=3s
```

HTTP integration tests cover owner login, untrusted origins, CSRF, rate limits, sample upload, downloads, and malformed-file publication. Processor tests cover independently known outputs, decimal boundaries, quoted records, file/record limits, and malformed input. CLI tests cover expected-output mismatch, overwrite refusal, and public-bind rejection.

## Next milestones

Direct browser multipart uploads and measured larger workloads; coordinated retention cleanup; local Kubernetes and recovery recordings; separately authorized AWS provisioning/service validation; public portfolio publication after approval. No CI/CD or hosted automation was added.

See [implementation status](docs/status.md) for implemented guarantees and remaining coverage. No Git repository was initialized, and no files were staged, committed, pushed or deleted.
