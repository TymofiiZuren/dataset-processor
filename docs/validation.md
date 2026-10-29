# AWS connection milestone validation — 2026-10-25

Primary AWS signal **partially validated**: explicit target/credential/endpoint integration and resource contracts are implemented and exercised offline; no real AWS processing/recovery acceptance run has occurred. All live service integration in this milestone used local PostgreSQL/Moto. No AWS service request, resource creation, IAM policy attachment, remote database migration or external publication was executed.

Checks:

- `./scripts/test-integration.sh` — passed with race detection across CLI, infrastructure, processor, durable state and web packages. The script now owns the isolated local processor_aws_test database; the running workspace schema/data was not migrated.
- AWS SDK transport tests — passed: signed HTTPS object writes, exact-version reads, expected bucket owner, refusal of local endpoint overrides, account/region/standard-queue validation, redacted errors, and read-only preflight. Valid resource responses pass; wrong region, suspended versioning, public access, missing encryption, expiring versions, wrong queue account, missing DLQ and unencrypted DLQ fail. These are mock HTTP contracts, not AWS service evidence.
- PostgreSQL tests — passed: immutable backend identity, both switch directions rejected, legacy unbound jobs cannot become cloud jobs, runtime IO blocked before binding, idempotent migration/binding, and eight concurrent admission-versus-cloud-binding races with exactly one namespace winning. Admission and binding share the owner lock inside the quota/job transaction.
- CLI offline smoke — `config init --profile aws` and `migrate --profile aws` passed using synthetic resource configuration and the isolated **local** database. No AWS client operation was invoked. Local init/demo/debug flags refuse AWS profile; cloud mode never falls back to the emulator.
- `go vet ./...` and `go build -o .scratch/processor-aws ./cmd/processor` — passed.
- `cfn-lint 1.58.0 infra/aws/template.json` — passed without diagnostics. The validator was installed only in a temporary .scratch virtual environment after finding no installed cfn-lint; no production dependency was added. Additional Go tests check retained/private/encrypted resources, scoped role capabilities, known references and absence of dependency cycles. CloudFormation service-side validation/deployment has not occurred.
- `docker build -t dataset-processor:aws-ready .` and `docker run --rm dataset-processor:aws-ready doctor` — passed. The scratch image now copies a system CA bundle from the builder. Real AWS/RDS TLS handshakes remain untested.
- New binary's local `demo recovery`, pointed at the isolated database — passed actual worker process interruption/takeover, multiple chunks, stale success/failure rejection and independent output equality. Evidence: `.scratch/recovery-2329245559/evidence.json`.
- Mode-label tests confirm AWS mode does not claim local/no-cloud behavior, while the local workspace retains its explicit emulation label. Owner auth, strict Host/Origin and CSRF tests remain green.

Meaningful implementation finding resolved: a separate read-only backend check before admission was insufficient against a concurrent first binding. Namespace verification/creation now happens in the same admission transaction under capacity → owner binding → job ordering, with concurrency tests. Migration 1 source/checksum is unchanged; migration 2 is additive. New code supports schema 1 only for legacy local use and schema 2 for bound profiles; AWS requires schema 2. Older binaries must be drained before explicit migration.

Remaining acceptance: account/region/budget and workload identity selection; approved provisioning/role attachment; a reviewed PostgreSQL environment; real AWS permission, service, TLS, fixture and recovery tests; compute/Kubernetes/ingress/secret distribution; monitoring, budget enforcement and coordinated retention cleanup. See docs/aws.md and infra/aws/template.json. The existing local workspace and processes were left running.

---

## Previous local durable validation — 2026-10-25

Primary signal **met for the bounded local durable slice**: an actual interrupted worker process leaves a persisted claim; a replacement executes multiple chunks in reverse order; stale success and failure are rejected; both downloaded output files exactly match independently authored fixture bytes. Same job and input version remain authoritative. This does not establish AWS or Kubernetes behavior.

Checks completed against real local PostgreSQL 18.0 and Moto 5.1.15:

- `./scripts/test-integration.sh` (`go test -race ./... -count=1` with explicit local service endpoints) — passed. Covers record semantics, output budgets, real database lock barriers for both cancel/publish orderings, lease takeover/stale error fencing, idempotency/admission limits, unselected preparation isolation, ambiguous input recovery, S3 version pinning/corrupt references, actual SQS redelivery after durable commit before acknowledgement, schema checksum compatibility, session/job persistence, download expiry and missing committed object HTTP behavior.
- `PROCESSOR_TEST_DATABASE_URL=<local connection> PROCESSOR_TEST_ENDPOINT=<local endpoint> go test -race ./internal/durable ./internal/web -count=2` — passed after correcting the asynchronous test assumption described below. No credentials printed.
- `go test ./...` — zero-cloud tests passed; service-dependent cases intentionally skip unless explicit integration configuration is present. This command alone is not durable acceptance.
- `go vet ./...` and `go build -o .scratch/processor-durable-current ./cmd/processor` — passed.
- `go test ./internal/processor -run '^$' -fuzz FuzzProcess -fuzztime=3s` — passed, 49,965 observed executions after baseline collection.
- `.scratch/processor-durable-current demo recovery` — passed with a real scenario-owned subprocess interruption and replacement; uniquely named evidence reports live under `.scratch/recovery-*`.
- `docker build -t dataset-processor:local .` and `docker run --rm dataset-processor:local doctor` — passed. Non-root scratch-image binary smoke tested; native role networking is the validated durable path.
- Real Chromium browser: synthetic upload, SQS-backed completion at 6/3/3, byte-exact clean **and rejected** downloads, stable URL after API process restart, reauthentication return, unauthenticated download 401, 320px mobile no overflow, zero page errors — passed. Desktop/mobile screenshots `.scratch/durable-desktop.png`, `.scratch/durable-mobile.png` were visually inspected for the retained design.
- Browser cancellation on 100,000 synthetic records — passed; terminal cancelled state and download HTTP 409, zero page errors. Only the synthetic fixture/derived counts appeared in screenshots.

Test-first implementation: new processor-part, store, pipeline and durable-session tests first failed on missing symbols, then passed after implementation. Additional adversarial cases extended the same owning boundaries. Two meaningful failures were found and resolved:

1. Initial persistence omitted internal lease fields through JSON exclusions. Real PostgreSQL tests immediately rejected subsequent staging as stale. Lease/token/preparation/reservation fields now persist; owner authentication data stays outside public serialization.
2. A later web integration test expected preparation to run in one coordinator tick while another test package held the globally bounded preparation slot. That is valid backpressure, not synchronous admission. Fixtures and the recovery scenario now wait for authoritative state with a deadline; repeated concurrent package race checks passed. No limiter or durable invariant was weakened.

Current API/coordinator/worker were started locally. Only the three verified processes created during this build were gracefully restarted to load updated code; no unrelated listener was stopped. Existing reference workspace remains separate. Docker/Moto/PostgreSQL services and retained test objects remain available; no purge occurred.

Limits and missing coverage: no actual AWS services/permissions, Kubernetes, mixed-version deployment, automatic retention/reclamation or cleanup read leases, containerized durable-role networking, full-machine/Moto replacement persistence, throughput/resident-memory capacity benchmark, or full screen-reader/keyboard audit. Initial API upload is an 8 MiB buffer; prep attempts restart rather than resume a cursor. Global retained storage is quota-accounted and can fill. These are explicit boundaries, not hidden success claims. No Git repository exists; no Git mutations or external publication occurred.

---

## Historical reference milestone — 2026-10-24

Primary signal **met for the local reference milestone**: synthetic sample yields 6 source records, 3 accepted, 3 rejected. CLI verification and browser-downloaded clean output match independently authored expected bytes. Both clean and rejected CLI output hashes match expectations.

Checks completed:

- `go test ./...` — passed, processor, HTTP integration and CLI contracts.
- `go test -race ./...` — passed.
- `go vet ./...` — passed.
- `go build -o .scratch/processor-v2 ./cmd/processor` — passed.
- `go run ./cmd/processor verify --input testdata/transactions.csv --expected testdata/expected --output .scratch/verified-sample` — passed.
- `go test ./internal/processor -run '^$' -fuzz FuzzProcess -fuzztime=3s` — passed, 94,429 executions in the observed run.
- Browser using existing Playwright runtime: login, sample upload, result counts, exact clean download, 1440px desktop and 390px mobile captures, no page overflow or JavaScript errors, logout — passed after fixing the Origin issue below.
- Statement coverage snapshot: processor 91.4%; web 70.7%. These are secondary signals, not proof of correctness or complete branch coverage.

Test-first sequence: processor and web tests failed before their implementations; login rate-limit test failed before adding the limiter; same-origin policy regression test failed before the policy correction. CLI mismatch and overwrite tests assert no false successful publication.

Failures encountered and resolved:

1. Initial test command ran in the parent Applications directory, which is not a Go module. Corrected working directory to dataset-processor; the expected missing-implementation failures then appeared.
2. Impeccable context launcher was not executable. Read approved design and local skill references directly; no setup or system permission changes performed.
3. First launch preceded build completion. Waited for build completion, then launched the binary on a free port.
4. Browser login returned 403 because Referrer-Policy no-referrer caused Origin:null on same-site form submission. Confirmed the browser request header, added the regression check, and changed the owning response policy to same-origin. Host/Origin checks and CSRF were retained. Browser validation then passed.

Evidence screenshots are in `.scratch/desktop.png` and `.scratch/mobile.png`; they contain only synthetic sample counts and public contract text. Password/session values were never included in console output, screenshots, or artifacts.

Not validated or implemented: distributed correctness, PostgreSQL locking, AWS permissions or services, durable jobs, background recovery/cancellation, Kubernetes, performance capacity, 1 GiB processing benchmark, production hosting. Web upload is limited to 8 MiB for this reference milestone. Runtime is loopback-only; no cloud spending, Git mutations, or external publishing occurred.

### Historical independent review
A fresh reviewer inspected the source, tests, approved design, and desktop/mobile screenshots and independently reran tests, race checks, vet, and build. Disposition: ship this local milestone; no material blocker found. Browser interactions were verified by the primary agent, not repeated by the reviewer. Remaining coverage includes session/result expiry, logout during processing, concurrent requests, writer failures, cross-filesystem publication, and full keyboard/screen-reader behavior. These do not establish distributed or production readiness.
