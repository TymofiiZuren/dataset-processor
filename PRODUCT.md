# Dataset Processor

Purpose: engineering portfolio showing deterministic transaction processing and bounded local distributed recovery.
Audience: private owner operating jobs; engineers inspecting source, independent outputs and reproducible failure evidence.
Current truth: reference CLI/workspace plus a separate local durable API/coordinator/worker runtime using real PostgreSQL and versioned S3/SQS emulation through Moto. An explicit AWS backend and infrastructure foundation are implemented but not deployed or live-validated. Local Kubernetes role replacement is validated with a dedicated kind cluster and pod-recovery evidence; real AWS behavior remains unvalidated. The authenticated workspace lists the owner's 20 most recent unexpired jobs.
Approved direction retained: Go; PostgreSQL; private S3; SQS; one binary/image; Docker/Kubernetes; public recorded evidence with private live access; low ongoing cost.
Primary signal: a forced multi-chunk fixture survives a worker process interruption and replacement, rejects stale commits/failures, and produces byte-exact independently expected downloads. Owner job URLs survive sessions/API restarts; explicit cancellation serializes with final publication.
