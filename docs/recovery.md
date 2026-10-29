# Reproducible recovery proof

Start local services and run `local init`, then `go run ./cmd/processor demo recovery` from the project root. Normal workspace roles may keep running: the scenario has a separate owner and queue. No cloud service is contacted.

The executable persists a fixture job, prepares multiple original-record chunks, starts a worker subprocess and records its durable claim before processing/commit. It interrupts only that scenario-owned worker, leaves its lease abandoned, waits past database-recorded lease expiry, then starts a replacement executable. Replacement chunks finish in descending index order. The scenario asserts one replacement attempt, rejects both stale success and stale failure, finalizes the same job/input version and downloads clean/rejected files. Both files must exactly equal the independently authored expectations.

A new `.scratch/recovery-*` directory contains private owner/checkpoint files, synthetic output CSVs and `evidence.json`. The report includes job/input identity, source checksum, chunk count, replacement attempts, checkpoint/interruption/command descriptions, expected-byte comparison and final manifest. Private password/owner file values are never part of the report. It exits nonzero if any asserted recovery signal is missing; a restart after completion is insufficient.

`./scripts/test-integration.sh` adds real PostgreSQL lock barriers for both cancellation/publication orderings, stage/worker stale fences, malformed preparation isolation, ambiguous upload reconciliation, version replacement/corrupt references, actual SQS receive/redelivery after commit-before-ack, output budgets, schema compatibility and authenticated job/session/expiry behavior. Services unavailable means failed validation, not a green mock result.

This is a bounded local proof. It does not establish AWS service/permission behavior, Kubernetes failover, full-machine/object-emulator restart persistence, performance capacity, or mixed-version rollout. Moto replacement can discard emulator objects/queues; job integrity errors deliberately expose missing references rather than invent successful data.
