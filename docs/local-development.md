# Local durable development

Use the README quickstart from the project root. Tested here: macOS arm64, Go 1.27.1, Docker 28.5.1, Compose 2.40.2, PostgreSQL 18.0 Alpine and Moto 5.1.15. `compose.local.yaml` pins tested image digests. Linux/Windows and containerized role networking have not been validated.

Default PostgreSQL loopback port: 15439. Default Moto loopback port: 15019. Override `PROCESSOR_PG_PORT` / `PROCESSOR_MOTO_PORT` for Compose, and set corresponding `PROCESSOR_DATABASE_URL` / `PROCESSOR_LOCAL_ENDPOINT` for Go roles. Choose isolated ports; never kill unrelated listeners. PostgreSQL uses a named volume. The local Moto image does not provide durable object/queue state across emulator replacement: restarting only Go roles is supported; replacing/resetting Moto can break retained object references, which surface as integrity errors. This emulator limitation does not establish AWS persistence behavior.

Configuration is explicit nonsecret command flags, then environment, then local defaults. Secret values are never flags/URLs. Supported environment names:

| Name | Purpose |
|---|---|
| PROCESSOR_DATABASE_URL | Local PostgreSQL connection; never log its value |
| PROCESSOR_LOCAL_ENDPOINT | Loopback HTTP Moto endpoint; remote AWS fallback refused |
| PROCESSOR_BUCKET / PROCESSOR_QUEUE | Local resource names |
| PROCESSOR_OWNER_FILE | Private stable owner identifier file |
| PROCESSOR_PASSWORD_FILE | Private owner password file |

Defaults for the two files are `.scratch/durable-owner` and `.scratch/durable-password`. Do not regenerate them to restart roles. `local init` creates them only if missing and checks restrictive permissions; symbolic links are refused. It initializes schema/bucket/queues without resetting prior state. `migrate` can be run independently for explicit schema application. `doctor` alone stays zero-cloud; `doctor --profile local` checks the daemon and dependency capabilities safely.

Read-only diagnosis: `go run ./cmd/processor job inspect --job JOB_ID`. It reports only owned jobs and their selected chunks, attempts, lease times and safe error codes. PostgreSQL events record phase/claim/commit/publication transitions. No manual row editing is needed. Never print environment dumps, full connection settings, password files or CSV bodies while debugging.

This milestone intentionally retains objects, reservations and local test records. Expiry disables access, not storage. Cleanup/reclamation is not implemented; do not run volume removal or object deletion as a troubleshooting shortcut.

## AWS profile separation

Use docs/aws.md for `--profile aws`; never reuse emulator endpoint overrides or local owner identity for cloud jobs. Existing local binaries/database remain untouched by this milestone's AWS tests. The new migration is tested in a separate processor_aws_test database. Before upgrading an existing schema-1 workspace, follow docs/upgrades.md and restart its roles with the new binary. New code preserves legacy local schema-1 behavior, but AWS mode requires an explicit schema-2 binding.

The default integration script now creates/uses the dedicated processor_aws_test database through the local Compose service, leaving the running workspace database alone. Set PROCESSOR_TEST_DATABASE_URL explicitly for another isolated test database; never point it at live application data. Tests retain their synthetic rows/objects and do not purge volumes.
