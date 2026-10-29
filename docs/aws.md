# AWS backend: implemented, not deployed

The binary now supports an explicit `--profile aws` for existing AWS S3/SQS resources. It uses the AWS SDK credential chain and signed HTTPS requests. It does not create resources, attach permissions, configure compute, provision PostgreSQL, or expose the owner API publicly. No real AWS acceptance run has been performed.

The reviewable infrastructure source is `infra/aws/template.json`: a private, versioned, SSE-S3 encrypted bucket; encrypted standard SQS work/dead-letter queues; TLS-only resource policies; and separate API/coordinator/worker managed policies. It contains no IAM users/access keys, role trust policies, compute, database, release pipeline or credentials. Attach each policy to its intended existing workload identity after review. Bucket and queues are retained on replacement/deletion, and there are no expiration rules that could delete referenced versions. Retention also means storage is not reclaimed automatically.

## Required configuration

Use a dedicated owner and database for AWS. Values below are configuration names, not usable credentials or a deployed environment:

| Variable | Required value |
|---|---|
| PROCESSOR_AWS_REGION | Explicit commercial AWS region, such as eu-west-1 |
| PROCESSOR_AWS_ACCOUNT_ID | The 12-digit account that owns bucket/queues |
| PROCESSOR_BUCKET | General-purpose bucket name; use the template output |
| PROCESSOR_QUEUE_URL | HTTPS regional URL for the standard work queue, in the selected account |
| PROCESSOR_DATABASE_URL | Explicit PostgreSQL connection; remote hosts require sslmode=verify-full |
| PROCESSOR_OWNER_FILE | Optional private file path; AWS default .scratch/aws-owner |
| PROCESSOR_PASSWORD_FILE | Optional private file path; AWS default .scratch/aws-password |

`--profile aws` selects the application's backend. `AWS_PROFILE`, if used, selects an SDK credential profile. Prefer temporary workload role credentials or SSO; the application does not accept credential flags, write AWS keys, or log provider values. Supply database credentials through the operator's private environment/secret mechanism, never command-line arguments or committed files. Remote PostgreSQL needs a trusted CA; `PGSSLROOTCERT` can point to the provider's CA bundle. Loopback PostgreSQL remains supported for an explicitly scoped first AWS-service validation, but it is not an AWS database deployment.

AWS mode refuses `PROCESSOR_LOCAL_ENDPOINT`, all `AWS_ENDPOINT_URL*` overrides, nonregional/non-HTTPS queue URLs, mismatched queue accounts/regions, FIFO queues and special bucket ARN forms. It pins standard AWS service endpoints and sends ExpectedBucketOwner for bucket/object access. China, GovCloud, isolated partitions, custom endpoints, SSE-KMS and public ingress are outside this profile's supported scope.

## Operator sequence after resources and budget are approved

First review the template, account/region, workload policies, PostgreSQL connection, secret delivery and budget. None has been selected/provisioned by this implementation task. Once those resources exist and the required environment is set:

```sh
go run ./cmd/processor config init --profile aws
go run ./cmd/processor migrate --profile aws
go run ./cmd/processor doctor --profile aws
```

`config init` only creates mode-0600 owner/password files and makes no database/AWS request. `migrate` applies schema/checksums and binds the owner to account/region/bucket/queue in PostgreSQL; it makes no AWS request. Migration is a database write and must target the reviewed database. `doctor` reads the database/binding and existing AWS resource configuration; it does not upload objects or send messages. A passing doctor does not prove permission for every processing operation.

Doctor checks expected bucket owner and region, enabled versioning, all four public-access blocks, SSE-S3, bucket-owner-enforced ownership, and absence of enabled current/noncurrent version expiration. It checks queue identity, SSE-SQS, at least 90-second visibility, and a different encrypted dead-letter queue in the same account/region with a bounded redrive count. Failures are redacted. The preflight has a 45-second deadline; database connections have a 10-second timeout.

After preflight, start the normal roles in separate terminals:

```sh
go run ./cmd/processor coordinator --profile aws
go run ./cmd/processor worker --profile aws
go run ./cmd/processor api --profile aws --addr 127.0.0.1:8087
```

The API remains authenticated and loopback-only. It labels AWS mode separately from local emulation. Keep owner files stable across restarts and available only to the runtime identity. Container images now include a CA trust bundle; mounted private configuration must be readable by the non-root runtime user with restrictive permissions. Container orchestration/secret distribution and remote ingress are not supplied by this foundation.

## Real AWS acceptance still required

With explicit authorization for AWS writes, use the existing private UI to submit `testdata/transactions.csv`, retain the job URL, and download both output CSVs. Compare the complete bytes with `testdata/expected/clean.csv` and `rejected.csv`; counts must be 6/3/3. Record source/image identity, region, selected object versions and output hashes without credentials or raw data. Then perform a reviewed worker interruption/takeover experiment on a synthetic job and verify single publication/stale fencing. Local recovery evidence does not substitute for this run.

`local init`, `demo recovery`, shortened leases and worker checkpoint/debug flags explicitly refuse AWS mode. The local demo is not silently converted into a cloud workload. No object deletion/queue purge is performed; retained objects and reservations remain accounted. Automatic cleanup, larger direct uploads, RDS/compute/Kubernetes deployment, monitoring/budget enforcement and external publication remain separate work.

## References

- AWS SDK Go endpoint resolution: https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-endpoints.html
- Bucket region discovery via HeadBucket: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
- SQS-managed encryption: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sqs-sse-queue.html
