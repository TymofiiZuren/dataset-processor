# Local Kubernetes workspace

This milestone runs the existing Go API, coordinator, and worker as separate non-root pods in a dedicated kind cluster. PostgreSQL remains the job, lease, fencing, and publication authority. Moto supplies development-only S3/SQS emulation. This is local Kubernetes acceptance evidence, not a real AWS deployment or production capacity claim.

## Start

From the repository root, with Go 1.27+, Python 3, Docker, and kubectl installed:

```sh
python3 scripts/kubernetes.py up
python3 scripts/kubernetes.py forward
```

Open `http://127.0.0.1:8089`. The password remains in `.scratch/k8s/private/password`; open it locally and never paste it into logs, chat, or screenshots. If port 8089 is occupied, choose a free port with `up --port PORT`; the pod bind port and forwarded port must match because the API validates the exact Host and Origin. No process is stopped to free a port. The forwarding helper reconnects when the API pod is replaced; sign in again after API restart because sessions are ephemeral.

The setup installs a checksum-verified kind v0.33.0 development binary under `.scratch/tools/kind` on macOS arm64. Other platforms must place their matching kind v0.33.0 binary there. It creates only `dataset-processor-demo`, uses `.scratch/k8s/kubeconfig`, explicitly selects `kind-dataset-processor-demo` for every operation, and refuses a kubeconfig containing other contexts. It does not change the default kubeconfig. Dependencies use pinned image digests; the app uses a local content-derived tag loaded into kind.

The owner API binds loopback inside its pod. There is no API Service, ingress, NodePort, or public bind; kubectl port-forward provides local access. The existing password, session, exact Host/Origin, CSRF, rate limit, and owner checks still apply.

## Prove pod recovery

```sh
python3 scripts/kubernetes.py recovery
```

Run without another active upload: the demonstration temporarily pauses the dedicated worker and changes the dedicated coordinator's chunk target. It submits the synthetic six-record fixture through the same runtime as the UI, forces multiple chunks, starts a worker that claims chunk zero and pauses, then replaces that pod with the normal worker. After lease expiry the new worker must complete attempt two for that chunk. The proof checks:

- Different worker pod UIDs with the same persisted input version/hash.
- Six records, three accepted, three rejected, and every output chunk selected.
- Exact clean/rejected CSV bytes against independently authored fixtures.
- Coordinator and API pod replacement preserves the completed job and all three downloaded artifact hashes.

A passing run writes `.scratch/k8s/recovery-*/evidence.json` only after every assertion succeeds. Its summaries exclude fencing tokens, owner credentials, and input contents. Downloaded synthetic artifacts are retained beside it. Failures exit nonzero and do not write a passing report. Normal worker/coordinator arguments and one worker replica are restored in a finally block. The demonstration never deletes files or issues kubectl delete.

## Identity, storage, and boundaries

`processor cluster init --output .scratch/k8s/private` creates private regular files: owner and login identity, PostgreSQL password and pgpass, and an ECDSA CA/server certificate. Completed reruns preserve identity; incomplete bundles fail. Immutable Kubernetes Secrets are created without printing their values and are compared to the local bundle digest on reruns. They are never silently rotated.

An init container copies the trusted Secret projection into regular mode-0600 files in an in-memory volume; the runtime's existing private-file checks remain intact. PostgreSQL uses SCRAM password authentication over verified TLS. The explicit `cluster` profile accepts only `postgres.dataset-processor.svc.cluster.local:5432`, database/user `processor`, and the fixed Moto service endpoint. The original local loopback and AWS endpoint restrictions are retained. The database server certificate expires after 90 days; renewal is not yet implemented.

All pods run non-root with read-only container root filesystems, dropped capabilities, RuntimeDefault seccomp, and no service-account token mount. PostgreSQL uses a 2 GiB PVC. Resource limits are development defaults, not performance measurements. The cluster is dedicated to this local exercise: network-policy isolation from untrusted neighboring workloads is not claimed. Local Kubernetes Secrets and kind's Docker storage are development storage, not an external secret manager or production encryption boundary.

## Limits and diagnostics

PostgreSQL and Moto remain running throughout the recovery proof. Moto stores emulated objects and queues in memory: restarting/replacing Moto or the whole cluster may lose those objects even though database rows persist. Do not treat this as whole-cluster disaster recovery. There is no cleanup, quota reclamation, certificate rotation, autoscaling, production ingress, hosted pipeline, or AWS provisioning in this milestone.

Inspect status without printing Secrets:

```sh
kubectl --kubeconfig .scratch/k8s/kubeconfig --context kind-dataset-processor-demo -n dataset-processor get pods
kubectl --kubeconfig .scratch/k8s/kubeconfig --context kind-dataset-processor-demo -n dataset-processor logs deployment/processor-worker
```

Infrastructure source is `internal/cluster/resources.go`; `cluster manifest` emits JSON without credentials. Change that source rather than generated manifests. Teardown and any retained-data deletion require an explicit decision and are deliberately absent from the setup script.
