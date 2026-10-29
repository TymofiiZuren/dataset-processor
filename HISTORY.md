# Reconstructed local history

This repository was initialized on 2026-10-29 after the application had already been built. Earlier milestones had no Git snapshots. The initial six commits are retrospective reconstructions from the current source, conversation milestones, and local file timestamps; they are not recovered original commits or an exact record of source at each time.

The October 24 fixture date, October 25 processing/distributed-runtime work, and October 29 owner-interface milestones reflect the work history. Times within those days are representative sequencing choices, not measured completion times. Author and committer dates were set to those milestone dates at the owner's request. Every reconstructed commit says so in its message.

Milestones:

1. Synthetic transaction examples and independently expected outputs (October 24).
2. Shared deterministic processor and module configuration (October 25).
3. Durable runtime, private owner workspace, explicit AWS backend, and local Kubernetes recovery tooling (October 25).
4. Owner job history (October 29).
5. Persistent activity timeline (October 29).
6. Owner operations overview and current project documentation (October 29).

Code snapshots are grouped into buildable milestones and validated with Go tests before committing. Integration-dependent tests in reconstructed snapshots are skipped without explicit test-service configuration; the current source is separately checked with the real local PostgreSQL/Moto integration suite. Runtime secrets, local state, binaries, kubeconfig, and generated evidence remain outside Git. The reconstruction itself did not create a remote repository or push code.

## Subsequent GitHub publication — 2026-10-29

At the owner's separate request, the project was published to the private [TymofiiZuren/dataset-processor repository](https://github.com/TymofiiZuren/dataset-processor). The original fixture commit seeded `main`. Five named feature branches published the remaining milestone commits through pull requests merged in order:

- [PR #1](https://github.com/TymofiiZuren/dataset-processor/pull/1): `feature/deterministic-processor`
- [PR #2](https://github.com/TymofiiZuren/dataset-processor/pull/2): `feature/durable-cloud-kubernetes`
- [PR #3](https://github.com/TymofiiZuren/dataset-processor/pull/3): `feature/owner-job-history`
- [PR #4](https://github.com/TymofiiZuren/dataset-processor/pull/4): `feature/job-activity-timeline`
- [PR #5](https://github.com/TymofiiZuren/dataset-processor/pull/5): `feature/owner-operations-overview`

These are new publication PRs, not recovered historical reviews. Original milestone commit dates remain intact; PR and merge timestamps reflect actual publication. Feature branches are retained. Each merged tree was compared to its validated milestone tree. Local integration/race checks passed against a fresh isolated test database; the reused test database had reached the configured retained-input quota and was left intact. No GitHub Actions pipeline, release, AWS provisioning or deployment was added.
