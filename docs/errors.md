# Durable errors and next actions

| Outcome | HTTP / safe code | Action |
|---|---|---|
| Missing/unowned job | 404 | Check the authenticated job link; no ownership disclosure |
| No session | 401 for downloads/status; job page offers login | Sign in; relative job return URL is validated |
| Bad Host/Origin/CSRF | 403 | Use the printed loopback URL and current form/session |
| Upload above 8 MiB | 413 | Reduce input; no new input admitted |
| Conflicting idempotency key | 409 | Reload for a new submission, or retry the original bytes |
| Two active jobs or retained store full | 429 / capacity | Wait for active work or inspect retained quota; Retry-After is explicit |
| Download slots full | 429 | Retry after five seconds |
| Cancellation loses to terminal outcome | 409 | Reload the job; its committed outcome stands |
| Expired result | 410 / RESULT_EXPIRED | Do not retry download; terminal counts remain until metadata expiry |
| Local storage outage | 503 / STORAGE_UNAVAILABLE | Restore local service, then retry with backoff |
| Missing/corrupt committed version before expiry | 500 / INTEGRITY_ERROR | Investigate selected object versions; ordinary retries do not repair references |

Job failures preserve a safe code: invalid CSV/header/UTF-8/record limit, `CAPACITY_EXHAUSTED`, `RECOVERY_EXHAUSTED`, `RECOVERY_DEADLINE`, or stage/worker infrastructure failure. Invalid individual fields produce rejected records rather than job failure. Arbitrary-input hashes establish integrity, not business correctness. The known fixture comparison establishes expected bytes only for that fixture.

The browser uses server-confirmed stage, counts and revision, not an inferred percentage. Network loss retains last confirmed state. Lost cancellation response explicitly says its outcome is unknown. Downloads are checked before response headers; a failure during the second streaming pass aborts the connection. Check the manifest hash for a complete downloaded file. CLI failures exit nonzero; safe role logs identify role/job/code, with durable events/chunk details available through read-only diagnostics.
