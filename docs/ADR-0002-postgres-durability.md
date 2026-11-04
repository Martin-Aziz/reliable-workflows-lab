# ADR 0002 — PostgreSQL durable leases and idempotent submission

**Status:** Proposed for review (Phase 2)

## Context

The Phase 1 mutex protects only one process. Restart loses jobs and idempotency keys; two processes cannot safely share work. This ADR specifies a durable queue architecture for PostgreSQL deployments while retaining the in-memory reference backend for testability.

## Decision

- Persist jobs in `workflow_jobs`, key/fingerprint pairs in `workflow_idempotency`, and worker attempts in `workflow_attempts`.
- Enqueue a job and reserve its idempotency key in **one database transaction**. A second concurrent enqueue with the same key waits on the unique index: identical canonical requests replay the committed job, mismatched requests conflict.
- Claim at most one **due** job with `FOR UPDATE SKIP LOCKED` in a data-modifying CTE; increment `attempts`, set worker and cryptographically random lease token, and insert attempt history in the **same SQL statement**.
- Do **not** hold a transaction open while executing a handler. The claim is committed before work begins.
- Complete or fail by an atomic token-qualified update requiring `lease_until > now()`. That same statement records the attempt outcome; stale workers cannot commit job results.
- A polling worker reaps batches of expired leases using an indexed `SKIP LOCKED` selection; each consumed attempt is marked expired and either retried after backoff or marked dead.
- Use the database clock for persisted time comparisons and lease deadlines, avoiding drift between independent app processes.
- Use short, explicit readiness pings in `/readyz`; `/healthz` deliberately reports only process liveness.

## Guarantees

**Provided when the PostgreSQL integration tests pass:** durable jobs and idempotency through process restarts; at most one valid lease at a time per job; fenced acknowledgments; bounded attempts; stored attempt history. These are *not* verified by Go-only unit tests.

**Not provided:** exactly-once handler execution or exactly-once external side effects. Work can run twice if a worker completes an external action but crashes before the database acknowledgment; handlers interacting with outside systems must use their own idempotency keys. The default 10-second lease is longer than the built-in task timeout, but not renewable.

## Operational limits and trade-offs

- Both backends use the same HTTP API, but state is not automatically migrated from an in-memory process to PostgreSQL.
- Lock contention is reduced with `SKIP LOCKED` but job selection is not strict FIFO. Reaping is bounded to 100 expired leases per pass.
- Migrations currently run at server startup and are deliberately additive (`IF NOT EXISTS`). There is no rolling upgrade framework or schema-version lock yet.
- Idempotency entries are retained indefinitely until an operator-defined retention policy is implemented.
- The database is a single durable dependency; it needs real backups, access controls, least-privilege credentials, pooling, and monitoring before production use.
- There is no externally authenticated API. Default binding is loopback; **do not expose the demo publicly**.

## Alternatives considered

Redis Streams, RabbitMQ, and managed queues are viable but would obscure the SQL transaction and concurrency demonstration. PostgreSQL keeps the state machine inspectable for interview discussion and eliminates a separate broker in this phase.

## References

[PostgreSQL `SELECT` locking clause](https://www.postgresql.org/docs/current/sql-select.html) documents `SKIP LOCKED` as suitable for multiple consumers of a queue-like table, while warning that it produces an inconsistent view for ordinary reads.
