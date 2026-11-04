# ADR 0001 — Queue state machine, leases, and durable evolution

**Status:** Accepted for Phase 1 (2026-11-04)

## Problem

Demonstrate backend reliability fundamentals under concurrent workers, including duplicate submission, retries, worker failure, and stale acknowledgments. The implementation must be small enough to reason about and test deterministically.

## Decision

Use a serialized, in-memory queue reference implementation in Phase 1, then replace state storage with PostgreSQL in Phase 2. Keep the service API and worker behavior stable so the storage implementation can be swapped without rewriting user-facing semantics.

### Semantics

1. **Enqueue:** when a nonempty `Idempotency-Key` is supplied, the same canonical request returns the same job; differing requests return a conflict. Deduplication is atomic inside a mutex.
2. **Claim:** one worker obtains a lease token and deadline. Attempts increase at claim time, including claims ultimately lost to crashes.
3. **Complete:** the successful worker may complete only while it still owns an unexpired lease. A stale token is rejected.
4. **Failure:** attempts remaining -> queued after exponential backoff; exhausted -> dead.
5. **Expiry:** a subsequent claim pass reaps expired leases, schedules a retry, or marks the job dead if the budget is exhausted.
6. **Visibility:** `GET` may temporarily show an expired lease as running until the next worker poll. This is an acknowledged limitation in Phase 1.

### Non-guarantees

- No durable state, zero loss under restart, or multi-process coordination.
- No exactly-once effects in downstream services.
- No strict FIFO ordering during retries or claims; later due jobs may be selected first while older jobs are in backoff.
- No bounded memory usage as the number of jobs increases.

## Why PostgreSQL next?

PostgreSQL transactions can make job insertion and idempotency constraints durable, and `FOR UPDATE SKIP LOCKED` supports competing worker claims without waiting on already-locked queue rows. The locking behavior is purpose-built for a queue-like workload, **not** for a general consistent read. Phase 2 must test simultaneous claims, crash recovery, process restart, and transaction boundaries against a real Postgres instance.

Reference: [PostgreSQL SELECT locking clause](https://www.postgresql.org/docs/current/sql-select.html).

## Alternatives

- **Redis Streams:** powerful consumer groups and visibility management, but requires careful acknowledgments and a separate durable idempotency model; useful for Phase 3 comparison.
- **RabbitMQ:** strong delivery features, but adds a broker before we have demonstrated fundamental state and recovery invariants.
- **Cloud managed queue:** fast operational start, but external cost/dependency makes the core portfolio demo harder to reproduce.

## Key correctness invariant

Once a lease token is replaced or expired, **that token must never be able to commit a result**. A database version will implement this with an atomic, version- or token-qualified `UPDATE`, checking affected row count.
