# Interview guide: what you can demonstrate and defend

## 60-second walkthrough

"I built a small job-processing system to study reliability rather than hide it behind a framework. An HTTP API accepts jobs with optional idempotency keys. Multiple workers use atomic leases, bounded retries, and fencing tokens. The tests deliberately race submissions and claims, expire leases, and prove that stale acknowledgments fail. Phase 1 is deliberately memory-only; the next phase is Postgres-backed durability and crash-recovery tests."

Only say this after reviewing and personally demonstrating the code. Do not claim production deployment or user adoption.

## Engineering questions to rehearse

**Why does the queue use a mutex?** The Phase 1 state machine is single-process; protecting the find/claim/update as one critical section prevents duplicate live claims. It is not a distributed lock.

**Does this guarantee exactly-once execution?** No. A worker can perform an external effect before its acknowledgment is received, lose a lease, and cause replay. Leases protect state updates, not external services. An external sink needs an idempotency key and a suitable transaction boundary.

**What happens if two clients submit the same idempotency key simultaneously?** Both operations are serialized by one mutex; only one inserts, and the other returns the same job if its canonical request matches. A conflicting payload returns 409.

**What happens if a worker dies while processing?** In-process, after a lease expires the next claim loop schedules a retry, or marks dead if there are no attempts remaining. If the entire process dies, all jobs are lost in Phase 1 — a deliberate limitation to fix.

**What happens if a handler ignores cancellation?** It may exceed the lease and run concurrently with a retry. Phase 3 introduces stronger timeout/heartbeat assumptions and fault tests.

**Why not use RabbitMQ immediately?** The goal is to prove the delivery invariant, retry state machine and ownership fencing independently before choosing a broker. An ADR documents the trade-off.

**How do you test it?** State-machine tests use a fake clock, API tests use `httptest`, concurrent claims use multiple goroutines, and `go test -race` checks data races. True multi-process/Postgres integration is Phase 2.

## Resume bullets (adapt only after verifying personally)

- "Built a Go job-processing prototype with concurrency-safe scheduling, idempotent HTTP submission, bounded retries, and lease-token fencing."
- "Developed deterministic failure-recovery and concurrent-claim tests; validated the local implementation using the Go race detector."

Do not fabricate throughput, uptime, tests count, users, or production status.

## Demo sequence (3 minutes)

1. Submit a `sum` job, inspect result via `GET /v1/jobs/{id}`.
2. Repeat submission with same idempotency key; demonstrate the same ID.
3. Change payload with same key; demonstrate 409 Conflict.
4. Submit `fail`, observe retries/backoff and terminal `dead`.
5. Run `go test -race -count=1 ./...` and explain expiry and fencing tests.
