# Implementation phases and acceptance gates

This project is a standalone, open-source portfolio artifact, not a commercial product. Complete and verify each phase before extending scope. The statements below are plans unless explicitly marked complete.

## Phase 1 — Minimal correct execution core — IMPLEMENTED, local verification

Deliverables:
- Go HTTP service and explicit demo task registry.
- Mutex-protected in-memory job state machine.
- Atomic idempotent submission with conflicting-key rejection.
- Concurrent leased claims, stale-token fencing, bounded retries and backoff.
- `GET` endpoints for job status, listing, state counts; health probe.
- Unit/race tests and reproducible manual smoke test.

Exit gate: `go test ./...`, `go test -race ./...`, `go vet ./...`, and HTTP smoke pass; no false claims of durability.

## Phase 2 — PostgreSQL durability — NOT STARTED

- Migration/schema with jobs, attempt history, unique scoped idempotency keys.
- Transactional enqueue and idempotency (`INSERT ... ON CONFLICT` with fingerprint matching).
- Transactional claims with `FOR UPDATE SKIP LOCKED` and a fencing token/version.
- Worker completion, retry, and dead transitions in SQL transactions.
- Crash/restart and concurrent-process integration tests (real PostgreSQL, ideally Testcontainers).
- Explicit database connectivity/readiness checks and failure handling.

Exit gate: restart the API and workers; previously enqueued jobs and deduplication survive; multiple worker processes do not successfully claim the same unexpired lease.

## Phase 3 — Execution reliability — NOT STARTED

- Lease heartbeat or deliberate strict task-duration bound, with token-qualified renewals.
- Cancellation, timeout enforcement, jitter, and configurable retry policies.
- Dead-letter investigation/replay with a new attempt budget and audited operator action.
- Optional outbox pattern for safe publication of business events.

Exit gate: deterministic fault-injection suite covering worker crash, lost acknowledgment, delayed handler, and duplicate external effect.

## Phase 4 — Operations and performance — NOT STARTED

- Structured tracing, Prometheus metrics and dashboards.
- Benchmarks under measured workload and hardware with p50/p95 latency, claim throughput, error rates, and queue lag.
- CPU/memory profile and queue cardinality limits.
- Dependency/security scans and CI with minimum token permissions.

Exit gate: reproducible benchmark report with graphs, environment, commands, and interpretation rather than an unsupported performance number.

## Phase 5 — Product/PM narrative — NOT STARTED

- 2–3 developer interviews about queue reliability/operational pain.
- One-page product brief and documented prioritization decisions.
- Screenshots/video of planned failures, recovery, and operator observations.
- Public demo with sanitized data and a short trade-off explainer.

Exit gate: defend technical decisions and user impact in a 10-minute interview walkthrough.
