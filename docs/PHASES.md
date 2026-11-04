# Implementation phases and acceptance gates

This is a portfolio-oriented reference system, not production software. Implement and verify each phase with explicit evidence; a merged PR is not proof that external integration tests passed.

## Phase 1 — Minimal correct execution core — COMPLETE

Go HTTP service, in-memory leased queue, idempotency, concurrent workers, retries, fencing, deterministic unit tests, and race detection.

## Phase 2 — PostgreSQL durability — MERGED / STILL REQUIRES POSTGRESQL CERTIFICATION

- [x] Shared storage interface with a separate in-memory backend.
- [x] Schema migration for jobs, idempotency keys, and attempt history.
- [x] Transactional idempotent enqueue with unique-key fingerprint comparison.
- [x] `SKIP LOCKED` claims and atomic creation of attempt records.
- [x] SQL failure, success, lease expiry, and retry transitions with token fencing.
- [x] PostgreSQL integration tests authored (concurrency, restart, idempotency, expiry, stale ack).
- [x] Liveness and DB-backed readiness endpoints, local Compose setup.
- [ ] Execute real PostgreSQL integration tests in an environment with a running PostgreSQL server.
- [ ] Run an end-to-end two-process crash/restart certification and document results.

Phase 2 was merged, but **its certification gate remains open until real PostgreSQL tests run**.

## Phase 3 — Execution reliability — IN PROGRESS

- [x] Token-fenced lease renewal in both storage backends.
- [x] Worker heartbeat and explicit task timeout handling.
- [x] Logical cancellation with terminal state and attempt outcome.
- [x] Tracked, serialized database migrations for Phase 2 adoption and Phase 3 schema.
- [ ] Real PostgreSQL integration certification and restart / crash tests.
- [ ] Dead-letter replay, retry jitter, and audited operator actions.

Cancellation fences completion but cannot force-stop code that ignores context. No authentication is present: keep API bound to loopback.

## Phase 4 — Performance and operations — NOT STARTED

Load tests, p50/p95 latency, Prometheus/OTel, bounded retention, error budgets, security scans, optional minimal CI.

## Phase 5 — Product and interview narrative — NOT STARTED

Developer research, prioritization case study, benchmark report, public demo with synthetic data, and technical interview walkthrough.
