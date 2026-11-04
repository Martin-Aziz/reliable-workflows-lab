# Implementation phases and acceptance gates

This is a portfolio-oriented reference system, not production software. Implement and verify each phase with explicit evidence; a merged PR is not proof that external integration tests passed.

## Phase 1 — Minimal correct execution core — COMPLETE

Go HTTP service, in-memory leased queue, idempotency, concurrent workers, retries, fencing, deterministic unit tests, and race detection.

## Phase 2 — PostgreSQL durability — IN REVIEW / REQUIRES POSTGRESQL CERTIFICATION

- [x] Shared storage interface with a separate in-memory backend.
- [x] Schema migration for jobs, idempotency keys, and attempt history.
- [x] Transactional idempotent enqueue with unique-key fingerprint comparison.
- [x] `SKIP LOCKED` claims and atomic creation of attempt records.
- [x] SQL failure, success, lease expiry, and retry transitions with token fencing.
- [x] PostgreSQL integration tests authored (concurrency, restart, idempotency, expiry, stale ack).
- [x] Liveness and DB-backed readiness endpoints, local Compose setup.
- [ ] Execute real PostgreSQL integration tests in an environment with a running PostgreSQL server.
- [ ] Run an end-to-end two-process crash/restart certification and document results.

The current code is ready for review but **does not pass the phase exit gate until real PostgreSQL tests run**.

## Phase 3 — Execution reliability — NOT STARTED

Lease heartbeat/renewal; cancellation; task timeouts; jitter; dead-letter replays; fault injection; audit actions.

## Phase 4 — Performance and operations — NOT STARTED

Load tests, p50/p95 latency, Prometheus/OTel, bounded retention, error budgets, security scans, optional minimal CI.

## Phase 5 — Product and interview narrative — NOT STARTED

Developer research, prioritization case study, benchmark report, public demo with synthetic data, and technical interview walkthrough.
