# ADR-0003 — Lease heartbeat and logical cancellation

**Status:** Proposed (Phase 3 review); PostgreSQL integration certification pending.

## Problem
Phase 2 gives workers fixed-duration leases. Jobs that require more time may
expire and be retried while the previous handler is still running. Operators
also need to cancel a queued task or prevent results from an already-running
task from being accepted.

## Decision
- Each active Processor renews its token-owned lease periodically. A renewal
  must fail if the lease expired, the token changed, or the job is not running.
- A renewal error cancels the handler context and prevents completion or
  failure acknowledgement. The original lease expires and can be retried.
- Cancellation is a terminal state distinct from handler failure. Queued
  cancellation prevents claims; running cancellation revokes the token and
  writes an attempt outcome of `canceled` atomically in PostgreSQL.
- Late handler completion after its task deadline is rejected even if that
  handler ignored its cancellation context.
- Cancellation remains logically idempotent, but already-succeeded/dead jobs
  cannot be rewritten. This avoids retroactive history modification.
- Versioned migrations run in one transaction under a transaction-scoped
  advisory lock. Old Phase 2 schema is adopted via idempotent 001_init.

## Tradeoffs and threats
- Cancellation is **not** process termination. Side effects are not rolled
  back. Handlers must check their context and use downstream idempotency.
- Lease renewal uses database updates, increasing write load for long jobs.
  Keep the heartbeat interval shorter than the lease duration with sufficient
  margin for DB stalls and scheduling jitter.
- No authentication is present. Keep the development API bound to loopback;
  do not expose cancellation to untrusted clients.
- The API promises at-least-once attempts, **not** exactly-once external effects.
- All PostgreSQL behavior remains unverified until real-database tests pass.

## Acceptance tests
- Renew only a live lease with the correct token, including after process
  connection-pool changes.
- Cancel queued/running jobs; old acknowledgements fail; cancellation is
  idempotent; terminal jobs cannot be retroactively canceled.
- Running attempt history remains explicit and the canceled count is correct.
- Long handlers retain ownership using heartbeat while timed-out handlers
  cannot commit late results.
- Run against a real PostgreSQL server and document results before merge.
