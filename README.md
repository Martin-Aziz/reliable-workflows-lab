# Reliable Workflows Lab

**A transparent Go backend-engineering portfolio project about transactional queues, concurrency, retries, and crash recovery.**

> **Status: Phase 2 implementation under review.** The Phase 1 in-memory engine is locally verified. An optional PostgreSQL backend has been implemented, but **its real-database integration suite still needs execution** before durability is certified. This is not a production service.

## Features

- HTTP endpoint to submit `echo`, `sum`, or intentionally failing demo jobs. No execution of user-supplied code or shell commands.
- Atomic idempotent submissions: identical key/request replays the same job, conflicting key reuse returns HTTP 409.
- Concurrency-safe worker leasing with unpredictable ownership tokens and stale-completion fencing.
- Retry backoff, configurable max attempts (1–8), and terminal `dead` state.
- **Two storage modes:** in-memory for fast local experiments; PostgreSQL for persisted jobs and cross-process workers.
- PostgreSQL jobs, idempotency fingerprints, and per-attempt outcome records; `FOR UPDATE SKIP LOCKED` job claims.
- `/healthz` liveness, `/readyz` storage readiness, and `/v1/stats` counts.

## Architecture

```mermaid
flowchart LR
  Client[REST Client] --> API[Go HTTP API]
  API --> Store{Storage interface}
  Store --> Memory[Memory / Phase 1]
  Store --> DB[(PostgreSQL / Phase 2)]
  DB --> WorkerA[Worker A / Process A]
  DB --> WorkerB[Worker B / Process B]
  WorkerA --> Handlers[Task handlers]
  WorkerB --> Handlers
```

The real execution path is *worker -> Store.Claim -> handler -> Store.Complete/Fail*. Workers do not hold SQL transactions open while executing handlers. See [ADR 0002](docs/ADR-0002-postgres-durability.md).

## Quick start — in-memory mode

Requires Go 1.23 or newer. The server binds to loopback by default.

```bash
go mod download
go run ./cmd/server
# In another terminal:
curl -s http://127.0.0.1:8080/readyz
```

In this mode, state is lost after a restart.

## PostgreSQL mode (local Docker)

```bash
docker compose up -d postgres
export DATABASE_URL='postgres://rwl:rwl_dev_only@127.0.0.1:5432/rwl?sslmode=disable'
go run ./cmd/server
```

**These are example development credentials only.** Do not copy them into an Internet-accessible deployment. The server creates the initial schema when it starts; it exits if the database cannot be reached or migrations fail. Compose binds PostgreSQL to loopback and keeps its data in a named Docker volume.

A second process can use the same `DATABASE_URL` to share jobs, provided it binds to a different HTTP address:

```bash
HTTP_ADDR=127.0.0.1:8081 WORKERS=2 go run ./cmd/server
```

A durable restart exercise is: start with `WORKERS=0` (API only), submit a job with an idempotency key, stop the process, and restart with `WORKERS=2` and the same `DATABASE_URL`. Verify the original job completes and reusing the key returns the same ID. A stopped worker's in-flight lease is recovered after its expiry and the retry delay. **The external PostgreSQL restart exercise remains to be certified.**

## API exercise

```bash
curl -i -X POST http://127.0.0.1:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-001' \
  -d '{"kind":"sum","payload":{"numbers":[4,5]},"max_attempts":3}'
```

The first submission returns **201** with a `Location` header. Repeating the exact request with the same key returns **200** and the same ID. Reusing the key with different data returns **409**.

| Method | Path | Meaning |
|---|---|---|
| `GET` | `/healthz` | Process liveness (not DB health) |
| `GET` | `/readyz` | Storage readiness (503 when unavailable) |
| `POST` | `/v1/jobs` | Create/replay/conflict |
| `GET` | `/v1/jobs/{id}` | State, result, attempts |
| `GET` | `/v1/jobs?limit=20` | Up to 100 recent jobs |
| `GET` | `/v1/stats` | State counts |

## Tests

```bash
go test ./...
go test -race -count=1 ./...
go vet ./...
# With a running server in another terminal:
bash scripts/smoke.sh
```

For **real PostgreSQL integration tests**, create a separate database whose name ends in `_test` and set `TEST_DATABASE_URL`; the test suite creates and drops a uniquely named schema, never truncates the application database, and skips when no test URL is provided.

```bash
# If using the local Compose container, create the test database once:
docker compose exec -T postgres psql -U rwl -d postgres -c 'CREATE DATABASE rwl_test;'
export TEST_DATABASE_URL='postgres://rwl:rwl_dev_only@127.0.0.1:5432/rwl_test?sslmode=disable'
make integration
```

The `integration` build tag is intentional: ordinary unit tests need no running PostgreSQL server. A schema teardown runs even when a test fails. Do not set `TEST_DATABASE_URL` to a production database.

## Correctness and limitations

- **At-least-once attempts, not exactly-once effects.** A worker can perform an external side effect and crash before its completion is acknowledged. Integrations must implement idempotent effects.
- An expired lease may still appear `running` until a worker reaps it. The API gives no strict FIFO guarantee.
- The PostgreSQL backend coordinates independent server processes, but has no automatic lease heartbeat. The built-in task timeout is 5s and lease deadline 10s.
- Jobs, idempotency keys, and attempt records currently have no TTL or retention job.
- There is **no authentication, tenant separation, or public API hardening**. Do not expose the server publicly.
- Real PostgreSQL concurrency, failure recovery and migration behavior have not yet been executed in this environment; integration tests exist for external verification.
- The demo's in-memory state is **not** automatically copied into PostgreSQL when switching modes.

## Source navigation

| Path | Purpose |
|---|---|
| `cmd/server/main.go` | Startup, DB connection, workers, graceful shutdown |
| `internal/jobs/` | Shared contracts, memory engine, task execution |
| `internal/postgres/` | SQL store, migration, PostgreSQL integration tests |
| `internal/httpapi/` | HTTP API and readiness semantics |
| `compose.yaml` | Local PostgreSQL 16 development environment |
| `docs/ADR-0002-postgres-durability.md` | Transactional design and explicit non-guarantees |
| `docs/PHASES.md` | Exit gates and current certification status |

## License

MIT — see [LICENSE](LICENSE).


## Phase 3 preview: heartbeat and logical cancellation

**Experimental until real PostgreSQL certification.** Keep the API on
`127.0.0.1`: this development server does not implement authentication.

Use `POST /v1/jobs/{id}/cancel` (no body) to cancel a queued or running job.
Repeated cancellation returns HTTP 200; completed and dead jobs return 409;
unknown IDs return 404. The canceled state appears in `GET /v1/jobs/{id}`
and the `canceled` count in `GET /v1/stats`.

Canceling queued work prevents future claims. Canceling running work revokes
the lease token and prevents stale renewals or acknowledgements; it does
**not** forcibly terminate a process or guarantee that external effects stop.
Handlers must cooperatively respect their context.

The processor renews active leases every 2 seconds by default (configurable
via `LeaseRenewInterval`). Keep this interval below the configured lease
duration. Its 5-second default task timeout remains independent. On heartbeat
loss, the processor cancels the handler context and skips acknowledgement.
External effects remain at-least-once, never exactly-once.

Migrations are tracked in `workflow_schema_migrations` with a PostgreSQL
transaction advisory lock. Existing Phase 2 databases are adopted by replaying
the idempotent initial schema migration before applying migration 002.

Run `go test ./...`, `go test -race ./...`, `go vet ./...` and
`make integration` against a dedicated `*_test` PostgreSQL database.
See `docs/ADR-0003-lease-control.md` for failure boundaries.
