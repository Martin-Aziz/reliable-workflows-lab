# Phase 1 verification — 2026-11-04

## Environment

- Linux amd64
- Go `go1.23.2` available in the authoring container. This is an older local compiler used for verification, not a recommendation to deploy on an unsupported Go release.
- No external Go dependencies, database, Docker, or network services required for the unit tests.

## Commands executed successfully

```text
go test ./...
go test -race -count=1 ./...
go vet ./...
go test -cover ./...
go build -o /tmp/reliable-workflows-lab-server ./cmd/server
BASE_URL=http://127.0.0.1:18888 bash scripts/smoke.sh
```

Observed coverage for tested packages:

```text
internal/httpapi: 91.2% of statements
internal/jobs:    77.0% of statements
cmd/server:        0.0% (no direct command-package tests)
```

Live smoke test against the compiled server:

```text
PASS: job succeeded with sum=9
POST /v1/jobs (new): HTTP 201
POST /v1/jobs (same idempotency key + request): HTTP 200
POST /v1/jobs (same key + changed request): HTTP 409
GET /v1/stats: succeeded=1 at the time of verification
```

## Not verified

- PostgreSQL persistence, database-backed concurrency or failover
- Cross-process coordination and restart durability
- Cloud deployment or remote authentication
- High-load throughput and p95 latency
- External side-effect exactly-once behavior

The Phase 1 prototype must not be described as production-ready.
