# Phase 2 verification record — 2026-11-04

**Status:** local core regressions passed; PostgreSQL runtime certification still pending.

## Environment

- Linux amd64, Go 1.23.2.
- No reachable PostgreSQL server or container runtime in the authoring environment.
- Restricted external network access means the real `github.com/lib/pq` module cannot be downloaded locally.

## Checks actually executed

```text
go test ./internal/...
go test -race -count=5 ./internal/...
go vet ./internal/...
```

Results: **PASS** for the in-memory engine, common job contracts, HTTP API and compilation/vet of the SQL store. `internal/postgres` has no database-free runtime test (its runtime integration tests are build-tagged).

For a separate syntax/compile check only, a temporary local `github.com/lib/pq` **compile stub** was substituted outside the repository. With that placeholder:

```text
go test ./...                                         PASS (compilation + unit tests)
go test -tags=integration -run '^$' ./internal/postgres  PASS (compilation only)
go vet ./...                                          PASS
```

This did **not** validate the real PostgreSQL driver. The stub was NOT committed, and `go.mod` and `go.sum` specify the actual `lib/pq` v1.12.3 module.

The in-memory server was also compiled and run with this compile-only stub. Live smoke results:

```text
GET /readyz                              200 {"status":"ready"}
POST /v1/jobs (sum task)               201, eventually succeeded (sum=9)
HTTP_ADDR=... WORKERS=0                  201 with job initially queued
```

## Prepared but NOT executed

```bash
docker compose up -d postgres
docker compose exec -T postgres psql -U rwl -d postgres -c 'CREATE DATABASE rwl_test;'
export TEST_DATABASE_URL='postgres://rwl:rwl_dev_only@127.0.0.1:5432/rwl_test?sslmode=disable'
make integration
```

The PostgreSQL integration suite covers independent connection pools, simultaneous idempotent enqueue, different-payload conflict, durable replay, due-job contention, expired lease recovery, stale completion fencing, persisted attempt history, and reopening a connection pool after shutdown. **The source exists; passing results are not claimed.**

## Remaining release gates

1. Run integration suite using the real driver and PostgreSQL 16.
2. Manually restart API and worker processes against a named PostgreSQL volume; confirm data survival.
3. Fix any SQL/runtime discrepancies found by those checks before describing Phase 2 as verified.

This PR should remain a draft until the real-database checks pass.
