.PHONY: run test race vet cover build smoke integration
run:
	go run ./cmd/server

test:
	go test ./...

race:
	go test -race -count=1 ./...

vet:
	go vet ./...

cover:
	go test -cover ./...

build:
	mkdir -p bin
	go build -o bin/workflows ./cmd/server

smoke:
	bash scripts/smoke.sh

# Requires a dedicated database ending in _test (for example rwl_test).
integration:
	@test -n "$(TEST_DATABASE_URL)" || (echo "Set TEST_DATABASE_URL to a dedicated *_test database URL" >&2; exit 1)
	go test -tags=integration -count=1 -v ./internal/postgres
