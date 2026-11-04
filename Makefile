.PHONY: run test race vet cover build smoke
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
