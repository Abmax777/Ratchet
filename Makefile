DSN ?= postgres://postgres@localhost:5432/ratchet?sslmode=disable

.PHONY: up migrate demo resume crash-test worker-kill-test chaos chaos-mem test test-pg vet clean

up:                ## start postgres in docker
	docker compose up -d

migrate:           ## apply migrations
	psql "$(DSN)" -f migrations/001_init.sql
	psql "$(DSN)" -f migrations/002_leases_retries.sql

demo:              ## run one workflow end to end
	RATCHET_DSN="$(DSN)" go run ./cmd/demo

resume:            ## finish any unfinished workflow
	RATCHET_DSN="$(DSN)" go run ./cmd/demo -resume

crash-test:        ## kill a workflow mid-flight and prove it resumes cleanly
	RATCHET_DSN="$(DSN)" ./scripts/crash_test.sh

worker-kill-test:  ## stage 2: three workers, SIGKILL one mid-run, everything still completes
	RATCHET_DSN="$(DSN)" ./scripts/worker_kill_test.sh

chaos:             ## stage 3: chaos harness against postgres (truncates tables)
	RATCHET_DSN="$(DSN)" go run ./cmd/chaos -store pg

chaos-mem:         ## stage 3: chaos harness in memory
	go run ./cmd/chaos -store mem -runs 2000

test:              ## run the runtime tests (no database needed)
	go test ./rt/... -race -count=1

test-pg:           ## store conformance suite against postgres (truncates tables)
	RATCHET_TEST_DSN="$(DSN)" go test ./store/... -race -count=1

check:             ## build and vet everything
	go build ./... && go vet ./...

vet:
	go vet ./...

clean:
	psql "$(DSN)" -c "TRUNCATE steps, workflows, side_effects RESTART IDENTITY CASCADE;"
