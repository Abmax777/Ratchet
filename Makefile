DSN ?= postgres://postgres@localhost:5432/ratchet?sslmode=disable

.PHONY: up migrate demo resume crash-test test vet clean

up:                ## start postgres in docker
	docker compose up -d

migrate:           ## apply migrations
	psql "$(DSN)" -f migrations/001_init.sql

demo:              ## run one workflow end to end
	RATCHET_DSN="$(DSN)" go run ./cmd/demo

resume:            ## finish any unfinished workflow
	RATCHET_DSN="$(DSN)" go run ./cmd/demo -resume

crash-test:        ## kill a workflow mid-flight and prove it resumes cleanly
	RATCHET_DSN="$(DSN)" ./scripts/crash_test.sh

test:              ## run the runtime tests (no database needed)
	go test ./rt/... -count=1

check:             ## build and vet everything (needs task 5 done)
	go build ./... && go vet ./...

vet:
	go vet ./...

clean:
	psql "$(DSN)" -c "TRUNCATE steps, workflows, side_effects RESTART IDENTITY CASCADE;"
