# Shortcuts for reviewers. Every recipe is one self-contained shell command, so it can also be
# pasted into a terminal on a machine without make. Override a variable on the command line,
# e.g. `make test-integration DB_PORT=15432`.

# Must match DB_PORT in .env when you changed it there.
DB_PORT ?= 55432
TEST_DATABASE_URL ?= postgres://catalog:catalog@localhost:$(DB_PORT)/catalog_test?sslmode=disable
# The race run executes inside the compose network, where PostgreSQL is reachable as "db".
COMPOSE_NETWORK ?= catalog-sync_default
RACE_DATABASE_URL ?= postgres://catalog:catalog@db:5432/catalog_test_race?sslmode=disable

.PHONY: up down reset logs test test-integration test-race smoke demo

# Build the image and start the API and PostgreSQL in the background.
up:
	docker compose up --build -d

# Stop the stack; the database volume and all data survive.
down:
	docker compose down

# Stop the stack and DELETE the database volume: every product, sync job and issue is lost.
reset:
	echo 'WARNING: deleting the pgdata volume; all products, sync jobs and issues of this compose project are lost'; docker compose down -v

logs:
	docker compose logs -f app

# Unit tests only; database tests skip when TEST_DATABASE_URL is not set.
test:
	go test ./...

# Everything, including database and process tests. Needs the database from "make up" (or "docker compose up -d db").
test-integration:
	TEST_DATABASE_URL='$(TEST_DATABASE_URL)' go test -p 1 -count=1 ./...

# Data-race run. -race needs gcc: the Debian golang image has it, the alpine one does not.
# On Windows Git Bash prefix the command with MSYS_NO_PATHCONV=1 so "-w /src" is not rewritten.
test-race:
	docker run --rm --network $(COMPOSE_NETWORK) -v "$$PWD":/src -w /src -e CGO_ENABLED=1 -e TEST_DATABASE_URL='$(RACE_DATABASE_URL)' golang:1.27 go test -race -p 1 -count=1 ./...

# Black-box check of every HTTP route on its own throwaway compose project.
smoke:
	bash scripts/smoke.sh

# Scripted walkthrough (sync, duplicates, outage, restart, resume) on its own compose project.
demo:
	bash scripts/demo.sh
