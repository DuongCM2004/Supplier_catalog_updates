# Resilient Supplier Catalog Sync

A Go service and PostgreSQL database that import a supplier's product updates into a catalog
without losing or corrupting data. The supplier sends an update as many batches; the service
applies each batch in one database transaction, remembers how far it got, and can pick up
again after a supplier outage or an application restart. Older revisions never overwrite newer
ones, a withdrawn product stays withdrawn, and a sync is reported as completed only when the
supplier really signalled the end of the feed and no blocking problem is open.

## 1. Description and architecture

(Architecture diagram and a short walk through one sync, added after the API lands.)

## 2. Run it

Prerequisites: Docker with Compose v2 or newer (developed with Docker 29). Go 1.27 is only
needed to run the tests directly on your machine.

```bash
cp .env.example .env
docker compose up --build
```

The first start builds the image, starts PostgreSQL 17, waits until it is healthy, and then
the API starts and applies the SQL migrations automatically. Once both containers are running,
check it:

```bash
curl http://localhost:8088/healthz
```

The API is on `http://localhost:8088` and PostgreSQL on `localhost:55432` (for `psql` and the
tests). Both host ports are deliberately unusual so they do not clash with a PostgreSQL or web
server already running on your machine. The containers still listen on 8080 and 5432
internally.

### Change the ports

Set `APP_PORT` (API) and `DB_PORT` (PostgreSQL) in `.env`, or for one command only:

```bash
APP_PORT=18088 DB_PORT=55433 docker compose up --build        # bash
$env:APP_PORT = 18088; $env:DB_PORT = 55433; docker compose up --build   # PowerShell
```

A variable set in your shell wins over `.env`. To run a second, isolated stack next to the
first, give it a new `COMPOSE_PROJECT_NAME` (its own containers and data) and different
`APP_PORT` and `DB_PORT` values.

### Stop and reset

```bash
docker compose down        # stop; the catalog and job history stay in the pgdata volume
docker compose down -v     # WARNING: also deletes the volume - every product, sync job and issue is lost
```

If you have `make`, `make up`, `make down`, `make reset` (the `down -v` above, with a warning)
and `make logs` do the same. Every target in the `Makefile` is a single command that you can
copy and run without make.

### Settings in `.env`

| Variable | Default | Purpose |
|---|---|---|
| `APP_PORT` | `8088` | host port of the API |
| `DB_PORT` | `55432` | host port of PostgreSQL |
| `API_TOKEN` | empty | when set, the `POST` routes require `Authorization: Bearer <API_TOKEN>`; reads stay public |
| `LOG_LEVEL` | `info` | log verbosity |
| `SUPPLIER_OUTAGE_CURSORS`, `SUPPLIER_DELAY_MS`, `CRASH_POINT` | empty / `0` / empty | failure switches for demos and tests only; see "Tests" |

## 3. Using the API

The service reads supplier updates from the JSON files in [`fixtures/`](fixtures), one file per
update id. The database schema is created by the migrations at startup; there is nothing to
run by hand.

| Update | What it shows | Needs first | Expected outcome |
|---|---|---|---|
| `UPDATE-2026-001` | normal import: create, update, withdraw, and withdraw a product never seen before | - | COMPLETED: 5 created, 2 updated, 2 withdrawn |
| `UPDATE-2026-002` | old revisions, exact repeats, a withdrawn product offered as active again, revisions out of order inside a batch | 001 | COMPLETED: 1 created, 2 updated, 3 duplicate, 2 stale |
| `UPDATE-2026-003` | five clean batches, for the outage and restart demo | - | COMPLETED: 10 created; with `SUPPLIER_OUTAGE_CURSORS=cursor-003` it stops INTERRUPTED after 2 batches |
| `UPDATE-2026-004` | a negative price in the second batch | - | NEEDS_ATTENTION, `INVALID_PRODUCT_DATA`; the first batch stays committed |
| `UPDATE-2026-005` | revision 2 of a product repeated with a different price | 001 | NEEDS_ATTENTION, `VERSION_CONFLICT`; the whole batch is rolled back |
| `UPDATE-2026-006` | a withdrawn product offered as active at a higher revision | 001 | NEEDS_ATTENTION, `REACTIVATION_BLOCKED`; the whole batch is rolled back |
| `UPDATE-2026-007` | the last page has no `has_more` marker | - | NEEDS_ATTENTION, `INVALID_BATCH`; never COMPLETED |
| `UPDATE-2026-008` | `has_more` is true but the next page does not exist | - | NEEDS_ATTENTION, `INVALID_CURSOR` |
| `UPDATE-2026-009` | a later page reuses an already committed batch id with different products | - | NEEDS_ATTENTION, `BATCH_CONFLICT` |

(Request and response examples for starting a sync, reading its status, resuming it and
listing products, changes and issues, added after the API lands.)

## 4. Tests

Unit tests need no database:

```bash
go test ./...
```

Tests that use PostgreSQL run against the database named in `TEST_DATABASE_URL`, which is
created and migrated on first use. Start the database, then run everything one package at a
time (`-p 1`, because the packages share one database):

```bash
docker compose up -d db
TEST_DATABASE_URL='postgres://catalog:catalog@localhost:55432/catalog_test?sslmode=disable' \
  go test -p 1 -count=1 ./...
```

Without `TEST_DATABASE_URL` those tests are reported as skipped, not passed.

Data-race run. `-race` needs a C compiler, so it runs in the Debian-based Go image on the
compose network (the database must be up):

```bash
docker run --rm --network catalog-sync_default -v "$PWD":/src -w /src -e CGO_ENABLED=1 \
  -e TEST_DATABASE_URL='postgres://catalog:catalog@db:5432/catalog_test_race?sslmode=disable' \
  golang:1.27 go test -race -p 1 -count=1 ./...
```

On Windows Git Bash, put `MSYS_NO_PATHCONV=1` in front of `docker run`; otherwise Git Bash
rewrites `-w /src` into a Windows path and Docker rejects it.

(Reproducing a supplier outage and an application restart, and the end-to-end and smoke
checks, added after the API lands.)

## 5. Assumptions

(Added after the catalog rules land.)

## 6. Limitations and future work

(Added after the catalog rules land.)

## 7. AI disclosure

(Wording drafted separately and confirmed by the author before submission.)

## 8. Demo video and deployment

(Added once the video and the deployment exist.)
