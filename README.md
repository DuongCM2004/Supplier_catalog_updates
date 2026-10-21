# Resilient Supplier Catalog Sync

A Go service and PostgreSQL database that import a supplier's product updates into a catalog
without losing or corrupting data. The supplier sends an update as many batches; the service
applies each batch in one database transaction, remembers how far it got, and can pick up
again after a supplier outage or an application restart. Older revisions never overwrite newer
ones, a withdrawn product stays withdrawn, and a sync is reported as completed only when the
supplier really signalled the end of the feed and no blocking problem is open.

## 1. Description and architecture

The service is a modular monolith: one Go process (HTTP API, job manager and sync workers in
the same binary) and one PostgreSQL database. Everything that must survive, the catalog and
the progress of every sync, lives in PostgreSQL, so the process can be killed and started
again at any moment without losing track of where it was.

```mermaid
flowchart TB
  staff["Staff / curl"] -->|"JSON over HTTP"| api
  subgraph app["Go service (single instance)"]
    api["api: routes and handlers<br/>request log"]
    mgr["syncjob.Manager<br/>start, resume, reconcile, shutdown"]
    wrk["worker: one goroutine per running job<br/>bounded retry with backoff"]
    prc["Processor<br/>one transaction per batch"]
    cat["catalog<br/>validate, revision rules, apply"]
    sup["supplier: fixture client<br/>demo failure switches"]
    api --> mgr --> wrk
    wrk --> sup
    wrk --> prc --> cat
  end
  sup --> fx[("fixtures/*.json")]
  prc --> pg
  cat --> pg
  api -->|"read queries"| pg
  pg[("PostgreSQL 17<br/>products, sync_jobs, sync_batches,<br/>sync_changes, sync_issues")]
```

**One transaction per batch.** The worker fetches a batch from the supplier outside any
transaction, so a slow supplier never holds database locks. It then applies the whole batch in
a single PostgreSQL transaction: the revision rules change the catalog, and the audit rows,
the batch marker, the job counters and the next cursor are written in that same transaction,
so all of it is saved or none of it is.

**Six guarantees**, enforced in code and, where it matters, again by the database itself:

1. **Repeats change nothing.** Starting the same update twice, receiving a batch again or
   receiving the same record again never changes the catalog or the counters a second time.
2. **Revisions only go up.** A product's stored revision never decreases; an older revision
   that arrives late is counted as stale and ignored.
3. **Withdrawn stays withdrawn.** A withdrawal is kept as a tombstone (even for a product the
   catalog never saw) and no later event brings the product back.
4. **A batch is applied whole or not at all.** A crash or a bad record cannot leave catalog
   changes without their progress record, or progress without its changes.
5. **Progress survives failures.** After a supplier outage, a crash or a restart, a job
   continues from its saved cursor and ends with the same result as a run without failures.
6. **Completed means completed.** A job is COMPLETED only when the supplier signalled the end
   of the feed, every batch was committed and no blocking problem is open. A timeout or a
   missing end marker never counts as success.

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
APP_PORT=18088 DB_PORT=55432 docker compose up --build        # bash
$env:APP_PORT = 18088; $env:DB_PORT = 55432; docker compose up --build   # PowerShell
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
| `SUPPLIER_OUTAGE_CURSORS`, `SUPPLIER_DELAY_MS`, `CRASH_POINT` | empty / `0` / empty | failure switches for demos and tests only; described under "Server configuration" |

### Server configuration

The server reads all of its settings from environment variables, once, at startup. Compose
passes the ones above through; the full list matters if you run the binary yourself or deploy
it elsewhere (the Render deployment sets `DATABASE_URL`, `API_TOKEN` and `SUPPLIER_ID`).

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | required | PostgreSQL connection URL |
| `HTTP_ADDR` | `:8080`, or `:$PORT` when `PORT` is set and `HTTP_ADDR` is not | listen address inside the container; platforms such as Render inject `PORT` |
| `API_TOKEN` | empty | when set, `POST` routes need `Authorization: Bearer <API_TOKEN>`; empty leaves them open |
| `SUPPLIER_ID` | `SUP-001` | supplier used when a request does not name one |
| `FIXTURES_DIR` | `/app/fixtures` | folder of supplier update files read by the fixture supplier |
| `RETRY_MAX_ATTEMPTS` | `4` | supplier attempts per fetch before the job is interrupted |
| `RETRY_BASE_DELAY_MS`, `RETRY_MAX_DELAY_MS` | `200`, `2000` | first wait between attempts (doubles each attempt) and its cap |
| `SUPPLIER_TIMEOUT_MS` | `2000` | time limit for one supplier fetch attempt |
| `DB_WAIT_SECONDS` | `30` | how long start-up waits for PostgreSQL |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |
| `SUPPLIER_OUTAGE_CURSORS` | empty | demo/test switch: comma-separated cursors on which the supplier always answers "unavailable" |
| `SUPPLIER_DELAY_MS` | `0` | demo/test switch: pause before every supplier fetch |
| `CRASH_POINT` | empty | demo/test switch: `before_commit:<batch_id>` or `after_commit:<batch_id>` ends the process with exit status 3 at that batch, just before its transaction commits or just after; a malformed value stops start-up with status 1. Restart without it, or the service crashes at the same batch again |

Invalid values are all reported together and the process exits with status 1 before it
touches the database. Start-up waits for PostgreSQL, applies the embedded SQL migrations,
marks any job a previous process left `PENDING` or `RUNNING` as `INTERRUPTED`, and only then
starts listening. On SIGINT or SIGTERM the server stops accepting connections and, at the same
moment, tells the sync workers to stop: a running job is saved as `INTERRUPTED` with no issue
recorded and can be resumed, normally within a fraction of a second. Requests in flight are
allowed to finish. The HTTP shutdown and the worker stop share one limit of 8 seconds, so the
whole stop fits inside Docker's default 10-second grace period, and the database pool is closed
only after both have finished. A client holding an unused connection open can delay the HTTP
part by about 5 seconds, never the workers. A second SIGINT or SIGTERM during the stop ends the
process at once. A request to start or resume a job that is handled while the service stops is
answered `503 SHUTTING_DOWN`. The server closes a request that takes more than 15 seconds to
arrive (5 seconds for its headers) and a keep-alive connection that stays idle for 60 seconds.

`GET /healthz` answers `200 {"status":"ok","database":"ok"}`, or
`503 {"status":"unavailable","database":"error"}` when PostgreSQL does not answer within one
second.

Logs are JSON on stdout (`docker compose logs app`). `startup.config` shows the effective
settings with the database password replaced by `xxxxx` and only whether `API_TOKEN` is set,
never its value; every request logs `http.request` with the method, route pattern, status and
`duration_ms`. A sync job logs `job.created`, `job.transition`, one `batch.committed` per
batch and `job.completed`, and every retried supplier call logs `supplier.retry` with its
attempt and delay. `startup.reconciled` is logged on every start with the `count` and
`job_ids` of the jobs it interrupted (an empty list is normal).

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

### Catalog rules

Every supplier product event carries a revision, and the revision decides what happens:

| Incoming event, compared with the stored product | Result |
|---|---|
| no stored product, ACTIVE | created |
| no stored product, WITHDRAWN | a tombstone is stored (a withdrawal of a product never seen is kept) |
| higher revision | updated, or withdrawn if the event is a withdrawal |
| lower revision | ignored as stale |
| same revision, same content | ignored as a harmless duplicate |
| same revision, different content | the batch is blocked: `VERSION_CONFLICT` |
| WITHDRAWN at a higher revision for a withdrawn product | only the tombstone's revision moves forward; nothing else changes |
| ACTIVE at a higher revision for a withdrawn product | the batch is blocked: `REACTIVATION_BLOCKED` |
| ACTIVE at a lower revision for a withdrawn product | ignored as stale |

A batch is validated first (every problem is reported at once, including NUL bytes and invalid
UTF-8 in text), then applied all-or-nothing in product-key order under row locks, so
concurrent writers cannot deadlock. PostgreSQL triggers independently refuse any revision
decrease or reactivation, so the rules also hold against manual SQL.

A batch of more than 1000 products is refused as `INVALID_BATCH` before the catalog is touched,
because the batch transaction makes a database round trip per product and would hold its locks
too long. The same code covers a batch id or next cursor that contains NUL bytes or invalid
UTF-8, and an update that announces fewer than 1 or more than 2147483647 batches.

### Job states

| Status | Meaning | What you can do |
|---|---|---|
| `PENDING` | the job exists and its worker is starting | wait |
| `RUNNING` | a worker is fetching and applying batches | wait |
| `COMPLETED` | the supplier signalled the end of the feed, every batch is committed, no blocking issue is open | nothing, it is final |
| `NEEDS_ATTENTION` | a batch was refused (conflicting revisions, invalid record, broken cursor chain, ...) or the database failed in a way that trying again cannot fix (a rejected value or constraint, a defect); none of that batch was applied and the issue says why | fix the data at the supplier or the cause named in the issue, then resume |
| `INTERRUPTED` | the supplier stayed unavailable after the retries, PostgreSQL was temporarily unavailable or overloaded (connection lost, no free connection, timeout, serialization failure, deadlock), or the service was stopped or restarted | resume |
| `FAILED` | the supplier does not know that update | nothing, it is final |

Only one job is active (`PENDING` or `RUNNING`) at a time. Starting a different update while one
is active is refused with `409`; starting an update that already has a job returns that job
and never restarts it. Recovery is always an explicit `POST .../resume`.

### Calling the API

The examples use the compose default `http://localhost:8088`; use your `APP_PORT`, or the
Render URL, instead. Reads (`GET`) never need a token. The two `POST` routes need
`Authorization: Bearer <API_TOKEN>` only when the server runs with `API_TOKEN` set (always on
Render, empty in the default local setup); the header variant is shown with each `POST`.
Identifiers and timestamps below differ on every run. Responses are compact JSON; they are
reformatted here.

```bash
BASE=http://localhost:8088
AUTH=(-H "Authorization: Bearer $API_TOKEN")     # only when the server has API_TOKEN set
```

**Start a sync.** `202 Accepted` with a `Location` header for a new update, `200 OK` with the
existing job when the update is already known.

```bash
curl -i -X POST $BASE/api/syncs -H 'Content-Type: application/json' \
  -d '{"update_id":"UPDATE-2026-001"}'
# with a token:
curl -i -X POST $BASE/api/syncs "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d '{"update_id":"UPDATE-2026-001"}'
```

```text
HTTP/1.1 202 Accepted
Location: /api/syncs/4dda37ad-86f1-41ef-9447-62f0b51512a2
Content-Type: application/json

{"job_id":"4dda37ad-86f1-41ef-9447-62f0b51512a2","status":"PENDING","supplier_id":"SUP-001","supplier_update_id":"UPDATE-2026-001", ...}
```

The optional `"supplier_id"` field selects another supplier (default `SUP-001`). Ids may use
`A-Z a-z 0-9 . _ -`, up to 64 characters.

**Follow it.** A job is the unit you poll:

```bash
curl $BASE/api/syncs/4dda37ad-86f1-41ef-9447-62f0b51512a2
curl "$BASE/api/syncs?status=INTERRUPTED&limit=20"     # newest first; limit 1-500 (default 50), offset >= 0
```

```json
{
  "job_id": "4dda37ad-86f1-41ef-9447-62f0b51512a2",
  "status": "COMPLETED",
  "supplier_id": "SUP-001",
  "supplier_update_id": "UPDATE-2026-001",
  "progress": { "processed_batches": 3, "total_batches": 3, "next_cursor": "",
                "last_batch_id": "BATCH-003", "end_of_feed_seen": true },
  "changes": { "created": 5, "updated": 2, "withdrawn": 2 },
  "ignored": { "duplicate": 0, "stale": 0 },
  "open_blocking_issues": 0,
  "last_error": null,
  "created_at": "2026-10-20T15:18:02.553403Z",
  "started_at": "2026-10-20T15:18:02.610623Z",
  "completed_at": "2026-10-20T15:18:02.810894Z",
  "updated_at": "2026-10-20T15:18:02.810894Z"
}
```

`total_batches` is `null` until the supplier says how many batches to expect, and
`next_cursor` is the saved position a resume continues from. `ignored` counts records that were
recognised as repeats or as older revisions and therefore changed nothing.

**What changed, and what went wrong.**

```bash
curl "$BASE/api/syncs/<job_id>/changes?type=WITHDRAWN"   # type: CREATED | UPDATED | WITHDRAWN
curl "$BASE/api/syncs/<job_id>/issues?status=OPEN"       # status: OPEN | RESOLVED; severity: BLOCKING | TRANSIENT
```

Each change carries the product's state `before` and `after` (`before` is `null` for a product
that did not exist). The issues of `UPDATE-2026-004`, whose second batch has a negative price:

```json
{
  "items": [
    { "id": "baaace6e-653f-43a7-9f1f-96e3cad44482", "batch_id": "BATCH-002", "cursor": "cursor-002",
      "supplier_id": null, "product_id": null,
      "code": "INVALID_PRODUCT_DATA", "severity": "BLOCKING", "status": "OPEN",
      "message": "invalid product data: SKU-302: price_minor must be >= 0", "retry_count": 0,
      "first_seen_at": "2026-10-20T15:18:23.489791Z", "last_seen_at": "2026-10-20T15:18:23.489791Z",
      "resolved_at": null }
  ],
  "open_blocking": 1
}
```

`open_blocking` is the job's number of open blocking issues; it ignores the filters and the
paging. It is what keeps a job out of `COMPLETED`.

**Resume a stopped job** (`INTERRUPTED` or `NEEDS_ATTENTION`) from its saved cursor. It answers
`202` with the same job, now `RUNNING`; batches that are already committed are not fetched
again.

```bash
curl -i -X POST $BASE/api/syncs/<job_id>/resume
curl -i -X POST $BASE/api/syncs/<job_id>/resume "${AUTH[@]}"      # with a token
```

**Browse the catalog.** `status` is `ACTIVE` (default), `WITHDRAWN` or `ALL`; `supplier_id` is
optional. Withdrawn products are kept as tombstones and a single product can always be fetched:

```bash
curl "$BASE/api/products?status=ALL&limit=100"
curl $BASE/api/products/SKU-103
```

```json
{ "supplier_id": "SUP-001", "product_id": "SKU-103", "status": "WITHDRAWN", "revision": 2,
  "name": "Camping Lantern", "price_minor": 3200, "currency": "USD",
  "created_at": "2026-10-20T15:18:02.6418Z", "updated_at": "2026-10-20T15:18:02.810894Z",
  "withdrawn_at": "2026-10-20T15:18:02.810894Z" }
```

A product that was withdrawn before the catalog ever saw it (`SKU-900` in
`UPDATE-2026-001`) is a tombstone with `name`, `price_minor` and `currency` set to `null`.

**Errors** always have the form `{"error":{"code":"...","message":"...", ...}}`; extra fields sit
inside the `error` object. For example, resuming a job that is already `COMPLETED`:

```text
HTTP/1.1 409 Conflict
{"error":{"code":"INVALID_STATE","current_status":"COMPLETED","message":"the job is not in a state that allows this operation"}}
```

| Status | `code` | When |
|---|---|---|
| 400 | `INVALID_JSON` | the body is empty, truncated, has an unknown field, a wrong type, trailing data or is over 1 MiB |
| 401 | `UNAUTHORIZED` | `API_TOKEN` is set and a `POST` has no or a wrong bearer token (answered before the body is read; carries `WWW-Authenticate: Bearer`) |
| 404 | `NOT_FOUND` | unknown job or product; a job id that is not a UUID is also a plain 404 |
| 409 | `ACTIVE_JOB_EXISTS` | another job is active; `error.active_job_id` names it when starting |
| 409 | `INVALID_STATE` | the job cannot be resumed (`COMPLETED`, `FAILED`, or `RUNNING` with a live worker); `error.current_status` says which |
| 422 | `VALIDATION_FAILED` | a bad `update_id` or `supplier_id`, `limit` outside 1-500, a negative `offset`, an unknown `status`, `type` or `severity` value |
| 503 | `DB_UNAVAILABLE` | PostgreSQL does not answer, has no free connection, or cancelled the query (timeout, serialization failure, deadlock): trying again shortly can succeed; the message never contains connection details |
| 503 | `SHUTTING_DOWN` | `POST /api/syncs` or `POST /api/syncs/{id}/resume` was handled while the service is stopping; nothing was created or resumed, retry after the restart |

`GET /healthz` is the liveness check described under "Server configuration".

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

| Layer | Where | What it proves |
|---|---|---|
| unit | most packages | revision rules, validation, hashing, retry, job state table, fixture reader, token check |
| repository and processor | `internal/storage`, `internal/catalog`, `internal/syncjob` | SQL, constraints and triggers, one transaction per batch, rollback on failure |
| scenarios | `tests/integration` | whole jobs through the Manager with a fake supplier and injected failures: first import, replays, stale and out-of-order revisions, withdrawal, outage and resume, crash before and after a commit, shutdown, parallel starts and resumes, every fixture's expected outcome |
| HTTP API | `internal/api` | every endpoint and status code over a real listener, the error bodies, the token rule, and that what the change and issue endpoints report equals what is in PostgreSQL |
| end to end | `tests/e2e` | the real server process killed and restarted (below) |

The tests that carry each guarantee from section 1:

| Guarantee | Tests |
|---|---|
| repeats change nothing | `TestReplayOfSameUpdateIsNoOp`, `TestResentRecordsUnderNewUpdateAreDuplicates`, `TestCrashAfterCommitDoesNotDuplicateEffects` |
| revisions only go up | `TestLateLowerRevisionIsIgnored`, `TestHigherRevisionUpdatesProduct` |
| withdrawn stays withdrawn | `TestStaleActiveCannotResurrectWithdrawnProduct`, `TestWithdrawalOfUnseenProductCreatesTombstone` |
| a batch is applied whole or not at all | `TestInvalidRecordLeavesBatchUncommitted`, `TestCrashBeforeCommitLeavesNoPartialEffects`, `TestProcessCrashBeforeCommitRecovers` |
| progress survives failures | `TestSupplierOutageInterruptsAndResumeCompletes`, `TestRestartedManagerResumesFromCheckpoint`, `TestProcessCrashAfterCommitRecovers`, `TestGracefulShutdownInterruptsJob` |
| completed means completed | `TestMissingEndOfFeedNeverCompletes`, `TestOpenBlockingIssuePreventsCompletion` |

### End-to-end tests

```bash
TEST_DATABASE_URL='postgres://catalog:catalog@localhost:55432/catalog_test?sslmode=disable' \
  go test -p 1 -count=1 -v ./tests/e2e/
```

The tests build `cmd/server` into a temporary directory (so `go` must be on the `PATH`), start
it as a child process on a free port against `TEST_DATABASE_URL`, and break it for real, using
`UPDATE-2026-003` (five batches). Each test ends by comparing the job with the fixture's
`expected` block, checking that no product or change row exists twice and that no issue was
recorded, and every child process is killed when its test ends.

- `TestProcessCrashAfterCommitRecovers`: with `CRASH_POINT=after_commit:BATCH-002` the process
  exits with status 3 right after the second batch commits. The database holds two committed
  batches and a job still `RUNNING`. A second process, started without `CRASH_POINT`, marks
  the job `INTERRUPTED` before it opens its port; `POST .../resume` then completes it.
- `TestProcessCrashBeforeCommitRecovers`: the same with `CRASH_POINT=before_commit:BATCH-003`,
  where the process dies with the third batch's transaction open. Nothing of that batch
  exists afterwards, and the resumed job still ends with the same result.
- `TestGracefulShutdownInterruptsJob`: with `SUPPLIER_DELAY_MS=400` the job is mid-run when
  the process gets an interrupt signal. It exits with status 0, its job is `INTERRUPTED` with
  no issue row (a requested stop is not a problem), the next start has nothing to reconcile,
  and a resume completes the job. This test is skipped on Windows, because a console
  interrupt cannot be sent to a child process there; it runs on Linux and macOS and in the
  race container below.

### Data-race run

`-race` needs a C compiler, so it runs in the Debian-based Go image on the compose network (the
database must be up):

```bash
docker run --rm --network catalog-sync_default -v "$PWD":/src -w /src -e CGO_ENABLED=1 \
  -e TEST_DATABASE_URL='postgres://catalog:catalog@db:5432/catalog_test_race?sslmode=disable' \
  golang:1.27 go test -race -p 1 -count=1 ./...
```

On Windows Git Bash, put `MSYS_NO_PATHCONV=1` in front of `docker run` and mount with
`-v "$(pwd -W)":/src`; otherwise Git Bash rewrites `-w /src` and the mount path into Windows
paths and Docker rejects them.

### Smoke check and demo

Both scripts need `bash`, `docker` with Compose and `curl` (no `jq`), and by default run on
their own compose projects, so the stack you started with `docker compose up` is left alone.
The last step of `demo.sh` runs `go test`, so it also needs Go.

```bash
bash scripts/smoke.sh                # project rcs-smoke (ports 18086 / 55436)
API_TOKEN=<token> bash scripts/smoke.sh      # the same with the bearer token required; also checks the 401 answers
DEMO_PAUSE=0 bash scripts/demo.sh    # project rcs-demo (ports 18087 / 55437)
```

`smoke.sh` builds and starts a throwaway stack, runs one update to `COMPLETED`, checks the
status code of every endpoint, then removes the stack and its volume. `demo.sh` is the scripted
walkthrough (normal sync and withdrawal, repeats and late records, a supplier outage, a restart
and resume, an unresolved issue, and finally the test suite); every step asserts the status and
counters its fixture promises and the script stops at the first mismatch. It refuses to start
on a database that already has jobs, because posting a known update returns the old job; reset
its stack with `docker compose -p rcs-demo down -v` (this deletes that stack's data). Set
`DEMO_PAUSE` to the seconds to wait between steps (default 3) when presenting it.

### Check a deployed instance

```bash
bash scripts/remote-check.sh https://<service>.onrender.com     # no token: reads and the 401 check only
read -rs API_TOKEN && export API_TOKEN       # paste the token, press Enter; nothing is echoed
bash scripts/remote-check.sh https://<service>.onrender.com     # with the token: also runs a sync
```

Without a token the script waits up to two minutes for a sleeping instance to answer
`/healthz`, checks that the list endpoints are public and that a `POST` without a token is
refused with `401` and `WWW-Authenticate: Bearer`. With `API_TOKEN` exported in your own shell
it also starts `UPDATE-2026-001`, waits for `COMPLETED` and checks that at least four products
are `ACTIVE`. The token reaches `curl` as a configuration line on standard input (`curl -K -`),
so it never appears on a command line or in the process list; do not put it on the command
line yourself. The script refuses to send the token over plain `http` to anything but
`localhost`. It needs `bash`, `curl`, `grep` and `sed`; it exits non-zero at the first failing
step and names it.

### Reproduce an outage and a restart by hand

Start a clean stack (`docker compose up --build -d`), then, with `BASE=http://localhost:8088`:

```bash
# 1. The supplier goes down at the third batch of UPDATE-2026-003.
SUPPLIER_OUTAGE_CURSORS=cursor-003 docker compose up -d app
curl -X POST $BASE/api/syncs -H 'Content-Type: application/json' -d '{"update_id":"UPDATE-2026-003"}'
curl $BASE/api/syncs/<job_id>                       # a few seconds later: INTERRUPTED after 2 batches
curl "$BASE/api/syncs/<job_id>/issues?status=OPEN"  # SUPPLIER_UNAVAILABLE, retry_count 4, TRANSIENT
curl $BASE/api/products                             # the 4 products of the two committed batches are there

# 2. The supplier is back (the container is recreated without the switch); continue the job.
docker compose up -d app
curl -X POST $BASE/api/syncs/<job_id>/resume        # 202; continues at cursor-003
curl $BASE/api/syncs/<job_id>                       # COMPLETED: 5 batches, 10 created, outage issue RESOLVED
```

The same with a hard crash of the process instead of an outage. Run it on a fresh stack
(`docker compose down -v`, then `docker compose up -d`), because an update that already has a
job is never run again:

```bash
CRASH_POINT=after_commit:BATCH-002 docker compose up -d app
curl -X POST $BASE/api/syncs -H 'Content-Type: application/json' -d '{"update_id":"UPDATE-2026-003"}'
docker compose ps -a app                            # the container exited with status 3
docker compose up -d app                            # recreated WITHOUT CRASH_POINT, or it crashes again
docker compose logs app | grep -E 'fault.crash_point|startup.reconciled'
curl $BASE/api/syncs/<job_id>                       # INTERRUPTED, 2 batches, "the service restarted"
curl -X POST $BASE/api/syncs/<job_id>/resume        # then poll until COMPLETED with 10 created
```

On a server that has `API_TOKEN` set, add `-H "Authorization: Bearer $API_TOKEN"` to the two
`POST` calls. `before_commit:<batch_id>` crashes with that batch's transaction still open.
`docker compose down -v` ends the experiment and deletes its data.

## 5. Assumptions

About the supplier (a fictional one, simulated by the files in `fixtures/`):

- Cursors and batch contents are stable across retries for the same update id. The service
  stores a hash of every committed batch; the same batch id with different content is
  reported as `BATCH_CONFLICT` and stops the job instead of being skipped or re-applied.
- Revisions increase per product (supplier id plus product id), across batches and across
  updates. Arrival order is not authoritative: an old revision that arrives late is ignored.
- `WITHDRAWN` is terminal. A withdrawn product is kept as a tombstone, and a later ACTIVE
  event for it is blocked, not applied. Re-enabling products would need an explicit
  reactivation event and a business policy that does not exist here.
- A withdrawal for a product the catalog has never seen is still stored as a tombstone, with
  no name, price or currency, so an ACTIVE event that arrives for it later is not applied
  (an older revision is ignored, a newer one is blocked).
- Prices are integers in minor units (cents). An ACTIVE product needs a name, a currency and a
  price of zero or more; a withdrawal carries only identity, revision and status.
- The supplier must signal the end of the feed (`has_more` present and false). A missing
  marker or a timeout never counts as success: the job stays out of COMPLETED.
- One sync job is active at a time, across all suppliers, and PostgreSQL enforces it with a
  partial unique index. Starting a different update while one is active answers
  `409 Conflict`.
- The catalog stays readable during a sync and each batch becomes visible when it commits.
  Publication is batch-atomic, not all-or-nothing for the whole supplier update.

About how staff drive the service:

- Resuming is explicit. On every start, before it accepts requests, the service marks jobs it
  finds `PENDING` or `RUNNING` as `INTERRUPTED` (they belonged to a process that is gone), but
  it never continues them by itself. Staff call `POST /api/syncs/{id}/resume`, which continues
  from the saved cursor. The same call retries a `NEEDS_ATTENTION` job after the source data
  was corrected, and takes over a `PENDING` or `RUNNING` job whose worker is gone while the
  service kept running (for example PostgreSQL was unreachable just when the worker tried to
  record why it stopped), so that case needs no restart.
- Posting an update that is already known returns the existing job (`200`, in whatever state
  it is in) and does not start a second one or resume it. Starting and resuming are separate
  actions.
- A withdrawal at a higher revision for an already withdrawn product only advances the
  tombstone's revision, so later stale events are still recognised as stale. It is not a
  catalog change and writes no change row.
- There is no endpoint to mark an issue resolved by hand: it would let staff declare data
  fine that is still wrong. A blocking issue closes automatically when the batch at its
  cursor commits after the supplier fixed the data; if the same problem is seen again, the
  open issue's `retry_count` goes up instead of a duplicate issue appearing.
- Reads are public. When `API_TOKEN` is set, the two `POST` routes need it as a bearer token;
  there are no user accounts or roles.

## 6. Limitations and future work

Left out on purpose to keep the scope to one day, with what would come next:

| Today | Next step |
|---|---|
| modular monolith with an in-process worker and all job state in PostgreSQL | a separate worker process fed by a queue, with leases |
| one active sync job for the whole service | several suppliers and jobs with coordination between them |
| every batch commits on its own, so the catalog changes while a sync runs | stage the whole update and publish it atomically |
| `WITHDRAWN` is final | an explicit reactivation event with a revision and approval policy |
| a fixture supplier that reads local JSON files | authenticated HTTP integrations with real suppliers |
| API only, driven with curl | a staff dashboard |
| Docker Compose locally and a free Render deployment | CI/CD, monitoring and alerting |

There is no scheduler either: nothing starts or resumes a sync unless someone calls the API.

Known limits to keep in mind:

- **Single instance only.** The start-up step that marks `PENDING`/`RUNNING` jobs as
  `INTERRUPTED` assumes no other copy of the service is running. With two replicas it would
  interrupt a job the other replica is still working on. Starting a job is guarded by the
  database itself (the one-active-job index), but "one worker per job" on resume relies on the
  process's own lock and its set of live workers, so it holds only with a single instance. The
  upgrade path for both is a lease or heartbeat on the job row.
- **Insert race left to the primary key.** When a product does not exist yet, two writers
  could both try to insert it; the primary key makes the second one fail loudly instead of
  overwriting. Under the single-active-job rule that cannot happen, so no extra locking was
  added; running jobs in parallel would need it.
- **A supplier that breaks the contract.** Without a definite end-of-feed marker the service
  cannot report a sync as completed. If cursors or batch contents are not stable across
  retries, a resume could repeat or skip records: repeats are harmless (same revision, same
  content is a duplicate), but a skipped record is only corrected when the supplier sends it
  again. The service detects a batch id that comes back with different content and stops;
  it cannot repair the supplier.
- **Render free tier.** The web service sleeps after 15 minutes without traffic and the
  first request afterwards takes about a minute. The free PostgreSQL database expires 30 days
  after it is created (14 days of grace to upgrade, then it is deleted), holds 1 GB and has no
  backups. See "Deploy to Render".
- **A Render deploy overlaps two instances.** While Render replaces the service, the old and
  the new instance run side by side for a short time. The new instance's start-up step
  interrupts a sync that is running; the old worker stops at its next batch without writing
  anything, because it finds the job no longer `RUNNING`. Resume the job with
  `POST /api/syncs/{id}/resume` once the deploy has finished and the old instance is gone (a
  resume while the old worker is still alive would put two workers on one job).
- **Issues are matched to a batch by cursor.** A blocking issue closes when the batch at its
  cursor commits, and recording an issue does not check that the cursor is still the job's
  current position. With one instance only the job's own worker moves the cursor, so that
  holds. With several instances a stale worker could record an issue at a cursor the job has
  already passed; it would never close and the job could not complete. Multi-instance use would
  need a cursor check when an issue is recorded.
- **The manager's lock covers short database calls.** Starting or resuming a job takes the
  manager's lock and runs a few quick queries while holding it. That is simple and enough for
  one active job, but a database that stops answering makes other start and resume requests
  wait behind it.
- **`retry_count` has two meanings.** On the first sighting of an issue it is the number of
  attempts made (for example 4 for a supplier outage, 0 for a rejected batch); each later
  recurrence of the same problem adds 1.
- **Error text is public.** Issue `message` and job `last_error` can contain PostgreSQL's own
  error text (constraint or trigger names, error codes), and the `GET` routes are public. The
  sensitivity is low, but a deployment that cares should restrict the read routes too.

## 7. AI disclosure

This project was built with AI assistance. Exactly how:

- Requirements: the one-day blueprint was brainstormed by the author with an AI assistant and
  reviewed by the author.
- Planning: the execution plan (architecture, schema, test plan, task breakdown, risk register)
  was produced with Claude and reviewed by the author. These planning documents are kept out of
  this repository by the author's choice.
- Implementation: the Go code, SQL migration, tests, scripts, Docker/Render configuration and
  this README were written by Claude Code agents (one lead agent coordinating three parallel
  agents) following that plan.
- Decisions by the author: repository name and contents, demo video plus Render deployment,
  optional API token on write routes, explicit resume after restart, interviewer-readable commit
  messages, project-local Go toolchain.
- Verification: every result quoted here comes from running the commands in this repository
  against real PostgreSQL; an AI code reviewer and the author reviewed the code.
- The author is responsible for this submission.

## 8. Demo video and deployment

(Links to the demo video and the deployed service are added once they exist.)

### Deploy to Render

[`render.yaml`](render.yaml) describes the whole deployment: a Docker web service built from the
`Dockerfile` on Render's free plan, checked at `/healthz`, and a free managed PostgreSQL 17
database that only the web service can reach. The database connection string, a generated
`API_TOKEN` and `SUPPLIER_ID=SUP-001` are wired in as environment variables; the server listens
on the `PORT` Render provides. You need a Render account and the repository on GitHub.

1. In the Render dashboard choose **New**, then **Blueprint**, and connect the GitHub
   repository.
2. Keep the default Blueprint path (`render.yaml` in the repository root), pick the branch,
   review the two resources Render lists, and click **Deploy Blueprint**.
3. Wait for the database and the web service to finish deploying; the first build compiles
   the Go server inside Docker, so allow several minutes. The service URL
   (`https://<service>.onrender.com`) is shown at the top of the service page. Check
   `<service URL>/healthz`.
4. Read the generated token in the dashboard: open the web service, then **Environment**, then
   **Environment Variables**, and find `API_TOKEN`. Reads need no token; the two `POST` routes
   do.
5. Use the token from your own terminal without typing it into the command line:

   ```bash
   read -rs API_TOKEN && export API_TOKEN      # paste the token, press Enter; nothing is echoed
   curl -X POST "https://<service>.onrender.com/api/syncs" \
     -H "Authorization: Bearer $API_TOKEN" -H "Content-Type: application/json" \
     -d '{"update_id":"UPDATE-2026-001"}'
   ```

   A `POST` without the header, or with a wrong token, answers `401`. To run the whole
   sequence as a script that keeps the token off the command line, see "Check a deployed
   instance" under section 4.

Things to know:

- **Never paste the token** into an issue, a chat, a commit or a screenshot. If it leaks,
  change its value on the same Environment page.
- **Cold start.** The free web service goes to sleep after 15 minutes without traffic, and the
  first request afterwards takes about a minute to be answered; retry if it times out. Open
  `/healthz` a minute before a demo.
- **The free database expires 30 days after it is created.** After that there are 14 days to
  upgrade it before it is deleted; it has 1 GB of storage and no backups. Only one free
  database can be active per Render workspace, so a workspace that already has one cannot
  create this one.
- **Redeploys.** A new Render service deploys on every push to the linked branch.
- The deployed service reads the same JSON files from `fixtures/` as the local one, and none of
  the demo failure switches (`SUPPLIER_OUTAGE_CURSORS`, `SUPPLIER_DELAY_MS`, `CRASH_POINT`) are
  set there.
- To open the database from your own machine, add your IP address to `ipAllowList` in
  `render.yaml` (or to the database's access rules in the dashboard); by default only the web
  service can connect.
