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
starts listening. On SIGINT or SIGTERM the server stops accepting connections, lets requests
in flight finish (up to 10 seconds), stops the sync workers (a running job is saved as
`INTERRUPTED` with no issue recorded and can be resumed; up to 10 more seconds) and closes the
database pool.

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
  interrupt a job the other replica is still working on. The upgrade path is a lease or
  heartbeat on each running job.
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

## 7. AI disclosure

(Wording drafted separately and confirmed by the author before submission.)

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

   A `POST` without the header, or with a wrong token, answers `401`.

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
