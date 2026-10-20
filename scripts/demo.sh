#!/usr/bin/env bash
# Scripted walkthrough of the catalog sync service, used for the demo video and as a check:
# every step asserts the status and counters the matching fixture promises, and the script
# exits non-zero naming the step that failed.
#
# It runs on its own compose project with its own ports and refuses to start unless that
# project holds no sync jobs yet (re-posting a known update returns the old job, so a used
# database would hide the first run). Reset a used demo project with:
#   docker compose -p rcs-demo down -v        # WARNING: deletes that project's database volume
#
# Settings (environment):
#   DEMO_PAUSE            seconds to wait between steps (default 3; 0 for unattended runs)
#   COMPOSE_PROJECT_NAME  default rcs-demo; never the shared catalog-sync project
#   APP_PORT, DB_PORT     default 18087 and 55437
#   API_TOKEN             if set, sent as "Authorization: Bearer ..." on POST requests (never printed)
#   DEMO_REMOTE_URL       optional, e.g. a deployed base URL: adds a read-only check of that service
#   TEST_DATABASE_URL     used by the final test run; default: a catalog_test database on the demo PostgreSQL
set -euo pipefail

cd "$(dirname "$0")/.."

DEMO_PAUSE="${DEMO_PAUSE:-3}"
POLL_TIMEOUT=30
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-rcs-demo}"
export APP_PORT="${APP_PORT:-18087}"
export DB_PORT="${DB_PORT:-55437}"
# The app must run with the token this script sends, and with no faults unless a step asks
# for one, whatever .env or the caller's shell holds.
export API_TOKEN="${API_TOKEN-}"
export SUPPLIER_OUTAGE_CURSORS= SUPPLIER_DELAY_MS=0 CRASH_POINT=
BASE="http://localhost:$APP_PORT"

STEP="startup"
STEPS=0
STACK_STARTED=0
STATUS=""
BODY=""

fail() {
  {
    echo
    echo "DEMO FAILED at step: $STEP"
    echo "  $*"
    if [ "$STACK_STARTED" = 1 ]; then
      echo "  The demo stack is left running for inspection: docker compose -p $COMPOSE_PROJECT_NAME logs app"
    fi
  } >&2
  exit 1
}

# Every compose call is pinned to the demo project, so the shared dev project cannot be touched.
dc() { docker compose -p "$COMPOSE_PROJECT_NAME" "$@" || fail "docker compose $* failed"; }

step() {
  if [ "$STEPS" -gt 0 ]; then
    [ "$DEMO_PAUSE" = 0 ] || sleep "$DEMO_PAUSE"
  fi
  STEPS=$((STEPS + 1))
  STEP="$STEPS. $1"
  echo
  echo "=== $STEP ==="
}

# --- JSON without jq: strip whitespace, then pick the first "key":value pair ----------------
jstr() { printf '%s' "$BODY" | tr -d ' \t\r\n' | { grep -Eo "\"$1\":\"[^\"]*\"" || true; } | sed -n '1{s/^[^:]*:"//;s/"$//;p;}'; }
jnum() { printf '%s' "$BODY" | tr -d ' \t\r\n' | { grep -Eo "\"$1\":-?[0-9]+" || true; } | sed -n '1{s/^[^:]*://;p;}'; }
# Number of times the regex matches in the compacted body (one list item per match).
jcount() { printf '%s' "$BODY" | tr -d ' \t\r\n' | { grep -Eo "$1" || true; } | wc -l | tr -d ' '; }

expect() { # expect WHAT ACTUAL EXPECTED
  [ "$2" = "$3" ] || fail "$1: expected '$3', got '$2'"
  echo "   ok  $1 = $3"
}

# --- HTTP ------------------------------------------------------------------------------------
# request METHOD PATH [JSON_BODY] sets STATUS and BODY. POSTs carry the token when API_TOKEN is set.
request() {
  local method=$1 path=$2 data=${3-} out
  local args=(-sS --max-time 30 -X "$method" -w '\n%{http_code}')
  if [ "$method" = POST ]; then
    args+=(-H 'Content-Type: application/json' -d "$data")
    if [ -n "$API_TOKEN" ]; then
      args+=(-H "Authorization: Bearer $API_TOKEN")
    fi
  fi
  out=$(curl "${args[@]}" "$BASE$path") || fail "cannot reach $BASE$path (curl exit $?)"
  STATUS=${out##*$'\n'}
  BODY=${out%$'\n'*}
}

show() { echo "<< $STATUS $(printf '%s' "$BODY" | tr -d '\r\n' | cut -c1-600)"; }

call() { # call METHOD PATH [JSON_BODY]: request, narrate, show
  echo ">> $1 $2${3:+ $3}"
  request "$@"
  show
}

wait_healthy() {
  local deadline=$((SECONDS + 90))
  until curl -fsS -o /dev/null --max-time 2 "$BASE/healthz" 2>/dev/null; do
    [ "$SECONDS" -lt "$deadline" ] || fail "the API did not answer $BASE/healthz within 90s"
    sleep 1
  done
}

wait_status() { # wait_status JOB_ID WANTED_STATUS: poll until the job reaches it
  local id=$1 want=$2 deadline=$((SECONDS + POLL_TIMEOUT)) now
  echo ">> waiting for job $id to become $want"
  while :; do
    request GET "/api/syncs/$id"
    [ "$STATUS" = 200 ] || fail "GET /api/syncs/$id answered $STATUS"
    now=$(jstr status)
    if [ "$now" = "$want" ]; then
      break
    fi
    case "$now" in
      COMPLETED | FAILED | NEEDS_ATTENTION | INTERRUPTED) fail "job $id stopped as $now, expected $want" ;;
    esac
    [ "$SECONDS" -lt "$deadline" ] || fail "job $id is still $now after ${POLL_TIMEOUT}s, expected $want"
    sleep 0.5
  done
  show
}

start_update() { # start_update UPDATE_ID: POST a new update and expect 202; sets JOB_ID
  call POST /api/syncs "{\"update_id\":\"$1\"}"
  expect "start $1 answers" "$STATUS" 202
  JOB_ID=$(jstr job_id)
  [ -n "$JOB_ID" ] || fail "no job_id in the response to start $1"
}

expect_counters() { # expect_counters CREATED UPDATED WITHDRAWN DUPLICATE STALE (the fixture's expected block)
  expect "created" "$(jnum created)" "$1"
  expect "updated" "$(jnum updated)" "$2"
  expect "withdrawn" "$(jnum withdrawn)" "$3"
  expect "duplicate" "$(jnum duplicate)" "$4"
  expect "stale" "$(jnum stale)" "$5"
}

# --- prerequisites ---------------------------------------------------------------------------
for tool in docker curl; do
  command -v "$tool" >/dev/null 2>&1 || fail "$tool is required but was not found on PATH"
done
if [ "$COMPOSE_PROJECT_NAME" = catalog-sync ]; then
  fail "COMPOSE_PROJECT_NAME=catalog-sync is the shared dev project; the demo needs its own (default rcs-demo)"
fi

# --- 1. start ----------------------------------------------------------------------------------
step "Start the stack on a fresh database"
echo "\$ docker compose up --build -d   (project $COMPOSE_PROJECT_NAME, API on $BASE)"
STACK_STARTED=1
dc up --build -d
echo "\$ docker compose ps"
dc ps
wait_healthy
call GET /healthz
expect "healthz status" "$STATUS" 200
expect "healthz body" "$(jstr status)" ok
call GET /api/syncs
[ "$STATUS" = 200 ] || fail "GET /api/syncs answered $STATUS, expected 200"
if [ "$(jcount '"job_id"')" != 0 ]; then
  fail "refusing to run: project $COMPOSE_PROJECT_NAME already holds sync jobs. Reset it with: docker compose -p $COMPOSE_PROJECT_NAME down -v"
fi
echo "   ok  no sync jobs yet"
LOGS=$(dc logs --no-color app 2>&1)
echo "\$ docker compose logs app | grep migrations.applied"
grep migrations.applied <<<"$LOGS" || fail "no migrations.applied event in the app logs"

# --- 2. normal sync and withdrawal --------------------------------------------------------------
step "Normal sync and withdrawal (UPDATE-2026-001)"
start_update UPDATE-2026-001
JOB_001=$JOB_ID
wait_status "$JOB_001" COMPLETED
expect_counters 5 2 2 0 0
call GET /api/products
expect "products status" "$STATUS" 200
expect "active products" "$(jcount '"status":"ACTIVE"')" 4
expect "withdrawn products listed by default" "$(jcount '"status":"WITHDRAWN"')" 0
call GET /api/products/SKU-103
expect "SKU-103 status" "$(jstr status)" WITHDRAWN
call GET /api/products/SKU-900
expect "SKU-900 status (withdrawal of a product never seen)" "$(jstr status)" WITHDRAWN
expect "SKU-900 name strings" "$(jcount '"name":"')" 0

# --- 3. duplicates and out-of-order revisions ----------------------------------------------------
step "Repeated update, late and duplicate records (UPDATE-2026-002)"
call POST /api/syncs '{"update_id":"UPDATE-2026-001"}'
expect "re-posting a known update answers" "$STATUS" 200
expect "re-posting returns the same job" "$(jstr job_id)" "$JOB_001"
start_update UPDATE-2026-002
JOB_002=$JOB_ID
wait_status "$JOB_002" COMPLETED
expect_counters 1 2 0 3 2
call GET "/api/syncs/$JOB_002/changes"
expect "changes written (created + updated)" "$(jcount '"change_type"')" 3
call GET /api/products/SKU-103
expect "SKU-103 stays" "$(jstr status)" WITHDRAWN

# --- 4. supplier failure ---------------------------------------------------------------------------
step "Supplier outage after two batches (UPDATE-2026-003)"
echo "\$ SUPPLIER_OUTAGE_CURSORS=cursor-003 docker compose up -d app"
SUPPLIER_OUTAGE_CURSORS=cursor-003 dc up -d app
wait_healthy
start_update UPDATE-2026-003
JOB_003=$JOB_ID
wait_status "$JOB_003" INTERRUPTED
expect "batches committed before the outage" "$(jnum processed_batches)" 2
expect "checkpoint cursor" "$(jstr next_cursor)" cursor-003
expect "created so far" "$(jnum created)" 4
call GET "/api/syncs/$JOB_003/issues?status=OPEN"
expect "open issue" "$(jstr code)" SUPPLIER_UNAVAILABLE
expect "open issues" "$(jcount '"code"')" 1
expect "supplier attempts" "$(jnum retry_count)" 4
call GET /api/products
expect "active products (the two committed batches are visible)" "$(jcount '"status":"ACTIVE"')" 9

# --- 5. restart and resume -------------------------------------------------------------------------
step "Application restart and resume"
echo "\$ docker compose up -d app   (supplier healthy again, the app container is recreated)"
dc up -d app
wait_healthy
echo "\$ docker compose logs app | grep startup.reconciled"
LOGS=$(dc logs --no-color app 2>&1)
grep startup.reconciled <<<"$LOGS" || fail "no startup.reconciled event in the app logs after the restart"
call GET "/api/syncs/$JOB_003"
expect "job survived the restart as" "$(jstr status)" INTERRUPTED
call POST "/api/syncs/$JOB_003/resume" ''
expect "resume answers" "$STATUS" 202
wait_status "$JOB_003" COMPLETED
expect "batches processed in total" "$(jnum processed_batches)" 5
expect_counters 10 0 0 0 0
call GET "/api/syncs/$JOB_003/issues?status=OPEN"
expect "open issues after resume" "$(jcount '"code"')" 0
call GET "/api/syncs/$JOB_003/issues?status=RESOLVED"
expect "resolved issue" "$(jstr code)" SUPPLIER_UNAVAILABLE
call GET "/api/syncs/$JOB_003/changes?limit=500"
expect "changes (no duplicates after resume)" "$(jcount '"change_type"')" 10

# --- 6. history, changes, unresolved issues --------------------------------------------------------
step "Job history, change log and an unresolved issue (UPDATE-2026-004)"
call GET /api/syncs
expect "jobs listed" "$(jcount '"job_id"')" 3
call GET "/api/syncs/$JOB_001/changes?type=WITHDRAWN"
expect "withdrawal changes of the first update" "$(jcount '"change_type":"WITHDRAWN"')" 2
start_update UPDATE-2026-004
wait_status "$JOB_ID" NEEDS_ATTENTION
expect "created before the bad batch" "$(jnum created)" 1
expect "batches committed" "$(jnum processed_batches)" 1
expect "checkpoint stays at the bad batch" "$(jstr next_cursor)" cursor-002
expect "open blocking issues" "$(jnum open_blocking_issues)" 1
call GET "/api/syncs/$JOB_ID/issues?status=OPEN"
expect "issue code" "$(jstr code)" INVALID_PRODUCT_DATA
expect "issue severity" "$(jstr severity)" BLOCKING

# --- 7. deployed service (optional, read-only) -------------------------------------------------------
if [ -n "${DEMO_REMOTE_URL-}" ]; then
  step "Deployed service"
  REMOTE=${DEMO_REMOTE_URL%/}
  remote_expect() { # remote_expect METHOD PATH EXPECTED_STATUS [JSON_BODY]; the local token is never sent to a remote host
    local code args=(-sS -o /dev/null -w '%{http_code}' --max-time 120 -X "$1")
    if [ "$1" = POST ]; then
      args+=(-H 'Content-Type: application/json' -d "${4-}")
    fi
    echo ">> $1 $REMOTE$2"
    code=$(curl "${args[@]}" "$REMOTE$2") || fail "cannot reach $REMOTE$2 (curl exit $?)"
    expect "$1 $2" "$code" "$3"
  }
  remote_expect GET /healthz 200
  remote_expect GET /api/syncs 200
  remote_expect POST /api/syncs 401 '{"update_id":"UPDATE-2026-001"}'
fi

# --- 8. tests ----------------------------------------------------------------------------------------------
step "Tests"
TESTS="all packages ok"
if command -v go >/dev/null 2>&1; then
  export TEST_DATABASE_URL="${TEST_DATABASE_URL:-postgres://catalog:catalog@localhost:$DB_PORT/catalog_test?sslmode=disable}"
  echo '$ go test -p 1 -count=1 ./...'
  rc=0
  OUT=$(go test -p 1 -count=1 ./... 2>&1) || rc=$?
  grep -E '^(ok|FAIL|--- FAIL|panic)' <<<"$OUT" || true
  [ "$rc" -eq 0 ] || fail "go test exited with $rc"
else
  TESTS="test run skipped: go not found on PATH"
  echo "go is not on PATH, so the test run was skipped. With Go 1.27 installed run: go test -p 1 -count=1 ./..."
fi

echo
echo "DEMO PASSED: $STEPS steps ($TESTS)."
echo "Stop and delete the demo stack with: docker compose -p $COMPOSE_PROJECT_NAME down -v"
