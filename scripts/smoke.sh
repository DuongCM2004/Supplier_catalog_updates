#!/usr/bin/env bash
# Contract check for the HTTP API on a throwaway compose stack.
#
# Builds and starts the app and PostgreSQL under their OWN compose project, runs one
# supplier update through to COMPLETED, asserts the status code of every endpoint, then
# deletes the stack and its volume. It never touches the shared development project.
#
# Needs: bash, docker (with compose), curl, grep, sed. No jq.
#
#   scripts/smoke.sh
#   COMPOSE_PROJECT_NAME=my-smoke APP_PORT=18090 DB_PORT=55440 scripts/smoke.sh
#   API_TOKEN=... scripts/smoke.sh      # app then requires the bearer token on POST routes
#
# Exits non-zero at the first check that does not hold and names that check.
set -euo pipefail

cd "$(dirname "$0")/.."

PROJECT="${COMPOSE_PROJECT_NAME:-rcs-smoke}"
APP_PORT="${APP_PORT:-18086}"
DB_PORT="${DB_PORT:-55436}"
BASE="http://127.0.0.1:$APP_PORT"
HEALTH_WAIT_SECONDS=60
JOB_WAIT_SECONDS=60

# The cleanup below runs `down -v`, which deletes the database volume of the project.
# The shared development project must never be reachable from here.
if [ "$PROJECT" = "catalog-sync" ]; then
  echo "FAIL setup: refusing to run against the shared project 'catalog-sync'; this script deletes its project's volume on exit" >&2
  exit 2
fi

# Compose reads these for the app and database. Exported (even when empty) so values in the
# caller's .env cannot change what is tested: faults off, and the token the app enforces is
# exactly the one this script sends.
export APP_PORT DB_PORT
export API_TOKEN="${API_TOKEN:-}"
export SUPPLIER_OUTAGE_CURSORS=""
export CRASH_POINT=""
# Each supplier fetch pauses, so a started job stays RUNNING for about a second and a half;
# that window is what lets the second start below be rejected as ACTIVE_JOB_EXISTS.
export SUPPLIER_DELAY_MS=500

dc() { docker compose -p "$PROJECT" "$@"; }

TMP="$(mktemp -d)"
BODY_F="$TMP/body"
HDR_F="$TMP/headers"
CODE=""
SEND_AUTH=1

cleanup() {
  local status=$?
  trap - EXIT
  if [ "$status" -ne 0 ]; then
    echo "--- last app log lines ($PROJECT) ---" >&2
    dc logs --no-color --tail 30 app >&2 2>&1 || true
  fi
  echo "removing compose project $PROJECT (containers, network, volume)"
  dc down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$TMP"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

fail() { echo "FAIL $*" >&2; exit 1; }

# req METHOD PATH [JSON_BODY]: sets CODE, and writes the response to $BODY_F / $HDR_F.
# The bearer token goes on POST requests only, and only when API_TOKEN is set.
req() {
  local method=$1 path=$2 data=${3:-}
  set -- -s --max-time 15 -D "$HDR_F" -o "$BODY_F" -w '%{http_code}' -X "$method"
  if [ -n "$data" ]; then
    set -- "$@" -H 'Content-Type: application/json' -d "$data"
  fi
  if [ "$method" = POST ] && [ "$SEND_AUTH" = 1 ] && [ -n "$API_TOKEN" ]; then
    set -- "$@" -H "Authorization: Bearer $API_TOKEN"
  fi
  CODE=$(curl "$@" "$BASE$path") || fail "$method $path: curl could not complete the request"
}

# check STEP WANT_CODE METHOD PATH [JSON_BODY]
check() {
  local step=$1 want=$2
  shift 2
  req "$@"
  [ "$CODE" = "$want" ] || fail "$step: expected HTTP $want, got $CODE: $(head -c 300 "$BODY_F")"
  echo "ok   $step (HTTP $CODE)"
}

# expect STEP REGEX: the last response body must match; expect_not: it must not.
expect() {
  grep -Eq -- "$2" "$BODY_F" || fail "$1: body does not match /$2/: $(head -c 300 "$BODY_F")"
  echo "ok   $1"
}
expect_not() {
  if grep -Eq -- "$2" "$BODY_F"; then fail "$1: body unexpectedly matches /$2/: $(head -c 300 "$BODY_F")"; fi
  echo "ok   $1"
}
expect_header() {
  grep -Eiq -- "$2" "$HDR_F" || fail "$1: response headers do not match /$2/: $(tr -d '\r' <"$HDR_F" | head -n 8)"
  echo "ok   $1"
}

# JSON without jq: the first string / integer value of "key" in the last response body.
json_str() { { grep -Eo "\"$1\": *\"[^\"]*\"" "$BODY_F" | head -n 1 | sed 's/^[^:]*: *"//; s/"$//'; } || true; }
json_num() { { grep -Eo "\"$1\": *-?[0-9]+" "$BODY_F" | head -n 1 | sed 's/^[^:]*: *//'; } || true; }
count_matches() { { grep -Eo -- "$1" "$BODY_F" || true; } | wc -l | tr -d ' '; }

expect_num() { # step key want
  local got
  got=$(json_num "$2")
  [ "$got" = "$3" ] || fail "$1: \"$2\" is '${got:-missing}', expected $3: $(head -c 300 "$BODY_F")"
  echo "ok   $1 ($2=$3)"
}

# wait_completed STEP JOB_ID SECONDS: poll the job until COMPLETED. Any state that needs a
# human or a resume ends the wait at once instead of burning the whole timeout.
wait_completed() {
  local step=$1 id=$2 limit=$3 started=$SECONDS state=""
  while [ $((SECONDS - started)) -lt "$limit" ]; do
    req GET "/api/syncs/$id"
    [ "$CODE" = 200 ] || fail "$step: GET /api/syncs/$id returned HTTP $CODE"
    state=$(json_str status)
    case "$state" in
      COMPLETED) echo "ok   $step (COMPLETED after $((SECONDS - started))s)"; return 0 ;;
      NEEDS_ATTENTION | FAILED | INTERRUPTED)
        fail "$step: job ended as $state (last_error: $(json_str last_error))" ;;
    esac
    sleep 1
  done
  fail "$step: job still ${state:-unknown} after ${limit}s"
}

for tool in docker curl grep sed; do
  command -v "$tool" >/dev/null 2>&1 || fail "setup: '$tool' is not installed"
done
docker info >/dev/null 2>&1 || fail "setup: the Docker daemon is not reachable"

echo "== compose project $PROJECT (app $BASE, database port $DB_PORT)"
# A volume left over from an aborted run would make the first start answer 200, not 202.
dc down -v --remove-orphans >/dev/null 2>&1 || true
dc up --build -d || fail "compose up: the stack did not start"

started=$SECONDS
until [ "$(curl -s -o /dev/null --max-time 2 -w '%{http_code}' "$BASE/healthz" || true)" = 200 ]; do
  [ $((SECONDS - started)) -lt "$HEALTH_WAIT_SECONDS" ] || fail "healthz wait: no HTTP 200 from $BASE/healthz within ${HEALTH_WAIT_SECONDS}s"
  sleep 1
done
echo "ok   healthz wait (up after $((SECONDS - started))s)"

echo "== health and empty reads"
check "health" 200 GET /healthz
expect "health reports database ok" '"database": *"ok"'
check "list jobs on empty database" 200 GET /api/syncs
check "list products on empty database" 200 GET /api/products

echo "== rejected requests"
check "start with invalid JSON" 400 POST /api/syncs '{"update_id":'
expect "invalid JSON error code" '"code": *"INVALID_JSON"'
check "start without update_id" 422 POST /api/syncs '{}'
expect "validation error code" '"code": *"VALIDATION_FAILED"'
check "start with malformed update_id" 422 POST /api/syncs '{"update_id":"bad id!"}'
check "list jobs limit 0" 422 GET '/api/syncs?limit=0'
check "list jobs limit 501" 422 GET '/api/syncs?limit=501'
check "list jobs limit not a number" 422 GET '/api/syncs?limit=abc'
check "list jobs negative offset" 422 GET '/api/syncs?offset=-1'
check "list products limit 0" 422 GET '/api/products?limit=0'
check "unknown job id" 404 GET /api/syncs/00000000-0000-4000-8000-000000000000
expect "not found error code" '"code": *"NOT_FOUND"'
check "malformed job id" 404 GET /api/syncs/not-a-uuid
check "changes of unknown job" 404 GET /api/syncs/00000000-0000-4000-8000-000000000000/changes
check "issues of unknown job" 404 GET /api/syncs/00000000-0000-4000-8000-000000000000/issues
check "resume unknown job" 404 POST /api/syncs/00000000-0000-4000-8000-000000000000/resume
check "resume malformed job id" 404 POST /api/syncs/not-a-uuid/resume
check "unknown product" 404 GET /api/products/SKU-101

if [ -n "$API_TOKEN" ]; then
  SEND_AUTH=0
  check "start without token" 401 POST /api/syncs '{"update_id":"UPDATE-2026-001"}'
  expect_header "start without token offers bearer auth" '^www-authenticate: *bearer'
  check "resume without token" 401 POST /api/syncs/00000000-0000-4000-8000-000000000000/resume
  SEND_AUTH=1
else
  echo "skip start/resume without token -> 401 (API_TOKEN is not set, auth is off)"
fi

echo "== start a sync"
check "start UPDATE-2026-001" 202 POST /api/syncs '{"update_id":"UPDATE-2026-001"}'
JOB=$(json_str job_id)
[ -n "$JOB" ] || fail "start UPDATE-2026-001: no job_id in response: $(head -c 300 "$BODY_F")"
expect_header "start returns Location of the job" "^location:.*$JOB"

# The job is still inside its first fetch pause here.
check "start another update while a job runs" 409 POST /api/syncs '{"update_id":"UPDATE-2026-003"}'
expect "conflict error code" '"code": *"ACTIVE_JOB_EXISTS"'
expect "conflict names the running job" "\"active_job_id\": *\"$JOB\""
check "resume a job whose worker is alive" 409 POST "/api/syncs/$JOB/resume"
expect "resume error code" '"code": *"INVALID_STATE"'

wait_completed "job reaches COMPLETED" "$JOB" "$JOB_WAIT_SECONDS"
expect_num "completed job: batches processed" processed_batches 3
expect_num "completed job: created" created 5
expect_num "completed job: updated" updated 2
expect_num "completed job: withdrawn" withdrawn 2
expect_num "completed job: open blocking issues" open_blocking_issues 0

echo "== finished job"
check "start the same update again" 200 POST /api/syncs '{"update_id":"UPDATE-2026-001"}'
expect "same update returns the same job" "\"job_id\": *\"$JOB\""
check "resume a COMPLETED job" 409 POST "/api/syncs/$JOB/resume"
expect "resume error code" '"code": *"INVALID_STATE"'
expect "resume reports current status" '"current_status": *"COMPLETED"'

check "list jobs" 200 GET /api/syncs
expect "list contains the job" "\"job_id\": *\"$JOB\""
# The rejected second start must not have left a job behind.
[ "$(count_matches '"job_id"')" = 1 ] || fail "list jobs: expected exactly 1 job, found $(count_matches '"job_id"')"
echo "ok   list holds exactly one job"
check "list jobs filtered and paged" 200 GET '/api/syncs?status=COMPLETED&limit=1&offset=0'
check "job changes" 200 GET "/api/syncs/$JOB/changes"
expect "changes include creations" '"change_type": *"CREATED"'
check "job changes of one type" 200 GET "/api/syncs/$JOB/changes?type=WITHDRAWN"
expect "changes filter keeps withdrawals" '"change_type": *"WITHDRAWN"'
expect_not "changes filter drops creations" '"change_type": *"CREATED"'
check "job changes unknown type" 422 GET "/api/syncs/$JOB/changes?type=BOGUS"
check "job changes limit 0" 422 GET "/api/syncs/$JOB/changes?limit=0"
check "job issues" 200 GET "/api/syncs/$JOB/issues"
check "job issues open only" 200 GET "/api/syncs/$JOB/issues?status=OPEN"
check "job issues unknown status" 422 GET "/api/syncs/$JOB/issues?status=BOGUS"

echo "== catalog"
check "products default to ACTIVE" 200 GET /api/products
[ "$(count_matches '"status": *"ACTIVE"')" = 4 ] || fail "products default to ACTIVE: expected 4 active products, found $(count_matches '"status": *"ACTIVE"'): $(head -c 300 "$BODY_F")"
echo "ok   products default holds 4 ACTIVE items"
expect_not "products default hides withdrawn" '"status": *"WITHDRAWN"'
check "products of one supplier" 200 GET '/api/products?supplier_id=SUP-001'
check "products including withdrawn" 200 GET '/api/products?status=ALL'
expect "ALL includes withdrawn products" '"status": *"WITHDRAWN"'
check "products withdrawn only" 200 GET '/api/products?status=WITHDRAWN'
expect_not "WITHDRAWN hides active products" '"status": *"ACTIVE"'
check "products unknown status" 422 GET '/api/products?status=BOGUS'
check "product by id" 200 GET /api/products/SKU-101
check "withdrawn product by id" 200 GET /api/products/SKU-103
expect "withdrawn product keeps its tombstone" '"status": *"WITHDRAWN"'
check "tombstone of a never-seen product" 200 GET /api/products/SKU-900
expect "unseen product is a tombstone" '"status": *"WITHDRAWN"'
check "product by id and supplier" 200 GET '/api/products/SKU-101?supplier_id=SUP-001'
check "unknown product id" 404 GET /api/products/SKU-NOPE
check "product of another supplier" 404 GET '/api/products/SKU-101?supplier_id=SUP-999'

check "health at the end" 200 GET /healthz

echo "PASS all contract checks against $BASE (project $PROJECT)"
