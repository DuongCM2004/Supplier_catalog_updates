#!/usr/bin/env bash
# Black-box check of a running (typically deployed) instance.
#
#   scripts/remote-check.sh https://your-service.example.com
#
# Without a token it checks that the service is up, that reads are public and that writes
# are closed. To also run a real sync end to end, put the write token in the environment
# of your shell. Never put it on a command line, where it lands in history and process
# lists:
#
#   read -rs API_TOKEN && export API_TOKEN
#
# Needs: bash, curl, grep, sed. No jq. Exits non-zero at the first failing step and names it.
set -euo pipefail

if [ $# -ne 1 ] || [ -z "$1" ]; then
  echo "usage: $0 BASE_URL    e.g. $0 https://your-service.example.com" >&2
  echo "       export API_TOKEN first to also run the authenticated sync steps" >&2
  exit 2
fi

BASE="${1%/}"
HEALTH_WAIT_SECONDS=120 # a free-tier instance that went to sleep needs a while to wake up
JOB_WAIT_SECONDS=90
API_TOKEN="${API_TOKEN:-}"

TMP="$(mktemp -d)"
BODY_F="$TMP/body"
HDR_F="$TMP/headers"
CODE=""
SEND_AUTH=1
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL $*" >&2; exit 1; }

case "$BASE" in
  http://* | https://*) ;;
  *) fail "setup: BASE_URL must start with http:// or https://, got '$BASE'" ;;
esac
command -v curl >/dev/null 2>&1 || fail "setup: curl is not installed"

# The token must not cross the network in clear text. Plain http is fine for a local server.
if [ -n "$API_TOKEN" ]; then
  case "$BASE" in
    https://* | http://localhost[:/]* | http://localhost | http://127.0.0.1[:/]* | http://127.0.0.1) ;;
    *) fail "setup: refusing to send API_TOKEN over plain http to $BASE; use https" ;;
  esac
fi

# req METHOD PATH [JSON_BODY]: sets CODE, and writes the response to $BODY_F / $HDR_F.
# The bearer token goes on POST requests only, and only when API_TOKEN is set.
req() {
  local method=$1 path=$2 data=${3:-}
  set -- -s --max-time 20 -D "$HDR_F" -o "$BODY_F" -w '%{http_code}' -X "$method"
  if [ -n "$data" ]; then
    set -- "$@" -H 'Content-Type: application/json' -d "$data"
  fi
  if [ "$method" = POST ] && [ "$SEND_AUTH" = 1 ] && [ -n "$API_TOKEN" ]; then
    set -- "$@" -H "Authorization: Bearer $API_TOKEN"
  fi
  CODE=$(curl "$@" "$BASE$path") || fail "$method $path: curl could not complete the request to $BASE"
}

# check STEP WANT_CODE METHOD PATH [JSON_BODY]; WANT_CODE may list alternatives: "202|200".
check() {
  local step=$1 want=$2
  shift 2
  req "$@"
  case "|$want|" in
    *"|$CODE|"*) ;;
    *) fail "$step: expected HTTP $want, got $CODE: $(head -c 300 "$BODY_F")" ;;
  esac
  echo "ok   $step (HTTP $CODE)"
}

json_str() { { grep -Eo "\"$1\": *\"[^\"]*\"" "$BODY_F" | head -n 1 | sed 's/^[^:]*: *"//; s/"$//'; } || true; }
count_matches() { { grep -Eo -- "$1" "$BODY_F" || true; } | wc -l | tr -d ' '; }

echo "== $BASE"
# Retry until the service answers 200; curl failures and 5xx while it boots are expected.
started=$SECONDS
until [ "$(curl -s -o /dev/null --max-time 10 -w '%{http_code}' "$BASE/healthz" || true)" = 200 ]; do
  [ $((SECONDS - started)) -lt "$HEALTH_WAIT_SECONDS" ] || fail "healthz wait: no HTTP 200 from $BASE/healthz within ${HEALTH_WAIT_SECONDS}s"
  sleep 3
done
echo "ok   healthz wait (up after $((SECONDS - started))s)"

check "list jobs is public" 200 GET /api/syncs
check "list products is public" 200 GET /api/products

# `{}` is deliberate: if the service wrongly accepted the request without a token it would
# answer 422 for the empty body rather than start a sync on a deployment we are only probing.
SEND_AUTH=0
check "start without token is refused (writes are protected)" 401 POST /api/syncs '{}'
grep -Eiq '^www-authenticate: *bearer' "$HDR_F" || fail "start without token: 401 lacks 'WWW-Authenticate: Bearer'"
echo "ok   refusal offers bearer auth"
SEND_AUTH=1

if [ -z "$API_TOKEN" ]; then
  echo "skip authenticated sync steps (API_TOKEN is not exported in this shell)"
  echo "PASS unauthenticated checks against $BASE"
  exit 0
fi

check "start UPDATE-2026-001 with token" "202|200" POST /api/syncs '{"update_id":"UPDATE-2026-001"}'
JOB=$(json_str job_id)
[ -n "$JOB" ] || fail "start UPDATE-2026-001: no job_id in response: $(head -c 300 "$BODY_F")"

started=$SECONDS
state=""
while :; do
  req GET "/api/syncs/$JOB"
  [ "$CODE" = 200 ] || fail "job reaches COMPLETED: GET /api/syncs/$JOB returned HTTP $CODE"
  state=$(json_str status)
  case "$state" in
    COMPLETED) break ;;
    NEEDS_ATTENTION | FAILED | INTERRUPTED)
      fail "job reaches COMPLETED: job ended as $state (last_error: $(json_str last_error))" ;;
  esac
  [ $((SECONDS - started)) -lt "$JOB_WAIT_SECONDS" ] || fail "job reaches COMPLETED: still ${state:-unknown} after ${JOB_WAIT_SECONDS}s"
  sleep 2
done
echo "ok   job reaches COMPLETED (after $((SECONDS - started))s)"

check "list ACTIVE products" 200 GET /api/products
active=$(count_matches '"status": *"ACTIVE"')
[ "$active" -ge 4 ] || fail "list ACTIVE products: expected at least 4 ACTIVE items, found $active: $(head -c 300 "$BODY_F")"
echo "ok   $active ACTIVE products"

echo "PASS all checks against $BASE"
