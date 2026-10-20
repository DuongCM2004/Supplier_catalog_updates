package api_test

// These tests use the API the way a client does: a real listener, a real PostgreSQL
// database, a fake supplier and the real job Manager behind api.NewRouter. Jobs are driven
// with a request followed by Manager.Wait(); only where the asynchronous hand-off itself is
// under test (a started or resumed job becoming COMPLETED) does a test poll the job over
// HTTP, with a deadline.

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/api"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/syncjob"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/testutil"
)

const (
	htDeadline = 10 * time.Second
	// htToken is a throwaway value that only these tests know.
	htToken      = "handlers-test-token"
	htUnknownJob = "00000000-0000-4000-8000-000000000000"
)

// htEnv is one running API over a blank database. token is what post() sends as the bearer
// credential (nothing when empty); raw() sends exactly the headers it is given.
type htEnv struct {
	pool   *pgxpool.Pool
	sup    *testutil.FakeSupplier
	mgr    *syncjob.Manager
	client *http.Client
	url    string
	token  string
}

// htNewEnv builds the API with the given API_TOKEN setting (empty = writes are open). The
// supplier timeout is long so a Block()ed fetch keeps its job RUNNING until the test lets it
// go; cleanup shuts the Manager down, which also releases a worker still parked there.
func htNewEnv(t *testing.T, token string) *htEnv {
	t.Helper()
	pool := testutil.DB(t)
	testutil.Reset(t, pool)
	sup := testutil.NewFakeSupplier()
	cfg := syncjob.Config{
		RetryMaxAttempts: 2,
		RetryBaseDelay:   time.Millisecond,
		RetryMaxDelay:    time.Millisecond,
		SupplierTimeout:  30 * time.Second,
	}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	mgr := syncjob.NewManager(pool, sup, cfg, syncjob.Hooks{}, log)
	// Cleanups run last in, first out: idle connections close, then the server stops taking
	// requests, and only then are the workers stopped.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), htDeadline)
		defer cancel()
		if err := mgr.Shutdown(ctx); err != nil {
			t.Errorf("manager shutdown: %v", err)
		}
	})
	srv := httptest.NewServer(api.NewRouter(api.Deps{
		Pool: pool, Log: log, Manager: mgr, SupplierID: testutil.SupplierID, APIToken: token,
	}))
	t.Cleanup(srv.Close)
	client := &http.Client{Timeout: htDeadline}
	t.Cleanup(client.CloseIdleConnections)
	return &htEnv{pool: pool, sup: sup, mgr: mgr, client: client, url: srv.URL, token: token}
}

// htReply is one HTTP answer, read completely.
type htReply struct {
	status int
	header http.Header
	raw    []byte
}

// raw sends one request with exactly the given headers (name, value pairs).
func (e *htEnv) raw(t *testing.T, method, path, body string, headers ...string) htReply {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, e.url+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build %s %s: %v", method, path, err)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := e.client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read answer of %s %s: %v", method, path, err)
	}
	return htReply{status: resp.StatusCode, header: resp.Header, raw: raw}
}

func (e *htEnv) get(t *testing.T, path string) htReply {
	t.Helper()
	return e.raw(t, http.MethodGet, path, "")
}

// post sends the bearer token when the environment has one, like a client that knows it.
func (e *htEnv) post(t *testing.T, path, body string) htReply {
	t.Helper()
	headers := []string{"Content-Type", "application/json"}
	if e.token != "" {
		headers = append(headers, "Authorization", "Bearer "+e.token)
	}
	return e.raw(t, http.MethodPost, path, body, headers...)
}

// want fails the test unless the status matches, and returns the reply for chaining.
func (r htReply) want(t *testing.T, status int) htReply {
	t.Helper()
	if r.status != status {
		t.Fatalf("status = %d, want %d: %s", r.status, status, r.raw)
	}
	return r
}

// object decodes a JSON object answer.
func (r htReply) object(t *testing.T) map[string]any {
	t.Helper()
	if ct := r.header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var m map[string]any
	if err := json.Unmarshal(r.raw, &m); err != nil {
		t.Fatalf("answer is not a JSON object: %v\n%s", err, r.raw)
	}
	return m
}

// items decodes the "items" array of a list answer. An empty list must be [], never null.
func (r htReply) items(t *testing.T) []map[string]any {
	t.Helper()
	body := r.object(t)
	raw, ok := body["items"].([]any)
	if !ok {
		t.Fatalf("items is not an array (null is not acceptable for an empty list): %s", r.raw)
	}
	items := make([]map[string]any, len(raw))
	for i, v := range raw {
		if items[i], ok = v.(map[string]any); !ok {
			t.Fatalf("items[%d] is not an object: %v", i, v)
		}
	}
	return items
}

// wantError checks the status, the {"error":{"code","message"}} envelope and the code, and
// returns the error object for assertions on its extra fields.
func (r htReply) wantError(t *testing.T, status int, code string) map[string]any {
	t.Helper()
	r.want(t, status)
	body := r.object(t)
	if len(body) != 1 {
		t.Errorf("error answer has keys %v, want only \"error\": %s", htKeys(body), r.raw)
	}
	errObj := htObj(t, body, "error")
	if errObj["code"] != code {
		t.Fatalf("error code = %v, want %s: %s", errObj["code"], code, r.raw)
	}
	if msg, _ := errObj["message"].(string); msg == "" {
		t.Errorf("error has no message: %s", r.raw)
	}
	return errObj
}

func htKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// htAssertKeys fails unless m has exactly these keys: it pins the response shape.
func htAssertKeys(t *testing.T, what string, m map[string]any, want ...string) {
	t.Helper()
	slices.Sort(want)
	if got := htKeys(m); !slices.Equal(got, want) {
		t.Errorf("%s keys = %v, want %v", what, got, want)
	}
}

func htObj(t *testing.T, m map[string]any, key string) map[string]any {
	t.Helper()
	obj, ok := m[key].(map[string]any)
	if !ok {
		t.Fatalf("%q is not an object in %v", key, m)
	}
	return obj
}

func htStr(t *testing.T, m map[string]any, key string) string {
	t.Helper()
	s, ok := m[key].(string)
	if !ok || s == "" {
		t.Fatalf("%q is not a non-empty string in %v", key, m)
	}
	return s
}

// htEventually polls cond until it holds, failing after the deadline. It is only for waiting
// on something that really happens in the background.
func htEventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(htDeadline)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("gave up after %s waiting for %s", htDeadline, what)
		}
		<-time.After(2 * time.Millisecond)
	}
}

// job reads one job over HTTP.
func (e *htEnv) job(t *testing.T, id string) map[string]any {
	t.Helper()
	return e.get(t, "/api/syncs/"+id).want(t, http.StatusOK).object(t)
}

// awaitStatus polls the job over HTTP until it has the wanted status. A job that stops in any
// other final-looking state fails at once instead of waiting out the deadline.
func (e *htEnv) awaitStatus(t *testing.T, id, want string) map[string]any {
	t.Helper()
	var job map[string]any
	htEventually(t, "job "+id+" to become "+want, func() bool {
		job = e.job(t, id)
		switch job["status"] {
		case want:
			return true
		case "INTERRUPTED", "NEEDS_ATTENTION", "COMPLETED", "FAILED":
			t.Fatalf("job %s stopped as %v, want %s: %v", id, job["status"], want, job)
		}
		return false
	})
	return job
}

// awaitFetch returns once the fake supplier has been asked for cursor at least n times in
// total. A fetch is logged before it blocks, so the worker is parked there afterwards.
func (e *htEnv) awaitFetch(t *testing.T, cursor string, n int) {
	t.Helper()
	htEventually(t, fmt.Sprintf("%d fetches at cursor %q", n, cursor), func() bool { return e.sup.FetchCalls(cursor) >= n })
}

// start posts an update, expects 202 and returns the new job's id.
func (e *htEnv) start(t *testing.T, updateID string) string {
	t.Helper()
	r := e.post(t, "/api/syncs", `{"update_id":"`+updateID+`"}`).want(t, http.StatusAccepted)
	return htStr(t, r.object(t), "job_id")
}

// htServe makes the fake supplier serve a two-batch update. Applied in order it creates
// SKU-1 and SKU-2, then updates SKU-1, withdraws SKU-2 and creates SKU-3: created 3, updated
// 1, withdrawn 1, nothing ignored. Pages are keyed by cursor, so serve one update at a time.
func htServe(sup *testutil.FakeSupplier, updateID string) {
	sup.SetUpdate(testutil.Info(updateID, 2))
	sup.AddPage("", testutil.Page(updateID, "BATCH-1", "cursor-2", true,
		testutil.Active("SKU-1", 1, "Widget", 1000), testutil.Active("SKU-2", 1, "Gadget", 2000)))
	sup.AddPage("cursor-2", testutil.Page(updateID, "BATCH-2", "", false,
		testutil.Active("SKU-1", 2, "Widget", 1200), testutil.Withdrawn("SKU-2", 2), testutil.Active("SKU-3", 1, "Gizmo", 300)))
}

// htServeBroken serves an update whose second batch has a negative price: the job stops at
// cursor-2 with a blocking issue after committing SKU-1.
func htServeBroken(sup *testutil.FakeSupplier, updateID string) {
	sup.SetUpdate(testutil.Info(updateID, 2))
	sup.AddPage("", testutil.Page(updateID, "BATCH-1", "cursor-2", true, testutil.Active("SKU-1", 1, "Widget", 1000)))
	sup.AddPage("cursor-2", testutil.Page(updateID, "BATCH-2", "", false, testutil.Active("SKU-9", 1, "Broken", -5)))
}

// htWantTotals checks the five counters of a job body.
func htWantTotals(t *testing.T, job map[string]any, created, updated, withdrawn, duplicate, stale int) {
	t.Helper()
	changes, ignored := htObj(t, job, "changes"), htObj(t, job, "ignored")
	got := [5]any{changes["created"], changes["updated"], changes["withdrawn"], ignored["duplicate"], ignored["stale"]}
	want := [5]any{float64(created), float64(updated), float64(withdrawn), float64(duplicate), float64(stale)}
	if got != want {
		t.Errorf("created/updated/withdrawn/duplicate/stale = %v, want %v", got, want)
	}
}

func htRandomUUID(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("random uuid: %v", err)
	}
	b[6], b[8] = b[6]&0x0f|0x40, b[8]&0x3f|0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func TestStartNewUpdateAnswers202WithLocationAndTheJobCompletes(t *testing.T) {
	e := htNewEnv(t, "")
	htServe(e.sup, "UPDATE-1")

	r := e.post(t, "/api/syncs", `{"update_id":"UPDATE-1"}`).want(t, http.StatusAccepted)
	job := r.object(t)
	id := htStr(t, job, "job_id")
	if got := r.header.Get("Location"); got != "/api/syncs/"+id {
		t.Errorf("Location = %q, want %q", got, "/api/syncs/"+id)
	}
	if s := job["status"]; s != "PENDING" && s != "RUNNING" {
		t.Errorf("status in the 202 body = %v, want PENDING or RUNNING", s)
	}
	if job["supplier_id"] != testutil.SupplierID || job["supplier_update_id"] != "UPDATE-1" {
		t.Errorf("job identity = %v / %v", job["supplier_id"], job["supplier_update_id"])
	}
	if followed := e.get(t, r.header.Get("Location")).want(t, http.StatusOK).object(t); followed["job_id"] != id {
		t.Errorf("Location led to job %v, want %s", followed["job_id"], id)
	}

	done := e.awaitStatus(t, id, "COMPLETED")
	progress := htObj(t, done, "progress")
	if progress["processed_batches"] != float64(2) || progress["total_batches"] != float64(2) ||
		progress["end_of_feed_seen"] != true || progress["last_batch_id"] != "BATCH-2" || progress["next_cursor"] != "" {
		t.Errorf("progress = %v", progress)
	}
	htWantTotals(t, done, 3, 1, 1, 0, 0)
	if done["completed_at"] == nil || done["last_error"] != nil || done["open_blocking_issues"] != float64(0) {
		t.Errorf("completed job fields = %v", done)
	}
	e.mgr.Wait()
	testutil.AssertCounters(t, e.pool, id)
	testutil.AssertCompletedConsistent(t, e.pool, id)
}

func TestStartKnownUpdateAnswers200WithTheSameJobAndChangesNothing(t *testing.T) {
	e := htNewEnv(t, "")
	htServe(e.sup, "UPDATE-1")
	id := e.start(t, "UPDATE-1")
	e.mgr.Wait()
	before, calls := testutil.Snapshot(t, e.pool), len(e.sup.Calls())

	r := e.post(t, "/api/syncs", `{"update_id":"UPDATE-1"}`).want(t, http.StatusOK)
	again := r.object(t)
	if again["job_id"] != id || again["status"] != "COMPLETED" {
		t.Errorf("repeated start = job %v %v, want %s COMPLETED", again["job_id"], again["status"], id)
	}
	if loc := r.header.Get("Location"); loc != "" {
		t.Errorf("repeated start sent Location %q; nothing new was created", loc)
	}
	e.mgr.Wait()
	testutil.AssertUnchanged(t, before, testutil.Snapshot(t, e.pool))
	if after := len(e.sup.Calls()); after != calls {
		t.Errorf("repeated start contacted the supplier %d more times", after-calls)
	}
}

func TestStartRejectsUnreadableBodiesWith400AndWritesNothing(t *testing.T) {
	e := htNewEnv(t, "")
	before := testutil.Snapshot(t, e.pool)
	// Exactly one byte over the 1 MiB limit, so the server has read all of it when it answers.
	oversized := `{"update_id":"` + strings.Repeat("a", 1<<20+1-len(`{"update_id":""}`)) + `"}`
	for name, body := range map[string]string{
		"truncated JSON":        `{"update_id":`,
		"empty body":            ``,
		"not JSON":              `update_id=U1`,
		"unknown field":         `{"update_id":"U1","extra":1}`,
		"update_id of a number": `{"update_id":5}`,
		"array":                 `[{"update_id":"U1"}]`,
		"two objects":           `{"update_id":"U1"}{"update_id":"U2"}`,
		"trailing garbage":      `{"update_id":"U1"} x`,
		"body over 1 MiB":       oversized,
	} {
		t.Run(name, func(t *testing.T) {
			e.post(t, "/api/syncs", body).wantError(t, http.StatusBadRequest, "INVALID_JSON")
		})
	}
	testutil.AssertUnchanged(t, before, testutil.Snapshot(t, e.pool))
}

func TestStartRejectsBadIdsWith422AndWritesNothing(t *testing.T) {
	e := htNewEnv(t, "")
	before := testutil.Snapshot(t, e.pool)
	for name, body := range map[string]string{
		"missing update_id":      `{}`,
		"empty update_id":        `{"update_id":""}`,
		"path-like update_id":    `{"update_id":"../etc"}`,
		"update_id with a slash": `{"update_id":"a/b"}`,
		"update_id with a space": `{"update_id":"bad id"}`,
		"65 character update_id": `{"update_id":"` + strings.Repeat("a", 65) + `"}`,
		"empty supplier_id":      `{"update_id":"U1","supplier_id":""}`,
		"supplier_id with space": `{"update_id":"U1","supplier_id":"SUP 1"}`,
	} {
		t.Run(name, func(t *testing.T) {
			e.post(t, "/api/syncs", body).wantError(t, http.StatusUnprocessableEntity, "VALIDATION_FAILED")
		})
	}
	testutil.AssertUnchanged(t, before, testutil.Snapshot(t, e.pool))

	// 64 characters is the longest id that is accepted.
	longest := strings.Repeat("a", 64)
	e.sup.SetUpdate(testutil.Info(longest, 1))
	e.start(t, longest)
	e.mgr.Wait()
}

func TestStartWhileAnotherJobRunsAnswers409NamingTheActiveJob(t *testing.T) {
	e := htNewEnv(t, "")
	htServe(e.sup, "UPDATE-1")
	e.sup.SetUpdate(testutil.Info("UPDATE-2", 2))
	e.sup.Block("")
	first := e.start(t, "UPDATE-1")
	e.awaitFetch(t, "", 1)
	before := testutil.Snapshot(t, e.pool)

	errObj := e.post(t, "/api/syncs", `{"update_id":"UPDATE-2"}`).wantError(t, http.StatusConflict, "ACTIVE_JOB_EXISTS")
	if errObj["active_job_id"] != first {
		t.Errorf("active_job_id = %v, want the running job %s", errObj["active_job_id"], first)
	}
	testutil.AssertUnchanged(t, before, testutil.Snapshot(t, e.pool))

	// Asking again for the update that is running is not a conflict: it is already known.
	again := e.post(t, "/api/syncs", `{"update_id":"UPDATE-1"}`).want(t, http.StatusOK).object(t)
	if again["job_id"] != first || again["status"] != "RUNNING" {
		t.Errorf("repeat of the running update = %v %v, want %s RUNNING", again["job_id"], again["status"], first)
	}

	// Once the running job is done the refused update can be started.
	e.sup.Unblock("")
	e.awaitStatus(t, first, "COMPLETED")
	e.mgr.Wait()
	htServe(e.sup, "UPDATE-2")
	e.start(t, "UPDATE-2")
	e.mgr.Wait()
}

func TestStartUnknownUpdateAnswers202ThenTheJobFailsWithUnknownUpdateIssue(t *testing.T) {
	e := htNewEnv(t, "")
	id := e.start(t, "UPDATE-NOPE") // the supplier has never heard of it
	failed := e.awaitStatus(t, id, "FAILED")
	e.mgr.Wait()
	if msg, _ := failed["last_error"].(string); msg == "" {
		t.Errorf("FAILED job has no last_error: %v", failed)
	}

	r := e.get(t, "/api/syncs/"+id+"/issues").want(t, http.StatusOK)
	issues := r.items(t)
	if len(issues) != 1 || issues[0]["code"] != "UNKNOWN_UPDATE" || issues[0]["severity"] != "BLOCKING" || issues[0]["status"] != "OPEN" {
		t.Fatalf("issues = %s, want one open blocking UNKNOWN_UPDATE", r.raw)
	}
	testutil.AssertCounters(t, e.pool, id)

	// A FAILED job is not active, so it does not stand in the way of the next update.
	htServe(e.sup, "UPDATE-1")
	e.start(t, "UPDATE-1")
	e.mgr.Wait()
}

func TestListSyncsPagesNewestFirstAndRejectsInvalidValues(t *testing.T) {
	e := htNewEnv(t, "")
	htServe(e.sup, "UPDATE-1")
	e.start(t, "UPDATE-1")
	e.mgr.Wait()
	htServe(e.sup, "UPDATE-2")
	second := e.start(t, "UPDATE-2")
	e.mgr.Wait()

	for _, query := range []string{
		"limit=0", "limit=501", "limit=abc", "limit=-1", "offset=-1", "offset=1.5", "status=BOGUS", "status=completed",
	} {
		t.Run(query, func(t *testing.T) {
			e.get(t, "/api/syncs?"+query).wantError(t, http.StatusUnprocessableEntity, "VALIDATION_FAILED")
		})
	}

	r := e.get(t, "/api/syncs?limit=1&offset=0").want(t, http.StatusOK)
	page := r.object(t)
	htAssertKeys(t, "job list", page, "items", "limit", "offset")
	items := r.items(t)
	if len(items) != 1 || items[0]["job_id"] != second || page["limit"] != float64(1) || page["offset"] != float64(0) {
		t.Errorf("limit=1&offset=0 = %s, want only the newest job (UPDATE-2)", r.raw)
	}
	if next := e.get(t, "/api/syncs?limit=1&offset=1").want(t, http.StatusOK).items(t); len(next) != 1 || next[0]["supplier_update_id"] != "UPDATE-1" {
		t.Errorf("limit=1&offset=1 = %v, want UPDATE-1", next)
	}
	if all := e.get(t, "/api/syncs").want(t, http.StatusOK); len(all.items(t)) != 2 {
		t.Errorf("default listing = %s, want both jobs", all.raw)
	}
	if completed := e.get(t, "/api/syncs?status=COMPLETED").want(t, http.StatusOK).items(t); len(completed) != 2 {
		t.Errorf("status=COMPLETED returned %d jobs, want 2", len(completed))
	}
	if none := e.get(t, "/api/syncs?status=FAILED").want(t, http.StatusOK).items(t); len(none) != 0 {
		t.Errorf("status=FAILED returned %v, want an empty list", none)
	}
}

func TestUnknownOrMalformedJobIdsAnswer404OnEveryJobRoute(t *testing.T) {
	e := htNewEnv(t, "")
	for _, id := range []string{htRandomUUID(t), htUnknownJob, "not-a-uuid", "1;DROP-TABLE-sync_jobs"} {
		for _, route := range []struct{ method, suffix string }{
			{http.MethodGet, ""}, {http.MethodGet, "/changes"}, {http.MethodGet, "/issues"}, {http.MethodPost, "/resume"},
		} {
			t.Run(route.method+" "+id+route.suffix, func(t *testing.T) {
				e.raw(t, route.method, "/api/syncs/"+id+route.suffix, "").wantError(t, http.StatusNotFound, "NOT_FOUND")
			})
		}
	}
}

func TestResumeContinuesAnInterruptedJobFromItsSavedCursorToCompletion(t *testing.T) {
	e := htNewEnv(t, "")
	htServe(e.sup, "UPDATE-1")
	e.sup.FailTimes("cursor-2", -1)
	id := e.start(t, "UPDATE-1")
	e.mgr.Wait()

	stopped := e.job(t, id)
	progress := htObj(t, stopped, "progress")
	if stopped["status"] != "INTERRUPTED" || progress["processed_batches"] != float64(1) || progress["next_cursor"] != "cursor-2" {
		t.Fatalf("job before resume = %v, want INTERRUPTED after the first batch, waiting at cursor-2", stopped)
	}

	e.sup.FailTimes("cursor-2", 0) // the supplier recovers
	resumed := e.post(t, "/api/syncs/"+id+"/resume", "").want(t, http.StatusAccepted).object(t)
	if resumed["job_id"] != id || resumed["status"] != "RUNNING" || htObj(t, resumed, "progress")["processed_batches"] != float64(1) {
		t.Errorf("202 body = %v, want the same job RUNNING with its first batch kept", resumed)
	}

	done := e.awaitStatus(t, id, "COMPLETED")
	htWantTotals(t, done, 3, 1, 1, 0, 0) // the same counters a run without the outage ends with
	if done["last_error"] != nil || done["open_blocking_issues"] != float64(0) {
		t.Errorf("completed job = %v, want no leftover error", done)
	}
	e.mgr.Wait()
	testutil.AssertCounters(t, e.pool, id)
	testutil.AssertCompletedConsistent(t, e.pool, id)
	if n := e.sup.FetchCalls(""); n != 1 {
		t.Errorf("first page fetched %d times; resume must continue at cursor-2 and never refetch a committed batch", n)
	}
	issues := e.get(t, "/api/syncs/"+id+"/issues").want(t, http.StatusOK).items(t)
	if len(issues) != 1 || issues[0]["code"] != "SUPPLIER_UNAVAILABLE" || issues[0]["status"] != "RESOLVED" || issues[0]["resolved_at"] == nil {
		t.Errorf("issues after resume = %v, want the outage issue RESOLVED", issues)
	}
}

func TestResumeRefusesCompletedFailedAndRunningJobsWithTheirStatus(t *testing.T) {
	e := htNewEnv(t, "")
	htServe(e.sup, "UPDATE-1")
	completed := e.start(t, "UPDATE-1")
	e.mgr.Wait()
	failed := e.start(t, "UPDATE-NOPE")
	e.awaitStatus(t, failed, "FAILED")
	e.mgr.Wait()

	htServe(e.sup, "UPDATE-3")
	e.sup.Block("")
	fetches := e.sup.FetchCalls("")
	running := e.start(t, "UPDATE-3")
	e.awaitFetch(t, "", fetches+1)

	for name, tc := range map[string]struct{ id, status string }{
		"completed": {completed, "COMPLETED"}, "failed": {failed, "FAILED"}, "running with a live worker": {running, "RUNNING"},
	} {
		t.Run(name, func(t *testing.T) {
			before := testutil.Snapshot(t, e.pool)
			errObj := e.post(t, "/api/syncs/"+tc.id+"/resume", "").wantError(t, http.StatusConflict, "INVALID_STATE")
			if errObj["current_status"] != tc.status {
				t.Errorf("current_status = %v, want %s", errObj["current_status"], tc.status)
			}
			testutil.AssertUnchanged(t, before, testutil.Snapshot(t, e.pool))
		})
	}
	e.sup.Unblock("")
	e.awaitStatus(t, running, "COMPLETED")
}

func TestResumeWhileAnotherJobIsActiveAnswers409AndLeavesTheJobStopped(t *testing.T) {
	e := htNewEnv(t, "")
	htServe(e.sup, "UPDATE-1")
	e.sup.FailTimes("cursor-2", -1)
	stopped := e.start(t, "UPDATE-1")
	e.mgr.Wait()
	cursor2Fetches := e.sup.FetchCalls("cursor-2")

	htServe(e.sup, "UPDATE-2")
	e.sup.Block("")
	fetches := e.sup.FetchCalls("")
	e.start(t, "UPDATE-2")
	e.awaitFetch(t, "", fetches+1)

	e.post(t, "/api/syncs/"+stopped+"/resume", "").wantError(t, http.StatusConflict, "ACTIVE_JOB_EXISTS")
	if status := e.job(t, stopped)["status"]; status != "INTERRUPTED" {
		t.Errorf("refused job is %v, want it left INTERRUPTED", status)
	}
	if n := e.sup.FetchCalls("cursor-2"); n != cursor2Fetches {
		t.Errorf("the refused resume asked the supplier for cursor-2 %d more times", n-cursor2Fetches)
	}
	e.sup.Unblock("")
	e.mgr.Wait()
}

func TestChangesEndpointListsAppliedChangesAndFiltersByType(t *testing.T) {
	e := htNewEnv(t, "")
	htServe(e.sup, "UPDATE-1")
	id := e.start(t, "UPDATE-1")
	e.mgr.Wait()
	path := "/api/syncs/" + id + "/changes"

	r := e.get(t, path).want(t, http.StatusOK)
	htAssertKeys(t, "change list", r.object(t), "items")
	var order []string
	for _, it := range r.items(t) {
		htAssertKeys(t, "change", it, "id", "batch_id", "supplier_id", "product_id", "revision", "change_type", "before", "after", "created_at")
		order = append(order, fmt.Sprintf("%v %v r%v", it["change_type"], it["product_id"], it["revision"]))
	}
	want := []string{"CREATED SKU-1 r1", "CREATED SKU-2 r1", "UPDATED SKU-1 r2", "WITHDRAWN SKU-2 r2", "CREATED SKU-3 r1"}
	if !slices.Equal(order, want) {
		t.Errorf("changes in application order = %v, want %v", order, want)
	}
	items := r.items(t)
	if items[0]["before"] != nil {
		t.Errorf("a created product has before = %v, want null", items[0]["before"])
	}
	if before, after := htObj(t, items[2], "before"), htObj(t, items[2], "after"); before["price_minor"] != float64(1000) || after["price_minor"] != float64(1200) {
		t.Errorf("update before/after price = %v / %v, want 1000 / 1200", before["price_minor"], after["price_minor"])
	}

	for filter, wantCount := range map[string]int{"CREATED": 3, "UPDATED": 1, "WITHDRAWN": 1} {
		got := e.get(t, path+"?type="+filter).want(t, http.StatusOK).items(t)
		if len(got) != wantCount {
			t.Errorf("type=%s returned %d changes, want %d", filter, len(got), wantCount)
		}
		for _, it := range got {
			if it["change_type"] != filter {
				t.Errorf("type=%s returned a %v change", filter, it["change_type"])
			}
		}
	}
	for _, bad := range []string{"?type=BOGUS", "?type=created", "?limit=0", "?offset=-1"} {
		e.get(t, path+bad).wantError(t, http.StatusUnprocessableEntity, "VALIDATION_FAILED")
	}
	if page := e.get(t, path+"?limit=2&offset=1").want(t, http.StatusOK).items(t); len(page) != 2 || page[0]["product_id"] != "SKU-2" {
		t.Errorf("limit=2&offset=1 = %v, want the second and third change", page)
	}
	if past := e.get(t, path+"?offset=50").want(t, http.StatusOK).items(t); len(past) != 0 {
		t.Errorf("offset past the end = %v, want an empty list", past)
	}
}

func TestIssuesEndpointReportsOpenBlockingIssuesAndTheirResolution(t *testing.T) {
	e := htNewEnv(t, "")
	htServeBroken(e.sup, "UPDATE-1")
	id := e.start(t, "UPDATE-1")
	e.mgr.Wait()
	path := "/api/syncs/" + id + "/issues"

	if job := e.job(t, id); job["status"] != "NEEDS_ATTENTION" || job["open_blocking_issues"] != float64(1) {
		t.Fatalf("job = %v, want NEEDS_ATTENTION with one open blocking issue", job)
	}
	r := e.get(t, path+"?status=OPEN").want(t, http.StatusOK)
	htAssertKeys(t, "issue list", r.object(t), "items", "open_blocking")
	if r.object(t)["open_blocking"] != float64(1) {
		t.Errorf("open_blocking = %v, want 1", r.object(t)["open_blocking"])
	}
	items := r.items(t)
	if len(items) != 1 {
		t.Fatalf("open issues = %s, want exactly one", r.raw)
	}
	issue := items[0]
	htAssertKeys(t, "issue", issue, "id", "batch_id", "cursor", "supplier_id", "product_id", "code", "severity", "status",
		"message", "retry_count", "first_seen_at", "last_seen_at", "resolved_at")
	if issue["code"] != "INVALID_PRODUCT_DATA" || issue["severity"] != "BLOCKING" || issue["status"] != "OPEN" ||
		issue["cursor"] != "cursor-2" || issue["resolved_at"] != nil {
		t.Errorf("issue = %v", issue)
	}
	if msg, _ := issue["message"].(string); !strings.Contains(msg, "SKU-9") {
		t.Errorf("message %q does not name the offending product", msg)
	}

	// Filters narrow the items, never the job-level count of blocking issues.
	for query, wantItems := range map[string]int{
		"": 1, "?status=RESOLVED": 0, "?severity=BLOCKING": 1, "?severity=TRANSIENT": 0, "?status=OPEN&severity=TRANSIENT": 0,
	} {
		filtered := e.get(t, path+query).want(t, http.StatusOK)
		if n := len(filtered.items(t)); n != wantItems || filtered.object(t)["open_blocking"] != float64(1) {
			t.Errorf("%s = %d items, open_blocking %v; want %d and 1", query, n, filtered.object(t)["open_blocking"], wantItems)
		}
	}
	for _, bad := range []string{"?status=BOGUS", "?status=open", "?severity=BOGUS", "?limit=501", "?offset=x"} {
		e.get(t, path+bad).wantError(t, http.StatusUnprocessableEntity, "VALIDATION_FAILED")
	}

	// The supplier corrects the batch; resuming commits it and closes the issue.
	e.sup.AddPage("cursor-2", testutil.Page("UPDATE-1", "BATCH-2", "", false, testutil.Active("SKU-2", 1, "Gadget", 2000)))
	e.post(t, "/api/syncs/"+id+"/resume", "").want(t, http.StatusAccepted)
	e.awaitStatus(t, id, "COMPLETED")
	e.mgr.Wait()
	after := e.get(t, path).want(t, http.StatusOK)
	resolved := after.items(t)
	if len(resolved) != 1 || resolved[0]["status"] != "RESOLVED" || resolved[0]["resolved_at"] == nil || after.object(t)["open_blocking"] != float64(0) {
		t.Errorf("issues after the correction = %s, want the same issue RESOLVED and open_blocking 0", after.raw)
	}
	if open := e.get(t, path+"?status=OPEN").want(t, http.StatusOK).items(t); len(open) != 0 {
		t.Errorf("open issues after the correction = %v, want none", open)
	}
}

// htCatalog leaves SKU-1 ACTIVE r2, SKU-2 and SKU-900 WITHDRAWN, SKU-3 ACTIVE r1. SKU-900 was
// never seen before its withdrawal, so it is a tombstone with no details.
func htCatalog(t *testing.T) *htEnv {
	t.Helper()
	e := htNewEnv(t, "")
	htServe(e.sup, "UPDATE-1")
	e.start(t, "UPDATE-1")
	e.mgr.Wait()
	e.sup.SetUpdate(testutil.Info("UPDATE-2", 1))
	e.sup.AddPage("", testutil.Page("UPDATE-2", "BATCH-1", "", false, testutil.Withdrawn("SKU-900", 4)))
	e.start(t, "UPDATE-2")
	e.mgr.Wait()
	return e
}

func htProductIDs(items []map[string]any) []string {
	ids := make([]string, len(items))
	for i, it := range items {
		ids[i], _ = it["product_id"].(string)
	}
	return ids
}

func TestProductsEndpointListsActiveByDefaultAndWithdrawnOnRequest(t *testing.T) {
	e := htCatalog(t)
	for query, want := range map[string][]string{
		"":                             {"SKU-1", "SKU-3"},
		"?status=ACTIVE":               {"SKU-1", "SKU-3"},
		"?status=WITHDRAWN":            {"SKU-2", "SKU-900"},
		"?status=ALL":                  {"SKU-1", "SKU-2", "SKU-3", "SKU-900"},
		"?supplier_id=SUP-001":         {"SKU-1", "SKU-3"},
		"?supplier_id=SUP-404":         {},
		"?status=ALL&limit=2&offset=1": {"SKU-2", "SKU-3"},
		"?offset=10":                   {},
	} {
		r := e.get(t, "/api/products"+query).want(t, http.StatusOK)
		htAssertKeys(t, "product list", r.object(t), "items")
		items := r.items(t)
		for _, it := range items {
			htAssertKeys(t, "product", it, "supplier_id", "product_id", "status", "revision", "name", "price_minor",
				"currency", "created_at", "updated_at", "withdrawn_at")
		}
		if got := htProductIDs(items); !slices.Equal(got, want) {
			t.Errorf("GET /api/products%s = %v, want %v", query, got, want)
		}
	}
	for _, bad := range []string{"?status=x", "?status=active", "?status=ACTIVE,WITHDRAWN", "?limit=0", "?limit=501", "?offset=-1", "?supplier_id=a%00b"} {
		e.get(t, "/api/products"+bad).wantError(t, http.StatusUnprocessableEntity, "VALIDATION_FAILED")
	}
}

func TestProductByIdReturnsActiveProductsAndTombstonesAnd404ForTheRest(t *testing.T) {
	e := htCatalog(t)

	widget := e.get(t, "/api/products/SKU-1").want(t, http.StatusOK).object(t)
	if widget["status"] != "ACTIVE" || widget["revision"] != float64(2) || widget["name"] != "Widget" ||
		widget["price_minor"] != float64(1200) || widget["currency"] != "USD" || widget["withdrawn_at"] != nil {
		t.Errorf("SKU-1 = %v", widget)
	}
	if with := e.get(t, "/api/products/SKU-1?supplier_id=SUP-001").want(t, http.StatusOK).object(t); with["product_id"] != "SKU-1" {
		t.Errorf("SKU-1 with an explicit supplier = %v", with)
	}
	withdrawn := e.get(t, "/api/products/SKU-2").want(t, http.StatusOK).object(t)
	if withdrawn["status"] != "WITHDRAWN" || withdrawn["revision"] != float64(2) || withdrawn["withdrawn_at"] == nil {
		t.Errorf("SKU-2 = %v, want a WITHDRAWN tombstone at revision 2", withdrawn)
	}
	unseen := e.get(t, "/api/products/SKU-900").want(t, http.StatusOK).object(t)
	if unseen["status"] != "WITHDRAWN" || unseen["revision"] != float64(4) || unseen["withdrawn_at"] == nil ||
		unseen["name"] != nil || unseen["price_minor"] != nil || unseen["currency"] != nil {
		t.Errorf("SKU-900 = %v, want a tombstone at revision 4 with no name, price or currency", unseen)
	}

	for _, path := range []string{
		"/api/products/SKU-NOPE", "/api/products/SKU-1?supplier_id=SUP-404", "/api/products/a%00b", "/api/products/%ff",
	} {
		e.get(t, path).wantError(t, http.StatusNotFound, "NOT_FOUND")
	}
}

func TestHealthzAnswers200WithALivePoolAnd503WithAClosedOne(t *testing.T) {
	live := testutil.DB(t)
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	closed, err := pgxpool.New(context.Background(), os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatalf("open a second pool: %v", err)
	}
	closed.Close()

	for name, tc := range map[string]struct {
		pool     *pgxpool.Pool
		status   int
		wantBody map[string]any
	}{
		"live pool":   {live, http.StatusOK, map[string]any{"status": "ok", "database": "ok"}},
		"closed pool": {closed, http.StatusServiceUnavailable, map[string]any{"status": "unavailable", "database": "error"}},
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(api.NewRouter(api.Deps{Pool: tc.pool, Log: log}))
			defer srv.Close()
			e := &htEnv{client: srv.Client(), url: srv.URL}
			r := e.get(t, "/healthz").want(t, tc.status)
			if body := r.object(t); !reflect.DeepEqual(body, tc.wantBody) {
				t.Errorf("body = %v, want %v", body, tc.wantBody)
			}
		})
	}
}

// htRows runs a query and returns each row as the JSON the API should have produced for it:
// timestamps as UTC text, numbers as float64, jsonb decoded, NULL as nil.
func htRows(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) []map[string]any {
	t.Helper()
	rows, err := pool.Query(context.Background(), sql, args...)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowToMap)
	if err != nil {
		t.Fatalf("read rows of %q: %v", sql, err)
	}
	out := make([]map[string]any, len(got))
	for i, row := range got {
		for k, v := range row {
			if ts, ok := v.(time.Time); ok {
				row[k] = ts.UTC()
			}
		}
		raw, err := json.Marshal(row)
		if err != nil {
			t.Fatalf("encode row %v: %v", row, err)
		}
		if err := json.Unmarshal(raw, &out[i]); err != nil {
			t.Fatalf("decode row %s: %v", raw, err)
		}
	}
	return out
}

// htAssertSameRows fails unless the API items equal the database rows, field for field.
func htAssertSameRows(t *testing.T, what string, items, rows []map[string]any) {
	t.Helper()
	if len(items) != len(rows) {
		t.Errorf("%s: the API returned %d items, the database holds %d rows", what, len(items), len(rows))
		return
	}
	for i := range items {
		if !reflect.DeepEqual(items[i], rows[i]) {
			fromAPI, _ := json.Marshal(items[i])
			fromDB, _ := json.Marshal(rows[i])
			t.Errorf("%s[%d] differs from the database\n  api: %s\n  db:  %s", what, i, fromAPI, fromDB)
		}
	}
}

const (
	htChangeRowsSQL = `SELECT id::text AS id, batch_id, supplier_id, product_id, revision, change_type, before, after, created_at
FROM sync_changes WHERE job_id = $1::uuid ORDER BY created_at, id`
	htIssueRowsSQL = `SELECT id::text AS id, batch_id, cursor, supplier_id, product_id, code, severity, status, message,
  retry_count, first_seen_at, last_seen_at, resolved_at
FROM sync_issues WHERE job_id = $1::uuid ORDER BY first_seen_at, id`
	htProductRowsSQL = `SELECT supplier_id, product_id, status, revision, name, price_minor, currency, created_at, updated_at, withdrawn_at
FROM products ORDER BY supplier_id, product_id`
	htJobRowSQL = `SELECT status, processed_batches, next_cursor, created_count, updated_count, withdrawn_count, duplicate_count, stale_count,
  (SELECT count(*) FROM sync_issues i WHERE i.job_id = j.id AND i.status = 'OPEN' AND i.severity = 'BLOCKING') AS open_blocking
FROM sync_jobs j WHERE id = $1::uuid`
)

func TestChangeAndIssueEndpointsReportPersistedState(t *testing.T) {
	e := htNewEnv(t, "")
	// First import: three products created over two batches.
	e.sup.SetUpdate(testutil.Info("UPDATE-1", 2))
	e.sup.AddPage("", testutil.Page("UPDATE-1", "BATCH-1", "cursor-2", true,
		testutil.Active("SKU-A", 1, "Alpha", 100), testutil.Active("SKU-B", 1, "Bravo", 200)))
	e.sup.AddPage("cursor-2", testutil.Page("UPDATE-1", "BATCH-2", "", false, testutil.Active("SKU-C", 1, "Charlie", 300)))
	imported := e.start(t, "UPDATE-1")
	e.mgr.Wait()
	// Revision 2 of SKU-A with a new price: one UPDATED change.
	e.sup.SetUpdate(testutil.Info("UPDATE-2", 1))
	e.sup.AddPage("", testutil.Page("UPDATE-2", "BATCH-1", "", false, testutil.Active("SKU-A", 2, "Alpha", 150)))
	updated := e.start(t, "UPDATE-2")
	e.mgr.Wait()
	// Revision 2 of SKU-A again with a different price: the batch is refused.
	e.sup.SetUpdate(testutil.Info("UPDATE-3", 1))
	e.sup.AddPage("", testutil.Page("UPDATE-3", "BATCH-1", "", false, testutil.Active("SKU-A", 2, "Alpha", 200)))
	conflicted := e.start(t, "UPDATE-3")
	e.mgr.Wait()

	changes := func(id, query string) []map[string]any {
		return e.get(t, "/api/syncs/"+id+"/changes"+query).want(t, http.StatusOK).items(t)
	}
	issues := func(id, query string) htReply {
		return e.get(t, "/api/syncs/"+id+"/issues"+query).want(t, http.StatusOK)
	}

	// First import: three CREATED changes with no before-state and the stored state as after.
	first := changes(imported, "")
	if got := htProductIDs(first); !slices.Equal(got, []string{"SKU-A", "SKU-B", "SKU-C"}) {
		t.Fatalf("changes of the import = %v, want SKU-A, SKU-B, SKU-C", got)
	}
	for _, c := range first {
		if c["change_type"] != "CREATED" || c["before"] != nil || htObj(t, c, "after")["status"] != "ACTIVE" {
			t.Errorf("import change = %v, want CREATED, before null, after ACTIVE", c)
		}
	}
	if created := changes(imported, "?type=CREATED"); len(created) != 3 {
		t.Errorf("type=CREATED returned %d, want 3", len(created))
	}
	if other := changes(imported, "?type=UPDATED"); len(other) != 0 {
		t.Errorf("type=UPDATED on the import returned %v, want none", other)
	}
	if open := issues(imported, "?status=OPEN"); len(open.items(t)) != 0 || open.object(t)["open_blocking"] != float64(0) {
		t.Errorf("issues of a clean import = %s, want none", open.raw)
	}

	// The price change: before and after carry the old and new price.
	change := changes(updated, "")
	if len(change) != 1 || change[0]["change_type"] != "UPDATED" || change[0]["revision"] != float64(2) ||
		htObj(t, change[0], "before")["price_minor"] != float64(100) || htObj(t, change[0], "after")["price_minor"] != float64(150) {
		t.Errorf("changes of the price update = %v, want one UPDATED r2 from 100 to 150", change)
	}

	// The conflicting update: nothing applied, one open VERSION_CONFLICT naming the product.
	if none := changes(conflicted, ""); len(none) != 0 {
		t.Errorf("changes of the refused update = %v, want none", none)
	}
	open := issues(conflicted, "?status=OPEN")
	openItems := open.items(t)
	if len(openItems) != 1 || open.object(t)["open_blocking"] != float64(1) {
		t.Fatalf("open issues of the refused update = %s, want exactly one", open.raw)
	}
	if c := openItems[0]; c["code"] != "VERSION_CONFLICT" || c["severity"] != "BLOCKING" || c["status"] != "OPEN" ||
		c["supplier_id"] != testutil.SupplierID || c["product_id"] != "SKU-A" {
		t.Errorf("issue = %v, want an open blocking VERSION_CONFLICT for SUP-001/SKU-A", c)
	}
	if resolved := issues(conflicted, "?status=RESOLVED"); len(resolved.items(t)) != 0 {
		t.Errorf("resolved issues = %s, want none", resolved.raw)
	}
	for _, sub := range []string{"/changes", "/issues"} {
		e.get(t, "/api/syncs/"+htRandomUUID(t)+sub).wantError(t, http.StatusNotFound, "NOT_FOUND")
	}

	// What the endpoints report is what PostgreSQL holds, field for field.
	for name, id := range map[string]string{"import": imported, "update": updated, "conflict": conflicted} {
		htAssertSameRows(t, name+" changes", changes(id, ""), htRows(t, e.pool, htChangeRowsSQL, id))
		htAssertSameRows(t, name+" issues", issues(id, "").items(t), htRows(t, e.pool, htIssueRowsSQL, id))

		job, row := e.job(t, id), htRows(t, e.pool, htJobRowSQL, id)[0]
		progress, totals, ignored := htObj(t, job, "progress"), htObj(t, job, "changes"), htObj(t, job, "ignored")
		got := []any{job["status"], progress["processed_batches"], progress["next_cursor"], totals["created"], totals["updated"],
			totals["withdrawn"], ignored["duplicate"], ignored["stale"], job["open_blocking_issues"]}
		want := []any{row["status"], row["processed_batches"], row["next_cursor"], row["created_count"], row["updated_count"],
			row["withdrawn_count"], row["duplicate_count"], row["stale_count"], row["open_blocking"]}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s job: API says %v, database says %v", name, got, want)
		}
	}
	htAssertSameRows(t, "products", e.get(t, "/api/products?status=ALL").want(t, http.StatusOK).items(t), htRows(t, e.pool, htProductRowsSQL))
	for _, id := range []string{imported, updated, conflicted} {
		testutil.AssertCounters(t, e.pool, id)
	}
}

func TestPostRoutesRejectMissingOrWrongTokenAndChangeNothing(t *testing.T) {
	e := htNewEnv(t, htToken)
	htServe(e.sup, "UPDATE-1")
	e.sup.FailTimes("cursor-2", -1)
	stopped := e.start(t, "UPDATE-1") // with the right token
	e.mgr.Wait()
	e.sup.FailTimes("cursor-2", 0)
	before, calls := testutil.Snapshot(t, e.pool), len(e.sup.Calls())

	htServe(e.sup, "UPDATE-2")
	for name, headers := range map[string][]string{
		"no Authorization header": nil,
		"wrong token":             {"Authorization", "Bearer not-the-token"},
		"token without scheme":    {"Authorization", htToken},
		"wrong scheme":            {"Authorization", "Basic " + htToken},
	} {
		for _, tc := range []struct{ route, path, body string }{
			{"start", "/api/syncs", `{"update_id":"UPDATE-2"}`},
			{"start with a broken body", "/api/syncs", `{"update_id":`},
			{"resume", "/api/syncs/" + stopped + "/resume", ""},
			{"resume of an unknown job", "/api/syncs/" + htUnknownJob + "/resume", ""},
		} {
			t.Run(name+"/"+tc.route, func(t *testing.T) {
				r := e.raw(t, http.MethodPost, tc.path, tc.body, headers...)
				r.wantError(t, http.StatusUnauthorized, "UNAUTHORIZED")
				if got := r.header.Get("WWW-Authenticate"); got != "Bearer" {
					t.Errorf("WWW-Authenticate = %q, want Bearer", got)
				}
				if strings.Contains(string(r.raw), htToken) {
					t.Errorf("answer echoes the token: %s", r.raw)
				}
			})
		}
	}
	e.mgr.Wait()
	testutil.AssertUnchanged(t, before, testutil.Snapshot(t, e.pool))
	if after := len(e.sup.Calls()); after != calls {
		t.Errorf("rejected requests made the service call the supplier %d times", after-calls)
	}
	if status := e.job(t, stopped)["status"]; status != "INTERRUPTED" {
		t.Errorf("job after rejected resumes = %v, want it still INTERRUPTED", status)
	}
}

func TestPostRoutesAcceptTheRightTokenAndReadsStayPublic(t *testing.T) {
	e := htNewEnv(t, htToken)
	htServe(e.sup, "UPDATE-1")
	e.sup.FailTimes("cursor-2", -1)
	id := e.start(t, "UPDATE-1") // post() sends the right bearer token: 202
	e.mgr.Wait()
	e.sup.FailTimes("cursor-2", 0)

	resumed := e.post(t, "/api/syncs/"+id+"/resume", "").want(t, http.StatusAccepted).object(t)
	if resumed["job_id"] != id || resumed["status"] != "RUNNING" {
		t.Errorf("resume with the right token = %v, want the job RUNNING", resumed)
	}
	e.awaitStatus(t, id, "COMPLETED")
	e.mgr.Wait()

	// No Authorization header at all on any read.
	for _, path := range []string{
		"/healthz", "/api/syncs", "/api/syncs?status=COMPLETED", "/api/syncs/" + id, "/api/syncs/" + id + "/changes",
		"/api/syncs/" + id + "/issues", "/api/products", "/api/products?status=ALL", "/api/products/SKU-1",
	} {
		if r := e.get(t, path); r.status != http.StatusOK {
			t.Errorf("GET %s without a token = %d, want 200: %s", path, r.status, r.raw)
		}
	}
	// A known update with the right token is the ordinary 200, not a 401.
	e.post(t, "/api/syncs", `{"update_id":"UPDATE-1"}`).want(t, http.StatusOK)
}
