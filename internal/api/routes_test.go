package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/catalog"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/syncjob"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/testutil"
)

// routesUnreachablePool points at a port nothing listens on; pgxpool connects lazily, so creating
// it succeeds and every use fails fast.
func routesUnreachablePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://nobody:nothing@127.0.0.1:1/none?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func routesDecodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not JSON: %v\n%s", err, rec.Body.String())
	}
	return body
}

func TestHealthEndpointAnswers200WhenDatabaseIsUp(t *testing.T) {
	pool := testutil.DB(t)
	h := NewRouter(Deps{Pool: pool, Log: slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("status %d, content-type %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	if body := routesDecodeBody(t, rec); body["status"] != "ok" || body["database"] != "ok" {
		t.Fatalf("body = %v", body)
	}
}

func TestHealthEndpointAnswers503WhenDatabaseIsDown(t *testing.T) {
	h := NewRouter(Deps{Pool: routesUnreachablePool(t), Log: slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if body := routesDecodeBody(t, rec); body["status"] != "unavailable" || body["database"] != "error" {
		t.Fatalf("body = %v", body)
	}
}

func TestRequestLogNamesRoutePatternStatusAndDuration(t *testing.T) {
	var buf bytes.Buffer
	h := NewRouter(Deps{Pool: routesUnreachablePool(t), Log: slog.New(slog.NewJSONHandler(&buf, nil))})

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/healthz", nil))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/no/such/route", nil))

	var events []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		if ev["msg"] == "http.request" {
			events = append(events, ev)
		}
	}
	if len(events) != 2 {
		t.Fatalf("http.request events = %d, want 2:\n%s", len(events), buf.String())
	}
	want := []struct {
		path   string
		status float64
	}{{"/healthz", 503}, {"unmatched", 404}}
	for i, w := range want {
		ev := events[i]
		if ev["method"] != "GET" || ev["path"] != w.path || ev["status"] != w.status {
			t.Errorf("event %d = %v, want GET %s %v", i, ev, w.path, w.status)
		}
		if _, ok := ev["duration_ms"].(float64); !ok {
			t.Errorf("event %d has no numeric duration_ms: %v", i, ev)
		}
	}
}

func TestParsePageAppliesDefaultsAndBounds(t *testing.T) {
	tests := []struct {
		query          string
		limit, offset  int
		wantErrContain string
	}{
		{"", 50, 0, ""},
		{"limit=1&offset=0", 1, 0, ""},
		{"limit=500&offset=40", 500, 40, ""},
		{"offset=7", 50, 7, ""},
		{"limit=0", 0, 0, "limit"},
		{"limit=501", 0, 0, "limit"},
		{"limit=abc", 0, 0, "limit"},
		{"limit=-3", 0, 0, "limit"},
		{"offset=-1", 0, 0, "offset"},
		{"offset=1.5", 0, 0, "offset"},
		{"limit=10&offset=99999999999999999999", 0, 0, "offset"},
	}
	for _, tc := range tests {
		q, _ := url.ParseQuery(tc.query)
		limit, offset, err := parsePage(q)
		switch {
		case tc.wantErrContain != "":
			if err == nil || !strings.Contains(err.Error(), tc.wantErrContain) {
				t.Errorf("parsePage(%q) err = %v, want one mentioning %q", tc.query, err, tc.wantErrContain)
			}
		case err != nil || limit != tc.limit || offset != tc.offset:
			t.Errorf("parsePage(%q) = %d, %d, %v, want %d, %d", tc.query, limit, offset, err, tc.limit, tc.offset)
		}
	}
}

func TestDecodeJSONAcceptsOneKnownObjectOnly(t *testing.T) {
	type body struct {
		UpdateID string `json:"update_id"`
	}
	tests := []struct {
		name, payload string
		wantErr       bool
	}{
		{"valid object", `{"update_id":"U1"}`, false},
		{"valid with trailing whitespace", "{\"update_id\":\"U1\"}\n  ", false},
		{"unknown field", `{"update_id":"U1","extra":1}`, true},
		{"malformed", `{"update_id":`, true},
		{"empty body", ``, true},
		{"two objects", `{"update_id":"U1"}{"update_id":"U2"}`, true},
		{"trailing garbage", `{"update_id":"U1"} x`, true},
		{"array instead of object", `[{"update_id":"U1"}]`, true},
		{"wrong field type", `{"update_id":5}`, true},
		{"over 1 MiB", `{"update_id":"` + strings.Repeat("a", maxBodyBytes) + `"}`, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.payload))
			var got body
			err := decodeJSON(rec, req, &got)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if err == nil && got.UpdateID != "U1" {
				t.Fatalf("decoded %+v", got)
			}
		})
	}
}

func TestErrorStatusMapsDomainAndDatabaseErrors(t *testing.T) {
	tests := []struct {
		err    error
		status int
		code   string
	}{
		{fmt.Errorf("get job: %w", syncjob.ErrNotFound), 404, "NOT_FOUND"},
		{fmt.Errorf("get product: %w", catalog.ErrNotFound), 404, "NOT_FOUND"},
		{fmt.Errorf("start: %w", syncjob.ErrActiveJobExists), 409, "ACTIVE_JOB_EXISTS"},
		{fmt.Errorf("resume: %w", syncjob.ErrInvalidTransition), 409, "INVALID_STATE"},
		{fmt.Errorf("query: %w", &pgconn.PgError{Code: "08006"}), 503, "DB_UNAVAILABLE"},
		{fmt.Errorf("query: %w", context.DeadlineExceeded), 503, "DB_UNAVAILABLE"},
		{errors.New("boom: password=hunter2"), 500, "INTERNAL"},
		{fmt.Errorf("insert: %w", &pgconn.PgError{Code: "23514"}), 500, "INTERNAL"},
	}
	for _, tc := range tests {
		status, code, message := errorStatus(tc.err)
		if status != tc.status || code != tc.code {
			t.Errorf("errorStatus(%v) = %d %s, want %d %s", tc.err, status, code, tc.status, tc.code)
		}
		if strings.Contains(message, "hunter2") || strings.Contains(message, "boom") {
			t.Errorf("message %q leaks the underlying error", message)
		}
	}
}

func TestWriteFailureLogsServerErrorsButNotClientErrors(t *testing.T) {
	var buf bytes.Buffer
	d := Deps{Log: slog.New(slog.NewJSONHandler(&buf, nil))}
	req := httptest.NewRequest(http.MethodGet, "/api/syncs/x", nil)

	rec := httptest.NewRecorder()
	d.writeFailure(rec, req, syncjob.ErrNotFound, nil)
	if rec.Code != 404 || buf.Len() != 0 {
		t.Fatalf("not found: status %d, logged %q", rec.Code, buf.String())
	}

	rec = httptest.NewRecorder()
	d.writeFailure(rec, req, errors.New("disk on fire"), nil)
	if rec.Code != 500 || !strings.Contains(buf.String(), "disk on fire") {
		t.Fatalf("internal: status %d, logged %q", rec.Code, buf.String())
	}
	if strings.Contains(rec.Body.String(), "disk on fire") {
		t.Fatalf("response leaks the error: %s", rec.Body.String())
	}
}

func TestWriteErrorEnvelopeCarriesExtraFieldsButNotOverrides(t *testing.T) {
	rec := httptest.NewRecorder()
	writeError(rec, http.StatusConflict, "ACTIVE_JOB_EXISTS", "another sync job is active",
		map[string]any{"active_job_id": "abc", "code": "HIJACK", "message": "HIJACK"})

	if rec.Code != 409 || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("status %d, content-type %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	errBody, _ := routesDecodeBody(t, rec)["error"].(map[string]any)
	if errBody["code"] != "ACTIVE_JOB_EXISTS" || errBody["message"] != "another sync job is active" || errBody["active_job_id"] != "abc" {
		t.Fatalf("error body = %v", errBody)
	}
}

func TestUUIDPatternAcceptsOnlyCanonicalUUIDs(t *testing.T) {
	for id, want := range map[string]bool{
		"3f7c2b1a-9d4e-4c1a-8b6f-0123456789ab": true,
		"3F7C2B1A-9D4E-4C1A-8B6F-0123456789AB": true,
		"":                                     false,
		"not-a-uuid":                           false,
		"3f7c2b1a9d4e4c1a8b6f0123456789ab":     false,
		"3f7c2b1a-9d4e-4c1a-8b6f-0123456789ab'; DROP TABLE x;--": false,
	} {
		if got := isUUID(id); got != want {
			t.Errorf("isUUID(%q) = %v, want %v", id, got, want)
		}
	}
}

// rtEnv is the router over a real database, a fake supplier and a real Manager, so a
// request can start a job that really runs. Tests drive jobs with a request followed by
// mgr.Wait(), never by calling the worker directly.
type rtEnv struct {
	pool *pgxpool.Pool
	sup  *testutil.FakeSupplier
	mgr  *syncjob.Manager
	h    http.Handler
}

// rtNewEnv builds an empty environment. The supplier timeout is long so a Block()ed fetch
// keeps its job RUNNING until the test releases it or the cleanup shuts the Manager down.
func rtNewEnv(t *testing.T, token string) *rtEnv {
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
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := mgr.Shutdown(ctx); err != nil {
			t.Errorf("manager shutdown: %v", err)
		}
	})
	h := NewRouter(Deps{Pool: pool, Log: log, Manager: mgr, SupplierID: testutil.SupplierID, APIToken: token})
	return &rtEnv{pool: pool, sup: sup, mgr: mgr, h: h}
}

// rtServe makes the fake supplier serve a two-batch update. Applied in order it creates
// SKU-1, SKU-2 and SKU-3, updates SKU-1 and withdraws SKU-2: created 3, updated 1,
// withdrawn 1, nothing ignored.
func rtServe(sup *testutil.FakeSupplier, updateID string) {
	sup.SetUpdate(testutil.Info(updateID, 2))
	sup.AddPage("", testutil.Page(updateID, "BATCH-1", "cursor-2", true,
		testutil.Active("SKU-1", 1, "Widget", 1000), testutil.Active("SKU-2", 1, "Gadget", 2000)))
	sup.AddPage("cursor-2", testutil.Page(updateID, "BATCH-2", "", false,
		testutil.Active("SKU-1", 2, "Widget", 1200), testutil.Withdrawn("SKU-2", 2), testutil.Active("SKU-3", 1, "Gizmo", 300)))
}

// do sends one request with the given headers (name, value pairs) through the router.
func (e *rtEnv) do(t *testing.T, method, path, body string, headers ...string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	e.h.ServeHTTP(rec, req)
	return rec
}

// rtStart starts an update through the API, expects 202 and returns the job id.
func (e *rtEnv) rtStart(t *testing.T, updateID string) string {
	t.Helper()
	rec := e.do(t, http.MethodPost, "/api/syncs", `{"update_id":"`+updateID+`"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("start %s = %d, want 202: %s", updateID, rec.Code, rec.Body.String())
	}
	id, _ := routesDecodeBody(t, rec)["job_id"].(string)
	if id == "" {
		t.Fatalf("start %s: no job_id in %s", updateID, rec.Body.String())
	}
	return id
}

// rtWaitForFetches returns once the fake has logged n fetches at cursor. A fetch is logged
// before it starts waiting, so the job is RUNNING and parked at the supplier by then,
// without sleeping a guessed duration.
func rtWaitForFetches(t *testing.T, sup *testutil.FakeSupplier, cursor string, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for sup.FetchCalls(cursor) < n {
		if time.Now().After(deadline) {
			t.Fatalf("fetch at %q not seen %d times", cursor, n)
		}
		runtime.Gosched()
	}
}

// rtKeys is the sorted key set of a decoded JSON object, to assert a response shape.
func rtKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func rtAssertKeys(t *testing.T, what string, m map[string]any, want ...string) {
	t.Helper()
	slices.Sort(want)
	if got := rtKeys(m); !slices.Equal(got, want) {
		t.Errorf("%s keys = %v, want %v", what, got, want)
	}
}

// rtObject returns m[key] as a JSON object, failing the test when it is not one.
func rtObject(t *testing.T, m map[string]any, key string) map[string]any {
	t.Helper()
	obj, ok := m[key].(map[string]any)
	if !ok {
		t.Fatalf("%q is not an object in %v", key, m)
	}
	return obj
}

// rtAssertError checks the status and the error envelope code and returns the error object.
func rtAssertError(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) map[string]any {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d: %s", rec.Code, status, rec.Body.String())
	}
	body := routesDecodeBody(t, rec)
	rtAssertKeys(t, "error response", body, "error")
	errObj := rtObject(t, body, "error")
	if errObj["code"] != code {
		t.Fatalf("error code = %v, want %s: %s", errObj["code"], code, rec.Body.String())
	}
	if msg, _ := errObj["message"].(string); msg == "" {
		t.Errorf("error has no message: %s", rec.Body.String())
	}
	return errObj
}

// rtCountJobs is the number of rows in sync_jobs.
func rtCountJobs(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM sync_jobs`).Scan(&n); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	return n
}

var rtJobKeys = []string{
	"job_id", "status", "supplier_id", "supplier_update_id", "progress", "changes", "ignored",
	"open_blocking_issues", "last_error", "created_at", "started_at", "completed_at", "updated_at",
}

func TestStartSyncAnswers202ForNewUpdateAndThen200WithoutResuming(t *testing.T) {
	env := rtNewEnv(t, "")
	rtServe(env.sup, "UPDATE-1")

	rec := env.do(t, http.MethodPost, "/api/syncs", `{"update_id":"UPDATE-1"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("first start = %d, want 202: %s", rec.Code, rec.Body.String())
	}
	job := routesDecodeBody(t, rec)
	id, _ := job["job_id"].(string)
	if want := "/api/syncs/" + id; id == "" || rec.Header().Get("Location") != want {
		t.Fatalf("Location = %q, want %q", rec.Header().Get("Location"), want)
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Errorf("content-type = %q", rec.Header().Get("Content-Type"))
	}
	rtAssertKeys(t, "job", job, rtJobKeys...)
	rtAssertKeys(t, "job.progress", rtObject(t, job, "progress"),
		"processed_batches", "total_batches", "next_cursor", "last_batch_id", "end_of_feed_seen")
	rtAssertKeys(t, "job.changes", rtObject(t, job, "changes"), "created", "updated", "withdrawn")
	rtAssertKeys(t, "job.ignored", rtObject(t, job, "ignored"), "duplicate", "stale")
	if job["supplier_id"] != testutil.SupplierID || job["supplier_update_id"] != "UPDATE-1" {
		t.Errorf("job identity = %v / %v", job["supplier_id"], job["supplier_update_id"])
	}

	env.mgr.Wait()
	callsBefore := len(env.sup.Calls())

	rec = env.do(t, http.MethodPost, "/api/syncs", `{"update_id":"UPDATE-1"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("repeated start = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	again := routesDecodeBody(t, rec)
	if again["job_id"] != id || again["status"] != "COMPLETED" {
		t.Fatalf("repeated start returned job %v status %v, want %s COMPLETED", again["job_id"], again["status"], id)
	}
	progress := rtObject(t, again, "progress")
	if progress["processed_batches"] != float64(2) || progress["total_batches"] != float64(2) ||
		progress["end_of_feed_seen"] != true || progress["last_batch_id"] != "BATCH-2" || progress["next_cursor"] != "" {
		t.Errorf("progress = %v", progress)
	}
	if got := rtObject(t, again, "changes"); got["created"] != float64(3) || got["updated"] != float64(1) || got["withdrawn"] != float64(1) {
		t.Errorf("changes = %v", got)
	}
	if again["completed_at"] == nil || again["started_at"] == nil || again["last_error"] != nil || again["open_blocking_issues"] != float64(0) {
		t.Errorf("completed job fields = %v", again)
	}
	env.mgr.Wait()
	if after := len(env.sup.Calls()); after != callsBefore {
		t.Errorf("repeated start contacted the supplier %d more times; a known update must not be resumed", after-callsBefore)
	}
}

func TestStartSyncUsesTheDefaultSupplierUnlessOneIsGiven(t *testing.T) {
	env := rtNewEnv(t, "")
	rtServe(env.sup, "UPDATE-1")

	rec := env.do(t, http.MethodPost, "/api/syncs", `{"update_id":"UPDATE-1","supplier_id":"SUP-777"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("start = %d: %s", rec.Code, rec.Body.String())
	}
	if got := routesDecodeBody(t, rec)["supplier_id"]; got != "SUP-777" {
		t.Errorf("supplier_id = %v, want the one in the request", got)
	}
	env.mgr.Wait()
}

func TestStartSyncRejectsInvalidRequestsAndCreatesNoJob(t *testing.T) {
	env := rtNewEnv(t, "")
	tests := []struct {
		name, body string
		status     int
		code       string
	}{
		{"truncated JSON", `{"update_id":`, 400, "INVALID_JSON"},
		{"empty body", ``, 400, "INVALID_JSON"},
		{"unknown field", `{"update_id":"U1","extra":1}`, 400, "INVALID_JSON"},
		{"update_id of the wrong type", `{"update_id":5}`, 400, "INVALID_JSON"},
		{"array instead of object", `[{"update_id":"U1"}]`, 400, "INVALID_JSON"},
		{"two objects", `{"update_id":"U1"}{"update_id":"U2"}`, 400, "INVALID_JSON"},
		{"missing update_id", `{}`, 422, "VALIDATION_FAILED"},
		{"empty update_id", `{"update_id":""}`, 422, "VALIDATION_FAILED"},
		{"update_id with a space", `{"update_id":"bad id!"}`, 422, "VALIDATION_FAILED"},
		{"update_id that looks like a path", `{"update_id":"../etc/passwd"}`, 422, "VALIDATION_FAILED"},
		{"update_id with a trailing newline", `{"update_id":"U1\n"}`, 422, "VALIDATION_FAILED"},
		{"update_id of 65 characters", `{"update_id":"` + strings.Repeat("a", 65) + `"}`, 422, "VALIDATION_FAILED"},
		{"empty supplier_id", `{"update_id":"U1","supplier_id":""}`, 422, "VALIDATION_FAILED"},
		{"supplier_id with a space", `{"update_id":"U1","supplier_id":"SUP 1"}`, 422, "VALIDATION_FAILED"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rtAssertError(t, env.do(t, http.MethodPost, "/api/syncs", tc.body), tc.status, tc.code)
		})
	}
	if n := rtCountJobs(t, env.pool); n != 0 {
		t.Fatalf("jobs after rejected requests = %d, want none", n)
	}
}

func TestStartSyncWhileAnotherJobIsActiveNamesTheActiveJobInsideTheError(t *testing.T) {
	env := rtNewEnv(t, "")
	rtServe(env.sup, "UPDATE-1")
	rtServe(env.sup, "UPDATE-2")
	env.sup.Block("")
	first := env.rtStart(t, "UPDATE-1")

	rec := env.do(t, http.MethodPost, "/api/syncs", `{"update_id":"UPDATE-2"}`)
	errObj := rtAssertError(t, rec, http.StatusConflict, "ACTIVE_JOB_EXISTS")
	if errObj["active_job_id"] != first {
		t.Errorf("active_job_id = %v, want %s", errObj["active_job_id"], first)
	}
	rtAssertKeys(t, "conflict error", errObj, "code", "message", "active_job_id")
	if n := rtCountJobs(t, env.pool); n != 1 {
		t.Errorf("jobs after the refused start = %d, want 1", n)
	}
	env.sup.Unblock("")
	env.mgr.Wait()
}

// rtItems returns the "items" array of a list response as objects.
func rtItems(t *testing.T, body map[string]any) []map[string]any {
	t.Helper()
	raw, ok := body["items"].([]any)
	if !ok {
		t.Fatalf("items is not an array in %v", body)
	}
	items := make([]map[string]any, len(raw))
	for i, v := range raw {
		if items[i], ok = v.(map[string]any); !ok {
			t.Fatalf("items[%d] is not an object: %v", i, v)
		}
	}
	return items
}

// rtUpdateIDs lists supplier_update_id of each job item, in response order.
func rtUpdateIDs(items []map[string]any) []string {
	ids := make([]string, len(items))
	for i, it := range items {
		ids[i], _ = it["supplier_update_id"].(string)
	}
	return ids
}

// rtThreeJobs leaves UPDATE-1 and UPDATE-2 COMPLETED and UPDATE-3 INTERRUPTED after its
// first batch (the supplier fails at cursor-2 until the test says otherwise). It returns
// the job ids in that order.
func rtThreeJobs(t *testing.T, env *rtEnv) (first, second, third string) {
	t.Helper()
	rtServe(env.sup, "UPDATE-1")
	first = env.rtStart(t, "UPDATE-1")
	env.mgr.Wait()
	rtServe(env.sup, "UPDATE-2")
	second = env.rtStart(t, "UPDATE-2")
	env.mgr.Wait()
	rtServe(env.sup, "UPDATE-3")
	env.sup.FailTimes("cursor-2", -1)
	third = env.rtStart(t, "UPDATE-3")
	env.mgr.Wait()
	return first, second, third
}

func TestListSyncsReturnsNewestFirstAndHonoursStatusAndPaging(t *testing.T) {
	env := rtNewEnv(t, "")
	_, _, third := rtThreeJobs(t, env)

	rec := env.do(t, http.MethodGet, "/api/syncs", "")
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("list = %d %q: %s", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}
	body := routesDecodeBody(t, rec)
	rtAssertKeys(t, "job list", body, "items", "limit", "offset")
	if body["limit"] != float64(50) || body["offset"] != float64(0) {
		t.Errorf("limit/offset = %v/%v, want the defaults 50/0", body["limit"], body["offset"])
	}
	items := rtItems(t, body)
	if got := rtUpdateIDs(items); !slices.Equal(got, []string{"UPDATE-3", "UPDATE-2", "UPDATE-1"}) {
		t.Fatalf("order = %v, want newest first", got)
	}
	for _, it := range items {
		rtAssertKeys(t, "job item", it, rtJobKeys...)
	}
	interrupted := items[0]
	progress := rtObject(t, interrupted, "progress")
	if interrupted["job_id"] != third || interrupted["status"] != "INTERRUPTED" ||
		progress["processed_batches"] != float64(1) || progress["total_batches"] != float64(2) ||
		progress["next_cursor"] != "cursor-2" || progress["last_batch_id"] != "BATCH-1" || progress["end_of_feed_seen"] != false {
		t.Errorf("interrupted job = %v", interrupted)
	}
	if msg, _ := interrupted["last_error"].(string); !strings.Contains(msg, "cursor-2") {
		t.Errorf("last_error = %v, want the reason naming the cursor", interrupted["last_error"])
	}
	if interrupted["completed_at"] != nil {
		t.Errorf("an interrupted job has completed_at = %v", interrupted["completed_at"])
	}

	paged := routesDecodeBody(t, env.do(t, http.MethodGet, "/api/syncs?limit=1&offset=1", ""))
	if got := rtUpdateIDs(rtItems(t, paged)); !slices.Equal(got, []string{"UPDATE-2"}) || paged["limit"] != float64(1) || paged["offset"] != float64(1) {
		t.Errorf("page = %v (limit %v, offset %v), want only UPDATE-2 echoing limit 1 offset 1", got, paged["limit"], paged["offset"])
	}
	completed := routesDecodeBody(t, env.do(t, http.MethodGet, "/api/syncs?status=COMPLETED", ""))
	if got := rtUpdateIDs(rtItems(t, completed)); !slices.Equal(got, []string{"UPDATE-2", "UPDATE-1"}) {
		t.Errorf("COMPLETED filter = %v", got)
	}

	empty := env.do(t, http.MethodGet, "/api/syncs?status=NEEDS_ATTENTION", "")
	if empty.Code != http.StatusOK || !strings.Contains(empty.Body.String(), `"items":[]`) {
		t.Errorf("no match = %d %s, want 200 with an empty array, not null", empty.Code, empty.Body.String())
	}
}

func TestListSyncsRejectsInvalidParametersWith422(t *testing.T) {
	env := rtNewEnv(t, "")
	for _, query := range []string{
		"limit=0", "limit=501", "limit=abc", "limit=-1", "offset=-1", "offset=1.5",
		"status=BOGUS", "status=completed", "status=COMPLETED,FAILED",
	} {
		t.Run(query, func(t *testing.T) {
			rtAssertError(t, env.do(t, http.MethodGet, "/api/syncs?"+query, ""), http.StatusUnprocessableEntity, "VALIDATION_FAILED")
		})
	}
}

func TestGetSyncReturnsTheJobAndAnswers404ForUnknownOrMalformedIds(t *testing.T) {
	env := rtNewEnv(t, "")
	first, _, _ := rtThreeJobs(t, env)

	rec := env.do(t, http.MethodGet, "/api/syncs/"+first, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("get = %d: %s", rec.Code, rec.Body.String())
	}
	job := routesDecodeBody(t, rec)
	rtAssertKeys(t, "job", job, rtJobKeys...)
	if job["job_id"] != first || job["status"] != "COMPLETED" || job["supplier_update_id"] != "UPDATE-1" {
		t.Errorf("job = %v", job)
	}

	for _, id := range []string{
		"00000000-0000-4000-8000-000000000000", "not-a-uuid", "3f7c2b1a9d4e4c1a8b6f0123456789ab", "1;DROP TABLE sync_jobs",
	} {
		rec := env.do(t, http.MethodGet, "/api/syncs/"+url.PathEscape(id), "")
		rtAssertError(t, rec, http.StatusNotFound, "NOT_FOUND")
	}
}

func TestResumeAnswers202ContinuesTheJobAndRefusesWhatCannotBeResumed(t *testing.T) {
	env := rtNewEnv(t, "")
	rtServe(env.sup, "UPDATE-1")
	env.sup.FailTimes("cursor-2", -1)
	id := env.rtStart(t, "UPDATE-1")
	env.mgr.Wait() // supplier unavailable at cursor-2: INTERRUPTED after the first batch

	// The supplier is back but hangs, so the resumed job stays RUNNING while it is inspected.
	env.sup.FailTimes("cursor-2", 0)
	env.sup.Block("cursor-2")
	failedFetches := env.sup.FetchCalls("cursor-2")

	rec := env.do(t, http.MethodPost, "/api/syncs/"+id+"/resume", "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("resume = %d, want 202: %s", rec.Code, rec.Body.String())
	}
	job := routesDecodeBody(t, rec)
	rtAssertKeys(t, "job", job, rtJobKeys...)
	if progress := rtObject(t, job, "progress"); job["job_id"] != id || job["status"] != "RUNNING" || progress["processed_batches"] != float64(1) {
		t.Fatalf("resumed job = %v, want the same job RUNNING with its first batch kept", job)
	}

	rtWaitForFetches(t, env.sup, "cursor-2", failedFetches+1)
	errObj := rtAssertError(t, env.do(t, http.MethodPost, "/api/syncs/"+id+"/resume", ""), http.StatusConflict, "INVALID_STATE")
	if errObj["current_status"] != "RUNNING" {
		t.Errorf("current_status = %v, want RUNNING for a job whose worker is alive", errObj["current_status"])
	}
	rtAssertKeys(t, "invalid state error", errObj, "code", "message", "current_status")

	env.sup.Unblock("cursor-2")
	env.mgr.Wait()
	done := routesDecodeBody(t, env.do(t, http.MethodGet, "/api/syncs/"+id, ""))
	if changes := rtObject(t, done, "changes"); done["status"] != "COMPLETED" || changes["created"] != float64(3) ||
		changes["updated"] != float64(1) || changes["withdrawn"] != float64(1) || done["last_error"] != nil {
		t.Fatalf("job after resume = %v, want COMPLETED with the full counters and no leftover error", done)
	}

	errObj = rtAssertError(t, env.do(t, http.MethodPost, "/api/syncs/"+id+"/resume", ""), http.StatusConflict, "INVALID_STATE")
	if errObj["current_status"] != "COMPLETED" {
		t.Errorf("current_status = %v, want COMPLETED", errObj["current_status"])
	}
}

func TestResumeAnswers404ForUnknownOrMalformedJobIds(t *testing.T) {
	env := rtNewEnv(t, "")
	for _, id := range []string{"00000000-0000-4000-8000-000000000000", "not-a-uuid"} {
		rtAssertError(t, env.do(t, http.MethodPost, "/api/syncs/"+id+"/resume", ""), http.StatusNotFound, "NOT_FOUND")
	}
}

func TestResumeWhileAnotherJobIsActiveAnswers409ActiveJobExists(t *testing.T) {
	env := rtNewEnv(t, "")
	rtServe(env.sup, "UPDATE-1")
	env.sup.FailTimes("cursor-2", -1)
	stopped := env.rtStart(t, "UPDATE-1")
	env.mgr.Wait()

	rtServe(env.sup, "UPDATE-2")
	env.sup.Block("")
	env.rtStart(t, "UPDATE-2")
	rtWaitForFetches(t, env.sup, "", 1)

	errObj := rtAssertError(t, env.do(t, http.MethodPost, "/api/syncs/"+stopped+"/resume", ""), http.StatusConflict, "ACTIVE_JOB_EXISTS")
	rtAssertKeys(t, "active job error", errObj, "code", "message")
	if got := routesDecodeBody(t, env.do(t, http.MethodGet, "/api/syncs/"+stopped, ""))["status"]; got != "INTERRUPTED" {
		t.Errorf("refused job status = %v, want it left INTERRUPTED", got)
	}
	env.sup.Unblock("")
	env.mgr.Wait()
}

// rtServeBroken makes the fake supplier serve an update whose second batch carries a
// product with a negative price, so the job stops at cursor-2 with a blocking issue after
// committing the first batch (SKU-1 created).
func rtServeBroken(sup *testutil.FakeSupplier, updateID string) {
	sup.SetUpdate(testutil.Info(updateID, 2))
	sup.AddPage("", testutil.Page(updateID, "BATCH-1", "cursor-2", true, testutil.Active("SKU-1", 1, "Widget", 1000)))
	sup.AddPage("cursor-2", testutil.Page(updateID, "BATCH-2", "", false, testutil.Active("SKU-9", 1, "Broken", -5)))
}

var rtChangeKeys = []string{"id", "batch_id", "supplier_id", "product_id", "revision", "change_type", "before", "after", "created_at"}

var rtIssueKeys = []string{
	"id", "batch_id", "cursor", "supplier_id", "product_id", "code", "severity", "status", "message",
	"retry_count", "first_seen_at", "last_seen_at", "resolved_at",
}

func TestChangesEndpointListsAppliedChangesInOrderWithBeforeAndAfter(t *testing.T) {
	env := rtNewEnv(t, "")
	rtServe(env.sup, "UPDATE-1")
	id := env.rtStart(t, "UPDATE-1")
	env.mgr.Wait()

	rec := env.do(t, http.MethodGet, "/api/syncs/"+id+"/changes", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("changes = %d: %s", rec.Code, rec.Body.String())
	}
	body := routesDecodeBody(t, rec)
	rtAssertKeys(t, "change list", body, "items")
	items := rtItems(t, body)
	var order []string
	for _, it := range items {
		rtAssertKeys(t, "change item", it, rtChangeKeys...)
		order = append(order, fmt.Sprintf("%v %v r%v", it["change_type"], it["product_id"], it["revision"]))
	}
	want := []string{"CREATED SKU-1 r1", "CREATED SKU-2 r1", "UPDATED SKU-1 r2", "WITHDRAWN SKU-2 r2", "CREATED SKU-3 r1"}
	if !slices.Equal(order, want) {
		t.Fatalf("changes = %v, want %v", order, want)
	}

	created := items[0]
	if created["before"] != nil || created["supplier_id"] != testutil.SupplierID || created["batch_id"] != "BATCH-1" {
		t.Errorf("created change = %v, want a null before-state", created)
	}
	if after := rtObject(t, created, "after"); after["status"] != "ACTIVE" || after["revision"] != float64(1) ||
		after["name"] != "Widget" || after["price_minor"] != float64(1000) || after["currency"] != "USD" {
		t.Errorf("created after-state = %v", after)
	}
	updated := items[2]
	if before, after := rtObject(t, updated, "before"), rtObject(t, updated, "after"); before["price_minor"] != float64(1000) || after["price_minor"] != float64(1200) {
		t.Errorf("updated change before/after = %v / %v", before, after)
	}
	withdrawn := items[3]
	if before, after := rtObject(t, withdrawn, "before"), rtObject(t, withdrawn, "after"); before["status"] != "ACTIVE" || after["status"] != "WITHDRAWN" {
		t.Errorf("withdrawn change before/after = %v / %v", before, after)
	}

	filtered := routesDecodeBody(t, env.do(t, http.MethodGet, "/api/syncs/"+id+"/changes?type=CREATED", ""))
	if n := len(rtItems(t, filtered)); n != 3 {
		t.Errorf("type=CREATED returned %d items, want 3", n)
	}
	paged := rtItems(t, routesDecodeBody(t, env.do(t, http.MethodGet, "/api/syncs/"+id+"/changes?limit=2&offset=1", "")))
	if len(paged) != 2 || paged[0]["product_id"] != "SKU-2" || paged[1]["change_type"] != "UPDATED" {
		t.Errorf("limit=2&offset=1 returned %v", paged)
	}
	past := env.do(t, http.MethodGet, "/api/syncs/"+id+"/changes?offset=50", "")
	if past.Code != http.StatusOK || !strings.Contains(past.Body.String(), `"items":[]`) {
		t.Errorf("offset past the end = %d %s, want 200 with an empty array", past.Code, past.Body.String())
	}
}

func TestChangesAndIssuesEndpointsRejectBadParametersAndUnknownJobs(t *testing.T) {
	env := rtNewEnv(t, "")
	rtServe(env.sup, "UPDATE-1")
	id := env.rtStart(t, "UPDATE-1")
	env.mgr.Wait()

	for _, path := range []string{
		"/changes?type=BOGUS", "/changes?type=created", "/changes?limit=0", "/changes?limit=501", "/changes?offset=-1",
		"/issues?status=BOGUS", "/issues?status=open", "/issues?severity=BOGUS", "/issues?limit=0", "/issues?offset=x",
	} {
		t.Run(path, func(t *testing.T) {
			rtAssertError(t, env.do(t, http.MethodGet, "/api/syncs/"+id+path, ""), http.StatusUnprocessableEntity, "VALIDATION_FAILED")
		})
	}
	for _, job := range []string{"00000000-0000-4000-8000-000000000000", "not-a-uuid"} {
		for _, sub := range []string{"/changes", "/issues"} {
			rtAssertError(t, env.do(t, http.MethodGet, "/api/syncs/"+job+sub, ""), http.StatusNotFound, "NOT_FOUND")
		}
	}
}

func TestIssuesEndpointShowsBlockingIssueThenItsResolutionAfterTheSupplierCorrectsTheBatch(t *testing.T) {
	env := rtNewEnv(t, "")
	rtServeBroken(env.sup, "UPDATE-1")
	id := env.rtStart(t, "UPDATE-1")
	env.mgr.Wait()

	job := routesDecodeBody(t, env.do(t, http.MethodGet, "/api/syncs/"+id, ""))
	if job["status"] != "NEEDS_ATTENTION" || job["open_blocking_issues"] != float64(1) || job["last_error"] == nil {
		t.Fatalf("job = %v, want NEEDS_ATTENTION with one open blocking issue", job)
	}

	body := routesDecodeBody(t, env.do(t, http.MethodGet, "/api/syncs/"+id+"/issues", ""))
	rtAssertKeys(t, "issue list", body, "items", "open_blocking")
	items := rtItems(t, body)
	if len(items) != 1 || body["open_blocking"] != float64(1) {
		t.Fatalf("issues = %v, want exactly one with open_blocking 1", body)
	}
	issue := items[0]
	rtAssertKeys(t, "issue item", issue, rtIssueKeys...)
	if issue["code"] != "INVALID_PRODUCT_DATA" || issue["severity"] != "BLOCKING" || issue["status"] != "OPEN" ||
		issue["cursor"] != "cursor-2" || issue["retry_count"] != float64(0) || issue["resolved_at"] != nil {
		t.Errorf("issue = %v", issue)
	}
	if msg, _ := issue["message"].(string); !strings.Contains(msg, "SKU-9") {
		t.Errorf("message %q does not name the offending product", msg)
	}

	// Filters narrow the items but never the job-level blocking count.
	for query, wantItems := range map[string]int{
		"status=OPEN": 1, "status=RESOLVED": 0, "severity=BLOCKING": 1, "severity=TRANSIENT": 0,
		"status=OPEN&severity=BLOCKING": 1, "status=RESOLVED&severity=BLOCKING": 0,
	} {
		filtered := routesDecodeBody(t, env.do(t, http.MethodGet, "/api/syncs/"+id+"/issues?"+query, ""))
		if n := len(rtItems(t, filtered)); n != wantItems || filtered["open_blocking"] != float64(1) {
			t.Errorf("?%s = %d items, open_blocking %v; want %d items and 1", query, n, filtered["open_blocking"], wantItems)
		}
	}

	// The supplier corrects the batch; resuming commits it and closes the issue.
	env.sup.AddPage("cursor-2", testutil.Page("UPDATE-1", "BATCH-2", "", false, testutil.Active("SKU-2", 1, "Gadget", 2000)))
	if rec := env.do(t, http.MethodPost, "/api/syncs/"+id+"/resume", ""); rec.Code != http.StatusAccepted {
		t.Fatalf("resume = %d: %s", rec.Code, rec.Body.String())
	}
	env.mgr.Wait()

	after := routesDecodeBody(t, env.do(t, http.MethodGet, "/api/syncs/"+id+"/issues", ""))
	resolved := rtItems(t, after)
	if len(resolved) != 1 || after["open_blocking"] != float64(0) || resolved[0]["status"] != "RESOLVED" || resolved[0]["resolved_at"] == nil {
		t.Errorf("issues after the correction = %v, want the same issue RESOLVED and open_blocking 0", after)
	}
	if got := routesDecodeBody(t, env.do(t, http.MethodGet, "/api/syncs/"+id, ""))["status"]; got != "COMPLETED" {
		t.Errorf("job status after the correction = %v, want COMPLETED", got)
	}
}

func TestIssuesEndpointShowsSupplierOutageAsTransientWithAttemptCount(t *testing.T) {
	env := rtNewEnv(t, "")
	rtServe(env.sup, "UPDATE-1")
	env.sup.FailTimes("cursor-2", -1)
	id := env.rtStart(t, "UPDATE-1")
	env.mgr.Wait()

	body := routesDecodeBody(t, env.do(t, http.MethodGet, "/api/syncs/"+id+"/issues?status=OPEN", ""))
	items := rtItems(t, body)
	if len(items) != 1 || body["open_blocking"] != float64(0) {
		t.Fatalf("issues = %v, want one open issue that does not block completion", body)
	}
	if it := items[0]; it["code"] != "SUPPLIER_UNAVAILABLE" || it["severity"] != "TRANSIENT" ||
		it["cursor"] != "cursor-2" || it["retry_count"] != float64(2) {
		t.Errorf("issue = %v, want SUPPLIER_UNAVAILABLE TRANSIENT at cursor-2 after 2 attempts", it)
	}
}
