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
