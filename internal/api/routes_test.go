package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

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
