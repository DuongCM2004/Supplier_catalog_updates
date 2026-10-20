package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func serveWithToken(token, authorization string) *httptest.ResponseRecorder {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) })
	req := httptest.NewRequest(http.MethodPost, "/api/syncs", strings.NewReader(`{}`))
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	rec := httptest.NewRecorder()
	RequireToken(token, ok).ServeHTTP(rec, req)
	return rec
}

func TestRequireTokenIsDisabledWhenTokenEmpty(t *testing.T) {
	if rec := serveWithToken("", ""); rec.Code != http.StatusAccepted {
		t.Fatalf("status %d, want 202 (guard off)", rec.Code)
	}
}

func TestRequireTokenRejectsMissingOrWrongToken(t *testing.T) {
	for _, header := range []string{"", "Bearer wrong", "Basic s3cret", "Bearer", "s3cret"} {
		rec := serveWithToken("s3cret", header)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("header %q: status %d, want 401", header, rec.Code)
		}
		if rec.Header().Get("WWW-Authenticate") != "Bearer" || !strings.Contains(rec.Body.String(), `"code":"UNAUTHORIZED"`) {
			t.Fatalf("header %q: missing challenge or error envelope: %q", header, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "s3cret") {
			t.Fatalf("response leaks the token")
		}
	}
}

func TestRequireTokenPassesMatchingToken(t *testing.T) {
	for _, header := range []string{"Bearer s3cret", "bearer s3cret"} {
		if rec := serveWithToken("s3cret", header); rec.Code != http.StatusAccepted {
			t.Fatalf("header %q: status %d, want 202", header, rec.Code)
		}
	}
}
