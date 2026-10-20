package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
)

// RequireToken protects next with a static bearer token. An empty token disables the
// check, so local runs need no setup while a public deployment keeps its write routes
// closed. The token is never logged or echoed.
func RequireToken(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	want := sha256.Sum256([]byte(token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scheme, given, _ := strings.Cut(r.Header.Get("Authorization"), " ")
		// Hashing both sides keeps the comparison constant-time even when lengths differ.
		got := sha256.Sum256([]byte(strings.TrimSpace(given)))
		if !strings.EqualFold(scheme, "Bearer") || subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"code":"UNAUTHORIZED","message":"missing or invalid bearer token"}}` + "\n"))
			return
		}
		next.ServeHTTP(w, r)
	})
}
