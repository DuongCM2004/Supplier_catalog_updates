// Package api is the HTTP layer: routing, JSON and error envelopes, and request logging.
// Handlers translate between HTTP and the sync job and catalog packages and hold no
// business rules of their own.
package api

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/syncjob"
)

// Deps is everything the handlers need. Build it with field names: endpoints added later
// append fields here.
type Deps struct {
	Pool       *pgxpool.Pool
	Log        *slog.Logger
	Manager    *syncjob.Manager // starts and resumes sync jobs; the job routes use it
	SupplierID string           // default supplier for starting syncs and looking up products
	APIToken   string           // when set, POST routes require "Authorization: Bearer <token>"
}

// NewRouter returns the full HTTP handler: every route wrapped in request logging.
func NewRouter(d Deps) http.Handler {
	if d.Log == nil {
		d.Log = slog.Default()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", d.healthz)
	mux.HandleFunc("POST /api/syncs", d.startSync)
	mux.HandleFunc("GET /api/syncs", d.listSyncs)
	mux.HandleFunc("GET /api/syncs/{id}", d.getSync)
	mux.HandleFunc("POST /api/syncs/{id}/resume", d.resumeSync)
	mux.HandleFunc("GET /api/syncs/{id}/changes", d.listChanges)
	mux.HandleFunc("GET /api/syncs/{id}/issues", d.listIssues)
	return d.requestLog(mux)
}

// requestLog writes one http.request event per request, labelled with the matched route
// pattern rather than the raw URL so job ids do not create a new label per request.
func (d Deps) requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		// ServeMux records the matched pattern on r while routing, so it is read afterwards.
		d.Log.Info("http.request",
			"method", r.Method,
			"path", routePath(r),
			"status", rec.status,
			"duration_ms", float64(time.Since(start).Microseconds())/1000)
	})
}

// routePath is the matched route without its method ("/api/syncs/{id}"), or "unmatched"
// for requests no route accepted.
func routePath(r *http.Request) string {
	if r.Pattern == "" {
		return "unmatched"
	}
	if _, path, ok := strings.Cut(r.Pattern, " "); ok {
		return path
	}
	return r.Pattern
}

// statusRecorder remembers the first status written, which is the one the client sees.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.wroteHeader {
		s.status, s.wroteHeader = code, true
	}
	s.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach the real writer (flush, deadlines).
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }
