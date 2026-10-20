package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/catalog"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/storage"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/syncjob"
)

const (
	maxBodyBytes  = 1 << 20 // 1 MiB: far above any real request, small enough to refuse abuse
	defaultLimit  = 50
	maxLimit      = 500
	healthTimeout = time.Second
)

// uuidPattern guards every job id taken from a URL before it reaches SQL; anything else is
// answered as "not found" instead of a database cast error.
var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func isUUID(s string) bool { return uuidPattern.MatchString(s) }

// writeJSON encodes before writing, so a value that cannot be encoded produces a clean 500
// instead of a truncated 200.
func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		status = http.StatusInternalServerError
		body = []byte(`{"error":{"code":"INTERNAL","message":"internal error"}}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// A failed write means the client went away; there is nobody left to tell.
	_, _ = w.Write(append(body, '\n'))
}

// writeError sends the error envelope {"error":{"code","message",...extra}}. Extra fields
// (for example active_job_id) sit next to code and message.
func writeError(w http.ResponseWriter, status int, code, message string, extra map[string]any) {
	body := map[string]any{"code": code, "message": message}
	for k, v := range extra {
		if k != "code" && k != "message" {
			body[k] = v
		}
	}
	writeJSON(w, status, map[string]any{"error": body})
}

// errorStatus maps a domain or database error to its HTTP answer. Unknown errors are 500
// with a generic message: their details belong in the log, not in the response.
func errorStatus(err error) (status int, code, message string) {
	switch {
	case errors.Is(err, syncjob.ErrNotFound), errors.Is(err, catalog.ErrNotFound):
		return http.StatusNotFound, "NOT_FOUND", "resource not found"
	case errors.Is(err, syncjob.ErrActiveJobExists):
		return http.StatusConflict, "ACTIVE_JOB_EXISTS", "another sync job is active"
	case errors.Is(err, syncjob.ErrInvalidTransition):
		return http.StatusConflict, "INVALID_STATE", "the job is not in a state that allows this operation"
	case storage.IsConnectionError(err):
		return http.StatusServiceUnavailable, "DB_UNAVAILABLE", "database unavailable, retry shortly"
	}
	return http.StatusInternalServerError, "INTERNAL", "internal error"
}

// writeFailure answers with the mapped status and logs what the client must not see.
func (d Deps) writeFailure(w http.ResponseWriter, r *http.Request, err error, extra map[string]any) {
	status, code, message := errorStatus(err)
	switch status {
	case http.StatusInternalServerError:
		d.Log.Error("http.error", "method", r.Method, "path", r.URL.Path, "error", err)
	case http.StatusServiceUnavailable:
		d.Log.Warn("http.database_unavailable", "method", r.Method, "path", r.URL.Path, "error", err)
	}
	writeError(w, status, code, message, extra)
}

// decodeJSON reads exactly one JSON object into dst: at most 1 MiB, no unknown fields, no
// trailing data. Every failure is a client error, so callers answer 400 INVALID_JSON.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		if errors.Is(err, io.EOF) {
			return errors.New("request body is empty")
		}
		return fmt.Errorf("invalid request body: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain a single JSON object")
	}
	return nil
}

// parsePage reads limit (1-500, default 50) and offset (>= 0, default 0). Invalid values
// are an error for the caller to answer with 422.
func parsePage(q url.Values) (limit, offset int, err error) {
	limit = defaultLimit
	if raw := q.Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > maxLimit {
			return 0, 0, fmt.Errorf("limit must be an integer between 1 and %d", maxLimit)
		}
	}
	if raw := q.Get("offset"); raw != "" {
		offset, err = strconv.Atoi(raw)
		if err != nil || offset < 0 {
			return 0, 0, errors.New("offset must be a non-negative integer")
		}
	}
	return limit, offset, nil
}

// healthz reports whether the database answers within a second, so an orchestrator can
// tell "process up" from "process up and usable".
func (d Deps) healthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), healthTimeout)
	defer cancel()
	if err := d.Pool.Ping(ctx); err != nil {
		d.Log.Warn("healthz.database_unreachable", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "database": "error"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "database": "ok"})
}
