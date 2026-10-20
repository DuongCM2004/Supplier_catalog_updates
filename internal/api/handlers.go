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

// idPattern is the shape of the supplier and update ids a client may send. Go's $ matches
// only at the very end of the text, so a trailing newline does not slip through.
var idPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

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

// writeValidation answers 422 for a well-formed request whose values are not acceptable.
func writeValidation(w http.ResponseWriter, message string) {
	writeError(w, http.StatusUnprocessableEntity, "VALIDATION_FAILED", message, nil)
}

// requireManager answers 500 and reports false when the router was built without a Manager.
// That is a wiring mistake in the process, and it must not turn into a nil dereference
// inside a request.
func (d Deps) requireManager(w http.ResponseWriter, r *http.Request) bool {
	if d.Manager == nil {
		d.writeFailure(w, r, errors.New("api: sync manager is not configured"), nil)
		return false
	}
	return true
}

// Response shapes. They are written out field by field so the JSON contract is visible in
// one place and no database or domain struct ever reaches a client by accident. Timestamps
// are sent in UTC, whatever zone the database session reports.

type jobView struct {
	JobID              string         `json:"job_id"`
	Status             syncjob.Status `json:"status"`
	SupplierID         string         `json:"supplier_id"`
	SupplierUpdateID   string         `json:"supplier_update_id"`
	Progress           progressView   `json:"progress"`
	Changes            changeTotals   `json:"changes"`
	Ignored            ignoredTotals  `json:"ignored"`
	OpenBlockingIssues int            `json:"open_blocking_issues"`
	LastError          *string        `json:"last_error"`
	CreatedAt          time.Time      `json:"created_at"`
	StartedAt          *time.Time     `json:"started_at"`
	CompletedAt        *time.Time     `json:"completed_at"`
	UpdatedAt          time.Time      `json:"updated_at"`
}

type progressView struct {
	ProcessedBatches int     `json:"processed_batches"`
	TotalBatches     *int    `json:"total_batches"` // null while the supplier has not said how many
	NextCursor       string  `json:"next_cursor"`   // "" before the first batch and after the last
	LastBatchID      *string `json:"last_batch_id"`
	EndOfFeedSeen    bool    `json:"end_of_feed_seen"`
}

type changeTotals struct {
	Created   int `json:"created"`
	Updated   int `json:"updated"`
	Withdrawn int `json:"withdrawn"`
}

type ignoredTotals struct {
	Duplicate int `json:"duplicate"`
	Stale     int `json:"stale"`
}

func newJobView(j syncjob.Job) jobView {
	return jobView{
		JobID:            j.ID,
		Status:           j.Status,
		SupplierID:       j.SupplierID,
		SupplierUpdateID: j.UpdateID,
		Progress: progressView{
			ProcessedBatches: j.ProcessedBatches,
			TotalBatches:     j.TotalBatches,
			NextCursor:       j.NextCursor,
			LastBatchID:      j.LastBatchID,
			EndOfFeedSeen:    j.EndOfFeedSeen,
		},
		Changes:            changeTotals{Created: j.Created, Updated: j.Updated, Withdrawn: j.Withdrawn},
		Ignored:            ignoredTotals{Duplicate: j.Duplicate, Stale: j.Stale},
		OpenBlockingIssues: j.OpenBlockingIssues,
		LastError:          j.LastError,
		CreatedAt:          j.CreatedAt.UTC(),
		StartedAt:          utcPtr(j.StartedAt),
		CompletedAt:        utcPtr(j.CompletedAt),
		UpdatedAt:          j.UpdatedAt.UTC(),
	}
}

func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

// startRequest is the body of POST /api/syncs.
type startRequest struct {
	UpdateID   string  `json:"update_id"`
	SupplierID *string `json:"supplier_id"` // nil = the configured default supplier
}

// startSync begins syncing one supplier update. A new update answers 202 and a Location to
// poll; an update that already has a job answers 200 with that job and does not resume it,
// so a repeated request is harmless and recovery stays an explicit decision.
func (d Deps) startSync(w http.ResponseWriter, r *http.Request) {
	if !d.requireManager(w, r) {
		return
	}
	var req startRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", err.Error(), nil)
		return
	}
	supplierID := d.SupplierID
	if req.SupplierID != nil {
		supplierID = *req.SupplierID
	}
	switch {
	case req.UpdateID == "":
		writeValidation(w, "update_id is required")
		return
	case !idPattern.MatchString(req.UpdateID):
		writeValidation(w, "update_id must be 1-64 characters from A-Z a-z 0-9 . _ -")
		return
	case !idPattern.MatchString(supplierID):
		writeValidation(w, "supplier_id must be 1-64 characters from A-Z a-z 0-9 . _ -")
		return
	}

	job, created, err := d.Manager.Start(r.Context(), supplierID, req.UpdateID)
	switch {
	case errors.Is(err, syncjob.ErrActiveJobExists):
		// The active job is empty only when it finished between the conflict and the lookup.
		var extra map[string]any
		if job.ID != "" {
			extra = map[string]any{"active_job_id": job.ID}
		}
		d.writeFailure(w, r, err, extra)
	case err != nil:
		d.writeFailure(w, r, err, nil)
	case created:
		w.Header().Set("Location", "/api/syncs/"+job.ID)
		writeJSON(w, http.StatusAccepted, newJobView(job))
	default:
		writeJSON(w, http.StatusOK, newJobView(job))
	}
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
