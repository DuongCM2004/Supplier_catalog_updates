package api

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

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
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	// Messages quote supplier data such as "price_minor must be >= 0"; Go's default HTML
	// escaping would print that as >= to every curl user. The body is never served as
	// HTML, and nosniff below stops browsers from guessing otherwise.
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		status = http.StatusInternalServerError
		body.Reset()
		body.WriteString(`{"error":{"code":"INTERNAL","message":"internal error"}}` + "\n")
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	// A failed write means the client went away; there is nobody left to tell.
	_, _ = w.Write(body.Bytes())
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
	case errors.Is(err, syncjob.ErrShuttingDown):
		return http.StatusServiceUnavailable, "SHUTTING_DOWN", "the service is shutting down, retry after it has restarted"
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
		if code == "DB_UNAVAILABLE" { // a refusal during shutdown is expected, not a database problem
			d.Log.Warn("http.database_unavailable", "method", r.Method, "path", r.URL.Path, "error", err)
		}
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

// queryChoice reads an optional query parameter that, when present, must be one of allowed.
// The error is worded for the 422 body.
func queryChoice(q url.Values, name string, allowed ...string) (string, error) {
	v := q.Get(name)
	if v == "" || slices.Contains(allowed, v) {
		return v, nil
	}
	return "", fmt.Errorf("%s must be one of %s", name, strings.Join(allowed, ", "))
}

// views converts a listing to its response items. The result is never nil, so an empty
// listing encodes as [] and clients need no special case for null.
func views[T, V any](in []T, convert func(T) V) []V {
	out := make([]V, len(in))
	for i, v := range in {
		out[i] = convert(v)
	}
	return out
}

// jobIDFromPath returns the {id} of a job route. An id that cannot be a job id is answered
// as "not found" here, before any SQL, exactly like an unknown one.
func (d Deps) jobIDFromPath(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if !isUUID(id) {
		d.writeFailure(w, r, syncjob.ErrNotFound, nil)
		return "", false
	}
	return id, true
}

// jobStatuses are the values the status filter of GET /api/syncs accepts.
var jobStatuses = []string{
	string(syncjob.StatusPending), string(syncjob.StatusRunning), string(syncjob.StatusInterrupted),
	string(syncjob.StatusNeedsAttention), string(syncjob.StatusCompleted), string(syncjob.StatusFailed),
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

type jobListView struct {
	Items  []jobView `json:"items"`
	Limit  int       `json:"limit"`
	Offset int       `json:"offset"`
}

// listSyncs pages through jobs, newest first, optionally of one status.
func (d Deps) listSyncs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, offset, err := parsePage(q)
	if err != nil {
		writeValidation(w, err.Error())
		return
	}
	status, err := queryChoice(q, "status", jobStatuses...)
	if err != nil {
		writeValidation(w, err.Error())
		return
	}
	jobs, err := syncjob.ListJobs(r.Context(), d.Pool, status, limit, offset)
	if err != nil {
		d.writeFailure(w, r, err, nil)
		return
	}
	writeJSON(w, http.StatusOK, jobListView{Items: views(jobs, newJobView), Limit: limit, Offset: offset})
}

// getSync returns one job with its progress, counters and open blocking issue count.
func (d Deps) getSync(w http.ResponseWriter, r *http.Request) {
	id, ok := d.jobIDFromPath(w, r)
	if !ok {
		return
	}
	job, err := syncjob.GetJob(r.Context(), d.Pool, id)
	if err != nil {
		d.writeFailure(w, r, err, nil)
		return
	}
	writeJSON(w, http.StatusOK, newJobView(job))
}

// changeView is one audit row. before is null when the product did not exist yet; after is
// the stored state the change produced.
type changeView struct {
	ID         string          `json:"id"`
	BatchID    string          `json:"batch_id"`
	SupplierID string          `json:"supplier_id"`
	ProductID  string          `json:"product_id"`
	Revision   int64           `json:"revision"`
	ChangeType string          `json:"change_type"`
	Before     json.RawMessage `json:"before"`
	After      json.RawMessage `json:"after"`
	CreatedAt  time.Time       `json:"created_at"`
}

func newChangeView(c syncjob.Change) changeView {
	return changeView{
		ID: c.ID, BatchID: c.BatchID, SupplierID: c.SupplierID, ProductID: c.ProductID,
		Revision: c.Revision, ChangeType: c.ChangeType, Before: c.Before, After: c.After,
		CreatedAt: c.CreatedAt.UTC(),
	}
}

type changeListView struct {
	Items []changeView `json:"items"`
}

// listChanges returns the catalog changes one job applied, in the order they happened.
func (d Deps) listChanges(w http.ResponseWriter, r *http.Request) {
	id, ok := d.jobIDFromPath(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	limit, offset, err := parsePage(q)
	if err != nil {
		writeValidation(w, err.Error())
		return
	}
	changeType, err := queryChoice(q, "type", "CREATED", "UPDATED", "WITHDRAWN")
	if err != nil {
		writeValidation(w, err.Error())
		return
	}
	changes, err := syncjob.ListChanges(r.Context(), d.Pool, id, changeType, limit, offset)
	if err != nil {
		d.writeFailure(w, r, err, nil)
		return
	}
	writeJSON(w, http.StatusOK, changeListView{Items: views(changes, newChangeView)})
}

type issueView struct {
	ID          string            `json:"id"`
	BatchID     *string           `json:"batch_id"`
	Cursor      *string           `json:"cursor"`
	SupplierID  *string           `json:"supplier_id"`
	ProductID   *string           `json:"product_id"`
	Code        syncjob.IssueCode `json:"code"`
	Severity    syncjob.Severity  `json:"severity"`
	Status      string            `json:"status"`
	Message     string            `json:"message"`
	RetryCount  int               `json:"retry_count"`
	FirstSeenAt time.Time         `json:"first_seen_at"`
	LastSeenAt  time.Time         `json:"last_seen_at"`
	ResolvedAt  *time.Time        `json:"resolved_at"`
}

func newIssueView(i syncjob.Issue) issueView {
	return issueView{
		ID: i.ID, BatchID: i.BatchID, Cursor: i.Cursor, SupplierID: i.SupplierID, ProductID: i.ProductID,
		Code: i.Code, Severity: i.Severity, Status: i.Status, Message: i.Message, RetryCount: i.RetryCount,
		FirstSeenAt: i.FirstSeenAt.UTC(), LastSeenAt: i.LastSeenAt.UTC(), ResolvedAt: utcPtr(i.ResolvedAt),
	}
}

// issueListView carries the job's open blocking count next to the (possibly filtered)
// items, so the one number that decides whether the job can complete is never filtered out.
type issueListView struct {
	Items        []issueView `json:"items"`
	OpenBlocking int         `json:"open_blocking"`
}

// listIssues returns the problems recorded for one job, oldest first.
func (d Deps) listIssues(w http.ResponseWriter, r *http.Request) {
	id, ok := d.jobIDFromPath(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	limit, offset, err := parsePage(q)
	if err != nil {
		writeValidation(w, err.Error())
		return
	}
	status, err := queryChoice(q, "status", "OPEN", "RESOLVED")
	if err != nil {
		writeValidation(w, err.Error())
		return
	}
	severity, err := queryChoice(q, "severity", string(syncjob.SeverityBlocking), string(syncjob.SeverityTransient))
	if err != nil {
		writeValidation(w, err.Error())
		return
	}
	// The count and the page are two reads; a commit between them can make them differ for
	// one response, which the next poll corrects.
	job, err := syncjob.GetJob(r.Context(), d.Pool, id)
	if err != nil {
		d.writeFailure(w, r, err, nil)
		return
	}
	issues, err := syncjob.ListIssues(r.Context(), d.Pool, id, status, severity, limit, offset)
	if err != nil {
		d.writeFailure(w, r, err, nil)
		return
	}
	writeJSON(w, http.StatusOK, issueListView{Items: views(issues, newIssueView), OpenBlocking: job.OpenBlockingIssues})
}

// resumeSync continues a stopped job from its saved cursor. The request has no body: the
// job id says everything, and committed batches are never fetched again.
func (d Deps) resumeSync(w http.ResponseWriter, r *http.Request) {
	if !d.requireManager(w, r) {
		return
	}
	id, ok := d.jobIDFromPath(w, r)
	if !ok {
		return
	}
	job, err := d.Manager.Resume(r.Context(), id)
	if err != nil {
		var extra map[string]any
		if errors.Is(err, syncjob.ErrInvalidTransition) {
			// "Not allowed" is only actionable together with the state that forbids it.
			if current, getErr := syncjob.GetJob(r.Context(), d.Pool, id); getErr == nil {
				extra = map[string]any{"current_status": current.Status}
			}
		}
		d.writeFailure(w, r, err, extra)
		return
	}
	writeJSON(w, http.StatusAccepted, newJobView(job))
}

// productView is a stored catalog row. Withdrawn products are tombstones: they keep their
// revision and report when they were withdrawn.
type productView struct {
	SupplierID  string         `json:"supplier_id"`
	ProductID   string         `json:"product_id"`
	Status      catalog.Status `json:"status"`
	Revision    int64          `json:"revision"`
	Name        *string        `json:"name"`
	PriceMinor  *int64         `json:"price_minor"`
	Currency    *string        `json:"currency"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	WithdrawnAt *time.Time     `json:"withdrawn_at"`
}

func newProductView(p catalog.Product) productView {
	return productView{
		SupplierID: p.SupplierID, ProductID: p.ProductID, Status: p.Status, Revision: p.Revision,
		Name: p.Name, PriceMinor: p.PriceMinor, Currency: p.Currency,
		CreatedAt: p.CreatedAt.UTC(), UpdatedAt: p.UpdatedAt.UTC(), WithdrawnAt: utcPtr(p.WithdrawnAt),
	}
}

type productListView struct {
	Items []productView `json:"items"`
}

// isStorableText reports whether s can be sent to PostgreSQL as text. A NUL byte or invalid
// UTF-8 in a product or supplier id would make the database refuse the query, which would
// surface as a server error for what is only a lookup that cannot match anything.
func isStorableText(s string) bool {
	return utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}

// listProducts pages through the catalog ordered by supplier and product id. Only ACTIVE
// products are listed unless status asks for WITHDRAWN tombstones or ALL.
func (d Deps) listProducts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, offset, err := parsePage(q)
	if err != nil {
		writeValidation(w, err.Error())
		return
	}
	// The catalog reports an unknown status as a plain error, so it is checked here to keep
	// a bad filter a 422 and not a 500.
	status, err := queryChoice(q, "status", string(catalog.StatusActive), string(catalog.StatusWithdrawn), "ALL")
	if err != nil {
		writeValidation(w, err.Error())
		return
	}
	if status == "" {
		status = string(catalog.StatusActive)
	}
	supplierID := q.Get("supplier_id")
	if !isStorableText(supplierID) {
		writeValidation(w, "supplier_id must be valid text without NUL bytes")
		return
	}
	products, err := catalog.ListProducts(r.Context(), d.Pool,
		catalog.ListFilter{SupplierID: supplierID, Status: status, Limit: limit, Offset: offset})
	if err != nil {
		d.writeFailure(w, r, err, nil)
		return
	}
	writeJSON(w, http.StatusOK, productListView{Items: views(products, newProductView)})
}

// getProduct returns one product of the configured supplier, or of the supplier_id given.
// Withdrawn products are returned too: a tombstone is a fact the supplier asserted.
func (d Deps) getProduct(w http.ResponseWriter, r *http.Request) {
	productID := r.PathValue("product_id")
	supplierID := cmp.Or(r.URL.Query().Get("supplier_id"), d.SupplierID)
	if !isStorableText(productID) || !isStorableText(supplierID) {
		d.writeFailure(w, r, catalog.ErrNotFound, nil)
		return
	}
	product, err := catalog.GetProduct(r.Context(), d.Pool, supplierID, productID)
	if err != nil {
		d.writeFailure(w, r, err, nil)
		return
	}
	writeJSON(w, http.StatusOK, newProductView(product))
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
