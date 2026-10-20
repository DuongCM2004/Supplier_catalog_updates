package syncjob

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/catalog"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/storage"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/supplier"
)

// Processor applies fetched supplier batches to the catalog. Each batch is one database
// transaction that also writes the batch marker, the audit rows, the job counters and the
// checkpoint, so a crash can never record progress without data or data without progress.
// It never talks to the supplier: the worker fetches the batch first and hands it over, so
// no database lock is ever held while waiting on the network.
type Processor struct {
	pool  *pgxpool.Pool
	hooks Hooks
	log   *slog.Logger
}

// NewProcessor returns a Processor on pool. hooks may be the zero Hooks; a nil log uses
// slog.Default().
func NewProcessor(pool *pgxpool.Pool, hooks Hooks, log *slog.Logger) *Processor {
	if log == nil {
		log = slog.Default()
	}
	return &Processor{pool: pool, hooks: hooks, log: log}
}

// blockingErr is a problem that makes this batch unusable as delivered. It is raised inside
// the batch transaction (or before it) and aborts it; the processor then records it as a
// blocking issue.
type blockingErr struct{ issue Issue }

func (e *blockingErr) Error() string { return string(e.issue.Code) + ": " + e.issue.Message }

// Issue is the blocking issue to record for this problem.
func (e *blockingErr) Issue() Issue { return e.issue }

func blockErr(code IssueCode, format string, args ...any) error {
	return &blockingErr{issue: Issue{Code: code, Severity: SeverityBlocking, Message: fmt.Sprintf(format, args...)}}
}

// validateEnvelope checks everything about a batch that can be judged without the database:
// that it belongs to the job's update, names itself, carries the end-of-feed marker and has
// a cursor that fits that marker. requestedCursor is the cursor the batch was fetched with.
// A nil result means the envelope is sound; otherwise the BLOCKING issue to record.
//
// A batch that points back at the cursor it was fetched with would be fetched again
// forever, so that is rejected here rather than discovered by looping.
//
// Text that PostgreSQL cannot store (a NUL byte, invalid UTF-8) is rejected too, as
// INVALID_BATCH, before it reaches a query: stored later it would fail with a database error
// that reads like an outage and could not even be written into the issue about it. Messages
// quote such values with %q, which escapes them, so they stay storable.
func validateEnvelope(job Job, requestedCursor string, b supplier.Batch) *Issue {
	invalid := func(code IssueCode, format string, args ...any) *Issue {
		return &Issue{Code: code, Severity: SeverityBlocking, Message: fmt.Sprintf(format, args...)}
	}
	switch {
	case b.UpdateID != job.UpdateID: // the job's id is stored text, so an unclean one is never equal to it
		return invalid(CodeInvalidBatch, "batch belongs to update %.80q, but this job syncs %q", b.UpdateID, job.UpdateID)
	case b.BatchID == "":
		return invalid(CodeInvalidBatch, "batch has no batch_id")
	case !cleanText(b.BatchID):
		return invalid(CodeInvalidBatch, "batch_id %.80q is not valid text: it has a NUL byte or invalid UTF-8", b.BatchID)
	case !cleanText(b.NextCursor):
		return invalid(CodeInvalidBatch, "batch %.80q has a next_cursor that is not valid text: it has a NUL byte or invalid UTF-8 (%.80q)", b.BatchID, b.NextCursor)
	case b.HasMore == nil:
		return invalid(CodeInvalidBatch, "batch %.80q has no has_more marker, so the end of the feed cannot be told", b.BatchID)
	case *b.HasMore && b.NextCursor == "":
		return invalid(CodeInvalidCursor, "batch %.80q says has_more but gives no next_cursor", b.BatchID)
	case *b.HasMore && b.NextCursor == requestedCursor:
		return invalid(CodeInvalidCursor, "batch %.80q points back at its own cursor %.80q", b.BatchID, requestedCursor)
	case !*b.HasMore && b.NextCursor != "":
		return invalid(CodeInvalidCursor, "batch %.80q ends the feed but still gives next_cursor %.80q", b.BatchID, b.NextCursor)
	}
	return nil
}

// cleanText reports whether s can be stored in a PostgreSQL text column.
func cleanText(s string) bool {
	return utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}

// hashedBatch fixes the field order of the hashed JSON, which is what makes BatchHash
// deterministic.
type hashedBatch struct {
	UpdateID   string                    `json:"update_id"`
	BatchID    string                    `json:"batch_id"`
	NextCursor string                    `json:"next_cursor"`
	HasMore    *bool                     `json:"has_more"`
	Products   []catalog.SupplierProduct `json:"products"`
}

// BatchHash identifies the content of a batch: the hex SHA-256 of its envelope and its
// products in canonical form, in the order the supplier listed them. Re-serving a batch
// with different whitespace or key order gives the same hash; changing any field of any
// product gives a different one. The hash is how a batch_id that comes back with other
// content is told apart from a harmless re-delivery.
func BatchHash(b supplier.Batch) string {
	products := make([]catalog.SupplierProduct, len(b.Products)) // [] rather than null for an empty batch
	for i, p := range b.Products {
		products[i] = catalog.Canonicalize(p)
	}
	// Marshalling cannot fail: the value holds only strings, integers and booleans.
	raw, _ := json.Marshal(hashedBatch{
		UpdateID:   b.UpdateID,
		BatchID:    b.BatchID,
		NextCursor: b.NextCursor,
		HasMore:    b.HasMore,
		Products:   products,
	})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// ProcessBatch applies one fetched batch to the job and reports what happened.
// requestedCursor is the cursor the worker fetched b with.
//
// The outcome is one of: KindCommitted (applied and checkpointed), KindReplayed (this exact
// batch was committed before: nothing changes), KindStale (the job is no longer RUNNING or
// has moved past requestedCursor: nothing changes) or KindBlocked (the batch cannot be
// applied; nothing was written except a blocking issue). A non-nil error means the
// database failed and the caller decides whether to retry later.
func (p *Processor) ProcessBatch(ctx context.Context, jobID, requestedCursor string, b supplier.Batch) (Outcome, error) {
	started := time.Now()
	job, err := GetJob(ctx, p.pool, jobID)
	if err != nil {
		return Outcome{}, err
	}
	log := p.log.With("job_id", jobID, "update_id", job.UpdateID, "batch_id", b.BatchID, "cursor", requestedCursor)

	out, seq, err := p.apply(ctx, job, requestedCursor, b)
	var blocking *blockingErr
	var conflict *catalog.BlockingError
	switch {
	case errors.As(err, &blocking):
		return p.block(ctx, log, jobID, requestedCursor, b.BatchID, blocking.Issue())
	case errors.As(err, &conflict):
		return p.block(ctx, log, jobID, requestedCursor, b.BatchID, issueFromCatalog(conflict))
	case storage.IsDataError(err):
		// The database refused the data itself (a constraint or trigger): the same batch
		// would be refused again, so retrying is pointless.
		return p.block(ctx, log, jobID, requestedCursor, b.BatchID, Issue{
			Code: CodeDatabaseError, Severity: SeverityBlocking,
			Message: "the database rejected the batch and rolled it back: " + err.Error(),
		})
	case err != nil:
		return Outcome{}, err // connection-class failure or a simulated crash: the caller decides
	}
	switch out.Kind {
	case KindCommitted:
		log.Info("batch.committed", "seq", seq, "records", len(b.Products),
			"created", out.Counts.Created, "updated", out.Counts.Updated, "withdrawn", out.Counts.Withdrawn,
			"duplicate", out.Counts.Duplicate, "stale", out.Counts.Stale,
			"job_status", out.Status, "duration_ms", float64(time.Since(started).Microseconds())/1000)
	case KindReplayed:
		log.Warn("batch.replayed", "reason", "batch was already committed; nothing applied")
	case KindStale:
		log.Warn("batch.stale_worker", "reason", "job is no longer waiting for this batch; nothing applied")
	}
	if out.Kind == KindCommitted {
		// Fault point: the batch is durable but the caller has not been told yet.
		if err := p.hooks.afterCommit(b.BatchID); err != nil {
			return out, fmt.Errorf("after-commit hook for batch %s: %w", b.BatchID, err)
		}
	}
	return out, nil
}

// block records why a batch cannot be applied and parks the job for a person to look at.
// The batch transaction has already rolled back, so the catalog and checkpoint are exactly
// as before; this separate transaction only adds the issue and moves the job from RUNNING
// to NEEDS_ATTENTION. If the job is no longer RUNNING (it was interrupted, or another
// worker got there first) nothing is recorded and the outcome is stale.
func (p *Processor) block(ctx context.Context, log *slog.Logger, jobID, cursor, batchID string, iss Issue) (Outcome, error) {
	iss.Cursor = &cursor
	if cleanText(batchID) { // an unstorable id would make recording this very issue fail
		iss.BatchID = nonEmpty(batchID)
	}
	_, applied, err := settleJob(ctx, p.pool, jobID, &iss, StatusNeedsAttention, iss.Message)
	if err != nil {
		return Outcome{}, fmt.Errorf("record %s issue: %w", iss.Code, err)
	}
	if !applied {
		log.Warn("batch.stale_worker", "reason", "job is no longer RUNNING; blocking issue not recorded", "code", iss.Code)
		return Outcome{Kind: KindStale}, nil
	}
	log.Warn("batch.blocked", "code", iss.Code, "message", iss.Message)
	log.Info("job.transition", "from", StatusRunning, "to", StatusNeedsAttention, "reason", string(iss.Code))
	return Outcome{Kind: KindBlocked}, nil
}

// issueFromCatalog names the offending product in the issue, since the catalog's own
// message describes only the revisions.
func issueFromCatalog(e *catalog.BlockingError) Issue {
	return Issue{
		Code:       IssueCode(e.Code),
		Severity:   SeverityBlocking,
		SupplierID: &e.SupplierID,
		ProductID:  &e.ProductID,
		Message:    fmt.Sprintf("%s/%s revision %d: %s", e.SupplierID, e.ProductID, e.Revision, e.Message),
	}
}

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// apply runs the checks that need no database, then the batch transaction. A problem that
// makes the batch unusable comes back as a *blockingErr or *catalog.BlockingError.
func (p *Processor) apply(ctx context.Context, job Job, requestedCursor string, b supplier.Batch) (Outcome, int, error) {
	if iss := validateEnvelope(job, requestedCursor, b); iss != nil {
		return Outcome{}, 0, &blockingErr{issue: *iss}
	}
	if err := catalog.ValidateBatch(b.Products, job.SupplierID); err != nil {
		return Outcome{}, 0, &blockingErr{issue: validationIssue(err)}
	}
	hash := BatchHash(b)

	var out Outcome
	var seq int
	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		var err error
		out, seq, err = p.commitBatch(ctx, tx, job.ID, requestedCursor, b, hash)
		return err
	})
	return out, seq, err
}

// commitBatch is the whole batch transaction. Locking order: the job row first, then the
// product rows (inside ApplyBatch, in key order), then plain inserts. Holding the job lock
// for the entire transaction is what lets the open-issue count at end of feed be exact.
func (p *Processor) commitBatch(ctx context.Context, tx pgx.Tx, jobID, requestedCursor string, b supplier.Batch, hash string) (Outcome, int, error) {
	j, err := lockJob(ctx, tx, jobID)
	if err != nil {
		return Outcome{}, 0, err
	}
	if j.Status != StatusRunning {
		return Outcome{Kind: KindStale}, 0, nil
	}
	// History comes before the cursor check on purpose: a batch that was already committed
	// is a replay (or a conflict), whichever cursor the caller believed it was at.
	if m, found, err := batchMarker(ctx, tx, jobID, b.BatchID); err != nil {
		return Outcome{}, 0, err
	} else if found {
		out, err := judgeCommittedBatch(m, requestedCursor, b, hash)
		return out, 0, err
	}
	if j.NextCursor != requestedCursor || j.EndOfFeedSeen {
		return Outcome{Kind: KindStale}, 0, nil
	}
	if err := checkFeedShape(ctx, tx, j, b); err != nil {
		return Outcome{}, 0, err
	}

	applied, err := catalog.ApplyBatch(ctx, tx, b.Products)
	if err != nil {
		return Outcome{}, 0, fmt.Errorf("apply batch %s: %w", b.BatchID, err)
	}
	counts, err := tally(applied)
	if err != nil {
		return Outcome{}, 0, err
	}
	seq := j.ProcessedBatches + 1
	if err := insertBatchMarker(ctx, tx, jobID, b, requestedCursor, seq, hash, counts); err != nil {
		return Outcome{}, 0, err
	}
	if err := insertChanges(ctx, tx, jobID, b.BatchID, applied); err != nil {
		return Outcome{}, 0, err
	}
	// A committed batch proves the supplier and database answer again, and that the problem
	// recorded at this cursor (if any) is gone.
	if err := resolveIssues(ctx, tx, jobID, requestedCursor); err != nil {
		return Outcome{}, 0, err
	}

	status, lastError := StatusRunning, (*string)(nil)
	if !*b.HasMore {
		open, err := countOpenBlocking(ctx, tx, jobID)
		if err != nil {
			return Outcome{}, 0, err
		}
		status = StatusCompleted
		if open > 0 {
			status = StatusNeedsAttention
			msg := fmt.Sprintf("end of feed reached but %d blocking issue(s) are still open", open)
			lastError = &msg
		}
	}
	// The table's CHECK and the completion trigger independently refuse a COMPLETED job
	// that has not seen end of feed or still has an open blocking issue.
	if err := advanceCheckpoint(ctx, tx, jobID, b, counts, status, lastError); err != nil {
		return Outcome{}, 0, err
	}
	out := Outcome{Kind: KindCommitted, Counts: counts, Status: status}
	return out, seq, p.hooks.beforeCommit(ctx, tx, b.BatchID) // last statement: an error here rolls back everything above
}

// judgeCommittedBatch decides what a batch whose id already has a marker is: the same batch
// again (a no-op), the same id with other content, or the same content at a different cursor
// (which would make the worker loop through the same batch forever).
func judgeCommittedBatch(m batchMark, requestedCursor string, b supplier.Batch, hash string) (Outcome, error) {
	switch {
	case m.PayloadHash != hash:
		return Outcome{}, blockErr(CodeBatchConflict, "batch %.80q was already committed with different content", b.BatchID)
	case m.CursorIn != requestedCursor:
		return Outcome{}, blockErr(CodeInvalidCursor, "batch %.80q was committed at cursor %.80q and is served again at %.80q",
			b.BatchID, m.CursorIn, requestedCursor)
	}
	return Outcome{Kind: KindReplayed}, nil
}

// checkFeedShape refuses a batch that contradicts the feed so far, before it can touch the
// catalog: a next cursor already visited would loop, and a batch count different from what
// the supplier announced means pages were lost or invented.
func checkFeedShape(ctx context.Context, tx pgx.Tx, j Job, b supplier.Batch) error {
	if *b.HasMore {
		used, err := cursorUsed(ctx, tx, j.ID, b.NextCursor)
		if err != nil {
			return err
		}
		if used {
			return blockErr(CodeInvalidCursor, "batch %.80q points at cursor %.80q, which was already processed", b.BatchID, b.NextCursor)
		}
	}
	if j.TotalBatches == nil {
		return nil
	}
	position := j.ProcessedBatches + 1
	switch {
	case *b.HasMore && position >= *j.TotalBatches:
		return blockErr(CodeInvalidBatch, "batch %d says more follows, but the supplier announced %d batches", position, *j.TotalBatches)
	case !*b.HasMore && position != *j.TotalBatches:
		return blockErr(CodeInvalidBatch, "feed ended after %d batches, but the supplier announced %d", position, *j.TotalBatches)
	}
	return nil
}

// tally turns per-event decisions into job counters. A withdrawal of a product that was
// never seen counts as withdrawn; moving a tombstone's revision forward changes no visible
// state, so it counts as a duplicate.
func tally(applied []catalog.Applied) (Counts, error) {
	var c Counts
	for _, a := range applied {
		switch a.Decision {
		case catalog.DecisionCreate:
			c.Created++
		case catalog.DecisionUpdate:
			c.Updated++
		case catalog.DecisionCreateTombstone, catalog.DecisionWithdraw:
			c.Withdrawn++
		case catalog.DecisionAdvanceTombstone, catalog.DecisionIgnoreDuplicate:
			c.Duplicate++
		case catalog.DecisionIgnoreStale:
			c.Stale++
		default:
			return Counts{}, fmt.Errorf("unexpected decision %q for product %s/%s", a.Decision, a.Event.SupplierID, a.Event.ProductID)
		}
	}
	return c, nil
}

// batchMark is what the marker row of a committed batch remembers about it.
type batchMark struct{ PayloadHash, CursorIn string }

const (
	sqlBatchMarker = `SELECT payload_hash, cursor_in FROM sync_batches WHERE job_id = $1::uuid AND batch_id = $2`
	sqlCursorUsed  = `SELECT EXISTS (SELECT 1 FROM sync_batches WHERE job_id = $1::uuid AND cursor_in = $2)`
)

func batchMarker(ctx context.Context, tx pgx.Tx, jobID, batchID string) (batchMark, bool, error) {
	var m batchMark
	err := tx.QueryRow(ctx, sqlBatchMarker, jobID, batchID).Scan(&m.PayloadHash, &m.CursorIn)
	if errors.Is(err, pgx.ErrNoRows) {
		return batchMark{}, false, nil
	}
	if err != nil {
		return batchMark{}, false, fmt.Errorf("look up batch %s of job %s: %w", batchID, jobID, err)
	}
	return m, true, nil
}

func cursorUsed(ctx context.Context, tx pgx.Tx, jobID, cursor string) (bool, error) {
	var used bool
	if err := tx.QueryRow(ctx, sqlCursorUsed, jobID, cursor).Scan(&used); err != nil {
		return false, fmt.Errorf("look up cursor %q of job %s: %w", cursor, jobID, err)
	}
	return used, nil
}

const sqlInsertBatchMarker = `INSERT INTO sync_batches
	(job_id, batch_id, seq, cursor_in, cursor_out, has_more, payload_hash, record_count,
	 created_count, updated_count, withdrawn_count, duplicate_count, stale_count)
	VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`

// insertBatchMarker remembers that this batch was committed, at which position and cursor,
// and with which content. The marker is what turns a repeated delivery into a no-op.
func insertBatchMarker(ctx context.Context, tx pgx.Tx, jobID string, b supplier.Batch, cursorIn string, seq int, hash string, c Counts) error {
	_, err := tx.Exec(ctx, sqlInsertBatchMarker, jobID, b.BatchID, seq, cursorIn, b.NextCursor, *b.HasMore, hash,
		len(b.Products), c.Created, c.Updated, c.Withdrawn, c.Duplicate, c.Stale)
	if err != nil {
		return fmt.Errorf("record batch %s of job %s: %w", b.BatchID, jobID, err)
	}
	return nil
}

// productSnapshot is the before/after picture stored in an audit row.
type productSnapshot struct {
	Status     catalog.Status `json:"status"`
	Revision   int64          `json:"revision"`
	Name       *string        `json:"name"`
	PriceMinor *int64         `json:"price_minor"`
	Currency   *string        `json:"currency"`
}

// snapshotJSON renders a stored product as audit JSON; nil (no row yet) is SQL NULL.
func snapshotJSON(p *catalog.Product) (*string, error) {
	if p == nil {
		return nil, nil
	}
	raw, err := json.Marshal(productSnapshot{p.Status, p.Revision, p.Name, p.PriceMinor, p.Currency})
	if err != nil {
		return nil, err
	}
	s := string(raw)
	return &s, nil
}

// changeTypeOf maps a decision that wrote to the catalog to its audit change type. Decisions
// that leave no visible change (duplicates, stale events, tombstone revision bumps) have none.
func changeTypeOf(d catalog.Decision) (string, bool) {
	switch d {
	case catalog.DecisionCreate:
		return "CREATED", true
	case catalog.DecisionUpdate:
		return "UPDATED", true
	case catalog.DecisionCreateTombstone, catalog.DecisionWithdraw:
		return "WITHDRAWN", true
	}
	return "", false
}

// created_at uses clock_timestamp() rather than now(): now() is the same for the whole
// transaction, which would make every row of a batch tie and lose the order the events
// were applied in. Rows are inserted one by one in application order so /changes lists
// them as they happened.
const sqlInsertChange = `INSERT INTO sync_changes
	(job_id, batch_id, supplier_id, product_id, revision, change_type, before, after, created_at)
	VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::text::jsonb, $8::text::jsonb, clock_timestamp())`

func insertChanges(ctx context.Context, tx pgx.Tx, jobID, batchID string, applied []catalog.Applied) error {
	for _, a := range applied {
		changeType, ok := changeTypeOf(a.Decision)
		if !ok {
			continue
		}
		before, err := snapshotJSON(a.Before)
		if err != nil {
			return fmt.Errorf("encode before-state of %s/%s: %w", a.Event.SupplierID, a.Event.ProductID, err)
		}
		after, err := snapshotJSON(a.After)
		if err != nil {
			return fmt.Errorf("encode after-state of %s/%s: %w", a.Event.SupplierID, a.Event.ProductID, err)
		}
		if after == nil {
			return fmt.Errorf("decision %s for %s/%s wrote no row", a.Decision, a.Event.SupplierID, a.Event.ProductID)
		}
		_, err = tx.Exec(ctx, sqlInsertChange, jobID, batchID, a.Event.SupplierID, a.Event.ProductID,
			a.Event.Revision, changeType, before, after)
		if err != nil {
			return fmt.Errorf("record change for %s/%s revision %d: %w", a.Event.SupplierID, a.Event.ProductID, a.Event.Revision, err)
		}
	}
	return nil
}

// One statement moves every piece of progress together: counters grow, the cursor advances
// and the status changes, so no reader can see half of a commit. completed_at is stamped
// only on the way to COMPLETED.
const sqlAdvanceCheckpoint = `UPDATE sync_jobs SET
	created_count = created_count + $2,
	updated_count = updated_count + $3,
	withdrawn_count = withdrawn_count + $4,
	duplicate_count = duplicate_count + $5,
	stale_count = stale_count + $6,
	processed_batches = processed_batches + 1,
	next_cursor = $7,
	last_batch_id = $8,
	end_of_feed_seen = $9,
	status = $10::text,
	last_error = $11::text,
	completed_at = CASE WHEN $10::text = 'COMPLETED' THEN now() ELSE completed_at END,
	updated_at = now()
	WHERE id = $1::uuid`

func advanceCheckpoint(ctx context.Context, tx pgx.Tx, jobID string, b supplier.Batch, c Counts, status Status, lastError *string) error {
	tag, err := tx.Exec(ctx, sqlAdvanceCheckpoint, jobID, c.Created, c.Updated, c.Withdrawn, c.Duplicate, c.Stale,
		b.NextCursor, b.BatchID, !*b.HasMore, string(status), lastError)
	if err != nil {
		return fmt.Errorf("advance checkpoint of job %s past batch %s: %w", jobID, b.BatchID, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("advance checkpoint of job %s past batch %s: %d rows updated, want 1", jobID, b.BatchID, tag.RowsAffected())
	}
	return nil
}

// maxReportedProblems keeps one issue message readable when a feed has many bad records.
const maxReportedProblems = 20

// validationIssue turns a record validation failure into the blocking issue to record,
// listing at most maxReportedProblems problems.
func validationIssue(err error) Issue {
	iss := Issue{Code: CodeInvalidProductData, Severity: SeverityBlocking, Message: err.Error()}
	var ve *catalog.ValidationError
	if errors.As(err, &ve) {
		shown := ve.Problems[:min(len(ve.Problems), maxReportedProblems)]
		msg := "invalid product data: " + strings.Join(shown, "; ")
		if more := len(ve.Problems) - len(shown); more > 0 {
			msg += fmt.Sprintf(" (and %d more)", more)
		}
		iss.Message = msg
	}
	return iss
}
