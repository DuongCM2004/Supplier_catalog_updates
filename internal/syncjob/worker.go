package syncjob

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/storage"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/supplier"
)

// This file is the body of one worker: it walks a job from its persisted cursor to the end
// of the feed, fetching each batch outside any transaction and handing it to the Processor.
//
// Every way a run can end leaves the job in a state a person or a later resume can act on:
// COMPLETED, NEEDS_ATTENTION (a batch was rejected), FAILED (the supplier does not know the
// update) or INTERRUPTED (supplier or database trouble, or shutdown). Every issue is
// recorded at the job's current next_cursor so it resolves when the batch there commits.

// RunJob processes the job until it completes, stops for attention, fails or is interrupted,
// and returns nil for all of those. It returns an error only when it could not even record
// why it stopped (the database is down) or for a simulated crash; the caller then makes sure
// the job does not stay RUNNING without a worker. It must not be called on a job that Start
// or Resume already spawned a worker for.
func (m *Manager) RunJob(ctx context.Context, jobID string) error {
	job, err := m.transitionIfPending(ctx, jobID)
	if err != nil && ctx.Err() != nil {
		return m.interrupt(ctx, jobID, nil)
	}
	if err != nil || job.Status != StatusRunning {
		return err
	}
	// The last batch is already committed (resumed after end of feed): only the completion
	// check is left, and the supplier is not asked anything.
	if job.EndOfFeedSeen {
		return m.finalize(ctx, jobID)
	}
	if stop, err := m.checkUpdate(ctx, job); stop {
		return err
	}
	return m.runBatches(ctx, jobID)
}

// transitionIfPending starts a freshly created job; a job that is already RUNNING (resumed)
// or has been settled in the meantime is returned as it is.
func (m *Manager) transitionIfPending(ctx context.Context, jobID string) (Job, error) {
	job, err := GetJob(ctx, m.pool, jobID)
	if err != nil || job.Status != StatusPending {
		return job, err
	}
	err = transition(ctx, m.pool, jobID, []Status{StatusPending}, StatusRunning, nil)
	switch {
	case err == nil:
		m.log.Info("job.transition", "job_id", jobID, "update_id", job.UpdateID,
			"from", StatusPending, "to", StatusRunning, "reason", "worker started")
	case !errors.Is(err, ErrInvalidTransition): // ErrInvalidTransition: reconciled or settled before the worker got here
		return Job{}, err
	}
	return GetJob(ctx, m.pool, jobID)
}

// checkUpdate asks the supplier about the update before the first fetch: it must know it,
// belong to the same supplier and announce a sane batch count. stop is true when the run is
// over (the job was settled, or settling failed and err says so).
func (m *Manager) checkUpdate(ctx context.Context, job Job) (stop bool, err error) {
	var info supplier.UpdateInfo
	callErr := withRetry(ctx, m.cfg, m.log, "get_update", func(c context.Context) error {
		var e error
		info, e = m.sup.GetUpdate(c, job.UpdateID)
		return e
	})
	cursor := job.NextCursor
	switch {
	case ctx.Err() != nil: // shutdown is not a supplier problem
		return true, m.interrupt(ctx, job.ID, nil)
	case errors.Is(callErr, supplier.ErrNotFound):
		return true, m.fail(ctx, job.ID, CodeUnknownUpdate, cursor, callErr)
	case errors.Is(callErr, supplier.ErrMalformed):
		return true, m.blockJob(ctx, job.ID, CodeInvalidBatch, cursor, callErr.Error())
	case callErr != nil:
		return true, m.interrupt(ctx, job.ID, supplierDown(cursor, callErr))
	case info.SupplierID != job.SupplierID:
		return true, m.blockJob(ctx, job.ID, CodeInvalidBatch, cursor,
			fmt.Sprintf("update %q belongs to supplier %.80q, but this job syncs supplier %q", job.UpdateID, info.SupplierID, job.SupplierID))
	case info.TotalBatches != nil && *info.TotalBatches < 1:
		return true, m.blockJob(ctx, job.ID, CodeInvalidBatch, cursor,
			fmt.Sprintf("supplier announced %d batches", *info.TotalBatches))
	}
	if err := setTotalBatchesIfUnknown(ctx, m.pool, job.ID, info.TotalBatches); err != nil {
		return true, m.dbFailure(ctx, job.ID, &cursor, err)
	}
	return false, nil
}

// runBatches is the fetch-process loop. It re-reads the job every round, so what it fetches
// is always what the database says comes next, and it ends as soon as the job is not
// RUNNING any more. It terminates because each round either commits a batch (the cursor
// moves toward the end of the feed, which the processor guards against looping) or ends the
// run; a replayed or stale outcome means the stored cursor already moved past the batch.
func (m *Manager) runBatches(ctx context.Context, jobID string) error {
	for {
		if ctx.Err() != nil {
			return m.interrupt(ctx, jobID, nil)
		}
		job, err := GetJob(ctx, m.pool, jobID)
		if err != nil {
			return m.dbFailure(ctx, jobID, nil, err)
		}
		if job.Status != StatusRunning {
			return nil
		}
		if job.EndOfFeedSeen {
			return m.finalize(ctx, jobID)
		}

		b, stop, err := m.fetch(ctx, job)
		if stop {
			return err
		}
		out, err := m.proc.ProcessBatch(ctx, jobID, job.NextCursor, b)
		switch {
		case errors.Is(err, ErrSimulatedCrash):
			return err // tests: abandon the job exactly like a process that died here
		case err != nil && ctx.Err() != nil:
			return m.interrupt(ctx, jobID, nil)
		case err != nil:
			return m.dbFailure(ctx, jobID, &job.NextCursor, err)
		case out.Kind == KindBlocked:
			return nil // the processor already parked the job
		case out.Kind == KindCommitted && out.Status != StatusRunning:
			m.logFinished(ctx, jobID, out.Status)
			return nil
		}
	}
}

// fetch gets the batch at the job's cursor with bounded retries. stop is true when the run
// is over and err is the result of settling the job.
func (m *Manager) fetch(ctx context.Context, job Job) (b supplier.Batch, stop bool, err error) {
	callErr := withRetry(ctx, m.cfg, m.log, "fetch_batch", func(c context.Context) error {
		var e error
		b, e = m.sup.FetchBatch(c, job.UpdateID, job.NextCursor)
		return e
	})
	cursor := job.NextCursor
	switch {
	case callErr == nil:
		var hasMore any = "missing"
		if b.HasMore != nil {
			hasMore = *b.HasMore
		}
		m.log.Debug("batch.fetched", "job_id", job.ID, "update_id", job.UpdateID, "batch_id", b.BatchID,
			"cursor", cursor, "records", len(b.Products), "has_more", hasMore)
		return b, false, nil
	case ctx.Err() != nil:
		return b, true, m.interrupt(ctx, job.ID, nil)
	case errors.Is(callErr, supplier.ErrNotFound):
		return b, true, m.blockJob(ctx, job.ID, CodeInvalidCursor, cursor,
			fmt.Sprintf("supplier has no page for cursor %q", cursor))
	case errors.Is(callErr, supplier.ErrMalformed):
		return b, true, m.blockJob(ctx, job.ID, CodeInvalidBatch, cursor, callErr.Error())
	}
	return b, true, m.interrupt(ctx, job.ID, supplierDown(cursor, callErr))
}

// supplierDown is the transient issue for a supplier that stayed unreachable through every
// retry. retry_count starts at the number of attempts made, so staff see how hard it was tried.
func supplierDown(cursor string, err error) *Issue {
	return &Issue{
		Code:       CodeSupplierUnavailable,
		Severity:   SeverityTransient,
		Cursor:     &cursor,
		RetryCount: attemptsOf(err),
		Message:    fmt.Sprintf("supplier unavailable at cursor %q: %v", cursor, err),
	}
}

const sqlSetTotalBatches = `UPDATE sync_jobs SET total_batches = $2, updated_at = now()
	WHERE id = $1::uuid AND total_batches IS NULL`

// setTotalBatchesIfUnknown remembers how many batches the supplier announced. The first
// announcement wins, so a count that changes mid-feed cannot move the goalposts.
func setTotalBatchesIfUnknown(ctx context.Context, db storage.DB, jobID string, total *int) error {
	if total == nil {
		return nil
	}
	if _, err := db.Exec(ctx, sqlSetTotalBatches, jobID, *total); err != nil {
		return fmt.Errorf("store announced batch count of job %s: %w", jobID, err)
	}
	return nil
}

// finalize is the completion check for a job whose end-of-feed batch is already committed:
// COMPLETED only if no blocking issue is open, otherwise it waits for attention. Counting
// under the job lock keeps the answer exact.
func (m *Manager) finalize(ctx context.Context, jobID string) error {
	var to Status // stays empty when the job was not RUNNING any more
	err := pgx.BeginFunc(ctx, m.pool, func(tx pgx.Tx) error {
		j, err := lockJob(ctx, tx, jobID)
		if err != nil || j.Status != StatusRunning {
			return err
		}
		if err := resolveTransientIssues(ctx, tx, jobID); err != nil {
			return err
		}
		open, err := countOpenBlocking(ctx, tx, jobID)
		if err != nil {
			return err
		}
		target, lastError := StatusCompleted, (*string)(nil)
		if open > 0 {
			target = StatusNeedsAttention
			msg := fmt.Sprintf("end of feed reached but %d blocking issue(s) are still open", open)
			lastError = &msg
		}
		if err := transition(ctx, tx, jobID, []Status{StatusRunning}, target, lastError); err != nil {
			return err
		}
		to = target
		return nil
	})
	switch {
	case err != nil:
		return m.dbFailure(ctx, jobID, nil, err)
	case to != "":
		m.logFinished(ctx, jobID, to)
	}
	return nil
}

// shutdownReason is the last_error of a job stopped because the service is stopping.
const shutdownReason = "interrupted: the service is shutting down"

// settleTimeout bounds recording how a run ended. It runs on a context detached from the
// worker's, because the worker's context is exactly what shutdown cancels.
const settleTimeout = 5 * time.Second

// settle records iss (if any) and moves the RUNNING job to `to`; see settleJob. A job that
// already left RUNNING is not touched and nothing is logged. The returned error means the
// outcome could not be stored, and the caller falls back to ensureNotRunning.
func (m *Manager) settle(ctx context.Context, jobID string, iss *Issue, to Status, reason string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()
	prev, applied, err := settleJob(ctx, m.pool, jobID, iss, to, reason)
	if err != nil {
		return fmt.Errorf("move job %s to %s: %w", jobID, to, err)
	}
	if !applied {
		return nil
	}
	log := m.log.With("job_id", jobID, "update_id", prev.UpdateID)
	log.Info("job.transition", "from", prev.Status, "to", to, "reason", reason)
	switch to {
	case StatusInterrupted:
		log.Warn("job.interrupted", "reason", reason, "processed_batches", prev.ProcessedBatches, "next_cursor", prev.NextCursor)
	case StatusFailed:
		log.Error("job.failed", "reason", reason, "processed_batches", prev.ProcessedBatches)
	case StatusNeedsAttention:
		if iss != nil {
			log.Warn("batch.blocked", "code", iss.Code, "message", iss.Message, "cursor", prev.NextCursor)
		}
	}
	return nil
}

// interrupt stops a RUNNING job so it can be resumed. iss is the transient problem to
// record; nil records none, which is what a graceful shutdown wants: stopping on request is
// not a supplier or database problem.
func (m *Manager) interrupt(ctx context.Context, jobID string, iss *Issue) error {
	reason := shutdownReason
	if iss != nil {
		reason = iss.Message
	}
	return m.settle(ctx, jobID, iss, StatusInterrupted, reason)
}

// blockJob parks the job for attention with a blocking issue found outside batch
// processing (the supplier answered, but with something unusable).
func (m *Manager) blockJob(ctx context.Context, jobID string, code IssueCode, cursor, message string) error {
	iss := Issue{Code: code, Severity: SeverityBlocking, Cursor: &cursor, Message: message}
	return m.settle(ctx, jobID, &iss, StatusNeedsAttention, message)
}

// fail ends the job for good: the supplier does not know the update, so no resume can help.
func (m *Manager) fail(ctx context.Context, jobID string, code IssueCode, cursor string, cause error) error {
	message := "the supplier does not know this update: " + cause.Error()
	iss := Issue{Code: code, Severity: SeverityBlocking, Cursor: &cursor, Message: message}
	return m.settle(ctx, jobID, &iss, StatusFailed, message)
}

// dbFailure handles a database error the run cannot continue from. Only an error that a
// later attempt can get past (storage.IsRetryable: the connection, load or timing) is
// transient: the job is interrupted and can be resumed. Anything else (a constraint or
// trigger refusal, a client-side encoding failure, a bug) would fail the same way on every
// resume, so it blocks the job for attention instead of inviting a resume loop. A nil
// cursor means "wherever the job is now".
func (m *Manager) dbFailure(ctx context.Context, jobID string, cursor *string, cause error) error {
	if ctx.Err() != nil {
		return m.interrupt(ctx, jobID, nil)
	}
	iss := Issue{Code: CodeDatabaseError, Severity: SeverityTransient, Cursor: cursor, Message: "database error: " + cause.Error()}
	to := StatusInterrupted
	if !storage.IsRetryable(cause) {
		iss.Severity, to = SeverityBlocking, StatusNeedsAttention
	}
	if err := m.settle(ctx, jobID, &iss, to, iss.Message); err != nil {
		return errors.Join(fmt.Errorf("job %s: %w", jobID, cause), err)
	}
	return nil
}

// logFinished reports a job that left RUNNING through its last batch or the completion
// check. A completed job also logs its totals.
func (m *Manager) logFinished(ctx context.Context, jobID string, status Status) {
	job, err := GetJob(context.WithoutCancel(ctx), m.pool, jobID)
	if err != nil {
		m.log.Warn("job.finished_unreadable", "job_id", jobID, "status", status, "error", err.Error())
		return
	}
	log := m.log.With("job_id", jobID, "update_id", job.UpdateID)
	reason := "end of feed reached"
	if job.LastError != nil {
		reason = *job.LastError
	}
	log.Info("job.transition", "from", StatusRunning, "to", status, "reason", reason)
	if status == StatusCompleted {
		log.Info("job.completed", "processed_batches", job.ProcessedBatches,
			"created", job.Created, "updated", job.Updated, "withdrawn", job.Withdrawn,
			"duplicate", job.Duplicate, "stale", job.Stale)
	}
}
