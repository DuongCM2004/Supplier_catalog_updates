package syncjob

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/storage"
)

// This file is the persistence layer for jobs. Functions that must run inside a
// transaction take pgx.Tx; reads and single-statement writes take storage.DB so they work
// on the pool and inside a transaction alike.
//
// Locking contract: every transaction that writes job-scoped state first takes the job row
// lock with lockJob, then product rows. Counting open blocking issues inside that
// transaction is therefore race-free.

// allowedTransitions is the single source of the job state machine. COMPLETED and FAILED
// are terminal. RUNNING -> RUNNING exists only so resume can adopt a job whose worker died
// without recording it (the manager checks no live worker owns it first).
var allowedTransitions = map[Status][]Status{
	StatusPending:        {StatusRunning, StatusInterrupted},
	StatusRunning:        {StatusRunning, StatusCompleted, StatusNeedsAttention, StatusInterrupted, StatusFailed},
	StatusInterrupted:    {StatusRunning},
	StatusNeedsAttention: {StatusRunning},
	StatusCompleted:      nil,
	StatusFailed:         nil,
}

func canTransition(from, to Status) bool {
	return slices.Contains(allowedTransitions[from], to)
}

// sourcesOf lists, in a stable order, every state the table allows to move to `to`.
func sourcesOf(to Status) []Status {
	var from []Status
	for _, s := range slices.Sorted(maps.Keys(allowedTransitions)) {
		if canTransition(s, to) {
			from = append(from, s)
		}
	}
	return from
}

func statusStrings(ss []Status) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = string(s)
	}
	return out
}

// jobIDPattern is checked before any SQL so that text which is not a UUID reads as "no such
// job" instead of surfacing a PostgreSQL cast error.
var jobIDPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func isJobID(id string) bool { return jobIDPattern.MatchString(id) }

// jobFields is the column list scanJob expects, from a table aliased as j. The blocking
// issue count is computed here so every reader reports it the same way.
const jobFields = `j.id::text, j.supplier_id, j.supplier_update_id, j.status, j.next_cursor, j.last_batch_id,
	j.processed_batches, j.total_batches, j.end_of_feed_seen,
	j.created_count, j.updated_count, j.withdrawn_count, j.duplicate_count, j.stale_count,
	(SELECT count(*) FROM sync_issues i WHERE i.job_id = j.id AND i.status = 'OPEN' AND i.severity = 'BLOCKING')::int,
	j.last_error, j.created_at, j.started_at, j.completed_at, j.updated_at`

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.SupplierID, &j.UpdateID, &j.Status, &j.NextCursor, &j.LastBatchID,
		&j.ProcessedBatches, &j.TotalBatches, &j.EndOfFeedSeen,
		&j.Created, &j.Updated, &j.Withdrawn, &j.Duplicate, &j.Stale,
		&j.OpenBlockingIssues,
		&j.LastError, &j.CreatedAt, &j.StartedAt, &j.CompletedAt, &j.UpdatedAt)
	return j, err
}

const sqlGetJob = `SELECT ` + jobFields + ` FROM sync_jobs j WHERE j.id = $1::uuid`

// GetJob loads one job with its open blocking issue count. Text that is not a job id and
// ids that do not exist both return ErrNotFound.
func GetJob(ctx context.Context, db storage.DB, id string) (Job, error) {
	if !isJobID(id) {
		return Job{}, fmt.Errorf("get job %q: %w", id, ErrNotFound)
	}
	job, err := scanJob(db.QueryRow(ctx, sqlGetJob, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, fmt.Errorf("get job %s: %w", id, ErrNotFound)
	}
	if err != nil {
		return Job{}, fmt.Errorf("get job %s: %w", id, err)
	}
	return job, nil
}

const sqlLockJob = `SELECT ` + jobFields + ` FROM sync_jobs j WHERE j.id = $1::uuid FOR UPDATE OF j`

// lockJob reads the job and holds its row lock until the transaction ends, serialising
// every writer of that job's state (batch commits, issue recording, resume).
func lockJob(ctx context.Context, tx pgx.Tx, id string) (Job, error) {
	if !isJobID(id) {
		return Job{}, fmt.Errorf("lock job %q: %w", id, ErrNotFound)
	}
	job, err := scanJob(tx.QueryRow(ctx, sqlLockJob, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, fmt.Errorf("lock job %s: %w", id, ErrNotFound)
	}
	if err != nil {
		return Job{}, fmt.Errorf("lock job %s: %w", id, err)
	}
	return job, nil
}

const (
	sqlInsertJob = `INSERT INTO sync_jobs AS j (supplier_id, supplier_update_id) VALUES ($1, $2)
		ON CONFLICT (supplier_id, supplier_update_id) DO NOTHING RETURNING ` + jobFields
	sqlJobByUpdate = `SELECT ` + jobFields + ` FROM sync_jobs j WHERE j.supplier_id = $1 AND j.supplier_update_id = $2`
	sqlActiveJob   = `SELECT ` + jobFields + ` FROM sync_jobs j WHERE j.status IN ('PENDING','RUNNING') LIMIT 1`
)

// oneActiveIndex is the partial unique index that admits a single PENDING/RUNNING job.
const oneActiveIndex = "sync_jobs_one_active"

// createJob starts tracking an update. Starting the same update again returns the job it
// already has with created=false, whatever state that job is in. Starting a different
// update while another job is PENDING/RUNNING returns ErrActiveJobExists together with the
// active job (zero Job if it ended in the meantime) so the caller can name it.
func createJob(ctx context.Context, db storage.DB, supplierID, updateID string) (Job, bool, error) {
	job, err := scanJob(db.QueryRow(ctx, sqlInsertJob, supplierID, updateID))
	switch {
	case err == nil:
		return job, true, nil
	case errors.Is(err, pgx.ErrNoRows):
		// ON CONFLICT DO NOTHING: this update already has a job.
		existing, err := jobByUpdate(ctx, db, supplierID, updateID)
		if err != nil {
			return Job{}, false, fmt.Errorf("create job: load existing job: %w", err)
		}
		return existing, false, nil
	case storage.IsUniqueViolation(err, oneActiveIndex):
		return resolveActiveConflict(ctx, db, supplierID, updateID)
	}
	return Job{}, false, fmt.Errorf("create job for update %s: %w", updateID, err)
}

// resolveActiveConflict runs after the one-active index rejected an insert. Two concurrent
// starts of the same update both pass the ON CONFLICT check and the loser is stopped by
// this index instead, so the update's own job is looked up first: finding it means the
// start succeeded elsewhere and is idempotent; only an absent job means a different update
// holds the slot.
func resolveActiveConflict(ctx context.Context, db storage.DB, supplierID, updateID string) (Job, bool, error) {
	job, err := jobByUpdate(ctx, db, supplierID, updateID)
	if err == nil {
		return job, false, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return Job{}, false, fmt.Errorf("create job: look up concurrent start: %w", err)
	}
	active, err := scanJob(db.QueryRow(ctx, sqlActiveJob))
	switch {
	case err == nil:
		return active, false, ErrActiveJobExists
	case errors.Is(err, pgx.ErrNoRows): // it finished between the insert and this lookup
		return Job{}, false, ErrActiveJobExists
	}
	return Job{}, false, fmt.Errorf("%w (active job lookup failed: %v)", ErrActiveJobExists, err)
}

func jobByUpdate(ctx context.Context, db storage.DB, supplierID, updateID string) (Job, error) {
	job, err := scanJob(db.QueryRow(ctx, sqlJobByUpdate, supplierID, updateID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, fmt.Errorf("update %s: %w", updateID, ErrNotFound)
	}
	if err != nil {
		return Job{}, fmt.Errorf("update %s: %w", updateID, err)
	}
	return job, nil
}

const (
	// last_error is overwritten on every transition so a job that recovered stops showing
	// the failure it had before; started_at records the first time the job ran.
	sqlTransition = `UPDATE sync_jobs SET
			status = $2::text,
			last_error = $3::text,
			updated_at = now(),
			started_at = CASE WHEN $2::text = 'RUNNING' THEN COALESCE(started_at, now()) ELSE started_at END,
			completed_at = CASE WHEN $2::text = 'COMPLETED' THEN now() ELSE completed_at END
		WHERE id = $1::uuid AND status = ANY($4::text[])`
	sqlJobStatus = `SELECT status FROM sync_jobs WHERE id = $1::uuid`
)

// transition moves a job to `to` only if it is currently in one of the `from` states and
// the state machine allows that edge; sources the table forbids are dropped from `from`,
// and nil `from` means every state the table allows. A job in any other state is left
// untouched and ErrInvalidTransition is returned (ErrNotFound if there is no such job).
// lastError replaces the stored last_error; nil clears it.
func transition(ctx context.Context, db storage.DB, id string, from []Status, to Status, lastError *string) error {
	if !isJobID(id) {
		return fmt.Errorf("transition job %q: %w", id, ErrNotFound)
	}
	allowed := sourcesOf(to)
	if from != nil {
		allowed = slices.DeleteFunc(slices.Clone(from), func(s Status) bool { return !canTransition(s, to) })
	}
	if len(allowed) == 0 {
		return fmt.Errorf("transition job %s to %s from %v: %w", id, to, from, ErrInvalidTransition)
	}
	tag, err := db.Exec(ctx, sqlTransition, id, string(to), lastError, statusStrings(allowed))
	if err != nil {
		return fmt.Errorf("transition job %s to %s: %w", id, to, err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var current string
	err = db.QueryRow(ctx, sqlJobStatus, id).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("transition job %s: %w", id, ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("transition job %s: read status: %w", id, err)
	}
	return fmt.Errorf("job %s is %s, cannot move to %s: %w", id, current, to, ErrInvalidTransition)
}

const sqlReconcile = `UPDATE sync_jobs SET status = 'INTERRUPTED', last_error = $1, updated_at = now()
	WHERE status = ANY($2::text[]) RETURNING id::text`

// reconcile marks every job that was PENDING or RUNNING as INTERRUPTED and returns their
// ids. It is only safe at startup, before any worker exists: at that moment a job still
// marked active belongs to a process that no longer runs. Resuming stays an explicit
// operator action.
func reconcile(ctx context.Context, db storage.DB, lastError string) ([]string, error) {
	rows, err := db.Query(ctx, sqlReconcile, lastError, statusStrings(sourcesOf(StatusInterrupted)))
	if err != nil {
		return nil, fmt.Errorf("reconcile jobs: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("reconcile jobs: %w", err)
	}
	slices.Sort(ids)
	return ids, nil
}

// fingerprint identifies "this problem at this place" within a job: the same code at the
// same cursor is the same issue, so a retry that fails the same way updates one open row
// instead of adding another. The job comes from the unique index, not from the hash.
func fingerprint(code IssueCode, cursor string) string {
	sum := sha256.Sum256([]byte(string(code) + "|" + cursor))
	return hex.EncodeToString(sum[:])
}

// A recurrence refreshes everything that describes the latest occurrence. Severity only
// ever escalates: a DATABASE_ERROR first seen as a connection blip (TRANSIENT) and later
// as a data error (BLOCKING) must end up blocking, while a later blip must not hide a
// blocking error that is still unresolved.
const sqlRecordIssue = `INSERT INTO sync_issues
		(job_id, batch_id, cursor, supplier_id, product_id, code, severity, message, fingerprint, retry_count)
	VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	ON CONFLICT (job_id, fingerprint) WHERE status = 'OPEN' DO UPDATE SET
		retry_count = sync_issues.retry_count + 1,
		last_seen_at = now(),
		message = EXCLUDED.message,
		batch_id = EXCLUDED.batch_id,
		supplier_id = EXCLUDED.supplier_id,
		product_id = EXCLUDED.product_id,
		severity = CASE WHEN EXCLUDED.severity = 'BLOCKING' THEN 'BLOCKING' ELSE sync_issues.severity END`

// recordIssue stores a problem for iss.JobID, or bumps retry_count on the open issue with
// the same code and cursor. Callers hold the job row lock (lockJob) in tx, which is what
// makes the open-issue count read at completion time race-free. iss.Cursor is required and
// must be the job's next_cursor when the problem was found: it is what lets the issue
// resolve itself once the batch at that cursor commits.
func recordIssue(ctx context.Context, tx pgx.Tx, iss Issue) error {
	if iss.Cursor == nil {
		return fmt.Errorf("record %s issue for job %s: cursor is required so the issue can resolve", iss.Code, iss.JobID)
	}
	_, err := tx.Exec(ctx, sqlRecordIssue, iss.JobID, iss.BatchID, iss.Cursor, iss.SupplierID, iss.ProductID,
		string(iss.Code), string(iss.Severity), iss.Message, fingerprint(iss.Code, *iss.Cursor), iss.RetryCount)
	if err != nil {
		return fmt.Errorf("record %s issue for job %s: %w", iss.Code, iss.JobID, err)
	}
	return nil
}

const sqlResolveIssues = `UPDATE sync_issues SET status = 'RESOLVED', resolved_at = now()
	WHERE job_id = $1::uuid AND status = 'OPEN' AND (severity = 'TRANSIENT' OR cursor = $2)`

// resolveIssues closes what a successful commit of the batch at `cursor` proves is over:
// every transient problem (the supplier or database answered again) and the blocking
// problems raised at that same cursor. Blocking issues at other cursors stay open. Callers
// hold the job row lock in tx.
func resolveIssues(ctx context.Context, tx pgx.Tx, jobID, cursor string) error {
	if _, err := tx.Exec(ctx, sqlResolveIssues, jobID, cursor); err != nil {
		return fmt.Errorf("resolve issues for job %s at cursor %q: %w", jobID, cursor, err)
	}
	return nil
}

const sqlResolveTransientIssues = `UPDATE sync_issues SET status = 'RESOLVED', resolved_at = now()
	WHERE job_id = $1::uuid AND status = 'OPEN' AND severity = 'TRANSIENT'`

// resolveTransientIssues closes every open transient problem of the job, for runs that
// finish without committing a batch (the completion check after end of feed): the database
// answered, so a recorded outage is over. Blocking issues are never closed this way. Callers
// hold the job row lock in tx.
func resolveTransientIssues(ctx context.Context, tx pgx.Tx, jobID string) error {
	if _, err := tx.Exec(ctx, sqlResolveTransientIssues, jobID); err != nil {
		return fmt.Errorf("resolve transient issues for job %s: %w", jobID, err)
	}
	return nil
}

const sqlCountOpenBlocking = `SELECT count(*) FROM sync_issues WHERE job_id = $1::uuid AND status = 'OPEN' AND severity = 'BLOCKING'`

// countOpenBlocking is the number of unresolved issues that forbid COMPLETED. It is only
// race-free when the caller holds the job row lock.
func countOpenBlocking(ctx context.Context, db storage.DB, jobID string) (int, error) {
	var n int
	if err := db.QueryRow(ctx, sqlCountOpenBlocking, jobID).Scan(&n); err != nil {
		return 0, fmt.Errorf("count open blocking issues for job %s: %w", jobID, err)
	}
	return n, nil
}

// settleJob ends a worker's run of a job in one transaction under the job lock: it records
// iss (when not nil) and moves the job from RUNNING to `to`, with lastError as the reason
// shown to people. A job that is no longer RUNNING (shut down, already settled, resumed
// elsewhere) is left alone and applied is false, so a late arrival can neither overwrite a
// newer state nor attach an issue to a job that has moved on. An issue without a cursor gets
// the job's own next_cursor. prev is the job as it was when locked, for logging.
func settleJob(ctx context.Context, pool *pgxpool.Pool, jobID string, iss *Issue, to Status, lastError string) (prev Job, applied bool, err error) {
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		j, err := lockJob(ctx, tx, jobID)
		if err != nil {
			return err
		}
		prev = j
		if j.Status != StatusRunning {
			return nil
		}
		if iss != nil {
			rec := *iss
			rec.JobID = jobID
			if rec.Cursor == nil {
				cursor := j.NextCursor
				rec.Cursor = &cursor
			}
			if err := recordIssue(ctx, tx, rec); err != nil {
				return err
			}
		}
		if err := transition(ctx, tx, jobID, []Status{StatusRunning}, to, &lastError); err != nil {
			return err
		}
		applied = true
		return nil
	})
	if err != nil {
		return Job{}, false, err
	}
	return prev, applied, nil
}
