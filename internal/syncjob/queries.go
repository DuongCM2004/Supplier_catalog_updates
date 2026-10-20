package syncjob

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/storage"
)

// Read queries behind the API listings. They take storage.DB so they work on a pool or
// inside a transaction, return rows as the shared model types, and never write.

const (
	listDefaultLimit = 50
	listMaxLimit     = 500
)

// listPage keeps every listing bounded: a zero or negative limit means the default page
// size, anything above the cap is cut to the cap, and a negative offset is zero.
func listPage(limit, offset int) (int, int) {
	if limit <= 0 {
		limit = listDefaultLimit
	}
	return min(limit, listMaxLimit), max(offset, 0)
}

// listUUIDPattern is the canonical text form of a job id. Anything else cannot name a job,
// so it is answered as "not found" instead of being sent to PostgreSQL, which would reject
// it with an error that looks like a server failure.
var listUUIDPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

const listJobExistsSQL = `SELECT EXISTS (SELECT 1 FROM sync_jobs WHERE id = $1::uuid)`

// listRequireJob returns ErrNotFound unless jobID names an existing job, so an unknown job
// is distinguishable from a job that simply has no rows yet.
func listRequireJob(ctx context.Context, db storage.DB, jobID string) error {
	if !listUUIDPattern.MatchString(jobID) {
		return ErrNotFound
	}
	var exists bool
	if err := db.QueryRow(ctx, listJobExistsSQL, jobID).Scan(&exists); err != nil {
		return fmt.Errorf("syncjob: check job %s: %w", jobID, err)
	}
	if !exists {
		return ErrNotFound
	}
	return nil
}

// The open blocking issue count is computed per job instead of stored, so it can never
// drift from the issues table.
const listJobColumns = `j.id::text, j.supplier_id, j.supplier_update_id, j.status, j.next_cursor, j.last_batch_id,
  j.processed_batches, j.total_batches, j.end_of_feed_seen,
  j.created_count, j.updated_count, j.withdrawn_count, j.duplicate_count, j.stale_count,
  (SELECT count(*) FROM sync_issues i WHERE i.job_id = j.id AND i.status = 'OPEN' AND i.severity = 'BLOCKING')::int,
  j.last_error, j.created_at, j.started_at, j.completed_at, j.updated_at`

func listScanJob(row pgx.CollectableRow) (Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.SupplierID, &j.UpdateID, &j.Status, &j.NextCursor, &j.LastBatchID,
		&j.ProcessedBatches, &j.TotalBatches, &j.EndOfFeedSeen,
		&j.Created, &j.Updated, &j.Withdrawn, &j.Duplicate, &j.Stale,
		&j.OpenBlockingIssues,
		&j.LastError, &j.CreatedAt, &j.StartedAt, &j.CompletedAt, &j.UpdatedAt)
	return j, err
}

const listJobsSQL = `
SELECT ` + listJobColumns + `
FROM sync_jobs j
WHERE ($1::text = '' OR j.status = $1)
ORDER BY j.created_at DESC, j.id
LIMIT $2 OFFSET $3`

// ListJobs returns jobs newest first; status "" means every status. Jobs created at the
// same instant are ordered by id so paging is stable.
func ListJobs(ctx context.Context, db storage.DB, status string, limit, offset int) ([]Job, error) {
	limit, offset = listPage(limit, offset)
	rows, err := db.Query(ctx, listJobsSQL, status, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("syncjob: list jobs: %w", err)
	}
	jobs, err := pgx.CollectRows(rows, listScanJob)
	if err != nil {
		return nil, fmt.Errorf("syncjob: list jobs: %w", err)
	}
	return jobs, nil
}

const listChangesSQL = `
SELECT id::text, job_id::text, batch_id, supplier_id, product_id, revision, change_type, before, after, created_at
FROM sync_changes
WHERE job_id = $1::uuid AND ($2::text = '' OR change_type = $2)
ORDER BY created_at, id
LIMIT $3 OFFSET $4`

func listScanChange(row pgx.CollectableRow) (Change, error) {
	var c Change
	var before, after []byte
	err := row.Scan(&c.ID, &c.JobID, &c.BatchID, &c.SupplierID, &c.ProductID, &c.Revision, &c.ChangeType,
		&before, &after, &c.CreatedAt)
	// Before is SQL NULL when the product did not exist yet; it stays nil, which encodes
	// as JSON null.
	c.Before, c.After = json.RawMessage(before), json.RawMessage(after)
	return c, err
}

// ListChanges returns the catalog changes a job applied, ordered by creation time then id;
// changeType "" means every type. Changes written by one transaction share a timestamp
// unless the writer sets it per row, so within a batch the id decides the order: stable for
// paging, but not necessarily the order the events were applied in. An unknown or
// malformed job id is ErrNotFound.
func ListChanges(ctx context.Context, db storage.DB, jobID, changeType string, limit, offset int) ([]Change, error) {
	if err := listRequireJob(ctx, db, jobID); err != nil {
		return nil, err
	}
	limit, offset = listPage(limit, offset)
	rows, err := db.Query(ctx, listChangesSQL, jobID, changeType, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("syncjob: list changes of job %s: %w", jobID, err)
	}
	changes, err := pgx.CollectRows(rows, listScanChange)
	if err != nil {
		return nil, fmt.Errorf("syncjob: list changes of job %s: %w", jobID, err)
	}
	return changes, nil
}

const listIssuesSQL = `
SELECT id::text, job_id::text, batch_id, cursor, supplier_id, product_id, code, severity, status, message,
  retry_count, first_seen_at, last_seen_at, resolved_at
FROM sync_issues
WHERE job_id = $1::uuid AND ($2::text = '' OR status = $2) AND ($3::text = '' OR severity = $3)
ORDER BY first_seen_at, id
LIMIT $4 OFFSET $5`

func listScanIssue(row pgx.CollectableRow) (Issue, error) {
	var i Issue
	err := row.Scan(&i.ID, &i.JobID, &i.BatchID, &i.Cursor, &i.SupplierID, &i.ProductID, &i.Code, &i.Severity,
		&i.Status, &i.Message, &i.RetryCount, &i.FirstSeenAt, &i.LastSeenAt, &i.ResolvedAt)
	return i, err
}

// ListIssues returns a job's issues oldest first; status ("OPEN" or "RESOLVED") and
// severity filters are skipped when empty. An unknown or malformed job id is ErrNotFound.
func ListIssues(ctx context.Context, db storage.DB, jobID, status, severity string, limit, offset int) ([]Issue, error) {
	if err := listRequireJob(ctx, db, jobID); err != nil {
		return nil, err
	}
	limit, offset = listPage(limit, offset)
	rows, err := db.Query(ctx, listIssuesSQL, jobID, status, severity, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("syncjob: list issues of job %s: %w", jobID, err)
	}
	issues, err := pgx.CollectRows(rows, listScanIssue)
	if err != nil {
		return nil, fmt.Errorf("syncjob: list issues of job %s: %w", jobID, err)
	}
	return issues, nil
}
