package syncjob_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/syncjob"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/testutil"
)

// listBase is the instant seeded rows are measured from; tests seed with explicit timestamps so
// ordering never depends on how fast the test runs.
var listBase = time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)

func listPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testutil.DB(t)
	testutil.Reset(t, pool)
	return pool
}

func listExec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("seed %q: %v", sql, err)
	}
}

// listSeedJob inserts a job with plain SQL. A COMPLETED job gets the end-of-feed facts the
// table requires for that status.
func listSeedJob(t *testing.T, pool *pgxpool.Pool, updateID, status string, created time.Time) string {
	t.Helper()
	var id string
	err := pool.QueryRow(context.Background(),
		`INSERT INTO sync_jobs (supplier_id, supplier_update_id, status, end_of_feed_seen, completed_at, created_at, updated_at)
		 VALUES ('SUP-001', $1, $2::text, $2::text = 'COMPLETED', CASE WHEN $2::text = 'COMPLETED' THEN $3::timestamptz END, $3, $3)
		 RETURNING id::text`, updateID, status, created).Scan(&id)
	if err != nil {
		t.Fatalf("seed %s job %s: %v", status, updateID, err)
	}
	return id
}

func listSeedIssue(t *testing.T, pool *pgxpool.Pool, jobID, code, severity, status, cursor string, seen time.Time) {
	t.Helper()
	var resolvedAt *time.Time
	if status == "RESOLVED" {
		r := seen.Add(time.Minute)
		resolvedAt = &r
	}
	listExec(t, pool,
		`INSERT INTO sync_issues (job_id, batch_id, cursor, supplier_id, product_id, code, severity, status, message, fingerprint, retry_count, first_seen_at, last_seen_at, resolved_at)
		 VALUES ($1::uuid, 'BATCH-002', $2, 'SUP-001', 'SKU-302', $3, $4, $5, $3 || ' at ' || $2, $3 || '|' || $2, 2, $6, $6, $7)`,
		jobID, cursor, code, severity, status, seen, resolvedAt)
}

func listSeedBatch(t *testing.T, pool *pgxpool.Pool, jobID, batchID string, seq int, products ...string) {
	t.Helper()
	listExec(t, pool,
		`INSERT INTO sync_batches (job_id, batch_id, seq, cursor_in, cursor_out, has_more, payload_hash, record_count)
		 VALUES ($1::uuid, $2, $3, 'in-' || $2, 'out-' || $2, true, 'h', 1)`, jobID, batchID, seq)
	for _, p := range products {
		listExec(t, pool,
			`INSERT INTO products (supplier_id, product_id, status, revision, name, price_minor, currency, payload_hash)
			 VALUES ('SUP-001', $1, 'ACTIVE', 1, 'Widget', 100, 'USD', 'h') ON CONFLICT DO NOTHING`, p)
	}
}

// listSeedChange inserts an audit row; before may be empty for a product that did not exist.
func listSeedChange(t *testing.T, pool *pgxpool.Pool, jobID, batchID, productID string, revision int, changeType, before string, created time.Time) {
	t.Helper()
	var beforeJSON *string
	if before != "" {
		beforeJSON = &before
	}
	listExec(t, pool,
		`INSERT INTO sync_changes (job_id, batch_id, supplier_id, product_id, revision, change_type, before, after, created_at)
		 VALUES ($1::uuid, $2, 'SUP-001', $3, $4, $5, $6::jsonb, '{"status":"ACTIVE","revision":2}'::jsonb, $7)`,
		jobID, batchID, productID, revision, changeType, beforeJSON, created)
}

func listJobIDs(jobs []syncjob.Job) []string {
	out := []string{}
	for _, j := range jobs {
		out = append(out, j.ID)
	}
	return out
}

func TestListJobsReturnsNewestFirstWithFiltersAndPaging(t *testing.T) {
	pool := listPool(t)
	ctx := context.Background()
	a := listSeedJob(t, pool, "UPDATE-A", "COMPLETED", listBase)
	b := listSeedJob(t, pool, "UPDATE-B", "INTERRUPTED", listBase.Add(1*time.Minute))
	c := listSeedJob(t, pool, "UPDATE-C", "NEEDS_ATTENTION", listBase.Add(2*time.Minute))
	d := listSeedJob(t, pool, "UPDATE-D", "RUNNING", listBase.Add(3*time.Minute))
	// Two jobs created at the same instant: the id breaks the tie so pages never overlap.
	e := listSeedJob(t, pool, "UPDATE-E", "FAILED", listBase.Add(4*time.Minute))
	f := listSeedJob(t, pool, "UPDATE-F", "INTERRUPTED", listBase.Add(4*time.Minute))
	tied := []string{e, f}
	slices.Sort(tied)

	all, err := syncjob.ListJobs(ctx, pool, "", 50, 0)
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if want := append(tied, d, c, b, a); !slices.Equal(listJobIDs(all), want) {
		t.Fatalf("order = %v, want %v", listJobIDs(all), want)
	}

	tests := []struct {
		name          string
		status        string
		limit, offset int
		want          []string
	}{
		{"one status, newest first", "INTERRUPTED", 50, 0, []string{f, b}},
		{"status without jobs", "PENDING", 50, 0, []string{}},
		{"unknown status", "BOGUS", 50, 0, []string{}},
		{"first page", "", 2, 0, tied},
		{"second page", "", 2, 2, []string{d, c}},
		{"last page", "", 2, 4, []string{b, a}},
		{"past the end", "", 2, 6, []string{}},
		{"default page size", "", 0, 0, append(slices.Clone(tied), d, c, b, a)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := syncjob.ListJobs(ctx, pool, tc.status, tc.limit, tc.offset)
			if err != nil {
				t.Fatalf("ListJobs: %v", err)
			}
			if got == nil || !slices.Equal(listJobIDs(got), tc.want) {
				t.Fatalf("jobs = %v (nil=%t), want %v", listJobIDs(got), got == nil, tc.want)
			}
		})
	}
}

func TestListJobsFillsEveryJobField(t *testing.T) {
	pool := listPool(t)
	id := listSeedJob(t, pool, "UPDATE-1", "INTERRUPTED", listBase)
	listExec(t, pool,
		`UPDATE sync_jobs SET next_cursor = 'cursor-003', last_batch_id = 'BATCH-002', processed_batches = 2, total_batches = 5,
		   created_count = 4, updated_count = 3, withdrawn_count = 2, duplicate_count = 1, stale_count = 7,
		   last_error = 'supplier unavailable at cursor-003 after 4 attempts', started_at = $2
		 WHERE id = $1::uuid`, id, listBase.Add(time.Second))

	jobs, err := syncjob.ListJobs(context.Background(), pool, "", 10, 0)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("ListJobs = %v, %v", jobs, err)
	}
	j := jobs[0]
	if j.ID != id || j.SupplierID != "SUP-001" || j.UpdateID != "UPDATE-1" || j.Status != syncjob.StatusInterrupted {
		t.Fatalf("identity = %+v", j)
	}
	if j.NextCursor != "cursor-003" || j.LastBatchID == nil || *j.LastBatchID != "BATCH-002" || j.ProcessedBatches != 2 || j.TotalBatches == nil || *j.TotalBatches != 5 || j.EndOfFeedSeen {
		t.Fatalf("progress = %+v", j)
	}
	if j.Created != 4 || j.Updated != 3 || j.Withdrawn != 2 || j.Duplicate != 1 || j.Stale != 7 {
		t.Fatalf("counters = %+v", j)
	}
	if j.LastError == nil || !strings.Contains(*j.LastError, "cursor-003") {
		t.Fatalf("last_error = %v", j.LastError)
	}
	if !j.CreatedAt.Equal(listBase) || j.StartedAt == nil || !j.StartedAt.Equal(listBase.Add(time.Second)) || j.CompletedAt != nil {
		t.Fatalf("timestamps = %+v", j)
	}

	// A job that has not started and has no announced total keeps those fields empty.
	listExec(t, pool, `UPDATE sync_jobs SET total_batches = NULL, last_batch_id = NULL, last_error = NULL, started_at = NULL WHERE id = $1::uuid`, id)
	jobs, err = syncjob.ListJobs(context.Background(), pool, "", 10, 0)
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if j := jobs[0]; j.TotalBatches != nil || j.LastBatchID != nil || j.LastError != nil || j.StartedAt != nil {
		t.Fatalf("empty fields = %+v", j)
	}
}

func TestListJobsCountsOnlyOpenBlockingIssues(t *testing.T) {
	pool := listPool(t)
	clean := listSeedJob(t, pool, "UPDATE-CLEAN", "COMPLETED", listBase)
	stuck := listSeedJob(t, pool, "UPDATE-STUCK", "NEEDS_ATTENTION", listBase.Add(time.Minute))
	listSeedIssue(t, pool, stuck, "INVALID_PRODUCT_DATA", "BLOCKING", "OPEN", "cursor-002", listBase)
	listSeedIssue(t, pool, stuck, "VERSION_CONFLICT", "BLOCKING", "OPEN", "cursor-003", listBase)
	listSeedIssue(t, pool, stuck, "INVALID_BATCH", "BLOCKING", "RESOLVED", "cursor-001", listBase)
	listSeedIssue(t, pool, stuck, "SUPPLIER_UNAVAILABLE", "TRANSIENT", "OPEN", "cursor-004", listBase)

	jobs, err := syncjob.ListJobs(context.Background(), pool, "", 10, 0)
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	open := map[string]int{}
	for _, j := range jobs {
		open[j.ID] = j.OpenBlockingIssues
	}
	if open[stuck] != 2 || open[clean] != 0 {
		t.Fatalf("open blocking issues = %v, want stuck=2 clean=0", open)
	}
}

func TestListChangesFiltersByTypeAndOrdersByCreationTime(t *testing.T) {
	pool := listPool(t)
	ctx := context.Background()
	job := listSeedJob(t, pool, "UPDATE-1", "INTERRUPTED", listBase)
	other := listSeedJob(t, pool, "UPDATE-2", "FAILED", listBase.Add(time.Minute))
	listSeedBatch(t, pool, job, "BATCH-001", 1, "P1", "P2", "P3", "P4")
	listSeedBatch(t, pool, job, "BATCH-002", 2)
	listSeedBatch(t, pool, other, "BATCH-001", 1, "P5")

	const wasActive = `{"status":"ACTIVE","revision":1,"name":"Widget","price_minor":100,"currency":"USD"}`
	listSeedChange(t, pool, job, "BATCH-001", "P1", 1, "CREATED", "", listBase.Add(1*time.Second))
	listSeedChange(t, pool, job, "BATCH-001", "P2", 1, "CREATED", "", listBase.Add(2*time.Second))
	listSeedChange(t, pool, job, "BATCH-002", "P1", 2, "UPDATED", wasActive, listBase.Add(3*time.Second))
	listSeedChange(t, pool, job, "BATCH-002", "P2", 2, "WITHDRAWN", wasActive, listBase.Add(4*time.Second))
	// Created at the same instant: the id decides.
	listSeedChange(t, pool, job, "BATCH-002", "P3", 1, "CREATED", "", listBase.Add(5*time.Second))
	listSeedChange(t, pool, job, "BATCH-002", "P4", 1, "CREATED", "", listBase.Add(5*time.Second))
	listSeedChange(t, pool, other, "BATCH-001", "P5", 1, "CREATED", "", listBase)

	all, err := syncjob.ListChanges(ctx, pool, job, "", 50, 0)
	if err != nil {
		t.Fatalf("ListChanges: %v", err)
	}
	var got []string
	for _, c := range all {
		got = append(got, c.ProductID+"@"+c.ChangeType)
	}
	if want := []string{"P1@CREATED", "P2@CREATED", "P1@UPDATED", "P2@WITHDRAWN"}; !slices.Equal(got[:4], want) {
		t.Fatalf("order = %v, want it to start with %v", got, want)
	}
	if len(all) != 6 {
		t.Fatalf("changes = %d, want 6 (the other job's change must not appear)", len(all))
	}
	if !(all[4].ID < all[5].ID) || !all[4].CreatedAt.Equal(all[5].CreatedAt) {
		t.Fatalf("tied changes not ordered by id: %s, %s", all[4].ID, all[5].ID)
	}

	tests := []struct {
		name          string
		changeType    string
		limit, offset int
		want          []string
	}{
		{"updated only", "UPDATED", 50, 0, []string{"P1@UPDATED"}},
		{"withdrawn only", "WITHDRAWN", 50, 0, []string{"P2@WITHDRAWN"}},
		{"created only", "CREATED", 2, 0, []string{"P1@CREATED", "P2@CREATED"}},
		{"unknown type", "DELETED", 50, 0, []string{}},
		{"middle page", "", 2, 1, []string{"P2@CREATED", "P1@UPDATED"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			changes, err := syncjob.ListChanges(ctx, pool, job, tc.changeType, tc.limit, tc.offset)
			if err != nil {
				t.Fatalf("ListChanges: %v", err)
			}
			got := []string{}
			for _, c := range changes {
				got = append(got, c.ProductID+"@"+c.ChangeType)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("changes = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestListChangesCarriesBeforeAndAfterSnapshots(t *testing.T) {
	pool := listPool(t)
	job := listSeedJob(t, pool, "UPDATE-1", "INTERRUPTED", listBase)
	listSeedBatch(t, pool, job, "BATCH-001", 1, "P1")
	listSeedChange(t, pool, job, "BATCH-001", "P1", 1, "CREATED", "", listBase)
	listSeedChange(t, pool, job, "BATCH-001", "P1", 2, "UPDATED", `{"status":"ACTIVE","revision":1}`, listBase.Add(time.Second))

	changes, err := syncjob.ListChanges(context.Background(), pool, job, "", 10, 0)
	if err != nil || len(changes) != 2 {
		t.Fatalf("ListChanges = %v, %v", changes, err)
	}

	created, updated := changes[0], changes[1]
	if created.JobID != job || created.BatchID != "BATCH-001" || created.SupplierID != "SUP-001" || created.Revision != 1 || created.CreatedAt.IsZero() {
		t.Fatalf("created change = %+v", created)
	}
	if created.Before != nil {
		t.Fatalf("Before of a created product = %s, want nil", created.Before)
	}
	if out, err := json.Marshal(created); err != nil || !strings.Contains(string(out), `"Before":null`) {
		t.Fatalf("created change encodes as %s, %v; want Before null", out, err)
	}

	var before, after map[string]any
	if err := json.Unmarshal(updated.Before, &before); err != nil || before["revision"] != float64(1) {
		t.Fatalf("Before = %s, %v", updated.Before, err)
	}
	if err := json.Unmarshal(updated.After, &after); err != nil || after["revision"] != float64(2) {
		t.Fatalf("After = %s, %v", updated.After, err)
	}
}

func TestListIssuesFiltersByStatusAndSeverity(t *testing.T) {
	pool := listPool(t)
	ctx := context.Background()
	job := listSeedJob(t, pool, "UPDATE-1", "NEEDS_ATTENTION", listBase)
	other := listSeedJob(t, pool, "UPDATE-2", "FAILED", listBase.Add(time.Minute))
	listSeedIssue(t, pool, job, "SUPPLIER_UNAVAILABLE", "TRANSIENT", "RESOLVED", "cursor-001", listBase)
	listSeedIssue(t, pool, job, "INVALID_PRODUCT_DATA", "BLOCKING", "OPEN", "cursor-002", listBase.Add(time.Minute))
	listSeedIssue(t, pool, job, "VERSION_CONFLICT", "BLOCKING", "OPEN", "cursor-003", listBase.Add(2*time.Minute))
	listSeedIssue(t, pool, job, "SUPPLIER_UNAVAILABLE", "TRANSIENT", "OPEN", "cursor-004", listBase.Add(3*time.Minute))
	listSeedIssue(t, pool, other, "UNKNOWN_UPDATE", "BLOCKING", "OPEN", "", listBase)

	tests := []struct {
		name             string
		status, severity string
		limit, offset    int
		want             []string
	}{
		{"everything oldest first", "", "", 50, 0, []string{"SUPPLIER_UNAVAILABLE", "INVALID_PRODUCT_DATA", "VERSION_CONFLICT", "SUPPLIER_UNAVAILABLE"}},
		{"open only", "OPEN", "", 50, 0, []string{"INVALID_PRODUCT_DATA", "VERSION_CONFLICT", "SUPPLIER_UNAVAILABLE"}},
		{"resolved only", "RESOLVED", "", 50, 0, []string{"SUPPLIER_UNAVAILABLE"}},
		{"blocking only", "", "BLOCKING", 50, 0, []string{"INVALID_PRODUCT_DATA", "VERSION_CONFLICT"}},
		{"open and transient", "OPEN", "TRANSIENT", 50, 0, []string{"SUPPLIER_UNAVAILABLE"}},
		{"no match", "RESOLVED", "BLOCKING", 50, 0, []string{}},
		{"second page", "", "", 2, 2, []string{"VERSION_CONFLICT", "SUPPLIER_UNAVAILABLE"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			issues, err := syncjob.ListIssues(ctx, pool, job, tc.status, tc.severity, tc.limit, tc.offset)
			if err != nil {
				t.Fatalf("ListIssues: %v", err)
			}
			got := []string{}
			for _, i := range issues {
				got = append(got, string(i.Code))
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("issues = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestListIssuesFillsEveryIssueField(t *testing.T) {
	pool := listPool(t)
	job := listSeedJob(t, pool, "UPDATE-1", "NEEDS_ATTENTION", listBase)
	listSeedIssue(t, pool, job, "INVALID_PRODUCT_DATA", "BLOCKING", "RESOLVED", "cursor-002", listBase)

	issues, err := syncjob.ListIssues(context.Background(), pool, job, "", "", 10, 0)
	if err != nil || len(issues) != 1 {
		t.Fatalf("ListIssues = %v, %v", issues, err)
	}
	i := issues[0]
	if i.ID == "" || i.JobID != job || i.Code != syncjob.CodeInvalidProductData || i.Severity != syncjob.SeverityBlocking || i.Status != "RESOLVED" {
		t.Fatalf("identity = %+v", i)
	}
	if i.BatchID == nil || *i.BatchID != "BATCH-002" || i.Cursor == nil || *i.Cursor != "cursor-002" ||
		i.SupplierID == nil || *i.SupplierID != "SUP-001" || i.ProductID == nil || *i.ProductID != "SKU-302" {
		t.Fatalf("location = %+v", i)
	}
	if i.Message != "INVALID_PRODUCT_DATA at cursor-002" || i.RetryCount != 2 {
		t.Fatalf("message/retry = %q / %d", i.Message, i.RetryCount)
	}
	if !i.FirstSeenAt.Equal(listBase) || !i.LastSeenAt.Equal(listBase) || i.ResolvedAt == nil || !i.ResolvedAt.Equal(listBase.Add(time.Minute)) {
		t.Fatalf("timestamps = %+v", i)
	}
}

func TestListChangesAndIssuesReportUnknownJobAsNotFound(t *testing.T) {
	pool := listPool(t)
	ctx := context.Background()
	empty := listSeedJob(t, pool, "UPDATE-1", "INTERRUPTED", listBase)

	unknown := []string{
		"00000000-0000-0000-0000-000000000000", // well-formed, no such job
		"not-a-uuid",
		"",
		"3f7c' OR '1'='1",
		strings.Repeat("a", 500),
	}
	for _, id := range unknown {
		if _, err := syncjob.ListChanges(ctx, pool, id, "", 10, 0); !errors.Is(err, syncjob.ErrNotFound) {
			t.Errorf("ListChanges(%.20q): want ErrNotFound, got %v", id, err)
		}
		if _, err := syncjob.ListIssues(ctx, pool, id, "", "", 10, 0); !errors.Is(err, syncjob.ErrNotFound) {
			t.Errorf("ListIssues(%.20q): want ErrNotFound, got %v", id, err)
		}
	}

	// A job that exists but has nothing yet is an empty list, not "not found".
	changes, err := syncjob.ListChanges(ctx, pool, empty, "", 10, 0)
	if err != nil || changes == nil || len(changes) != 0 {
		t.Fatalf("ListChanges of an empty job = %v, %v", changes, err)
	}
	issues, err := syncjob.ListIssues(ctx, pool, empty, "", "", 10, 0)
	if err != nil || issues == nil || len(issues) != 0 {
		t.Fatalf("ListIssues of an empty job = %v, %v", issues, err)
	}
}
