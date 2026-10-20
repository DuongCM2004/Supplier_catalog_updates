package syncjob_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/storage"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/supplier"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/syncjob"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/testutil"
)

// These tests call ProcessBatch directly on a job seeded with plain SQL, so each one pins a
// single decision of the batch transaction on a real PostgreSQL database. "Unchanged" is
// always proven with a snapshot of the tables, not by spot-checking a few columns.

const prcUpdate = "UPDATE-1"

func prcPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testutil.DB(t)
	testutil.Reset(t, pool)
	return pool
}

// prcSeedJob inserts a job in the given status. totalBatches 0 means the supplier did not
// announce a count.
func prcSeedJob(t *testing.T, pool *pgxpool.Pool, status string, totalBatches int) string {
	t.Helper()
	var id string
	err := pool.QueryRow(context.Background(),
		`INSERT INTO sync_jobs (supplier_id, supplier_update_id, status, total_batches, started_at)
		 VALUES ($1, $2, $3::text, NULLIF($4::int, 0), now()) RETURNING id::text`,
		testutil.SupplierID, prcUpdate, status, totalBatches).Scan(&id)
	if err != nil {
		t.Fatalf("seed %s job: %v", status, err)
	}
	return id
}

// prcSeedIssue opens an issue the way an earlier failed attempt would have left it.
func prcSeedIssue(t *testing.T, pool *pgxpool.Pool, jobID string, code syncjob.IssueCode, severity syncjob.Severity, cursor string) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO sync_issues (job_id, cursor, code, severity, message, fingerprint)
		 VALUES ($1::uuid, $2::text, $3::text, $4::text, 'seeded by the test', 'seeded|' || $2::text)`,
		jobID, cursor, string(code), string(severity))
	if err != nil {
		t.Fatalf("seed %s issue at %q: %v", code, cursor, err)
	}
}

func prcProcessor(pool *pgxpool.Pool, hooks syncjob.Hooks) *syncjob.Processor {
	return syncjob.NewProcessor(pool, hooks, slog.New(slog.DiscardHandler))
}

var prcKindNames = map[syncjob.OutcomeKind]string{
	syncjob.KindCommitted: "committed",
	syncjob.KindReplayed:  "replayed",
	syncjob.KindStale:     "stale",
	syncjob.KindBlocked:   "blocked",
}

// prcProcess runs one batch that must not fail at the database level and returns the outcome.
func prcProcess(t *testing.T, p *syncjob.Processor, jobID, cursor string, b supplier.Batch, want syncjob.OutcomeKind) syncjob.Outcome {
	t.Helper()
	out, err := p.ProcessBatch(context.Background(), jobID, cursor, b)
	if err != nil {
		t.Fatalf("ProcessBatch(%s at %q): %v", b.BatchID, cursor, err)
	}
	if out.Kind != want {
		t.Fatalf("ProcessBatch(%s at %q) = %s, want %s", b.BatchID, cursor, prcKindNames[out.Kind], prcKindNames[want])
	}
	return out
}

func prcCommit(t *testing.T, p *syncjob.Processor, jobID, cursor string, b supplier.Batch) syncjob.Outcome {
	t.Helper()
	return prcProcess(t, p, jobID, cursor, b, syncjob.KindCommitted)
}

func prcJob(t *testing.T, pool *pgxpool.Pool, jobID string) syncjob.Job {
	t.Helper()
	job, err := syncjob.GetJob(context.Background(), pool, jobID)
	if err != nil {
		t.Fatalf("GetJob(%s): %v", jobID, err)
	}
	return job
}

func prcCounts(j syncjob.Job) syncjob.Counts {
	return syncjob.Counts{Created: j.Created, Updated: j.Updated, Withdrawn: j.Withdrawn, Duplicate: j.Duplicate, Stale: j.Stale}
}

// prcAssertSameCheckpoint fails when anything that records progress differs between two reads
// of a job: position, last batch, end-of-feed flag and every counter.
func prcAssertSameCheckpoint(t *testing.T, before, after syncjob.Job) {
	t.Helper()
	lastBatch := func(j syncjob.Job) string {
		if j.LastBatchID == nil {
			return "<none>"
		}
		return *j.LastBatchID
	}
	if before.ProcessedBatches != after.ProcessedBatches || before.NextCursor != after.NextCursor ||
		lastBatch(before) != lastBatch(after) || before.EndOfFeedSeen != after.EndOfFeedSeen || prcCounts(before) != prcCounts(after) {
		t.Fatalf("checkpoint moved: %d batches, next %q, last %s, end of feed %v, %+v -> %d batches, next %q, last %s, end of feed %v, %+v",
			before.ProcessedBatches, before.NextCursor, lastBatch(before), before.EndOfFeedSeen, prcCounts(before),
			after.ProcessedBatches, after.NextCursor, lastBatch(after), after.EndOfFeedSeen, prcCounts(after))
	}
}

// prcOutsideIssues snapshots everything a rejected batch must leave alone: only the issue
// rows and the job row (status, last_error) are allowed to change.
func prcOutsideIssues(t *testing.T, pool *pgxpool.Pool) testutil.DBSnapshot {
	t.Helper()
	return testutil.SnapshotExcept(t, pool, "sync_issues", "sync_jobs")
}

// prcOnlyIssue requires exactly one issue on the job, OPEN, with this code, severity and cursor.
func prcOnlyIssue(t *testing.T, pool *pgxpool.Pool, jobID string, code syncjob.IssueCode, severity syncjob.Severity, cursor string) syncjob.Issue {
	t.Helper()
	issues, err := syncjob.ListIssues(context.Background(), pool, jobID, "", "", 50, 0)
	if err != nil {
		t.Fatalf("ListIssues: %v", err)
	}
	if len(issues) != 1 {
		t.Fatalf("issues = %+v, want exactly one", issues)
	}
	iss := issues[0]
	if iss.Code != code || iss.Severity != severity || iss.Status != "OPEN" || iss.Cursor == nil || *iss.Cursor != cursor {
		t.Fatalf("issue = %+v, want OPEN %s %s at cursor %q", iss, severity, code, cursor)
	}
	return iss
}

// prcAssertParked checks the job a rejected batch leaves behind: waiting for attention, with
// the reason shown to people, and not one step further than before.
func prcAssertParked(t *testing.T, pool *pgxpool.Pool, before syncjob.Job, iss syncjob.Issue) syncjob.Job {
	t.Helper()
	after := prcJob(t, pool, before.ID)
	if after.Status != syncjob.StatusNeedsAttention || after.OpenBlockingIssues != 1 || after.CompletedAt != nil {
		t.Fatalf("job = %s, %d open blocking, completed_at %v; want NEEDS_ATTENTION, 1, nil", after.Status, after.OpenBlockingIssues, after.CompletedAt)
	}
	if after.LastError == nil || *after.LastError != iss.Message {
		t.Fatalf("last_error = %v, want the issue message %q", after.LastError, iss.Message)
	}
	prcAssertSameCheckpoint(t, before, after)
	return after
}

type prcMarker struct {
	Seq                                                    int
	CursorIn, CursorOut, PayloadHash                       string
	HasMore                                                bool
	Records, Created, Updated, Withdrawn, Duplicate, Stale int
}

func prcReadMarker(t *testing.T, pool *pgxpool.Pool, jobID, batchID string) prcMarker {
	t.Helper()
	var m prcMarker
	err := pool.QueryRow(context.Background(),
		`SELECT seq, cursor_in, cursor_out, has_more, payload_hash, record_count,
		        created_count, updated_count, withdrawn_count, duplicate_count, stale_count
		 FROM sync_batches WHERE job_id = $1::uuid AND batch_id = $2`, jobID, batchID).
		Scan(&m.Seq, &m.CursorIn, &m.CursorOut, &m.HasMore, &m.PayloadHash, &m.Records,
			&m.Created, &m.Updated, &m.Withdrawn, &m.Duplicate, &m.Stale)
	if err != nil {
		t.Fatalf("read marker of %s: %v", batchID, err)
	}
	return m
}

func prcChangeList(t *testing.T, pool *pgxpool.Pool, jobID string) []string {
	t.Helper()
	changes, err := syncjob.ListChanges(context.Background(), pool, jobID, "", 100, 0)
	if err != nil {
		t.Fatalf("ListChanges: %v", err)
	}
	out := []string{}
	for _, c := range changes {
		out = append(out, fmt.Sprintf("%s r%d %s", c.ProductID, c.Revision, c.ChangeType))
	}
	return out
}

func prcCount(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// The batches below form one feed: batch 1 creates SKU-A and SKU-C, batch 2 updates SKU-A and
// creates SKU-B, and prcEnd closes the feed by withdrawing SKU-C.
func prcBatch1() supplier.Batch {
	return testutil.Page(prcUpdate, "BATCH-1", "cursor-2", true,
		testutil.Active("SKU-A", 1, "Alpha", 100), testutil.Active("SKU-C", 1, "Gamma", 300))
}

func prcBatch2() supplier.Batch {
	return testutil.Page(prcUpdate, "BATCH-2", "cursor-3", true,
		testutil.Active("SKU-A", 2, "Alpha", 150), testutil.Active("SKU-B", 1, "Beta", 200))
}

func prcEnd(batchID string) supplier.Batch {
	return testutil.Page(prcUpdate, batchID, "", false, testutil.Withdrawn("SKU-C", 2))
}

func TestCommitWritesCatalogChangesMarkerCountersAndCheckpointTogether(t *testing.T) {
	pool := prcPool(t)
	jobID := prcSeedJob(t, pool, "RUNNING", 0)
	p := prcProcessor(pool, syncjob.Hooks{})

	first := testutil.Page(prcUpdate, "BATCH-1", "cursor-2", true,
		testutil.Active("SKU-A", 1, "Alpha", 100),
		testutil.Active("SKU-B", 1, "Beta", 200),
		testutil.Withdrawn("SKU-Z", 4),            // never seen before: stored as a tombstone
		testutil.Active("SKU-A", 1, "Alpha", 100)) // the same record twice: the second is a duplicate
	out := prcCommit(t, p, jobID, "", first)
	if want := (syncjob.Counts{Created: 2, Withdrawn: 1, Duplicate: 1}); out.Counts != want || out.Status != syncjob.StatusRunning {
		t.Fatalf("outcome = %+v, status %s; want counts %+v and RUNNING", out.Counts, out.Status, want)
	}

	testutil.AssertProduct(t, pool, testutil.SupplierID, "SKU-A", "ACTIVE", 1)
	testutil.AssertProduct(t, pool, testutil.SupplierID, "SKU-B", "ACTIVE", 1)
	testutil.AssertProduct(t, pool, testutil.SupplierID, "SKU-Z", "WITHDRAWN", 4)
	if got, want := prcChangeList(t, pool, jobID), []string{"SKU-A r1 CREATED", "SKU-B r1 CREATED", "SKU-Z r4 WITHDRAWN"}; !slices.Equal(got, want) {
		t.Fatalf("changes = %v, want %v (the duplicate leaves no audit row)", got, want)
	}
	if m, want := prcReadMarker(t, pool, jobID, "BATCH-1"), (prcMarker{
		Seq: 1, CursorIn: "", CursorOut: "cursor-2", PayloadHash: syncjob.BatchHash(first), HasMore: true,
		Records: 4, Created: 2, Withdrawn: 1, Duplicate: 1,
	}); m != want {
		t.Fatalf("marker = %+v, want %+v", m, want)
	}
	job := prcJob(t, pool, jobID)
	if job.Status != syncjob.StatusRunning || job.ProcessedBatches != 1 || job.NextCursor != "cursor-2" || job.EndOfFeedSeen ||
		job.LastBatchID == nil || *job.LastBatchID != "BATCH-1" || prcCounts(job) != out.Counts || job.CompletedAt != nil {
		t.Fatalf("job after batch 1 = %+v", job)
	}
	testutil.AssertCounters(t, pool, jobID)

	// A second batch adds to the same totals and takes the next position in the marker order.
	second := testutil.Page(prcUpdate, "BATCH-2", "cursor-3", true,
		testutil.Active("SKU-A", 2, "Alpha", 150),
		testutil.Active("SKU-Z", 3, "Zed", 50)) // older than the tombstone at revision 4: stale
	out = prcCommit(t, p, jobID, "cursor-2", second)
	if want := (syncjob.Counts{Updated: 1, Stale: 1}); out.Counts != want {
		t.Fatalf("second outcome counts = %+v, want %+v", out.Counts, want)
	}
	job = prcJob(t, pool, jobID)
	if want := (syncjob.Counts{Created: 2, Updated: 1, Withdrawn: 1, Duplicate: 1, Stale: 1}); prcCounts(job) != want ||
		job.ProcessedBatches != 2 || job.NextCursor != "cursor-3" {
		t.Fatalf("job after batch 2 = %+v, want counts %+v at cursor-3", job, want)
	}
	if m := prcReadMarker(t, pool, jobID, "BATCH-2"); m.Seq != 2 || m.CursorIn != "cursor-2" || m.Updated != 1 || m.Stale != 1 {
		t.Fatalf("second marker = %+v", m)
	}
	testutil.AssertProduct(t, pool, testutil.SupplierID, "SKU-A", "ACTIVE", 2)
	testutil.AssertProduct(t, pool, testutil.SupplierID, "SKU-Z", "WITHDRAWN", 4)
	testutil.AssertCounters(t, pool, jobID)
}

func TestVersionConflictMidBatchRollsBackEverythingButTheIssue(t *testing.T) {
	pool := prcPool(t)
	jobID := prcSeedJob(t, pool, "RUNNING", 0)
	p := prcProcessor(pool, syncjob.Hooks{})
	prcCommit(t, p, jobID, "", prcBatch1()) // stores SKU-A r1 and SKU-C r1 at price 300
	jobBefore := prcJob(t, pool, jobID)
	rowsBefore := prcOutsideIssues(t, pool)

	// Products apply in key order: SKU-A is updated and SKU-B created before SKU-C turns out
	// to repeat its stored revision with another price. Both earlier writes must disappear.
	bad := testutil.Page(prcUpdate, "BATCH-2", "cursor-3", true,
		testutil.Active("SKU-A", 2, "Alpha", 150),
		testutil.Active("SKU-B", 1, "Beta", 200),
		testutil.Active("SKU-C", 1, "Gamma", 999))
	prcProcess(t, p, jobID, "cursor-2", bad, syncjob.KindBlocked)

	testutil.AssertUnchanged(t, rowsBefore, prcOutsideIssues(t, pool))
	iss := prcOnlyIssue(t, pool, jobID, syncjob.CodeVersionConflict, syncjob.SeverityBlocking, "cursor-2")
	if iss.BatchID == nil || *iss.BatchID != "BATCH-2" || iss.SupplierID == nil || *iss.SupplierID != testutil.SupplierID ||
		iss.ProductID == nil || *iss.ProductID != "SKU-C" || iss.RetryCount != 0 || !strings.Contains(iss.Message, "SKU-C") {
		t.Fatalf("issue = %+v, want it to name BATCH-2 and product SKU-C", iss)
	}
	prcAssertParked(t, pool, jobBefore, iss)
	testutil.AssertProduct(t, pool, testutil.SupplierID, "SKU-A", "ACTIVE", 1)
	if n := prcCount(t, pool, `SELECT count(*) FROM products WHERE product_id = 'SKU-B'`); n != 0 {
		t.Fatalf("SKU-B rows = %d, want 0: it was created in the rolled-back transaction", n)
	}
}

func TestCrashBeforeCommitRollsBackEveryWriteOfTheBatch(t *testing.T) {
	pool := prcPool(t)
	jobID := prcSeedJob(t, pool, "RUNNING", 0)
	crashing := prcProcessor(pool, syncjob.Hooks{BeforeCommit: func(_ context.Context, _ pgx.Tx, batchID string) error {
		if batchID == "BATCH-2" {
			return syncjob.ErrSimulatedCrash
		}
		return nil
	}})
	prcCommit(t, crashing, jobID, "", prcBatch1())
	// An outage note that the next successful commit would close: the rolled-back batch must
	// not have closed it either.
	prcSeedIssue(t, pool, jobID, syncjob.CodeSupplierUnavailable, syncjob.SeverityTransient, "cursor-2")
	before := testutil.Snapshot(t, pool)

	_, err := crashing.ProcessBatch(context.Background(), jobID, "cursor-2", prcBatch2())
	if !errors.Is(err, syncjob.ErrSimulatedCrash) {
		t.Fatalf("ProcessBatch error = %v, want ErrSimulatedCrash", err)
	}
	testutil.AssertUnchanged(t, before, testutil.Snapshot(t, pool))
	if job := prcJob(t, pool, jobID); job.Status != syncjob.StatusRunning || job.ProcessedBatches != 1 {
		t.Fatalf("job = %s with %d batches, want it RUNNING exactly as a dead process leaves it", job.Status, job.ProcessedBatches)
	}
	prcOnlyIssue(t, pool, jobID, syncjob.CodeSupplierUnavailable, syncjob.SeverityTransient, "cursor-2")

	// Nothing of the aborted attempt is in the way: the same batch commits once the fault is gone.
	prcCommit(t, prcProcessor(pool, syncjob.Hooks{}), jobID, "cursor-2", prcBatch2())
	if n := prcCount(t, pool, `SELECT count(*) FROM sync_issues WHERE status = 'OPEN'`); n != 0 {
		t.Fatalf("open issues after the retry = %d, want the outage note closed by the commit", n)
	}
	testutil.AssertCounters(t, pool, jobID)
}

func TestAfterCommitCrashThenSameBatchAgainIsReplayedWithoutChanges(t *testing.T) {
	pool := prcPool(t)
	jobID := prcSeedJob(t, pool, "RUNNING", 0)
	crashing := prcProcessor(pool, syncjob.Hooks{AfterCommit: func(batchID string) error {
		if batchID == "BATCH-1" {
			return syncjob.ErrSimulatedCrash
		}
		return nil
	}})

	out, err := crashing.ProcessBatch(context.Background(), jobID, "", prcBatch1())
	if !errors.Is(err, syncjob.ErrSimulatedCrash) || out.Kind != syncjob.KindCommitted {
		t.Fatalf("ProcessBatch = %d, %v; want the batch committed and then the crash", out.Kind, err)
	}
	committed := testutil.Snapshot(t, pool)
	counted := prcJob(t, pool, jobID)

	// The caller never learned of the commit, so it presents the same batch again.
	prcProcess(t, prcProcessor(pool, syncjob.Hooks{}), jobID, "", prcBatch1(), syncjob.KindReplayed)
	testutil.AssertUnchanged(t, committed, testutil.Snapshot(t, pool))
	prcAssertSameCheckpoint(t, counted, prcJob(t, pool, jobID))

	// Progress continues from the saved cursor as if nothing had happened.
	prcCommit(t, prcProcessor(pool, syncjob.Hooks{}), jobID, "cursor-2", prcBatch2())
	testutil.AssertCounters(t, pool, jobID)
	if job := prcJob(t, pool, jobID); job.ProcessedBatches != 2 || job.Created != 3 || job.Updated != 1 {
		t.Fatalf("job = %d batches, %+v; want 2 batches with created 3, updated 1", job.ProcessedBatches, prcCounts(job))
	}
}

func TestSameBatchIDWithDifferentContentIsABatchConflict(t *testing.T) {
	pool := prcPool(t)
	jobID := prcSeedJob(t, pool, "RUNNING", 0)
	p := prcProcessor(pool, syncjob.Hooks{})
	prcCommit(t, p, jobID, "", prcBatch1())
	jobBefore := prcJob(t, pool, jobID)
	rowsBefore := prcOutsideIssues(t, pool)

	// The supplier serves the next page under the id BATCH-1 again, with other products.
	reused := testutil.Page(prcUpdate, "BATCH-1", "cursor-3", true, testutil.Active("SKU-A", 2, "Alpha", 999))
	prcProcess(t, p, jobID, "cursor-2", reused, syncjob.KindBlocked)

	testutil.AssertUnchanged(t, rowsBefore, prcOutsideIssues(t, pool))
	iss := prcOnlyIssue(t, pool, jobID, syncjob.CodeBatchConflict, syncjob.SeverityBlocking, "cursor-2")
	prcAssertParked(t, pool, jobBefore, iss)
	testutil.AssertProduct(t, pool, testutil.SupplierID, "SKU-A", "ACTIVE", 1)
}

func TestIdenticalBatchServedAtANewCursorIsRejected(t *testing.T) {
	pool := prcPool(t)
	jobID := prcSeedJob(t, pool, "RUNNING", 0)
	p := prcProcessor(pool, syncjob.Hooks{})
	prcCommit(t, p, jobID, "", prcBatch1())
	prcCommit(t, p, jobID, "cursor-2", prcBatch2())
	jobBefore := prcJob(t, pool, jobID)
	rowsBefore := prcOutsideIssues(t, pool)

	// Batch 1 comes back unchanged at cursor-3, pointing back at cursor-2: following it would
	// walk the same two batches forever.
	prcProcess(t, p, jobID, "cursor-3", prcBatch1(), syncjob.KindBlocked)

	testutil.AssertUnchanged(t, rowsBefore, prcOutsideIssues(t, pool))
	iss := prcOnlyIssue(t, pool, jobID, syncjob.CodeInvalidCursor, syncjob.SeverityBlocking, "cursor-3")
	if !strings.Contains(iss.Message, "served again") {
		t.Fatalf("message = %q, want it to say the batch was served again", iss.Message)
	}
	prcAssertParked(t, pool, jobBefore, iss)
}

func TestEndOfFeedAfterTheWrongNumberOfBatchesIsInvalid(t *testing.T) {
	pool := prcPool(t)
	jobID := prcSeedJob(t, pool, "RUNNING", 3)
	p := prcProcessor(pool, syncjob.Hooks{})
	prcCommit(t, p, jobID, "", prcBatch1())
	jobBefore := prcJob(t, pool, jobID)
	rowsBefore := prcOutsideIssues(t, pool)

	// The supplier announced three batches and ends the feed after two.
	prcProcess(t, p, jobID, "cursor-2", prcEnd("BATCH-2"), syncjob.KindBlocked)

	testutil.AssertUnchanged(t, rowsBefore, prcOutsideIssues(t, pool))
	iss := prcOnlyIssue(t, pool, jobID, syncjob.CodeInvalidBatch, syncjob.SeverityBlocking, "cursor-2")
	after := prcAssertParked(t, pool, jobBefore, iss)
	if after.Status == syncjob.StatusCompleted || after.EndOfFeedSeen {
		t.Fatalf("job = %s, end of feed %v: a short feed must never count as finished", after.Status, after.EndOfFeedSeen)
	}
	testutil.AssertProduct(t, pool, testutil.SupplierID, "SKU-C", "ACTIVE", 1) // the withdrawal in the rejected batch was not applied
}

func TestMoreBatchesPastTheAnnouncedCountAreRejectedBeforeAnyCatalogWrite(t *testing.T) {
	pool := prcPool(t)
	jobID := prcSeedJob(t, pool, "RUNNING", 3)
	p := prcProcessor(pool, syncjob.Hooks{})
	prcCommit(t, p, jobID, "", prcBatch1())
	prcCommit(t, p, jobID, "cursor-2", prcBatch2())
	jobBefore := prcJob(t, pool, jobID)
	rowsBefore := prcOutsideIssues(t, pool)

	// Batch 3 of 3 claims that another batch follows.
	overrun := testutil.Page(prcUpdate, "BATCH-3", "cursor-4", true, testutil.Active("SKU-D", 1, "Delta", 400))
	prcProcess(t, p, jobID, "cursor-3", overrun, syncjob.KindBlocked)

	testutil.AssertUnchanged(t, rowsBefore, prcOutsideIssues(t, pool))
	iss := prcOnlyIssue(t, pool, jobID, syncjob.CodeInvalidBatch, syncjob.SeverityBlocking, "cursor-3")
	prcAssertParked(t, pool, jobBefore, iss)
	if n := prcCount(t, pool, `SELECT count(*) FROM products WHERE product_id = 'SKU-D'`); n != 0 {
		t.Fatalf("SKU-D rows = %d, want 0", n)
	}
}

func TestConnectionLossMidBatchRollsBack(t *testing.T) {
	pool := prcPool(t)
	jobID := prcSeedJob(t, pool, "RUNNING", 0)
	var hookErr error
	var terminated bool
	killing := prcProcessor(pool, syncjob.Hooks{BeforeCommit: func(ctx context.Context, tx pgx.Tx, batchID string) error {
		if batchID != "BATCH-2" {
			return nil
		}
		// The last statement before COMMIT: another connection ends this transaction's
		// backend, and the call returns only once that backend is gone.
		var pid int
		if hookErr = tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); hookErr != nil {
			return hookErr
		}
		if hookErr = pool.QueryRow(ctx, `SELECT pg_terminate_backend($1::int, 5000)`, pid).Scan(&terminated); hookErr != nil {
			return hookErr
		}
		return nil
	}})
	prcCommit(t, killing, jobID, "", prcBatch1())
	prcSeedIssue(t, pool, jobID, syncjob.CodeSupplierUnavailable, syncjob.SeverityTransient, "cursor-2")
	before := testutil.Snapshot(t, pool)

	_, err := killing.ProcessBatch(context.Background(), jobID, "cursor-2", prcBatch2())
	if hookErr != nil || !terminated {
		t.Fatalf("could not end the backend: terminated %v, error %v", terminated, hookErr)
	}
	if err == nil || !storage.IsConnectionError(err) {
		t.Fatalf("ProcessBatch error = %v, want a connection-class error the worker treats as transient", err)
	}
	testutil.AssertUnchanged(t, before, testutil.Snapshot(t, pool)) // marker, changes, issue resolution and checkpoint all rolled back

	// A fresh connection takes the same batch without trouble.
	prcCommit(t, prcProcessor(pool, syncjob.Hooks{}), jobID, "cursor-2", prcBatch2())
	if job := prcJob(t, pool, jobID); job.ProcessedBatches != 2 || job.Created != 3 || job.Updated != 1 {
		t.Fatalf("job = %d batches, %+v; want 2 batches with created 3, updated 1", job.ProcessedBatches, prcCounts(job))
	}
	testutil.AssertCounters(t, pool, jobID)
}

func TestEndOfFeedWithNoBlockingIssueCompletesTheJob(t *testing.T) {
	tests := []struct {
		name  string
		total int
		last  supplier.Batch
		want  syncjob.Counts
	}{
		{"announced_count_matches", 2, prcEnd("BATCH-2"), syncjob.Counts{Created: 2, Withdrawn: 1}},
		{"count_not_announced", 0, prcEnd("BATCH-2"), syncjob.Counts{Created: 2, Withdrawn: 1}},
		{"last_batch_without_products", 0, testutil.Page(prcUpdate, "BATCH-2", "", false), syncjob.Counts{Created: 2}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool := prcPool(t)
			jobID := prcSeedJob(t, pool, "RUNNING", tc.total)
			p := prcProcessor(pool, syncjob.Hooks{})
			prcCommit(t, p, jobID, "", prcBatch1())

			out := prcCommit(t, p, jobID, "cursor-2", tc.last)
			if out.Status != syncjob.StatusCompleted {
				t.Fatalf("outcome status = %s, want COMPLETED", out.Status)
			}
			job := prcJob(t, pool, jobID)
			if job.Status != syncjob.StatusCompleted || !job.EndOfFeedSeen || job.CompletedAt == nil || job.LastError != nil ||
				job.NextCursor != "" || job.ProcessedBatches != 2 || prcCounts(job) != tc.want {
				t.Fatalf("job = %+v, want COMPLETED after 2 batches with %+v", job, tc.want)
			}
			testutil.AssertCounters(t, pool, jobID)
			testutil.AssertCompletedConsistent(t, pool, jobID)
		})
	}
}

func TestBatchForAJobThatMovedOnIsStaleAndWritesNothing(t *testing.T) {
	// seed brings a job to "batch 1 committed, waiting for cursor-2", then bends it as needed.
	seed := func(t *testing.T, total int) (*pgxpool.Pool, *syncjob.Processor, string) {
		pool := prcPool(t)
		jobID := prcSeedJob(t, pool, "RUNNING", total)
		p := prcProcessor(pool, syncjob.Hooks{})
		prcCommit(t, p, jobID, "", prcBatch1())
		return pool, p, jobID
	}
	tests := []struct {
		name  string
		total int
		setup func(t *testing.T, pool *pgxpool.Pool, p *syncjob.Processor, jobID string)
		// cursor and batch are what the late caller presents.
		cursor string
		batch  func() supplier.Batch
	}{
		{
			name:   "cursor_behind_the_checkpoint",
			cursor: "",
			batch: func() supplier.Batch {
				return testutil.Page(prcUpdate, "BATCH-X", "cursor-9", true, testutil.Active("SKU-X", 1, "Xi", 10))
			},
		},
		{
			name:   "cursor_ahead_of_the_checkpoint",
			cursor: "cursor-7",
			batch: func() supplier.Batch {
				return testutil.Page(prcUpdate, "BATCH-X", "cursor-9", true, testutil.Active("SKU-X", 1, "Xi", 10))
			},
		},
		{
			name: "job_was_interrupted",
			setup: func(t *testing.T, pool *pgxpool.Pool, _ *syncjob.Processor, jobID string) {
				if _, err := pool.Exec(context.Background(), `UPDATE sync_jobs SET status = 'INTERRUPTED' WHERE id = $1::uuid`, jobID); err != nil {
					t.Fatal(err)
				}
			},
			cursor: "cursor-2",
			batch:  prcBatch2,
		},
		{
			// A broken envelope on a job that is not RUNNING must not park or touch it.
			name: "broken_batch_for_a_job_that_was_interrupted",
			setup: func(t *testing.T, pool *pgxpool.Pool, _ *syncjob.Processor, jobID string) {
				if _, err := pool.Exec(context.Background(), `UPDATE sync_jobs SET status = 'INTERRUPTED' WHERE id = $1::uuid`, jobID); err != nil {
					t.Fatal(err)
				}
			},
			cursor: "cursor-2",
			batch: func() supplier.Batch {
				b := prcBatch2()
				b.HasMore = nil
				return b
			},
		},
		{
			name:  "last_batch_presented_again_after_completion",
			total: 2,
			setup: func(t *testing.T, _ *pgxpool.Pool, p *syncjob.Processor, jobID string) {
				prcCommit(t, p, jobID, "cursor-2", prcEnd("BATCH-2"))
			},
			cursor: "cursor-2",
			batch:  func() supplier.Batch { return prcEnd("BATCH-2") },
		},
		{
			// The last batch stores an empty cursor, which a late first-page fetch would match.
			name: "end_of_feed_already_seen",
			setup: func(t *testing.T, pool *pgxpool.Pool, _ *syncjob.Processor, jobID string) {
				_, err := pool.Exec(context.Background(), `UPDATE sync_jobs SET end_of_feed_seen = true, next_cursor = '' WHERE id = $1::uuid`, jobID)
				if err != nil {
					t.Fatal(err)
				}
			},
			cursor: "",
			batch: func() supplier.Batch {
				return testutil.Page(prcUpdate, "BATCH-X", "", false, testutil.Active("SKU-X", 1, "Xi", 10))
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool, p, jobID := seed(t, tc.total)
			if tc.setup != nil {
				tc.setup(t, pool, p, jobID)
			}
			before := testutil.Snapshot(t, pool)
			prcProcess(t, p, jobID, tc.cursor, tc.batch(), syncjob.KindStale)
			testutil.AssertUnchanged(t, before, testutil.Snapshot(t, pool)) // includes sync_issues: no issue was recorded either
		})
	}
}

func TestOpenBlockingIssuePreventsCompletion(t *testing.T) {
	pool := prcPool(t)
	jobID := prcSeedJob(t, pool, "RUNNING", 2)
	p := prcProcessor(pool, syncjob.Hooks{})
	prcCommit(t, p, jobID, "", prcBatch1())
	// A blocking problem raised at a cursor other than the one about to commit stays open.
	prcSeedIssue(t, pool, jobID, syncjob.CodeInvalidProductData, syncjob.SeverityBlocking, "cursor-9")

	out := prcCommit(t, p, jobID, "cursor-2", prcEnd("BATCH-2"))
	if out.Status != syncjob.StatusNeedsAttention {
		t.Fatalf("status after the last batch = %s, want NEEDS_ATTENTION", out.Status)
	}
	job := prcJob(t, pool, jobID)
	if job.Status != syncjob.StatusNeedsAttention || !job.EndOfFeedSeen || job.ProcessedBatches != 2 || job.CompletedAt != nil ||
		job.LastError == nil || !strings.Contains(*job.LastError, "1 blocking issue") {
		t.Fatalf("job = %+v; want the last batch committed but the job NEEDS_ATTENTION, not completed", job)
	}

	// The database refuses the shortcut as well, whatever the application does.
	_, err := pool.Exec(context.Background(), `UPDATE sync_jobs SET status = 'COMPLETED', completed_at = now() WHERE id = $1::uuid`, jobID)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("direct UPDATE to COMPLETED error = %v, want SQLSTATE 23514 from the completion guard", err)
	}

	// Resuming runs only the completion check; it must keep the job waiting and never ask the
	// supplier anything (this one serves nothing).
	sup := testutil.NewFakeSupplier()
	m := syncjob.NewManager(pool, sup, syncjob.Config{RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond},
		syncjob.Hooks{}, slog.New(slog.DiscardHandler))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := m.Shutdown(ctx); err != nil {
			t.Errorf("manager shutdown: %v", err)
		}
	})
	resume := func() {
		t.Helper()
		if _, err := m.Resume(context.Background(), jobID); err != nil {
			t.Fatalf("Resume: %v", err)
		}
		m.Wait()
	}
	resume()
	if job := prcJob(t, pool, jobID); job.Status != syncjob.StatusNeedsAttention || job.OpenBlockingIssues != 1 || job.CompletedAt != nil {
		t.Fatalf("after resume with the issue open: %s, %d open blocking; want it kept waiting", job.Status, job.OpenBlockingIssues)
	}

	if _, err := pool.Exec(context.Background(), `UPDATE sync_issues SET status = 'RESOLVED', resolved_at = now() WHERE job_id = $1::uuid`, jobID); err != nil {
		t.Fatal(err)
	}
	resume()
	if job := prcJob(t, pool, jobID); job.Status != syncjob.StatusCompleted || job.LastError != nil {
		t.Fatalf("after resume with nothing open: %s, last_error %v; want COMPLETED", job.Status, job.LastError)
	}
	testutil.AssertCounters(t, pool, jobID)
	testutil.AssertCompletedConsistent(t, pool, jobID)
	if calls := sup.Calls(); len(calls) != 0 {
		t.Fatalf("supplier was called %v; a job past end of feed must never ask it anything", calls)
	}
}

func TestDatabaseRefusalInsideTheBatchBlocksTheJobInsteadOfRetrying(t *testing.T) {
	pool := prcPool(t)
	jobID := prcSeedJob(t, pool, "RUNNING", 0)
	violating := prcProcessor(pool, syncjob.Hooks{BeforeCommit: func(ctx context.Context, tx pgx.Tx, batchID string) error {
		if batchID != "BATCH-2" {
			return nil
		}
		// Rewriting a product without raising its revision is exactly what the guard trigger forbids.
		_, err := tx.Exec(ctx, `UPDATE products SET revision = revision WHERE supplier_id = 'SUP-001' AND product_id = 'SKU-A'`)
		return err
	}})
	prcCommit(t, violating, jobID, "", prcBatch1())
	jobBefore := prcJob(t, pool, jobID)
	rowsBefore := prcOutsideIssues(t, pool)

	prcProcess(t, violating, jobID, "cursor-2", prcBatch2(), syncjob.KindBlocked)

	testutil.AssertUnchanged(t, rowsBefore, prcOutsideIssues(t, pool))
	iss := prcOnlyIssue(t, pool, jobID, syncjob.CodeDatabaseError, syncjob.SeverityBlocking, "cursor-2")
	if !strings.Contains(iss.Message, "revision must increase") {
		t.Fatalf("message = %q, want the database's own reason", iss.Message)
	}
	prcAssertParked(t, pool, jobBefore, iss)
}
