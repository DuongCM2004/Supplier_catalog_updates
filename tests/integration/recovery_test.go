package integration

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/catalog"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/syncjob"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/testutil"
)

// Scenarios where a job stops, is parked for review or is cut short, and what a resume does
// about it. They use the flowEnv harness from catalog_flow_test.go.

// recovFeed is a four-batch update with two products per batch.
func recovFeed() [][]catalog.SupplierProduct {
	return [][]catalog.SupplierProduct{
		flowRecords(testutil.Active("SKU-1", 1, "One", 100), testutil.Active("SKU-2", 1, "Two", 200)),
		flowRecords(testutil.Active("SKU-3", 1, "Three", 300), testutil.Active("SKU-4", 1, "Four", 400)),
		flowRecords(testutil.Active("SKU-5", 1, "Five", 500), testutil.Active("SKU-6", 1, "Six", 600)),
		flowRecords(testutil.Active("SKU-7", 1, "Seven", 700), testutil.Active("SKU-8", 1, "Eight", 800)),
	}
}

// recovFeedCounts is what the whole four-batch update does when nothing goes wrong, which is
// also what it must add up to after any outage or restart.
var recovFeedCounts = syncjob.Counts{Created: 8}

// recovOnlyIssue returns the single issue a job is expected to have.
func recovOnlyIssue(t *testing.T, e *flowEnv, jobID string) syncjob.Issue {
	t.Helper()
	issues := e.issues(jobID)
	if len(issues) != 1 {
		t.Fatalf("issues = %+v, want exactly one", issues)
	}
	return issues[0]
}

func TestEqualRevisionDifferentPayloadBlocksBatch(t *testing.T) {
	e := flowNew(t)
	e.serve("U1", flowRecords(testutil.Active("SKU-A", 2, "Alpha", 100)))
	e.expect(e.run("U1"), syncjob.StatusCompleted, 1, syncjob.Counts{Created: 1})
	before := testutil.SnapshotExcept(t, e.pool, "sync_jobs", "sync_issues")

	e.serve("U2", flowRecords(testutil.Active("SKU-A", 2, "Alpha", 200)))
	job := e.run("U2")

	e.expect(job, syncjob.StatusNeedsAttention, 0, syncjob.Counts{})
	if job.NextCursor != "" || job.OpenBlockingIssues != 1 {
		t.Errorf("job next cursor %q, open blocking %d; want the checkpoint still at the first page and 1 open blocking issue", job.NextCursor, job.OpenBlockingIssues)
	}
	iss := recovOnlyIssue(t, e, job.ID)
	if iss.Code != syncjob.CodeVersionConflict || iss.Severity != syncjob.SeverityBlocking || iss.Status != "OPEN" ||
		iss.ProductID == nil || *iss.ProductID != "SKU-A" {
		t.Errorf("issue = %+v, want OPEN BLOCKING VERSION_CONFLICT for SKU-A", iss)
	}
	testutil.AssertProduct(t, e.pool, testutil.SupplierID, "SKU-A", catalog.StatusActive, 2)
	e.assertPrice("SKU-A", 100)
	// Nothing but the job and its issue changed: not the catalog, not the batches, not the audit trail.
	testutil.AssertUnchanged(t, before, testutil.SnapshotExcept(t, e.pool, "sync_jobs", "sync_issues"))
}

// recovServeInvalidRecord serves a three-batch update whose second batch has a negative price
// next to a valid record.
func recovServeInvalidRecord(e *flowEnv) {
	e.t.Helper()
	e.serve("U1",
		flowRecords(testutil.Active("SKU-A", 1, "Alpha", 1000)),
		flowRecords(testutil.Active("SKU-B", 1, "Beta", 2000), testutil.Active("SKU-C", 1, "Gamma", -5)),
		flowRecords(testutil.Active("SKU-D", 1, "Delta", 400)))
}

// recovCorrectSecondBatch replaces the bad page with a valid one, as a supplier fixing its data would.
func recovCorrectSecondBatch(e *flowEnv) {
	e.sup.AddPage(flowCursor(2), testutil.Page("U1", flowBatchID(2), flowCursor(3), true,
		testutil.Active("SKU-B", 1, "Beta", 2000), testutil.Active("SKU-C", 1, "Gamma", 500)))
}

func TestInvalidRecordLeavesBatchUncommitted(t *testing.T) {
	e := flowNew(t)
	recovServeInvalidRecord(e)
	job := e.run("U1")

	e.expect(job, syncjob.StatusNeedsAttention, 1, syncjob.Counts{Created: 1})
	if job.NextCursor != flowCursor(2) || job.LastBatchID == nil || *job.LastBatchID != flowBatchID(1) || job.EndOfFeedSeen || job.OpenBlockingIssues != 1 {
		t.Errorf("job next cursor %q, last batch %v, end of feed %v, open blocking %d; want the checkpoint at the end of the first batch",
			job.NextCursor, job.LastBatchID, job.EndOfFeedSeen, job.OpenBlockingIssues)
	}
	iss := recovOnlyIssue(t, e, job.ID)
	if iss.Code != syncjob.CodeInvalidProductData || iss.Severity != syncjob.SeverityBlocking || iss.Status != "OPEN" ||
		iss.Cursor == nil || *iss.Cursor != flowCursor(2) || !strings.Contains(iss.Message, "SKU-C: price_minor must be >= 0") {
		t.Errorf("issue = %+v, want OPEN BLOCKING INVALID_PRODUCT_DATA at %s naming SKU-C and its price", iss, flowCursor(2))
	}

	// Only the first batch is in the database: the valid SKU-B that shares a batch with the
	// bad record was rolled back with it, and the batch after it was never fetched.
	testutil.AssertProduct(t, e.pool, testutil.SupplierID, "SKU-A", catalog.StatusActive, 1)
	for _, id := range []string{"SKU-B", "SKU-C", "SKU-D"} {
		e.assertAbsent(id)
	}
	if got, want := e.batchMarkers(job.ID), []string{"1:BATCH-1"}; !slices.Equal(got, want) {
		t.Errorf("batch markers = %v, want %v", got, want)
	}
	if changes := e.changes(job.ID, ""); len(changes) != 1 || changes[0].ProductID != "SKU-A" {
		t.Errorf("changes = %+v, want only the creation of SKU-A", changes)
	}
	if n := e.sup.FetchCalls(flowCursor(3)); n != 0 {
		t.Errorf("fetches at %s = %d, want 0: a parked job must not read past the bad batch", flowCursor(3), n)
	}
}

func TestNeedsAttentionResumeWithoutCorrectionKeepsTheSameIssue(t *testing.T) {
	e := flowNew(t)
	recovServeInvalidRecord(e)
	job := e.run("U1")
	e.expect(job, syncjob.StatusNeedsAttention, 1, syncjob.Counts{Created: 1})
	first := recovOnlyIssue(t, e, job.ID)
	if first.RetryCount != 0 {
		t.Fatalf("retry_count after the first failure = %d, want 0", first.RetryCount)
	}
	stored := testutil.SnapshotExcept(t, e.pool, "sync_jobs", "sync_issues")

	job = e.resume(job.ID)

	e.expect(job, syncjob.StatusNeedsAttention, 1, syncjob.Counts{Created: 1})
	again := recovOnlyIssue(t, e, job.ID)
	if again.ID != first.ID || again.Status != "OPEN" || again.RetryCount != first.RetryCount+1 {
		t.Errorf("issue after resume = %+v, want the same OPEN issue %s with retry_count %d", again, first.ID, first.RetryCount+1)
	}
	if n := e.sup.FetchCalls(flowCursor(2)); n != 2 {
		t.Errorf("fetches at %s = %d, want 2: the resume must look at the bad page again", flowCursor(2), n)
	}
	testutil.AssertUnchanged(t, stored, testutil.SnapshotExcept(t, e.pool, "sync_jobs", "sync_issues"))
}

func TestNeedsAttentionResumeAfterCorrectionResolvesTheIssue(t *testing.T) {
	e := flowNew(t)
	recovServeInvalidRecord(e)
	job := e.run("U1")
	e.expect(job, syncjob.StatusNeedsAttention, 1, syncjob.Counts{Created: 1})
	parked := recovOnlyIssue(t, e, job.ID)

	recovCorrectSecondBatch(e)
	job = e.resume(job.ID)

	e.expect(job, syncjob.StatusCompleted, 3, syncjob.Counts{Created: 4})
	iss := recovOnlyIssue(t, e, job.ID)
	if iss.ID != parked.ID || iss.Status != "RESOLVED" || iss.ResolvedAt == nil {
		t.Errorf("issue = %+v, want the parked issue %s RESOLVED with a resolution time", iss, parked.ID)
	}
	for _, id := range []string{"SKU-A", "SKU-B", "SKU-C", "SKU-D"} {
		testutil.AssertProduct(t, e.pool, testutil.SupplierID, id, catalog.StatusActive, 1)
	}
	e.assertPrice("SKU-C", 500)
	if n := e.sup.FetchCalls(flowCursor(1)); n != 1 {
		t.Errorf("fetches at the first page = %d, want 1: the committed batch must not be fetched again", n)
	}
}

func TestSupplierOutageInterruptsAndResumeCompletes(t *testing.T) {
	e := flowNew(t)
	cfg := flowConfig()
	e.serve("U1", recovFeed()...)
	e.sup.SetUpdate(testutil.Info("U1", 0)) // the supplier does not say how many batches to expect
	e.sup.FailTimes(flowCursor(3), -1)

	job := e.run("U1")

	e.expect(job, syncjob.StatusInterrupted, 2, syncjob.Counts{Created: 4})
	if job.NextCursor != flowCursor(3) || job.TotalBatches != nil || job.EndOfFeedSeen || job.OpenBlockingIssues != 0 {
		t.Errorf("job next cursor %q, total %v, end of feed %v, open blocking %d; want the checkpoint at %s, no announced total and nothing blocking",
			job.NextCursor, job.TotalBatches, job.EndOfFeedSeen, job.OpenBlockingIssues, flowCursor(3))
	}
	if n := e.sup.FetchCalls(flowCursor(3)); n != cfg.RetryMaxAttempts {
		t.Errorf("fetches at %s = %d, want %d: every attempt and no more", flowCursor(3), n, cfg.RetryMaxAttempts)
	}
	iss := recovOnlyIssue(t, e, job.ID)
	if iss.Code != syncjob.CodeSupplierUnavailable || iss.Severity != syncjob.SeverityTransient || iss.Status != "OPEN" ||
		iss.Cursor == nil || *iss.Cursor != flowCursor(3) || iss.RetryCount != cfg.RetryMaxAttempts {
		t.Errorf("issue = %+v, want OPEN TRANSIENT SUPPLIER_UNAVAILABLE at %s with retry_count %d", iss, flowCursor(3), cfg.RetryMaxAttempts)
	}
	// What was committed before the outage stays; nothing from the unreachable batch exists.
	for _, id := range []string{"SKU-1", "SKU-2", "SKU-3", "SKU-4"} {
		testutil.AssertProduct(t, e.pool, testutil.SupplierID, id, catalog.StatusActive, 1)
	}
	e.assertAbsent("SKU-5")

	e.sup.FailTimes(flowCursor(3), 0) // the supplier is back
	job = e.resume(job.ID)

	e.expect(job, syncjob.StatusCompleted, 4, recovFeedCounts)
	if resolved := recovOnlyIssue(t, e, job.ID); resolved.ID != iss.ID || resolved.Status != "RESOLVED" {
		t.Errorf("issue after resume = %+v, want the outage issue %s RESOLVED", resolved, iss.ID)
	}
	for n := 1; n <= 2; n++ {
		if got := e.sup.FetchCalls(flowCursor(n)); got != 1 {
			t.Errorf("fetches at %s = %d, want 1: committed batches are not fetched again", flowCursor(n), got)
		}
	}
}

func TestRestartedManagerResumesFromCheckpoint(t *testing.T) {
	// The first process dies right after committing the second batch, before it can note anything.
	crash := syncjob.Hooks{AfterCommit: func(batchID string) error {
		if batchID == flowBatchID(2) {
			return syncjob.ErrSimulatedCrash
		}
		return nil
	}}
	e := flowNewWith(t, flowConfig(), crash, nil)
	e.serve("U1", recovFeed()...)
	crashed := e.run("U1")
	if crashed.Status != syncjob.StatusRunning || crashed.ProcessedBatches != 2 || crashed.NextCursor != flowCursor(3) {
		t.Fatalf("after the crash: %s, %d batches, next %q; want RUNNING (nobody noticed) with 2 batches committed",
			crashed.Status, crashed.ProcessedBatches, crashed.NextCursor)
	}

	// A new process on the same database finds the stranded job and interrupts it; nothing more.
	e.m = flowManager(t, e.pool, e.sup, flowConfig(), syncjob.Hooks{}, nil)
	ids, err := e.m.Reconcile(context.Background())
	if err != nil || !slices.Equal(ids, []string{crashed.ID}) {
		t.Fatalf("Reconcile = %v, %v; want [%s]", ids, err, crashed.ID)
	}
	if job := e.job(crashed.ID); job.Status != syncjob.StatusInterrupted || job.LastError == nil || job.ProcessedBatches != 2 {
		t.Fatalf("after Reconcile: %s, last_error %v, %d batches; want INTERRUPTED with a reason and progress kept", job.Status, job.LastError, job.ProcessedBatches)
	}

	callsBefore := len(e.sup.Calls())
	job := e.resume(crashed.ID)

	e.expect(job, syncjob.StatusCompleted, 4, recovFeedCounts)
	var fetched []string
	for _, c := range e.sup.Calls()[callsBefore:] {
		if c.Op == testutil.OpFetchBatch {
			fetched = append(fetched, c.Cursor)
		}
	}
	if want := []string{flowCursor(3), flowCursor(4)}; !slices.Equal(fetched, want) {
		t.Errorf("fetches after the restart = %v, want %v: the restart must continue at the saved cursor, not start over", fetched, want)
	}
	for n := 1; n <= 4; n++ {
		if got := e.sup.FetchCalls(flowCursor(n)); got != 1 {
			t.Errorf("fetches at %s over both processes = %d, want 1", flowCursor(n), got)
		}
	}
}

func TestMissingEndOfFeedNeverCompletes(t *testing.T) {
	t.Run("missing_has_more", func(t *testing.T) {
		e := flowNew(t)
		e.serve("U1", flowRecords(testutil.Active("SKU-A", 1, "Alpha", 100)), flowRecords(testutil.Active("SKU-B", 1, "Beta", 200)))
		last := testutil.Page("U1", flowBatchID(2), "", false, testutil.Active("SKU-B", 1, "Beta", 200))
		last.HasMore = nil // the last page does not say whether the feed ends
		e.sup.AddPage(flowCursor(2), last)

		job := e.run("U1")

		e.expect(job, syncjob.StatusNeedsAttention, 1, syncjob.Counts{Created: 1})
		if job.EndOfFeedSeen || job.NextCursor != flowCursor(2) {
			t.Errorf("job end of feed %v, next cursor %q; want the end of the feed not assumed and the checkpoint at %s", job.EndOfFeedSeen, job.NextCursor, flowCursor(2))
		}
		if iss := recovOnlyIssue(t, e, job.ID); iss.Code != syncjob.CodeInvalidBatch || iss.Severity != syncjob.SeverityBlocking || iss.Status != "OPEN" {
			t.Errorf("issue = %+v, want OPEN BLOCKING INVALID_BATCH", iss)
		}
		e.assertAbsent("SKU-B")
	})

	t.Run("dangling_cursor", func(t *testing.T) {
		e := flowNew(t)
		e.serve("U1", flowRecords(testutil.Active("SKU-A", 1, "Alpha", 100)), flowRecords(testutil.Active("SKU-B", 1, "Beta", 200)))
		e.sup.NotFound(flowCursor(2)) // the first page promises a next one that does not exist

		job := e.run("U1")

		e.expect(job, syncjob.StatusNeedsAttention, 1, syncjob.Counts{Created: 1})
		if job.EndOfFeedSeen || job.NextCursor != flowCursor(2) {
			t.Errorf("job end of feed %v, next cursor %q; want the end of the feed not assumed and the checkpoint at %s", job.EndOfFeedSeen, job.NextCursor, flowCursor(2))
		}
		if iss := recovOnlyIssue(t, e, job.ID); iss.Code != syncjob.CodeInvalidCursor || iss.Severity != syncjob.SeverityBlocking || iss.Status != "OPEN" {
			t.Errorf("issue = %+v, want OPEN BLOCKING INVALID_CURSOR", iss)
		}
	})

	t.Run("supplier_silent", func(t *testing.T) {
		e := flowNew(t)
		e.serve("U1", flowRecords(testutil.Active("SKU-A", 1, "Alpha", 100)), flowRecords(testutil.Active("SKU-B", 1, "Beta", 200)))
		e.sup.FailTimes(flowCursor(2), -1) // the supplier stops answering before the last page

		job := e.run("U1")
		e.expect(job, syncjob.StatusInterrupted, 1, syncjob.Counts{Created: 1})
		if iss := recovOnlyIssue(t, e, job.ID); iss.Code != syncjob.CodeSupplierUnavailable || iss.Severity != syncjob.SeverityTransient || iss.Status != "OPEN" {
			t.Errorf("issue = %+v, want OPEN TRANSIENT SUPPLIER_UNAVAILABLE", iss)
		}

		// Silence does not turn into success however often the job is resumed.
		job = e.resume(job.ID)
		e.expect(job, syncjob.StatusInterrupted, 1, syncjob.Counts{Created: 1})
		if job.EndOfFeedSeen || job.CompletedAt != nil {
			t.Errorf("job end of feed %v, completed_at %v; want neither", job.EndOfFeedSeen, job.CompletedAt)
		}
		if iss := recovOnlyIssue(t, e, job.ID); iss.Status != "OPEN" {
			t.Errorf("issue = %+v, want it still OPEN", iss)
		}
		e.assertAbsent("SKU-B")
	})
}

func TestSupplierTimeoutIsRetriedThenInterrupts(t *testing.T) {
	cfg := flowConfig()
	cfg.RetryMaxAttempts = 3
	cfg.SupplierTimeout = 50 * time.Millisecond
	e := flowNewWith(t, cfg, syncjob.Hooks{}, nil)
	e.serve("U1", flowRecords(testutil.Active("SKU-A", 1, "Alpha", 100)), flowRecords(testutil.Active("SKU-B", 1, "Beta", 200)))
	e.sup.Block(flowCursor(2)) // the supplier hangs on the second page

	started := time.Now()
	job := e.run("U1")
	elapsed := time.Since(started)

	e.expect(job, syncjob.StatusInterrupted, 1, syncjob.Counts{Created: 1})
	testutil.AssertProduct(t, e.pool, testutil.SupplierID, "SKU-A", catalog.StatusActive, 1)
	iss := recovOnlyIssue(t, e, job.ID)
	if iss.Code != syncjob.CodeSupplierUnavailable || iss.Severity != syncjob.SeverityTransient || iss.Status != "OPEN" ||
		iss.RetryCount != cfg.RetryMaxAttempts || !strings.Contains(iss.Message, "deadline exceeded") {
		t.Errorf("issue = %+v, want OPEN TRANSIENT SUPPLIER_UNAVAILABLE with retry_count %d mentioning the deadline", iss, cfg.RetryMaxAttempts)
	}
	if n := e.sup.FetchCalls(flowCursor(2)); n != cfg.RetryMaxAttempts {
		t.Errorf("fetches at %s = %d, want %d: each hung call is cut off and retried", flowCursor(2), n, cfg.RetryMaxAttempts)
	}
	// Three timeouts of 50ms each; waiting out the default timeout on every attempt would take seconds.
	if elapsed > time.Second {
		t.Errorf("job took %v, want well under a second", elapsed)
	}
}
