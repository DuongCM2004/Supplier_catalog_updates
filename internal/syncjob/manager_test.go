package syncjob

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/supplier"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/testutil"
)

// These tests drive whole jobs through a Manager on a real PostgreSQL database. A job is
// only ever driven with Start/Resume followed by Wait, never by calling RunJob on a job a
// worker already owns, so exactly one worker acts on a job at any moment.

const mgrUpdate = "UPDATE-1"

// mgrEnv is a database, a fake supplier serving a three-batch update, and a Manager on them.
type mgrEnv struct {
	pool *pgxpool.Pool
	sup  *testutil.FakeSupplier
	m    *Manager
}

// mgrServe fills sup with a three-batch update. Applied in order it creates SKU-1, SKU-2 (batch
// 1), updates SKU-1 and creates SKU-3 (batch 2), then withdraws SKU-2, repeats an old
// SKU-1 revision and repeats SKU-3 unchanged (batch 3): created 3, updated 1, withdrawn 1,
// duplicate 1, stale 1.
func mgrServe(sup *testutil.FakeSupplier, updateID string) {
	sup.SetUpdate(testutil.Info(updateID, 3))
	sup.AddPage("", testutil.Page(updateID, "BATCH-1", "cursor-2", true,
		testutil.Active("SKU-1", 1, "Widget", 1000), testutil.Active("SKU-2", 1, "Gadget", 2000)))
	sup.AddPage("cursor-2", testutil.Page(updateID, "BATCH-2", "cursor-3", true,
		testutil.Active("SKU-1", 2, "Widget", 1200), testutil.Active("SKU-3", 1, "Gizmo", 300)))
	sup.AddPage("cursor-3", testutil.Page(updateID, "BATCH-3", "", false,
		testutil.Withdrawn("SKU-2", 2), testutil.Active("SKU-1", 1, "Widget", 1000), testutil.Active("SKU-3", 1, "Gizmo", 300)))
}

var mgrExpectedCounts = Counts{Created: 3, Updated: 1, Withdrawn: 1, Duplicate: 1, Stale: 1}

func mgrNewManager(t *testing.T, pool *pgxpool.Pool, sup supplier.Client, cfg Config, hooks Hooks) *Manager {
	t.Helper()
	m := NewManager(pool, sup, cfg, hooks, nil)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := m.Shutdown(ctx); err != nil {
			t.Errorf("manager shutdown: %v", err)
		}
	})
	return m
}

func mgrSetup(t *testing.T, hooks Hooks) *mgrEnv {
	t.Helper()
	pool := testutil.DB(t)
	testutil.Reset(t, pool)
	sup := testutil.NewFakeSupplier()
	mgrServe(sup, mgrUpdate)
	return &mgrEnv{pool: pool, sup: sup, m: mgrNewManager(t, pool, sup, fastRetryConfig(), hooks)}
}

func mgrStart(t *testing.T, m *Manager, updateID string) Job {
	t.Helper()
	job, created, err := m.Start(context.Background(), testutil.SupplierID, updateID)
	if err != nil || !created {
		t.Fatalf("Start(%s) = created %v, err %v", updateID, created, err)
	}
	return job
}

func mgrResume(t *testing.T, m *Manager, jobID string) {
	t.Helper()
	job, err := m.Resume(context.Background(), jobID)
	if err != nil {
		t.Fatalf("Resume(%s): %v", jobID, err)
	}
	if job.Status != StatusRunning {
		t.Fatalf("Resume returned status %s, want RUNNING", job.Status)
	}
}

func mgrJob(t *testing.T, pool *pgxpool.Pool, jobID string) Job {
	t.Helper()
	job, err := GetJob(context.Background(), pool, jobID)
	if err != nil {
		t.Fatalf("GetJob(%s): %v", jobID, err)
	}
	return job
}

func mgrCounts(j Job) Counts {
	return Counts{Created: j.Created, Updated: j.Updated, Withdrawn: j.Withdrawn, Duplicate: j.Duplicate, Stale: j.Stale}
}

func mgrAssertCompleted(t *testing.T, pool *pgxpool.Pool, jobID string) {
	t.Helper()
	job := mgrJob(t, pool, jobID)
	if job.Status != StatusCompleted || job.ProcessedBatches != 3 || mgrCounts(job) != mgrExpectedCounts {
		t.Fatalf("job = %s, %d batches, %+v; want COMPLETED, 3 batches, %+v", job.Status, job.ProcessedBatches, mgrCounts(job), mgrExpectedCounts)
	}
	testutil.AssertCounters(t, pool, jobID)
	testutil.AssertCompletedConsistent(t, pool, jobID)
	testutil.AssertProduct(t, pool, testutil.SupplierID, "SKU-1", "ACTIVE", 2)
	testutil.AssertProduct(t, pool, testutil.SupplierID, "SKU-2", "WITHDRAWN", 2)
	testutil.AssertProduct(t, pool, testutil.SupplierID, "SKU-3", "ACTIVE", 1)
}

func TestStartAppliesEveryBatchAndCompletesWithConsistentCounters(t *testing.T) {
	env := mgrSetup(t, Hooks{})
	started := mgrStart(t, env.m, mgrUpdate)
	if started.Status != StatusPending {
		t.Fatalf("Start returned status %s, want the new job PENDING", started.Status)
	}
	env.m.Wait()

	mgrAssertCompleted(t, env.pool, started.ID)
	job := mgrJob(t, env.pool, started.ID)
	if job.TotalBatches == nil || *job.TotalBatches != 3 || !job.EndOfFeedSeen || job.CompletedAt == nil || job.StartedAt == nil {
		t.Fatalf("job = total %v, end of feed %v, started %v, completed %v", job.TotalBatches, job.EndOfFeedSeen, job.StartedAt, job.CompletedAt)
	}
	if job.LastBatchID == nil || *job.LastBatchID != "BATCH-3" {
		t.Fatalf("last batch = %v, want BATCH-3", job.LastBatchID)
	}

	// Audit rows read back in the order the events were applied, also inside one batch.
	rows, err := env.pool.Query(context.Background(),
		`SELECT product_id || ':' || change_type FROM sync_changes WHERE job_id = $1::uuid ORDER BY created_at, id`, started.ID)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		got = append(got, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []string{"SKU-1:CREATED", "SKU-2:CREATED", "SKU-1:UPDATED", "SKU-3:CREATED", "SKU-2:WITHDRAWN"}
	if !slices.Equal(got, want) {
		t.Fatalf("changes in order = %v, want %v", got, want)
	}

	// Starting the same update again is a lookup: nothing is fetched or applied twice.
	callsBefore := len(env.sup.Calls())
	again, created, err := env.m.Start(context.Background(), testutil.SupplierID, mgrUpdate)
	if err != nil || created || again.ID != started.ID || again.Status != StatusCompleted {
		t.Fatalf("second Start = job %s %s, created %v, err %v; want the completed job, not created", again.ID, again.Status, created, err)
	}
	env.m.Wait()
	if n := len(env.sup.Calls()); n != callsBefore {
		t.Fatalf("supplier calls after second Start = %d, want %d", n, callsBefore)
	}
	mgrAssertCompleted(t, env.pool, started.ID)
}

func TestSupplierOutageInterruptsThenResumeCompletesAndResolvesTheIssue(t *testing.T) {
	env := mgrSetup(t, Hooks{})
	cfg := fastRetryConfig()
	env.sup.FailTimes("cursor-3", -1)
	started := mgrStart(t, env.m, mgrUpdate)
	env.m.Wait()

	job := mgrJob(t, env.pool, started.ID)
	if job.Status != StatusInterrupted || job.ProcessedBatches != 2 || job.NextCursor != "cursor-3" || job.EndOfFeedSeen {
		t.Fatalf("after outage: %s, %d batches, next %q, end of feed %v; want INTERRUPTED at cursor-3 after 2 batches",
			job.Status, job.ProcessedBatches, job.NextCursor, job.EndOfFeedSeen)
	}
	if job.LastError == nil || job.OpenBlockingIssues != 0 {
		t.Fatalf("last_error %v, open blocking %d; want a reason and no blocking issue", job.LastError, job.OpenBlockingIssues)
	}
	if n := env.sup.FetchCalls("cursor-3"); n != cfg.RetryMaxAttempts {
		t.Fatalf("fetches at cursor-3 = %d, want %d (every attempt, no more)", n, cfg.RetryMaxAttempts)
	}
	issues := storeIssueRows(t, env.pool, started.ID)
	if len(issues) != 1 {
		t.Fatalf("issues = %+v, want exactly one", issues)
	}
	iss := issues[0]
	if iss.Code != string(CodeSupplierUnavailable) || iss.Severity != string(SeverityTransient) || iss.Status != "OPEN" ||
		iss.Cursor != "cursor-3" || iss.RetryCount != cfg.RetryMaxAttempts {
		t.Fatalf("issue = %+v, want OPEN TRANSIENT SUPPLIER_UNAVAILABLE at cursor-3 with retry_count %d", iss, cfg.RetryMaxAttempts)
	}
	// What was committed before the outage is kept.
	testutil.AssertProduct(t, env.pool, testutil.SupplierID, "SKU-1", "ACTIVE", 2)
	testutil.AssertProduct(t, env.pool, testutil.SupplierID, "SKU-3", "ACTIVE", 1)

	env.sup.FailTimes("cursor-3", 0)
	mgrResume(t, env.m, started.ID)
	env.m.Wait()

	mgrAssertCompleted(t, env.pool, started.ID)
	issues = storeIssueRows(t, env.pool, started.ID)
	if len(issues) != 1 || issues[0].Status != "RESOLVED" || !issues[0].Resolved {
		t.Fatalf("issues after resume = %+v, want the same single issue RESOLVED", issues)
	}
	if n := env.sup.FetchCalls("cursor-2"); n != 1 {
		t.Fatalf("fetches at cursor-2 = %d, want 1: batches already committed are not fetched again", n)
	}
}

func TestBlockingRecordStopsTheJobAtItsCheckpointUntilTheSupplierCorrectsIt(t *testing.T) {
	env := mgrSetup(t, Hooks{})
	// Batch 2 now carries a negative price for SKU-3 next to a valid update of SKU-1.
	bad := testutil.Active("SKU-3", 1, "Gizmo", -5)
	env.sup.AddPage("cursor-2", testutil.Page(mgrUpdate, "BATCH-2", "cursor-3", true, testutil.Active("SKU-1", 2, "Widget", 1200), bad))

	started := mgrStart(t, env.m, mgrUpdate)
	env.m.Wait()

	assertParked := func(wantRetries int) {
		t.Helper()
		job := mgrJob(t, env.pool, started.ID)
		if job.Status != StatusNeedsAttention || job.ProcessedBatches != 1 || job.NextCursor != "cursor-2" ||
			job.LastBatchID == nil || *job.LastBatchID != "BATCH-1" || job.OpenBlockingIssues != 1 || job.EndOfFeedSeen {
			t.Fatalf("parked job = %s, %d batches, next %q, last batch %v, open blocking %d; want NEEDS_ATTENTION still at the end of batch 1",
				job.Status, job.ProcessedBatches, job.NextCursor, job.LastBatchID, job.OpenBlockingIssues)
		}
		if mgrCounts(job) != (Counts{Created: 2}) {
			t.Fatalf("counters = %+v, want only batch 1 counted: %+v", mgrCounts(job), Counts{Created: 2})
		}
		// The valid SKU-1 update that shares the batch with the bad record was rolled back too.
		testutil.AssertProduct(t, env.pool, testutil.SupplierID, "SKU-1", "ACTIVE", 1)
		if n := storeCount(t, env.pool, `SELECT count(*) FROM products`); n != 2 {
			t.Fatalf("products = %d, want 2 (batch 1 only)", n)
		}
		issues := storeIssueRows(t, env.pool, started.ID)
		if len(issues) != 1 {
			t.Fatalf("issues = %+v, want exactly one", issues)
		}
		iss := issues[0]
		if iss.Code != string(CodeInvalidProductData) || iss.Severity != string(SeverityBlocking) || iss.Status != "OPEN" ||
			iss.Cursor != "cursor-2" || iss.BatchID == nil || *iss.BatchID != "BATCH-2" || iss.RetryCount != wantRetries {
			t.Fatalf("issue = %+v, want OPEN BLOCKING INVALID_PRODUCT_DATA at cursor-2 / BATCH-2 with retry_count %d", iss, wantRetries)
		}
	}
	assertParked(0)

	// Resuming without a correction fails the same way and does not add a second issue.
	mgrResume(t, env.m, started.ID)
	env.m.Wait()
	assertParked(1)

	// The supplier corrects the page; resuming now finishes the job and closes the issue.
	env.sup.AddPage("cursor-2", testutil.Page(mgrUpdate, "BATCH-2", "cursor-3", true,
		testutil.Active("SKU-1", 2, "Widget", 1200), testutil.Active("SKU-3", 1, "Gizmo", 300)))
	mgrResume(t, env.m, started.ID)
	env.m.Wait()

	mgrAssertCompleted(t, env.pool, started.ID)
	issues := storeIssueRows(t, env.pool, started.ID)
	if len(issues) != 1 || issues[0].Status != "RESOLVED" {
		t.Fatalf("issues after correction = %+v, want the single issue RESOLVED", issues)
	}
}

func TestRestartedManagerReconcilesAndResumesFromTheCheckpoint(t *testing.T) {
	pool := testutil.DB(t)
	testutil.Reset(t, pool)
	sup := testutil.NewFakeSupplier()
	mgrServe(sup, mgrUpdate)

	// The first process dies right after batch 2 is committed, before it can note anything.
	crash := Hooks{AfterCommit: func(batchID string) error {
		if batchID == "BATCH-2" {
			return ErrSimulatedCrash
		}
		return nil
	}}
	first := mgrNewManager(t, pool, sup, fastRetryConfig(), crash)
	started := mgrStart(t, first, mgrUpdate)
	first.Wait()

	job := mgrJob(t, pool, started.ID)
	if job.Status != StatusRunning || job.ProcessedBatches != 2 || job.NextCursor != "cursor-3" {
		t.Fatalf("after the crash: %s, %d batches, next %q; want RUNNING (nobody noticed) with 2 batches committed", job.Status, job.ProcessedBatches, job.NextCursor)
	}

	// The restarted process finds the job stranded as RUNNING and interrupts it; nothing else.
	second := mgrNewManager(t, pool, sup, fastRetryConfig(), Hooks{})
	ids, err := second.Reconcile(context.Background())
	if err != nil || !slices.Equal(ids, []string{started.ID}) {
		t.Fatalf("Reconcile = %v, %v; want [%s]", ids, err, started.ID)
	}
	job = mgrJob(t, pool, started.ID)
	if job.Status != StatusInterrupted || job.LastError == nil || job.ProcessedBatches != 2 {
		t.Fatalf("after Reconcile: %s, last_error %v, %d batches; want INTERRUPTED with a reason and progress kept", job.Status, job.LastError, job.ProcessedBatches)
	}
	if ids, err := second.Reconcile(context.Background()); err != nil || len(ids) != 0 {
		t.Fatalf("second Reconcile = %v, %v; want nothing left to do", ids, err)
	}

	mgrResume(t, second, started.ID)
	second.Wait()

	mgrAssertCompleted(t, pool, started.ID)
	for cursor, want := range map[string]int{"": 1, "cursor-2": 1, "cursor-3": 1} {
		if n := sup.FetchCalls(cursor); n != want {
			t.Errorf("fetches at %q = %d, want %d: the restart must continue at the saved cursor, not start over", cursor, n, want)
		}
	}
}

func TestResumeAndStartRefuseWhileAWorkerIsAlive(t *testing.T) {
	pool := testutil.DB(t)
	testutil.Reset(t, pool)
	sup := testutil.NewFakeSupplier()
	mgrServe(sup, mgrUpdate)
	sup.SetUpdate(testutil.Info("UPDATE-2", 1)) // pages are shared by cursor, so only the update itself is announced
	cfg := fastRetryConfig()
	cfg.SupplierTimeout = 30 * time.Second // the blocked fetch below must not time out and retry
	m := mgrNewManager(t, pool, sup, cfg, Hooks{})

	// The first fetch hangs, so the worker is certainly alive for the next few calls.
	sup.Block("")
	started := mgrStart(t, m, mgrUpdate)
	ctx := context.Background()

	if _, err := m.Resume(ctx, started.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("Resume of a job with a live worker: error = %v, want ErrInvalidTransition", err)
	}
	other, created, err := m.Start(ctx, testutil.SupplierID, "UPDATE-2")
	if !errors.Is(err, ErrActiveJobExists) || created || other.ID != started.ID {
		t.Errorf("Start of a second update = job %s, created %v, err %v; want ErrActiveJobExists naming job %s", other.ID, created, err, started.ID)
	}
	if _, err := m.Resume(ctx, "00000000-0000-0000-0000-000000000000"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Resume of an unknown job: error = %v, want ErrNotFound", err)
	}

	sup.Unblock("")
	m.Wait()
	mgrAssertCompleted(t, pool, started.ID)
	if _, err := m.Resume(ctx, started.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("Resume of a COMPLETED job: error = %v, want ErrInvalidTransition", err)
	}
	if n := storeCount(t, pool, `SELECT count(*) FROM sync_jobs`); n != 1 {
		t.Errorf("sync_jobs rows = %d, want 1: the refused start must not leave a job behind", n)
	}
}

// mgrPanickingSupplier stands in for a bug in a worker's code path.
type mgrPanickingSupplier struct{ *testutil.FakeSupplier }

func (mgrPanickingSupplier) FetchBatch(context.Context, string, string) (supplier.Batch, error) {
	panic("boom")
}

func TestWorkerPanicIsContainedAndLeavesTheJobResumable(t *testing.T) {
	pool := testutil.DB(t)
	testutil.Reset(t, pool)
	sup := testutil.NewFakeSupplier()
	mgrServe(sup, mgrUpdate)

	broken := mgrNewManager(t, pool, mgrPanickingSupplier{sup}, fastRetryConfig(), Hooks{})
	started := mgrStart(t, broken, mgrUpdate)
	broken.Wait() // returning at all means the panic did not take the process down

	job := mgrJob(t, pool, started.ID)
	if job.Status != StatusInterrupted || job.LastError == nil || !strings.Contains(*job.LastError, "boom") {
		t.Fatalf("after the panic: %s, last_error %v; want INTERRUPTED naming the panic", job.Status, job.LastError)
	}

	healthy := mgrNewManager(t, pool, sup, fastRetryConfig(), Hooks{})
	mgrResume(t, healthy, started.ID)
	healthy.Wait()
	mgrAssertCompleted(t, pool, started.ID)
}
