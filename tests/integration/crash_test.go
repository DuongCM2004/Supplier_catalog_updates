package integration

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/supplier"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/syncjob"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/testutil"
)

// These tests kill, restart, interrupt and replay whole sync jobs on a real PostgreSQL
// database and then compare the outcome with a run that never failed. A job is only ever
// driven with Start/Resume followed by Wait, so exactly one worker acts on it at a time, and
// nothing waits on the clock: the test learns where a worker is from channels and hooks.

const (
	crashUpdate      = "UPDATE-CRASH"
	crashOtherUpdate = "UPDATE-OTHER"

	// No scenario relies on a supplier timeout. The long value only guarantees that a fetch
	// held by FakeSupplier.Block ends because of a shutdown or an Unblock, never because it
	// timed out and was retried.
	crashSupplierTimeout = 30 * time.Second

	// crashWait is a failsafe for waiting on a worker, so a broken test fails instead of hanging.
	crashWait = 20 * time.Second
)

func crashConfig() syncjob.Config {
	return syncjob.Config{RetryMaxAttempts: 3, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond, SupplierTimeout: crashSupplierTimeout}
}

// crashPage is one page of the feed and the cursor it is served at.
type crashPage struct {
	cursor string
	batch  supplier.Batch
}

// crashPages is a four-batch feed that exercises every kind of counter: it creates, updates,
// withdraws, repeats a record and sends a stale one, and ends with a tombstone for a product
// the catalog never saw. Applied in order: created 4, updated 1, withdrawn 2, duplicate 1, stale 1.
func crashPages() []crashPage {
	u := crashUpdate
	return []crashPage{
		{"", testutil.Page(u, "BATCH-1", "cursor-2", true,
			testutil.Active("SKU-A", 1, "Alpha", 100), testutil.Active("SKU-B", 1, "Beta", 200))},
		{"cursor-2", testutil.Page(u, "BATCH-2", "cursor-3", true,
			testutil.Active("SKU-A", 2, "Alpha", 150), testutil.Active("SKU-C", 1, "Gamma", 300))},
		{"cursor-3", testutil.Page(u, "BATCH-3", "cursor-4", true,
			testutil.Withdrawn("SKU-B", 2), testutil.Active("SKU-A", 1, "Alpha", 100), testutil.Active("SKU-C", 1, "Gamma", 300))},
		{"cursor-4", testutil.Page(u, "BATCH-4", "", false,
			testutil.Active("SKU-D", 1, "Delta", 400), testutil.Withdrawn("SKU-Z", 5))},
	}
}

var crashExpected = syncjob.Counts{Created: 4, Updated: 1, Withdrawn: 2, Duplicate: 1, Stale: 1}

// crashSupplier serves crashPages as an update of four batches. Each call returns a supplier
// with its own call log.
func crashSupplier() *testutil.FakeSupplier {
	sup := testutil.NewFakeSupplier()
	sup.SetUpdate(testutil.Info(crashUpdate, len(crashPages())))
	for _, p := range crashPages() {
		sup.AddPage(p.cursor, p.batch)
	}
	return sup
}

func crashPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testutil.DB(t)
	testutil.Reset(t, pool)
	return pool
}

// crashNewManager returns a Manager standing for one run of the service. Its workers are
// stopped when the test ends.
func crashNewManager(t *testing.T, pool *pgxpool.Pool, sup supplier.Client, hooks syncjob.Hooks) *syncjob.Manager {
	t.Helper()
	m := syncjob.NewManager(pool, sup, crashConfig(), hooks, slog.New(slog.DiscardHandler))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := m.Shutdown(ctx); err != nil {
			t.Errorf("manager shutdown: %v", err)
		}
	})
	return m
}

func crashStart(t *testing.T, m *syncjob.Manager, updateID string) syncjob.Job {
	t.Helper()
	job, created, err := m.Start(context.Background(), testutil.SupplierID, updateID)
	if err != nil || !created {
		t.Fatalf("Start(%s) = created %v, err %v", updateID, created, err)
	}
	return job
}

func crashResume(t *testing.T, m *syncjob.Manager, jobID string) {
	t.Helper()
	job, err := m.Resume(context.Background(), jobID)
	if err != nil {
		t.Fatalf("Resume(%s): %v", jobID, err)
	}
	if job.Status != syncjob.StatusRunning {
		t.Fatalf("Resume returned status %s, want RUNNING", job.Status)
	}
}

func crashJob(t *testing.T, pool *pgxpool.Pool, jobID string) syncjob.Job {
	t.Helper()
	job, err := syncjob.GetJob(context.Background(), pool, jobID)
	if err != nil {
		t.Fatalf("GetJob(%s): %v", jobID, err)
	}
	return job
}

func crashCounts(j syncjob.Job) syncjob.Counts {
	return syncjob.Counts{Created: j.Created, Updated: j.Updated, Withdrawn: j.Withdrawn, Duplicate: j.Duplicate, Stale: j.Stale}
}

func crashCount(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func crashStrings(t *testing.T, pool *pgxpool.Pool, query string, args ...any) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(), query, args...)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return out
}

// crashSummary is everything about a finished job that must not depend on whether the run
// was interrupted on the way: what the catalog holds, which changes were recorded, which
// batches were committed (with their positions and content hashes) and the job's totals.
// Ids and timestamps are left out because they differ between any two runs.
type crashSummary struct {
	Status    syncjob.Status
	Processed int
	Counts    syncjob.Counts
	Products  []string
	Changes   []string
	Batches   []string
}

func crashSummarize(t *testing.T, pool *pgxpool.Pool, jobID string) crashSummary {
	t.Helper()
	job := crashJob(t, pool, jobID)
	return crashSummary{
		Status:    job.Status,
		Processed: job.ProcessedBatches,
		Counts:    crashCounts(job),
		Products: crashStrings(t, pool,
			`SELECT product_id || ' ' || status || ' r' || revision || ' ' || COALESCE(name, '-') || ' ' || COALESCE(price_minor::text, '-')
			 FROM products ORDER BY product_id`),
		Changes: crashStrings(t, pool,
			`SELECT product_id || ' r' || revision || ' ' || change_type FROM sync_changes
			 WHERE job_id = $1::uuid ORDER BY product_id, revision`, jobID),
		Batches: crashStrings(t, pool,
			`SELECT batch_id || ' seq ' || seq || ' in "' || cursor_in || '" out "' || cursor_out || '" ' || payload_hash
			 FROM sync_batches WHERE job_id = $1::uuid ORDER BY seq`, jobID),
	}
}

// crashBaseline runs the feed once with nothing going wrong and returns what a correct run
// leaves behind. It resets the database before and after, so the caller starts clean.
func crashBaseline(t *testing.T, pool *pgxpool.Pool) crashSummary {
	t.Helper()
	testutil.Reset(t, pool)
	m := crashNewManager(t, pool, crashSupplier(), syncjob.Hooks{})
	job := crashStart(t, m, crashUpdate)
	m.Wait()
	testutil.AssertCounters(t, pool, job.ID)
	testutil.AssertCompletedConsistent(t, pool, job.ID)
	want := crashSummarize(t, pool, job.ID)
	if want.Status != syncjob.StatusCompleted || want.Processed != len(crashPages()) || want.Counts != crashExpected ||
		len(want.Changes) != crashExpected.Created+crashExpected.Updated+crashExpected.Withdrawn {
		t.Fatalf("crash-free run = %+v; want COMPLETED after %d batches with %+v", want, len(crashPages()), crashExpected)
	}
	testutil.Reset(t, pool)
	return want
}

// crashAssertRecovered fails unless the job finished exactly like the crash-free run.
func crashAssertRecovered(t *testing.T, pool *pgxpool.Pool, jobID string, want crashSummary) {
	t.Helper()
	if got := crashSummarize(t, pool, jobID); !reflect.DeepEqual(got, want) {
		t.Fatalf("after recovery:\n got %+v\nwant %+v", got, want)
	}
	testutil.AssertCounters(t, pool, jobID)
	testutil.AssertCompletedConsistent(t, pool, jobID)
}

// crashAssertFetches checks how often each page of the feed was requested from the supplier.
func crashAssertFetches(t *testing.T, sup *testutil.FakeSupplier, perPage func(index int) int) {
	t.Helper()
	for i, p := range crashPages() {
		if got, want := sup.FetchCalls(p.cursor), perPage(i); got != want {
			t.Errorf("fetches of %s at cursor %q = %d, want %d", p.batch.BatchID, p.cursor, got, want)
		}
	}
}

func crashFetchedOnce(int) int { return 1 }

// crashOnlyIssue requires exactly one issue on the job and returns it.
func crashOnlyIssue(t *testing.T, pool *pgxpool.Pool, jobID string, code syncjob.IssueCode, severity syncjob.Severity, status, cursor string) syncjob.Issue {
	t.Helper()
	issues, err := syncjob.ListIssues(context.Background(), pool, jobID, "", "", 50, 0)
	if err != nil {
		t.Fatalf("ListIssues: %v", err)
	}
	if len(issues) != 1 {
		t.Fatalf("issues = %+v, want exactly one", issues)
	}
	iss := issues[0]
	if iss.Code != code || iss.Severity != severity || iss.Status != status || iss.Cursor == nil || *iss.Cursor != cursor {
		t.Fatalf("issue = %+v, want %s %s %s at cursor %q", iss, status, severity, code, cursor)
	}
	return iss
}

// crashGate parks a worker inside a hook until the test lets it go, so the test can look at
// the database at exactly that moment.
type crashGate struct {
	reached chan string
	release chan struct{}
	once    sync.Once
}

func crashNewGate() *crashGate {
	return &crashGate{reached: make(chan string, 8), release: make(chan struct{})}
}

// park is called by the worker: it announces the batch and waits for open.
func (g *crashGate) park(batchID string) {
	g.reached <- batchID
	<-g.release
}

// open lets every parked worker go. It is safe to call more than once.
func (g *crashGate) open() { g.once.Do(func() { close(g.release) }) }

func crashRecv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(crashWait):
		t.Fatalf("timed out waiting for %s", what)
	}
	var zero T
	return zero
}

func TestCrashBeforeCommitLeavesNoPartialEffects(t *testing.T) {
	pool := crashPool(t)
	want := crashBaseline(t, pool)

	for crashAt, page := range crashPages() {
		batchID := page.batch.BatchID
		t.Run("crash_in_"+batchID, func(t *testing.T) {
			testutil.Reset(t, pool)
			sup := crashSupplier()
			gate := crashNewGate()
			crashed := crashNewManager(t, pool, sup, syncjob.Hooks{BeforeCommit: func(_ context.Context, _ pgx.Tx, id string) error {
				if id != batchID {
					return nil
				}
				gate.park(id) // the batch transaction is open and every write of the batch is in it
				return syncjob.ErrSimulatedCrash
			}})
			t.Cleanup(gate.open) // runs before the manager's shutdown, so a failed test cannot hang on the parked worker

			job := crashStart(t, crashed, crashUpdate)
			crashRecv(t, gate.reached, "the batch transaction to be open")
			committedSoFar := testutil.Snapshot(t, pool) // the batches before this one, nothing of this one
			gate.open()
			crashed.Wait()

			testutil.AssertUnchanged(t, committedSoFar, testutil.Snapshot(t, pool))
			stranded := crashJob(t, pool, job.ID)
			if stranded.Status != syncjob.StatusRunning || stranded.ProcessedBatches != crashAt || stranded.NextCursor != page.cursor {
				t.Fatalf("after the crash: %s, %d batches, next %q; want RUNNING with %d batches, next %q",
					stranded.Status, stranded.ProcessedBatches, stranded.NextCursor, crashAt, page.cursor)
			}

			// The restarted service finds the job stranded, interrupts it and is told to resume it.
			restarted := crashNewManager(t, pool, sup, syncjob.Hooks{})
			ids, err := restarted.Reconcile(context.Background())
			if err != nil || len(ids) != 1 || ids[0] != job.ID {
				t.Fatalf("Reconcile = %v, %v; want [%s]", ids, err, job.ID)
			}
			crashResume(t, restarted, job.ID)
			restarted.Wait()

			crashAssertRecovered(t, pool, job.ID, want)
			// Only the batch that crashed is fetched a second time; every committed one stays committed.
			crashAssertFetches(t, sup, func(i int) int {
				if i == crashAt {
					return 2
				}
				return 1
			})
		})
	}
}

func TestCrashAfterCommitDoesNotDuplicateEffects(t *testing.T) {
	pool := crashPool(t)
	want := crashBaseline(t, pool)
	pages := crashPages()

	for crashAt, page := range pages {
		batchID := page.batch.BatchID
		last := crashAt == len(pages)-1
		t.Run("crash_after_"+batchID, func(t *testing.T) {
			testutil.Reset(t, pool)
			sup := crashSupplier()
			crashed := crashNewManager(t, pool, sup, syncjob.Hooks{AfterCommit: func(id string) error {
				if id == batchID {
					return syncjob.ErrSimulatedCrash
				}
				return nil
			}})
			job := crashStart(t, crashed, crashUpdate)
			crashed.Wait()

			stranded := crashJob(t, pool, job.ID)
			if last {
				// The final commit also completed the job; only the acknowledgement was lost.
				if stranded.Status != syncjob.StatusCompleted {
					t.Fatalf("after the crash: %s, want COMPLETED: the last batch was committed", stranded.Status)
				}
			} else if stranded.Status != syncjob.StatusRunning || stranded.ProcessedBatches != crashAt+1 || stranded.NextCursor != pages[crashAt+1].cursor {
				t.Fatalf("after the crash: %s, %d batches, next %q; want RUNNING with %d batches, next %q",
					stranded.Status, stranded.ProcessedBatches, stranded.NextCursor, crashAt+1, pages[crashAt+1].cursor)
			}
			committed := testutil.Snapshot(t, pool)

			// A worker that never saw the commit presents the same batch again: nothing changes.
			replay := syncjob.NewProcessor(pool, syncjob.Hooks{}, slog.New(slog.DiscardHandler))
			wantKind := syncjob.KindReplayed
			if last {
				wantKind = syncjob.KindStale // the job is COMPLETED, so there is nothing left to replay into
			}
			out, err := replay.ProcessBatch(context.Background(), job.ID, page.cursor, page.batch)
			if err != nil || out.Kind != wantKind {
				t.Fatalf("ProcessBatch of the committed batch = kind %d, %v; want kind %d", out.Kind, err, wantKind)
			}
			testutil.AssertUnchanged(t, committed, testutil.Snapshot(t, pool))

			restarted := crashNewManager(t, pool, sup, syncjob.Hooks{})
			ids, err := restarted.Reconcile(context.Background())
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if last {
				if len(ids) != 0 {
					t.Fatalf("Reconcile = %v, want nothing to interrupt", ids)
				}
				if _, err := restarted.Resume(context.Background(), job.ID); !errors.Is(err, syncjob.ErrInvalidTransition) {
					t.Fatalf("Resume of a COMPLETED job: error = %v, want ErrInvalidTransition", err)
				}
			} else {
				if len(ids) != 1 || ids[0] != job.ID {
					t.Fatalf("Reconcile = %v, want [%s]", ids, job.ID)
				}
				crashResume(t, restarted, job.ID)
				restarted.Wait()
			}

			crashAssertRecovered(t, pool, job.ID, want)
			crashAssertFetches(t, sup, crashFetchedOnce) // the committed batch is never fetched again
		})
	}
}

func TestStaleWorkerReplayOfCommittedBatchesChangesNothing(t *testing.T) {
	pool := crashPool(t)
	want := crashBaseline(t, pool)
	sup := crashSupplier()
	gate := crashNewGate()
	m := crashNewManager(t, pool, sup, syncjob.Hooks{AfterCommit: func(id string) error {
		if id == "BATCH-2" {
			gate.park(id) // batch 2 is committed, the worker has not moved on yet
		}
		return nil
	}})
	t.Cleanup(gate.open)

	job := crashStart(t, m, crashUpdate)
	crashRecv(t, gate.reached, "batch 2 to be committed")
	midRun := testutil.Snapshot(t, pool)

	// While the real worker is alive, a second one presents batches that are already in.
	late := syncjob.NewProcessor(pool, syncjob.Hooks{}, slog.New(slog.DiscardHandler))
	for _, p := range crashPages()[:2] {
		out, err := late.ProcessBatch(context.Background(), job.ID, p.cursor, p.batch)
		if err != nil || out.Kind != syncjob.KindReplayed {
			t.Fatalf("ProcessBatch(%s at %q) = kind %d, %v; want replayed", p.batch.BatchID, p.cursor, out.Kind, err)
		}
	}
	testutil.AssertUnchanged(t, midRun, testutil.Snapshot(t, pool))

	gate.open()
	m.Wait()
	crashAssertRecovered(t, pool, job.ID, want)
	crashAssertFetches(t, sup, crashFetchedOnce)
}

func TestBatchIDReusedWithDifferentPayloadConflicts(t *testing.T) {
	pool := crashPool(t)
	pages := crashPages()
	sup := testutil.NewFakeSupplier()
	sup.SetUpdate(testutil.Info(crashUpdate, 3))
	sup.AddPage("", pages[0].batch) // BATCH-1: creates SKU-A and SKU-B
	// The second page reuses the id BATCH-1 for other products.
	sup.AddPage("cursor-2", testutil.Page(crashUpdate, "BATCH-1", "cursor-3", true,
		testutil.Active("SKU-C", 1, "Gamma", 300), testutil.Active("SKU-A", 2, "Alpha", 150)))
	sup.AddPage("cursor-3", testutil.Page(crashUpdate, "BATCH-3", "", false, testutil.Active("SKU-D", 1, "Delta", 400)))

	m := crashNewManager(t, pool, sup, syncjob.Hooks{})
	job := crashStart(t, m, crashUpdate)
	m.Wait()

	assertParked := func(wantRetries int) {
		t.Helper()
		after := crashJob(t, pool, job.ID)
		if after.Status != syncjob.StatusNeedsAttention || after.ProcessedBatches != 1 || after.NextCursor != "cursor-2" ||
			after.LastBatchID == nil || *after.LastBatchID != "BATCH-1" || after.OpenBlockingIssues != 1 || after.EndOfFeedSeen {
			t.Fatalf("job = %s, %d batches, next %q, last batch %v, open blocking %d; want NEEDS_ATTENTION still after batch 1",
				after.Status, after.ProcessedBatches, after.NextCursor, after.LastBatchID, after.OpenBlockingIssues)
		}
		if crashCounts(after) != (syncjob.Counts{Created: 2}) {
			t.Fatalf("counters = %+v, want only the first batch counted", crashCounts(after))
		}
		// Batch 1's effects are kept; nothing of the conflicting page was applied.
		testutil.AssertProduct(t, pool, testutil.SupplierID, "SKU-A", "ACTIVE", 1)
		testutil.AssertProduct(t, pool, testutil.SupplierID, "SKU-B", "ACTIVE", 1)
		if n := crashCount(t, pool, `SELECT count(*) FROM products`); n != 2 {
			t.Fatalf("products = %d, want 2", n)
		}
		if n := crashCount(t, pool, `SELECT count(*) FROM sync_batches`); n != 1 {
			t.Fatalf("committed batches = %d, want 1", n)
		}
		iss := crashOnlyIssue(t, pool, job.ID, syncjob.CodeBatchConflict, syncjob.SeverityBlocking, "OPEN", "cursor-2")
		if iss.RetryCount != wantRetries {
			t.Fatalf("issue retry_count = %d, want %d", iss.RetryCount, wantRetries)
		}
	}
	assertParked(0)

	// Resuming without a fix hits the same conflict and counts it on the same issue.
	crashResume(t, m, job.ID)
	m.Wait()
	assertParked(1)

	// The supplier sends the real second batch; the job picks up where it stopped.
	sup.AddPage("cursor-2", testutil.Page(crashUpdate, "BATCH-2", "cursor-3", true,
		testutil.Active("SKU-C", 1, "Gamma", 300), testutil.Active("SKU-A", 2, "Alpha", 150)))
	crashResume(t, m, job.ID)
	m.Wait()

	testutil.AssertCounters(t, pool, job.ID)
	testutil.AssertCompletedConsistent(t, pool, job.ID)
	if done := crashJob(t, pool, job.ID); done.ProcessedBatches != 3 || crashCounts(done) != (syncjob.Counts{Created: 4, Updated: 1}) {
		t.Fatalf("job = %d batches, %+v; want 3 batches with created 4, updated 1", done.ProcessedBatches, crashCounts(done))
	}
	crashOnlyIssue(t, pool, job.ID, syncjob.CodeBatchConflict, syncjob.SeverityBlocking, "RESOLVED", "cursor-2")
	if n := sup.FetchCalls(""); n != 1 {
		t.Fatalf("fetches of the first page = %d, want 1: a committed batch is never fetched again", n)
	}
}

func TestIdenticalBatchAtNewCursorIsRejected(t *testing.T) {
	pool := crashPool(t)
	pages := crashPages()
	sup := testutil.NewFakeSupplier()
	sup.SetUpdate(testutil.Info(crashUpdate, 0)) // no announced count: only the cursor chain can show the loop
	sup.AddPage("", pages[0].batch)
	sup.AddPage("cursor-2", pages[1].batch)
	sup.AddPage("cursor-3", pages[0].batch) // batch 1 again, still pointing at cursor-2: the chain loops back

	m := crashNewManager(t, pool, sup, syncjob.Hooks{})
	job := crashStart(t, m, crashUpdate)
	m.Wait() // returning at all means the worker did not follow the loop

	after := crashJob(t, pool, job.ID)
	if after.Status != syncjob.StatusNeedsAttention || after.ProcessedBatches != 2 || after.NextCursor != "cursor-3" ||
		after.EndOfFeedSeen || after.OpenBlockingIssues != 1 {
		t.Fatalf("job = %s, %d batches, next %q, end of feed %v, open blocking %d; want NEEDS_ATTENTION at cursor-3 after 2 batches",
			after.Status, after.ProcessedBatches, after.NextCursor, after.EndOfFeedSeen, after.OpenBlockingIssues)
	}
	if crashCounts(after) != (syncjob.Counts{Created: 3, Updated: 1}) {
		t.Fatalf("counters = %+v, want batches 1 and 2 counted once", crashCounts(after))
	}
	crashOnlyIssue(t, pool, job.ID, syncjob.CodeInvalidCursor, syncjob.SeverityBlocking, "OPEN", "cursor-3")
	for cursor, want := range map[string]int{"": 1, "cursor-2": 1, "cursor-3": 1} {
		if got := sup.FetchCalls(cursor); got != want {
			t.Errorf("fetches at %q = %d, want %d", cursor, got, want)
		}
	}
	testutil.AssertCounters(t, pool, job.ID)
}

// crashSeedJob inserts a job that no worker owns, as a crash between the insert and the
// first transition (PENDING) or in the middle of a run (RUNNING) leaves it.
func crashSeedJob(t *testing.T, pool *pgxpool.Pool, status string) string {
	t.Helper()
	var id string
	err := pool.QueryRow(context.Background(),
		`INSERT INTO sync_jobs (supplier_id, supplier_update_id, status, started_at)
		 VALUES ($1, $2, $3::text, CASE WHEN $3::text = 'RUNNING' THEN now() END) RETURNING id::text`,
		testutil.SupplierID, crashUpdate, status).Scan(&id)
	if err != nil {
		t.Fatalf("seed %s job: %v", status, err)
	}
	return id
}

func TestResumeRecoversOrphanedRunningJob(t *testing.T) {
	pool := crashPool(t)
	want := crashBaseline(t, pool)

	t.Run("running_job_whose_worker_died_mid_feed", func(t *testing.T) {
		testutil.Reset(t, pool)
		sup := crashSupplier()
		dead := crashNewManager(t, pool, sup, syncjob.Hooks{AfterCommit: func(id string) error {
			if id == "BATCH-2" {
				return syncjob.ErrSimulatedCrash
			}
			return nil
		}})
		job := crashStart(t, dead, crashUpdate)
		dead.Wait()
		if stranded := crashJob(t, pool, job.ID); stranded.Status != syncjob.StatusRunning || stranded.ProcessedBatches != 2 {
			t.Fatalf("stranded job = %s with %d batches, want RUNNING with 2", stranded.Status, stranded.ProcessedBatches)
		}

		// This manager never started the job and the service was not restarted: no reconcile ran.
		adopter := crashNewManager(t, pool, sup, syncjob.Hooks{})
		other, created, err := adopter.Start(context.Background(), testutil.SupplierID, crashOtherUpdate)
		if !errors.Is(err, syncjob.ErrActiveJobExists) || created || other.ID != job.ID {
			t.Fatalf("Start of another update = job %s, created %v, err %v; want the orphan blocking it (ErrActiveJobExists naming %s)",
				other.ID, created, err, job.ID)
		}
		crashResume(t, adopter, job.ID)
		adopter.Wait()

		crashAssertRecovered(t, pool, job.ID, want)
		crashAssertFetches(t, sup, crashFetchedOnce)
	})

	t.Run("pending_job_whose_worker_never_started", func(t *testing.T) {
		testutil.Reset(t, pool)
		sup := crashSupplier()
		jobID := crashSeedJob(t, pool, "PENDING")
		adopter := crashNewManager(t, pool, sup, syncjob.Hooks{})
		crashResume(t, adopter, jobID)
		adopter.Wait()

		crashAssertRecovered(t, pool, jobID, want)
		crashAssertFetches(t, sup, crashFetchedOnce)
	})

	t.Run("concurrent_resumes_start_one_worker", func(t *testing.T) {
		testutil.Reset(t, pool)
		sup := crashSupplier()
		sup.Block("") // the first fetch hangs, so the winner's worker stays alive while the others try
		jobID := crashSeedJob(t, pool, "RUNNING")
		adopter := crashNewManager(t, pool, sup, syncjob.Hooks{})
		t.Cleanup(func() { sup.Unblock("") })

		const resumers = 8
		start := make(chan struct{})
		results := make(chan error, resumers)
		var wg sync.WaitGroup
		for range resumers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, err := adopter.Resume(context.Background(), jobID)
				results <- err
			}()
		}
		close(start)
		wg.Wait()
		close(results)

		accepted := 0
		for err := range results {
			switch {
			case err == nil:
				accepted++
			case !errors.Is(err, syncjob.ErrInvalidTransition):
				t.Errorf("a losing Resume returned %v, want ErrInvalidTransition", err)
			}
		}
		if accepted != 1 {
			t.Fatalf("%d of %d concurrent Resume calls were accepted, want exactly 1", accepted, resumers)
		}

		sup.Unblock("")
		adopter.Wait()
		crashAssertRecovered(t, pool, jobID, want)
		updateCalls := 0
		for _, c := range sup.Calls() {
			if c.Op == testutil.OpGetUpdate {
				updateCalls++
			}
		}
		if updateCalls != 1 {
			t.Errorf("GetUpdate calls = %d, want 1: only one worker may have run", updateCalls)
		}
		crashAssertFetches(t, sup, crashFetchedOnce)
	})
}

// crashSpySupplier tells the test when a fetch starts, so it knows the worker is inside the
// supplier call before it shuts the service down.
type crashSpySupplier struct {
	*testutil.FakeSupplier
	fetching chan string
}

func (s crashSpySupplier) FetchBatch(ctx context.Context, updateID, cursor string) (supplier.Batch, error) {
	s.fetching <- cursor
	return s.FakeSupplier.FetchBatch(ctx, updateID, cursor)
}

func crashShutdown(t *testing.T, m *syncjob.Manager) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := m.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// crashAssertInterruptedByShutdown checks what stopping the service must leave behind: an
// INTERRUPTED job at the saved checkpoint, a reason that names the shutdown, and no issue at
// all, because stopping on request is not a supplier or database problem.
func crashAssertInterruptedByShutdown(t *testing.T, pool *pgxpool.Pool, jobID string, processed int, nextCursor string) {
	t.Helper()
	job := crashJob(t, pool, jobID)
	if job.Status != syncjob.StatusInterrupted || job.ProcessedBatches != processed || job.NextCursor != nextCursor ||
		job.LastError == nil || !strings.Contains(*job.LastError, "shutting down") {
		t.Fatalf("after shutdown: %s, %d batches, next %q, last_error %v; want INTERRUPTED after %d batches at %q, citing the shutdown",
			job.Status, job.ProcessedBatches, job.NextCursor, job.LastError, processed, nextCursor)
	}
	if n := crashCount(t, pool, `SELECT count(*) FROM sync_issues`); n != 0 {
		t.Fatalf("issue rows after a graceful shutdown = %d, want 0", n)
	}
}

func TestShutdownInterruptsWithoutIssues(t *testing.T) {
	pool := crashPool(t)
	want := crashBaseline(t, pool)

	t.Run("during_supplier_fetch", func(t *testing.T) {
		testutil.Reset(t, pool)
		fake := crashSupplier()
		fake.Block("cursor-3") // the supplier hangs on the third page
		spy := crashSpySupplier{FakeSupplier: fake, fetching: make(chan string, 16)}
		m := crashNewManager(t, pool, spy, syncjob.Hooks{})
		job := crashStart(t, m, crashUpdate)

		for crashRecv(t, spy.fetching, "the worker to ask for the third page") != "cursor-3" {
		}
		crashShutdown(t, m)
		crashAssertInterruptedByShutdown(t, pool, job.ID, 2, "cursor-3")

		fake.Unblock("cursor-3")
		resumed := crashNewManager(t, pool, fake, syncjob.Hooks{})
		crashResume(t, resumed, job.ID)
		resumed.Wait()
		crashAssertRecovered(t, pool, job.ID, want)
		crashAssertFetches(t, fake, func(i int) int {
			if i == 2 {
				return 2 // the hung request and the one after the resume
			}
			return 1
		})
	})

	t.Run("during_batch_transaction", func(t *testing.T) {
		testutil.Reset(t, pool)
		sup := crashSupplier()
		inTx := make(chan string, 4)
		m := crashNewManager(t, pool, sup, syncjob.Hooks{BeforeCommit: func(ctx context.Context, _ pgx.Tx, id string) error {
			if id != "BATCH-3" {
				return nil
			}
			inTx <- id
			<-ctx.Done() // the transaction stays open until the service is told to stop
			return ctx.Err()
		}})
		job := crashStart(t, m, crashUpdate)

		crashRecv(t, inTx, "the third batch transaction to be open")
		beforeStop := testutil.SnapshotExcept(t, pool, "sync_jobs") // batches 1 and 2 only
		crashShutdown(t, m)

		testutil.AssertUnchanged(t, beforeStop, testutil.SnapshotExcept(t, pool, "sync_jobs"))
		crashAssertInterruptedByShutdown(t, pool, job.ID, 2, "cursor-3")

		resumed := crashNewManager(t, pool, sup, syncjob.Hooks{})
		crashResume(t, resumed, job.ID)
		resumed.Wait()
		crashAssertRecovered(t, pool, job.ID, want)
		crashAssertFetches(t, sup, func(i int) int {
			if i == 2 {
				return 2 // fetched before the stop, refetched because its transaction never committed
			}
			return 1
		})
	})
}

func TestConnectionLossMidBatchInterruptsJobAndResumeCompletes(t *testing.T) {
	pool := crashPool(t)
	want := crashBaseline(t, pool)
	sup := crashSupplier()

	var killed atomic.Bool
	m := crashNewManager(t, pool, sup, syncjob.Hooks{BeforeCommit: func(ctx context.Context, tx pgx.Tx, id string) error {
		if id != "BATCH-3" || killed.Load() {
			return nil
		}
		// Once, on the last statement before COMMIT, another connection ends this
		// transaction's backend and waits until it is gone.
		var pid int
		if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
			return err
		}
		var gone bool
		if err := pool.QueryRow(ctx, `SELECT pg_terminate_backend($1::int, 5000)`, pid).Scan(&gone); err != nil {
			return err
		}
		killed.Store(gone)
		return nil
	}})
	job := crashStart(t, m, crashUpdate)
	m.Wait()

	if !killed.Load() {
		t.Fatal("the hook did not manage to end the transaction's backend")
	}
	after := crashJob(t, pool, job.ID)
	if after.Status != syncjob.StatusInterrupted || after.ProcessedBatches != 2 || after.NextCursor != "cursor-3" {
		t.Fatalf("after the connection loss: %s, %d batches, next %q; want INTERRUPTED after 2 batches at cursor-3",
			after.Status, after.ProcessedBatches, after.NextCursor)
	}
	if n := crashCount(t, pool, `SELECT count(*) FROM sync_batches`); n != 2 {
		t.Fatalf("committed batches = %d, want 2: nothing of the third survives", n)
	}
	crashOnlyIssue(t, pool, job.ID, syncjob.CodeDatabaseError, syncjob.SeverityTransient, "OPEN", "cursor-3")

	crashResume(t, m, job.ID)
	m.Wait()
	crashAssertRecovered(t, pool, job.ID, want)
	crashOnlyIssue(t, pool, job.ID, syncjob.CodeDatabaseError, syncjob.SeverityTransient, "RESOLVED", "cursor-3")
	crashAssertFetches(t, sup, func(i int) int {
		if i == 2 {
			return 2
		}
		return 1
	})
}
