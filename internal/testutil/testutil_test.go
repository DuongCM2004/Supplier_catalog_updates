package testutil

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/catalog"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/supplier"
)

// recorder stands in for *testing.T so a test can prove an assertion helper fails when it
// must. Fatalf ends the calling goroutine just like the real one.
type recorder struct {
	testing.TB
	failures []string
}

func (r *recorder) Helper() {}

func (r *recorder) Errorf(format string, args ...any) {
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

func (r *recorder) Fatalf(format string, args ...any) {
	r.Errorf(format, args...)
	runtime.Goexit()
}

// failuresOf runs check against a recorder and returns what it reported.
func failuresOf(check func(tb testing.TB)) []string {
	r := &recorder{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		check(r)
	}()
	<-done
	return r.failures
}

func requireFailure(t *testing.T, failures []string, want string) {
	t.Helper()
	if !slices.ContainsFunc(failures, func(f string) bool { return strings.Contains(f, want) }) {
		t.Fatalf("want a failure mentioning %q, got %q", want, failures)
	}
}

func requireNoFailures(t *testing.T, failures []string) {
	t.Helper()
	if len(failures) != 0 {
		t.Fatalf("unexpected failures: %q", failures)
	}
}

func mustExec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

// seedCompletedJob inserts, with plain SQL, a COMPLETED job that committed one batch
// creating one product, so every counter and audit row agrees.
func seedCompletedJob(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var jobID string
	err := pool.QueryRow(context.Background(), `
		INSERT INTO sync_jobs (supplier_id, supplier_update_id, status, last_batch_id, processed_batches,
		                       total_batches, end_of_feed_seen, created_count, completed_at)
		VALUES ('SUP-001', 'UPDATE-X', 'COMPLETED', 'BATCH-001', 1, 1, true, 1, now())
		RETURNING id::text`).Scan(&jobID)
	if err != nil {
		t.Fatalf("insert job: %v", err)
	}
	mustExec(t, pool, `INSERT INTO products (supplier_id, product_id, status, revision, name, price_minor, currency, payload_hash)
		VALUES ('SUP-001', 'SKU-1', 'ACTIVE', 1, 'Lamp', 100, 'USD', 'hash')`)
	mustExec(t, pool, `INSERT INTO sync_batches (job_id, batch_id, seq, cursor_in, cursor_out, has_more, payload_hash,
		                                     record_count, created_count)
		VALUES ($1::uuid, 'BATCH-001', 1, '', '', false, 'hash', 1, 1)`, jobID)
	mustExec(t, pool, `INSERT INTO sync_changes (job_id, batch_id, supplier_id, product_id, revision, change_type, before, after)
		VALUES ($1::uuid, 'BATCH-001', 'SUP-001', 'SKU-1', 1, 'CREATED', NULL, '{"status":"ACTIVE","revision":1}')`, jobID)
	return jobID
}

func TestSnapshotDetectsContentAndRowCountChanges(t *testing.T) {
	pool := DB(t)
	Reset(t, pool)
	jobID := seedCompletedJob(t, pool)

	before := Snapshot(t, pool)
	if len(before) != len(allTables) {
		t.Fatalf("snapshot covers %d tables, want %d", len(before), len(allTables))
	}
	AssertUnchanged(t, before, Snapshot(t, pool))

	mustExec(t, pool, `UPDATE sync_jobs SET last_error = 'boom' WHERE id = $1::uuid`, jobID)
	afterEdit := Snapshot(t, pool)
	failures := failuresOf(func(tb testing.TB) { AssertUnchanged(tb, before, afterEdit) })
	requireFailure(t, failures, "table sync_jobs changed")
	if len(failures) != 1 {
		t.Fatalf("only sync_jobs should differ, got %q", failures)
	}

	mustExec(t, pool, `INSERT INTO sync_issues (job_id, code, severity, message, fingerprint)
		VALUES ($1::uuid, 'SUPPLIER_UNAVAILABLE', 'TRANSIENT', 'down', 'fp')`, jobID)
	requireFailure(t, failuresOf(func(tb testing.TB) { AssertUnchanged(tb, afterEdit, Snapshot(tb, pool)) }), "table sync_issues changed")
}

func TestSnapshotExceptIgnoresNamedTables(t *testing.T) {
	pool := DB(t)
	Reset(t, pool)
	jobID := seedCompletedJob(t, pool)

	before := SnapshotExcept(t, pool, "sync_jobs", "sync_issues")
	if _, ok := before["sync_jobs"]; ok {
		t.Fatal("sync_jobs must be excluded")
	}
	mustExec(t, pool, `UPDATE sync_jobs SET last_error = 'boom' WHERE id = $1::uuid`, jobID)
	mustExec(t, pool, `INSERT INTO sync_issues (job_id, code, severity, message, fingerprint)
		VALUES ($1::uuid, 'INVALID_BATCH', 'BLOCKING', 'bad', 'fp')`, jobID)
	AssertUnchanged(t, before, SnapshotExcept(t, pool, "sync_jobs", "sync_issues"))

	requireFailure(t, failuresOf(func(tb testing.TB) { AssertUnchanged(tb, before, Snapshot(tb, pool)) }), "only one of the two snapshots")
	requireFailure(t, failuresOf(func(tb testing.TB) { SnapshotExcept(tb, pool, "no_such_table") }), "unknown table")
}

func TestAssertCountersComparesJobBatchesAndChanges(t *testing.T) {
	pool := DB(t)
	Reset(t, pool)
	jobID := seedCompletedJob(t, pool)

	AssertCounters(t, pool, jobID)

	mustExec(t, pool, `UPDATE sync_jobs SET duplicate_count = 1 WHERE id = $1::uuid`, jobID)
	requireFailure(t, failuresOf(func(tb testing.TB) { AssertCounters(tb, pool, jobID) }), "duplicate_count = 1 but sum of batch duplicate_count = 0")

	mustExec(t, pool, `UPDATE sync_jobs SET duplicate_count = 0, created_count = 2 WHERE id = $1::uuid`, jobID)
	failures := failuresOf(func(tb testing.TB) { AssertCounters(tb, pool, jobID) })
	requireFailure(t, failures, "sum of batch created_count")
	requireFailure(t, failures, "CREATED change rows")

	requireFailure(t, failuresOf(func(tb testing.TB) { AssertCounters(tb, pool, "00000000-0000-0000-0000-000000000000") }), "read counters")
}

func TestAssertCompletedConsistentRejectsFalseSuccess(t *testing.T) {
	pool := DB(t)

	t.Run("consistent_job_passes", func(t *testing.T) {
		Reset(t, pool)
		AssertCompletedConsistent(t, pool, seedCompletedJob(t, pool))
	})

	cases := []struct {
		name, mutation, want string
	}{
		{"open_blocking_issue", `INSERT INTO sync_issues (job_id, code, severity, message, fingerprint)
			VALUES ($1::uuid, 'VERSION_CONFLICT', 'BLOCKING', 'conflict', 'fp')`, "blocking issues are still open"},
		{"last_batch_not_final", `UPDATE sync_batches SET has_more = true WHERE job_id = $1::uuid`, "still said has_more"},
		{"batch_rows_missing", `UPDATE sync_jobs SET processed_batches = 2, total_batches = NULL WHERE id = $1::uuid`, "batch rows are committed"},
		{"not_completed", `UPDATE sync_jobs SET status = 'INTERRUPTED' WHERE id = $1::uuid`, "want COMPLETED"},
		{"end_of_feed_unseen", `UPDATE sync_jobs SET status = 'INTERRUPTED', end_of_feed_seen = false WHERE id = $1::uuid`, "end_of_feed_seen is false"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			Reset(t, pool)
			jobID := seedCompletedJob(t, pool)
			mustExec(t, pool, tc.mutation, jobID)
			requireFailure(t, failuresOf(func(tb testing.TB) { AssertCompletedConsistent(tb, pool, jobID) }), tc.want)
		})
	}
}

func TestAssertProductMatchesStoredStatusAndRevision(t *testing.T) {
	pool := DB(t)
	Reset(t, pool)
	seedCompletedJob(t, pool)

	AssertProduct(t, pool, "SUP-001", "SKU-1", catalog.StatusActive, 1)
	requireFailure(t, failuresOf(func(tb testing.TB) {
		AssertProduct(tb, pool, "SUP-001", "SKU-1", catalog.StatusWithdrawn, 2)
	}), "= ACTIVE r1, want WITHDRAWN r2")
	requireFailure(t, failuresOf(func(tb testing.TB) {
		AssertProduct(tb, pool, "SUP-001", "SKU-404", catalog.StatusActive, 1)
	}), "does not exist")
}

func TestFakeSupplierServesPagesAndRecordsCalls(t *testing.T) {
	ctx := context.Background()
	f := NewFakeSupplier()
	f.SetUpdate(Info("U1", 2))
	f.AddPage("", Page("U1", "BATCH-001", "cursor-002", true, Active("SKU-1", 1, "Lamp", 100)))
	f.AddPage("cursor-002", Page("U1", "BATCH-002", "", false, Withdrawn("SKU-1", 2)))

	info, err := f.GetUpdate(ctx, "U1")
	if err != nil || info.TotalBatches == nil || *info.TotalBatches != 2 {
		t.Fatalf("GetUpdate = %+v, %v", info, err)
	}
	first, err := f.FetchBatch(ctx, "U1", "")
	if err != nil || first.BatchID != "BATCH-001" || first.HasMore == nil || !*first.HasMore || len(first.Products) != 1 {
		t.Fatalf("first page = %+v, %v", first, err)
	}
	last, err := f.FetchBatch(ctx, "U1", "cursor-002")
	if err != nil || last.BatchID != "BATCH-002" || *last.HasMore || last.Products[0].Status != catalog.StatusWithdrawn {
		t.Fatalf("last page = %+v, %v", last, err)
	}

	if _, err := f.FetchBatch(ctx, "U1", "cursor-9"); !errors.Is(err, supplier.ErrNotFound) {
		t.Fatalf("unknown cursor: want ErrNotFound, got %v", err)
	}
	if _, err := f.GetUpdate(ctx, "U2"); !errors.Is(err, supplier.ErrNotFound) {
		t.Fatalf("unknown update: want ErrNotFound, got %v", err)
	}
	if got := Info("U3", 0); got.TotalBatches != nil {
		t.Fatalf("Info with no total must leave TotalBatches nil, got %d", *got.TotalBatches)
	}

	want := []Call{
		{OpGetUpdate, ""}, {OpFetchBatch, ""}, {OpFetchBatch, "cursor-002"}, {OpFetchBatch, "cursor-9"}, {OpGetUpdate, ""},
	}
	if got := f.Calls(); !slices.Equal(got, want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
	f.Calls()[0].Op = "tampered"
	if f.Calls()[0].Op != OpGetUpdate {
		t.Fatal("Calls must return a copy")
	}
}

func TestFakeSupplierNeverSharesBatchesWithCallers(t *testing.T) {
	ctx := context.Background()
	f := NewFakeSupplier()
	stored := Page("U1", "BATCH-001", "", false, Active("SKU-1", 1, "Lamp", 100))
	f.AddPage("", stored)

	*stored.Products[0].Name = "changed after AddPage"
	got, _ := f.FetchBatch(ctx, "U1", "")
	if *got.Products[0].Name != "Lamp" {
		t.Fatalf("mutating the stored batch leaked into the fake: %q", *got.Products[0].Name)
	}

	*got.Products[0].PriceMinor = 1
	*got.HasMore = true
	got.Products = nil
	again, _ := f.FetchBatch(ctx, "U1", "")
	if *again.Products[0].PriceMinor != 100 || *again.HasMore || len(again.Products) != 1 {
		t.Fatalf("mutating a served batch leaked into the fake: %+v", again)
	}
}

func TestFakeSupplierFailTimesCountsDownThenServes(t *testing.T) {
	ctx := context.Background()
	f := NewFakeSupplier()
	f.AddPage("cursor-003", Page("U1", "BATCH-003", "", false))

	f.FailTimes("cursor-003", 2)
	for i := 0; i < 2; i++ {
		if _, err := f.FetchBatch(ctx, "U1", "cursor-003"); !errors.Is(err, supplier.ErrUnavailable) {
			t.Fatalf("call %d: want ErrUnavailable, got %v", i+1, err)
		}
	}
	if _, err := f.FetchBatch(ctx, "U1", "cursor-003"); err != nil {
		t.Fatalf("third call must succeed: %v", err)
	}

	f.FailTimes("cursor-003", -1)
	for i := 0; i < 5; i++ {
		if _, err := f.FetchBatch(ctx, "U1", "cursor-003"); !errors.Is(err, supplier.ErrUnavailable) {
			t.Fatalf("permanent failure, call %d: got %v", i+1, err)
		}
	}
	f.FailTimes("cursor-003", 0)
	if _, err := f.FetchBatch(ctx, "U1", "cursor-003"); err != nil {
		t.Fatalf("after clearing the failure: %v", err)
	}
	if got := f.FetchCalls("cursor-003"); got != 9 {
		t.Fatalf("FetchCalls = %d, want 9 (failed attempts count)", got)
	}

	f.SetUpdate(Info("U1", 1))
	f.FailUpdateTimes(1)
	if _, err := f.GetUpdate(ctx, "U1"); !errors.Is(err, supplier.ErrUnavailable) {
		t.Fatalf("first GetUpdate: want ErrUnavailable, got %v", err)
	}
	if _, err := f.GetUpdate(ctx, "U1"); err != nil {
		t.Fatalf("second GetUpdate: %v", err)
	}
	f.UpdateNotFound()
	if _, err := f.GetUpdate(ctx, "U1"); !errors.Is(err, supplier.ErrNotFound) {
		t.Fatalf("after UpdateNotFound: got %v", err)
	}
}

func TestFakeSupplierNotFoundHidesPageUntilReadded(t *testing.T) {
	ctx := context.Background()
	f := NewFakeSupplier()
	f.AddPage("cursor-002", Page("U1", "BATCH-002", "", false))
	f.NotFound("cursor-002")
	if _, err := f.FetchBatch(ctx, "U1", "cursor-002"); !errors.Is(err, supplier.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	f.AddPage("cursor-002", Page("U1", "BATCH-002", "", false))
	if _, err := f.FetchBatch(ctx, "U1", "cursor-002"); err != nil {
		t.Fatalf("a newly added page must be served: %v", err)
	}
}

func TestFakeSupplierBlockWaitsForContextOrUnblock(t *testing.T) {
	f := NewFakeSupplier()
	f.AddPage("cursor-002", Page("U1", "BATCH-002", "", false))
	f.Block("cursor-002")

	// A blocked fetch must wait for its context: with the block in place the cancellation
	// is what ends it, and without the block it would have returned the page instead.
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := f.FetchBatch(ctx, "U1", "cursor-002")
		errCh <- err
	}()
	waitForFetches(t, f, "cursor-002", 1)
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked fetch: want context.Canceled, got %v", err)
	}

	timeout, stop := context.WithTimeout(context.Background(), time.Millisecond)
	defer stop()
	if _, err := f.FetchBatch(timeout, "U1", "cursor-002"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked fetch with deadline: want DeadlineExceeded, got %v", err)
	}

	// Unblock releases a waiting call, which then gets the page.
	type result struct {
		batch supplier.Batch
		err   error
	}
	resCh := make(chan result, 1)
	go func() {
		b, err := f.FetchBatch(context.Background(), "U1", "cursor-002")
		resCh <- result{b, err}
	}()
	waitForFetches(t, f, "cursor-002", 3)
	f.Unblock("cursor-002")
	if res := <-resCh; res.err != nil || res.batch.BatchID != "BATCH-002" {
		t.Fatalf("released fetch = %+v, %v", res.batch, res.err)
	}
	if _, err := f.FetchBatch(context.Background(), "U1", "cursor-002"); err != nil {
		t.Fatalf("fetch after Unblock must not wait: %v", err)
	}
}

func TestFakeSupplierIsSafeForConcurrentUse(t *testing.T) {
	const workers = 50
	f := NewFakeSupplier()
	f.AddPage("", Page("U1", "BATCH-001", "", false, Active("SKU-1", 1, "Lamp", 100)))

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.AddPage("cursor-x", Page("U1", "BATCH-X", "", false))
			if _, err := f.FetchBatch(context.Background(), "U1", ""); err != nil {
				t.Errorf("FetchBatch: %v", err)
			}
		}()
	}
	wg.Wait()
	if got := f.FetchCalls(""); got != workers {
		t.Fatalf("FetchCalls = %d, want %d", got, workers)
	}
}

// waitForFetches returns once the fake has logged n fetches at cursor. A fetch is logged
// before it starts waiting, so this tells the test the goroutine has reached the supplier
// without sleeping a guessed duration.
func waitForFetches(t *testing.T, f *FakeSupplier, cursor string, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for f.FetchCalls(cursor) < n {
		if time.Now().After(deadline) {
			t.Fatalf("fetch at %q not seen %d times", cursor, n)
		}
		runtime.Gosched()
	}
}
