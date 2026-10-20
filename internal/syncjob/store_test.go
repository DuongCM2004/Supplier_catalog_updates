package syncjob

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/testutil"
)

const storeTestSupplier = "SUP-001"

// storeTestPool returns a pool on a freshly emptied test database.
func storeTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testutil.DB(t)
	testutil.Reset(t, pool)
	return pool
}

// storeSeedJob inserts a job directly in the given state, bypassing the state machine, so
// tests can start from any state.
func storeSeedJob(t *testing.T, pool *pgxpool.Pool, updateID string, status Status) string {
	t.Helper()
	const q = `INSERT INTO sync_jobs (supplier_id, supplier_update_id, status, end_of_feed_seen, completed_at)
		VALUES ($1, $2, $3::text, $3::text = 'COMPLETED', CASE WHEN $3::text = 'COMPLETED' THEN now() END)
		RETURNING id::text`
	var id string
	if err := pool.QueryRow(context.Background(), q, storeTestSupplier, updateID, string(status)).Scan(&id); err != nil {
		t.Fatalf("seed job %s as %s: %v", updateID, status, err)
	}
	return id
}

func storeMustCreate(t *testing.T, pool *pgxpool.Pool, updateID string) Job {
	t.Helper()
	job, created, err := createJob(context.Background(), pool, storeTestSupplier, updateID)
	if err != nil || !created {
		t.Fatalf("createJob(%s) = created %v, err %v", updateID, created, err)
	}
	return job
}

func storeCount(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return n
}

func TestTransitionTableAllowsOnlyDocumentedEdges(t *testing.T) {
	all := []Status{StatusPending, StatusRunning, StatusInterrupted, StatusNeedsAttention, StatusCompleted, StatusFailed}
	// Written out independently of the production table on purpose: the test is the oracle.
	allowed := map[[2]Status]bool{
		{StatusPending, StatusRunning}:        true,
		{StatusPending, StatusInterrupted}:    true,
		{StatusRunning, StatusRunning}:        true, // orphan adoption by resume
		{StatusRunning, StatusCompleted}:      true,
		{StatusRunning, StatusNeedsAttention}: true,
		{StatusRunning, StatusInterrupted}:    true,
		{StatusRunning, StatusFailed}:         true,
		{StatusInterrupted, StatusRunning}:    true,
		{StatusNeedsAttention, StatusRunning}: true,
	}
	for _, from := range all {
		for _, to := range all {
			if got, want := canTransition(from, to), allowed[[2]Status{from, to}]; got != want {
				t.Errorf("canTransition(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}
	for _, terminal := range []Status{StatusCompleted, StatusFailed} {
		for _, to := range all {
			if canTransition(terminal, to) {
				t.Errorf("terminal state %s must not move to %s", terminal, to)
			}
		}
	}
	if got, want := sourcesOf(StatusInterrupted), []Status{StatusPending, StatusRunning}; !slices.Equal(got, want) {
		t.Errorf("sourcesOf(INTERRUPTED) = %v, want %v", got, want)
	}
}

func TestCreateJobForSameUpdateReturnsExistingJob(t *testing.T) {
	pool := storeTestPool(t)
	ctx := context.Background()

	first := storeMustCreate(t, pool, "UPDATE-1")
	if first.Status != StatusPending || first.SupplierID != storeTestSupplier || first.UpdateID != "UPDATE-1" ||
		first.NextCursor != "" || first.ProcessedBatches != 0 || first.TotalBatches != nil || first.OpenBlockingIssues != 0 {
		t.Fatalf("new job = %+v", first)
	}

	again, created, err := createJob(ctx, pool, storeTestSupplier, "UPDATE-1")
	if err != nil || created || again.ID != first.ID {
		t.Fatalf("second create = id %s created %v err %v, want id %s created false", again.ID, created, err, first.ID)
	}

	// Still the same job once it has finished: the update is never started a second time.
	if _, err := pool.Exec(ctx, `UPDATE sync_jobs SET status = 'COMPLETED', end_of_feed_seen = true, completed_at = now()`); err != nil {
		t.Fatal(err)
	}
	done, created, err := createJob(ctx, pool, storeTestSupplier, "UPDATE-1")
	if err != nil || created || done.ID != first.ID || done.Status != StatusCompleted {
		t.Fatalf("create after completion = %+v created %v err %v", done, created, err)
	}
	if n := storeCount(t, pool, `SELECT count(*) FROM sync_jobs`); n != 1 {
		t.Fatalf("sync_jobs rows = %d, want 1", n)
	}
}

func TestCreateJobRejectsSecondActiveJobAndNamesTheActiveOne(t *testing.T) {
	pool := storeTestPool(t)
	ctx := context.Background()
	active := storeMustCreate(t, pool, "UPDATE-1")

	got, created, err := createJob(ctx, pool, storeTestSupplier, "UPDATE-2")
	if !errors.Is(err, ErrActiveJobExists) || created {
		t.Fatalf("second active job: created %v err %v, want ErrActiveJobExists", created, err)
	}
	if got.ID != active.ID {
		t.Fatalf("returned job %q, want the active job %q", got.ID, active.ID)
	}
	if n := storeCount(t, pool, `SELECT count(*) FROM sync_jobs WHERE supplier_update_id = 'UPDATE-2'`); n != 0 {
		t.Fatalf("rejected update left %d job rows", n)
	}

	// Once the active job stops being PENDING/RUNNING the slot is free again.
	if err := transition(ctx, pool, active.ID, nil, StatusInterrupted, nil); err != nil {
		t.Fatal(err)
	}
	if _, created, err := createJob(ctx, pool, storeTestSupplier, "UPDATE-2"); err != nil || !created {
		t.Fatalf("create after slot freed: created %v err %v", created, err)
	}
}

func TestConcurrentCreateOfSameUpdateYieldsOneJob(t *testing.T) {
	pool := storeTestPool(t)
	ctx := context.Background()
	const starters, rounds = 4, 25

	for round := 0; round < rounds; round++ {
		testutil.Reset(t, pool)
		type result struct {
			job     Job
			created bool
			err     error
		}
		results := make([]result, starters)
		var ready, done sync.WaitGroup
		release := make(chan struct{})
		ready.Add(starters)
		done.Add(starters)
		for i := range results {
			go func() {
				defer done.Done()
				ready.Done()
				<-release // every starter hits the database at the same moment
				job, created, err := createJob(ctx, pool, storeTestSupplier, "UPDATE-RACE")
				results[i] = result{job, created, err}
			}()
		}
		ready.Wait()
		close(release)
		done.Wait()

		createdCount := 0
		for i, r := range results {
			if r.err != nil {
				t.Fatalf("round %d starter %d: %v", round, i, r.err)
			}
			if r.job.ID != results[0].job.ID {
				t.Fatalf("round %d: starters got different jobs %q and %q", round, results[0].job.ID, r.job.ID)
			}
			if r.created {
				createdCount++
			}
		}
		if createdCount != 1 {
			t.Fatalf("round %d: %d starters reported created=true, want exactly 1", round, createdCount)
		}
		if n := storeCount(t, pool, `SELECT count(*) FROM sync_jobs`); n != 1 {
			t.Fatalf("round %d: sync_jobs rows = %d, want 1", round, n)
		}
	}
}

func TestTransitionRejectsEdgesOutsideTheStateMachine(t *testing.T) {
	pool := storeTestPool(t)
	ctx := context.Background()
	completed := storeSeedJob(t, pool, "UPDATE-DONE", StatusCompleted)

	err := transition(ctx, pool, completed, nil, StatusRunning, nil)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("COMPLETED -> RUNNING err = %v, want ErrInvalidTransition", err)
	}
	// A caller cannot widen the table by listing a forbidden source state.
	err = transition(ctx, pool, completed, []Status{StatusCompleted, StatusInterrupted}, StatusRunning, nil)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("COMPLETED listed as source err = %v, want ErrInvalidTransition", err)
	}
	if job, err := GetJob(ctx, pool, completed); err != nil || job.Status != StatusCompleted {
		t.Fatalf("job after rejected transitions = %+v err %v, want unchanged COMPLETED", job, err)
	}

	// Allowed edge, but the job is not in the state the caller expected.
	running := storeSeedJob(t, pool, "UPDATE-RUN", StatusRunning)
	err = transition(ctx, pool, running, []Status{StatusInterrupted}, StatusRunning, nil)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected-state mismatch err = %v, want ErrInvalidTransition", err)
	}
	err = transition(ctx, pool, running, []Status{}, StatusInterrupted, nil)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("empty source list err = %v, want ErrInvalidTransition", err)
	}

	for _, id := range []string{"00000000-0000-4000-8000-000000000000", "not-a-uuid", ""} {
		if err := transition(ctx, pool, id, nil, StatusRunning, nil); !errors.Is(err, ErrNotFound) {
			t.Errorf("transition(%q) err = %v, want ErrNotFound", id, err)
		}
	}
}

func TestTransitionStampsTimestampsAndLastError(t *testing.T) {
	pool := storeTestPool(t)
	ctx := context.Background()
	job := storeMustCreate(t, pool, "UPDATE-1")
	if job.StartedAt != nil || job.CompletedAt != nil {
		t.Fatalf("new job already has timestamps: %+v", job)
	}

	if err := transition(ctx, pool, job.ID, []Status{StatusPending}, StatusRunning, nil); err != nil {
		t.Fatal(err)
	}
	running, _ := GetJob(ctx, pool, job.ID)
	if running.Status != StatusRunning || running.StartedAt == nil || running.LastError != nil || !running.UpdatedAt.After(job.UpdatedAt) {
		t.Fatalf("after start: %+v", running)
	}

	msg := "supplier unavailable at cursor-003 after 4 attempts"
	if err := transition(ctx, pool, job.ID, []Status{StatusRunning}, StatusInterrupted, &msg); err != nil {
		t.Fatal(err)
	}
	interrupted, _ := GetJob(ctx, pool, job.ID)
	if interrupted.Status != StatusInterrupted || interrupted.LastError == nil || *interrupted.LastError != msg {
		t.Fatalf("after interrupt: %+v", interrupted)
	}

	// Resuming keeps the original start time and drops the error of the previous attempt.
	if err := transition(ctx, pool, job.ID, nil, StatusRunning, nil); err != nil {
		t.Fatal(err)
	}
	resumed, _ := GetJob(ctx, pool, job.ID)
	if resumed.StartedAt == nil || !resumed.StartedAt.Equal(*running.StartedAt) || resumed.LastError != nil {
		t.Fatalf("after resume: started_at %v (was %v), last_error %v", resumed.StartedAt, running.StartedAt, resumed.LastError)
	}

	if _, err := pool.Exec(ctx, `UPDATE sync_jobs SET end_of_feed_seen = true WHERE id = $1::uuid`, job.ID); err != nil {
		t.Fatal(err)
	}
	if err := transition(ctx, pool, job.ID, []Status{StatusRunning}, StatusCompleted, nil); err != nil {
		t.Fatal(err)
	}
	completed, _ := GetJob(ctx, pool, job.ID)
	if completed.Status != StatusCompleted || completed.CompletedAt == nil {
		t.Fatalf("after completion: %+v", completed)
	}
}

func TestGetJobReportsOpenBlockingIssuesAndUnknownIds(t *testing.T) {
	pool := storeTestPool(t)
	ctx := context.Background()
	job := storeMustCreate(t, pool, "UPDATE-1")

	const seed = `INSERT INTO sync_issues (job_id, cursor, code, severity, status, message, fingerprint, resolved_at)
		VALUES ($1::uuid, $2, $3, $4, $5, 'seeded', $6, CASE WHEN $5::text = 'RESOLVED' THEN now() END)`
	rows := []struct{ cursor, code, severity, status string }{
		{"c1", "VERSION_CONFLICT", "BLOCKING", "OPEN"},
		{"c2", "INVALID_BATCH", "BLOCKING", "RESOLVED"},
		{"c3", "SUPPLIER_UNAVAILABLE", "TRANSIENT", "OPEN"},
	}
	for _, r := range rows {
		if _, err := pool.Exec(ctx, seed, job.ID, r.cursor, r.code, r.severity, r.status, r.code+r.cursor); err != nil {
			t.Fatal(err)
		}
	}
	got, err := GetJob(ctx, pool, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.OpenBlockingIssues != 1 {
		t.Fatalf("OpenBlockingIssues = %d, want 1 (resolved and transient issues do not count)", got.OpenBlockingIssues)
	}

	for _, id := range []string{"00000000-0000-4000-8000-000000000000", "not-a-uuid", "", job.ID + "'; DROP TABLE sync_jobs;--"} {
		if _, err := GetJob(ctx, pool, id); !errors.Is(err, ErrNotFound) {
			t.Errorf("GetJob(%q) err = %v, want ErrNotFound", id, err)
		}
	}
}

func TestLockJobHoldsTheRowUntilTheTransactionEnds(t *testing.T) {
	pool := storeTestPool(t)
	ctx := context.Background()
	job := storeMustCreate(t, pool, "UPDATE-1")

	// tryLock asks for the same row lock from another connection without waiting for it.
	tryLock := func() error {
		return pgx.BeginFunc(ctx, pool, func(other pgx.Tx) error {
			_, err := other.Exec(ctx, `SELECT 1 FROM sync_jobs WHERE id = $1::uuid FOR UPDATE NOWAIT`, job.ID)
			return err
		})
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // after Commit this is a harmless no-op
	locked, err := lockJob(ctx, tx, job.ID)
	if err != nil || locked.ID != job.ID || locked.Status != StatusPending {
		t.Fatalf("lockJob = %+v, %v", locked, err)
	}
	var pgErr *pgconn.PgError
	if err := tryLock(); !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("second writer err = %v, want lock_not_available while the first holds the job", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tryLock(); err != nil {
		t.Fatalf("lock after the first transaction ended: %v", err)
	}
}

func TestLockJobOfUnknownIdIsNotFound(t *testing.T) {
	pool := storeTestPool(t)
	ctx := context.Background()
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		for _, id := range []string{"00000000-0000-4000-8000-000000000000", "not-a-uuid", ""} {
			if _, err := lockJob(ctx, tx, id); !errors.Is(err, ErrNotFound) {
				t.Errorf("lockJob(%q) err = %v, want ErrNotFound", id, err)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestReconcileInterruptsOnlyJobsThatWereActive(t *testing.T) {
	pool := storeTestPool(t)
	ctx := context.Background()

	if ids, err := reconcile(ctx, pool, "restart"); err != nil || len(ids) != 0 {
		t.Fatalf("reconcile on empty table = %v, %v", ids, err)
	}

	quiet := map[string]Status{
		storeSeedJob(t, pool, "U-DONE", StatusCompleted):           StatusCompleted,
		storeSeedJob(t, pool, "U-FAILED", StatusFailed):            StatusFailed,
		storeSeedJob(t, pool, "U-ATTENTION", StatusNeedsAttention): StatusNeedsAttention,
		storeSeedJob(t, pool, "U-INTERRUPTED", StatusInterrupted):  StatusInterrupted,
	}
	// Only one job can be PENDING or RUNNING at a time, so cover each state in turn.
	for _, active := range []Status{StatusPending, StatusRunning} {
		id := storeSeedJob(t, pool, "U-ACTIVE-"+string(active), active)
		ids, err := reconcile(ctx, pool, "process restarted while job was "+string(active))
		if err != nil || !slices.Equal(ids, []string{id}) {
			t.Fatalf("reconcile %s = %v, %v, want [%s]", active, ids, err, id)
		}
		job, _ := GetJob(ctx, pool, id)
		if job.Status != StatusInterrupted || job.LastError == nil || *job.LastError != "process restarted while job was "+string(active) {
			t.Fatalf("reconciled %s job = %+v", active, job)
		}
	}
	for id, want := range quiet {
		if job, _ := GetJob(ctx, pool, id); job.Status != want || job.LastError != nil {
			t.Errorf("job %s = %s (last_error %v), want untouched %s", id, job.Status, job.LastError, want)
		}
	}
	if ids, err := reconcile(ctx, pool, "again"); err != nil || len(ids) != 0 {
		t.Fatalf("second reconcile = %v, %v, want nothing left to do", ids, err)
	}
}

type storeIssueRow struct {
	Cursor, Code, Severity, Status, Message string
	BatchID, ProductID, SupplierID          *string
	RetryCount                              int
	Resolved                                bool
}

func storeIssueRows(t *testing.T, pool *pgxpool.Pool, jobID string) []storeIssueRow {
	t.Helper()
	const q = `SELECT cursor, code, severity, status, message, batch_id, product_id, supplier_id, retry_count, resolved_at IS NOT NULL
		FROM sync_issues WHERE job_id = $1::uuid ORDER BY first_seen_at, id`
	rows, err := pool.Query(context.Background(), q, jobID)
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (storeIssueRow, error) {
		var x storeIssueRow
		err := r.Scan(&x.Cursor, &x.Code, &x.Severity, &x.Status, &x.Message, &x.BatchID, &x.ProductID, &x.SupplierID, &x.RetryCount, &x.Resolved)
		return x, err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func storeIssue(jobID string, code IssueCode, severity Severity, cursor string) Issue {
	return Issue{JobID: jobID, Cursor: &cursor, Code: code, Severity: severity, Message: string(code) + " at " + cursor}
}

// storeInJobTx runs fn the way production writers do: job row lock first, then the writes.
func storeInJobTx(t *testing.T, pool *pgxpool.Pool, jobID string, fn func(ctx context.Context, tx pgx.Tx) error) {
	t.Helper()
	ctx := context.Background()
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := lockJob(ctx, tx, jobID); err != nil {
			return err
		}
		return fn(ctx, tx)
	})
	if err != nil {
		t.Fatalf("job transaction: %v", err)
	}
}

func storeRecord(t *testing.T, pool *pgxpool.Pool, iss Issue) {
	t.Helper()
	storeInJobTx(t, pool, iss.JobID, func(ctx context.Context, tx pgx.Tx) error { return recordIssue(ctx, tx, iss) })
}

func TestFingerprintIsStablePerCodeAndCursor(t *testing.T) {
	a := fingerprint(CodeVersionConflict, "cursor-002")
	if len(a) != 64 || a != fingerprint(CodeVersionConflict, "cursor-002") {
		t.Fatalf("fingerprint %q is not a stable sha256 hex", a)
	}
	if a == fingerprint(CodeVersionConflict, "cursor-003") || a == fingerprint(CodeInvalidBatch, "cursor-002") {
		t.Fatal("fingerprint must change with the code and with the cursor")
	}
}

func TestRecordIssueTwiceKeepsOneOpenRowAndCountsTheRecurrence(t *testing.T) {
	pool := storeTestPool(t)
	job := storeMustCreate(t, pool, "UPDATE-1")

	first := storeIssue(job.ID, CodeVersionConflict, SeverityBlocking, "cursor-002")
	storeRecord(t, pool, first)
	second := first
	second.Message = "still conflicting"
	storeRecord(t, pool, second)

	rows := storeIssueRows(t, pool, job.ID)
	if len(rows) != 1 {
		t.Fatalf("issue rows = %d, want 1: %+v", len(rows), rows)
	}
	if rows[0].RetryCount != 1 || rows[0].Message != "still conflicting" || rows[0].Status != "OPEN" {
		t.Fatalf("row = %+v, want retry_count 1, latest message, OPEN", rows[0])
	}

	// A different cursor or a different code is a different problem.
	storeRecord(t, pool, storeIssue(job.ID, CodeVersionConflict, SeverityBlocking, "cursor-003"))
	storeRecord(t, pool, storeIssue(job.ID, CodeInvalidBatch, SeverityBlocking, "cursor-002"))
	if n := len(storeIssueRows(t, pool, job.ID)); n != 3 {
		t.Fatalf("issue rows = %d, want 3 distinct problems", n)
	}
}

func TestIssueResolvedThenSeenAgainOpensNewRow(t *testing.T) {
	pool := storeTestPool(t)
	job := storeMustCreate(t, pool, "UPDATE-1")
	iss := storeIssue(job.ID, CodeInvalidProductData, SeverityBlocking, "cursor-002")

	storeRecord(t, pool, iss)
	storeInJobTx(t, pool, job.ID, func(ctx context.Context, tx pgx.Tx) error { return resolveIssues(ctx, tx, job.ID, "cursor-002") })
	storeRecord(t, pool, iss)

	rows := storeIssueRows(t, pool, job.ID)
	if len(rows) != 2 {
		t.Fatalf("issue rows = %d, want the resolved one plus a new open one: %+v", len(rows), rows)
	}
	if rows[0].Status != "RESOLVED" || !rows[0].Resolved || rows[1].Status != "OPEN" || rows[1].RetryCount != 0 {
		t.Fatalf("rows = %+v, want [RESOLVED with resolved_at, OPEN with retry_count 0]", rows)
	}
}

func TestRecordIssueRefreshesWhatDescribesTheLatestOccurrence(t *testing.T) {
	pool := storeTestPool(t)
	job := storeMustCreate(t, pool, "UPDATE-1")
	str := func(s string) *string { return &s }

	iss := storeIssue(job.ID, CodeVersionConflict, SeverityBlocking, "cursor-002")
	iss.BatchID, iss.SupplierID, iss.ProductID = str("BATCH-002"), str("SUP-001"), str("SKU-101")
	storeRecord(t, pool, iss)
	iss.BatchID, iss.SupplierID, iss.ProductID = str("BATCH-002b"), str("SUP-002"), str("SKU-202")
	storeRecord(t, pool, iss)

	rows := storeIssueRows(t, pool, job.ID)
	if len(rows) != 1 {
		t.Fatalf("issue rows = %d, want 1", len(rows))
	}
	r := rows[0]
	if r.ProductID == nil || *r.ProductID != "SKU-202" || r.BatchID == nil || *r.BatchID != "BATCH-002b" ||
		r.SupplierID == nil || *r.SupplierID != "SUP-002" {
		t.Fatalf("row = product %v batch %v supplier %v, want the second occurrence", r.ProductID, r.BatchID, r.SupplierID)
	}
}

func TestRecordIssueSeverityOnlyEscalates(t *testing.T) {
	pool := storeTestPool(t)
	job := storeMustCreate(t, pool, "UPDATE-1")

	storeRecord(t, pool, storeIssue(job.ID, CodeDatabaseError, SeverityTransient, "cursor-002"))
	storeRecord(t, pool, storeIssue(job.ID, CodeDatabaseError, SeverityBlocking, "cursor-002"))
	storeRecord(t, pool, storeIssue(job.ID, CodeDatabaseError, SeverityTransient, "cursor-002"))

	rows := storeIssueRows(t, pool, job.ID)
	if len(rows) != 1 || rows[0].Severity != "BLOCKING" || rows[0].RetryCount != 2 {
		t.Fatalf("rows = %+v, want one BLOCKING row seen 3 times", rows)
	}
	if n, err := countOpenBlocking(context.Background(), pool, job.ID); err != nil || n != 1 {
		t.Fatalf("countOpenBlocking = %d, %v, want 1", n, err)
	}
}

func TestRecordIssueWithoutCursorIsRejected(t *testing.T) {
	pool := storeTestPool(t)
	job := storeMustCreate(t, pool, "UPDATE-1")
	ctx := context.Background()

	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		return recordIssue(ctx, tx, Issue{JobID: job.ID, Code: CodeInvalidBatch, Severity: SeverityBlocking, Message: "no cursor"})
	})
	if err == nil {
		t.Fatal("an issue without a cursor could never resolve; recordIssue must refuse it")
	}
	if n := len(storeIssueRows(t, pool, job.ID)); n != 0 {
		t.Fatalf("issue rows = %d, want 0", n)
	}
}

func TestResolveIssuesClosesTransientAndOnlyThatCursorsBlockingIssues(t *testing.T) {
	pool := storeTestPool(t)
	ctx := context.Background()
	job := storeMustCreate(t, pool, "UPDATE-1")

	storeRecord(t, pool, storeIssue(job.ID, CodeSupplierUnavailable, SeverityTransient, "cursor-003"))
	storeRecord(t, pool, storeIssue(job.ID, CodeVersionConflict, SeverityBlocking, "cursor-002"))
	storeRecord(t, pool, storeIssue(job.ID, CodeInvalidBatch, SeverityBlocking, "cursor-004"))

	storeInJobTx(t, pool, job.ID, func(ctx context.Context, tx pgx.Tx) error {
		if n, err := countOpenBlocking(ctx, tx, job.ID); err != nil || n != 2 {
			t.Errorf("open blocking before = %d, %v, want 2", n, err)
		}
		return resolveIssues(ctx, tx, job.ID, "cursor-002")
	})

	open := map[string]bool{}
	for _, r := range storeIssueRows(t, pool, job.ID) {
		open[r.Code] = r.Status == "OPEN"
	}
	if open["SUPPLIER_UNAVAILABLE"] || open["VERSION_CONFLICT"] || !open["INVALID_BATCH"] {
		t.Fatalf("open after resolving cursor-002 = %v, want only INVALID_BATCH (cursor-004) open", open)
	}
	if n, err := countOpenBlocking(ctx, pool, job.ID); err != nil || n != 1 {
		t.Fatalf("countOpenBlocking = %d, %v, want 1", n, err)
	}
	if got, _ := GetJob(ctx, pool, job.ID); got.OpenBlockingIssues != 1 {
		t.Fatalf("GetJob.OpenBlockingIssues = %d, want 1", got.OpenBlockingIssues)
	}
}

func TestResolveIssuesLeavesOtherJobsAlone(t *testing.T) {
	pool := storeTestPool(t)
	ctx := context.Background()
	first := storeMustCreate(t, pool, "UPDATE-1")
	if err := transition(ctx, pool, first.ID, nil, StatusInterrupted, nil); err != nil {
		t.Fatal(err)
	}
	second := storeMustCreate(t, pool, "UPDATE-2")

	storeRecord(t, pool, storeIssue(first.ID, CodeVersionConflict, SeverityBlocking, "cursor-002"))
	storeRecord(t, pool, storeIssue(second.ID, CodeVersionConflict, SeverityBlocking, "cursor-002"))
	storeInJobTx(t, pool, second.ID, func(ctx context.Context, tx pgx.Tx) error { return resolveIssues(ctx, tx, second.ID, "cursor-002") })

	if n, _ := countOpenBlocking(ctx, pool, first.ID); n != 1 {
		t.Fatalf("first job open blocking = %d, want 1: resolving one job must not touch another", n)
	}
	if n, _ := countOpenBlocking(ctx, pool, second.ID); n != 0 {
		t.Fatalf("second job open blocking = %d, want 0", n)
	}
}

func TestSettleJobRecordsTheIssueAndMovesARunningJobTogether(t *testing.T) {
	pool := storeTestPool(t)
	ctx := context.Background()
	id := storeSeedJob(t, pool, "UPDATE-1", StatusRunning)
	if _, err := pool.Exec(ctx, `UPDATE sync_jobs SET next_cursor = 'cursor-003' WHERE id = $1::uuid`, id); err != nil {
		t.Fatal(err)
	}

	// No cursor on the issue: it takes the job's own, so it can resolve when that batch commits.
	iss := Issue{Code: CodeInvalidBatch, Severity: SeverityBlocking, Message: "no has_more marker"}
	prev, applied, err := settleJob(ctx, pool, id, &iss, StatusNeedsAttention, "no has_more marker")
	if err != nil || !applied || prev.Status != StatusRunning || prev.UpdateID != "UPDATE-1" {
		t.Fatalf("settleJob = applied %v err %v, want applied", applied, err)
	}

	job, err := GetJob(ctx, pool, id)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != StatusNeedsAttention || job.LastError == nil || *job.LastError != "no has_more marker" || job.OpenBlockingIssues != 1 {
		t.Fatalf("job = status %s last_error %v open_blocking %d", job.Status, job.LastError, job.OpenBlockingIssues)
	}
	rows := storeIssueRows(t, pool, id)
	if len(rows) != 1 || rows[0].Cursor != "cursor-003" || rows[0].Code != string(CodeInvalidBatch) || rows[0].Status != "OPEN" {
		t.Fatalf("issues = %+v, want one OPEN INVALID_BATCH at cursor-003", rows)
	}
}

func TestSettleJobWithoutIssueOnlyMovesTheJob(t *testing.T) {
	pool := storeTestPool(t)
	id := storeSeedJob(t, pool, "UPDATE-1", StatusRunning)

	_, applied, err := settleJob(context.Background(), pool, id, nil, StatusInterrupted, "interrupted")
	if err != nil || !applied {
		t.Fatalf("settleJob = applied %v err %v, want applied", applied, err)
	}
	if n := storeCount(t, pool, `SELECT count(*) FROM sync_issues`); n != 0 {
		t.Fatalf("issue rows = %d, want 0", n)
	}
	if job, _ := GetJob(context.Background(), pool, id); job.Status != StatusInterrupted {
		t.Fatalf("status = %s, want INTERRUPTED", job.Status)
	}
}

func TestSettleJobLeavesAJobThatIsNoLongerRunningAlone(t *testing.T) {
	pool := storeTestPool(t)
	ctx := context.Background()
	for _, status := range []Status{StatusInterrupted, StatusNeedsAttention, StatusCompleted, StatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			storeResetJobs(t, pool)
			id := storeSeedJob(t, pool, "UPDATE-1", status)
			iss := storeIssue(id, CodeInvalidBatch, SeverityBlocking, "")

			prev, applied, err := settleJob(ctx, pool, id, &iss, StatusNeedsAttention, "late")
			if err != nil || applied || prev.Status != status {
				t.Fatalf("settleJob = prev %s applied %v err %v, want not applied", prev.Status, applied, err)
			}
			job, _ := GetJob(ctx, pool, id)
			if job.Status != status || job.LastError != nil {
				t.Fatalf("job = %s / %v, want untouched %s", job.Status, job.LastError, status)
			}
			if n := storeCount(t, pool, `SELECT count(*) FROM sync_issues`); n != 0 {
				t.Fatalf("issue rows = %d, want none for a job that moved on", n)
			}
		})
	}
}

func TestSettleJobOfUnknownJobIsNotFound(t *testing.T) {
	pool := storeTestPool(t)
	for _, id := range []string{"00000000-0000-0000-0000-000000000000", "not-a-uuid"} {
		if _, _, err := settleJob(context.Background(), pool, id, nil, StatusInterrupted, "x"); !errors.Is(err, ErrNotFound) {
			t.Errorf("settleJob(%q) error = %v, want ErrNotFound", id, err)
		}
	}
}

// storeResetJobs empties the job tables between subtests that reuse one update id.
func storeResetJobs(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `TRUNCATE sync_issues, sync_changes, sync_batches, sync_jobs CASCADE`); err != nil {
		t.Fatal(err)
	}
}

func TestResolveTransientIssuesClosesOnlyTransientOnes(t *testing.T) {
	pool := storeTestPool(t)
	job := storeMustCreate(t, pool, "UPDATE-1")
	storeRecord(t, pool, storeIssue(job.ID, CodeSupplierUnavailable, SeverityTransient, "cursor-003"))
	storeRecord(t, pool, storeIssue(job.ID, CodeInvalidBatch, SeverityBlocking, "cursor-003"))

	storeInJobTx(t, pool, job.ID, func(ctx context.Context, tx pgx.Tx) error { return resolveTransientIssues(ctx, tx, job.ID) })

	for _, row := range storeIssueRows(t, pool, job.ID) {
		switch row.Severity {
		case string(SeverityTransient):
			if row.Status != "RESOLVED" || !row.Resolved {
				t.Errorf("transient issue = %+v, want RESOLVED with resolved_at", row)
			}
		case string(SeverityBlocking):
			if row.Status != "OPEN" {
				t.Errorf("blocking issue = %+v, want it left OPEN even at the same cursor", row)
			}
		}
	}
}
