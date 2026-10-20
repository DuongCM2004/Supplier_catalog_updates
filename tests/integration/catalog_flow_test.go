package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/catalog"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/supplier"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/syncjob"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/testutil"
)

// These tests run whole sync jobs through a Manager on a real PostgreSQL database, with a
// FakeSupplier standing in for the feed. A job is only ever driven with Start/Resume
// followed by Wait, so exactly one worker acts on it, and failures come from the fake
// supplier, never from timing. The flowEnv harness below is shared by the other scenario
// files of this package.

// flowConfig keeps retries fast and deterministic. The supplier timeout is generous because
// the fake answers instantly; the tests where a timeout is the point shorten it.
func flowConfig() syncjob.Config {
	return syncjob.Config{
		RetryMaxAttempts: 4,
		RetryBaseDelay:   time.Millisecond,
		RetryMaxDelay:    5 * time.Millisecond,
		SupplierTimeout:  5 * time.Second,
	}
}

// flowEnv is an emptied test database, a fake supplier and the Manager working on them.
type flowEnv struct {
	t    *testing.T
	pool *pgxpool.Pool
	sup  *testutil.FakeSupplier
	m    *syncjob.Manager
}

func flowNew(t *testing.T) *flowEnv {
	t.Helper()
	return flowNewWith(t, flowConfig(), syncjob.Hooks{}, nil)
}

// flowNewWith resets the database and builds a Manager with the given retry settings, fault
// hooks and logger (nil discards logs).
func flowNewWith(t *testing.T, cfg syncjob.Config, hooks syncjob.Hooks, log *slog.Logger) *flowEnv {
	t.Helper()
	pool := testutil.DB(t)
	testutil.Reset(t, pool)
	sup := testutil.NewFakeSupplier()
	return &flowEnv{t: t, pool: pool, sup: sup, m: flowManager(t, pool, sup, cfg, hooks, log)}
}

// flowManager builds a Manager that is shut down when the test ends. Building a second one
// on the same database models a restarted process.
func flowManager(t *testing.T, pool *pgxpool.Pool, sup supplier.Client, cfg syncjob.Config, hooks syncjob.Hooks, log *slog.Logger) *syncjob.Manager {
	t.Helper()
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	m := syncjob.NewManager(pool, sup, cfg, hooks, log)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := m.Shutdown(ctx); err != nil {
			t.Errorf("manager shutdown: %v", err)
		}
	})
	return m
}

// with returns the same environment reporting failures to another (sub)test.
func (e *flowEnv) with(t *testing.T) *flowEnv {
	cp := *e
	cp.t = t
	return &cp
}

// flowRecords reads better than a bare slice literal at the call sites.
func flowRecords(products ...catalog.SupplierProduct) []catalog.SupplierProduct { return products }

// flowCursor is the cursor of page n (1-based); the first page lives at "".
func flowCursor(n int) string {
	if n == 1 {
		return ""
	}
	return fmt.Sprintf("cursor-%d", n)
}

func flowBatchID(n int) string { return fmt.Sprintf("BATCH-%d", n) }

// serve announces updateID with one page per element of pages, chained "" -> cursor-2 ->
// cursor-3 ... with the last page ending the feed. Pages are keyed by cursor alone in the
// fake, so serving another update replaces the pages of the previous one.
func (e *flowEnv) serve(updateID string, pages ...[]catalog.SupplierProduct) {
	e.t.Helper()
	e.sup.SetUpdate(testutil.Info(updateID, len(pages)))
	for i, products := range pages {
		n := i + 1
		more := n < len(pages)
		next := ""
		if more {
			next = flowCursor(n + 1)
		}
		e.sup.AddPage(flowCursor(n), testutil.Page(updateID, flowBatchID(n), next, more, products...))
	}
}

// run starts updateID, waits for its worker to finish and returns the job as stored.
func (e *flowEnv) run(updateID string) syncjob.Job {
	e.t.Helper()
	job, created, err := e.m.Start(context.Background(), testutil.SupplierID, updateID)
	if err != nil || !created {
		e.t.Fatalf("Start(%s) = created %v, err %v; want a new job", updateID, created, err)
	}
	e.m.Wait()
	return e.job(job.ID)
}

// resume resumes the job, waits for its worker to finish and returns the job as stored.
func (e *flowEnv) resume(jobID string) syncjob.Job {
	e.t.Helper()
	job, err := e.m.Resume(context.Background(), jobID)
	if err != nil {
		e.t.Fatalf("Resume(%s): %v", jobID, err)
	}
	if job.Status != syncjob.StatusRunning {
		e.t.Fatalf("Resume returned status %s, want RUNNING", job.Status)
	}
	e.m.Wait()
	return e.job(jobID)
}

func (e *flowEnv) job(id string) syncjob.Job {
	e.t.Helper()
	job, err := syncjob.GetJob(context.Background(), e.pool, id)
	if err != nil {
		e.t.Fatalf("GetJob(%s): %v", id, err)
	}
	return job
}

func (e *flowEnv) issues(jobID string) []syncjob.Issue {
	e.t.Helper()
	issues, err := syncjob.ListIssues(context.Background(), e.pool, jobID, "", "", 500, 0)
	if err != nil {
		e.t.Fatalf("ListIssues(%s): %v", jobID, err)
	}
	return issues
}

// changes lists the audit rows of a job in the order they were applied; changeType "" = all.
func (e *flowEnv) changes(jobID, changeType string) []syncjob.Change {
	e.t.Helper()
	changes, err := syncjob.ListChanges(context.Background(), e.pool, jobID, changeType, 500, 0)
	if err != nil {
		e.t.Fatalf("ListChanges(%s): %v", jobID, err)
	}
	return changes
}

func (e *flowEnv) product(id string) catalog.Product {
	e.t.Helper()
	p, err := catalog.GetProduct(context.Background(), e.pool, testutil.SupplierID, id)
	if err != nil {
		e.t.Fatalf("GetProduct(%s): %v", id, err)
	}
	return p
}

func (e *flowEnv) assertAbsent(id string) {
	e.t.Helper()
	if _, err := catalog.GetProduct(context.Background(), e.pool, testutil.SupplierID, id); !errors.Is(err, catalog.ErrNotFound) {
		e.t.Errorf("product %s: error = %v, want it absent from the catalog", id, err)
	}
}

// assertPrice checks the stored price of a product on top of its status and revision.
func (e *flowEnv) assertPrice(id string, want int64) {
	e.t.Helper()
	if p := e.product(id); p.PriceMinor == nil || *p.PriceMinor != want {
		e.t.Errorf("product %s price = %v, want %d", id, p.PriceMinor, want)
	}
}

// batchMarkers returns "seq:batch_id" for every committed batch of the job, in order.
func (e *flowEnv) batchMarkers(jobID string) []string {
	e.t.Helper()
	rows, err := e.pool.Query(context.Background(),
		`SELECT seq || ':' || batch_id FROM sync_batches WHERE job_id = $1::uuid ORDER BY seq`, jobID)
	if err != nil {
		e.t.Fatalf("read batch markers of job %s: %v", jobID, err)
	}
	markers, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		e.t.Fatalf("read batch markers of job %s: %v", jobID, err)
	}
	return markers
}

func flowCounts(j syncjob.Job) syncjob.Counts {
	return syncjob.Counts{Created: j.Created, Updated: j.Updated, Withdrawn: j.Withdrawn, Duplicate: j.Duplicate, Stale: j.Stale}
}

// expect checks where a job ended up, then the cross-checks every job must satisfy.
func (e *flowEnv) expect(job syncjob.Job, status syncjob.Status, batches int, counts syncjob.Counts) {
	e.t.Helper()
	if got := flowCounts(job); job.Status != status || job.ProcessedBatches != batches || got != counts {
		e.t.Fatalf("job = %s, %d batches, %+v; want %s, %d batches, %+v (last_error %v)",
			job.Status, job.ProcessedBatches, got, status, batches, counts, job.LastError)
	}
	e.verify(job)
}

// verify holds for any job in any state: its counters agree with its batches and audit rows,
// and only a COMPLETED job has a completion time and passes the completion consistency check.
func (e *flowEnv) verify(job syncjob.Job) {
	e.t.Helper()
	testutil.AssertCounters(e.t, e.pool, job.ID)
	if job.Status == syncjob.StatusCompleted {
		testutil.AssertCompletedConsistent(e.t, e.pool, job.ID)
	} else if job.CompletedAt != nil {
		e.t.Errorf("job %s is %s but has completed_at %v", job.ID, job.Status, job.CompletedAt)
	}
}

// flowAudit is the before/after picture stored in an audit row.
type flowAudit struct {
	Status     string  `json:"status"`
	Revision   int64   `json:"revision"`
	Name       *string `json:"name"`
	PriceMinor *int64  `json:"price_minor"`
	Currency   *string `json:"currency"`
}

// flowDecode returns nil for a missing picture (SQL NULL or JSON null).
func flowDecode(t *testing.T, raw json.RawMessage) *flowAudit {
	t.Helper()
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var a flowAudit
	if err := json.Unmarshal(raw, &a); err != nil {
		t.Fatalf("decode audit picture %s: %v", raw, err)
	}
	return &a
}

// flowBaseline is the first import the follow-up scenarios build on: SKU-A and SKU-B in the
// first batch, SKU-C in the last. Every call returns fresh slices.
func flowBaseline() [][]catalog.SupplierProduct {
	return [][]catalog.SupplierProduct{
		flowRecords(testutil.Active("SKU-A", 1, "Alpha", 1000), testutil.Active("SKU-B", 1, "Beta", 2000)),
		flowRecords(testutil.Active("SKU-C", 1, "Gamma", 300)),
	}
}

func (e *flowEnv) importBaseline() syncjob.Job {
	e.t.Helper()
	e.serve("U1", flowBaseline()...)
	job := e.run("U1")
	e.expect(job, syncjob.StatusCompleted, 2, syncjob.Counts{Created: 3})
	return job
}

func TestFirstImportCreatesProducts(t *testing.T) {
	e := flowNew(t)
	job := e.importBaseline()

	for _, id := range []string{"SKU-A", "SKU-B", "SKU-C"} {
		testutil.AssertProduct(t, e.pool, testutil.SupplierID, id, catalog.StatusActive, 1)
	}
	if job.TotalBatches == nil || *job.TotalBatches != 2 || !job.EndOfFeedSeen || job.CompletedAt == nil {
		t.Errorf("job = total %v, end of feed %v, completed %v; want 2 announced batches, end of feed seen and a completion time",
			job.TotalBatches, job.EndOfFeedSeen, job.CompletedAt)
	}
	if got, want := e.batchMarkers(job.ID), []string{"1:BATCH-1", "2:BATCH-2"}; !slices.Equal(got, want) {
		t.Errorf("batch markers = %v, want %v", got, want)
	}

	changes := e.changes(job.ID, "CREATED")
	if len(changes) != 3 {
		t.Fatalf("CREATED changes = %d, want 3", len(changes))
	}
	for i, id := range []string{"SKU-A", "SKU-B", "SKU-C"} {
		c := changes[i]
		after := flowDecode(t, c.After)
		if c.ProductID != id || c.Revision != 1 || flowDecode(t, c.Before) != nil || after == nil || after.Status != "ACTIVE" || after.Revision != 1 {
			t.Errorf("change %d = %s r%d before %s after %s; want %s r1 created from nothing", i, c.ProductID, c.Revision, c.Before, c.After, id)
		}
	}
	if all := e.changes(job.ID, ""); len(all) != 3 {
		t.Errorf("all changes = %d, want only the 3 creations", len(all))
	}
}

func TestHigherRevisionUpdatesProduct(t *testing.T) {
	e := flowNew(t)
	first := e.importBaseline()

	e.serve("U2", flowRecords(testutil.Active("SKU-A", 2, "Alpha", 1500)))
	second := e.run("U2")

	e.expect(second, syncjob.StatusCompleted, 1, syncjob.Counts{Updated: 1})
	testutil.AssertProduct(t, e.pool, testutil.SupplierID, "SKU-A", catalog.StatusActive, 2)
	e.assertPrice("SKU-A", 1500)
	testutil.AssertProduct(t, e.pool, testutil.SupplierID, "SKU-B", catalog.StatusActive, 1)

	changes := e.changes(second.ID, "")
	if len(changes) != 1 || changes[0].ChangeType != "UPDATED" || changes[0].ProductID != "SKU-A" {
		t.Fatalf("changes = %+v, want exactly one UPDATED change of SKU-A", changes)
	}
	before, after := flowDecode(t, changes[0].Before), flowDecode(t, changes[0].After)
	if before == nil || before.Revision != 1 || *before.PriceMinor != 1000 || after == nil || after.Revision != 2 || *after.PriceMinor != 1500 {
		t.Errorf("change before %s after %s; want revision 1 at 1000 become revision 2 at 1500", changes[0].Before, changes[0].After)
	}
	e.verify(e.job(first.ID)) // the earlier job's records are untouched by the later one
}

func TestReplayOfSameUpdateIsNoOp(t *testing.T) {
	e := flowNew(t)
	first := e.importBaseline()
	before := testutil.Snapshot(t, e.pool)
	callsBefore := len(e.sup.Calls())

	again, created, err := e.m.Start(context.Background(), testutil.SupplierID, "U1")
	e.m.Wait()

	if err != nil || created || again.ID != first.ID || again.Status != syncjob.StatusCompleted {
		t.Fatalf("second Start = job %s %s, created %v, err %v; want the completed job %s, not created", again.ID, again.Status, created, err, first.ID)
	}
	testutil.AssertUnchanged(t, before, testutil.Snapshot(t, e.pool))
	if n := len(e.sup.Calls()); n != callsBefore {
		t.Errorf("supplier calls = %d, want %d: a known update must not be fetched again", n, callsBefore)
	}
	e.verify(e.job(first.ID))
}

func TestResentRecordsUnderNewUpdateAreDuplicates(t *testing.T) {
	e := flowNew(t)
	e.importBaseline()
	// Products and the audit trail are what a resend must leave alone; jobs and batches
	// naturally gain the new job.
	before := testutil.SnapshotExcept(t, e.pool, "sync_jobs", "sync_batches", "sync_issues")

	e.serve("U3", flowBaseline()...)
	resent := e.run("U3")

	e.expect(resent, syncjob.StatusCompleted, 2, syncjob.Counts{Duplicate: 3})
	testutil.AssertUnchanged(t, before, testutil.SnapshotExcept(t, e.pool, "sync_jobs", "sync_batches", "sync_issues"))
	if changes := e.changes(resent.ID, ""); len(changes) != 0 {
		t.Errorf("changes of the resent update = %+v, want none", changes)
	}
}

func TestLateLowerRevisionIsIgnored(t *testing.T) {
	e := flowNew(t)
	e.serve("U1", flowRecords(testutil.Active("SKU-A", 5, "Alpha", 1000)))
	e.expect(e.run("U1"), syncjob.StatusCompleted, 1, syncjob.Counts{Created: 1})
	before := testutil.SnapshotExcept(t, e.pool, "sync_jobs", "sync_batches", "sync_issues")

	e.serve("U2", flowRecords(testutil.Active("SKU-A", 4, "Alpha", 900)))
	late := e.run("U2")

	e.expect(late, syncjob.StatusCompleted, 1, syncjob.Counts{Stale: 1})
	testutil.AssertProduct(t, e.pool, testutil.SupplierID, "SKU-A", catalog.StatusActive, 5)
	e.assertPrice("SKU-A", 1000)
	if changes := e.changes(late.ID, ""); len(changes) != 0 {
		t.Errorf("changes = %+v, want none for an ignored revision", changes)
	}
	testutil.AssertUnchanged(t, before, testutil.SnapshotExcept(t, e.pool, "sync_jobs", "sync_batches", "sync_issues"))
}

func TestStaleActiveCannotResurrectWithdrawnProduct(t *testing.T) {
	e := flowNew(t)
	e.serve("U1", flowRecords(testutil.Active("SKU-A", 1, "Alpha", 1000), testutil.Active("SKU-B", 1, "Beta", 2000)))
	e.expect(e.run("U1"), syncjob.StatusCompleted, 1, syncjob.Counts{Created: 2})
	e.serve("U2", flowRecords(testutil.Withdrawn("SKU-A", 3)))
	e.expect(e.run("U2"), syncjob.StatusCompleted, 1, syncjob.Counts{Withdrawn: 1})
	before := testutil.SnapshotExcept(t, e.pool, "sync_jobs", "sync_batches", "sync_issues")

	e.serve("U3", flowRecords(testutil.Active("SKU-A", 2, "Alpha", 1200)))
	late := e.run("U3")

	e.expect(late, syncjob.StatusCompleted, 1, syncjob.Counts{Stale: 1})
	testutil.AssertProduct(t, e.pool, testutil.SupplierID, "SKU-A", catalog.StatusWithdrawn, 3)
	if changes := e.changes(late.ID, ""); len(changes) != 0 {
		t.Errorf("changes = %+v, want none", changes)
	}
	testutil.AssertUnchanged(t, before, testutil.SnapshotExcept(t, e.pool, "sync_jobs", "sync_batches", "sync_issues"))

	// What a shopper sees: the withdrawn product is out of the default listing, but its
	// tombstone is still there.
	active, err := catalog.ListProducts(context.Background(), e.pool, catalog.ListFilter{})
	if err != nil || len(active) != 1 || active[0].ProductID != "SKU-B" {
		t.Errorf("default listing = %+v, err %v; want only SKU-B", active, err)
	}
	if p := e.product("SKU-A"); p.Status != catalog.StatusWithdrawn || p.WithdrawnAt == nil {
		t.Errorf("SKU-A = %s withdrawn_at %v, want a WITHDRAWN tombstone", p.Status, p.WithdrawnAt)
	}
}

func TestWithdrawalOfUnseenProductCreatesTombstone(t *testing.T) {
	e := flowNew(t)
	e.serve("U1", flowRecords(testutil.Withdrawn("SKU-Z", 4)))
	job := e.run("U1")

	e.expect(job, syncjob.StatusCompleted, 1, syncjob.Counts{Withdrawn: 1})
	testutil.AssertProduct(t, e.pool, testutil.SupplierID, "SKU-Z", catalog.StatusWithdrawn, 4)
	if p := e.product("SKU-Z"); p.Name != nil || p.PriceMinor != nil || p.Currency != nil || p.WithdrawnAt == nil {
		t.Errorf("tombstone = name %v price %v currency %v withdrawn_at %v; want no details and a withdrawal time", p.Name, p.PriceMinor, p.Currency, p.WithdrawnAt)
	}
	changes := e.changes(job.ID, "")
	if len(changes) != 1 || changes[0].ChangeType != "WITHDRAWN" || changes[0].ProductID != "SKU-Z" {
		t.Fatalf("changes = %+v, want one WITHDRAWN change of SKU-Z", changes)
	}
	if before, after := flowDecode(t, changes[0].Before), flowDecode(t, changes[0].After); before != nil || after == nil || after.Status != "WITHDRAWN" || after.Revision != 4 {
		t.Errorf("change before %s after %s; want nothing before and a WITHDRAWN r4 picture after", changes[0].Before, changes[0].After)
	}
	if active, err := catalog.ListProducts(context.Background(), e.pool, catalog.ListFilter{}); err != nil || len(active) != 0 {
		t.Errorf("default listing = %+v, err %v; want the tombstone hidden", active, err)
	}
}

// flowReadLogs decodes the JSON log lines a handler wrote into buf.
func flowReadLogs(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var lines []map[string]any
	dec := json.NewDecoder(bytes.NewReader(buf.Bytes()))
	for dec.More() {
		var line map[string]any
		if err := dec.Decode(&line); err != nil {
			t.Fatalf("decode log line: %v", err)
		}
		lines = append(lines, line)
	}
	return lines
}

func flowMessages(lines []map[string]any) []string {
	msgs := make([]string, len(lines))
	for i, l := range lines {
		msgs[i], _ = l["msg"].(string)
	}
	return msgs
}

func TestJobEmitsStructuredLifecycleLogs(t *testing.T) {
	t.Run("lifecycle_sequence", func(t *testing.T) {
		var buf bytes.Buffer
		e := flowNewWith(t, flowConfig(), syncjob.Hooks{}, slog.New(slog.NewJSONHandler(&buf, nil)))
		e.serve("U1",
			flowRecords(testutil.Active("SKU-A", 1, "Alpha", 1000)),
			flowRecords(testutil.Active("SKU-B", 1, "Beta", 2000)),
			flowRecords(testutil.Active("SKU-C", 1, "Gamma", 300)))
		job := e.run("U1")
		e.expect(job, syncjob.StatusCompleted, 3, syncjob.Counts{Created: 3})

		lines := flowReadLogs(t, &buf)
		want := []string{"job.created", "job.transition", "batch.committed", "batch.committed", "batch.committed", "job.transition", "job.completed"}
		if got := flowMessages(lines); !slices.Equal(got, want) {
			t.Fatalf("log events = %v, want %v", got, want)
		}
		for i, l := range lines {
			if l["job_id"] != job.ID {
				t.Errorf("event %d (%v): job_id = %v, want %s", i, l["msg"], l["job_id"], job.ID)
			}
		}
		if l := lines[1]; l["from"] != "PENDING" || l["to"] != "RUNNING" {
			t.Errorf("first transition = %v -> %v, want PENDING -> RUNNING", l["from"], l["to"])
		}
		for i, l := range lines[2:5] {
			if l["seq"] != float64(i+1) || l["batch_id"] != flowBatchID(i+1) || l["created"] != float64(1) {
				t.Errorf("batch.committed %d = seq %v batch %v created %v, want seq %d %s created 1", i+1, l["seq"], l["batch_id"], l["created"], i+1, flowBatchID(i+1))
			}
		}
		if l := lines[5]; l["from"] != "RUNNING" || l["to"] != "COMPLETED" {
			t.Errorf("last transition = %v -> %v, want RUNNING -> COMPLETED", l["from"], l["to"])
		}
		if l := lines[6]; l["processed_batches"] != float64(3) || l["created"] != float64(3) {
			t.Errorf("job.completed = processed %v created %v, want 3 and 3", l["processed_batches"], l["created"])
		}
	})

	t.Run("retried_outage", func(t *testing.T) {
		var buf bytes.Buffer
		e := flowNewWith(t, flowConfig(), syncjob.Hooks{}, slog.New(slog.NewJSONHandler(&buf, nil)))
		e.serve("U1", flowRecords(testutil.Active("SKU-A", 1, "Alpha", 1000)), flowRecords(testutil.Active("SKU-B", 1, "Beta", 2000)))
		e.sup.FailTimes(flowCursor(2), 2) // two failed attempts, then the supplier answers
		job := e.run("U1")
		e.expect(job, syncjob.StatusCompleted, 2, syncjob.Counts{Created: 2})
		if n := e.sup.FetchCalls(flowCursor(2)); n != 3 {
			t.Errorf("fetches at %s = %d, want 3 (two failures, one success)", flowCursor(2), n)
		}

		var retries []map[string]any
		for _, l := range flowReadLogs(t, &buf) {
			if l["msg"] == "supplier.retry" {
				retries = append(retries, l)
			}
		}
		if len(retries) != 2 {
			t.Fatalf("supplier.retry entries = %d, want 2", len(retries))
		}
		// Delays double from the 1ms base: 1ms after the first failure, 2ms after the second.
		for i, l := range retries {
			delayMs := 1 << i
			if l["level"] != "WARN" || l["op"] != "fetch_batch" || l["attempt"] != float64(i+1) ||
				l["max_attempts"] != float64(4) || l["delay_ms"] != float64(delayMs) || l["error"] == nil ||
				l["job_id"] != job.ID || l["cursor"] != flowCursor(2) {
				t.Errorf("supplier.retry %d = %v; want WARN fetch_batch attempt %d of 4 with delay_ms %d, the error, job_id %s and cursor %s",
					i+1, l, i+1, delayMs, job.ID, flowCursor(2))
			}
		}
	})
}
