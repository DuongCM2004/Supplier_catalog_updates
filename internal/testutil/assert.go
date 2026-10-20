package testutil

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/catalog"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/storage"
)

// allTables lists, in a fixed order, every table whose content a failed or replayed attempt
// must leave untouched.
var allTables = []string{"products", "sync_jobs", "sync_batches", "sync_changes", "sync_issues"}

// TableState is the row count and content checksum of one table.
type TableState struct {
	Rows     int64
	Checksum string
}

// DBSnapshot maps table name to its state at one moment.
type DBSnapshot map[string]TableState

// Table names come from allTables, never from input, so formatting them into SQL is safe
// (identifiers cannot be bound as parameters). Rows are rendered to text and sorted before
// hashing, so the checksum depends on content only, not on physical order or random ids.
const snapshotSQL = `SELECT count(*), COALESCE(md5(string_agg(r::text, E'\n' ORDER BY r::text)), '') FROM %s r`

// Snapshot captures all five tables. Comparing two snapshots proves that nothing, down to
// timestamps, changed between them.
func Snapshot(t testing.TB, db storage.DB) DBSnapshot {
	t.Helper()
	return SnapshotExcept(t, db)
}

// SnapshotExcept captures every table except the named ones, for tests where one table
// (typically sync_issues or sync_jobs) is expected to change while the rest must not.
func SnapshotExcept(t testing.TB, db storage.DB, except ...string) DBSnapshot {
	t.Helper()
	for _, name := range except {
		if !slices.Contains(allTables, name) {
			t.Fatalf("snapshot: unknown table %q (known: %v)", name, allTables)
		}
	}
	snap := DBSnapshot{}
	for _, table := range allTables {
		if slices.Contains(except, table) {
			continue
		}
		var s TableState
		err := db.QueryRow(context.Background(), fmt.Sprintf(snapshotSQL, table)).Scan(&s.Rows, &s.Checksum)
		if err != nil {
			t.Fatalf("snapshot %s: %v", table, err)
		}
		snap[table] = s
	}
	return snap
}

// AssertUnchanged fails for every table whose row count or content differs between the two
// snapshots, and when they cover different tables.
func AssertUnchanged(t testing.TB, before, after DBSnapshot) {
	t.Helper()
	for _, table := range allTables {
		b, inBefore := before[table]
		a, inAfter := after[table]
		switch {
		case !inBefore && !inAfter:
		case inBefore != inAfter:
			t.Errorf("table %s is captured in only one of the two snapshots", table)
		case b != a:
			t.Errorf("table %s changed: rows %d -> %d, checksum %s -> %s", table, b.Rows, a.Rows, b.Checksum, a.Checksum)
		}
	}
}

const jobCountersSQL = `SELECT processed_batches, created_count, updated_count, withdrawn_count, duplicate_count, stale_count
FROM sync_jobs WHERE id = $1::uuid`

const batchSumsSQL = `SELECT count(*), COALESCE(sum(created_count), 0), COALESCE(sum(updated_count), 0),
  COALESCE(sum(withdrawn_count), 0), COALESCE(sum(duplicate_count), 0), COALESCE(sum(stale_count), 0)
FROM sync_batches WHERE job_id = $1::uuid`

const changeCountsSQL = `SELECT count(*) FILTER (WHERE change_type = 'CREATED'),
  count(*) FILTER (WHERE change_type = 'UPDATED'),
  count(*) FILTER (WHERE change_type = 'WITHDRAWN')
FROM sync_changes WHERE job_id = $1::uuid`

// AssertCounters checks that the three independent records of a job's work agree: the
// counters on the job equal the sum of its committed batch counters, and created, updated
// and withdrawn also equal the number of audit rows of that type. Duplicate and stale
// events leave no audit row, so they are checked against the batches only.
func AssertCounters(t testing.TB, db storage.DB, jobID string) {
	t.Helper()
	ctx := context.Background()
	var processed, created, updated, withdrawn, duplicate, stale int64
	if err := db.QueryRow(ctx, jobCountersSQL, jobID).
		Scan(&processed, &created, &updated, &withdrawn, &duplicate, &stale); err != nil {
		t.Fatalf("read counters of job %s: %v", jobID, err)
	}
	var batches, sumCreated, sumUpdated, sumWithdrawn, sumDuplicate, sumStale int64
	if err := db.QueryRow(ctx, batchSumsSQL, jobID).
		Scan(&batches, &sumCreated, &sumUpdated, &sumWithdrawn, &sumDuplicate, &sumStale); err != nil {
		t.Fatalf("sum batch counters of job %s: %v", jobID, err)
	}
	var changeCreated, changeUpdated, changeWithdrawn int64
	if err := db.QueryRow(ctx, changeCountsSQL, jobID).
		Scan(&changeCreated, &changeUpdated, &changeWithdrawn); err != nil {
		t.Fatalf("count changes of job %s: %v", jobID, err)
	}

	check := func(name string, job, other int64, otherName string) {
		if job != other {
			t.Errorf("job %s: %s = %d but %s = %d", jobID, name, job, otherName, other)
		}
	}
	check("processed_batches", processed, batches, "committed batch rows")
	check("created_count", created, sumCreated, "sum of batch created_count")
	check("updated_count", updated, sumUpdated, "sum of batch updated_count")
	check("withdrawn_count", withdrawn, sumWithdrawn, "sum of batch withdrawn_count")
	check("duplicate_count", duplicate, sumDuplicate, "sum of batch duplicate_count")
	check("stale_count", stale, sumStale, "sum of batch stale_count")
	check("created_count", created, changeCreated, "CREATED change rows")
	check("updated_count", updated, changeUpdated, "UPDATED change rows")
	check("withdrawn_count", withdrawn, changeWithdrawn, "WITHDRAWN change rows")
}

const completedStateSQL = `SELECT j.status, j.end_of_feed_seen, j.completed_at IS NOT NULL,
  j.processed_batches, j.total_batches,
  (SELECT count(*) FROM sync_batches b WHERE b.job_id = j.id),
  (SELECT b.has_more FROM sync_batches b WHERE b.job_id = j.id ORDER BY b.seq DESC LIMIT 1),
  (SELECT count(*) FROM sync_issues i WHERE i.job_id = j.id AND i.status = 'OPEN' AND i.severity = 'BLOCKING')
FROM sync_jobs j WHERE j.id = $1::uuid`

// AssertCompletedConsistent fails unless the job is COMPLETED for the right reasons: the
// supplier signalled end of feed, the last committed batch really was the last page, the
// batch count matches what was committed and announced, and no blocking issue is open.
// A job in any other status fails too, so a test cannot pass by never finishing.
func AssertCompletedConsistent(t testing.TB, db storage.DB, jobID string) {
	t.Helper()
	var (
		status        string
		endOfFeedSeen bool
		hasCompleted  bool
		processed     int64
		total         *int64
		batches       int64
		lastHasMore   *bool
		openBlocking  int64
	)
	err := db.QueryRow(context.Background(), completedStateSQL, jobID).
		Scan(&status, &endOfFeedSeen, &hasCompleted, &processed, &total, &batches, &lastHasMore, &openBlocking)
	if errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("job %s does not exist", jobID)
	}
	if err != nil {
		t.Fatalf("read completion state of job %s: %v", jobID, err)
	}

	if status != "COMPLETED" {
		t.Errorf("job %s: status = %s, want COMPLETED", jobID, status)
	}
	if !endOfFeedSeen {
		t.Errorf("job %s: end_of_feed_seen is false", jobID)
	}
	if !hasCompleted {
		t.Errorf("job %s: completed_at is not set", jobID)
	}
	if processed != batches {
		t.Errorf("job %s: processed_batches = %d but %d batch rows are committed", jobID, processed, batches)
	}
	if total != nil && *total != processed {
		t.Errorf("job %s: supplier announced %d batches but %d were processed", jobID, *total, processed)
	}
	switch {
	case lastHasMore == nil:
		t.Errorf("job %s: completed without any committed batch", jobID)
	case *lastHasMore:
		t.Errorf("job %s: the last committed batch still said has_more", jobID)
	}
	if openBlocking != 0 {
		t.Errorf("job %s: %d blocking issues are still open", jobID, openBlocking)
	}
}

// AssertProduct fails unless the stored product exists with exactly this status and revision.
func AssertProduct(t testing.TB, db storage.DB, supplierID, productID string, status catalog.Status, revision int64) {
	t.Helper()
	const q = `SELECT status, revision FROM products WHERE supplier_id = $1 AND product_id = $2`
	var gotStatus string
	var gotRevision int64
	err := db.QueryRow(context.Background(), q, supplierID, productID).Scan(&gotStatus, &gotRevision)
	if errors.Is(err, pgx.ErrNoRows) {
		t.Errorf("product %s/%s does not exist, want %s r%d", supplierID, productID, status, revision)
		return
	}
	if err != nil {
		t.Fatalf("read product %s/%s: %v", supplierID, productID, err)
	}
	if gotStatus != string(status) || gotRevision != revision {
		t.Errorf("product %s/%s = %s r%d, want %s r%d", supplierID, productID, gotStatus, gotRevision, status, revision)
	}
}
