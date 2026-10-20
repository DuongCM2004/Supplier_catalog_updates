package storage_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/storage"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/testutil"
	"github.com/DuongCM2004/resilient-catalog-sync/migrations"
)

const checkViolation = "23514"

// embeddedVersions lists the versions Migrate must record, so the tests keep working when
// a later migration file is added.
func embeddedVersions(t *testing.T) []string {
	t.Helper()
	files, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		t.Fatalf("list embedded migrations: %v", err)
	}
	var versions []string
	for _, f := range files {
		versions = append(versions, strings.TrimSuffix(f, ".sql"))
	}
	slices.Sort(versions)
	return versions
}

// scratchPool returns a pool whose connections resolve every table name inside a private
// schema, so a test can migrate from zero without touching the shared test tables.
func scratchPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	shared := testutil.DB(t) // skips when TEST_DATABASE_URL is not set
	ctx := context.Background()
	schema := fmt.Sprintf("migrate_scratch_%d", time.Now().UnixNano())
	if _, err := shared.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create scratch schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := shared.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop scratch schema: %v", err)
		}
	})

	cfg, err := pgxpool.ParseConfig(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("open scratch pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestMigrateSecondRunAppliesNothing(t *testing.T) {
	pool := scratchPool(t)
	ctx := context.Background()
	want := embeddedVersions(t)

	first, err := storage.Migrate(ctx, pool, migrations.FS)
	if err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if !slices.Equal(first, want) {
		t.Fatalf("first migrate applied %v, want %v", first, want)
	}

	second, err := storage.Migrate(ctx, pool, migrations.FS)
	if err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("second migrate applied %v, want nothing", second)
	}

	var recorded int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&recorded); err != nil {
		t.Fatalf("count schema_migrations: %v", err)
	}
	if recorded != len(want) {
		t.Fatalf("schema_migrations has %d rows, want %d", recorded, len(want))
	}
}

func TestMigrateConcurrentRunsApplySchemaOnce(t *testing.T) {
	pool := scratchPool(t)
	ctx := context.Background()
	want := embeddedVersions(t)

	const runners = 2
	var (
		wg      sync.WaitGroup
		start   = make(chan struct{})
		applied [runners][]string
		errs    [runners]error
	)
	for i := range runners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			applied[i], errs[i] = storage.Migrate(ctx, pool, migrations.FS)
		}()
	}
	close(start)
	wg.Wait()

	var all []string
	for i := range runners {
		if errs[i] != nil {
			t.Fatalf("runner %d: %v", i, errs[i])
		}
		all = append(all, applied[i]...)
	}
	slices.Sort(all)
	if !slices.Equal(all, want) {
		t.Fatalf("runners applied %v in total, want each version exactly once: %v", all, want)
	}
}

// pgError unwraps the PostgreSQL error behind err, failing the test when there is none.
func pgError(t *testing.T, err error) *pgconn.PgError {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("want a PostgreSQL error, got %v", err)
	}
	return pgErr
}

// wantRejected asserts the database refused the statement with the given SQLSTATE and, for
// declarative constraints, the constraint name (triggers report their message instead).
func wantRejected(t *testing.T, err error, code, constraintOrMessage string) {
	t.Helper()
	pgErr := pgError(t, err)
	if pgErr.Code != code {
		t.Fatalf("SQLSTATE = %s (%s), want %s", pgErr.Code, pgErr.Message, code)
	}
	if pgErr.ConstraintName != constraintOrMessage && !strings.Contains(pgErr.Message, constraintOrMessage) {
		t.Fatalf("rejected by %q / %q, want %q", pgErr.ConstraintName, pgErr.Message, constraintOrMessage)
	}
}

func mustExec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func insertJob(t *testing.T, pool *pgxpool.Pool, updateID, status string) string {
	t.Helper()
	var id string
	err := pool.QueryRow(context.Background(),
		`INSERT INTO sync_jobs (supplier_id, supplier_update_id, status) VALUES ('SUP-001', $1, $2) RETURNING id::text`,
		updateID, status).Scan(&id)
	if err != nil {
		t.Fatalf("insert %s job %s: %v", status, updateID, err)
	}
	return id
}

func TestSecondActiveJobIsRejectedByDatabase(t *testing.T) {
	pool := testutil.DB(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	insertJob(t, pool, "UPDATE-1", "RUNNING")

	for _, status := range []string{"PENDING", "RUNNING"} {
		_, err := pool.Exec(ctx,
			`INSERT INTO sync_jobs (supplier_id, supplier_update_id, status) VALUES ('SUP-001', $1, $2)`,
			"UPDATE-2-"+status, status)
		if !storage.IsUniqueViolation(err, "sync_jobs_one_active") {
			t.Fatalf("second %s job: want unique violation on sync_jobs_one_active, got %v", status, err)
		}
	}

	// Jobs that are not actively running never compete for the single slot.
	for _, status := range []string{"INTERRUPTED", "NEEDS_ATTENTION", "FAILED"} {
		insertJob(t, pool, "UPDATE-3-"+status, status)
	}
}

func TestCompletedJobRequiresEndOfFeedAndBatchCount(t *testing.T) {
	pool := testutil.DB(t)
	testutil.Reset(t, pool)

	const insertCompleted = `INSERT INTO sync_jobs (supplier_id, supplier_update_id, status, end_of_feed_seen, completed_at, total_batches, processed_batches)
		VALUES ('SUP-001', $1, 'COMPLETED', $2, $3, $4, $5)`
	now := time.Now()
	three := 3
	tests := []struct {
		name        string
		endOfFeed   bool
		completedAt *time.Time
		total       *int
		processed   int
		wantErr     bool
	}{
		{name: "without end of feed", endOfFeed: false, completedAt: &now, processed: 3, wantErr: true},
		{name: "without completion time", endOfFeed: true, completedAt: nil, processed: 3, wantErr: true},
		{name: "fewer batches than announced", endOfFeed: true, completedAt: &now, total: &three, processed: 2, wantErr: true},
		{name: "all announced batches", endOfFeed: true, completedAt: &now, total: &three, processed: 3},
		{name: "unannounced total", endOfFeed: true, completedAt: &now, processed: 2},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := pool.Exec(context.Background(), insertCompleted,
				fmt.Sprintf("UPDATE-%d", i), tc.endOfFeed, tc.completedAt, tc.total, tc.processed)
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("valid completed job rejected: %v", err)
				}
				return
			}
			wantRejected(t, err, checkViolation, "sync_jobs_completed_gate")
		})
	}
}

func TestActiveProductRequiresDetailsButTombstoneDoesNot(t *testing.T) {
	pool := testutil.DB(t)
	testutil.Reset(t, pool)

	const insertProduct = `INSERT INTO products (supplier_id, product_id, status, revision, name, price_minor, currency, payload_hash, withdrawn_at)
		VALUES ('SUP-001', $1, $2, 1, $3, $4, $5, 'h', $6)`
	now := time.Now()
	name, blank, price, usd := "Widget", "   ", int64(1250), "USD"
	tests := []struct {
		id      string
		status  string
		name    *string
		price   *int64
		cur     *string
		wdAt    *time.Time
		wantErr bool
	}{
		{id: "no-name", status: "ACTIVE", price: &price, cur: &usd, wantErr: true},
		{id: "blank-name", status: "ACTIVE", name: &blank, price: &price, cur: &usd, wantErr: true},
		{id: "no-price", status: "ACTIVE", name: &name, cur: &usd, wantErr: true},
		{id: "no-currency", status: "ACTIVE", name: &name, price: &price, wantErr: true},
		{id: "complete", status: "ACTIVE", name: &name, price: &price, cur: &usd},
		{id: "tombstone-without-details", status: "WITHDRAWN", wdAt: &now},
	}
	for _, tc := range tests {
		t.Run(tc.id, func(t *testing.T) {
			_, err := pool.Exec(context.Background(), insertProduct, tc.id, tc.status, tc.name, tc.price, tc.cur, tc.wdAt)
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("valid product rejected: %v", err)
				}
				return
			}
			wantRejected(t, err, checkViolation, "products_active_details")
		})
	}
}

func TestProductGuardRejectsRevisionRegressionAndResurrection(t *testing.T) {
	pool := testutil.DB(t)
	testutil.Reset(t, pool)

	mustExec(t, pool, `INSERT INTO products (supplier_id, product_id, status, revision, name, price_minor, currency, payload_hash)
		VALUES ('SUP-001', 'ACTIVE-5', 'ACTIVE', 5, 'Widget', 100, 'USD', 'h')`)
	mustExec(t, pool, `INSERT INTO products (supplier_id, product_id, status, revision, payload_hash, withdrawn_at)
		VALUES ('SUP-001', 'GONE-6', 'WITHDRAWN', 6, 'h', now())`)

	rejected := []struct {
		name, sql, want string
	}{
		{"same revision rewrite", `UPDATE products SET name = 'Other', revision = 5 WHERE product_id = 'ACTIVE-5'`, "revision must increase"},
		{"lower revision", `UPDATE products SET revision = 4 WHERE product_id = 'ACTIVE-5'`, "revision must increase"},
		{"touching a row without raising its revision", `UPDATE products SET revision = revision WHERE product_id = 'GONE-6'`, "revision must increase"},
		{"tombstone back to active at the same revision", `UPDATE products SET status = 'ACTIVE', withdrawn_at = NULL, name = 'Back', price_minor = 1, currency = 'USD' WHERE product_id = 'GONE-6'`, "revision must increase"},
		{"tombstone back to active at a higher revision", `UPDATE products SET status = 'ACTIVE', withdrawn_at = NULL, name = 'Back', price_minor = 1, currency = 'USD', revision = 9 WHERE product_id = 'GONE-6'`, "WITHDRAWN is terminal"},
	}
	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			_, err := pool.Exec(context.Background(), tc.sql)
			wantRejected(t, err, checkViolation, tc.want)
		})
	}

	t.Run("higher revision is accepted", func(t *testing.T) {
		mustExec(t, pool, `UPDATE products SET name = 'Widget v6', revision = 6 WHERE product_id = 'ACTIVE-5'`)
	})
	t.Run("tombstone may advance its revision", func(t *testing.T) {
		mustExec(t, pool, `UPDATE products SET revision = 8 WHERE product_id = 'GONE-6'`)
	})
}

func TestCompletionGuardRejectsCompletedWithOpenBlockingIssue(t *testing.T) {
	pool := testutil.DB(t)
	testutil.Reset(t, pool)

	jobID := insertJob(t, pool, "UPDATE-1", "RUNNING")
	mustExec(t, pool, `UPDATE sync_jobs SET end_of_feed_seen = true WHERE id = $1::uuid`, jobID)

	const insertIssue = `INSERT INTO sync_issues (job_id, cursor, code, severity, message, fingerprint)
		VALUES ($1::uuid, 'cursor-002', $2, $3, 'problem', $2 || '|cursor-002')`
	const complete = `UPDATE sync_jobs SET status = 'COMPLETED', completed_at = now() WHERE id = $1::uuid`

	mustExec(t, pool, insertIssue, jobID, "INVALID_PRODUCT_DATA", "BLOCKING")
	_, err := pool.Exec(context.Background(), complete, jobID)
	wantRejected(t, err, checkViolation, "cannot complete with open blocking issues")

	// Resolving the blocking issue and leaving only a transient one open allows completion:
	// a retried outage must not keep a finished feed from completing.
	mustExec(t, pool, `UPDATE sync_issues SET status = 'RESOLVED', resolved_at = now() WHERE job_id = $1::uuid`, jobID)
	mustExec(t, pool, insertIssue, jobID, "SUPPLIER_UNAVAILABLE", "TRANSIENT")
	mustExec(t, pool, complete, jobID)
}
