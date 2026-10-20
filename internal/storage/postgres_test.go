package storage_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/storage"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/testutil"
)

func TestErrorClassification(t *testing.T) {
	pg := func(code, constraint string) error {
		return fmt.Errorf("apply batch: %w", &pgconn.PgError{Code: code, ConstraintName: constraint, Message: "boom"})
	}
	tests := []struct {
		name       string
		err        error
		uniqueOnce bool // IsUniqueViolation(err, "sync_jobs_one_active")
		connection bool
		data       bool
	}{
		{name: "no error"},
		{name: "unrelated error", err: errors.New("boom")},
		{name: "unique violation on the named constraint", err: pg("23505", "sync_jobs_one_active"), uniqueOnce: true, data: true},
		{name: "unique violation on another constraint", err: pg("23505", "sync_jobs_update_uq"), data: true},
		{name: "check violation", err: pg("23514", "products_active_details"), data: true},
		{name: "NUL byte in text", err: pg("22021", ""), data: true},
		{name: "connection exception", err: pg("08006", ""), connection: true},
		{name: "server shutting down", err: pg("57P01", ""), connection: true},
		{name: "deadlock is neither a data nor a connection error", err: pg("40P01", "")},
		{name: "network failure", err: fmt.Errorf("query: %w", &net.OpError{Op: "read", Err: errors.New("connection reset")}), connection: true},
		{name: "database call timed out", err: fmt.Errorf("query: %w", context.DeadlineExceeded), connection: true},
		{name: "shutdown cancellation is not an outage", err: fmt.Errorf("query: %w", context.Canceled)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := storage.IsUniqueViolation(tc.err, "sync_jobs_one_active"); got != tc.uniqueOnce {
				t.Errorf("IsUniqueViolation = %t, want %t", got, tc.uniqueOnce)
			}
			if got := storage.IsConnectionError(tc.err); got != tc.connection {
				t.Errorf("IsConnectionError = %t, want %t", got, tc.connection)
			}
			if got := storage.IsDataError(tc.err); got != tc.data {
				t.Errorf("IsDataError = %t, want %t", got, tc.data)
			}
		})
	}
}

func TestIsRetryableCoversConnectionLoadAndTimingFailuresOnly(t *testing.T) {
	pg := func(code string) error {
		return fmt.Errorf("apply batch: %w", &pgconn.PgError{Code: code, Message: "boom"})
	}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"no error", nil, false},
		{"connection exception", pg("08006"), true},
		{"server shutting down", pg("57P01"), true},
		{"network failure", fmt.Errorf("query: %w", &net.OpError{Op: "read", Err: errors.New("connection reset")}), true},
		{"database call timed out", fmt.Errorf("query: %w", context.DeadlineExceeded), true},
		{"serialization failure", pg("40001"), true},
		{"deadlock", pg("40P01"), true},
		{"too many connections", pg("53300"), true},
		{"disk full", pg("53100"), true},
		{"statement timeout", pg("57014"), true},
		{"data exception", pg("22003"), false},
		{"NUL byte in text", pg("22021"), false},
		{"check violation", pg("23514"), false},
		{"undefined column", pg("42703"), false},
		{"internal error", pg("XX000"), false},
		{"client-side encoding failure", errors.New("unable to encode 3000000000 into binary format for int4 (OID 23)"), false},
		{"shutdown cancellation", fmt.Errorf("query: %w", context.Canceled), false},
		{"no row", fmt.Errorf("get job: %w", pgx.ErrNoRows), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := storage.IsRetryable(tc.err); got != tc.want {
				t.Errorf("IsRetryable(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

// The classifier must agree with what PostgreSQL really reports, not only with the codes
// this file assumes.
func TestErrorClassificationMatchesRealDatabaseFailures(t *testing.T) {
	pool := testutil.DB(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	t.Run("NUL byte is a permanent data error", func(t *testing.T) {
		_, err := pool.Exec(ctx, `SELECT $1::text`, "wid\x00get")
		if !storage.IsDataError(err) || storage.IsConnectionError(err) {
			t.Fatalf("data=%t connection=%t for %v, want data only", storage.IsDataError(err), storage.IsConnectionError(err), err)
		}
	})

	t.Run("duplicate key names its constraint", func(t *testing.T) {
		const insert = `INSERT INTO products (supplier_id, product_id, status, revision, name, price_minor, currency, payload_hash)
			VALUES ('SUP-001', 'A', 'ACTIVE', 1, 'Widget', 1, 'USD', 'h')`
		mustExec(t, pool, insert)
		_, err := pool.Exec(ctx, insert)
		if !storage.IsUniqueViolation(err, "products_pkey") || storage.IsUniqueViolation(err, "sync_jobs_one_active") {
			t.Fatalf("IsUniqueViolation for %v", err)
		}
	})

	t.Run("a backend killed mid-session is retryable", func(t *testing.T) {
		// A dedicated connection, not a pool, so the query below cannot quietly move to a
		// fresh connection.
		victim, err := pgx.Connect(ctx, os.Getenv("TEST_DATABASE_URL"))
		if err != nil {
			t.Fatalf("connect victim: %v", err)
		}
		defer victim.Close(ctx)
		var pid int
		if err := victim.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
			t.Fatalf("backend pid: %v", err)
		}
		if _, err := pool.Exec(ctx, `SELECT pg_terminate_backend($1)`, pid); err != nil {
			t.Fatalf("terminate backend: %v", err)
		}
		// Termination is asynchronous: wait until the server has really dropped the session,
		// otherwise the query below could still be answered.
		for deadline := time.Now().Add(5 * time.Second); ; {
			var alive int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE pid = $1`, pid).Scan(&alive); err != nil {
				t.Fatalf("check backend: %v", err)
			}
			if alive == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("backend %d still running after pg_terminate_backend", pid)
			}
			time.Sleep(20 * time.Millisecond)
		}

		// Whether the client notices the dead session while writing or while reading, it is
		// the same kind of retryable outage.
		_, err = victim.Exec(ctx, `SELECT 1`)
		if err == nil {
			t.Fatal("query on a terminated backend succeeded")
		}
		if !storage.IsConnectionError(err) || storage.IsDataError(err) {
			t.Fatalf("connection=%t data=%t for %v, want connection only", storage.IsConnectionError(err), storage.IsDataError(err), err)
		}
	})
}
