// Package testutil bootstraps a real PostgreSQL test database and provides fakes and
// invariant assertions. Only test code imports it.
package testutil

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/storage"
	"github.com/DuongCM2004/resilient-catalog-sync/migrations"
)

var (
	setupOnce sync.Once
	sharedDB  *pgxpool.Pool
	setupErr  error
)

// DB returns a pool on the database named in TEST_DATABASE_URL, creating and migrating it
// once per test binary. Without the variable the test is skipped with an explicit message,
// so `go test ./...` stays green on machines without PostgreSQL but never pretends to pass.
func DB(t testing.TB) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("integration test skipped: TEST_DATABASE_URL not set")
	}
	setupOnce.Do(func() { sharedDB, setupErr = setup(url) })
	if setupErr != nil {
		t.Fatalf("test database: %v", setupErr)
	}
	return sharedDB
}

// Reset empties every table so each test starts from a blank catalog and job history.
func Reset(t testing.TB, pool *pgxpool.Pool) {
	t.Helper()
	const truncate = `TRUNCATE sync_changes, sync_batches, sync_issues, sync_jobs, products RESTART IDENTITY CASCADE`
	if _, err := pool.Exec(context.Background(), truncate); err != nil {
		t.Fatalf("reset test database: %v", err)
	}
}

func setup(url string) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := ensureDatabase(ctx, url); err != nil {
		return nil, err
	}
	pool, err := storage.Open(ctx, url, 20*time.Second)
	if err != nil {
		return nil, err
	}
	if _, err := storage.Migrate(ctx, pool, migrations.FS); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// ensureDatabase creates the database named in url (e.g. a lane's own test database) via
// the server's maintenance database when it does not exist yet.
func ensureDatabase(ctx context.Context, url string) error {
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		return fmt.Errorf("parse TEST_DATABASE_URL: %w", err)
	}
	name := cfg.Database
	cfg.Database = "postgres"
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect to maintenance database: %w", err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
	var pgErr *pgconn.PgError
	// 42P04: already exists. 23505: a parallel test binary created it at the same moment.
	if err != nil && !(errors.As(err, &pgErr) && (pgErr.Code == "42P04" || pgErr.Code == "23505")) {
		return fmt.Errorf("create database %s: %w", name, err)
	}
	return nil
}
