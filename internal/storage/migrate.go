package storage

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// migrationLockID serialises concurrent Migrate calls (app start, parallel test packages).
const migrationLockID = 727001

const createMigrationsTable = `CREATE TABLE IF NOT EXISTS schema_migrations (
  version    TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`

// Migrate applies every embedded NNN_*.sql file not yet recorded in schema_migrations, in
// lexical order, each in its own transaction together with its version row. Re-running it
// is a no-op; concurrent callers wait on an advisory lock instead of racing.
func Migrate(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS) (applied []string, err error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("migrate: acquire connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return nil, fmt.Errorf("migrate: take advisory lock: %w", err)
	}
	defer func() {
		// A background context so the lock is released even when ctx was cancelled.
		if _, unlockErr := conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationLockID); unlockErr != nil && err == nil {
			err = fmt.Errorf("migrate: release advisory lock: %w", unlockErr)
		}
	}()

	if _, err := conn.Exec(ctx, createMigrationsTable); err != nil {
		return nil, fmt.Errorf("migrate: create schema_migrations: %w", err)
	}

	files, err := fs.Glob(fsys, "*.sql")
	if err != nil {
		return nil, fmt.Errorf("migrate: list files: %w", err)
	}
	sort.Strings(files)

	for _, file := range files {
		version := strings.TrimSuffix(file, ".sql")
		var done bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, version).Scan(&done); err != nil {
			return applied, fmt.Errorf("migrate: check %s: %w", version, err)
		}
		if done {
			continue
		}
		sql, err := fs.ReadFile(fsys, file)
		if err != nil {
			return applied, fmt.Errorf("migrate: read %s: %w", file, err)
		}
		err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, string(sql)); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version)
			return err
		})
		if err != nil {
			return applied, fmt.Errorf("migrate: apply %s: %w", version, err)
		}
		applied = append(applied, version)
	}
	return applied, nil
}
