// Package storage opens the PostgreSQL pool, applies embedded migrations and classifies
// database errors into "retry later" and "retrying cannot help".
package storage

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DB is the query surface shared by *pgxpool.Pool and pgx.Tx, so read functions work both
// inside and outside a transaction.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

const pingInterval = 500 * time.Millisecond

// Open creates the pool and pings until the database answers or wait elapses, so the app
// can start while the database container is still booting.
func Open(ctx context.Context, url string, wait time.Duration) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("storage: open pool: %w", err)
	}
	deadline := time.Now().Add(wait)
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err = pool.Ping(pingCtx)
		cancel()
		if err == nil {
			return pool, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			pool.Close()
			return nil, fmt.Errorf("storage: database not reachable after %s: %w", wait, err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(pingInterval):
		}
	}
}

// IsUniqueViolation reports whether err is SQLSTATE 23505 on the named constraint or index.
func IsUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == constraint
}

// IsConnectionError reports failures where the same work may succeed later: network
// errors, timeouts, connection exceptions (class 08) and server shutdown (57P01).
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return strings.HasPrefix(pgErr.Code, "08") || pgErr.Code == "57P01" || pgErr.Code == "57P02" || pgErr.Code == "57P03"
	}
	var netErr net.Error
	if errors.As(err, &netErr) || pgconn.SafeToRetry(err) || pgconn.Timeout(err) {
		return true
	}
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return true
	}
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, pgx.ErrTxClosed) ||
		strings.Contains(err.Error(), "conn closed") || strings.Contains(err.Error(), "closed pool")
}

// IsRetryable reports failures where running the same work again later can succeed: every
// connection error, plus the SQLSTATE classes whose cause is load or timing rather than the
// statement: 40 (the transaction was rolled back: serialization failure, deadlock), 53
// (insufficient resources: too many connections, out of memory or disk) and 57 (operator
// intervention, including 57014 when a statement timeout cancels the query). Anything else,
// whether a data error, a bug or a client-side encoding failure, would fail the same way
// every time.
func IsRetryable(err error) bool {
	if IsConnectionError(err) {
		return true
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	for _, class := range []string{"40", "53", "57"} {
		if strings.HasPrefix(pgErr.Code, class) {
			return true
		}
	}
	return false
}

// IsDataError reports SQLSTATE class 22 (data exception) or 23 (integrity constraint):
// the statement itself is wrong, so retrying the same batch can never succeed.
func IsDataError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (strings.HasPrefix(pgErr.Code, "22") || strings.HasPrefix(pgErr.Code, "23"))
}
