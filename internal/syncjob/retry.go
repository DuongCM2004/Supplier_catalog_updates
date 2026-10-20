package syncjob

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/supplier"
)

// withDefaults fills unset retry settings with production values.
func (c Config) withDefaults() Config {
	if c.RetryMaxAttempts <= 0 {
		c.RetryMaxAttempts = 4
	}
	if c.RetryBaseDelay <= 0 {
		c.RetryBaseDelay = 200 * time.Millisecond
	}
	if c.RetryMaxDelay <= 0 {
		c.RetryMaxDelay = 2 * time.Second
	}
	if c.SupplierTimeout <= 0 {
		c.SupplierTimeout = 2 * time.Second
	}
	return c
}

// retryError is the last supplier error after every attempt failed.
type retryError struct {
	op       string
	attempts int
	err      error
}

func (e *retryError) Error() string {
	return fmt.Sprintf("%s failed after %d attempts: %v", e.op, e.attempts, e.err)
}

func (e *retryError) Unwrap() error { return e.err }

// attemptsOf reports how many calls withRetry made before giving up (1 when the error was
// not retried at all).
func attemptsOf(err error) int {
	var re *retryError
	if errors.As(err, &re) {
		return re.attempts
	}
	return 1
}

// withRetry calls a supplier operation with a per-call timeout and bounded exponential
// backoff. Only outages and timeouts are retried: an unknown update/cursor or an
// undecodable payload fails the same way every time, so those return at once.
// Supplier I/O never runs inside a database transaction, so waiting here holds no locks.
func withRetry(ctx context.Context, cfg Config, log *slog.Logger, op string, call func(context.Context) error) error {
	cfg = cfg.withDefaults()
	for attempt := 1; ; attempt++ {
		callCtx, cancel := context.WithTimeout(ctx, cfg.SupplierTimeout)
		err := call(callCtx)
		cancel()
		switch {
		case err == nil:
			return nil
		case ctx.Err() != nil:
			return fmt.Errorf("%s: %w", op, ctx.Err())
		case !isTransient(ctx, err):
			return err
		case attempt >= cfg.RetryMaxAttempts:
			return &retryError{op: op, attempts: attempt, err: err}
		}
		delay := backoffDelay(cfg, attempt)
		if log != nil {
			log.Warn("supplier.retry", "op", op, "attempt", attempt, "max_attempts", cfg.RetryMaxAttempts,
				"delay_ms", delay.Milliseconds(), "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", op, ctx.Err())
		case <-time.After(delay):
		}
	}
}

// isTransient: the supplier said it is unavailable, or one call timed out while the caller
// is still alive (a cancelled caller is shutdown, not a supplier problem).
func isTransient(ctx context.Context, err error) bool {
	if errors.Is(err, supplier.ErrNotFound) || errors.Is(err, supplier.ErrMalformed) {
		return false
	}
	return errors.Is(err, supplier.ErrUnavailable) ||
		(errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil)
}

// backoffDelay doubles from the base delay and is capped at the max delay.
// Deliberately no jitter so retry timing is reproducible in tests and demos; add jitter
// when many workers share one supplier and could retry in lockstep.
func backoffDelay(cfg Config, attempt int) time.Duration {
	if attempt-1 >= 30 {
		return cfg.RetryMaxDelay
	}
	d := cfg.RetryBaseDelay << (attempt - 1)
	if d <= 0 || d > cfg.RetryMaxDelay {
		return cfg.RetryMaxDelay
	}
	return d
}
