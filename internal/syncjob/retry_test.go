package syncjob

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/supplier"
)

func fastRetryConfig() Config {
	return Config{RetryMaxAttempts: 4, RetryBaseDelay: time.Millisecond, RetryMaxDelay: 4 * time.Millisecond, SupplierTimeout: 50 * time.Millisecond}
}

func TestWithRetrySucceedsAfterTransientFailures(t *testing.T) {
	calls := 0
	err := withRetry(context.Background(), fastRetryConfig(), nil, "fetch_batch", func(context.Context) error {
		calls++
		if calls < 3 {
			return supplier.ErrUnavailable
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("err=%v calls=%d, want nil after 3 calls", err, calls)
	}
}

func TestWithRetryDoesNotRetryPermanentErrors(t *testing.T) {
	for _, permanent := range []error{supplier.ErrNotFound, supplier.ErrMalformed} {
		calls := 0
		err := withRetry(context.Background(), fastRetryConfig(), nil, "fetch_batch", func(context.Context) error {
			calls++
			return permanent
		})
		if !errors.Is(err, permanent) || calls != 1 {
			t.Fatalf("%v: err=%v calls=%d, want the same error after exactly 1 call", permanent, err, calls)
		}
	}
}

func TestWithRetryGivesUpAfterMaxAttempts(t *testing.T) {
	calls := 0
	err := withRetry(context.Background(), fastRetryConfig(), nil, "fetch_batch", func(context.Context) error {
		calls++
		return supplier.ErrUnavailable
	})
	if calls != 4 || !errors.Is(err, supplier.ErrUnavailable) || attemptsOf(err) != 4 {
		t.Fatalf("err=%v calls=%d attempts=%d, want 4 calls wrapping ErrUnavailable", err, calls, attemptsOf(err))
	}
}

func TestWithRetryStopsWhenCallerIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := fastRetryConfig()
	cfg.RetryBaseDelay, cfg.RetryMaxDelay = time.Hour, time.Hour // would block forever without cancellation
	calls := 0
	done := make(chan error, 1)
	go func() {
		done <- withRetry(ctx, cfg, nil, "fetch_batch", func(context.Context) error {
			calls++
			cancel() // shutdown arrives while the first attempt fails
			return supplier.ErrUnavailable
		})
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || calls != 1 {
			t.Fatalf("err=%v calls=%d, want context.Canceled after 1 call", err, calls)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("withRetry kept waiting after the caller was cancelled")
	}
}

func TestWithRetryTreatsPerCallTimeoutAsTransient(t *testing.T) {
	calls := 0
	err := withRetry(context.Background(), fastRetryConfig(), nil, "fetch_batch", func(c context.Context) error {
		calls++
		if calls == 1 {
			<-c.Done() // supplier hangs until this call's own deadline
			return c.Err()
		}
		return nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("err=%v calls=%d, want success on the second call", err, calls)
	}
}

func TestBackoffDelayDoublesUpToTheCap(t *testing.T) {
	cfg := Config{RetryBaseDelay: 200 * time.Millisecond, RetryMaxDelay: 2 * time.Second}
	want := []time.Duration{200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, 1600 * time.Millisecond, 2 * time.Second, 2 * time.Second}
	for i, w := range want {
		if got := backoffDelay(cfg, i+1); got != w {
			t.Errorf("attempt %d: delay %v, want %v", i+1, got, w)
		}
	}
	if got := backoffDelay(cfg, 64); got != cfg.RetryMaxDelay {
		t.Errorf("large attempt: delay %v, want cap %v", got, cfg.RetryMaxDelay)
	}
}
