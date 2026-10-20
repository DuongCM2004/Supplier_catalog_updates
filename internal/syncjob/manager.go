package syncjob

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/storage"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/supplier"
)

// Manager owns the workers of this process: it starts and resumes jobs, runs one goroutine
// per running job, and shuts them down. All durable state lives in PostgreSQL; the worker
// goroutines and the live set are disposable and rebuilt from the database after a restart.
//
// Ceiling: the database admits a single PENDING/RUNNING job and the Manager assumes it is
// the only process doing so. Running several instances would need a lease or heartbeat on
// the job row so one instance cannot adopt another's job.
type Manager struct {
	pool *pgxpool.Pool
	sup  supplier.Client
	cfg  Config
	log  *slog.Logger
	proc *Processor

	// baseCtx is the one place a context is kept in a struct: workers outlive the request
	// that started them, so they run on a context that only Shutdown cancels.
	baseCtx context.Context
	cancel  context.CancelFunc

	mu   sync.Mutex
	live map[string]chan struct{} // jobs with a worker in this process; closed when that worker has fully finished
	wg   sync.WaitGroup

	cleanupBackoff time.Duration // first wait between attempts to un-strand a job; doubles up to cleanupMaxBackoff
}

// NewManager returns a Manager that fetches from sup and stores everything in pool. Unset
// cfg values take production defaults.
func NewManager(pool *pgxpool.Pool, sup supplier.Client, cfg Config, hooks Hooks, log *slog.Logger) *Manager {
	if log == nil {
		log = slog.Default()
	}
	baseCtx, cancel := context.WithCancel(context.Background())
	return &Manager{
		pool:           pool,
		sup:            sup,
		cfg:            cfg.withDefaults(),
		log:            log,
		proc:           NewProcessor(pool, hooks, log),
		baseCtx:        baseCtx,
		cancel:         cancel,
		live:           map[string]chan struct{}{},
		cleanupBackoff: 500 * time.Millisecond,
	}
}

// Start begins syncing an update. A new update gets a PENDING job and a worker, and
// created is true. An update that already has a job returns that job with created=false and
// is not touched, whatever state it is in: resuming is always an explicit Resume. Starting
// a different update while another job is PENDING/RUNNING returns ErrActiveJobExists together
// with the active job (zero Job if it just ended).
func (m *Manager) Start(ctx context.Context, supplierID, updateID string) (Job, bool, error) {
	// The lock covers insert and spawn, so a Resume of the new job cannot slip in between
	// and mistake it for an orphan that has no worker yet.
	m.mu.Lock()
	defer m.mu.Unlock()
	job, created, err := createJob(ctx, m.pool, supplierID, updateID)
	if err != nil || !created {
		return job, false, err
	}
	m.log.Info("job.created", "job_id", job.ID, "update_id", updateID, "supplier_id", supplierID)
	m.spawn(job.ID)
	return job, true, nil
}

// Resume restarts a job from its saved cursor. INTERRUPTED and NEEDS_ATTENTION jobs can be
// resumed. A job that is PENDING/RUNNING but has no worker in this process is an orphan (its
// worker could not record how it ended, for example while the database was down) and is
// adopted the same way, so no application restart is needed. A job whose worker is alive,
// or that is COMPLETED/FAILED, returns ErrInvalidTransition; another active job returns
// ErrActiveJobExists; an unknown id returns ErrNotFound.
func (m *Manager) Resume(ctx context.Context, jobID string) (Job, error) {
	// Held from the live-set check to spawn, so concurrent resumes of one orphan start
	// exactly one worker.
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.awaitWorkerExit(ctx, jobID); err != nil {
		return Job{}, err
	}
	prev, err := GetJob(ctx, m.pool, jobID)
	if err != nil {
		return Job{}, err
	}
	from := []Status{StatusInterrupted, StatusNeedsAttention, StatusPending, StatusRunning}
	switch err := transition(ctx, m.pool, jobID, from, StatusRunning, nil); {
	case storage.IsUniqueViolation(err, oneActiveIndex):
		return Job{}, fmt.Errorf("resume job %s: %w", jobID, ErrActiveJobExists)
	case err != nil:
		return Job{}, err
	}
	m.log.Info("job.transition", "job_id", jobID, "update_id", prev.UpdateID,
		"from", prev.Status, "to", StatusRunning, "reason", "resumed")
	// The snapshot is read before the worker exists, so it is the RUNNING state this call
	// created. Reading after spawn would let a fast worker finish, park or interrupt the job
	// first and make the caller see the outcome of the run instead of its start.
	snapshot, readErr := GetJob(ctx, m.pool, jobID)
	// The job is RUNNING in the database now, so it needs its worker even if the read failed.
	m.spawn(jobID)
	if readErr != nil {
		return Job{}, readErr
	}
	return snapshot, nil
}

// awaitWorkerExit is called with m.mu held and returns with it held. A worker that has
// already recorded its job as INTERRUPTED or NEEDS_ATTENTION is still in the live set for a
// moment while it unwinds; resuming now would let its final cleanup undo the new run, so
// Resume waits for it to finish. A worker whose job is PENDING/RUNNING is genuinely
// working, and that is refused.
func (m *Manager) awaitWorkerExit(ctx context.Context, jobID string) error {
	for {
		done, live := m.live[jobID]
		if !live {
			return nil
		}
		job, err := GetJob(ctx, m.pool, jobID)
		if err != nil {
			return err
		}
		if job.Status == StatusPending || job.Status == StatusRunning {
			return fmt.Errorf("job %s has a running worker: %w", jobID, ErrInvalidTransition)
		}
		m.mu.Unlock()
		select {
		case <-done:
			m.mu.Lock()
		case <-ctx.Done():
			m.mu.Lock()
			return fmt.Errorf("wait for the worker of job %s to finish: %w", jobID, ctx.Err())
		}
	}
}

// Reconcile marks every job that was PENDING or RUNNING as INTERRUPTED and returns their
// ids. Call it once at startup, before the HTTP listener opens and before any worker exists:
// at that moment such a job belongs to a process that no longer runs. Resuming stays an
// explicit action.
//
// Ceiling: this is only correct with a single application instance; with several, one
// instance would interrupt jobs another is running. A lease or heartbeat column on the
// job row is the upgrade path.
func (m *Manager) Reconcile(ctx context.Context) ([]string, error) {
	ids, err := reconcile(ctx, m.pool, "interrupted: the service restarted while this job was active")
	if err != nil {
		return nil, err
	}
	if ids == nil {
		ids = []string{}
	}
	level := slog.LevelInfo
	if len(ids) > 0 {
		level = slog.LevelWarn
	}
	m.log.Log(ctx, level, "startup.reconciled", "count", len(ids), "job_ids", ids)
	return ids, nil
}

// Wait blocks until every worker spawned so far has returned.
func (m *Manager) Wait() { m.wg.Wait() }

// Shutdown tells every worker to stop and waits for them, up to ctx. A stopping worker
// records its job as INTERRUPTED without an issue, so the job can simply be resumed.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.cancel()
	finished := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("shutdown: workers are still running: %w", ctx.Err())
	}
}

// spawn starts the worker goroutine for a job whose state is already PENDING or RUNNING.
// The caller holds m.mu. The job stays in the live set until its worker has finished all
// cleanup, which is what Resume's orphan check relies on.
func (m *Manager) spawn(jobID string) {
	done := make(chan struct{})
	m.live[jobID] = done
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer m.forget(jobID, done)
		defer func() {
			// A bug in a worker must not take the service down or leave a job RUNNING forever.
			if r := recover(); r != nil {
				m.log.Error("job.panic", "job_id", jobID, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
				m.ensureNotRunning(jobID, fmt.Sprintf("the worker panicked: %v", r))
			}
		}()
		err := m.RunJob(m.baseCtx, jobID)
		if errors.Is(err, ErrSimulatedCrash) {
			return // act like a dead process: leave the database exactly as it is
		}
		if err != nil {
			m.log.Error("job.worker_error", "job_id", jobID, "error", err.Error())
		}
		m.ensureNotRunning(jobID, "the worker stopped before the job finished")
	}()
}

func (m *Manager) forget(jobID string, done chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.live[jobID] == done {
		delete(m.live, jobID)
	}
	close(done)
}

const (
	cleanupRetries    = 5 // after the first attempt: waits of 0.5s, 1s, 2s, 4s, 8s add up to about 15s
	cleanupMaxBackoff = 8 * time.Second
)

// ensureNotRunning is the safety net after a worker exits: a job still PENDING/RUNNING
// without a worker would block every other job, so it is set to INTERRUPTED. It does nothing
// when the worker already recorded how the job ended. When the database is unreachable (the
// usual reason a worker could not record it) it keeps trying for about 15 seconds; after
// that the job stays stranded until Resume adopts it as an orphan.
func (m *Manager) ensureNotRunning(jobID, reason string) {
	ctx := context.WithoutCancel(m.baseCtx) // must still work while shutting down
	backoff := m.cleanupBackoff
	for attempt := 0; ; attempt++ {
		callCtx, cancel := context.WithTimeout(ctx, settleTimeout)
		err := transition(callCtx, m.pool, jobID, []Status{StatusPending, StatusRunning}, StatusInterrupted, &reason)
		cancel()
		switch {
		case err == nil:
			m.log.Warn("job.interrupted", "job_id", jobID, "reason", reason)
			return
		case errors.Is(err, ErrInvalidTransition), errors.Is(err, ErrNotFound):
			return // already recorded, or the job is gone
		case !storage.IsConnectionError(err) || attempt >= cleanupRetries:
			m.log.Error("job.cleanup_failed", "job_id", jobID, "attempts", attempt+1, "error", err.Error())
			return
		}
		time.Sleep(backoff)
		backoff = min(backoff*2, cleanupMaxBackoff)
	}
}
