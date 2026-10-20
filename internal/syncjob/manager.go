package syncjob

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

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
