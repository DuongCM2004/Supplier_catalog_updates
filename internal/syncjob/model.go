// Package syncjob runs durable supplier sync jobs: it stores job state, applies each
// supplier batch in one transaction together with its checkpoint, and resumes from the
// persisted cursor after an outage or restart.
package syncjob

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// Status is a sync job state. COMPLETED and FAILED are terminal.
type Status string

const (
	StatusPending        Status = "PENDING"
	StatusRunning        Status = "RUNNING"
	StatusInterrupted    Status = "INTERRUPTED"
	StatusNeedsAttention Status = "NEEDS_ATTENTION"
	StatusCompleted      Status = "COMPLETED"
	StatusFailed         Status = "FAILED"
)

// IssueCode names why a job stopped or a batch was rejected.
type IssueCode string

const (
	CodeSupplierUnavailable IssueCode = "SUPPLIER_UNAVAILABLE"
	CodeDatabaseError       IssueCode = "DATABASE_ERROR"
	CodeInvalidProductData  IssueCode = "INVALID_PRODUCT_DATA"
	CodeVersionConflict     IssueCode = "VERSION_CONFLICT"
	CodeBatchConflict       IssueCode = "BATCH_CONFLICT"
	CodeInvalidCursor       IssueCode = "INVALID_CURSOR"
	CodeInvalidBatch        IssueCode = "INVALID_BATCH"
	CodeReactivationBlocked IssueCode = "REACTIVATION_BLOCKED"
	CodeUnknownUpdate       IssueCode = "UNKNOWN_UPDATE"
)

// Severity decides whether an issue blocks completion. BLOCKING issues keep a job from
// ever reaching COMPLETED while they are open; TRANSIENT ones resolve on the next commit.
type Severity string

const (
	SeverityBlocking  Severity = "BLOCKING"
	SeverityTransient Severity = "TRANSIENT"
)

// Issue is a recorded problem. Every issue carries the cursor the job was at when it was
// raised, so it resolves automatically once the batch at that cursor commits.
type Issue struct {
	ID, JobID               string
	BatchID, Cursor         *string
	SupplierID, ProductID   *string
	Code                    IssueCode
	Severity                Severity
	Status                  string // OPEN | RESOLVED
	Message                 string
	RetryCount              int
	FirstSeenAt, LastSeenAt time.Time
	ResolvedAt              *time.Time
}

// Change is one audit row for an applied catalog event.
type Change struct {
	ID, JobID, BatchID, SupplierID, ProductID string
	Revision                                  int64
	ChangeType                                string          // CREATED | UPDATED | WITHDRAWN
	Before                                    json.RawMessage // null when the row was absent
	After                                     json.RawMessage
	CreatedAt                                 time.Time
}

// OutcomeKind is what ProcessBatch did with one fetched batch.
type OutcomeKind int

const (
	KindCommitted OutcomeKind = iota + 1 // applied and checkpointed
	KindReplayed                         // already committed earlier: no-op
	KindStale                            // the job moved on (another worker or state change): no-op
	KindBlocked                          // rejected: rolled back and recorded as a blocking issue
)

// Counts tallies decisions for one batch or one job.
type Counts struct{ Created, Updated, Withdrawn, Duplicate, Stale int }

// Outcome is the result of processing one batch.
type Outcome struct {
	Kind   OutcomeKind
	Counts Counts
	Status Status // job status after a committed batch
}

// Job is the durable state of one supplier update sync.
type Job struct {
	ID, SupplierID, UpdateID                      string
	Status                                        Status
	NextCursor                                    string
	LastBatchID                                   *string
	ProcessedBatches                              int
	TotalBatches                                  *int
	EndOfFeedSeen                                 bool
	Created, Updated, Withdrawn, Duplicate, Stale int
	OpenBlockingIssues                            int // filled by read queries
	LastError                                     *string
	CreatedAt, UpdatedAt                          time.Time
	StartedAt, CompletedAt                        *time.Time
}

var (
	ErrNotFound          = errors.New("job not found")
	ErrActiveJobExists   = errors.New("another sync job is active")
	ErrInvalidTransition = errors.New("invalid job state transition")
	// ErrSimulatedCrash is returned by test hooks: the worker abandons the job without
	// touching its state, exactly like a process that died at that point.
	ErrSimulatedCrash = errors.New("simulated crash")
)

// Hooks are deterministic fault-injection points. The zero value disables them.
type Hooks struct {
	// BeforeCommit runs as the last statement inside the batch transaction, after the
	// checkpoint update. A non-nil error rolls back every write of that batch.
	BeforeCommit func(ctx context.Context, tx pgx.Tx, batchID string) error
	// AfterCommit runs once COMMIT has returned successfully.
	AfterCommit func(batchID string) error
}

func (h Hooks) beforeCommit(ctx context.Context, tx pgx.Tx, batchID string) error {
	if h.BeforeCommit == nil {
		return nil
	}
	return h.BeforeCommit(ctx, tx, batchID)
}

func (h Hooks) afterCommit(batchID string) error {
	if h.AfterCommit == nil {
		return nil
	}
	return h.AfterCommit(batchID)
}

// Config bounds supplier retries.
type Config struct {
	RetryMaxAttempts int           // default 4
	RetryBaseDelay   time.Duration // default 200ms (tests: 1ms)
	RetryMaxDelay    time.Duration // default 2s
	SupplierTimeout  time.Duration // default 2s per call
}

// Manager, worker and queries (manager.go, worker.go, store.go, queries.go):
//
//	func NewManager(pool *pgxpool.Pool, sup supplier.Client, cfg Config, hooks Hooks, log *slog.Logger) *Manager
//	func (m *Manager) Start(ctx, supplierID, updateID string) (job Job, created bool, err error) // ErrActiveJobExists; spawns a worker when created
//	func (m *Manager) Resume(ctx, jobID string) (Job, error) // ErrNotFound | ErrInvalidTransition | ErrActiveJobExists; spawns a worker
//	func (m *Manager) Reconcile(ctx) (ids []string, err error) // startup only, before HTTP listens
//	func (m *Manager) RunJob(ctx, jobID string) error // worker body; never call it on a job Start/Resume already spawned
//	func (m *Manager) Wait() // blocks until every spawned worker returned
//	func (m *Manager) Shutdown(ctx) error // cancel workers, wait (bounded by ctx)
//	func GetJob(ctx, db storage.DB, id string) (Job, error) // ErrNotFound
//	func ListJobs(ctx, db storage.DB, status string, limit, offset int) ([]Job, error)
//	func ListChanges(ctx, db storage.DB, jobID, changeType string, limit, offset int) ([]Change, error)
//	func ListIssues(ctx, db storage.DB, jobID, status, severity string, limit, offset int) ([]Issue, error)
