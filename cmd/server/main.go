// Command server runs the catalog sync service: it connects to PostgreSQL, applies the
// embedded migrations and serves the HTTP API until it is told to stop.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/api"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/storage"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/supplier"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/syncjob"
	"github.com/DuongCM2004/resilient-catalog-sync/migrations"
)

const (
	readHeaderTimeout = 5 * time.Second
	shutdownTimeout   = 10 * time.Second
)

// config is the whole process configuration, read once from the environment at startup.
type config struct {
	DatabaseURL string
	DBWait      time.Duration
	HTTPAddr    string
	APIToken    string
	SupplierID  string
	FixturesDir string
	LogLevel    slog.Level
	Retry       syncjob.Config
	// Fault switches exist for demos and tests; all are off by default.
	Faults     supplier.FaultOptions
	CrashPoint string // "before_commit:<batch_id>" or "after_commit:<batch_id>"
}

func main() {
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		// Logged as JSON like everything else; the exit code tells the orchestrator.
		slog.New(slog.NewJSONHandler(os.Stdout, nil)).Error("server.failed", "error", err)
		os.Exit(1)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(log)

	// A stop signal during the (possibly long) wait for PostgreSQL ends that wait too.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err = run(ctx, cfg, log)
	stop()
	if err != nil {
		log.Error("server.failed", "error", err)
		os.Exit(1)
	}
}

// run starts the service and returns nil after a graceful stop caused by ctx ending. It is
// separate from main so shutdown can be tested without sending process signals.
func run(ctx context.Context, cfg config, log *slog.Logger) error {
	log.Info("startup.config",
		"http_addr", cfg.HTTPAddr,
		"database_url", redactURL(cfg.DatabaseURL),
		"db_wait_seconds", int(cfg.DBWait/time.Second),
		"supplier_id", cfg.SupplierID,
		"fixtures_dir", cfg.FixturesDir,
		"api_token_set", cfg.APIToken != "",
		"log_level", cfg.LogLevel.String(),
		"retry_max_attempts", cfg.Retry.RetryMaxAttempts,
		"retry_base_delay_ms", cfg.Retry.RetryBaseDelay.Milliseconds(),
		"retry_max_delay_ms", cfg.Retry.RetryMaxDelay.Milliseconds(),
		"supplier_timeout_ms", cfg.Retry.SupplierTimeout.Milliseconds(),
		"supplier_outage_cursors", len(cfg.Faults.OutageCursors),
		"supplier_delay_ms", cfg.Faults.Delay.Milliseconds(),
		"crash_point", cfg.CrashPoint)

	pool, err := storage.Open(ctx, cfg.DatabaseURL, cfg.DBWait)
	if err != nil {
		return err
	}
	defer pool.Close()
	log.Info("db.connected")

	applied, err := storage.Migrate(ctx, pool, migrations.FS)
	if err != nil {
		return err
	}
	if applied == nil {
		applied = []string{} // logs as [] ("nothing new"), not null
	}
	log.Info("migrations.applied", "versions", applied)

	crash, err := parseCrashPoint(cfg.CrashPoint)
	if err != nil {
		return err
	}
	mgr := syncjob.NewManager(pool, supplier.NewFixtureClient(cfg.FixturesDir, cfg.Faults), cfg.Retry,
		crash.hooks(log, os.Exit), log)
	// Registered after the pool's Close, so it runs first: workers stop before their pool goes.
	defer stopManager(mgr, log)

	// Jobs left PENDING/RUNNING by a previous process have no worker. They are marked
	// INTERRUPTED before the listener opens so no request can see or resume a stale state.
	if _, err := mgr.Reconcile(ctx); err != nil {
		return fmt.Errorf("reconcile jobs: %w", err)
	}

	srv := &http.Server{
		Handler: api.NewRouter(api.Deps{
			Pool:       pool,
			Log:        log,
			Manager:    mgr,
			SupplierID: cfg.SupplierID,
			APIToken:   cfg.APIToken,
		}),
		ReadHeaderTimeout: readHeaderTimeout,
	}
	ln, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
	}
	log.Info("server.listening", "addr", ln.Addr().String())

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	select {
	case err := <-serveErr:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	// Stop accepting and let in-flight requests finish; then (deferred) interrupt the
	// workers so their jobs can be resumed, and only then close the pool.
	log.Info("server.stopping")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}
	log.Info("server.stopped")
	return nil
}

// loadConfig reads and validates every setting, reporting all problems at once so a bad
// deployment is fixed in one pass.
func loadConfig(getenv func(string) string) (config, error) {
	var errs []error
	fail := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }
	intVar := func(name string, def, min int) int {
		raw := getenv(name)
		if raw == "" {
			return def
		}
		n, err := strconv.Atoi(raw)
		if err != nil || n < min {
			fail("%s must be an integer >= %d, got %q", name, min, raw)
			return def
		}
		return n
	}
	text := func(name, def string) string {
		if v := getenv(name); v != "" {
			return v
		}
		return def
	}

	cfg := config{
		DatabaseURL: getenv("DATABASE_URL"),
		APIToken:    getenv("API_TOKEN"),
		SupplierID:  text("SUPPLIER_ID", "SUP-001"),
		FixturesDir: text("FIXTURES_DIR", "/app/fixtures"),
		CrashPoint:  getenv("CRASH_POINT"),
	}
	if cfg.DatabaseURL == "" {
		fail("DATABASE_URL is required")
	}
	if _, err := parseCrashPoint(cfg.CrashPoint); err != nil {
		fail("%v", err)
	}

	// Render (and similar platforms) inject PORT; an explicit HTTP_ADDR always wins.
	cfg.HTTPAddr = getenv("HTTP_ADDR")
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = ":8080"
		if port := getenv("PORT"); port != "" {
			if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
				fail("PORT must be a port number, got %q", port)
			} else {
				cfg.HTTPAddr = ":" + port
			}
		}
	}

	cfg.DBWait = time.Duration(intVar("DB_WAIT_SECONDS", 30, 0)) * time.Second
	cfg.Retry = syncjob.Config{
		RetryMaxAttempts: intVar("RETRY_MAX_ATTEMPTS", 4, 1),
		RetryBaseDelay:   time.Duration(intVar("RETRY_BASE_DELAY_MS", 200, 1)) * time.Millisecond,
		RetryMaxDelay:    time.Duration(intVar("RETRY_MAX_DELAY_MS", 2000, 1)) * time.Millisecond,
		SupplierTimeout:  time.Duration(intVar("SUPPLIER_TIMEOUT_MS", 2000, 1)) * time.Millisecond,
	}
	cfg.Faults = supplier.FaultOptions{
		OutageCursors: splitSet(getenv("SUPPLIER_OUTAGE_CURSORS")),
		Delay:         time.Duration(intVar("SUPPLIER_DELAY_MS", 0, 0)) * time.Millisecond,
	}

	if raw := getenv("LOG_LEVEL"); raw != "" {
		if err := cfg.LogLevel.UnmarshalText([]byte(raw)); err != nil {
			fail("LOG_LEVEL must be debug, info, warn or error, got %q", raw)
		}
	}
	return cfg, errors.Join(errs...)
}

// splitSet turns "a, b,,c" into {a, b, c}; empty entries are dropped.
func splitSet(list string) map[string]bool {
	set := map[string]bool{}
	for _, item := range strings.Split(list, ",") {
		if item = strings.TrimSpace(item); item != "" {
			set[item] = true
		}
	}
	return set
}

// redactURL hides the password of a postgres URL for logging, whether it sits in the
// user:password part or in a ?password= / ?sslpassword= parameter. Anything that is not a
// URL (a key=value DSN can carry the password as plain text) is not logged at all.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return "(not logged: not a postgres URL)"
	}
	if q := u.Query(); len(q) > 0 {
		for name := range q {
			if strings.Contains(strings.ToLower(name), "password") {
				q.Set(name, "xxxxx")
			}
		}
		u.RawQuery = q.Encode()
	}
	return u.Redacted()
}

// stopManager interrupts every running job and waits for the workers, bounded by
// shutdownTimeout. A job whose worker could not finish in time is left RUNNING with no
// worker; the next start's reconcile (or a resume) deals with it.
func stopManager(m *syncjob.Manager, log *slog.Logger) {
	log.Info("workers.stopping")
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := m.Shutdown(ctx); err != nil {
		log.Error("workers.stop_incomplete", "error", err)
		return
	}
	log.Info("workers.stopped")
}

// crashPoint is a demo/test fault: end the process abruptly at one named batch, either just
// before its transaction commits or just after, to show that a real crash loses nothing and
// duplicates nothing. The zero value means no fault.
type crashPoint struct {
	when    string // crashBeforeCommit or crashAfterCommit
	batchID string
}

const (
	crashBeforeCommit = "before_commit"
	crashAfterCommit  = "after_commit"
)

// exitCodeCrashPoint is the process exit status of an injected crash, distinct from the 1
// of a configuration or startup failure.
const exitCodeCrashPoint = 3

// parseCrashPoint reads CRASH_POINT. An empty value is no fault; anything that is not
// exactly "<when>:<batch_id>" is an error, so a typo cannot silently disable the fault a
// demo depends on.
func parseCrashPoint(raw string) (crashPoint, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return crashPoint{}, nil
	}
	when, batchID, ok := strings.Cut(raw, ":")
	if !ok || batchID == "" || (when != crashBeforeCommit && when != crashAfterCommit) {
		return crashPoint{}, fmt.Errorf("CRASH_POINT must be %s:<batch_id> or %s:<batch_id>, got %q",
			crashBeforeCommit, crashAfterCommit, raw)
	}
	return crashPoint{when: when, batchID: batchID}, nil
}

func (c crashPoint) String() string {
	if c.when == "" {
		return ""
	}
	return c.when + ":" + c.batchID
}

// hooks returns the processor hooks that call exit(3) at the configured batch, after
// logging fault.crash_point. exit is os.Exit in production. Should it return (tests), the
// hook reports ErrSimulatedCrash so the worker abandons the job like a dead process would.
func (c crashPoint) hooks(log *slog.Logger, exit func(int)) syncjob.Hooks {
	crash := func(batchID string) error {
		log.Error("fault.crash_point", "point", c.String(), "batch_id", batchID)
		exit(exitCodeCrashPoint)
		return syncjob.ErrSimulatedCrash
	}
	switch c.when {
	case crashBeforeCommit:
		return syncjob.Hooks{BeforeCommit: func(_ context.Context, _ pgx.Tx, batchID string) error {
			if batchID == c.batchID {
				return crash(batchID)
			}
			return nil
		}}
	case crashAfterCommit:
		return syncjob.Hooks{AfterCommit: func(batchID string) error {
			if batchID == c.batchID {
				return crash(batchID)
			}
			return nil
		}}
	}
	return syncjob.Hooks{}
}
