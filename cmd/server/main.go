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
	CrashPoint string
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

	srv := &http.Server{
		Handler: api.NewRouter(api.Deps{
			Pool:       pool,
			Log:        log,
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

	// Stop accepting, let in-flight requests finish, then (deferred) close the pool.
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
