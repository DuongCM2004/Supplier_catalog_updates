package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/testutil"
)

func env(pairs map[string]string) func(string) string {
	return func(k string) string { return pairs[k] }
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := loadConfig(env(map[string]string{"DATABASE_URL": "postgres://u:p@db:5432/catalog"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != ":8080" || cfg.SupplierID != "SUP-001" || cfg.FixturesDir != "/app/fixtures" ||
		cfg.APIToken != "" || cfg.DBWait != 30*time.Second || cfg.LogLevel != slog.LevelInfo {
		t.Fatalf("defaults wrong: %+v", cfg)
	}
	r := cfg.Retry
	if r.RetryMaxAttempts != 4 || r.RetryBaseDelay != 200*time.Millisecond || r.RetryMaxDelay != 2*time.Second || r.SupplierTimeout != 2*time.Second {
		t.Fatalf("retry defaults wrong: %+v", r)
	}
	if len(cfg.Faults.OutageCursors) != 0 || cfg.Faults.Delay != 0 || cfg.CrashPoint != "" {
		t.Fatalf("fault switches must be off by default: %+v", cfg.Faults)
	}
}

func TestLoadConfigListenAddressPrecedence(t *testing.T) {
	base := map[string]string{"DATABASE_URL": "postgres://u:p@db/c"}
	tests := []struct {
		name string
		set  map[string]string
		want string
	}{
		{"neither", nil, ":8080"},
		{"PORT only (Render)", map[string]string{"PORT": "10000"}, ":10000"},
		{"HTTP_ADDR only", map[string]string{"HTTP_ADDR": "127.0.0.1:18092"}, "127.0.0.1:18092"},
		{"HTTP_ADDR wins over PORT", map[string]string{"HTTP_ADDR": ":9000", "PORT": "10000"}, ":9000"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vars := map[string]string{}
			for k, v := range base {
				vars[k] = v
			}
			for k, v := range tc.set {
				vars[k] = v
			}
			cfg, err := loadConfig(env(vars))
			if err != nil || cfg.HTTPAddr != tc.want {
				t.Fatalf("HTTPAddr = %q, err %v, want %q", cfg.HTTPAddr, err, tc.want)
			}
		})
	}
}

func TestLoadConfigReadsEveryOverride(t *testing.T) {
	cfg, err := loadConfig(env(map[string]string{
		"DATABASE_URL": "postgres://u:p@db/c", "API_TOKEN": "s3cret", "SUPPLIER_ID": "SUP-009",
		"FIXTURES_DIR": "/data/fx", "DB_WAIT_SECONDS": "5", "LOG_LEVEL": "debug",
		"RETRY_MAX_ATTEMPTS": "2", "RETRY_BASE_DELAY_MS": "1", "RETRY_MAX_DELAY_MS": "10", "SUPPLIER_TIMEOUT_MS": "50",
		"SUPPLIER_OUTAGE_CURSORS": " cursor-003, ,cursor-004 ", "SUPPLIER_DELAY_MS": "250",
		"CRASH_POINT": "before_commit:BATCH-002",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.APIToken != "s3cret" || cfg.SupplierID != "SUP-009" || cfg.FixturesDir != "/data/fx" ||
		cfg.DBWait != 5*time.Second || cfg.LogLevel != slog.LevelDebug || cfg.CrashPoint != "before_commit:BATCH-002" {
		t.Fatalf("overrides not applied: %+v", cfg)
	}
	if cfg.Retry.RetryMaxAttempts != 2 || cfg.Retry.RetryBaseDelay != time.Millisecond ||
		cfg.Retry.RetryMaxDelay != 10*time.Millisecond || cfg.Retry.SupplierTimeout != 50*time.Millisecond {
		t.Fatalf("retry overrides not applied: %+v", cfg.Retry)
	}
	if got := cfg.Faults.OutageCursors; len(got) != 2 || !got["cursor-003"] || !got["cursor-004"] || cfg.Faults.Delay != 250*time.Millisecond {
		t.Fatalf("fault overrides not applied: %+v", cfg.Faults)
	}
}

func TestLoadConfigReportsEveryProblemAtOnce(t *testing.T) {
	_, err := loadConfig(env(map[string]string{
		"PORT": "http", "DB_WAIT_SECONDS": "-1", "RETRY_MAX_ATTEMPTS": "0", "SUPPLIER_DELAY_MS": "soon", "LOG_LEVEL": "loud",
	}))
	if err == nil {
		t.Fatal("invalid configuration was accepted")
	}
	for _, want := range []string{"DATABASE_URL", "PORT", "DB_WAIT_SECONDS", "RETRY_MAX_ATTEMPTS", "SUPPLIER_DELAY_MS", "LOG_LEVEL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s:\n%v", want, err)
		}
	}
}

func TestRedactURLHidesPasswords(t *testing.T) {
	got := redactURL("postgres://catalog:hunter2@db:5432/catalog?sslmode=disable")
	if strings.Contains(got, "hunter2") || !strings.Contains(got, "catalog") || !strings.Contains(got, "db:5432") {
		t.Fatalf("redactURL = %q", got)
	}
	got = redactURL("postgres://catalog@db/catalog?password=hunter2&sslmode=disable")
	if strings.Contains(got, "hunter2") || !strings.Contains(got, "sslmode=disable") {
		t.Fatalf("redactURL with password parameter = %q", got)
	}
	for _, dsn := range []string{"host=db user=catalog password=hunter2 dbname=catalog", "not a url at all hunter2", ""} {
		if out := redactURL(dsn); strings.Contains(out, "hunter2") {
			t.Errorf("redactURL(%q) leaked the password: %q", dsn, out)
		}
	}
}

// freeAddr returns a loopback address that was free a moment ago.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func TestRunServesHealthAndStopsGracefullyWhenContextEnds(t *testing.T) {
	testutil.DB(t) // skips without TEST_DATABASE_URL and makes sure the database exists
	addr := freeAddr(t)
	cfg := config{
		DatabaseURL: os.Getenv("TEST_DATABASE_URL"), DBWait: 5 * time.Second, HTTPAddr: addr,
		SupplierID: "SUP-001",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, slog.New(slog.NewJSONHandler(io.Discard, nil))) }()

	// Readiness poll with a deadline: the server needs a moment to connect and migrate.
	url := "http://" + addr + "/healthz"
	deadline := time.Now().Add(15 * time.Second)
	for {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("GET /healthz = %d, want 200", resp.StatusCode)
			}
			break
		}
		select {
		case runErr := <-done:
			t.Fatalf("run ended before serving: %v", runErr)
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("server never answered on %s: %v", addr, err)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("graceful stop returned %v, want nil", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run did not return after its context ended")
	}
	if resp, err := http.Get(url); err == nil {
		resp.Body.Close()
		t.Fatal("server still answers after shutdown")
	}
}

func TestRunFailsWhenDatabaseNeverAnswers(t *testing.T) {
	cfg := config{DatabaseURL: "postgres://nobody:nothing@127.0.0.1:1/none?sslmode=disable", DBWait: 0, HTTPAddr: freeAddr(t)}
	err := run(context.Background(), cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err == nil || !strings.Contains(err.Error(), "not reachable") {
		t.Fatalf("run err = %v, want a database-not-reachable error", err)
	}
}
