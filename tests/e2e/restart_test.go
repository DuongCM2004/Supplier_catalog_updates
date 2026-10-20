package e2e

// These tests run the real server binary as a child process against the test database, end
// it the hard way (an injected crash) or the polite way (an interrupt), start it again and
// drive the job to the end through HTTP. They prove what an in-process test cannot: that a
// process that really died leaves a database from which the next process finishes the work
// exactly once.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/testutil"
)

const (
	// The fixture used by every scenario: five clean batches of two new products each.
	updateID = "UPDATE-2026-003"

	startWait = 20 * time.Second // a cold Go binary can be slow to start on a loaded machine
	exitWait  = 20 * time.Second // the server's own graceful stop takes up to 10s + 10s
	// resumeWait is how long a resumed job may take to finish the remaining batches.
	resumeWait = 10 * time.Second
)

var (
	serverBinary string // built once in TestMain; empty when the tests will be skipped
	fixturesDir  string // absolute path of the repository's fixtures/
)

func TestMain(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	// Without a database every test skips, so there is nothing to build.
	if os.Getenv("TEST_DATABASE_URL") == "" {
		return m.Run()
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e: locate the repository:", err)
		return 1
	}
	fixturesDir = filepath.Join(root, "fixtures")

	dir, err := os.MkdirTemp("", "catalog-sync-e2e-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e: create build directory:", err)
		return 1
	}
	defer os.RemoveAll(dir)

	exe := "server"
	if runtime.GOOS == "windows" {
		exe += ".exe"
	}
	serverBinary = filepath.Join(dir, exe)
	build := exec.Command(goTool(), "build", "-o", serverBinary, "./cmd/server")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: go build ./cmd/server failed: %v\n%s", err, out)
		return 1
	}
	return m.Run()
}

// goTool is the go command of the toolchain running these tests, so a machine with several
// Go installations builds the server with the same one; it falls back to PATH.
func goTool() string {
	name := "go"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	if goroot := runtime.GOROOT(); goroot != "" {
		if path := filepath.Join(goroot, "bin", name); fileExists(path) {
			return path
		}
	}
	return name
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// syncBuffer collects a child's output while the test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// server is one running (or finished) server process.
type server struct {
	cmd  *exec.Cmd
	url  string
	logs *syncBuffer
	done chan struct{} // closed once the process has exited
}

// startServer launches the built binary on a free port with small retry settings and the
// given extra environment ("NAME=value"), and returns once /healthz answers. That also means
// start-up reconciliation has run, because the listener opens only after it. The process is
// killed when the test ends, whatever happens.
func startServer(t *testing.T, extraEnv ...string) *server {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find a free port: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release the free port: %v", err)
	}

	env := []string{
		"DATABASE_URL=" + os.Getenv("TEST_DATABASE_URL"),
		"HTTP_ADDR=" + addr,
		"FIXTURES_DIR=" + fixturesDir,
		"DB_WAIT_SECONDS=10",
		"RETRY_MAX_ATTEMPTS=2",
		"RETRY_BASE_DELAY_MS=1",
		"RETRY_MAX_DELAY_MS=5",
		"LOG_LEVEL=info",
	}
	env = append(env, extraEnv...)
	cmd := exec.Command(serverBinary)
	cmd.Env = append(cleanEnv(os.Environ()), env...)
	logs := &syncBuffer{}
	cmd.Stdout, cmd.Stderr = logs, logs
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", serverBinary, err)
	}
	s := &server{cmd: cmd, url: "http://" + addr, logs: logs, done: make(chan struct{})}
	go func() {
		_ = cmd.Wait() // the exit status is read from cmd.ProcessState once done is closed
		close(s.done)
	}()
	t.Cleanup(func() {
		select {
		case <-s.done:
		default:
			_ = cmd.Process.Kill()
		}
		select {
		case <-s.done:
		case <-time.After(10 * time.Second):
			t.Errorf("server process %d is still running after being killed", cmd.Process.Pid)
		}
		if t.Failed() {
			t.Logf("server %s logs:\n%s", s.url, logs.String())
		}
	})
	s.awaitReady(t)
	return s
}

// cleanEnv drops from the inherited environment every variable the server reads, so a value
// exported in the developer's shell (an API_TOKEN, a CRASH_POINT) cannot change a test.
func cleanEnv(inherited []string) []string {
	owned := []string{
		"DATABASE_URL", "HTTP_ADDR", "PORT", "API_TOKEN", "SUPPLIER_ID", "FIXTURES_DIR", "LOG_LEVEL", "DB_WAIT_SECONDS",
		"RETRY_MAX_ATTEMPTS", "RETRY_BASE_DELAY_MS", "RETRY_MAX_DELAY_MS", "SUPPLIER_TIMEOUT_MS",
		"SUPPLIER_OUTAGE_CURSORS", "SUPPLIER_DELAY_MS", "CRASH_POINT",
	}
	var kept []string
	for _, kv := range inherited {
		name, _, _ := strings.Cut(kv, "=")
		if !slices.Contains(owned, strings.ToUpper(name)) { // Windows variable names are case-insensitive
			kept = append(kept, kv)
		}
	}
	return kept
}

// oneShotClient opens a new connection for every request and closes it afterwards. A client
// that keeps connections alive can leave one open that never carried a request (it dialled a
// second connection and then reused the first); net/http's graceful Shutdown waits about five
// seconds for such a connection before it counts it as idle, which would hold up the stop of a
// server under test while its job keeps running.
func oneShotClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, Transport: &http.Transport{DisableKeepAlives: true}}
}

func (s *server) awaitReady(t *testing.T) {
	t.Helper()
	client := oneShotClient(2 * time.Second)
	deadline := time.Now().Add(startWait)
	for {
		select {
		case <-s.done:
			t.Fatalf("server exited with status %d before it answered /healthz\n%s", s.cmd.ProcessState.ExitCode(), s.logs.String())
		default:
		}
		if resp, err := client.Get(s.url + "/healthz"); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("server did not answer /healthz within %s\n%s", startWait, s.logs.String())
		}
		<-time.After(25 * time.Millisecond)
	}
}

// waitExit blocks until the process has exited and returns its exit status.
func (s *server) waitExit(t *testing.T) int {
	t.Helper()
	select {
	case <-s.done:
		return s.cmd.ProcessState.ExitCode()
	case <-time.After(exitWait):
		t.Fatalf("server is still running %s after it should have stopped\n%s", exitWait, s.logs.String())
		return -1
	}
}

// events returns the log lines whose message is msg, decoded.
func (s *server) events(msg string) []map[string]any {
	var found []map[string]any
	for _, line := range strings.Split(s.logs.String(), "\n") {
		var ev map[string]any
		if json.Unmarshal([]byte(line), &ev) == nil && ev["msg"] == msg {
			found = append(found, ev)
		}
	}
	return found
}

// call sends one request and decodes the JSON object answer.
func (s *server) call(t *testing.T, method, path, body string, wantStatus int) map[string]any {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, s.url+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build %s %s: %v", method, path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := oneShotClient(5 * time.Second).Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read answer of %s %s: %v", method, path, err)
	}
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s = %d, want %d: %s", method, path, resp.StatusCode, wantStatus, raw)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("answer of %s %s is not a JSON object: %v\n%s", method, path, err, raw)
	}
	return out
}

func (s *server) startSync(t *testing.T) string {
	t.Helper()
	id, _ := s.call(t, http.MethodPost, "/api/syncs", `{"update_id":"`+updateID+`"}`, http.StatusAccepted)["job_id"].(string)
	if id == "" {
		t.Fatal("start answered without a job_id")
	}
	return id
}

func (s *server) resume(t *testing.T, jobID string) {
	t.Helper()
	s.call(t, http.MethodPost, "/api/syncs/"+jobID+"/resume", "", http.StatusAccepted)
}

func (s *server) status(t *testing.T, jobID string) string {
	t.Helper()
	status, _ := s.call(t, http.MethodGet, "/api/syncs/"+jobID, "", http.StatusOK)["status"].(string)
	return status
}

// awaitStatus polls the job until it has the wanted status. Any other stopped state fails at
// once. The wait only covers work the server really does in the background.
func (s *server) awaitStatus(t *testing.T, jobID, want string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		switch got := s.status(t, jobID); got {
		case want:
			return
		case "INTERRUPTED", "NEEDS_ATTENTION", "COMPLETED", "FAILED":
			t.Fatalf("job %s is %s, want %s\n%s", jobID, got, want, s.logs.String())
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s did not become %s within %s\n%s", jobID, want, within, s.logs.String())
		}
		<-time.After(25 * time.Millisecond)
	}
}

// cleanRun is what the fixture says a run without any failure ends with.
type cleanRun struct {
	TotalBatches int `json:"total_batches"`
	Expected     struct {
		Status    string `json:"status"`
		Created   int    `json:"created"`
		Updated   int    `json:"updated"`
		Withdrawn int    `json:"withdrawn"`
		Duplicate int    `json:"duplicate"`
		Stale     int    `json:"stale"`
	} `json:"expected"`
}

func loadCleanRun(t *testing.T) cleanRun {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(fixturesDir, updateID+".json"))
	if err != nil {
		t.Fatalf("read the fixture: %v", err)
	}
	var run cleanRun
	if err := json.Unmarshal(raw, &run); err != nil {
		t.Fatalf("decode the fixture: %v", err)
	}
	return run
}

func count(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return n
}

// jobRow is the persisted state of a job.
type jobRow struct {
	status                                 string
	nextCursor                             string
	lastError                              *string
	processed, created, updated, withdrawn int
	duplicate, stale                       int
}

func readJob(t *testing.T, pool *pgxpool.Pool, jobID string) jobRow {
	t.Helper()
	var j jobRow
	err := pool.QueryRow(context.Background(), `SELECT status, next_cursor, last_error, processed_batches,
  created_count, updated_count, withdrawn_count, duplicate_count, stale_count FROM sync_jobs WHERE id = $1::uuid`, jobID).
		Scan(&j.status, &j.nextCursor, &j.lastError, &j.processed, &j.created, &j.updated, &j.withdrawn, &j.duplicate, &j.stale)
	if err != nil {
		t.Fatalf("read job %s: %v", jobID, err)
	}
	return j
}

// assertCommittedBatches checks that exactly the first n batches of the fixture are saved:
// their markers, their products, and a checkpoint pointing at the next one, with nothing of
// the batch that was cut short.
func assertCommittedBatches(t *testing.T, pool *pgxpool.Pool, jobID string, n int, wantCursor string) {
	t.Helper()
	if got := count(t, pool, `SELECT count(*) FROM sync_batches WHERE job_id = $1::uuid`, jobID); got != n {
		t.Errorf("committed batches = %d, want %d", got, n)
	}
	job := readJob(t, pool, jobID)
	if job.processed != n || job.nextCursor != wantCursor || job.created != 2*n {
		t.Errorf("job checkpoint = %d batches, cursor %q, created %d; want %d batches, cursor %q, created %d",
			job.processed, job.nextCursor, job.created, n, wantCursor, 2*n)
	}
	if got := count(t, pool, `SELECT count(*) FROM products`); got != 2*n {
		t.Errorf("products = %d, want %d (two per committed batch, none from the batch that was cut short)", got, 2*n)
	}
	if got := count(t, pool, `SELECT count(*) FROM sync_changes WHERE job_id = $1::uuid`, jobID); got != 2*n {
		t.Errorf("change rows = %d, want %d", got, 2*n)
	}
	testutil.AssertCounters(t, pool, jobID)
}

// assertSameAsCleanRun checks the final state against the fixture's expectation for a run
// that never failed, and that nothing was recorded twice.
func assertSameAsCleanRun(t *testing.T, pool *pgxpool.Pool, jobID string) {
	t.Helper()
	want := loadCleanRun(t)
	job := readJob(t, pool, jobID)
	got := [7]any{job.status, job.processed, job.created, job.updated, job.withdrawn, job.duplicate, job.stale}
	exp := [7]any{want.Expected.Status, want.TotalBatches, want.Expected.Created, want.Expected.Updated,
		want.Expected.Withdrawn, want.Expected.Duplicate, want.Expected.Stale}
	if got != exp {
		t.Errorf("status/batches/created/updated/withdrawn/duplicate/stale = %v, want %v (a run without failures)", got, exp)
	}
	changes := count(t, pool, `SELECT count(*) FROM sync_changes WHERE job_id = $1::uuid AND change_type = 'CREATED'`, jobID)
	distinct := count(t, pool, `SELECT count(DISTINCT product_id) FROM sync_changes WHERE job_id = $1::uuid`, jobID)
	if changes != want.Expected.Created || distinct != want.Expected.Created {
		t.Errorf("CREATED change rows = %d for %d distinct products, want %d of each (no duplicate effects)",
			changes, distinct, want.Expected.Created)
	}
	if products := count(t, pool, `SELECT count(*) FROM products WHERE status = 'ACTIVE'`); products != want.Expected.Created {
		t.Errorf("ACTIVE products = %d, want %d", products, want.Expected.Created)
	}
	if issues := count(t, pool, `SELECT count(*) FROM sync_issues`); issues != 0 {
		t.Errorf("issue rows = %d, want none: a crash or a stop is not a data problem", issues)
	}
	testutil.AssertCounters(t, pool, jobID)
	testutil.AssertCompletedConsistent(t, pool, jobID)
}

// runCrashAndRecover is the whole story for one injected crash: the first process dies with
// exit status 3 after the stated point; the database keeps the two batches that were
// committed; the second process, started without the crash switch, marks the job
// INTERRUPTED before it listens; an explicit resume then finishes the job.
func runCrashAndRecover(t *testing.T, crashPoint string) {
	t.Helper()
	pool := testutil.DB(t)
	testutil.Reset(t, pool)

	// The delay keeps the 202 ahead of the crash: the answer is on the wire long before the
	// third batch is reached.
	crashing := startServer(t, "CRASH_POINT="+crashPoint, "SUPPLIER_DELAY_MS=50")
	jobID := crashing.startSync(t)
	if code := crashing.waitExit(t); code != 3 {
		t.Fatalf("exit status = %d, want 3 (injected crash)\n%s", code, crashing.logs.String())
	}
	if ev := crashing.events("fault.crash_point"); len(ev) != 1 || ev[0]["point"] != crashPoint {
		t.Errorf("fault.crash_point events = %v, want one for %s", ev, crashPoint)
	}

	// What the dead process left behind: two committed batches and a job nobody is running.
	job := readJob(t, pool, jobID)
	if job.status != "RUNNING" {
		t.Errorf("job status after the crash = %s, want RUNNING (a dead process cannot update it)", job.status)
	}
	assertCommittedBatches(t, pool, jobID, 2, "cursor-003")
	if got := count(t, pool, `SELECT count(*) FROM sync_batches WHERE job_id = $1::uuid AND batch_id = 'BATCH-003'`, jobID); got != 0 {
		t.Errorf("BATCH-003 has a marker after a crash that stopped it from being saved")
	}

	// The restart must not carry the crash switch, or it would die at the same batch again.
	restarted := startServer(t)
	if got := restarted.status(t, jobID); got != "INTERRUPTED" {
		t.Fatalf("job status after restart = %s, want INTERRUPTED (reconciled before listening)", got)
	}
	if ev := restarted.events("startup.reconciled"); len(ev) != 1 || ev[0]["count"] != float64(1) {
		t.Errorf("startup.reconciled events = %v, want one with count 1", ev)
	}
	if msg := readJob(t, pool, jobID).lastError; msg == nil || !strings.Contains(*msg, "restarted") {
		t.Errorf("last_error after reconcile = %v, want it to say the service restarted", msg)
	}
	assertCommittedBatches(t, pool, jobID, 2, "cursor-003") // reconcile only changes the status

	restarted.resume(t, jobID)
	restarted.awaitStatus(t, jobID, "COMPLETED", resumeWait)
	assertSameAsCleanRun(t, pool, jobID)
}

func TestProcessCrashAfterCommitRecovers(t *testing.T) {
	runCrashAndRecover(t, "after_commit:BATCH-002")
}

func TestProcessCrashBeforeCommitRecovers(t *testing.T) {
	runCrashAndRecover(t, "before_commit:BATCH-003")
}

func TestGracefulShutdownInterruptsJob(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("graceful signal delivery to a child process is not portable to Windows; covered in the Linux race container")
	}
	pool := testutil.DB(t)
	testutil.Reset(t, pool)

	// 400 ms before every fetch keeps the job mid-run for about two seconds.
	running := startServer(t, "SUPPLIER_DELAY_MS=400")
	jobID := running.startSync(t)
	deadline := time.Now().Add(exitWait)
	for readJob(t, pool, jobID).processed < 1 { // wait until at least one batch is saved
		if time.Now().After(deadline) {
			t.Fatalf("no batch was committed within %s\n%s", exitWait, running.logs.String())
		}
		<-time.After(10 * time.Millisecond)
	}

	if err := running.cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("send interrupt: %v", err)
	}
	if code := running.waitExit(t); code != 0 {
		t.Fatalf("exit status = %d, want 0 (graceful stop)\n%s", code, running.logs.String())
	}
	for _, msg := range []string{"server.stopping", "server.stopped", "workers.stopping", "workers.stopped"} {
		if len(running.events(msg)) != 1 {
			t.Errorf("log event %s seen %d times, want once", msg, len(running.events(msg)))
		}
	}

	// The worker itself recorded the stop: the job is INTERRUPTED with no issue row, and it is
	// partly done.
	job := readJob(t, pool, jobID)
	if job.status != "INTERRUPTED" || job.lastError == nil || !strings.Contains(*job.lastError, "shutting down") {
		t.Errorf("job after the stop = %s / %v, want INTERRUPTED because the service is shutting down", job.status, job.lastError)
	}
	if job.processed < 1 || job.processed >= loadCleanRun(t).TotalBatches {
		t.Errorf("committed batches = %d, want some but not all", job.processed)
	}
	if issues := count(t, pool, `SELECT count(*) FROM sync_issues`); issues != 0 {
		t.Errorf("issue rows after a requested stop = %d, want none", issues)
	}
	testutil.AssertCounters(t, pool, jobID)

	// Nothing is left for the next start to reconcile, and an explicit resume finishes the job.
	restarted := startServer(t)
	if ev := restarted.events("startup.reconciled"); len(ev) != 1 || ev[0]["count"] != float64(0) {
		t.Errorf("startup.reconciled events = %v, want one with count 0: the stop already settled the job", ev)
	}
	restarted.resume(t, jobID)
	restarted.awaitStatus(t, jobID, "COMPLETED", resumeWait)
	assertSameAsCleanRun(t, pool, jobID)
}
