package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/api"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/supplier"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/syncjob"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/testutil"
)

// These tests fire many requests at the same moment and check what PostgreSQL lets through:
// never more than one PENDING/RUNNING job, a repeated start of one update is the same job,
// and a stopped job gets one worker however many resumes arrive together.
//
// Each case builds its own service (a Manager behind the real router served over HTTP) on a
// freshly reset database. A sampler watches the number of active jobs for the whole case.
// Requests leave from a barrier, and nothing waits on the clock: the supplier holds the
// winning worker in place, so every loser is refused for a reason that cannot expire while
// the test is looking.

const (
	// Large enough that most requests of a burst are in flight at the same moment.
	concStarters = 20

	// How many times the two-Manager races are repeated. Each round is an independent
	// race; the cost of a round is a table reset and two Managers.
	concRaceRounds = 25

	concActiveJobs = `SELECT count(*) FROM sync_jobs WHERE status IN ('PENDING','RUNNING')`
)

// concSupplier announces every FetchBatch on entered, so a test can wait until a worker is
// really parked inside FakeSupplier.Block instead of guessing how long that takes. The
// announcement comes before the fake logs the call: read FetchCalls only once the worker has
// finished or been released.
type concSupplier struct {
	*testutil.FakeSupplier
	entered chan string
}

func concWrap(f *testutil.FakeSupplier) *concSupplier {
	return &concSupplier{FakeSupplier: f, entered: make(chan string, 32)}
}

func (s *concSupplier) FetchBatch(ctx context.Context, updateID, cursor string) (supplier.Batch, error) {
	select {
	case s.entered <- cursor:
	default: // the test is not listening and the buffer is full: never stall a worker
	}
	return s.FakeSupplier.FetchBatch(ctx, updateID, cursor)
}

// forget discards announcements about fetches the test does not care about any more.
func (s *concSupplier) forget() {
	for {
		select {
		case <-s.entered:
		default:
			return
		}
	}
}

// concStartSupplier answers GetUpdate for every update in the burst, so whichever of them
// wins the active slot gets as far as the held fetch. Without this the winner would fail as
// an unknown update and free the slot, and a start that arrived a moment later would succeed.
func concStartSupplier(updateIDs []string) *concSupplier {
	f := testutil.NewFakeSupplier()
	for _, id := range updateIDs {
		f.SetUpdate(testutil.Info(id, 1))
	}
	return concWrap(f)
}

// concEnv is the service as it runs in production: one Manager, the real router in front of
// it, and the test database behind both.
type concEnv struct {
	pool   *pgxpool.Pool
	sup    *concSupplier
	mgr    *syncjob.Manager
	server *httptest.Server
	client *http.Client
	held   []string
}

func concNew(t *testing.T, sup *concSupplier) *concEnv {
	t.Helper()
	e := &concEnv{pool: crashPool(t), sup: sup}
	// Registered before the Manager, so it runs after the Manager's shutdown: workers are
	// stopped by cancellation first, and only then is a fetch that is still parked released.
	t.Cleanup(func() {
		for _, cursor := range e.held {
			sup.Unblock(cursor)
		}
	})
	e.mgr = crashNewManager(t, e.pool, sup, syncjob.Hooks{})
	e.server = httptest.NewServer(api.NewRouter(api.Deps{
		Pool:       e.pool,
		Log:        slog.New(slog.DiscardHandler),
		Manager:    e.mgr,
		SupplierID: testutil.SupplierID,
	}))
	e.client = &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: concStarters}, Timeout: crashWait}
	t.Cleanup(func() {
		e.client.CloseIdleConnections()
		e.server.Close()
	})
	return e
}

// hold parks every fetch at cursor until the test releases it (or the service shuts down).
func (e *concEnv) hold(cursor string) {
	e.sup.Block(cursor)
	e.held = append(e.held, cursor)
}

// concReply is what a client sees: the HTTP status and the fields of a job or of an error.
type concReply struct {
	HTTPStatus int    `json:"-"`
	Location   string `json:"-"`
	Failure    string `json:"-"` // set when the request or the decoding of its answer failed

	JobID     string `json:"job_id"`
	JobStatus string `json:"status"`
	UpdateID  string `json:"supplier_update_id"`
	Error     struct {
		Code          string `json:"code"`
		ActiveJobID   string `json:"active_job_id"`
		CurrentStatus string `json:"current_status"`
	} `json:"error"`
}

// post sends a request and never fails the test itself, because it runs in goroutines;
// concTally reports what went wrong.
func (e *concEnv) post(path, body string) concReply {
	var r concReply
	req, err := http.NewRequest(http.MethodPost, e.server.URL+path, strings.NewReader(body))
	if err != nil {
		r.Failure = fmt.Sprintf("POST %s: build request: %v", path, err)
		return r
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		r.Failure = fmt.Sprintf("POST %s: %v", path, err)
		return r
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		r.Failure = fmt.Sprintf("POST %s: read answer: %v", path, err)
		return r
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		r.Failure = fmt.Sprintf("POST %s: answer %q is not JSON: %v", path, raw, err)
	}
	r.HTTPStatus, r.Location = resp.StatusCode, resp.Header.Get("Location")
	return r
}

func concStartBody(updateID string) string { return fmt.Sprintf(`{"update_id":%q}`, updateID) }

// concBurst runs fn n times in parallel and releases all of them at the same instant, once
// every goroutine is waiting at the barrier. Results come back in goroutine order.
func concBurst[T any](n int, fn func(i int) T) []T {
	out := make([]T, n)
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	ready.Add(n)
	done.Add(n)
	for i := range n {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			out[i] = fn(i)
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	return out
}

// concTally groups replies by HTTP status and requires that every request got an answer.
func concTally(t *testing.T, replies []concReply, wantByStatus map[int]int) map[int][]concReply {
	t.Helper()
	byStatus := map[int][]concReply{}
	got := map[int]int{}
	for i, r := range replies {
		if r.Failure != "" {
			t.Errorf("request %d: %s", i, r.Failure)
			continue
		}
		byStatus[r.HTTPStatus] = append(byStatus[r.HTTPStatus], r)
		got[r.HTTPStatus]++
	}
	if t.Failed() {
		t.FailNow()
	}
	if !maps.Equal(got, wantByStatus) {
		t.Fatalf("answers by HTTP status = %v, want %v", got, wantByStatus)
	}
	return byStatus
}

// concWatchActiveJobs counts PENDING/RUNNING jobs, as fast as the database answers, until
// stop is called. stop fails the test if the count was ever above one, or if nothing was
// sampled, and returns the highest count seen so a test can also require that the sampler
// really saw the job it is guarding. stop may be called more than once; it also runs when
// the test ends.
func concWatchActiveJobs(t *testing.T, pool *pgxpool.Pool) (stop func() int) {
	t.Helper()
	var (
		samples, highest int
		failure          string
		quit             = make(chan struct{})
		finished         = make(chan struct{})
		once             sync.Once
	)
	go func() {
		defer close(finished)
		for {
			select {
			case <-quit:
				return
			default:
			}
			var n int
			if err := pool.QueryRow(context.Background(), concActiveJobs).Scan(&n); err != nil {
				failure = err.Error()
				return
			}
			samples++
			highest = max(highest, n)
		}
	}()
	stop = func() int {
		once.Do(func() {
			close(quit)
			<-finished // makes the goroutine's writes visible here
			switch {
			case failure != "":
				t.Errorf("active job sampler: %s", failure)
			case samples == 0:
				t.Errorf("active job sampler took no sample")
			case highest > 1:
				t.Errorf("sampler saw %d active jobs at once, want at most 1", highest)
			}
		})
		return highest
	}
	t.Cleanup(func() { stop() })
	return stop
}

func concRequireHighest(t *testing.T, stop func() int, want int) {
	t.Helper()
	if got := stop(); got != want {
		t.Errorf("highest number of active jobs sampled = %d, want %d (the sampler must have seen the job it guards)", got, want)
	}
}

func concWaitForFetch(t *testing.T, sup *concSupplier, cursor string) {
	t.Helper()
	if got := crashRecv(t, sup.entered, fmt.Sprintf("a worker to ask the supplier for cursor %q", cursor)); got != cursor {
		t.Fatalf("first fetch after the burst was at cursor %q, want %q", got, cursor)
	}
}

func concJobCount(t *testing.T, e *concEnv) int {
	t.Helper()
	return crashCount(t, e.pool, `SELECT count(*) FROM sync_jobs`)
}

func concActiveJobCount(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	return crashCount(t, pool, concActiveJobs)
}

// concUpdateIDs names n distinct updates.
func concUpdateIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("UPDATE-RACE-%02d", i+1)
	}
	return ids
}

func concCalls(sup *concSupplier, op string) int {
	n := 0
	for _, c := range sup.Calls() {
		if c.Op == op {
			n++
		}
	}
	return n
}

func TestConcurrentStartsAllowOneActiveJob(t *testing.T) {
	updates := concUpdateIDs(concStarters)
	sup := concStartSupplier(updates)
	e := concNew(t, sup)
	e.hold("") // the winner's worker stays RUNNING for the whole test

	stop := concWatchActiveJobs(t, e.pool)
	replies := concBurst(concStarters, func(i int) concReply {
		return e.post("/api/syncs", concStartBody(updates[i]))
	})
	byStatus := concTally(t, replies, map[int]int{http.StatusAccepted: 1, http.StatusConflict: concStarters - 1})

	winner := byStatus[http.StatusAccepted][0]
	if winner.JobID == "" || winner.Location != "/api/syncs/"+winner.JobID || !slices.Contains(updates, winner.UpdateID) {
		t.Fatalf("202 answer = %+v, want a job id, Location /api/syncs/<job id> and one of the posted updates", winner)
	}
	for i, r := range byStatus[http.StatusConflict] {
		if r.Error.Code != "ACTIVE_JOB_EXISTS" || r.Error.ActiveJobID != winner.JobID || r.JobID != "" {
			t.Errorf("409 answer %d = %+v, want ACTIVE_JOB_EXISTS naming the winner %s and no job of its own", i, r, winner.JobID)
		}
	}

	concWaitForFetch(t, sup, "") // the winner really is running, parked at the supplier
	if n := concJobCount(t, e); n != 1 {
		t.Errorf("sync_jobs rows = %d, want 1: a refused start must not leave a row behind", n)
	}
	if job := crashJob(t, e.pool, winner.JobID); job.Status != syncjob.StatusRunning || job.UpdateID != winner.UpdateID {
		t.Errorf("winner job = %s for update %s, want RUNNING for %s", job.Status, job.UpdateID, winner.UpdateID)
	}
	concRequireHighest(t, stop, 1)

	crashShutdown(t, e.mgr)
	if got := concCalls(sup, testutil.OpGetUpdate); got != 1 {
		t.Errorf("GetUpdate calls = %d, want 1: only the winner's job may have had a worker", got)
	}
	if got := sup.FetchCalls(""); got != 1 {
		t.Errorf("fetches at the first cursor = %d, want 1", got)
	}
}

func TestConcurrentStartsOfSameUpdateReturnOneJob(t *testing.T) {
	const starters = 10
	sup := concWrap(crashSupplier())
	e := concNew(t, sup)
	e.hold("") // keeps the job active while the duplicates arrive

	stop := concWatchActiveJobs(t, e.pool)
	replies := concBurst(starters, func(int) concReply { return e.post("/api/syncs", concStartBody(crashUpdate)) })
	byStatus := concTally(t, replies, map[int]int{http.StatusAccepted: 1, http.StatusOK: starters - 1})

	created := byStatus[http.StatusAccepted][0]
	if created.JobID == "" || created.Location != "/api/syncs/"+created.JobID {
		t.Fatalf("202 answer = %+v, want a job id and Location /api/syncs/<job id>", created)
	}
	for i, r := range byStatus[http.StatusOK] {
		if r.JobID != created.JobID || r.UpdateID != crashUpdate || r.Location != "" {
			t.Errorf("200 answer %d = %+v, want the job %s again and no Location", i, r, created.JobID)
		}
		if r.JobStatus != string(syncjob.StatusPending) && r.JobStatus != string(syncjob.StatusRunning) {
			t.Errorf("200 answer %d shows status %s, want PENDING or RUNNING (the worker is held)", i, r.JobStatus)
		}
	}

	concWaitForFetch(t, sup, "")
	if n := concJobCount(t, e); n != 1 {
		t.Errorf("sync_jobs rows = %d, want exactly 1 for one update", n)
	}
	concRequireHighest(t, stop, 1)

	// Release the one worker: the update is applied once, not once per request.
	e.sup.Unblock("")
	e.mgr.Wait()
	concRequireHighest(t, stop, 1)
	job := crashJob(t, e.pool, created.JobID)
	if job.Status != syncjob.StatusCompleted || crashCounts(job) != crashExpected {
		t.Errorf("job = %s %+v, want COMPLETED %+v", job.Status, crashCounts(job), crashExpected)
	}
	testutil.AssertCounters(t, e.pool, job.ID)
	testutil.AssertCompletedConsistent(t, e.pool, job.ID)
	crashAssertFetches(t, sup.FakeSupplier, crashFetchedOnce)
	if got := concCalls(sup, testutil.OpGetUpdate); got != 1 {
		t.Errorf("GetUpdate calls = %d, want 1: duplicate starts must not run the update again", got)
	}
}

func TestConcurrentResumesStartOneWorker(t *testing.T) {
	for _, resumers := range []int{2, 8} {
		t.Run(fmt.Sprintf("%d_resumes", resumers), func(t *testing.T) {
			sup := concWrap(crashSupplier())
			e := concNew(t, sup)

			// An outage at the third page leaves a job INTERRUPTED with two batches committed.
			sup.FailTimes("cursor-3", -1)
			job := crashStart(t, e.mgr, crashUpdate)
			e.mgr.Wait()
			if got := crashJob(t, e.pool, job.ID); got.Status != syncjob.StatusInterrupted || got.ProcessedBatches != 2 || got.NextCursor != "cursor-3" {
				t.Fatalf("job before the resumes = %s after %d batches at %q, want INTERRUPTED after 2 at \"cursor-3\"",
					got.Status, got.ProcessedBatches, got.NextCursor)
			}
			crashOnlyIssue(t, e.pool, job.ID, syncjob.CodeSupplierUnavailable, syncjob.SeverityTransient, "OPEN", "cursor-3")

			// The supplier is reachable again, but its next answer is held: the worker that
			// wins the resume stays alive while the other requests are decided.
			sup.FailTimes("cursor-3", 0)
			e.hold("cursor-3")
			sup.forget()
			earlier := len(sup.Calls())

			stop := concWatchActiveJobs(t, e.pool)
			path := "/api/syncs/" + job.ID + "/resume"
			replies := concBurst(resumers, func(int) concReply { return e.post(path, "") })
			byStatus := concTally(t, replies, map[int]int{http.StatusAccepted: 1, http.StatusConflict: resumers - 1})

			if r := byStatus[http.StatusAccepted][0]; r.JobID != job.ID || r.JobStatus != string(syncjob.StatusRunning) {
				t.Errorf("202 answer = %+v, want job %s RUNNING", r, job.ID)
			}
			for i, r := range byStatus[http.StatusConflict] {
				if r.Error.Code != "INVALID_STATE" || r.Error.CurrentStatus != string(syncjob.StatusRunning) {
					t.Errorf("409 answer %d = %+v, want INVALID_STATE with current_status RUNNING", i, r)
				}
			}

			concWaitForFetch(t, sup, "cursor-3")
			if got := crashJob(t, e.pool, job.ID); got.Status != syncjob.StatusRunning {
				t.Errorf("job while its worker is parked = %s, want RUNNING", got.Status)
			}
			concRequireHighest(t, stop, 1)

			e.sup.Unblock("cursor-3")
			e.mgr.Wait()
			concRequireHighest(t, stop, 1)

			done := crashJob(t, e.pool, job.ID)
			if done.Status != syncjob.StatusCompleted || done.ProcessedBatches != len(crashPages()) || crashCounts(done) != crashExpected {
				t.Errorf("job after the resumes = %s after %d batches %+v, want COMPLETED after %d %+v",
					done.Status, done.ProcessedBatches, crashCounts(done), len(crashPages()), crashExpected)
			}
			testutil.AssertCounters(t, e.pool, job.ID)
			testutil.AssertCompletedConsistent(t, e.pool, job.ID)
			// One run, whoever asked: one question about the update and one fetch per page
			// that was still missing, never one per request.
			want := []testutil.Call{
				{Op: testutil.OpGetUpdate},
				{Op: testutil.OpFetchBatch, Cursor: "cursor-3"},
				{Op: testutil.OpFetchBatch, Cursor: "cursor-4"},
			}
			if got := sup.Calls()[earlier:]; !slices.Equal(got, want) {
				t.Errorf("supplier calls since the first resume = %v, want %v", got, want)
			}
		})
	}
}

// concStartResult is what one Manager.Start call returned.
type concStartResult struct {
	job     syncjob.Job
	created bool
	err     error
}

func (r concStartResult) String() string {
	return fmt.Sprintf("{job %q for update %q, created %v, err %v}", r.job.ID, r.job.UpdateID, r.created, r.err)
}

func concStartOn(m *syncjob.Manager, updateID string) concStartResult {
	job, created, err := m.Start(context.Background(), testutil.SupplierID, updateID)
	return concStartResult{job: job, created: created, err: err}
}

// concRaceRound builds the setup shared by the two-Manager races: two Managers on the same
// pool, which share nothing but the database. Their mutexes are separate, so only
// PostgreSQL can decide between them, which is what a second application process would see.
func concRaceRound(t *testing.T) (pool *pgxpool.Pool, sup *concSupplier, first, second *syncjob.Manager) {
	t.Helper()
	pool = crashPool(t)
	sup = concWrap(crashSupplier())
	sup.SetUpdate(testutil.Info(crashOtherUpdate, len(crashPages())))
	sup.Block("")
	t.Cleanup(func() { sup.Unblock("") }) // registered before the Managers: runs after their shutdown
	return pool, sup, crashNewManager(t, pool, sup, syncjob.Hooks{}), crashNewManager(t, pool, sup, syncjob.Hooks{})
}

// Two service instances starting different updates at once: the one-active-job index is the
// only thing between them, and the loser must be refused (naming the winner), not stored.
// Measured over 300 rounds, the loser was stopped by the index in every one of them.
func TestManagersRacingToStartDifferentUpdatesLetOneWin(t *testing.T) {
	for round := 1; round <= concRaceRounds; round++ {
		t.Run(fmt.Sprintf("round_%02d", round), func(t *testing.T) {
			pool, _, first, second := concRaceRound(t)
			stop := concWatchActiveJobs(t, pool)

			managers, updates := []*syncjob.Manager{first, second}, []string{crashUpdate, crashOtherUpdate}
			results := concBurst(2, func(i int) concStartResult { return concStartOn(managers[i], updates[i]) })

			var winners, losers []concStartResult
			for _, r := range results {
				switch {
				case r.err == nil && r.created:
					winners = append(winners, r)
				case r.err != nil && !r.created:
					losers = append(losers, r)
				}
			}
			if len(winners) != 1 || len(losers) != 1 {
				t.Fatalf("results = %+v, want exactly one created job and one refused start", results)
			}
			winner, loser := winners[0], losers[0]
			if !errors.Is(loser.err, syncjob.ErrActiveJobExists) {
				t.Errorf("refused start returned %v, want ErrActiveJobExists", loser.err)
			}
			if loser.job.ID != winner.job.ID {
				t.Errorf("refused start names job %q, want the winner %q", loser.job.ID, winner.job.ID)
			}
			if n := crashCount(t, pool, `SELECT count(*) FROM sync_jobs`); n != 1 {
				t.Errorf("sync_jobs rows = %d, want 1: the losing insert must be rejected, not stored", n)
			}
			if n := concActiveJobCount(t, pool); n != 1 {
				t.Errorf("active jobs = %d, want 1", n)
			}
			concRequireHighest(t, stop, 1)
		})
	}
}

// Two service instances starting the same update at once: both must get the same job and
// only the one that inserted it may run it. The losing insert is turned away either by the
// update's own unique key or, when both inserts were in flight together, by the one-active-job
// index, after which the start is resolved by looking the update up. Measured over 300 rounds,
// roughly one round in four takes the index path, so a run of these rounds exercises both.
func TestManagersRacingToStartTheSameUpdateShareOneJob(t *testing.T) {
	for round := 1; round <= concRaceRounds; round++ {
		t.Run(fmt.Sprintf("round_%02d", round), func(t *testing.T) {
			pool, sup, first, second := concRaceRound(t)
			stop := concWatchActiveJobs(t, pool)

			managers := []*syncjob.Manager{first, second}
			results := concBurst(2, func(i int) concStartResult { return concStartOn(managers[i], crashUpdate) })

			for i, r := range results {
				if r.err != nil {
					t.Fatalf("Manager %d: Start of the same update failed: %v", i+1, r.err)
				}
			}
			if results[0].job.ID == "" || results[0].job.ID != results[1].job.ID {
				t.Fatalf("job ids = %q and %q, want the same job", results[0].job.ID, results[1].job.ID)
			}
			if results[0].created == results[1].created {
				t.Errorf("created = %v and %v, want exactly one Manager to have created the job", results[0].created, results[1].created)
			}
			if n := crashCount(t, pool, `SELECT count(*) FROM sync_jobs`); n != 1 {
				t.Errorf("sync_jobs rows = %d, want 1", n)
			}
			concRequireHighest(t, stop, 1)

			// Only the Manager that created the job started a worker: the update runs once.
			sup.Unblock("")
			first.Wait()
			second.Wait()
			jobID := results[0].job.ID
			if job := crashJob(t, pool, jobID); job.Status != syncjob.StatusCompleted || crashCounts(job) != crashExpected {
				t.Errorf("job = %s %+v, want COMPLETED %+v", job.Status, crashCounts(job), crashExpected)
			}
			testutil.AssertCounters(t, pool, jobID)
			testutil.AssertCompletedConsistent(t, pool, jobID)
			crashAssertFetches(t, sup.FakeSupplier, crashFetchedOnce)
			if got := concCalls(sup, testutil.OpGetUpdate); got != 1 {
				t.Errorf("GetUpdate calls = %d, want 1: the second Manager must not have run the job", got)
			}
		})
	}
}
