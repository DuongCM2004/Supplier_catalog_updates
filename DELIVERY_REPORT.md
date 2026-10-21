# Delivery report — Resilient Supplier Catalog Sync

Status: **all automated work done and verified; repository published; author actions pending**
(Render deployment, demo video). Repository: https://github.com/DuongCM2004/Supplier_catalog_updates.
Nothing below is claimed unless a command in this repository produced it.

## 1. Timeline

| When (Asia/Saigon) | Milestone |
|---|---|
| 2026-10-20 18:55 | Kickoff (24 h window ends 2026-10-21 18:55) |
| 19:00 | First commit: module, LF line endings, commit-message checks |
| ≈19:50 | Pause; every command moved into a project-local Go 1.27.3 toolchain (author's rule) |
| 20:19 | Environment and catalog core merged and verified: compose stack, migrations, health checks, revision rules |
| 21:47 | Sync engine verified: atomic batches, replay, crash and restart recovery |
| 22:01 | REST API merged; contract smoke script green with and without the API token |
| 23:26 | Independent code review: 0 critical, 0 high |
| 2026-10-21 00:13 | Review fixes merged, each with a test that fails without it |
| 00:37 | Verification: two consecutive full runs, race run, smoke, demo — all green |
| 00:55–01:05 | Pre-publish checks: hygiene scan clean, clean clone green |
| 16:36 | Repository published as public `DuongCM2004/Supplier_catalog_updates`; `main` pushed |

## 2. Requirements

| Requirement | Status | Evidence |
|---|---|---|
| Start a durable, asynchronous sync job | DONE | `TestFirstImportCreatesProducts`, handler tests (202 + Location, 200 for a known update) |
| Process paginated batches | DONE | `TestFixturesMatchExpectedOutcomes` (nine supplier feeds), demo step 1 |
| Create, update, withdraw products | DONE | `TestHigherRevisionUpdatesProduct`, `TestWithdrawalOfUnseenProductCreatesTombstone` |
| Ignore duplicate events and batches | DONE | `TestReplayOfSameUpdateIsNoOp`, `TestCrashAfterCommitDoesNotDuplicateEffects` |
| Prevent stale overwrites | DONE | `TestLateLowerRevisionIsIgnored`, `products_guard` trigger tests |
| Prevent resurrection of withdrawn products | DONE | `TestStaleActiveCannotResurrectWithdrawnProduct`, trigger rejects WITHDRAWN → ACTIVE |
| Inspect progress and status | DONE | `TestChangeAndIssueEndpointsReportPersistedState`, job endpoints |
| Inspect changes and unresolved issues | DONE | same, plus `/changes` and `/issues` filter tests |
| Resume after outage or restart | DONE | `TestSupplierOutageInterruptsAndResumeCompletes`, `TestRestartedManagerResumesFromCheckpoint`, `TestProcessCrashAfterCommitRecovers`, demo steps 4–5 |
| No false completion | DONE | `TestMissingEndOfFeedNeverCompletes`, `TestOpenBlockingIssuePreventsCompletion`, completion CHECK and trigger |
| Real-PostgreSQL integration tests | DONE | every database test runs on PostgreSQL 17; none skip when `TEST_DATABASE_URL` is set |
| Deterministic outage and crash injection | DONE | fake supplier, commit hooks, `CRASH_POINT`, `SUPPLIER_OUTAGE_CURSORS` |
| Database-enforced concurrency guard | DONE | `TestConcurrentStartsAllowOneActiveJob` (1×202, 19×409), two-manager race tests |
| Bounded retry and backoff | DONE | retry unit tests, `TestSupplierTimeoutIsRetriedThenInterrupts` |
| Structured logs | DONE | `TestJobEmitsStructuredLifecycleLogs`; compose logs show the full job sequence, no password |
| Automated migrations | DONE | applied at start, second start applies nothing |
| Reproducible demo script | DONE | `scripts/demo.sh` asserts every step on a fresh stack |
| Property test of the revision rules (stretch) | DONE | `TestShuffledValidHistoriesConvergeToMaxRevision` |

## 3. Guarantees and where they are enforced

| Guarantee | Code | Database |
|---|---|---|
| Idempotency | batch markers and replay check in `internal/syncjob/processor.go` `ProcessBatch`; duplicate decisions in `internal/catalog/rules.go` `Decide` | `sync_jobs_update_uq`, `sync_batches` keys, `sync_changes_event_uq` |
| Revisions never go down | `Decide` ignores lower revisions | `products_guard` trigger |
| Withdrawal is final | `Decide` blocks a higher ACTIVE event on a tombstone | `products_guard` trigger |
| Atomic batch commit | one transaction per batch in `ProcessBatch`; supplier calls happen outside it | foreign keys from audit rows to batch markers |
| Durable recovery | `Manager.Reconcile`, `Manager.Resume` (incl. orphaned jobs), checkpoint in the batch transaction | all state in PostgreSQL on a named volume |
| Accurate completion | completion gate in `ProcessBatch` and `finalize` (`internal/syncjob/worker.go`) | `sync_jobs_completed_gate` CHECK, `sync_jobs_completion_guard` trigger |

## 4. Verification results

All commands ran inside the project-local Go 1.27.3 environment on Windows 11 with Docker Desktop.

| Check | Command | Result |
|---|---|---|
| Final tree | `go test -p 1 -count=1 -timeout 300s ./...` with `TEST_DATABASE_URL` | 667 pass, 0 fail, 1 skip |
| Two consecutive runs | same, twice | 666 pass, 0 fail, 1 skip — both runs |
| Race detector | `go test -race -p 1 -count=1 ./...` in the `golang:1.27` container | 667 pass, 0 skip, no data race |
| API contract | `bash scripts/smoke.sh` | 73 checks pass; 76 with `API_TOKEN` set, incl. both 401 refusals |
| Demo | `DEMO_PAUSE=0 bash scripts/demo.sh` on a fresh project | all 7 steps pass |
| Clean clone | fresh clone → compose up → demo → tests → smoke | all green |
| Static checks | `gofmt`, `go vet ./...`, `go build ./...`, `go mod verify` | clean |

The only skip is `TestGracefulShutdownInterruptsJob` on the Windows host: a graceful signal
cannot be delivered to a child process there. It runs and passes in the Linux container.

Earlier evidence: an independent code review found no critical or high issues; the medium
findings (database error classification, start/resume during shutdown, unbounded batch size)
and several low ones were fixed, each with a test that fails without the fix. Package test
coverage measured during review: 76.6–97.0 %.

## 5. Outstanding

| Item | Owner | Next step |
|---|---|---|
| Publish the public repository | author | DONE 2026-10-21 16:36: https://github.com/DuongCM2004/Supplier_catalog_updates is public; remote `main` matched local `main` (`gh repo view`, `git ls-remote`) |
| Render deployment | author | Render dashboard → New → Blueprint → `Supplier_catalog_updates` → Deploy Blueprint |
| Deployed check | agent + author | `bash scripts/remote-check.sh <url>` (public steps), then once with `API_TOKEN` exported |
| Demo video (3–5 min) | author | record `DEMO_PAUSE=3 bash scripts/demo.sh` on a fresh `rcs-demo` project |
| README section 8 links | agent | insert the real repository, video and Render URLs once they exist |

Known limits are listed in README sections 3 and 6 (single instance, one active job, no
retry jitter, 1000 products per batch, Render deploy overlap).

## 6. Interview notes

| Topic | Where to look |
|---|---|
| PostgreSQL rather than SQLite | row locks, the partial unique index `sync_jobs_one_active`, triggers and CHECKs in `migrations/001_init.sql` |
| Supplier revisions instead of timestamps | `Decide` in `internal/catalog/rules.go`; revisions are the supplier's order, clocks are not |
| `ON CONFLICT`, unique constraints and locks | `createJob` in `internal/syncjob/store.go`; key-ordered `FOR UPDATE` in `ApplyBatch` (`internal/catalog/repository.go`) |
| Equal revision, different content | `Decide` returns a version conflict; the batch is rolled back and one blocking issue is recorded |
| Tombstone for a never-seen product | a withdrawal creates the row as WITHDRAWN, so a late ACTIVE event cannot create it |
| Transaction boundaries, crash before or after commit | `ProcessBatch`; hooks and `CRASH_POINT` in `cmd/server/main.go`; the e2e restart tests |
| Completion gate | `ProcessBatch` and `finalize`, backed by the CHECK and the completion trigger |
| Retry versus idempotent replay | `withRetry` in `internal/syncjob/retry.go` retries supplier calls; replayed batches are no-ops via markers |
| Batch-atomic instead of all-or-nothing per update | each committed batch is published; a later failure parks the job at its checkpoint |
| Which database errors are retried | `IsRetryable` in `internal/storage/postgres.go`; anything else parks the job for attention |
| Adding a REACTIVATE event | new `002_*.sql` relaxing `products_guard`, a new `Decide` row, a change type, tests |
