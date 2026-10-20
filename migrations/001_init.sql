-- 001_init.sql: catalog + durable sync state. Applied by internal/storage.Migrate.

CREATE TABLE products (
  supplier_id  TEXT        NOT NULL,
  product_id   TEXT        NOT NULL,
  status       TEXT        NOT NULL CHECK (status IN ('ACTIVE','WITHDRAWN')),
  revision     BIGINT      NOT NULL CHECK (revision > 0),
  name         TEXT,
  price_minor  BIGINT      CHECK (price_minor >= 0),
  currency     TEXT        CHECK (currency ~ '^[A-Z]{3}$'),
  payload_hash TEXT        NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  withdrawn_at TIMESTAMPTZ,
  PRIMARY KEY (supplier_id, product_id),
  -- ACTIVE rows must be sellable; tombstones may carry null/last-known details.
  CONSTRAINT products_active_details CHECK (
    status <> 'ACTIVE' OR (name IS NOT NULL AND btrim(name) <> '' AND price_minor IS NOT NULL AND currency IS NOT NULL)),
  CONSTRAINT products_withdrawn_at CHECK ((status = 'WITHDRAWN') = (withdrawn_at IS NOT NULL))
);
CREATE INDEX products_status_idx ON products (status, supplier_id, product_id);

-- Every legitimate UPDATE strictly raises the revision; WITHDRAWN is terminal.
-- Enforced here so no code path (or manual SQL) can violate invariants 2 and 3.
CREATE FUNCTION products_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.revision <= OLD.revision THEN
    RAISE EXCEPTION 'products %/%: revision must increase (% -> %)', OLD.supplier_id, OLD.product_id, OLD.revision, NEW.revision
      USING ERRCODE = 'check_violation';
  END IF;
  IF OLD.status = 'WITHDRAWN' AND NEW.status <> 'WITHDRAWN' THEN
    RAISE EXCEPTION 'products %/%: WITHDRAWN is terminal', OLD.supplier_id, OLD.product_id
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER products_guard BEFORE UPDATE ON products FOR EACH ROW EXECUTE FUNCTION products_guard();

CREATE TABLE sync_jobs (
  id                 UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  supplier_id        TEXT        NOT NULL,
  supplier_update_id TEXT        NOT NULL,
  status             TEXT        NOT NULL DEFAULT 'PENDING'
                     CHECK (status IN ('PENDING','RUNNING','INTERRUPTED','NEEDS_ATTENTION','COMPLETED','FAILED')),
  next_cursor        TEXT        NOT NULL DEFAULT '',   -- '' = first page
  last_batch_id      TEXT,
  processed_batches  INT         NOT NULL DEFAULT 0 CHECK (processed_batches >= 0),
  total_batches      INT         CHECK (total_batches >= 1), -- NULL = supplier did not say
  end_of_feed_seen   BOOLEAN     NOT NULL DEFAULT false,
  created_count      INT         NOT NULL DEFAULT 0 CHECK (created_count >= 0),
  updated_count      INT         NOT NULL DEFAULT 0 CHECK (updated_count >= 0),
  withdrawn_count    INT         NOT NULL DEFAULT 0 CHECK (withdrawn_count >= 0),
  duplicate_count    INT         NOT NULL DEFAULT 0 CHECK (duplicate_count >= 0),
  stale_count        INT         NOT NULL DEFAULT 0 CHECK (stale_count >= 0),
  last_error         TEXT,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at         TIMESTAMPTZ,
  completed_at       TIMESTAMPTZ,
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT sync_jobs_update_uq UNIQUE (supplier_id, supplier_update_id),
  -- DB backstop for "no false success": EOF seen and batch count consistent.
  CONSTRAINT sync_jobs_completed_gate CHECK (
    status <> 'COMPLETED' OR (end_of_feed_seen AND completed_at IS NOT NULL
      AND (total_batches IS NULL OR processed_batches = total_batches)))
);
-- At most one PENDING/RUNNING job globally (expression is TRUE for every indexed row).
CREATE UNIQUE INDEX sync_jobs_one_active ON sync_jobs ((status IN ('PENDING','RUNNING')))
  WHERE status IN ('PENDING','RUNNING');
CREATE INDEX sync_jobs_created_idx ON sync_jobs (created_at DESC, id);

CREATE TABLE sync_batches (
  job_id          UUID        NOT NULL REFERENCES sync_jobs(id),
  batch_id        TEXT        NOT NULL,
  seq             INT         NOT NULL CHECK (seq >= 1),
  cursor_in       TEXT        NOT NULL,
  cursor_out      TEXT        NOT NULL,
  has_more        BOOLEAN     NOT NULL,
  payload_hash    TEXT        NOT NULL,
  record_count    INT         NOT NULL,
  created_count   INT         NOT NULL DEFAULT 0,
  updated_count   INT         NOT NULL DEFAULT 0,
  withdrawn_count INT         NOT NULL DEFAULT 0,
  duplicate_count INT         NOT NULL DEFAULT 0,
  stale_count     INT         NOT NULL DEFAULT 0,
  committed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (job_id, batch_id),
  CONSTRAINT sync_batches_cursor_uq UNIQUE (job_id, cursor_in),
  CONSTRAINT sync_batches_seq_uq UNIQUE (job_id, seq)
);

CREATE TABLE sync_changes (
  id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id      UUID        NOT NULL,
  batch_id    TEXT        NOT NULL,
  supplier_id TEXT        NOT NULL,
  product_id  TEXT        NOT NULL,
  revision    BIGINT      NOT NULL,
  change_type TEXT        NOT NULL CHECK (change_type IN ('CREATED','UPDATED','WITHDRAWN')),
  before      JSONB,
  after       JSONB       NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (job_id, batch_id) REFERENCES sync_batches(job_id, batch_id),
  FOREIGN KEY (supplier_id, product_id) REFERENCES products(supplier_id, product_id),
  -- One applied event per product revision, ever: replays cannot duplicate audit rows.
  CONSTRAINT sync_changes_event_uq UNIQUE (supplier_id, product_id, revision)
);
CREATE INDEX sync_changes_job_idx ON sync_changes (job_id, created_at, id);

CREATE TABLE sync_issues (
  id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id        UUID        NOT NULL REFERENCES sync_jobs(id),
  batch_id      TEXT,
  cursor        TEXT,
  supplier_id   TEXT,
  product_id    TEXT,
  code          TEXT        NOT NULL CHECK (code IN ('SUPPLIER_UNAVAILABLE','DATABASE_ERROR','INVALID_PRODUCT_DATA',
                  'VERSION_CONFLICT','BATCH_CONFLICT','INVALID_CURSOR','INVALID_BATCH','REACTIVATION_BLOCKED','UNKNOWN_UPDATE')),
  severity      TEXT        NOT NULL CHECK (severity IN ('BLOCKING','TRANSIENT')),
  status        TEXT        NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','RESOLVED')),
  message       TEXT        NOT NULL,
  fingerprint   TEXT        NOT NULL,
  retry_count   INT         NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at   TIMESTAMPTZ,
  CONSTRAINT sync_issues_resolved_at CHECK ((status = 'RESOLVED') = (resolved_at IS NOT NULL))
);
-- One OPEN row per (job, cause); recurrences bump retry_count instead of duplicating.
CREATE UNIQUE INDEX sync_issues_open_fp ON sync_issues (job_id, fingerprint) WHERE status = 'OPEN';
CREATE INDEX sync_issues_job_status_idx ON sync_issues (job_id, status);

-- DB backstop for "no false success": a job cannot become COMPLETED while a blocking issue is open.
-- Race-free because every issue write for a job holds that job's row lock.
CREATE FUNCTION sync_jobs_completion_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.status = 'COMPLETED' AND EXISTS (
       SELECT 1 FROM sync_issues WHERE job_id = NEW.id AND status = 'OPEN' AND severity = 'BLOCKING') THEN
    RAISE EXCEPTION 'sync job %: cannot complete with open blocking issues', NEW.id USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER sync_jobs_completion_guard BEFORE UPDATE OF status ON sync_jobs
  FOR EACH ROW EXECUTE FUNCTION sync_jobs_completion_guard();
