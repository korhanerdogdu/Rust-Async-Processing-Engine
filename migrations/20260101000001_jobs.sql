-- Domain storage: jobs and their status history.
-- PostgreSQL is the source of truth; the worker's in-memory MPSC queue is not.

CREATE TABLE jobs (
    id                    UUID        PRIMARY KEY,
    job_type              TEXT        NOT NULL
        CHECK (job_type IN ('json_format','text_transform','simulate_delay','html_to_pdf','cpu_compute')),
    -- Canonical validated payload. May contain user content (HTML/text):
    -- never copied into logs. See README "Retention".
    payload               JSONB       NOT NULL,
    status                TEXT        NOT NULL
        CHECK (status IN ('queued','processing','completed','failed','cancelled')),
    -- Bumped by trigger on every status change; clients use it to order and
    -- de-duplicate notifications.
    status_version        BIGINT      NOT NULL DEFAULT 1,
    attempts              INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts          INTEGER     NOT NULL CHECK (max_attempts >= 1),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    queued_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at            TIMESTAMPTZ,
    finished_at           TIMESTAMPTZ,
    next_attempt_at       TIMESTAMPTZ,
    -- Lease: which worker incarnation owns the current attempt, until when.
    lease_owner           TEXT,
    lease_expires_at      TIMESTAMPTZ,
    error_code            TEXT,
    error_message         TEXT,
    -- Small results inline; large binary results as artifact references.
    result                JSONB,
    artifact_path         TEXT,
    artifact_content_type TEXT,
    artifact_size_bytes   BIGINT,
    artifact_sha256       TEXT,
    idempotency_key       TEXT,
    request_id            TEXT,
    trace_id              TEXT,
    traceparent           TEXT,
    -- Written by every status-changing statement; copied into history.
    transition_reason     TEXT,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT completed_has_result CHECK (status <> 'completed' OR result IS NOT NULL)
);

CREATE UNIQUE INDEX jobs_idempotency_key_uq ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;
-- Dispatcher scan: oldest due queued jobs.
CREATE INDEX jobs_dispatch_idx ON jobs (queued_at) WHERE status = 'queued';
-- Lease recovery scan.
CREATE INDEX jobs_lease_idx ON jobs (lease_expires_at) WHERE status = 'processing';
-- GET /jobs?status=...
CREATE INDEX jobs_status_created_idx ON jobs (status, created_at DESC);

CREATE TABLE job_status_history (
    id             BIGSERIAL   PRIMARY KEY,
    job_id         UUID        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    from_status    TEXT,
    to_status      TEXT        NOT NULL,
    status_version BIGINT      NOT NULL,
    attempt        INTEGER     NOT NULL,
    reason         TEXT,
    actor          TEXT,
    error_code     TEXT,
    at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX job_status_history_job_idx ON job_status_history (job_id, id);

-- The state machine, enforced by the database itself (mirrors
-- engine_core::JobStatus::can_transition_to). Any illegal UPDATE fails.
CREATE FUNCTION jobs_before_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status IS DISTINCT FROM OLD.status THEN
        IF NOT (
            (OLD.status = 'queued'     AND NEW.status IN ('processing', 'cancelled', 'failed')) OR
            (OLD.status = 'processing' AND NEW.status IN ('completed', 'failed', 'cancelled', 'queued'))
        ) THEN
            RAISE EXCEPTION 'illegal job status transition % -> % (job %)', OLD.status, NEW.status, OLD.id
                USING ERRCODE = 'check_violation';
        END IF;
        NEW.status_version := OLD.status_version + 1;
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$;

CREATE TRIGGER jobs_before_update
    BEFORE UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION jobs_before_update();

-- History is written in the same statement/transaction as the change itself:
-- a transition can never be committed without its audit row.
CREATE FUNCTION jobs_record_history() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO job_status_history (job_id, from_status, to_status, status_version, attempt, reason, actor, error_code)
        VALUES (NEW.id, NULL, NEW.status, NEW.status_version, NEW.attempts, NEW.transition_reason, NEW.lease_owner, NEW.error_code);
    ELSIF NEW.status IS DISTINCT FROM OLD.status THEN
        INSERT INTO job_status_history (job_id, from_status, to_status, status_version, attempt, reason, actor, error_code)
        VALUES (NEW.id, OLD.status, NEW.status, NEW.status_version, NEW.attempts, NEW.transition_reason, NEW.lease_owner, NEW.error_code);
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER jobs_record_history
    AFTER INSERT OR UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION jobs_record_history();
