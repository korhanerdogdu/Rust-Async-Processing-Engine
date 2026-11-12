-- Operational data (separate from domain job storage): TimescaleDB hypertables.
-- Rows arrive from the bounded async batch writers in engine-observability.
-- Nothing here is ingested automatically from OpenTelemetry: traces go to
-- Jaeger through the OTel Collector; these tables hold our own structured
-- log events and metric snapshots.

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE operational_logs (
    time        TIMESTAMPTZ NOT NULL,
    id          UUID        NOT NULL DEFAULT gen_random_uuid(),
    level       TEXT        NOT NULL,
    service     TEXT        NOT NULL,
    process_id  INTEGER     NOT NULL,
    thread_id   TEXT        NOT NULL,
    thread_name TEXT,
    target      TEXT        NOT NULL,
    job_id      UUID,
    request_id  TEXT,
    trace_id    TEXT,
    span_id     TEXT,
    event       TEXT,
    message     TEXT        NOT NULL,
    attributes  JSONB       NOT NULL DEFAULT '{}'::jsonb,
    -- Unique constraints on a hypertable must include the partitioning column.
    PRIMARY KEY (time, id)
);

-- TimescaleDB >= 2.13 dimension-builder syntax.
SELECT create_hypertable('operational_logs', by_range('time', INTERVAL '1 day'), if_not_exists => TRUE);

CREATE INDEX operational_logs_job_idx   ON operational_logs (job_id, time DESC) WHERE job_id IS NOT NULL;
CREATE INDEX operational_logs_trace_idx ON operational_logs (trace_id, time DESC) WHERE trace_id IS NOT NULL;
CREATE INDEX operational_logs_event_idx ON operational_logs (event, time DESC);

CREATE TABLE operational_metrics (
    time    TIMESTAMPTZ      NOT NULL,
    service TEXT             NOT NULL,
    metric  TEXT             NOT NULL,
    labels  JSONB            NOT NULL DEFAULT '{}'::jsonb,
    value   DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable('operational_metrics', by_range('time', INTERVAL '1 day'), if_not_exists => TRUE);

CREATE INDEX operational_metrics_metric_idx ON operational_metrics (metric, time DESC);

-- Minimal retention for a learning project: operational data older than 14
-- days is dropped chunk-by-chunk (cheap: whole chunks are deleted, no VACUUM).
SELECT add_retention_policy('operational_logs',    INTERVAL '14 days', if_not_exists => TRUE);
SELECT add_retention_policy('operational_metrics', INTERVAL '14 days', if_not_exists => TRUE);
