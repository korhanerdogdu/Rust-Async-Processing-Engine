-- Useful queries.  psql: docker compose exec timescaledb psql -U engine -d engine -f /dev/stdin < scripts/queries.sql

-- Job status overview
SELECT status, count(*) FROM jobs GROUP BY status ORDER BY status;

-- Latest jobs with queue wait and processing time
SELECT id, job_type, status, attempts,
       round(extract(epoch FROM (started_at - queued_at)) * 1000) AS queue_wait_ms,
       round(extract(epoch FROM (finished_at - started_at)) * 1000) AS processing_ms
FROM jobs ORDER BY created_at DESC LIMIT 20;

-- Full transition audit of the most recent job
SELECT h.at, h.from_status, h.to_status, h.status_version, h.attempt, h.reason, h.actor, h.error_code
FROM job_status_history h
WHERE h.job_id = (SELECT id FROM jobs ORDER BY created_at DESC LIMIT 1)
ORDER BY h.id;

-- TimescaleDB hypertables
SELECT hypertable_name, num_chunks FROM timescaledb_information.hypertables;

-- Structured logs of one job across BOTH processes (replace the id)
-- SELECT time, service, thread_name, level, event, message, trace_id
-- FROM operational_logs WHERE job_id = '<job-uuid>' ORDER BY time;

-- Event counts per minute and service (time_bucket is a TimescaleDB function)
SELECT time_bucket('1 minute', time) AS minute, service, event, count(*)
FROM operational_logs
WHERE time > now() - interval '1 hour' AND event IS NOT NULL
GROUP BY 1, 2, 3 ORDER BY 1 DESC, 4 DESC LIMIT 40;

-- Queue depth over time (worker metric snapshots)
SELECT time_bucket('10 seconds', time) AS t, max(value) AS max_queue_depth
FROM operational_metrics
WHERE metric = 'engine_queue_depth' AND time > now() - interval '30 minutes'
GROUP BY 1 ORDER BY 1 DESC LIMIT 30;

-- Latest processing-time percentiles per job type
SELECT DISTINCT ON (metric, labels) time, metric, labels->>'job_type' AS job_type, round(value::numeric, 1) AS ms
FROM operational_metrics
WHERE metric IN ('job_processing_ms_p50', 'job_processing_ms_p95', 'job_processing_ms_p99', 'job_queue_wait_ms_p95')
ORDER BY metric, labels, time DESC;

-- Dropped log records (log sink backpressure)
SELECT time, value FROM operational_metrics WHERE metric = 'log_sink_dropped_total' ORDER BY time DESC LIMIT 5;
