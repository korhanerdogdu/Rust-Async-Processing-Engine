# Observability

## Three signals, three questions

| Signal | Question | Implementation | Storage / UI |
|---|---|---|---|
| **Logs** (events) | What happened, with which ids? | `tracing::info!(event = "...", ...)` | stdout (pretty/JSON) + TimescaleDB `operational_logs` |
| **Traces** (spans) | How did *this* request flow across processes, where did time go? | `tracing` spans → `tracing-opentelemetry` → OTLP/HTTP | OTel Collector → Jaeger |
| **Metrics** | How is the system doing overall? | `MetricsRegistry` (atomics + sliding-window histograms) | `/api/v1/metrics` (Prometheus text) + TimescaleDB `operational_metrics` |

Jaeger is built to navigate *individual traces*. TimescaleDB is used for *SQL over time* –
counting, bucketing, joining log events with job ids. They are fed by different paths;
TimescaleDB does **not** ingest OpenTelemetry data by itself.

## Trace context propagation

```text
client ──traceparent?──► gateway TraceLayer span "http POST /api/v1/jobs" (parent = client ctx, if any)
                           └ gateway.submit
                              └ "WorkerEngine/SubmitJob" (client span) ── inject traceparent into gRPC metadata
                                   ═══════ process boundary ═══════
worker TraceLayer span "grpc.internal POST /engine.v1.WorkerEngine/SubmitJob" (parent extracted from metadata)
   └ engine.submit ── stores traceparent + trace_id in the job row
      ├ queue.admission
      └ db.insert_job
... later, possibly after a restart ...
job.execute <type>  (parent = stored traceparent → same trace id)
   ├ db.claim_job
   ├ handler <type>
   └ db.complete_job
```

* W3C `traceparent` via `TraceContextPropagator`, injected by `engine_ipc::metadata::outbound`
  and extracted by `engine_observability::propagation::server_span` (REST, public gRPC and
  internal gRPC all use it).
* **Async job strategy:** the HTTP request span ends at the 202. The execution span is a
  *child of the stored submission context*, started when the job actually runs. Jaeger shows
  one trace with a gap between acceptance and execution – honest about the asynchrony instead
  of pretending the request stayed open. `job_id` is an attribute on the spans.
* The tracer provider is always installed (spans get ids, propagation works); exporting
  happens only if `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Spans are flushed on shutdown
  (`TelemetryGuard::shutdown`).

### Jaeger walkthrough

1. `docker compose up -d`, run both processes with `OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318`.
2. Submit a job; note `job.trace_id` in the 202 response.
3. Open http://127.0.0.1:16686, service `api-gateway` or `worker-engine`, or paste the trace id
   into the search box. You see spans from **both** services in one trace.

Verified example (from a real run):

```text
api-gateway      http POST /api/v1/jobs                                15624us
api-gateway      gateway.submit                                        14532us
api-gateway      WorkerEngine/SubmitJob                                14192us
worker-engine    grpc.internal POST /engine.v1.WorkerEngine/SubmitJob  11046us
worker-engine    engine.submit                                          9875us
worker-engine    queue.admission                                          11us
worker-engine    db.insert_job                                          9439us
worker-engine    job.execute text_transform                            14931us
worker-engine    db.claim_job                                           7687us
worker-engine    handler text_transform                                   95us
worker-engine    db.complete_job                                        6312us
```

## Logs into TimescaleDB without blocking

```text
tracing::info!  ─►  DbLogLayer::on_event  ──try_send──►  mpsc(DB_LOG_CAPACITY)  ──►  run_log_writer
(any thread,        (sync callback: collects            full? drop + count            batch ≤ 256 rows
 inline)             fields, span correlation,           (log_sink_dropped_total)      or every 500 ms,
                     OTel trace/span id)                                                one multi-row INSERT
```

* Only targets of our crates are captured (`api_gateway`, `worker_engine`, `engine_*`), so
  sqlx's own events (emitted while the writer inserts) never loop back into the sink.
  Writer failures are reported with `eprintln!` – deliberately *not* via `tracing`.
* Fields named `payload`, `html`, `input`, `text`, `password`, `secret` are stored as
  `[redacted]`; the code never logs payloads anyway (the e2e test checks for leaks).
* `job_id` / `request_id` are inherited from enclosing spans, `trace_id`/`span_id` read from the
  OpenTelemetry layer (`tracing_opentelemetry::get_otel_context`).
* Shutdown: stop waiting, drain the channel within `drain_timeout` (3 s), then give up.

Stages captured as `event` values: `http.job_accepted`, `gateway.job_validated`,
`job.accepted`, `job.rejected`, `job.processing` (with `queue_wait_ms`), `job.completed`,
`job.failed`, `job.retry_scheduled`, `job.recovered`, `job.released`, `job.cancelled`,
`ws.notification_sent`, `hub.upstream_connected`, `db.connect_failed`, … 

## Metrics

| Metric | Kind | Meaning |
|---|---|---|
| `http_requests_total{method,status}` / `http_request_duration_ms` | counter / summary | gateway HTTP (hand-written Tower `Service`) |
| `gateway_jobs_submitted_total{job_type}` | counter | accepted via the gateway |
| `jobs_accepted_total`, `jobs_rejected_total{reason}` | counter | worker admission |
| `jobs_completed_total`, `jobs_failed_total{job_type,error_code}`, `jobs_retried_total`, `jobs_recovered_total`, `jobs_cancelled_total` | counter | outcomes |
| `engine_queue_depth`, `engine_queue_capacity`, `engine_active_jobs`, `engine_max_active_jobs` | gauge | queue/concurrency, measured vs configured |
| `job_queue_wait_ms{job_type}`, `job_processing_ms{job_type}`, `job_end_to_end_ms{job_type}`, `queue_channel_wait_ms` | summary | p50/p95/p99 from the last 2048 observations |
| `tokio_runtime_workers`, `tokio_alive_tasks`, `tokio_global_queue_depth` | gauge | **measured** runtime facts |
| `tokio_configured_worker_threads`, `rayon_pool_threads` | gauge | **configured** values |
| `log_sink_dropped_total`, `log_sink_written_total` | gauge | log backpressure |
| `websocket_connections`, `websocket_notifications_total`, `worker_rpc_errors_total` | gauge / counter | gateway side |

Percentiles are computed from real observations (nearest rank over a sliding window), not
estimated buckets. `GET /api/v1/metrics` returns gateway metrics plus the worker's (fetched
over IPC); set `METRICS_TOKEN` to require a bearer token.

## SQL you can run

See `scripts/queries.sql`. Highlights:

```sql
-- one job's story across both processes
SELECT time, service, thread_name, level, event, message, trace_id
FROM operational_logs WHERE job_id = '<job-uuid>' ORDER BY time;

-- queue depth over time
SELECT time_bucket('10 seconds', time) AS t, max(value)
FROM operational_metrics WHERE metric = 'engine_queue_depth' GROUP BY 1 ORDER BY 1 DESC;

-- hypertables
SELECT hypertable_name, num_chunks FROM timescaledb_information.hypertables;
```

`docker compose exec timescaledb psql -U engine -d engine` opens a shell.
Retention: `add_retention_policy(..., INTERVAL '14 days')` drops old chunks of both hypertables.
