//! Asynchronous, bounded, batching writers for TimescaleDB.
//!
//! ```text
//!  tracing::info!(..) ─► DbLogLayer::on_event ─try_send─► mpsc(bounded) ─► run_log_writer task
//!     (any thread, sync, never blocks)                       │ drop+count     │ batch ≤ N rows or
//!                                                            ▼ when full      │ every flush_interval
//!                                                                             ▼
//!                                                     INSERT .. VALUES (..),(..),.. (one round trip)
//! ```
//!
//! On shutdown the writer stops waiting for new records, drains what is
//! already queued and gives up after `drain_timeout` (bounded shutdown).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use chrono::Utc;
use serde_json::{Map, Value};
use sqlx::{PgPool, Postgres, QueryBuilder};
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout_at};
use tokio_util::sync::CancellationToken;

use crate::db_layer::{LogRecord, SinkStats};
use crate::metrics::MetricsRegistry;

#[derive(Debug, Clone)]
pub struct SinkConfig {
    pub batch_size: usize,
    pub flush_interval: Duration,
    pub drain_timeout: Duration,
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self {
            batch_size: 256,
            flush_interval: Duration::from_millis(500),
            drain_timeout: Duration::from_secs(3),
        }
    }
}

/// The single consumer of the log channel.
pub async fn run_log_writer(
    pool: PgPool,
    mut rx: mpsc::Receiver<LogRecord>,
    cfg: SinkConfig,
    stats: Arc<SinkStats>,
    shutdown: CancellationToken,
) {
    loop {
        let first = tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            rec = rx.recv() => match rec {
                Some(r) => r,
                None => return,
            },
        };
        let mut batch = Vec::with_capacity(cfg.batch_size);
        batch.push(first);
        let deadline = Instant::now() + cfg.flush_interval;
        while batch.len() < cfg.batch_size {
            match timeout_at(deadline, rx.recv()).await {
                Ok(Some(r)) => batch.push(r),
                Ok(None) | Err(_) => break,
            }
        }
        write_logs(&pool, &batch, &stats).await;
    }

    // Shutdown: stop accepting, drain within a bounded deadline.
    rx.close();
    let deadline = Instant::now() + cfg.drain_timeout;
    let mut batch = Vec::with_capacity(cfg.batch_size);
    while let Ok(r) = rx.try_recv() {
        batch.push(r);
        if batch.len() == cfg.batch_size {
            if timeout_at(deadline, write_logs(&pool, &batch, &stats))
                .await
                .is_err()
            {
                eprintln!("[log-sink] drain deadline exceeded; remaining records dropped");
                return;
            }
            batch.clear();
        }
    }
    if !batch.is_empty()
        && timeout_at(deadline, write_logs(&pool, &batch, &stats))
            .await
            .is_err()
    {
        eprintln!("[log-sink] drain deadline exceeded; remaining records dropped");
    }
}

async fn write_logs(pool: &PgPool, batch: &[LogRecord], stats: &SinkStats) {
    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        "INSERT INTO operational_logs (time, level, service, process_id, thread_id, thread_name, \
         target, job_id, request_id, trace_id, span_id, event, message, attributes) ",
    );
    qb.push_values(batch, |mut b, r| {
        b.push_bind(r.time)
            .push_bind(&r.level)
            .push_bind(&r.service)
            .push_bind(r.process_id)
            .push_bind(&r.thread_id)
            .push_bind(&r.thread_name)
            .push_bind(&r.target)
            .push_bind(r.job_id)
            .push_bind(&r.request_id)
            .push_bind(&r.trace_id)
            .push_bind(&r.span_id)
            .push_bind(&r.event)
            .push_bind(&r.message)
            .push_bind(&r.attributes);
    });
    match qb.build().execute(pool).await {
        Ok(_) => {
            stats
                .written
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
        }
        Err(e) => {
            stats.write_failures.fetch_add(1, Ordering::Relaxed);
            stats
                .dropped
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
            // stderr, NOT tracing: logging this through tracing would feed the
            // failure back into the sink we are failing to write.
            eprintln!("[log-sink] failed to write {} log rows: {e}", batch.len());
        }
    }
}

/// Periodically snapshots the metrics registry into `operational_metrics`.
pub async fn run_metrics_writer(
    pool: PgPool,
    registry: Arc<MetricsRegistry>,
    service: String,
    interval: Duration,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            _ = ticker.tick() => {}
        }
        write_metrics(&pool, &registry, &service).await;
    }
    // One final snapshot so the last state before shutdown is queryable.
    let _ = tokio::time::timeout(
        Duration::from_secs(2),
        write_metrics(&pool, &registry, &service),
    )
    .await;
}

async fn write_metrics(pool: &PgPool, registry: &MetricsRegistry, service: &str) {
    let samples = registry.samples();
    if samples.is_empty() {
        return;
    }
    let now = Utc::now();
    for chunk in samples.chunks(500) {
        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO operational_metrics (time, service, metric, labels, value) ",
        );
        qb.push_values(chunk, |mut b, s| {
            let labels: Map<String, Value> = s
                .labels
                .iter()
                .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                .collect();
            b.push_bind(now)
                .push_bind(service)
                .push_bind(&s.name)
                .push_bind(Value::Object(labels))
                .push_bind(s.value);
        });
        if let Err(e) = qb.build().execute(pool).await {
            eprintln!(
                "[metrics-sink] failed to write {} metric rows: {e}",
                chunk.len()
            );
            return;
        }
    }
}
