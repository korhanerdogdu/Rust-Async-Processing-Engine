//! # engine-observability
//!
//! Three different signals, three different jobs (see `docs/observability.md`):
//!
//! | Signal  | Question it answers                       | Where it goes                         |
//! |---------|-------------------------------------------|---------------------------------------|
//! | logs    | "what happened, with which ids?"          | stdout + TimescaleDB `operational_logs` |
//! | traces  | "how did *this* request flow, and where did time go?" | OTLP -> Collector -> Jaeger |
//! | metrics | "how is the system doing overall?"        | `/api/v1/metrics` + TimescaleDB `operational_metrics` |

pub mod db_layer;
pub mod metrics;
pub mod propagation;
pub mod sink;
pub mod telemetry;

pub use db_layer::{LogRecord, SinkStats};
pub use metrics::{Counter, Gauge, Histogram, HistogramSnapshot, MetricsRegistry};
pub use telemetry::{LogFormat, TelemetryConfig, TelemetryGuard};

/// Portable graceful-shutdown trigger for supervisors and tests: when
/// `SHUTDOWN_ON_STDIN_CLOSE=1`, closing the process's stdin cancels `token`
/// (same effect as Ctrl-C / SIGTERM). Needed because a parent process on
/// Windows cannot deliver Ctrl-C to one specific child.
///
/// Reading stdin is *blocking* I/O, so it runs on a dedicated OS thread
/// rather than on a Tokio worker.
pub fn shutdown_on_stdin_close(token: tokio_util::sync::CancellationToken) {
    if std::env::var("SHUTDOWN_ON_STDIN_CLOSE").as_deref() != Ok("1") {
        return;
    }
    let _ = std::thread::Builder::new()
        .name("stdin-watch".into())
        .spawn(move || {
            use std::io::Read;
            let mut buf = [0u8; 64];
            let mut stdin = std::io::stdin();
            while matches!(stdin.read(&mut buf), Ok(n) if n > 0) {}
            token.cancel();
        });
}

/// Records runtime facts as gauges so they can be compared with the
/// *configured* values. `num_workers` is what the Tokio runtime really has;
/// `alive_tasks` is a live measurement (tasks != threads).
pub fn record_runtime_metrics(registry: &MetricsRegistry, service: &str) {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        let m = handle.metrics();
        registry
            .gauge("tokio_runtime_workers", &[("service", service)])
            .set(m.num_workers() as i64);
        registry
            .gauge("tokio_alive_tasks", &[("service", service)])
            .set(m.num_alive_tasks() as i64);
        registry
            .gauge("tokio_global_queue_depth", &[("service", service)])
            .set(m.global_queue_depth() as i64);
    }
}
