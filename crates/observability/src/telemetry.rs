//! Per-process telemetry bootstrap. Each binary calls [`init`] once.
//!
//! Layers installed on the `tracing` registry:
//!
//! 1. `fmt` – human (`LOG_FORMAT=pretty`) or JSON (`LOG_FORMAT=json`) logs to
//!    stdout with UTC timestamps, level, target, **thread id and thread
//!    name**, filtered by `RUST_LOG`.
//! 2. `tracing-opentelemetry` – turns spans into OpenTelemetry spans. Always
//!    installed (so trace ids exist and W3C context propagates even without a
//!    collector); exported over OTLP/HTTP only if `OTEL_EXPORTER_OTLP_ENDPOINT`
//!    is set.
//! 3. [`DbLogLayer`] – structured events -> bounded channel -> TimescaleDB.
//!
//! **Call `init` before building the Tokio runtime.** The OTLP HTTP exporter
//! uses a blocking reqwest client on the batch processor's own thread; creating
//! it inside an async context can panic.

use std::sync::Arc;
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use tokio::sync::mpsc;
use tracing::Level;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

use crate::db_layer::{DbLogLayer, LogRecord, SinkStats};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Pretty,
    Json,
}

#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    pub service_name: String,
    /// Base OTLP/HTTP endpoint, e.g. `http://localhost:4318`.
    pub otlp_endpoint: Option<String>,
    pub log_format: LogFormat,
    /// Capacity of the DB log channel (records). Overflow is dropped + counted.
    pub db_log_capacity: usize,
    pub db_log_level: Level,
}

impl TelemetryConfig {
    /// Reads `OTEL_EXPORTER_OTLP_ENDPOINT`, `LOG_FORMAT`, `DB_LOG_CAPACITY`,
    /// `DB_LOG_LEVEL`.
    pub fn from_env(service_name: &str) -> Self {
        let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .ok()
            .map(|s| s.trim().trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty());
        let log_format = match std::env::var("LOG_FORMAT").as_deref() {
            Ok("json") => LogFormat::Json,
            _ => LogFormat::Pretty,
        };
        let db_log_capacity = std::env::var("DB_LOG_CAPACITY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8192);
        let db_log_level = std::env::var("DB_LOG_LEVEL")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(Level::INFO);
        Self {
            service_name: service_name.to_string(),
            otlp_endpoint,
            log_format,
            db_log_capacity,
            db_log_level,
        }
    }
}

/// Keeps the tracer provider alive; call [`TelemetryGuard::shutdown`] at exit
/// to flush pending spans.
pub struct TelemetryGuard {
    provider: SdkTracerProvider,
    log_rx: Option<mpsc::Receiver<LogRecord>>,
    pub sink_stats: Arc<SinkStats>,
    pub otlp_enabled: bool,
}

impl TelemetryGuard {
    /// The receiving end of the DB log channel, to be handed to
    /// [`crate::sink::run_log_writer`] once a database pool exists.
    pub fn take_log_receiver(&mut self) -> Option<mpsc::Receiver<LogRecord>> {
        self.log_rx.take()
    }

    /// Flushes and shuts down the span exporter. Blocking: call it after the
    /// Tokio runtime has been shut down (or from `spawn_blocking`).
    pub fn shutdown(self) {
        if let Err(e) = self.provider.shutdown_with_timeout(Duration::from_secs(5)) {
            eprintln!("[telemetry] tracer provider shutdown: {e}");
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TelemetryError {
    #[error("failed to build OTLP exporter: {0}")]
    Exporter(String),
    #[error("failed to install tracing subscriber: {0}")]
    Subscriber(String),
}

pub fn init(cfg: TelemetryConfig) -> Result<TelemetryGuard, TelemetryError> {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let resource = Resource::builder()
        .with_service_name(cfg.service_name.clone())
        .with_attribute(KeyValue::new("process.pid", std::process::id() as i64))
        .build();

    let mut builder = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::AlwaysOn)));
    if let Some(base) = &cfg.otlp_endpoint {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(format!("{base}/v1/traces"))
            .with_timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| TelemetryError::Exporter(e.to_string()))?;
        builder = builder.with_batch_exporter(exporter);
    }
    let provider = builder.build();
    let tracer = provider.tracer(cfg.service_name.clone());
    opentelemetry::global::set_tracer_provider(provider.clone());

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("info,h2=warn,hyper=warn,tower=warn,tonic=info,sqlx=warn,opentelemetry=warn,reqwest=warn")
    });

    let fmt_layer = match cfg.log_format {
        LogFormat::Pretty => tracing_subscriber::fmt::layer()
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_target(true)
            .boxed(),
        LogFormat::Json => tracing_subscriber::fmt::layer()
            .json()
            .with_current_span(true)
            .with_span_list(false)
            .with_thread_ids(true)
            .with_thread_names(true)
            .boxed(),
    };

    let otel_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        // Spans from the OTel exporter's own HTTP client must never be traced.
        .with_filter(tracing_subscriber::filter::filter_fn(|meta| {
            !meta.target().starts_with("opentelemetry") && !meta.target().starts_with("reqwest")
        }));

    let (tx, rx) = mpsc::channel(cfg.db_log_capacity);
    let sink_stats = Arc::new(SinkStats::default());
    let db_layer = DbLogLayer::new(
        tx,
        cfg.service_name.clone(),
        cfg.db_log_level,
        vec![
            "api_gateway".into(),
            "worker_engine".into(),
            "engine_application".into(),
            "engine_handlers".into(),
            "engine_persistence".into(),
            "engine_ipc".into(),
        ],
        sink_stats.clone(),
    );

    tracing_subscriber::registry()
        .with(fmt_layer.with_filter(env_filter))
        .with(otel_layer)
        .with(db_layer)
        .try_init()
        .map_err(|e| TelemetryError::Subscriber(e.to_string()))?;

    Ok(TelemetryGuard {
        provider,
        log_rx: Some(rx),
        sink_stats,
        otlp_enabled: cfg.otlp_endpoint.is_some(),
    })
}
