//! A `tracing_subscriber::Layer` that turns selected events into
//! [`LogRecord`]s and hands them to a **bounded** `mpsc` channel with
//! `try_send`.
//!
//! Why `try_send` and not `send().await`?
//! `Layer::on_event` is a *synchronous* callback invoked inline wherever
//! `tracing::info!` is called – often on a Tokio worker thread in the middle of
//! request handling. It must never block or await. If the channel is full we
//! **drop** the record and increment a counter (`dropped`), which is exported
//! as `log_sink_dropped_total`. Critical processing is never slowed by
//! logging.
//!
//! Recursion: the writer task itself uses sqlx, which emits `tracing` events.
//! Those are excluded because only targets listed in `target_prefixes` are
//! captured, and the sink's own diagnostics go to stderr via `eprintln!`.
//!
//! This channel is a *second, independent* MPSC channel. It has nothing to do
//! with the main job queue (exactly one receiver there, too: the log writer).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use chrono::{DateTime, Utc};
use opentelemetry::trace::TraceContextExt;
use serde_json::{Map, Value};
use tokio::sync::mpsc;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Dispatch, Event, Level, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use uuid::Uuid;

/// Field names never copied into stored attributes, even if someone logs them.
const REDACTED_FIELDS: &[&str] = &["payload", "html", "input", "text", "password", "secret"];

/// One structured log row destined for `operational_logs`.
#[derive(Debug, Clone, PartialEq)]
pub struct LogRecord {
    pub time: DateTime<Utc>,
    pub level: String,
    pub service: String,
    pub process_id: i32,
    pub thread_id: String,
    pub thread_name: Option<String>,
    pub target: String,
    pub job_id: Option<Uuid>,
    pub request_id: Option<String>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub event: Option<String>,
    pub message: String,
    pub attributes: Value,
}

#[derive(Debug, Default)]
pub struct SinkStats {
    pub enqueued: AtomicU64,
    pub dropped: AtomicU64,
    pub written: AtomicU64,
    pub write_failures: AtomicU64,
}

impl SinkStats {
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
    pub fn written(&self) -> u64 {
        self.written.load(Ordering::Relaxed)
    }
}

pub struct DbLogLayer {
    tx: mpsc::Sender<LogRecord>,
    service: String,
    min_level: Level,
    target_prefixes: Vec<String>,
    stats: Arc<SinkStats>,
    dispatch: OnceLock<tracing::dispatcher::WeakDispatch>,
}

impl DbLogLayer {
    pub fn new(
        tx: mpsc::Sender<LogRecord>,
        service: impl Into<String>,
        min_level: Level,
        target_prefixes: Vec<String>,
        stats: Arc<SinkStats>,
    ) -> Self {
        Self {
            tx,
            service: service.into(),
            min_level,
            target_prefixes,
            stats,
            dispatch: OnceLock::new(),
        }
    }

    fn wants(&self, meta: &tracing::Metadata<'_>) -> bool {
        // Level ordering in `tracing`: TRACE > DEBUG > INFO > WARN > ERROR.
        *meta.level() <= self.min_level
            && self
                .target_prefixes
                .iter()
                .any(|p| meta.target().starts_with(p.as_str()))
    }
}

/// Correlation fields remembered per span so events inside the span inherit them.
#[derive(Debug, Default, Clone)]
struct SpanCorrelation {
    job_id: Option<Uuid>,
    request_id: Option<String>,
}

#[derive(Default)]
struct FieldCollector {
    message: Option<String>,
    event: Option<String>,
    job_id: Option<Uuid>,
    request_id: Option<String>,
    attributes: Map<String, Value>,
}

impl FieldCollector {
    fn put(&mut self, field: &Field, value: Value) {
        let name = field.name();
        match name {
            "message" => self.message = Some(value_to_string(value)),
            "event" => self.event = Some(value_to_string(value)),
            "job_id" => {
                let s = value_to_string(value);
                self.job_id = Uuid::parse_str(&s).ok();
                if self.job_id.is_none() {
                    self.attributes.insert(name.into(), Value::String(s));
                }
            }
            "request_id" => {
                let s = value_to_string(value);
                if s != "-" {
                    self.request_id = Some(s);
                }
            }
            n if REDACTED_FIELDS.contains(&n) => {
                self.attributes
                    .insert(n.into(), Value::String("[redacted]".into()));
            }
            n if n.starts_with("otel.") => {}
            n => {
                self.attributes.insert(n.into(), value);
            }
        }
    }
}

fn value_to_string(v: Value) -> String {
    match v {
        Value::String(s) => s,
        other => other.to_string(),
    }
}

impl Visit for FieldCollector {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.put(field, Value::String(format!("{value:?}")));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.put(field, Value::String(value.to_string()));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.put(field, Value::from(value));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.put(field, Value::from(value));
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.put(field, Value::from(value));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.put(field, Value::from(value));
    }
}

impl<S> tracing_subscriber::Layer<S> for DbLogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_register_dispatch(&self, subscriber: &Dispatch) {
        let _ = self.dispatch.set(subscriber.downgrade());
    }

    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut c = FieldCollector::default();
        attrs.record(&mut c);
        if c.job_id.is_some() || c.request_id.is_some() {
            if let Some(span) = ctx.span(id) {
                span.extensions_mut().insert(SpanCorrelation {
                    job_id: c.job_id,
                    request_id: c.request_id,
                });
            }
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let mut c = FieldCollector::default();
        values.record(&mut c);
        if c.job_id.is_none() && c.request_id.is_none() {
            return;
        }
        if let Some(span) = ctx.span(id) {
            let mut ext = span.extensions_mut();
            match ext.get_mut::<SpanCorrelation>() {
                Some(existing) => {
                    existing.job_id = c.job_id.or(existing.job_id);
                    existing.request_id = c.request_id.or(existing.request_id.take());
                }
                None => ext.insert(SpanCorrelation {
                    job_id: c.job_id,
                    request_id: c.request_id,
                }),
            }
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        if !self.wants(event.metadata()) {
            return;
        }
        let mut c = FieldCollector::default();
        event.record(&mut c);

        // Inherit correlation ids from enclosing spans (innermost wins).
        let mut innermost: Option<Id> = None;
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope {
                if innermost.is_none() {
                    innermost = Some(span.id());
                }
                if let Some(corr) = span.extensions().get::<SpanCorrelation>() {
                    if c.job_id.is_none() {
                        c.job_id = corr.job_id;
                    }
                    if c.request_id.is_none() {
                        c.request_id = corr.request_id.clone();
                    }
                }
            }
        }

        // Trace/span id from the OpenTelemetry layer. No extension guard is held
        // here (required by `get_otel_context`, otherwise it could deadlock).
        let (trace_id, span_id) = match (innermost, self.dispatch.get().and_then(|d| d.upgrade())) {
            (Some(id), Some(dispatch)) => tracing_opentelemetry::get_otel_context(&id, &dispatch)
                .map(|cx| {
                    let span = cx.span();
                    let sc = span.span_context();
                    if sc.is_valid() {
                        (
                            Some(sc.trace_id().to_string()),
                            Some(sc.span_id().to_string()),
                        )
                    } else {
                        (None, None)
                    }
                })
                .unwrap_or((None, None)),
            _ => (None, None),
        };

        let thread = std::thread::current();
        let record = LogRecord {
            time: Utc::now(),
            level: event.metadata().level().to_string(),
            service: self.service.clone(),
            process_id: std::process::id() as i32,
            thread_id: format!("{:?}", thread.id()),
            thread_name: thread.name().map(str::to_string),
            target: event.metadata().target().to_string(),
            job_id: c.job_id,
            request_id: c.request_id,
            trace_id,
            span_id,
            event: c.event,
            message: c.message.unwrap_or_default(),
            attributes: Value::Object(c.attributes),
        };

        match self.tx.try_send(record) {
            Ok(()) => {
                self.stats.enqueued.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                // Full (or writer gone): drop, never block the caller.
                self.stats.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::layer::SubscriberExt;

    fn layer(capacity: usize) -> (DbLogLayer, mpsc::Receiver<LogRecord>, Arc<SinkStats>) {
        let (tx, rx) = mpsc::channel(capacity);
        let stats = Arc::new(SinkStats::default());
        let layer = DbLogLayer::new(
            tx,
            "test-svc",
            Level::INFO,
            vec!["engine_observability".into()],
            stats.clone(),
        );
        (layer, rx, stats)
    }

    #[test]
    fn captures_fields_and_inherits_span_correlation() {
        let (layer, mut rx, _stats) = layer(16);
        let subscriber = tracing_subscriber::registry().with(layer);
        let job_id = Uuid::new_v4();
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("job", job_id = %job_id, request_id = "req-1");
            let _g = span.enter();
            tracing::info!(
                event = "job.started",
                attempt = 2,
                html = "<secret>",
                "starting job"
            );
            tracing::debug!("filtered out by level");
        });
        let rec = rx.try_recv().expect("one record");
        assert_eq!(rec.message, "starting job");
        assert_eq!(rec.event.as_deref(), Some("job.started"));
        assert_eq!(rec.job_id, Some(job_id));
        assert_eq!(rec.request_id.as_deref(), Some("req-1"));
        assert_eq!(rec.attributes["attempt"], 2);
        assert_eq!(rec.attributes["html"], "[redacted]");
        assert_eq!(rec.service, "test-svc");
        assert!(rx.try_recv().is_err(), "debug event must be filtered");
    }

    #[test]
    fn full_channel_drops_and_counts_instead_of_blocking() {
        let (layer, _rx, stats) = layer(2);
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            for i in 0..10 {
                tracing::info!(i, "burst");
            }
        });
        assert_eq!(stats.enqueued.load(Ordering::Relaxed), 2);
        assert_eq!(stats.dropped(), 8);
    }

    #[test]
    fn ignores_foreign_targets() {
        let (layer, mut rx, _) = layer(4);
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "sqlx::query", "SELECT 1");
        });
        assert!(rx.try_recv().is_err());
    }
}
