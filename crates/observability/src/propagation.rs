//! W3C Trace Context (`traceparent`) propagation helpers.
//!
//! * Inbound HTTP/gRPC: [`server_span`] creates the request span and sets its
//!   parent from the incoming `traceparent` header (gRPC metadata are HTTP/2
//!   headers, so one function serves REST, public gRPC and internal gRPC).
//! * Outbound: [`inject_current`] writes the *current* span context into any
//!   carrier implementing `opentelemetry::propagation::Injector`
//!   (see `engine-ipc` for the tonic metadata injector).
//! * Async jobs: the worker stores the submission `traceparent` in the job row;
//!   the execution span is parented with [`context_from_traceparent`].

use std::collections::HashMap;

use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::TraceContextExt;
use opentelemetry::{Context, global};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Request-id header used by both public transports and the internal IPC.
pub const REQUEST_ID_HEADER: &str = "x-request-id";

struct HeaderExtractor<'a>(&'a http::HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }
    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

pub fn extract_from_headers(headers: &http::HeaderMap) -> Context {
    global::get_text_map_propagator(|p| p.extract(&HeaderExtractor(headers)))
}

/// Builds the server-side span for an inbound HTTP or gRPC request.
pub fn server_span<B>(req: &http::Request<B>, kind: &'static str) -> Span {
    let request_id = req
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-")
        .to_string();
    let path = req.uri().path().to_string();
    let span = tracing::info_span!(
        "request",
        otel.name = %format!("{kind} {} {path}", req.method()),
        otel.kind = "server",
        rpc.kind = kind,
        http.method = %req.method(),
        url.path = %path,
        request_id = %request_id,
        http.status_code = tracing::field::Empty,
    );
    let parent = extract_from_headers(req.headers());
    if parent.span().span_context().is_valid() {
        let _ = span.set_parent(parent);
    }
    span
}

pub fn inject_current(injector: &mut dyn Injector) {
    let cx = Span::current().context();
    global::get_text_map_propagator(|p| p.inject_context(&cx, injector));
}

/// The current span's context serialized as a `traceparent` string.
pub fn current_traceparent() -> Option<String> {
    let mut map: HashMap<String, String> = HashMap::new();
    inject_current(&mut map);
    map.remove("traceparent")
}

pub fn context_from_traceparent(traceparent: &str) -> Context {
    let mut map = HashMap::new();
    map.insert("traceparent".to_string(), traceparent.to_string());
    global::get_text_map_propagator(|p| p.extract(&map))
}

/// Hex trace id of the current span, if a valid one exists.
pub fn current_trace_id() -> Option<String> {
    let cx = Span::current().context();
    let span = cx.span();
    let sc = span.span_context();
    sc.is_valid().then(|| sc.trace_id().to_string())
}
