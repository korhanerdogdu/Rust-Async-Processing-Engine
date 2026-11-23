//! Tower middleware for the REST/WebSocket router.
//!
//! ## The `Service` trait in one paragraph
//! `tower::Service<Request>` is "an async function with backpressure":
//! `poll_ready(&mut self, cx)` asks *"can you accept a request now?"* and may
//! return `Pending` (e.g. a concurrency limit is reached), and
//! `call(&mut self, req)` starts processing and returns a `Future` of the
//! response. Middleware is a `Service` that wraps another `Service` (built by
//! a `Layer`). Because readiness propagates through `poll_ready`, a limit deep
//! in the stack can push back on the caller – see `ConcurrencyLimit` +
//! `LoadShed` below.
//!
//! Stack (outermost first):
//! ```text
//! SetRequestId  -> x-request-id = uuid if the client sent none
//! Trace         -> one span per request, parent = incoming `traceparent`
//! PropagateRequestId -> echo x-request-id on the response
//! RequestMetrics -> hand-written Service: count + latency (below)
//! HandleError    -> converts LoadShed's `Overloaded` error into a 503 response
//! LoadShed       -> if the inner service is not ready, fail NOW instead of queueing
//! ConcurrencyLimit(MAX_INFLIGHT_REQUESTS) -> poll_ready = Pending when full
//! [route-level] Timeout (REST only, not WebSocket) -> 504 after REQUEST_TIMEOUT_MS
//! ```
//!
//! This is **HTTP-level** readiness (how many requests the gateway handles at
//! once). It is independent of the worker's **durable queue admission**
//! (bounded MPSC, 429 when full) – a request can pass this stack and still be
//! rejected by the worker queue, and vice versa.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::Router;
use axum::error_handling::HandleErrorLayer;
use axum::extract::ConnectInfo;
use axum::http::{Request, Response, StatusCode};
use axum::response::IntoResponse;
use engine_observability::MetricsRegistry;
use engine_observability::propagation::server_span_with_client;
use tower::{BoxError, Layer, Service, ServiceBuilder};
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::trace::TraceLayer;
use tracing::Span;

use crate::config::GatewayConfig;

/// A minimal hand-written middleware to show the `Service` contract.
#[derive(Clone)]
pub struct RequestMetricsLayer {
    metrics: Arc<MetricsRegistry>,
}

impl RequestMetricsLayer {
    pub fn new(metrics: Arc<MetricsRegistry>) -> Self {
        Self { metrics }
    }
}

impl<S> Layer<S> for RequestMetricsLayer {
    type Service = RequestMetrics<S>;
    fn layer(&self, inner: S) -> Self::Service {
        RequestMetrics {
            inner,
            metrics: self.metrics.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RequestMetrics<S> {
    inner: S,
    metrics: Arc<MetricsRegistry>,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for RequestMetrics<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    /// We are ready exactly when the wrapped service is ready: readiness (and
    /// therefore backpressure) passes straight through.
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let started = Instant::now();
        let method = req.method().as_str().to_string();
        let metrics = self.metrics.clone();
        let fut = self.inner.call(req);
        Box::pin(async move {
            let res = fut.await;
            if let Ok(r) = &res {
                let class = format!("{}xx", r.status().as_u16() / 100);
                metrics
                    .counter(
                        "http_requests_total",
                        &[("method", &method), ("status", &class)],
                    )
                    .inc();
                metrics
                    .histogram("http_request_duration_ms", &[("method", &method)])
                    .observe(started.elapsed().as_secs_f64() * 1000.0);
            }
            res
        })
    }
}

async fn handle_overload(err: BoxError) -> axum::response::Response {
    if err.is::<tower::load_shed::error::Overloaded>() {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [("retry-after", "1")],
            axum::Json(serde_json::json!({"error": {"code": "gateway_overloaded", "message": "too many concurrent requests"}})),
        )
            .into_response()
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(
                serde_json::json!({"error": {"code": "internal", "message": "internal error"}}),
            ),
        )
            .into_response()
    }
}

/// Applies the global stack to a router.
pub fn apply(router: Router, cfg: &GatewayConfig, metrics: Arc<MetricsRegistry>) -> Router {
    let trace = TraceLayer::new_for_http()
        .make_span_with(|req: &Request<axum::body::Body>| {
            // Present because main.rs serves with `into_make_service_with_connect_info`.
            let peer = req
                .extensions()
                .get::<ConnectInfo<SocketAddr>>()
                .map(|ci| ci.0);
            server_span_with_client(req, "http", peer)
        })
        .on_response(
            |res: &Response<axum::body::Body>, latency: Duration, span: &Span| {
                span.record("http.status_code", res.status().as_u16());
                tracing::info!(
                    event = "http.response",
                    status = res.status().as_u16(),
                    latency_ms = latency.as_secs_f64() * 1000.0,
                    "request finished"
                );
            },
        );
    router.layer(
        ServiceBuilder::new()
            .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
            .layer(trace)
            .layer(PropagateRequestIdLayer::x_request_id())
            .layer(RequestMetricsLayer::new(metrics))
            .layer(HandleErrorLayer::new(handle_overload))
            .load_shed()
            .concurrency_limit(cfg.max_inflight_requests),
    )
}
