//! REST API (axum). Versioned under `/api/v1`.
//!
//! | Method | Path                              | Success            |
//! |--------|-----------------------------------|--------------------|
//! | POST   | /api/v1/jobs                      | 202 + job + links  |
//! | GET    | /api/v1/jobs?status=&limit=       | 200 list           |
//! | GET    | /api/v1/jobs/{id}                 | 200 job            |
//! | GET    | /api/v1/jobs/{id}/result          | 200 / 409 not ready|
//! | GET    | /api/v1/jobs/{id}/artifact        | 200 application/pdf|
//! | GET    | /api/v1/jobs/{id}/history         | 200 transitions    |
//! | POST   | /api/v1/jobs/{id}/cancel          | 200 cancelled      |
//! | GET    | /api/v1/ws/jobs/{id}              | 101 WebSocket      |
//! | GET    | /api/v1/health/live, /health/ready| 200 / 503          |
//! | GET    | /api/v1/engine/stats              | 200 queue/runtime  |
//! | GET    | /api/v1/metrics                   | Prometheus text    |

use axum::body::Body;
use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRequestParts, Path, Query, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use engine_core::{EngineError, JobId, JobKind, JobStatus, JobSummary};
use engine_ipc::mapping::status_from_pb;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tower_http::timeout::TimeoutLayer;

use crate::state::AppState;
use crate::websocket;

// ---------------------------------------------------------------------------
// Errors: the ONLY place EngineError is mapped to HTTP.
// ---------------------------------------------------------------------------

pub enum ApiError {
    Engine(EngineError, Option<String>),
    BadRequest {
        status: StatusCode,
        code: &'static str,
        message: String,
    },
}

impl ApiError {
    fn engine(err: EngineError, rid: &RequestId) -> Self {
        ApiError::Engine(err, rid.0.clone())
    }
}

pub fn http_status(err: &EngineError) -> StatusCode {
    match err {
        EngineError::Validation(v) if v.code == "invalid_job_id" => StatusCode::BAD_REQUEST,
        EngineError::Validation(_) => StatusCode::UNPROCESSABLE_ENTITY,
        EngineError::NotFound(_) => StatusCode::NOT_FOUND,
        EngineError::ResultNotReady { .. }
        | EngineError::IdempotencyConflict
        | EngineError::InvalidState { .. } => StatusCode::CONFLICT,
        EngineError::Overloaded { .. } => StatusCode::TOO_MANY_REQUESTS,
        EngineError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        EngineError::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
        EngineError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::BadRequest {
                status,
                code,
                message,
            } => (
                status,
                Json(json!({"error": {"code": code, "message": message}})),
            )
                .into_response(),
            ApiError::Engine(err, request_id) => {
                let status = http_status(&err);
                let mut details = serde_json::Map::new();
                match &err {
                    EngineError::Validation(v) => {
                        details.insert("validation_code".into(), json!(v.code));
                        if let Some(f) = &v.field {
                            details.insert("field".into(), json!(f));
                        }
                    }
                    EngineError::ResultNotReady {
                        status,
                        error_code,
                        error_message,
                    } => {
                        details.insert("job_status".into(), json!(status));
                        if let Some(c) = error_code {
                            details.insert("job_error_code".into(), json!(c));
                        }
                        if let Some(m) = error_message {
                            details.insert("job_error_message".into(), json!(m));
                        }
                    }
                    EngineError::InvalidState { status } => {
                        details.insert("job_status".into(), json!(status));
                    }
                    _ => {}
                }
                if status.is_server_error() {
                    tracing::warn!(event = "http.error", code = err.code(), error = %err, "request failed");
                }
                let body = json!({"error": {
                    "code": err.code(),
                    "message": err.public_message(),
                    "request_id": request_id,
                    "details": details,
                }});
                let mut resp = (status, Json(body)).into_response();
                let retry_after = match &err {
                    EngineError::Overloaded { retry_after_ms } => {
                        Some(retry_after_ms.div_ceil(1000).max(1))
                    }
                    EngineError::Unavailable(_) => Some(1),
                    _ => None,
                };
                if let Some(secs) = retry_after {
                    resp.headers_mut()
                        .insert(header::RETRY_AFTER, HeaderValue::from(secs));
                }
                resp
            }
        }
    }
}

/// `x-request-id` (set by `SetRequestIdLayer` if the client sent none).
pub struct RequestId(pub Option<String>);

impl<S: Send + Sync> FromRequestParts<S> for RequestId {
    type Rejection = std::convert::Infallible;
    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(RequestId(
            parts
                .headers
                .get("x-request-id")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string),
        ))
    }
}

fn parse_id(raw: &str, rid: &RequestId) -> Result<JobId, ApiError> {
    raw.parse::<JobId>()
        .map_err(|e| ApiError::engine(e.into(), rid))
}

fn json_rejection(rej: JsonRejection) -> ApiError {
    let status = match rej.status() {
        StatusCode::PAYLOAD_TOO_LARGE => StatusCode::PAYLOAD_TOO_LARGE,
        StatusCode::UNSUPPORTED_MEDIA_TYPE => StatusCode::UNSUPPORTED_MEDIA_TYPE,
        _ => StatusCode::BAD_REQUEST,
    };
    ApiError::BadRequest {
        status,
        code: "malformed_request",
        message: rej.body_text(),
    }
}

// ---------------------------------------------------------------------------
// DTOs
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitBody {
    pub job_type: String,
    #[serde(default)]
    pub payload: Value,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct Links {
    #[serde(rename = "self")]
    pub self_: String,
    pub result: String,
    pub history: String,
    pub cancel: String,
    pub websocket: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct JobView {
    #[serde(flatten)]
    pub job: JobSummary,
    pub links: Links,
}

impl From<JobSummary> for JobView {
    fn from(job: JobSummary) -> Self {
        let base = format!("/api/v1/jobs/{}", job.id);
        let artifact = (job.kind == JobKind::HtmlToPdf).then(|| format!("{base}/artifact"));
        JobView {
            links: Links {
                result: format!("{base}/result"),
                history: format!("{base}/history"),
                cancel: format!("{base}/cancel"),
                websocket: format!("/api/v1/ws/jobs/{}", job.id),
                artifact,
                self_: base,
            },
            job,
        }
    }
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn router(state: AppState) -> Router {
    let timeout = state.config.request_timeout;
    let api = Router::new()
        .route("/api/v1/jobs", post(submit_job).get(list_jobs))
        .route("/api/v1/jobs/{id}", get(get_job))
        .route("/api/v1/jobs/{id}/result", get(get_result))
        .route("/api/v1/jobs/{id}/artifact", get(get_artifact))
        .route("/api/v1/jobs/{id}/history", get(get_history))
        .route("/api/v1/jobs/{id}/cancel", post(cancel_job))
        .route("/api/v1/health/live", get(live))
        .route("/api/v1/health/ready", get(ready))
        .route("/api/v1/engine/stats", get(engine_stats))
        .route("/api/v1/metrics", get(metrics))
        // Deadline for normal requests only; WebSockets are long-lived.
        .layer(TimeoutLayer::with_status_code(
            StatusCode::GATEWAY_TIMEOUT,
            timeout,
        ));
    let ws = Router::new().route("/api/v1/ws/jobs/{id}", get(websocket::ws_job));
    api.merge(ws)
        .fallback(|| async {
            (
                StatusCode::NOT_FOUND,
                Json(json!({"error": {"code": "route_not_found", "message": "no such route"}})),
            )
        })
        .layer(axum::extract::DefaultBodyLimit::max(
            state.config.max_body_bytes,
        ))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn submit_job(
    State(state): State<AppState>,
    rid: RequestId,
    headers: HeaderMap,
    body: Result<Json<SubmitBody>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(body) = body.map_err(json_rejection)?;
    let idempotency_key = body.idempotency_key.or_else(|| {
        headers
            .get("idempotency-key")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    });
    let (job, deduplicated) = state
        .service
        .submit(
            &body.job_type,
            body.payload,
            idempotency_key,
            rid.0.as_deref(),
        )
        .await
        .map_err(|e| ApiError::engine(e, &rid))?;
    tracing::info!(event = "http.job_accepted", job_id = %job.id, deduplicated, "202 Accepted");
    let view = JobView::from(job);
    let location = view.links.self_.clone();
    let mut resp = (
        StatusCode::ACCEPTED,
        Json(json!({"deduplicated": deduplicated, "job": view})),
    )
        .into_response();
    if let Ok(v) = HeaderValue::from_str(&location) {
        resp.headers_mut().insert(header::LOCATION, v);
    }
    if deduplicated {
        resp.headers_mut()
            .insert("idempotent-replayed", HeaderValue::from_static("true"));
    }
    Ok(resp)
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub status: Option<String>,
    pub limit: Option<u32>,
}

async fn list_jobs(
    State(state): State<AppState>,
    rid: RequestId,
    Query(q): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    let status = match q.status.as_deref() {
        Some(s) => Some(
            s.parse::<JobStatus>()
                .map_err(|e| ApiError::engine(e.into(), &rid))?,
        ),
        None => None,
    };
    let limit = q.limit.unwrap_or(50).clamp(1, 500);
    let jobs = state
        .service
        .list(status, limit, rid.0.as_deref())
        .await
        .map_err(|e| ApiError::engine(e, &rid))?;
    let views: Vec<JobView> = jobs.into_iter().map(JobView::from).collect();
    Ok(Json(json!({"count": views.len(), "jobs": views})))
}

async fn get_job(
    State(state): State<AppState>,
    rid: RequestId,
    Path(id): Path<String>,
) -> Result<Json<JobView>, ApiError> {
    let id = parse_id(&id, &rid)?;
    let job = state
        .service
        .get(id, rid.0.as_deref())
        .await
        .map_err(|e| ApiError::engine(e, &rid))?;
    Ok(Json(JobView::from(job)))
}

async fn get_result(
    State(state): State<AppState>,
    rid: RequestId,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let id = parse_id(&id, &rid)?;
    let r = state
        .service
        .result(id, rid.0.as_deref())
        .await
        .map_err(|e| ApiError::engine(e, &rid))?;
    let artifact = r.artifact.map(|a| {
        json!({
            "file_name": a.file_name,
            "content_type": a.content_type,
            "size_bytes": a.size_bytes,
            "sha256": a.sha256,
            "download_url": format!("/api/v1/jobs/{id}/artifact"),
        })
    });
    Ok(Json(
        json!({"job": JobView::from(r.job), "result": r.result, "artifact": artifact}),
    ))
}

async fn get_artifact(
    State(state): State<AppState>,
    rid: RequestId,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let id = parse_id(&id, &rid)?;
    let dl = state
        .service
        .download(id, rid.0.as_deref())
        .await
        .map_err(|e| ApiError::engine(e, &rid))?;
    let body = Body::from_stream(dl.body.map(|chunk| {
        chunk
            .map(axum::body::Bytes::from)
            .map_err(std::io::Error::other)
    }));
    let mut resp = Response::new(body);
    let h = resp.headers_mut();
    if let Ok(v) = HeaderValue::from_str(&dl.info.content_type) {
        h.insert(header::CONTENT_TYPE, v);
    }
    h.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from(dl.info.size_bytes),
    );
    if let Ok(v) = HeaderValue::from_str(&format!("attachment; filename=\"{}\"", dl.info.file_name))
    {
        h.insert(header::CONTENT_DISPOSITION, v);
    }
    if let Ok(v) = HeaderValue::from_str(&dl.info.sha256) {
        h.insert("x-content-sha256", v);
    }
    Ok(resp)
}

async fn get_history(
    State(state): State<AppState>,
    rid: RequestId,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let id = parse_id(&id, &rid)?;
    let entries = state
        .service
        .history(id, rid.0.as_deref())
        .await
        .map_err(|e| ApiError::engine(e, &rid))?;
    Ok(Json(json!({"job_id": id, "transitions": entries})))
}

async fn cancel_job(
    State(state): State<AppState>,
    rid: RequestId,
    Path(id): Path<String>,
) -> Result<Json<JobView>, ApiError> {
    let id = parse_id(&id, &rid)?;
    let job = state
        .service
        .cancel(id, rid.0.as_deref())
        .await
        .map_err(|e| ApiError::engine(e, &rid))?;
    Ok(Json(JobView::from(job)))
}

async fn live() -> Json<Value> {
    Json(json!({"status": "live", "service": "api-gateway"}))
}

/// Ready = the worker answers Health and reports itself ready (engine running,
/// database reachable). The upstream status-stream state is reported too.
async fn ready(State(state): State<AppState>) -> Response {
    let upstream = format!("{:?}", state.service.hub.upstream_state()).to_lowercase();
    match state.service.worker_health().await {
        Ok(h) if h.ready => {
            (StatusCode::OK, Json(json!({"status": "ready", "worker": {"state": h.state, "database_ok": h.database_ok}, "status_stream": upstream}))).into_response()
        }
        Ok(h) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "not_ready", "worker": {"state": h.state, "database_ok": h.database_ok, "detail": h.detail}, "status_stream": upstream})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "not_ready", "worker": {"error": e.public_message()}, "status_stream": upstream})),
        )
            .into_response(),
    }
}

async fn engine_stats(
    State(state): State<AppState>,
    rid: RequestId,
) -> Result<Json<Value>, ApiError> {
    let s = state
        .service
        .engine_stats(rid.0.as_deref())
        .await
        .map_err(|e| ApiError::engine(e, &rid))?;
    let by_status: serde_json::Map<String, Value> = s
        .jobs_by_status
        .iter()
        .filter_map(|c| {
            status_from_pb(c.status).map(|st| (st.as_str().to_string(), json!(c.count)))
        })
        .collect();
    let rt = tokio::runtime::Handle::current().metrics();
    Ok(Json(json!({
        "worker": {
            "worker_id": s.worker_id,
            "state": s.state,
            "queue_capacity": s.queue_capacity,
            "queue_depth": s.queue_depth,
            "max_active_jobs": s.max_active_jobs,
            "active_jobs": s.active_jobs,
            "running_job_ids": s.running_job_ids,
            "jobs_by_status": by_status,
            "rayon_threads": s.rayon_threads,
            "tokio_worker_threads": s.tokio_worker_threads,
            "tokio_alive_tasks": s.tokio_alive_tasks,
            "available_parallelism": s.available_parallelism,
            "status_subscribers": s.status_subscribers,
            "log_records_written": s.log_records_written,
            "log_records_dropped": s.log_records_dropped,
        },
        "gateway": {
            "tokio_worker_threads": rt.num_workers(),
            "tokio_alive_tasks": rt.num_alive_tasks(),
            "configured_tokio_worker_threads": state.config.tokio_worker_threads,
            "status_stream": format!("{:?}", state.service.hub.upstream_state()).to_lowercase(),
            "websocket_slots_free": state.ws_slots.available_permits(),
        }
    })))
}

/// Prometheus text: gateway metrics + the worker's (fetched over IPC).
/// Protected by `METRICS_TOKEN` (Bearer) when that variable is set.
async fn metrics(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Some(token) = &state.config.metrics_token {
        let ok = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.strip_prefix("Bearer ").is_some_and(|t| t == token));
        if !ok {
            return (StatusCode::UNAUTHORIZED, "metrics require a bearer token\n").into_response();
        }
    }
    engine_observability::record_runtime_metrics(&state.metrics, "api-gateway");
    let mut text = String::from("# ---- api-gateway ----\n");
    text.push_str(&state.metrics.render_prometheus());
    match state.service.engine_stats(None).await {
        Ok(s) => {
            text.push_str("# ---- worker-engine (via internal gRPC) ----\n");
            text.push_str(&s.metrics_prometheus);
        }
        Err(e) => text.push_str(&format!(
            "# worker-engine metrics unavailable: {}\n",
            e.code()
        )),
    }
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], text).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_core::ValidationError;

    #[test]
    fn engine_errors_map_to_http() {
        let id = JobId::generate();
        let cases = [
            (
                EngineError::Validation(ValidationError::new("out_of_range", "x")),
                422,
            ),
            (
                EngineError::Validation(ValidationError::new("invalid_job_id", "x")),
                400,
            ),
            (EngineError::NotFound(id), 404),
            (
                EngineError::ResultNotReady {
                    status: JobStatus::Queued,
                    error_code: None,
                    error_message: None,
                },
                409,
            ),
            (EngineError::IdempotencyConflict, 409),
            (
                EngineError::InvalidState {
                    status: JobStatus::Completed,
                },
                409,
            ),
            (
                EngineError::Overloaded {
                    retry_after_ms: 1500,
                },
                429,
            ),
            (EngineError::Unavailable("x".into()), 503),
            (EngineError::Timeout("x".into()), 504),
            (EngineError::Internal("x".into()), 500),
        ];
        for (err, code) in cases {
            assert_eq!(http_status(&err).as_u16(), code, "{err:?}");
        }
    }

    #[test]
    fn overloaded_sets_retry_after() {
        let resp = ApiError::Engine(
            EngineError::Overloaded {
                retry_after_ms: 1500,
            },
            None,
        )
        .into_response();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.headers()[header::RETRY_AFTER], "2");
    }
}
