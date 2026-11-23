//! `GET /api/v1/ws/jobs/{id}` – real WebSocket push of job status.
//!
//! Protocol (server -> client, JSON text frames):
//! ```json
//! {"type":"snapshot","job":{...}}          // immediately, from PostgreSQL via the worker
//! {"type":"status","event":{...}}          // each newer transition (originates in the worker process)
//! {"type":"error","error":{"code","message"}}
//! ```
//! The server closes the socket (code 1000) after a terminal status. A client
//! that reconnects simply gets a fresh snapshot (current truth) – there is no
//! durable event replay, by design.

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use engine_core::JobId;
use serde_json::json;
use tokio::sync::OwnedSemaphorePermit;
use tracing::Instrument;

use crate::hub::{WatchItem, watch_job};
use crate::rest::JobView;
use crate::state::AppState;

pub async fn ws_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
    ws: WebSocketUpgrade,
) -> Response {
    let Ok(job_id) = id.parse::<JobId>() else {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(
                json!({"error": {"code": "invalid_job_id", "message": "job id must be a UUID"}}),
            ),
        )
            .into_response();
    };
    let Ok(permit) = state.ws_slots.clone().try_acquire_owned() else {
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(json!({"error": {"code": "too_many_websockets", "message": "websocket limit reached"}}))).into_response();
    };
    // `on_upgrade` runs the socket in a new task, which would lose the request
    // span. Run it inside a child span so ws.* logs keep client_addr/client_name.
    let session = tracing::info_span!("ws.session", %job_id);
    ws.on_upgrade(move |socket| serve_socket(socket, state, job_id, permit).instrument(session))
}

async fn send_json(socket: &mut WebSocket, value: serde_json::Value) -> bool {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .is_ok()
}

async fn serve_socket(
    mut socket: WebSocket,
    state: AppState,
    job_id: JobId,
    _permit: OwnedSemaphorePermit,
) {
    let gauge = state.metrics.gauge("websocket_connections", &[]);
    gauge.inc();
    let sent = state.metrics.counter("websocket_notifications_total", &[]);
    tracing::info!(event = "ws.connected", %job_id, "websocket subscribed");

    let mut items = watch_job(state.service.clone(), job_id, state.shutdown.child_token());
    let mut ping = tokio::time::interval(std::time::Duration::from_secs(20));
    ping.tick().await; // first tick is immediate

    let close_reason = loop {
        tokio::select! {
            item = items.recv() => match item {
                Some(Ok(WatchItem::Snapshot(job))) => {
                    let status = job.status;
                    if !send_json(&mut socket, json!({"type": "snapshot", "job": JobView::from(job)})).await {
                        break "client gone";
                    }
                    sent.inc();
                    tracing::info!(event = "ws.notification_sent", %job_id, kind = "snapshot", status = %status, "snapshot delivered");
                }
                Some(Ok(WatchItem::Event(ev))) => {
                    let status = ev.status;
                    if !send_json(&mut socket, json!({"type": "status", "event": ev})).await {
                        break "client gone";
                    }
                    sent.inc();
                    tracing::info!(event = "ws.notification_sent", %job_id, kind = "status", status = %status, "status change delivered");
                }
                Some(Err(e)) => {
                    let _ = send_json(&mut socket, json!({"type": "error", "error": {"code": e.code(), "message": e.public_message()}})).await;
                    break "error";
                }
                None => break "terminal state reached",
            },
            msg = socket.recv() => match msg {
                // Pings from the client are answered automatically by the
                // WebSocket implementation; text from the client is ignored.
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break "client closed",
                Some(Ok(_)) => {}
            },
            _ = ping.tick() => {
                if socket.send(Message::Ping(axum::body::Bytes::new())).await.is_err() {
                    break "client gone";
                }
            }
        }
    };
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: 1000,
            reason: close_reason.into(),
        })))
        .await;
    gauge.dec();
    tracing::info!(event = "ws.closed", %job_id, reason = close_reason, "websocket closed");
}
