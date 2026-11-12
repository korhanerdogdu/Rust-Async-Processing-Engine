//! Cross-process status bridge.
//!
//! ```text
//!  worker process                         gateway process
//!  ─────────────                          ───────────────
//!  broadcast bus ─► SubscribeStatus ═gRPC stream═► bridge task ─► local broadcast ─► WS / WatchJob tasks
//!                   (ONE stream per gateway)        (reconnects)    (HubEvent)         (filter by job id)
//! ```
//!
//! * Exactly **one** upstream stream, regardless of how many WebSocket clients
//!   are connected – no per-client connection to the worker.
//! * The local `broadcast` is process-local and never crosses the boundary.
//! * Gaps are expected (lag, reconnect, worker restart). Instead of pretending
//!   to replay, watchers re-fetch a **snapshot** (worker -> PostgreSQL) on
//!   `Resync`/`Lagged`, and drop events whose `status_version` is not newer
//!   than what they already sent.

use std::time::Duration;

use engine_core::{EngineError, JobId, JobStatusEvent, JobSummary};
use engine_ipc::mapping::event_from_pb;
use engine_ipc::metadata::outbound;
use engine_ipc::pb;
use engine_ipc::pb::worker_engine_client::WorkerEngineClient;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

use crate::service::GatewayService;

#[derive(Debug, Clone)]
pub enum HubEvent {
    Status(JobStatusEvent),
    /// Events may have been missed: re-read the truth.
    Resync,
}

/// Connection state of the upstream status stream (watch: latest value only).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamState {
    Connecting,
    Connected,
    Disconnected,
}

#[derive(Clone)]
pub struct StatusHub {
    tx: broadcast::Sender<HubEvent>,
    upstream: watch::Receiver<UpstreamState>,
}

impl StatusHub {
    pub fn subscribe(&self) -> broadcast::Receiver<HubEvent> {
        self.tx.subscribe()
    }

    pub fn upstream_state(&self) -> UpstreamState {
        *self.upstream.borrow()
    }
}

/// Spawns the bridge task and returns the hub handle.
pub fn start_hub(
    client: WorkerEngineClient<Channel>,
    capacity: usize,
    shutdown: CancellationToken,
) -> StatusHub {
    let (tx, _) = broadcast::channel(capacity.max(16));
    let (state_tx, state_rx) = watch::channel(UpstreamState::Connecting);
    tokio::spawn(run_bridge(client, tx.clone(), state_tx, shutdown));
    StatusHub {
        tx,
        upstream: state_rx,
    }
}

async fn run_bridge(
    client: WorkerEngineClient<Channel>,
    tx: broadcast::Sender<HubEvent>,
    state: watch::Sender<UpstreamState>,
    shutdown: CancellationToken,
) {
    let mut backoff = Duration::from_millis(200);
    loop {
        if shutdown.is_cancelled() {
            return;
        }
        // `subscribe_status` borrows the client mutably for the lifetime of the
        // call future, so the clone must outlive the future (named binding).
        let mut c = client.clone();
        let call = c.subscribe_status(outbound(pb::SubscribeStatusRequest {}, None));
        let result = tokio::select! {
            _ = shutdown.cancelled() => return,
            r = call => r,
        };
        match result {
            Ok(resp) => {
                let mut stream = resp.into_inner();
                state.send_replace(UpstreamState::Connected);
                backoff = Duration::from_millis(200);
                tracing::info!(
                    event = "hub.upstream_connected",
                    "status stream from worker connected"
                );
                // We may have missed transitions while disconnected.
                let _ = tx.send(HubEvent::Resync);
                loop {
                    let msg = tokio::select! {
                        _ = shutdown.cancelled() => return,
                        m = stream.message() => m,
                    };
                    match msg {
                        Ok(Some(pb::StatusStreamMessage {
                            kind: Some(pb::status_stream_message::Kind::Event(ev)),
                        })) => {
                            if let Ok(ev) = event_from_pb(&ev) {
                                let _ = tx.send(HubEvent::Status(ev));
                            }
                        }
                        Ok(Some(pb::StatusStreamMessage {
                            kind: Some(pb::status_stream_message::Kind::Resync(r)),
                        })) => {
                            tracing::warn!(
                                event = "hub.upstream_lagged",
                                missed = r.missed,
                                "worker reported lag; resync"
                            );
                            let _ = tx.send(HubEvent::Resync);
                        }
                        Ok(Some(_)) => {}
                        Ok(None) => {
                            tracing::warn!(event = "hub.upstream_closed", "status stream ended");
                            break;
                        }
                        Err(e) => {
                            tracing::warn!(event = "hub.upstream_error", error = %e.message(), "status stream error");
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                tracing::debug!(event = "hub.upstream_connect_failed", error = %e.message(), "worker status stream unavailable");
            }
        }
        state.send_replace(UpstreamState::Disconnected);
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(Duration::from_secs(5));
    }
}

/// What a watcher (WebSocket client or gRPC WatchJob) receives.
#[derive(Debug, Clone)]
pub enum WatchItem {
    Snapshot(JobSummary),
    Event(JobStatusEvent),
}

/// Starts a watcher task for one job: snapshot first, then newer transitions,
/// until a terminal state, the client leaves (receiver dropped) or shutdown.
/// The returned channel is bounded (16): a slow client slows only itself.
pub fn watch_job(
    service: GatewayService,
    job_id: JobId,
    cancel: CancellationToken,
) -> mpsc::Receiver<Result<WatchItem, EngineError>> {
    let (out, rx) = mpsc::channel(16);
    // Subscribe BEFORE taking the snapshot so no transition can fall between.
    let mut events = service.hub.subscribe();
    tokio::spawn(async move {
        let mut last_version = match service.get(job_id, None).await {
            Ok(snapshot) => {
                let v = snapshot.status_version;
                let terminal = snapshot.status.is_terminal();
                if out.send(Ok(WatchItem::Snapshot(snapshot))).await.is_err() || terminal {
                    return;
                }
                v
            }
            Err(e) => {
                let _ = out.send(Err(e)).await;
                return;
            }
        };
        loop {
            let next = tokio::select! {
                _ = cancel.cancelled() => return,
                _ = out.closed() => return,
                r = events.recv() => r,
            };
            let resync = match next {
                Ok(HubEvent::Status(ev)) if ev.job_id == job_id => {
                    if ev.status_version <= last_version {
                        continue; // duplicate or older than our snapshot
                    }
                    last_version = ev.status_version;
                    let terminal = ev.status.is_terminal();
                    if out.send(Ok(WatchItem::Event(ev))).await.is_err() || terminal {
                        return;
                    }
                    false
                }
                Ok(HubEvent::Status(_)) => false,
                Ok(HubEvent::Resync) => true,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(event = "ws.watcher_lagged", missed = n, %job_id, "watcher lagged; refreshing snapshot");
                    true
                }
                Err(broadcast::error::RecvError::Closed) => return,
            };
            if resync {
                match service.get(job_id, None).await {
                    Ok(snapshot) if snapshot.status_version > last_version => {
                        last_version = snapshot.status_version;
                        let terminal = snapshot.status.is_terminal();
                        if out.send(Ok(WatchItem::Snapshot(snapshot))).await.is_err() || terminal {
                            return;
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::debug!(error = %e, "snapshot refresh failed; waiting for next event")
                    }
                }
            }
        }
    });
    rx
}
