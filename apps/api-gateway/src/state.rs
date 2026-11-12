use std::sync::Arc;

use engine_observability::MetricsRegistry;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::config::GatewayConfig;
use crate::service::GatewayService;

/// Shared by every request handler. Everything inside is either immutable or
/// internally synchronized, and cloning is cheap (`Arc`s / channel handles),
/// which is what axum's `State` extractor expects.
#[derive(Clone)]
pub struct AppState {
    pub service: GatewayService,
    pub config: Arc<GatewayConfig>,
    pub metrics: Arc<MetricsRegistry>,
    /// Bounds concurrent WebSocket connections (each holds a permit).
    pub ws_slots: Arc<Semaphore>,
    pub shutdown: CancellationToken,
}
