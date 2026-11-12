//! Gateway-side client for the internal WorkerEngine service.

use std::time::Duration;

use tonic::transport::{Channel, Endpoint};

use crate::pb::worker_engine_client::WorkerEngineClient;

#[derive(Debug, Clone)]
pub struct WorkerClientConfig {
    pub endpoint: String,
    pub connect_timeout: Duration,
}
// Per-RPC deadlines are set on each request (`Request::set_timeout`, which
// sends the standard `grpc-timeout` header) – see api-gateway `service.rs`.

/// `connect_lazy`: no connection is made until the first RPC, and tonic
/// reconnects automatically after the worker restarts. The gateway therefore
/// starts even when the worker is down (readiness reports it) instead of
/// crash-looping.
///
/// `Channel` is cheap to clone (it is a handle to a shared HTTP/2 connection
/// with its own internal buffer/worker), so every request clones the client.
pub fn connect_lazy(
    cfg: &WorkerClientConfig,
) -> Result<WorkerEngineClient<Channel>, tonic::transport::Error> {
    let channel = Endpoint::from_shared(cfg.endpoint.clone())?
        .connect_timeout(cfg.connect_timeout)
        .tcp_nodelay(true)
        .http2_keep_alive_interval(Duration::from_secs(20))
        .keep_alive_while_idle(true)
        .connect_lazy();
    Ok(WorkerEngineClient::new(channel).max_decoding_message_size(16 * 1024 * 1024))
}
