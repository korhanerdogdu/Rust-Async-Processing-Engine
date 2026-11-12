use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use engine_core::JobLimits;

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

fn parse<T: FromStr>(name: &str, default: T) -> Result<T, String> {
    match var(name) {
        Some(v) => v
            .trim()
            .parse()
            .map_err(|_| format!("{name}: cannot parse '{v}'")),
        None => Ok(default),
    }
}

#[derive(Debug, Clone)]
pub struct GatewayConfig {
    pub http_addr: SocketAddr,
    pub grpc_addr: SocketAddr,
    pub worker_endpoint: String,
    /// Optional: operational logs/metrics go to TimescaleDB when set.
    pub database_url: Option<String>,
    pub tokio_worker_threads: usize,
    /// Whole-request deadline for REST (tower-http TimeoutLayer) and public gRPC.
    pub request_timeout: Duration,
    /// Deadline for each unary gateway -> worker RPC.
    pub worker_rpc_timeout: Duration,
    pub worker_connect_timeout: Duration,
    /// Tower ConcurrencyLimit: requests beyond this are shed with 503.
    pub max_inflight_requests: usize,
    pub max_body_bytes: usize,
    pub max_ws_connections: usize,
    pub metrics_token: Option<String>,
    pub metrics_flush: Duration,
    pub limits: JobLimits,
}

impl GatewayConfig {
    pub fn from_env() -> Result<Self, String> {
        let logical = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Ok(Self {
            http_addr: parse(
                "GATEWAY_HTTP_ADDR",
                SocketAddr::from(([127, 0, 0, 1], 8080)),
            )?,
            grpc_addr: parse(
                "GATEWAY_GRPC_ADDR",
                SocketAddr::from(([127, 0, 0, 1], 50051)),
            )?,
            worker_endpoint: var("WORKER_ENDPOINT")
                .unwrap_or_else(|| "http://127.0.0.1:50052".into()),
            database_url: var("DATABASE_URL"),
            tokio_worker_threads: parse("TOKIO_WORKER_THREADS", logical)?.max(1),
            request_timeout: Duration::from_millis(parse("REQUEST_TIMEOUT_MS", 15_000)?),
            worker_rpc_timeout: Duration::from_millis(parse("WORKER_RPC_TIMEOUT_MS", 5_000)?),
            worker_connect_timeout: Duration::from_millis(parse(
                "WORKER_CONNECT_TIMEOUT_MS",
                1_000,
            )?),
            max_inflight_requests: parse("MAX_INFLIGHT_REQUESTS", 1024)?.max(1),
            max_body_bytes: parse("MAX_BODY_BYTES", 1024 * 1024)?,
            max_ws_connections: parse("MAX_WS_CONNECTIONS", 1000)?,
            metrics_token: var("METRICS_TOKEN"),
            metrics_flush: Duration::from_millis(parse("METRICS_FLUSH_MS", 5_000)?),
            limits: JobLimits::from_lookup(var)?,
        })
    }
}
