//! # api-gateway (PROCESS 1)
//!
//! Public edge of the system. One Tokio multi-thread runtime runs, side by side:
//! * axum REST + WebSocket server (`GATEWAY_HTTP_ADDR`, default 127.0.0.1:8080)
//! * tonic public gRPC server     (`GATEWAY_GRPC_ADDR`, default 127.0.0.1:50051)
//! * the status-stream bridge task (one gRPC stream from the worker)
//! * optional TimescaleDB log/metric writers
//!
//! It holds **no job state**: every job operation is an IPC call to the
//! worker-engine process (`WORKER_ENDPOINT`, default http://127.0.0.1:50052).
//! 



/*


gRPC (Tonic):

Client → Protobuf (binary)
    ↓
Tonic → Rust struct (SubmitJobRequest)
    ↓
grpc.rs (gRPC Handler)
    ↓
payload_json → serde_json::Value
    ↓
service.rs (GatewayService)
    ↓
Internal gRPC → Protobuf (binary)
    ↓
Worker Engine




REST (Axum):


Client → HTTP/JSON
    ↓
Axum + Serde → Rust struct (SubmitBody)

    ↓
rest.rs (REST Handler)
    ↓
service.rs (GatewayService)
    ↓
Internal gRPC → Protobuf (binary)
    ↓
Worker Engine




GatewayService her iki isteği de ortak mantıkla işler. JobCommand::parse ile doğrular, ardından WorkerEngineClient aracılığıyla internal gRPC isteği gönderir.

Yani Tonic gRPC için, Axum REST için kullanılıyor. İkisi de gelen verileri Rust tiplerine dönüştürüp aynı servis katmanına iletiyor.


İstek tipine göre uygun handlera otomatik mi gidiyor?

Evet, otomatik olarak uygun handler'a yönlendirilir. Bunu framework yapar:
REST (Axum): HTTP metodu ve URL'ye bakar. Örneğin POST /api/v1/jobs → rest.rs::submit_job
gRPC (Tonic): Protobuf'ta tanımlanan servis ve metot adına bakar. Örneğin JobGateway/SubmitJob → grpc.rs::submit_job
Sonrasında her iki handler da GatewayService::submit() metodunu çağırır.
Yani hangi handler'ın çalışacağını sen her istekte manuel belirlemezsin; routing tanımlarını önceden yaparsın, framework uygun handler'ı seçer.





*/

mod config;
mod grpc;
mod hub;
mod middleware;
mod rest;
mod service;
mod state;
mod websocket;

use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use engine_ipc::client::{WorkerClientConfig, connect_lazy};
use engine_ipc::pb::job_gateway_server::JobGatewayServer;
use engine_observability::propagation::server_span_with_client;
use engine_observability::sink::{SinkConfig, run_log_writer, run_metrics_writer};
use engine_observability::{MetricsRegistry, TelemetryConfig};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;

use crate::config::GatewayConfig;
use crate::service::GatewayService;
use crate::state::AppState;

const SERVICE: &str = "api-gateway";

fn main() -> ExitCode {
    let _ = dotenvy::dotenv();
    let cfg = match GatewayConfig::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("api-gateway: configuration error: {e}");
            return ExitCode::from(2);
        }
    };
    let mut telemetry =
        match engine_observability::telemetry::init(TelemetryConfig::from_env(SERVICE)) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("api-gateway: telemetry init failed: {e}");
                return ExitCode::from(2);
            }
        };
    let counter = AtomicUsize::new(0);
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cfg.tokio_worker_threads)
        .thread_name_fn(move || {
            format!("gateway-tokio-{}", counter.fetch_add(1, Ordering::Relaxed))
        })
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("api-gateway: cannot build Tokio runtime: {e}");
            return ExitCode::from(1);
        }
    };
    let log_rx = telemetry.take_log_receiver();
    let sink_stats = telemetry.sink_stats.clone();
    let otlp = telemetry.otlp_enabled;
    let code = runtime.block_on(async move {
        tracing::info!(
            event = "process.starting",
            tokio_worker_threads = cfg.tokio_worker_threads,
            worker_endpoint = %cfg.worker_endpoint,
            otlp_export = otlp,
            pid = std::process::id(),
            "api-gateway starting"
        );
        match run(cfg, log_rx, sink_stats).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                tracing::error!(event = "process.failed", error = %e, "api-gateway failed");
                ExitCode::from(1)
            }
        }
    });
    runtime.shutdown_timeout(Duration::from_secs(5));
    telemetry.shutdown();
    code
}

async fn run(
    cfg: GatewayConfig,
    log_rx: Option<tokio::sync::mpsc::Receiver<engine_observability::LogRecord>>,
    sink_stats: Arc<engine_observability::SinkStats>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let shutdown = CancellationToken::new();
    spawn_signal_handler(shutdown.clone());
    let metrics = MetricsRegistry::new();
    metrics
        .gauge("tokio_configured_worker_threads", &[])
        .set(cfg.tokio_worker_threads as i64);

    // Operational logs/metrics -> TimescaleDB (lazy pool: the gateway starts
    // even if the database is down; the writer drops + counts on failure).
    let sink_stop = CancellationToken::new();
    let mut sinks = Vec::new();
    if let Some(url) = &cfg.database_url {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_secs(2))
            .connect_lazy_with(
                url.parse::<sqlx::postgres::PgConnectOptions>()?
                    .application_name(SERVICE),
            );
        if let Some(rx) = log_rx {
            sinks.push(tokio::spawn(run_log_writer(
                pool.clone(),
                rx,
                SinkConfig::default(),
                sink_stats.clone(),
                sink_stop.clone(),
            )));
        }
        sinks.push(tokio::spawn(run_metrics_writer(
            pool,
            metrics.clone(),
            SERVICE.into(),
            cfg.metrics_flush,
            sink_stop.clone(),
        )));
    } else {
        tracing::warn!(
            event = "db_log.disabled",
            "DATABASE_URL not set: operational logs are not persisted"
        );
    }

    let client = connect_lazy(&WorkerClientConfig {
        endpoint: cfg.worker_endpoint.clone(),
        connect_timeout: cfg.worker_connect_timeout,
    })?;
    let hub = hub::start_hub(client.clone(), 4096, shutdown.clone());
    let service = GatewayService::new(
        client,
        cfg.limits.clone(),
        hub,
        cfg.worker_rpc_timeout,
        metrics.clone(),
    );
    let cfg = Arc::new(cfg);
    let state = AppState {
        service: service.clone(),
        config: cfg.clone(),
        metrics: metrics.clone(),
        ws_slots: Arc::new(Semaphore::new(cfg.max_ws_connections.max(1))),
        shutdown: shutdown.clone(),
    };

    // REST + WebSocket
    let app = middleware::apply(rest::router(state), &cfg, metrics.clone());
    let http_listener = tokio::net::TcpListener::bind(cfg.http_addr).await?;
    tracing::info!(event = "http.listening", addr = %cfg.http_addr, "REST + WebSocket listening");
    let http = tokio::spawn({
        let stop = shutdown.clone();
        async move {
            // `with_connect_info` exposes the caller's socket address to the
            // trace layer, so every log line shows which machine sent it.
            axum::serve(
                http_listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .with_graceful_shutdown(stop.cancelled_owned())
                .await
        }
    });

    // Public gRPC (+ reflection)
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(engine_ipc::pb::FILE_DESCRIPTOR_SET)
        .build_v1()?;
    let grpc_listener = tokio::net::TcpListener::bind(cfg.grpc_addr).await?;
    tracing::info!(event = "grpc.listening", addr = %cfg.grpc_addr, "public JobGateway gRPC listening");
    let public = grpc::PublicGrpc {
        service,
        shutdown: shutdown.clone(),
    };
    let grpc = tokio::spawn({
        let stop = shutdown.clone();
        let timeout = cfg.request_timeout;
        async move {
            tonic::transport::Server::builder()
                // Applies to unary handlers; streaming responses (WatchJob)
                // continue after the handler has returned the stream.
                .timeout(timeout)
                .concurrency_limit_per_connection(256)
                .layer(
                    TraceLayer::new_for_grpc()
                        .make_span_with(|req: &http::Request<_>| {
                            let peer = req
                                .extensions()
                                .get::<tonic::transport::server::TcpConnectInfo>()
                                .and_then(|ci| ci.remote_addr());
                            server_span_with_client(req, "grpc.public", peer)
                        }),
                )
                .add_service(JobGatewayServer::new(public))
                .add_service(reflection)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(grpc_listener),
                    stop.cancelled_owned(),
                )
                .await
        }
    });

    shutdown.cancelled().await;
    tracing::info!(
        event = "process.shutdown_requested",
        "shutdown requested; draining connections"
    );
    for (name, task) in [("http", http.map_err_box()), ("grpc", grpc.map_err_box())] {
        match tokio::time::timeout(Duration::from_secs(10), task).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::warn!(server = name, error = %e, "server stopped with error"),
            Err(_) => tracing::warn!(server = name, "server did not stop within 10s"),
        }
    }
    tracing::info!(
        event = "process.stopped",
        "api-gateway stopped; flushing logs"
    );
    sink_stop.cancel();
    for s in sinks {
        let _ = tokio::time::timeout(Duration::from_secs(5), s).await;
    }
    Ok(())
}

/// Small helper so both server JoinHandles have the same type.
trait MapErrBox {
    fn map_err_box(
        self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>;
}

impl<E: std::fmt::Display + Send + 'static> MapErrBox for tokio::task::JoinHandle<Result<(), E>> {
    fn map_err_box(
        self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>> {
        Box::pin(async move {
            match self.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(e.to_string()),
                Err(e) => Err(e.to_string()),
            }
        })
    }
}

fn spawn_signal_handler(token: CancellationToken) {
    engine_observability::shutdown_on_stdin_close(token.clone());
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        token.cancel();
    });
}
