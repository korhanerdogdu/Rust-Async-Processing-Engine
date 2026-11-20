//! # worker-engine (PROCESS 2)
//!
//! ```text
//!  gateway ──gRPC/TCP──► tonic server ──► EngineHandle::submit (producer tasks)
//!                                              │ bounded mpsc
//!                                              ▼
//!                                  single consumer task ──► job tasks ──► handlers
//!                                              │                            (async / sync / process / Rayon)
//!                                              ▼
//!                                  PostgreSQL (source of truth) ──► broadcast ──► SubscribeStatus stream
//! ```
//!
//! Startup order:
//! 1. telemetry (before the runtime: the OTLP HTTP exporter owns a blocking client)
//! 2. a *configured* multi-thread Tokio runtime (`TOKIO_WORKER_THREADS`)
//! 3. gRPC server (answers UNAVAILABLE until the engine is ready)
//! 4. database + migrations (retried), log/metric writers, engine start
//!
//! Shutdown (Ctrl-C / SIGTERM): engine drains (bounded), gRPC server stops,
//! log writer drains (bounded), spans are flushed.





/*




Runtime hazır olunca run() çalışıyor ve sırayla şunlar oluyor:

1. recovery::cleanup_artifacts : Bir önceki çökmeden yarım kalmış PDF dosyaları varsa siliniyor.

2. default_handlers(...) ve HandlerRegistry::new : Beş iş tipinin handler'ı kaydediliyor.

   Handler = bir iş tipini yapan kod. Mesela simulate_delay işini DelayHandler yapar.
   Projede 5 iş tipi var, her birinin kendi handler'ı var.

   Bunu bir restoran gibi düşün:
     - Her yemeğin bir aşçısı var. "Pizza gelirse Ali yapar, çorba gelirse Ayşe yapar."
     - Restoran açılırken bu liste duvara asılır.
     - Sipariş gelince listeye bakılır ve doğru aşçıya verilir.

   Burada da aynısı oluyor:
     - default_handlers(...) -> 5 aşçıyı (handler'ı) hazırlar.
     - HandlerRegistry::new(...) -> "hangi iş tipi -> hangi handler" listesini yapar:
           simulate_delay -> DelayHandler
           html_to_pdf    -> HtmlToPdfHandler
           cpu_compute    -> CpuComputeHandler
           json_format    -> JsonFormatHandler
           text_transform -> TextTransformHandler

   Sonra bir iş çalışacağı zaman listeye bakılır, doğru handler bulunur ve çalıştırılır.
   (Bu bakma işi crates/application/src/registry.rs içindeki execute fonksiyonunda.)

   "Kaydetmek" = bu listeyi worker açılırken bir kere hazırlamak.


let handlers = default_handlers(...)?;          // 5 handler'ı hazırla
let registry = HandlerRegistry::new(handlers);  // sözlüğü doldur
...
queue::start_engine(&cfg, pool, registry, ...); // motora teslim et

main execute'u hiç çağırmıyor. İş geldiğinde execute'u dispatcher çağırıyor.

Restoran benzetmesiyle söylersek: HandlerRegistry duvardaki "hangi yemeği hangi aşçı yapar" listesi. 
main o listeyi açılışta yazıp asıyor. Sipariş geldiğinde de dispatcher listeye bakıp siparişi doğru aşçıya veriyor.


3. gRPC sunucusu açılıyor, ama motor henüz yok. Bu yüzden bu aşamada gelen her istek
"UNAVAILABLE" alıyor.


4. recovery::connect_database : Veritabanı gelene kadar tekrar tekrar deneniyor, bağlanınca migration'lar
çalışıyor.


5. run_log_writer ve run_metrics_writer görevleri başlıyor.


6. queue::start_engine → EngineHandle::start . Asıl önemli an bu



main.rs              default_handlers(...)                       // 5 handler hazırla
  ↓
main.rs              HandlerRegistry::new(handlers)              // tablo kuruldu (bir kez)
  ↓
main.rs              queue::start_engine(..., registry, ...)     // motora teslim
  ↓
worker queue.rs      EngineHandle::start(..., registry, ...)
  ↓
engine.rs            Shared { registry, ... }                    // tablo BURADA yaşıyor (Arc içinde)
                     tokio::spawn(run_consumer(shared, ...))     // ayrı bir görev başlar
  ↓  (main burada bitti, artık beklemede)
dispatcher.rs        run_consumer döngüsü: iş geldikçe ↓
                       running.spawn(execute_job(...))          // her iş için yeni görev
                         → execute_job_inner
                           → shared.registry.execute(ctx, cmd)  // 205. satır
  ↓
registry.rs          self.handlers.get(&command.kind())         // tablodan handler'ı bul
                       → handler.execute(ctx, command)
  ↓
delay.rs             DelayHandler::execute                      // asıl iş
                       → tokio::time::sleep(5 sn)



*/
mod config;
mod grpc_server;
mod queue;
mod recovery;

use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use engine_application::HandlerRegistry; // registry.rs den implement ediliyor. lib crate. 
use engine_handlers::{ArtifactStore, default_handlers};
use engine_ipc::pb::worker_engine_server::WorkerEngineServer;
use engine_observability::propagation::server_span;
use engine_observability::sink::{SinkConfig, run_log_writer, run_metrics_writer};
use engine_observability::{MetricsRegistry, TelemetryConfig};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;

use crate::config::WorkerConfig;
use crate::grpc_server::WorkerGrpc;

const SERVICE: &str = "worker-engine";

fn main() -> ExitCode {
    let _ = dotenvy::dotenv();
    let cfg = match WorkerConfig::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("worker-engine: configuration error: {e}");
            return ExitCode::from(2);
        }
    };
    let mut telemetry =
        match engine_observability::telemetry::init(TelemetryConfig::from_env(SERVICE)) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("worker-engine: telemetry init failed: {e}");
                return ExitCode::from(2);
            }
        };

    // An explicitly configured runtime instead of #[tokio::main], so the
    // worker thread count and names are visible and tunable.
    let counter = AtomicUsize::new(0);
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cfg.tokio_worker_threads)
        .thread_name_fn(move || format!("worker-tokio-{}", counter.fetch_add(1, Ordering::Relaxed)))
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("worker-engine: cannot build Tokio runtime: {e}");
            return ExitCode::from(1);
        }
    };


    /*
    
    cfg.tokio_worker_threads değeri TOKIO_WORKER_THREADS ortam değişkeninden okunuyor.

    Thread sayısını biz belirliyoruz
    
     */

    let log_rx = telemetry.take_log_receiver();
    let sink_stats = telemetry.sink_stats.clone();
    let otlp = telemetry.otlp_enabled;
    let code = runtime.block_on(async move {
        tracing::info!(
            event = "process.starting",
            tokio_worker_threads = cfg.tokio_worker_threads,
            rayon_threads = cfg.handlers.cpu.threads,
            max_active_jobs = cfg.engine.max_active_jobs,
            queue_capacity = cfg.engine.queue_capacity,
            otlp_export = otlp,
            pid = std::process::id(),
            "worker-engine starting"
        );
        match run(cfg, log_rx, sink_stats).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                tracing::error!(event = "process.failed", error = %e, "worker-engine failed");
                ExitCode::from(1)
            }
        }
    });
    runtime.shutdown_timeout(Duration::from_secs(5));
    telemetry.shutdown();
    code
}

async fn run(
    cfg: WorkerConfig,
    log_rx: Option<tokio::sync::mpsc::Receiver<engine_observability::LogRecord>>,
    sink_stats: Arc<engine_observability::SinkStats>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let shutdown = CancellationToken::new();
    spawn_signal_handler(shutdown.clone());

    let metrics = MetricsRegistry::new();
    let artifacts = ArtifactStore::new(&cfg.artifact_dir)?;
    recovery::cleanup_artifacts(&artifacts);
    let handlers = default_handlers(&cfg.handlers, artifacts.clone())?;
    let registry = HandlerRegistry::new(handlers); 


    // **********************************************************************************************
    // **********************************************************************************************
    // **********************************************************************************************
    // **********************************************************************************************

    tracing::info!(event = "handlers.registered", kinds = ?registry.kinds(), "job handlers registered");
    metrics
        .gauge("rayon_pool_threads", &[])
        .set(cfg.handlers.cpu.threads as i64);
    metrics
        .gauge("tokio_configured_worker_threads", &[])
        .set(cfg.tokio_worker_threads as i64);

    // 1. gRPC server first (reports "starting" until the engine exists).
    let engine_slot = Arc::new(OnceLock::new());
    let server_stop = CancellationToken::new();
    let svc = WorkerGrpc {
        engine: engine_slot.clone(),
        artifacts: artifacts.clone(),
        metrics: metrics.clone(),
        sink_stats: sink_stats.clone(),
        rayon_threads: cfg.handlers.cpu.threads,
        status_streams: Arc::new(Semaphore::new(cfg.max_status_subscribers.max(1))),
        server_stop: server_stop.clone(),
    };
    let listener = tokio::net::TcpListener::bind(cfg.grpc_addr).await?;
    tracing::info!(event = "grpc.listening", addr = %cfg.grpc_addr, "internal WorkerEngine gRPC listening");
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let server = tokio::spawn({
        let stop = server_stop.clone();
        async move {
            tonic::transport::Server::builder()
                .layer(
                    TraceLayer::new_for_grpc()
                        .make_span_with(|req: &http::Request<_>| server_span(req, "grpc.internal")),
                )
                .add_service(
                    WorkerEngineServer::new(svc).max_decoding_message_size(4 * 1024 * 1024),
                )
                .serve_with_incoming_shutdown(incoming, stop.cancelled_owned())
                .await
        }
    });

    // 2. Database (retried while down), then sinks and the engine.
    let Some(pool) = recovery::connect_database(&cfg, &shutdown).await else {
        server_stop.cancel();
        let _ = server.await;
        return Ok(());
    };
    let sink_stop = CancellationToken::new();
    let mut sinks = Vec::new();
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
        pool.clone(),
        metrics.clone(),
        SERVICE.into(),
        cfg.metrics_flush,
        sink_stop.clone(),
    )));

    let engine = queue::start_engine(&cfg, pool.clone(), registry, artifacts, metrics.clone());
    let _ = engine_slot.set(engine.clone());

    // Periodic runtime facts (measured, not configured).
    let sampler = tokio::spawn({
        let metrics = metrics.clone();
        let stats = sink_stats.clone();
        let stop = sink_stop.clone();
        async move {
            let mut tick = tokio::time::interval(Duration::from_secs(2));
            loop {
                tokio::select! { _ = stop.cancelled() => break, _ = tick.tick() => {} }
                engine_observability::record_runtime_metrics(&metrics, SERVICE);
                metrics
                    .gauge("log_sink_dropped_total", &[])
                    .set(stats.dropped() as i64);
                metrics
                    .gauge("log_sink_written_total", &[])
                    .set(stats.written() as i64);
            }
        }
    });

    // 3. Run until a shutdown signal.
    shutdown.cancelled().await;
    tracing::info!(event = "process.shutdown_requested", "shutdown requested");
    engine.shutdown().await;
    server_stop.cancel();
    match tokio::time::timeout(Duration::from_secs(5), server).await {
        Ok(Ok(Err(e))) => tracing::warn!(error = %e, "gRPC server error during shutdown"),
        Err(_) => tracing::warn!("gRPC server did not stop within 5s"),
        _ => {}
    }
    tracing::info!(
        event = "process.stopped",
        "worker-engine stopped; flushing logs"
    );
    sink_stop.cancel();
    let _ = sampler.await;
    for s in sinks {
        let _ = tokio::time::timeout(Duration::from_secs(5), s).await;
    }
    pool.close().await;
    Ok(())
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
