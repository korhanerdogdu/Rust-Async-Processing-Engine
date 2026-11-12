//! Worker configuration from environment variables (see `.env.example`).
//! Invalid values are startup errors â€“ never silently defaulted.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use engine_application::{EngineConfig, KindTimeouts};
use engine_core::JobLimits;
use engine_handlers::{CpuConfig, HandlersConfig, PdfConfig};
use engine_persistence::PoolConfig;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ConfigError(String);

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

fn parse<T: FromStr>(name: &str, default: T) -> Result<T, ConfigError> {
    match var(name) {
        Some(v) => v
            .trim()
            .parse()
            .map_err(|_| ConfigError(format!("{name}: cannot parse '{v}'"))),
        None => Ok(default),
    }
}

fn millis(name: &str, default_ms: u64) -> Result<Duration, ConfigError> {
    parse(name, default_ms).map(Duration::from_millis)
}

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub database_url: String,
    pub grpc_addr: SocketAddr,
    pub tokio_worker_threads: usize,
    pub artifact_dir: PathBuf,
    pub migrate_on_start: bool,
    pub max_status_subscribers: usize,
    pub metrics_flush: Duration,
    pub pool: PoolConfig,
    pub engine: EngineConfig,
    pub handlers: HandlersConfig,
}

impl WorkerConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let logical = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let database_url =
            var("DATABASE_URL").ok_or_else(|| ConfigError("DATABASE_URL is required".into()))?;
        let limits = JobLimits::from_lookup(var).map_err(ConfigError)?;

        let max_active_jobs: usize = parse("MAX_ACTIVE_JOBS", 1)?;
        let queue_capacity: usize = parse("QUEUE_CAPACITY", 64)?;
        if max_active_jobs == 0 || queue_capacity == 0 {
            return Err(ConfigError(
                "MAX_ACTIVE_JOBS and QUEUE_CAPACITY must be >= 1".into(),
            ));
        }

        let pdf_timeout = millis("PDF_TIMEOUT_MS", 30_000)?;
        let timeouts = KindTimeouts {
            json_format: millis("JOB_TIMEOUT_JSON_MS", 5_000)?,
            text_transform: millis("JOB_TIMEOUT_TEXT_MS", 5_000)?,
            simulate_delay: millis(
                "JOB_TIMEOUT_DELAY_MS",
                limits.max_delay.as_millis() as u64 + 5_000,
            )?,
            // Outer backstop: renderer timeout + time to finalize the file.
            html_to_pdf: millis(
                "JOB_TIMEOUT_PDF_MS",
                pdf_timeout.as_millis() as u64 + 15_000,
            )?,
            cpu_compute: millis("JOB_TIMEOUT_CPU_MS", 120_000)?,
        };

        let engine = EngineConfig {
            worker_id: var("WORKER_ID").unwrap_or_else(|| "worker-1".into()),
            queue_capacity,
            max_active_jobs,
            enqueue_timeout: millis("ENQUEUE_TIMEOUT_MS", 0)?,
            max_attempts: parse("MAX_ATTEMPTS", 3)?,
            retry_base: millis("RETRY_BASE_MS", 1_000)?,
            retry_max: millis("RETRY_MAX_MS", 30_000)?,
            lease_grace: millis("LEASE_GRACE_MS", 15_000)?,
            reconcile_interval: millis("RECONCILE_INTERVAL_MS", 2_000)?,
            status_channel_capacity: parse("STATUS_CHANNEL_CAPACITY", 1024)?,
            shutdown_grace: millis("SHUTDOWN_GRACE_MS", 10_000)?,
            timeouts,
            limits,
        };
        if engine.worker_id.contains(':') {
            return Err(ConfigError("WORKER_ID must not contain ':'".into()));
        }

        let handlers = HandlersConfig {
            pdf: PdfConfig {
                browser: var("PDF_BROWSER_PATH").map(PathBuf::from),
                timeout: pdf_timeout,
                max_concurrency: parse("PDF_MAX_CONCURRENCY", 2)?,
                no_sandbox: parse("PDF_NO_SANDBOX", false)?,
                max_pdf_bytes: parse("PDF_MAX_BYTES", 20 * 1024 * 1024)?,
            },
            cpu: CpuConfig {
                threads: parse("RAYON_THREADS", logical)?,
                max_concurrent_jobs: parse("CPU_MAX_CONCURRENT_JOBS", 1)?,
            },
        };

        Ok(Self {
            database_url,
            grpc_addr: parse(
                "WORKER_GRPC_ADDR",
                SocketAddr::from(([127, 0, 0, 1], 50052)),
            )?,
            tokio_worker_threads: parse("TOKIO_WORKER_THREADS", logical)?.max(1),
            artifact_dir: var("ARTIFACT_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("data/artifacts")),
            migrate_on_start: parse("MIGRATE_ON_START", true)?,
            max_status_subscribers: parse("MAX_STATUS_SUBSCRIBERS", 16)?,
            metrics_flush: millis("METRICS_FLUSH_MS", 5_000)?,
            pool: PoolConfig {
                max_connections: parse("DB_MAX_CONNECTIONS", 20)?,
                ..PoolConfig::default()
            },
            engine,
            handlers,
        })
    }
}
