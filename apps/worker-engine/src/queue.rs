//! Wires the job queue for this process.
//!
//! The bounded MPSC channel, its single consumer and the dispatcher live in
//! `engine-application` (so they are unit-testable without gRPC/SQL). This
//! module is where the *process* decides how they are instantiated:
//! PostgreSQL store, real handlers, metrics, artifact cleanup.

use std::sync::Arc;

use engine_application::{ArtifactCleanup, EngineHandle, HandlerRegistry};
use engine_handlers::ArtifactStore;
use engine_observability::MetricsRegistry;
use engine_persistence::PgJobStore;
use sqlx::PgPool;

use crate::config::WorkerConfig;

pub fn start_engine(
    cfg: &WorkerConfig,
    pool: PgPool,
    registry: HandlerRegistry,
    artifacts: ArtifactStore,
    metrics: Arc<MetricsRegistry>,
) -> EngineHandle<PgJobStore> {
    let cleanup: ArtifactCleanup = Arc::new(move |art| artifacts.remove(&art.relative_path));
    EngineHandle::start(
        Arc::new(PgJobStore::new(pool)),
        registry,
        cfg.engine.clone(),
        metrics,
        Some(cleanup),
    )
}
