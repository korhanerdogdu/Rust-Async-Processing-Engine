//! Startup recovery: get a database connection (retrying while it is down),
//! apply migrations, and clean up interrupted artifact writes.
//!
//! Job-level recovery (stale leases, queued rows not in memory) is done by the
//! reconciler inside `engine-application`, which runs immediately when the
//! engine starts and then periodically.

use std::time::Duration;

use engine_handlers::ArtifactStore;
use engine_persistence::{MIGRATOR, connect_pool};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::config::WorkerConfig;

/// Connects with capped exponential backoff until success or shutdown.
/// Meanwhile the gRPC server is already up and answers UNAVAILABLE, so the
/// gateway reports "not ready" instead of the worker crash-looping.
pub async fn connect_database(cfg: &WorkerConfig, shutdown: &CancellationToken) -> Option<PgPool> {
    let mut backoff = Duration::from_millis(250);
    loop {
        let attempt = async {
            let pool = connect_pool(&cfg.database_url, &cfg.pool, "worker-engine").await?;
            if cfg.migrate_on_start {
                MIGRATOR.run(&pool).await?;
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(pool)
        };
        match attempt.await {
            Ok(pool) => {
                tracing::info!(
                    event = "db.connected",
                    migrated = cfg.migrate_on_start,
                    "database connected"
                );
                return Some(pool);
            }
            Err(e) => {
                tracing::warn!(event = "db.connect_failed", error = %e, retry_in_ms = backoff.as_millis() as u64, "database not available yet");
            }
        }
        tokio::select! {
            _ = shutdown.cancelled() => return None,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(Duration::from_secs(5));
    }
}

/// Removes `.part` files and render workdirs left by a crash (blocking fs
/// work, done once before serving).
pub fn cleanup_artifacts(store: &ArtifactStore) {
    match store.cleanup_tmp() {
        Ok(0) => {}
        Ok(n) => tracing::info!(
            event = "artifacts.tmp_cleaned",
            removed = n,
            "removed interrupted artifact writes"
        ),
        Err(e) => {
            tracing::warn!(event = "artifacts.tmp_cleanup_failed", error = %e, "could not clean artifact tmp dir")
        }
    }
}
