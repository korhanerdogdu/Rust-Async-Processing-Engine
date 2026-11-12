//! # engine-persistence
//!
//! PostgreSQL implementation of [`engine_core::JobStore`] using SQLx's async
//! pool.
//!
//! * Every transition is **one conditional `UPDATE ... WHERE status = <expected>
//!   AND lease_owner = $owner AND attempts = $attempt RETURNING ...`**. Under
//!   PostgreSQL's row locking, two concurrent claims of the same row cannot
//!   both succeed: the second re-evaluates the `WHERE` after the first commits
//!   and matches zero rows. That makes the claim atomic and idempotent.
//! * The `jobs_before_update` trigger rejects illegal transitions and bumps
//!   `status_version`; `jobs_record_history` writes the audit row in the same
//!   statement.
//! * Queries are runtime-checked (`sqlx::query_as` + binds) so the workspace
//!   builds without a live database; correctness is covered by the e2e tests.

use std::time::Duration;

use chrono::{DateTime, Utc};
use engine_core::store::StoreResult;
use engine_core::{
    ArtifactRef, CancelOutcome, InsertOutcome, JobFailure, JobId, JobKind, JobOutput, JobRecord,
    JobStatus, JobStore, LeaseInfo, ListFilter, NewJob, StatusHistoryEntry, StoreError,
};
use serde_json::Value;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

/// Embedded migrations (`/migrations`), applied in filename order.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout: Duration,
    pub idle_timeout: Duration,
    pub statement_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 10,
            min_connections: 1,
            acquire_timeout: Duration::from_secs(3),
            idle_timeout: Duration::from_secs(300),
            statement_timeout: Duration::from_secs(10),
        }
    }
}

fn options(
    url: &str,
    cfg: &PoolConfig,
    application_name: &str,
) -> Result<(PgPoolOptions, PgConnectOptions), sqlx::Error> {
    let opts: PgConnectOptions = url
        .parse::<PgConnectOptions>()?
        .application_name(application_name)
        .options([
            (
                "statement_timeout",
                format!("{}", cfg.statement_timeout.as_millis()),
            ),
            ("timezone", "UTC".to_string()),
        ]);
    let pool_opts = PgPoolOptions::new()
        .max_connections(cfg.max_connections)
        .acquire_timeout(cfg.acquire_timeout)
        .idle_timeout(Some(cfg.idle_timeout));
    Ok((pool_opts, opts))
}

/// Connects now (fails fast if the database is unreachable). Used by the
/// worker, which retries this in a loop at startup.
pub async fn connect_pool(
    url: &str,
    cfg: &PoolConfig,
    application_name: &str,
) -> Result<PgPool, sqlx::Error> {
    let (pool_opts, opts) = options(url, cfg, application_name)?;
    pool_opts
        .min_connections(cfg.min_connections)
        .connect_with(opts)
        .await
}

/// Maps SQLx errors onto the store's typed errors: anything that smells like
/// "the database is not reachable right now" becomes `Unavailable` (-> 503,
/// retryable by the client); the rest is `Internal`.
pub fn map_err(e: sqlx::Error) -> StoreError {
    match &e {
        sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::Io(_)
        | sqlx::Error::Tls(_) => StoreError::Unavailable(e.to_string()),
        sqlx::Error::Database(db) => {
            let code = db.code().map(|c| c.to_string()).unwrap_or_default();
            // 08xxx connection exceptions, 57P0x admin shutdown / crash shutdown, 53300 too many connections
            if code.starts_with("08") || code.starts_with("57P") || code == "53300" {
                StoreError::Unavailable(db.message().to_string())
            } else {
                StoreError::Internal(format!("{code}: {}", db.message()))
            }
        }
        _ => StoreError::Internal(e.to_string()),
    }
}

const JOB_COLUMNS: &str = "id, job_type, payload, status, status_version, attempts, max_attempts, created_at, \
    queued_at, started_at, finished_at, next_attempt_at, lease_owner, lease_expires_at, error_code, error_message, \
    result, artifact_path, artifact_content_type, artifact_size_bytes, artifact_sha256, idempotency_key, request_id, \
    trace_id, traceparent, updated_at";

#[derive(Debug, FromRow)]
struct JobRow {
    id: Uuid,
    job_type: String,
    payload: Value,
    status: String,
    status_version: i64,
    attempts: i32,
    max_attempts: i32,
    created_at: DateTime<Utc>,
    queued_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    next_attempt_at: Option<DateTime<Utc>>,
    lease_owner: Option<String>,
    lease_expires_at: Option<DateTime<Utc>>,
    error_code: Option<String>,
    error_message: Option<String>,
    result: Option<Value>,
    artifact_path: Option<String>,
    artifact_content_type: Option<String>,
    artifact_size_bytes: Option<i64>,
    artifact_sha256: Option<String>,
    idempotency_key: Option<String>,
    request_id: Option<String>,
    trace_id: Option<String>,
    traceparent: Option<String>,
    updated_at: DateTime<Utc>,
}

impl TryFrom<JobRow> for JobRecord {
    type Error = StoreError;

    fn try_from(r: JobRow) -> Result<Self, Self::Error> {
        let kind: JobKind = r
            .job_type
            .parse()
            .map_err(|e| StoreError::Internal(format!("corrupt job_type: {e}")))?;
        let status: JobStatus = r
            .status
            .parse()
            .map_err(|e| StoreError::Internal(format!("corrupt status: {e}")))?;
        let artifact = match (
            r.artifact_path,
            r.artifact_content_type,
            r.artifact_size_bytes,
            r.artifact_sha256,
        ) {
            (Some(relative_path), Some(content_type), Some(size), Some(sha256)) => {
                Some(ArtifactRef {
                    relative_path,
                    content_type,
                    size_bytes: size.max(0) as u64,
                    sha256,
                })
            }
            _ => None,
        };
        Ok(JobRecord {
            id: JobId::from_uuid(r.id),
            kind,
            payload: r.payload,
            status,
            status_version: r.status_version,
            attempts: r.attempts.max(0) as u32,
            max_attempts: r.max_attempts.max(1) as u32,
            created_at: r.created_at,
            queued_at: r.queued_at,
            started_at: r.started_at,
            finished_at: r.finished_at,
            next_attempt_at: r.next_attempt_at,
            lease_owner: r.lease_owner,
            lease_expires_at: r.lease_expires_at,
            error_code: r.error_code,
            error_message: r.error_message,
            result: r.result,
            artifact,
            idempotency_key: r.idempotency_key,
            request_id: r.request_id,
            trace_id: r.trace_id,
            traceparent: r.traceparent,
            updated_at: r.updated_at,
        })
    }
}

fn convert(row: Option<JobRow>) -> StoreResult<Option<JobRecord>> {
    row.map(JobRecord::try_from).transpose()
}

#[derive(Debug, FromRow)]
struct HistoryRow {
    from_status: Option<String>,
    to_status: String,
    status_version: i64,
    attempt: i32,
    reason: Option<String>,
    actor: Option<String>,
    error_code: Option<String>,
    at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct PgJobStore {
    pool: PgPool,
}

impl PgJobStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn fetch_one_opt(&self, sql: &str, id: Uuid) -> StoreResult<Option<JobRecord>> {
        let row: Option<JobRow> = sqlx::query_as(sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_err)?;
        convert(row)
    }
}

impl JobStore for PgJobStore {
    async fn insert_job(&self, job: NewJob) -> StoreResult<InsertOutcome> {
        let mut tx = self.pool.begin().await.map_err(map_err)?;
        let sql = format!(
            "INSERT INTO jobs (id, job_type, payload, status, max_attempts, idempotency_key, request_id, trace_id, \
             traceparent, transition_reason) \
             VALUES ($1, $2, $3, 'queued', $4, $5, $6, $7, $8, 'submitted') \
             ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING \
             RETURNING {JOB_COLUMNS}"
        );
        let inserted: Option<JobRow> = sqlx::query_as(&sql)
            .bind(job.id.as_uuid())
            .bind(job.kind.as_str())
            .bind(&job.payload)
            .bind(job.max_attempts as i32)
            .bind(&job.idempotency_key)
            .bind(&job.request_id)
            .bind(&job.trace_id)
            .bind(&job.traceparent)
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_err)?;

        let outcome = match convert(inserted)? {
            Some(record) => InsertOutcome::Created(record),
            None => {
                // Idempotency key already used: same request => same job.
                let sql = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE idempotency_key = $1");
                let existing: JobRow = sqlx::query_as(&sql)
                    .bind(&job.idempotency_key)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(map_err)?;
                let existing = JobRecord::try_from(existing)?;
                if existing.kind != job.kind || existing.payload != job.payload {
                    return Err(StoreError::Conflict(
                        "idempotency key reused with a different request".into(),
                    ));
                }
                InsertOutcome::Existing(existing)
            }
        };
        // 202 Accepted is only returned after this COMMIT succeeds.
        tx.commit().await.map_err(map_err)?;
        Ok(outcome)
    }

    async fn get_job(&self, id: JobId) -> StoreResult<Option<JobRecord>> {
        self.fetch_one_opt(
            &format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id = $1"),
            *id.as_uuid(),
        )
        .await
    }

    async fn list_jobs(&self, filter: ListFilter) -> StoreResult<Vec<JobRecord>> {
        let sql = format!(
            "SELECT {JOB_COLUMNS} FROM jobs WHERE ($1::text IS NULL OR status = $1) ORDER BY created_at DESC LIMIT $2"
        );
        let rows: Vec<JobRow> = sqlx::query_as(&sql)
            .bind(filter.status.map(|s| s.as_str()))
            .bind(filter.limit.clamp(1, 1000) as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(map_err)?;
        rows.into_iter().map(JobRecord::try_from).collect()
    }

    async fn claim_job(&self, id: JobId, lease: &LeaseInfo) -> StoreResult<Option<JobRecord>> {
        let sql = format!(
            "UPDATE jobs SET status = 'processing', attempts = attempts + 1, started_at = now(), finished_at = NULL, \
             lease_owner = $2, lease_expires_at = now() + make_interval(secs => $3), next_attempt_at = NULL, \
             error_code = NULL, error_message = NULL, transition_reason = 'claimed' \
             WHERE id = $1 AND status = 'queued' AND attempts < max_attempts \
               AND (next_attempt_at IS NULL OR next_attempt_at <= now()) \
             RETURNING {JOB_COLUMNS}"
        );
        let row: Option<JobRow> = sqlx::query_as(&sql)
            .bind(id.as_uuid())
            .bind(&lease.owner)
            .bind(lease.duration.as_secs_f64())
            .fetch_optional(&self.pool)
            .await
            .map_err(map_err)?;
        convert(row)
    }

    async fn complete_job(
        &self,
        id: JobId,
        owner: &str,
        attempt: u32,
        output: &JobOutput,
    ) -> StoreResult<Option<JobRecord>> {
        let art = output.artifact.as_ref();
        let sql = format!(
            "UPDATE jobs SET status = 'completed', finished_at = now(), result = $4, artifact_path = $5, \
             artifact_content_type = $6, artifact_size_bytes = $7, artifact_sha256 = $8, lease_expires_at = NULL, \
             transition_reason = 'completed' \
             WHERE id = $1 AND status = 'processing' AND lease_owner = $2 AND attempts = $3 \
             RETURNING {JOB_COLUMNS}"
        );
        let row: Option<JobRow> = sqlx::query_as(&sql)
            .bind(id.as_uuid())
            .bind(owner)
            .bind(attempt as i32)
            .bind(&output.result)
            .bind(art.map(|a| a.relative_path.as_str()))
            .bind(art.map(|a| a.content_type.as_str()))
            .bind(art.map(|a| a.size_bytes as i64))
            .bind(art.map(|a| a.sha256.as_str()))
            .fetch_optional(&self.pool)
            .await
            .map_err(map_err)?;
        convert(row)
    }

    async fn fail_job(
        &self,
        id: JobId,
        owner: &str,
        attempt: u32,
        failure: &JobFailure,
    ) -> StoreResult<Option<JobRecord>> {
        let sql = format!(
            "UPDATE jobs SET \
               status = CASE WHEN $6::timestamptz IS NULL THEN 'failed' ELSE 'queued' END, \
               finished_at = CASE WHEN $6::timestamptz IS NULL THEN now() ELSE NULL END, \
               queued_at = COALESCE($6::timestamptz, queued_at), \
               next_attempt_at = $6::timestamptz, \
               error_code = $4, error_message = $5, lease_expires_at = NULL, \
               transition_reason = CASE WHEN $6::timestamptz IS NULL THEN 'failed' ELSE 'retry_scheduled' END \
             WHERE id = $1 AND status = 'processing' AND lease_owner = $2 AND attempts = $3 \
             RETURNING {JOB_COLUMNS}"
        );
        let row: Option<JobRow> = sqlx::query_as(&sql)
            .bind(id.as_uuid())
            .bind(owner)
            .bind(attempt as i32)
            .bind(&failure.code)
            .bind(&failure.message)
            .bind(failure.retry_at)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_err)?;
        convert(row)
    }

    async fn release_job(
        &self,
        id: JobId,
        owner: &str,
        attempt: u32,
    ) -> StoreResult<Option<JobRecord>> {
        let sql = format!(
            "UPDATE jobs SET status = 'queued', attempts = GREATEST(attempts - 1, 0), queued_at = now(), \
             next_attempt_at = NULL, lease_expires_at = NULL, transition_reason = 'released_on_shutdown' \
             WHERE id = $1 AND status = 'processing' AND lease_owner = $2 AND attempts = $3 \
             RETURNING {JOB_COLUMNS}"
        );
        let row: Option<JobRow> = sqlx::query_as(&sql)
            .bind(id.as_uuid())
            .bind(owner)
            .bind(attempt as i32)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_err)?;
        convert(row)
    }

    async fn cancel_job(&self, id: JobId) -> StoreResult<CancelOutcome> {
        // Lock the row first so `previous` is exactly the state we cancelled.
        let mut tx = self.pool.begin().await.map_err(map_err)?;
        let current: Option<(String,)> =
            sqlx::query_as("SELECT status FROM jobs WHERE id = $1 FOR UPDATE")
                .bind(id.as_uuid())
                .fetch_optional(&mut *tx)
                .await
                .map_err(map_err)?;
        let Some((status,)) = current else {
            return Ok(CancelOutcome::NotFound);
        };
        let previous: JobStatus = status
            .parse()
            .map_err(|e| StoreError::Internal(format!("{e}")))?;
        if previous.is_terminal() {
            let sql = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id = $1");
            let row: JobRow = sqlx::query_as(&sql)
                .bind(id.as_uuid())
                .fetch_one(&mut *tx)
                .await
                .map_err(map_err)?;
            tx.commit().await.map_err(map_err)?;
            return Ok(CancelOutcome::AlreadyTerminal(row.try_into()?));
        }
        let sql = format!(
            "UPDATE jobs SET status = 'cancelled', finished_at = now(), lease_expires_at = NULL, \
             next_attempt_at = NULL, transition_reason = 'cancelled_by_client' \
             WHERE id = $1 RETURNING {JOB_COLUMNS}"
        );
        let row: JobRow = sqlx::query_as(&sql)
            .bind(id.as_uuid())
            .fetch_one(&mut *tx)
            .await
            .map_err(map_err)?;
        tx.commit().await.map_err(map_err)?;
        Ok(CancelOutcome::Cancelled {
            previous,
            record: row.try_into()?,
        })
    }

    async fn dispatchable_jobs(&self, limit: u32) -> StoreResult<Vec<JobRecord>> {
        let sql = format!(
            "SELECT {JOB_COLUMNS} FROM jobs WHERE status = 'queued' \
             AND (next_attempt_at IS NULL OR next_attempt_at <= now()) \
             ORDER BY queued_at, created_at LIMIT $1"
        );
        let rows: Vec<JobRow> = sqlx::query_as(&sql)
            .bind(limit.max(1) as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(map_err)?;
        rows.into_iter().map(JobRecord::try_from).collect()
    }

    async fn recover_stale_jobs(
        &self,
        worker_id: &str,
        current_owner: &str,
    ) -> StoreResult<Vec<JobRecord>> {
        let sql = format!(
            "UPDATE jobs SET \
               status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'queued' END, \
               finished_at = CASE WHEN attempts >= max_attempts THEN now() ELSE NULL END, \
               queued_at = CASE WHEN attempts >= max_attempts THEN queued_at ELSE now() END, \
               next_attempt_at = NULL, lease_expires_at = NULL, \
               error_code = CASE WHEN attempts >= max_attempts THEN 'attempts_exhausted' ELSE 'worker_lost' END, \
               error_message = 'worker lease expired or worker restarted during processing', \
               transition_reason = 'lease_recovered' \
             WHERE status = 'processing' AND ( \
               lease_expires_at < now() \
               OR (split_part(lease_owner, ':', 1) = $1 AND lease_owner <> $2)) \
             RETURNING {JOB_COLUMNS}"
        );
        let rows: Vec<JobRow> = sqlx::query_as(&sql)
            .bind(worker_id)
            .bind(current_owner)
            .fetch_all(&self.pool)
            .await
            .map_err(map_err)?;
        rows.into_iter().map(JobRecord::try_from).collect()
    }

    async fn status_history(&self, id: JobId) -> StoreResult<Vec<StatusHistoryEntry>> {
        let rows: Vec<HistoryRow> = sqlx::query_as(
            "SELECT from_status, to_status, status_version, attempt, reason, actor, error_code, at \
             FROM job_status_history WHERE job_id = $1 ORDER BY id",
        )
        .bind(id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(map_err)?;
        rows.into_iter()
            .map(|r| {
                Ok(StatusHistoryEntry {
                    from_status: r
                        .from_status
                        .map(|s| s.parse())
                        .transpose()
                        .map_err(|e| StoreError::Internal(format!("{e}")))?,
                    to_status: r
                        .to_status
                        .parse()
                        .map_err(|e| StoreError::Internal(format!("{e}")))?,
                    status_version: r.status_version,
                    attempt: r.attempt.max(0) as u32,
                    reason: r.reason,
                    actor: r.actor,
                    error_code: r.error_code,
                    at: r.at,
                })
            })
            .collect()
    }

    async fn count_by_status(&self) -> StoreResult<Vec<(JobStatus, i64)>> {
        let rows: Vec<(String, i64)> =
            sqlx::query_as("SELECT status, count(*) FROM jobs GROUP BY status")
                .fetch_all(&self.pool)
                .await
                .map_err(map_err)?;
        Ok(JobStatus::ALL
            .into_iter()
            .map(|s| {
                (
                    s,
                    rows.iter()
                        .find(|(n, _)| n == s.as_str())
                        .map(|(_, c)| *c)
                        .unwrap_or(0),
                )
            })
            .collect())
    }

    async fn ping(&self) -> StoreResult<()> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(map_err)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrations_are_embedded_in_order() {
        let names: Vec<_> = MIGRATOR.iter().map(|m| m.description.to_string()).collect();
        assert_eq!(names, vec!["jobs", "observability"]);
    }

    #[test]
    fn pool_errors_are_unavailable() {
        assert!(matches!(
            map_err(sqlx::Error::PoolTimedOut),
            StoreError::Unavailable(_)
        ));
        assert!(matches!(
            map_err(sqlx::Error::RowNotFound),
            StoreError::Internal(_)
        ));
    }
}
