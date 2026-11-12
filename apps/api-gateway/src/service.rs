//! `GatewayService` – the single application service behind **both** public
//! transports. REST handlers (`rest.rs`) and public gRPC handlers (`grpc.rs`)
//! call exactly these methods, so both APIs share validation, error semantics
//! and behavior. Transport-specific mapping happens only at the edges.

use std::time::Duration;

use engine_core::{
    EngineError, JobCommand, JobId, JobKind, JobStatus, JobSummary, StatusHistoryEntry,
};
use engine_ipc::mapping::{history_from_pb, job_from_pb, status_to_pb};
use engine_ipc::metadata::outbound;
use engine_ipc::pb;
use engine_ipc::pb::worker_engine_client::WorkerEngineClient;
use engine_ipc::status::status_to_error;
use engine_observability::MetricsRegistry;
use futures::{Stream, StreamExt};
use serde_json::Value;
use std::sync::Arc;
use tonic::transport::Channel;
use tracing::Instrument;

use crate::hub::StatusHub;

#[derive(Clone)]
pub struct GatewayService {
    client: WorkerEngineClient<Channel>,
    pub limits: engine_core::JobLimits,
    pub hub: StatusHub,
    rpc_timeout: Duration,
    pub metrics: Arc<MetricsRegistry>,
}

/// A completed job's result as seen by clients.
#[derive(Debug, Clone)]
pub struct JobResultView {
    pub job: JobSummary,
    pub result: Value,
    pub artifact: Option<pb::Artifact>,
}

pub struct ArtifactDownload {
    pub info: pb::Artifact,
    pub body: std::pin::Pin<Box<dyn Stream<Item = Result<Vec<u8>, EngineError>> + Send>>,
}

fn missing(what: &str) -> EngineError {
    EngineError::Internal(format!("worker response missing {what}"))
}

impl GatewayService {
    pub fn new(
        client: WorkerEngineClient<Channel>,
        limits: engine_core::JobLimits,
        hub: StatusHub,
        rpc_timeout: Duration,
        metrics: Arc<MetricsRegistry>,
    ) -> Self {
        Self {
            client,
            limits,
            hub,
            rpc_timeout,
            metrics,
        }
    }

    /// Builds an outbound request: trace context + request id + deadline.
    /// Must be called *inside* the client span so `traceparent` refers to it.
    fn req<T>(&self, msg: T, request_id: Option<&str>) -> tonic::Request<T> {
        let mut r = outbound(msg, request_id);
        r.set_timeout(self.rpc_timeout);
        r
    }

    fn client_span(&self, rpc: &'static str) -> tracing::Span {
        tracing::info_span!("ipc.call", otel.name = %format!("WorkerEngine/{rpc}"), otel.kind = "client", rpc.method = rpc)
    }

    fn rpc_failed(&self, rpc: &str, status: &tonic::Status) -> EngineError {
        let err = status_to_error(status);
        if matches!(err, EngineError::Unavailable(_) | EngineError::Timeout(_)) {
            self.metrics
                .counter(
                    "worker_rpc_errors_total",
                    &[("rpc", rpc), ("code", err.code())],
                )
                .inc();
        }
        err
    }

    #[tracing::instrument(name = "gateway.submit", skip_all, fields(job_type = %job_type))]
    pub async fn submit(
        &self,
        job_type: &str,
        payload: Value,
        idempotency_key: Option<String>,
        request_id: Option<&str>,
    ) -> Result<(JobSummary, bool), EngineError> {
        // Edge validation (short sync code) for a fast, cheap 422. The worker
        // validates again – it must never trust its caller.
        let kind: JobKind = job_type.parse()?;
        JobCommand::parse(kind, &payload, &self.limits)?;
        tracing::info!(
            event = "gateway.job_validated",
            "submission validated; forwarding to worker"
        );

        let msg = pb::SubmitJobRequest {
            job_type: job_type.to_string(),
            payload_json: payload.to_string(),
            idempotency_key,
        };
        let resp = async {
            let r = self.req(msg, request_id);
            self.client.clone().submit_job(r).await
        }
        .instrument(self.client_span("SubmitJob"))
        .await
        .map_err(|s| self.rpc_failed("SubmitJob", &s))?
        .into_inner();
        let job = job_from_pb(resp.job.as_ref().ok_or_else(|| missing("job"))?)?;
        self.metrics
            .counter("gateway_jobs_submitted_total", &[("job_type", job_type)])
            .inc();
        Ok((job, resp.deduplicated))
    }

    pub async fn get(
        &self,
        id: JobId,
        request_id: Option<&str>,
    ) -> Result<JobSummary, EngineError> {
        let job = async {
            let r = self.req(
                pb::GetJobRequest {
                    job_id: id.to_string(),
                },
                request_id,
            );
            self.client.clone().get_job(r).await
        }
        .instrument(self.client_span("GetJob"))
        .await
        .map_err(|s| self.rpc_failed("GetJob", &s))?
        .into_inner();
        job_from_pb(&job)
    }

    pub async fn result(
        &self,
        id: JobId,
        request_id: Option<&str>,
    ) -> Result<JobResultView, EngineError> {
        let r = async {
            let r = self.req(
                pb::GetJobResultRequest {
                    job_id: id.to_string(),
                },
                request_id,
            );
            self.client.clone().get_job_result(r).await
        }
        .instrument(self.client_span("GetJobResult"))
        .await
        .map_err(|s| self.rpc_failed("GetJobResult", &s))?
        .into_inner();
        Ok(JobResultView {
            job: job_from_pb(r.job.as_ref().ok_or_else(|| missing("job"))?)?,
            result: serde_json::from_str(&r.result_json).unwrap_or(Value::Null),
            artifact: r.artifact,
        })
    }

    pub async fn list(
        &self,
        status: Option<JobStatus>,
        limit: u32,
        request_id: Option<&str>,
    ) -> Result<Vec<JobSummary>, EngineError> {
        let msg = pb::ListJobsRequest {
            status: status.map(|s| status_to_pb(s) as i32),
            limit,
        };
        let resp = async {
            let r = self.req(msg, request_id);
            self.client.clone().list_jobs(r).await
        }
        .instrument(self.client_span("ListJobs"))
        .await
        .map_err(|s| self.rpc_failed("ListJobs", &s))?
        .into_inner();
        resp.jobs.iter().map(job_from_pb).collect()
    }

    pub async fn cancel(
        &self,
        id: JobId,
        request_id: Option<&str>,
    ) -> Result<JobSummary, EngineError> {
        let resp = async {
            let r = self.req(
                pb::CancelJobRequest {
                    job_id: id.to_string(),
                },
                request_id,
            );
            self.client.clone().cancel_job(r).await
        }
        .instrument(self.client_span("CancelJob"))
        .await
        .map_err(|s| self.rpc_failed("CancelJob", &s))?
        .into_inner();
        job_from_pb(resp.job.as_ref().ok_or_else(|| missing("job"))?)
    }

    pub async fn history(
        &self,
        id: JobId,
        request_id: Option<&str>,
    ) -> Result<Vec<StatusHistoryEntry>, EngineError> {
        let resp = async {
            let r = self.req(
                pb::GetJobHistoryRequest {
                    job_id: id.to_string(),
                },
                request_id,
            );
            self.client.clone().get_job_history(r).await
        }
        .instrument(self.client_span("GetJobHistory"))
        .await
        .map_err(|s| self.rpc_failed("GetJobHistory", &s))?
        .into_inner();
        resp.entries.iter().map(history_from_pb).collect()
    }

    /// Streams an artifact from the worker process chunk by chunk (the gateway
    /// never buffers the whole file).
    pub async fn download(
        &self,
        id: JobId,
        request_id: Option<&str>,
    ) -> Result<ArtifactDownload, EngineError> {
        let mut stream = async {
            // No deadline: large downloads may legitimately take a while.
            let r = outbound(
                pb::DownloadArtifactRequest {
                    job_id: id.to_string(),
                },
                request_id,
            );
            self.client.clone().download_artifact(r).await
        }
        .instrument(self.client_span("DownloadArtifact"))
        .await
        .map_err(|s| self.rpc_failed("DownloadArtifact", &s))?
        .into_inner();
        let first = stream
            .message()
            .await
            .map_err(|s| status_to_error(&s))?
            .ok_or_else(|| missing("artifact info"))?;
        let Some(pb::artifact_chunk::Kind::Info(info)) = first.kind else {
            return Err(missing("artifact info"));
        };
        let body = stream.map(|item| match item {
            Ok(pb::ArtifactChunk {
                kind: Some(pb::artifact_chunk::Kind::Data(bytes)),
            }) => Ok(bytes),
            Ok(_) => Ok(Vec::new()),
            Err(s) => Err(status_to_error(&s)),
        });
        Ok(ArtifactDownload {
            info,
            body: Box::pin(body),
        })
    }

    pub async fn engine_stats(
        &self,
        request_id: Option<&str>,
    ) -> Result<pb::EngineStats, EngineError> {
        let r = self.req(pb::EngineStatsRequest {}, request_id);
        Ok(self
            .client
            .clone()
            .get_engine_stats(r)
            .await
            .map_err(|s| self.rpc_failed("GetEngineStats", &s))?
            .into_inner())
    }

    pub async fn worker_health(&self) -> Result<pb::HealthResponse, EngineError> {
        let mut r = outbound(pb::HealthRequest {}, None);
        r.set_timeout(Duration::from_secs(2));
        Ok(self
            .client
            .clone()
            .health(r)
            .await
            .map_err(|s| status_to_error(&s))?
            .into_inner())
    }
}
