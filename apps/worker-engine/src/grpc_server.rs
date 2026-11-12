//! Internal gRPC server (WorkerEngine service) – the IPC endpoint the gateway
//! calls. A thin adapter: decode protobuf -> call `EngineHandle` -> encode.
//!
//! Concurrency: hyper/h2 runs every incoming RPC (HTTP/2 stream) as its own
//! Tokio task. So N concurrent `SubmitJob` calls are N concurrent *producer
//! tasks*, each cloning the MPSC `Sender` inside `EngineHandle::submit`.

use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use engine_application::{EngineHandle, EngineState, SubmitRequest};
use engine_core::{EngineError, JobId, JobKind, ListFilter, ValidationError};
use engine_handlers::ArtifactStore;
use engine_ipc::mapping::{
    artifact_to_pb, event_to_pb, history_to_pb, job_to_pb, status_from_pb, status_to_pb,
};
use engine_ipc::metadata::request_id;
use engine_ipc::pb::worker_engine_server::WorkerEngine;
use engine_ipc::pb::{self, artifact_chunk, status_stream_message};
use engine_ipc::status::error_to_status;
use engine_observability::{MetricsRegistry, SinkStats};
use engine_persistence::PgJobStore;
use futures::{Stream, StreamExt};
use tokio::sync::Semaphore;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_util::io::ReaderStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

pub type Engine = EngineHandle<PgJobStore>;
type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Clone)]
pub struct WorkerGrpc {
    /// Filled once the database is reachable. Until then every job RPC returns
    /// UNAVAILABLE and Health reports "starting".
    pub engine: Arc<OnceLock<Engine>>,
    pub artifacts: ArtifactStore,
    pub metrics: Arc<MetricsRegistry>,
    pub sink_stats: Arc<SinkStats>,
    pub rayon_threads: usize,
    pub status_streams: Arc<Semaphore>,
    pub server_stop: CancellationToken,
}

fn err(e: EngineError) -> Status {
    error_to_status(&e)
}

fn parse_id(s: &str) -> Result<JobId, Status> {
    s.parse::<JobId>().map_err(|e| err(e.into()))
}

impl WorkerGrpc {
    fn engine(&self) -> Result<&Engine, Status> {
        self.engine.get().ok_or_else(|| {
            err(EngineError::Unavailable(
                "engine starting: database not connected yet".into(),
            ))
        })
    }
}

#[tonic::async_trait]
impl WorkerEngine for WorkerGrpc {
    async fn submit_job(
        &self,
        request: Request<pb::SubmitJobRequest>,
    ) -> Result<Response<pb::SubmitJobResponse>, Status> {
        let request_id = request_id(&request);
        let req = request.into_inner();
        let engine = self.engine()?;
        let kind: JobKind = req
            .job_type
            .parse()
            .map_err(|e: ValidationError| err(e.into()))?;
        let payload: serde_json::Value = serde_json::from_str(&req.payload_json).map_err(|e| {
            err(
                ValidationError::new("invalid_payload", format!("payload_json is not JSON: {e}"))
                    .with_field("payload")
                    .into(),
            )
        })?;
        let out = engine
            .submit(SubmitRequest {
                kind,
                payload,
                idempotency_key: req.idempotency_key,
                request_id,
            })
            .await
            .map_err(err)?;
        Ok(Response::new(pb::SubmitJobResponse {
            job: Some(job_to_pb(&out.record.summary())),
            deduplicated: out.deduplicated,
        }))
    }

    async fn get_job(
        &self,
        request: Request<pb::GetJobRequest>,
    ) -> Result<Response<pb::Job>, Status> {
        let id = parse_id(&request.get_ref().job_id)?;
        let record = self.engine()?.get(id).await.map_err(err)?;
        Ok(Response::new(job_to_pb(&record.summary())))
    }

    async fn get_job_result(
        &self,
        request: Request<pb::GetJobResultRequest>,
    ) -> Result<Response<pb::JobResult>, Status> {
        let id = parse_id(&request.get_ref().job_id)?;
        let record = self.engine()?.result(id).await.map_err(err)?;
        Ok(Response::new(pb::JobResult {
            job: Some(job_to_pb(&record.summary())),
            result_json: record
                .result
                .as_ref()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "null".into()),
            artifact: record.artifact.as_ref().map(artifact_to_pb),
        }))
    }

    async fn list_jobs(
        &self,
        request: Request<pb::ListJobsRequest>,
    ) -> Result<Response<pb::ListJobsResponse>, Status> {
        let req = request.into_inner();
        let status = match req.status {
            Some(v) => Some(status_from_pb(v).ok_or_else(|| {
                err(ValidationError::new("invalid_status", "unknown status")
                    .with_field("status")
                    .into())
            })?),
            None => None,
        };
        let limit = if req.limit == 0 {
            50
        } else {
            req.limit.min(500)
        };
        let jobs = self
            .engine()?
            .list(ListFilter { status, limit })
            .await
            .map_err(err)?;
        Ok(Response::new(pb::ListJobsResponse {
            jobs: jobs.iter().map(|j| job_to_pb(&j.summary())).collect(),
        }))
    }

    async fn cancel_job(
        &self,
        request: Request<pb::CancelJobRequest>,
    ) -> Result<Response<pb::CancelJobResponse>, Status> {
        let id = parse_id(&request.get_ref().job_id)?;
        let record = self.engine()?.cancel(id).await.map_err(err)?;
        Ok(Response::new(pb::CancelJobResponse {
            job: Some(job_to_pb(&record.summary())),
        }))
    }

    async fn get_job_history(
        &self,
        request: Request<pb::GetJobHistoryRequest>,
    ) -> Result<Response<pb::GetJobHistoryResponse>, Status> {
        let id = parse_id(&request.get_ref().job_id)?;
        let entries = self.engine()?.history(id).await.map_err(err)?;
        Ok(Response::new(pb::GetJobHistoryResponse {
            entries: entries.iter().map(history_to_pb).collect(),
        }))
    }

    type DownloadArtifactStream = BoxStream<pb::ArtifactChunk>;

    /// Streams the artifact file in 64 KiB chunks (first message = metadata).
    /// The file is read with async I/O; memory use is bounded per stream.
    async fn download_artifact(
        &self,
        request: Request<pb::DownloadArtifactRequest>,
    ) -> Result<Response<Self::DownloadArtifactStream>, Status> {
        let id = parse_id(&request.get_ref().job_id)?;
        let record = self.engine()?.result(id).await.map_err(err)?;
        let artifact = record
            .artifact
            .ok_or_else(|| Status::failed_precondition("this job type produces no artifact"))?;
        let path = self
            .artifacts
            .resolve(&artifact.relative_path)
            .map_err(|_| Status::internal("invalid artifact path"))?;
        let file = tokio::fs::File::open(&path)
            .await
            .map_err(|e| Status::internal(format!("artifact unavailable: {}", e.kind())))?;
        let info = futures::stream::once(async move {
            Ok(pb::ArtifactChunk {
                kind: Some(artifact_chunk::Kind::Info(artifact_to_pb(&artifact))),
            })
        });
        let data = ReaderStream::with_capacity(file, 64 * 1024).map(|chunk| match chunk {
            Ok(bytes) => Ok(pb::ArtifactChunk {
                kind: Some(artifact_chunk::Kind::Data(bytes.to_vec())),
            }),
            Err(e) => Err(Status::internal(format!(
                "artifact read failed: {}",
                e.kind()
            ))),
        });
        Ok(Response::new(Box::pin(info.chain(data))))
    }

    type SubscribeStatusStream = BoxStream<pb::StatusStreamMessage>;

    /// Bridges the in-process `broadcast` bus to the other process. One stream
    /// per gateway; bounded by `MAX_STATUS_SUBSCRIBERS`.
    async fn subscribe_status(
        &self,
        _request: Request<pb::SubscribeStatusRequest>,
    ) -> Result<Response<Self::SubscribeStatusStream>, Status> {
        let engine = self.engine()?;
        let permit = self
            .status_streams
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("too many status subscribers"))?;
        let receiver = engine.subscribe();
        tracing::info!(
            event = "ipc.status_subscriber_connected",
            "gateway subscribed to status stream"
        );
        let stream = BroadcastStream::new(receiver)
            .map(move |item| {
                let _keep = &permit; // permit lives as long as the stream
                let kind = match item {
                    Ok(ev) => status_stream_message::Kind::Event(event_to_pb(&ev)),
                    Err(BroadcastStreamRecvError::Lagged(n)) => {
                        tracing::warn!(
                            event = "ipc.status_subscriber_lagged",
                            missed = n,
                            "subscriber lagged; sending resync"
                        );
                        status_stream_message::Kind::Resync(pb::Resync { missed: n })
                    }
                };
                Ok(pb::StatusStreamMessage { kind: Some(kind) })
            })
            .take_until(self.server_stop.clone().cancelled_owned());
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_engine_stats(
        &self,
        _request: Request<pb::EngineStatsRequest>,
    ) -> Result<Response<pb::EngineStats>, Status> {
        let engine = self.engine()?;
        let stats = engine.stats();
        let counts = engine.counts_by_status().await.unwrap_or_default();
        engine_observability::record_runtime_metrics(&self.metrics, "worker-engine");
        let rt = tokio::runtime::Handle::current().metrics();
        Ok(Response::new(pb::EngineStats {
            worker_id: stats.worker_id,
            state: stats.state.as_str().into(),
            queue_capacity: stats.queue_capacity as u32,
            queue_depth: stats.queue_depth as u32,
            max_active_jobs: stats.max_active_jobs as u32,
            active_jobs: stats.active_jobs as u32,
            running_job_ids: stats
                .running_job_ids
                .iter()
                .map(|id| id.to_string())
                .collect(),
            rayon_threads: self.rayon_threads as u32,
            tokio_worker_threads: rt.num_workers() as u32,
            tokio_alive_tasks: rt.num_alive_tasks() as u64,
            jobs_by_status: counts
                .into_iter()
                .map(|(s, c)| pb::StatusCount {
                    status: status_to_pb(s) as i32,
                    count: c,
                })
                .collect(),
            log_records_dropped: self.sink_stats.dropped(),
            log_records_written: self.sink_stats.written(),
            status_subscribers: stats.status_subscribers as u32,
            metrics_prometheus: self.metrics.render_prometheus(),
            available_parallelism: std::thread::available_parallelism()
                .map(|n| n.get() as u32)
                .unwrap_or(0),
        }))
    }

    async fn health(
        &self,
        _request: Request<pb::HealthRequest>,
    ) -> Result<Response<pb::HealthResponse>, Status> {
        let Some(engine) = self.engine.get() else {
            return Ok(Response::new(pb::HealthResponse {
                ready: false,
                state: "starting".into(),
                database_ok: false,
                detail: "waiting for database".into(),
            }));
        };
        let state = engine.state();
        let db = tokio::time::timeout(Duration::from_secs(1), engine.ping_store()).await;
        let (database_ok, detail) = match db {
            Ok(Ok(())) => (true, String::new()),
            Ok(Err(e)) => (false, e.to_string()),
            Err(_) => (false, "database ping timed out".into()),
        };
        Ok(Response::new(pb::HealthResponse {
            ready: state == EngineState::Ready && database_ok,
            state: state.as_str().into(),
            database_ok,
            detail,
        }))
    }
}
