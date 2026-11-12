//! Public gRPC API (`engine.v1.JobGateway`, port 50051).
//!
//! Same semantics as REST because both call [`GatewayService`]; only the
//! error mapping differs (`EngineError` -> `tonic::Status` here, -> HTTP in
//! `rest.rs`). Server reflection is enabled so `grpcurl` works without the
//! .proto file.

use std::pin::Pin;

use engine_core::{EngineError, JobId, ValidationError};
use engine_ipc::mapping::{event_to_pb, job_to_pb, status_from_pb};
use engine_ipc::metadata::request_id;
use engine_ipc::pb;
use engine_ipc::pb::job_gateway_server::JobGateway;
use engine_ipc::status::error_to_status;
use futures::{Stream, StreamExt};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use crate::hub::{WatchItem, watch_job};
use crate::service::GatewayService;

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Clone)]
pub struct PublicGrpc {
    pub service: GatewayService,
    pub shutdown: CancellationToken,
}

fn err(e: EngineError) -> Status {
    error_to_status(&e)
}

fn parse_id(s: &str) -> Result<JobId, Status> {
    s.parse::<JobId>().map_err(|e| err(e.into()))
}

#[tonic::async_trait]
impl JobGateway for PublicGrpc {
    async fn submit_job(
        &self,
        request: Request<pb::SubmitJobRequest>,
    ) -> Result<Response<pb::SubmitJobResponse>, Status> {
        let rid = request_id(&request);
        let req = request.into_inner();
        let payload: serde_json::Value = serde_json::from_str(&req.payload_json).map_err(|e| {
            err(
                ValidationError::new("invalid_payload", format!("payload_json is not JSON: {e}"))
                    .with_field("payload")
                    .into(),
            )
        })?;
        let (job, deduplicated) = self
            .service
            .submit(&req.job_type, payload, req.idempotency_key, rid.as_deref())
            .await
            .map_err(err)?;
        Ok(Response::new(pb::SubmitJobResponse {
            job: Some(job_to_pb(&job)),
            deduplicated,
        }))
    }

    async fn get_job(
        &self,
        request: Request<pb::GetJobRequest>,
    ) -> Result<Response<pb::Job>, Status> {
        let rid = request_id(&request);
        let id = parse_id(&request.get_ref().job_id)?;
        let job = self.service.get(id, rid.as_deref()).await.map_err(err)?;
        Ok(Response::new(job_to_pb(&job)))
    }

    async fn get_job_result(
        &self,
        request: Request<pb::GetJobResultRequest>,
    ) -> Result<Response<pb::JobResult>, Status> {
        let rid = request_id(&request);
        let id = parse_id(&request.get_ref().job_id)?;
        let r = self.service.result(id, rid.as_deref()).await.map_err(err)?;
        Ok(Response::new(pb::JobResult {
            job: Some(job_to_pb(&r.job)),
            result_json: r.result.to_string(),
            artifact: r.artifact,
        }))
    }

    async fn list_jobs(
        &self,
        request: Request<pb::ListJobsRequest>,
    ) -> Result<Response<pb::ListJobsResponse>, Status> {
        let rid = request_id(&request);
        let req = request.into_inner();
        let status = match req.status {
            Some(v) => Some(status_from_pb(v).ok_or_else(|| {
                err(ValidationError::new("invalid_status", "unknown status").into())
            })?),
            None => None,
        };
        let limit = if req.limit == 0 {
            50
        } else {
            req.limit.min(500)
        };
        let jobs = self
            .service
            .list(status, limit, rid.as_deref())
            .await
            .map_err(err)?;
        Ok(Response::new(pb::ListJobsResponse {
            jobs: jobs.iter().map(job_to_pb).collect(),
        }))
    }

    async fn cancel_job(
        &self,
        request: Request<pb::CancelJobRequest>,
    ) -> Result<Response<pb::CancelJobResponse>, Status> {
        let rid = request_id(&request);
        let id = parse_id(&request.get_ref().job_id)?;
        let job = self.service.cancel(id, rid.as_deref()).await.map_err(err)?;
        Ok(Response::new(pb::CancelJobResponse {
            job: Some(job_to_pb(&job)),
        }))
    }

    type DownloadArtifactStream = BoxStream<pb::ArtifactChunk>;

    async fn download_artifact(
        &self,
        request: Request<pb::DownloadArtifactRequest>,
    ) -> Result<Response<Self::DownloadArtifactStream>, Status> {
        let rid = request_id(&request);
        let id = parse_id(&request.get_ref().job_id)?;
        let dl = self
            .service
            .download(id, rid.as_deref())
            .await
            .map_err(err)?;
        let info = futures::stream::once(async move {
            Ok(pb::ArtifactChunk {
                kind: Some(pb::artifact_chunk::Kind::Info(dl.info)),
            })
        });
        let data = dl.body.map(|c| match c {
            Ok(bytes) => Ok(pb::ArtifactChunk {
                kind: Some(pb::artifact_chunk::Kind::Data(bytes)),
            }),
            Err(e) => Err(err(e)),
        });
        Ok(Response::new(Box::pin(info.chain(data))))
    }

    type WatchJobStream = BoxStream<pb::WatchJobMessage>;

    async fn watch_job(
        &self,
        request: Request<pb::WatchJobRequest>,
    ) -> Result<Response<Self::WatchJobStream>, Status> {
        let id = parse_id(&request.get_ref().job_id)?;
        let rx = watch_job(self.service.clone(), id, self.shutdown.child_token());
        let stream = ReceiverStream::new(rx).map(|item| match item {
            Ok(WatchItem::Snapshot(job)) => Ok(pb::WatchJobMessage {
                kind: Some(pb::watch_job_message::Kind::Snapshot(job_to_pb(&job))),
            }),
            Ok(WatchItem::Event(ev)) => Ok(pb::WatchJobMessage {
                kind: Some(pb::watch_job_message::Kind::Event(event_to_pb(&ev))),
            }),
            Err(e) => Err(err(e)),
        });
        Ok(Response::new(Box::pin(stream)))
    }
}
