//! # engine-core
//!
//! The transport-independent heart of the project. Nothing in this crate knows
//! about HTTP, gRPC, SQL or Tokio's runtime: it only defines *what* a job is,
//! *which* state transitions are legal, *which* errors exist and *which* traits
//! the outer layers implement.
//!
//! Keeping this crate free of `axum`, `tonic` and `sqlx` is deliberate: error
//! mapping (`EngineError` -> HTTP status / gRPC code) happens at the transport
//! boundary, never here. See `docs/architecture.md`.

pub mod error;
pub mod event;
pub mod handler;
pub mod ids;
pub mod job;
pub mod limits;
pub mod output;
pub mod record;
pub mod status;
pub mod store;

pub use error::{EngineError, HandlerError, ValidationError};
pub use event::JobStatusEvent;
pub use handler::{BoxFuture, JobContext, JobHandler};
pub use ids::JobId;
pub use job::{
    CpuComputeParams, CpuOperation, DelayParams, HtmlToPdfParams, JobCommand, JobKind,
    JsonFormatParams, TextOperation, TextTransformParams,
};
pub use limits::JobLimits;
pub use output::{ArtifactRef, JobOutput};
pub use record::{
    CancelOutcome, InsertOutcome, JobFailure, JobRecord, JobSummary, LeaseInfo, ListFilter, NewJob,
    StatusHistoryEntry,
};
pub use status::JobStatus;
pub use store::{JobStore, StoreError};

#[cfg(test)]
mod send_sync_tests {
    //! Compile-time proofs that the types we move between tasks/threads are
    //! `Send` (may be moved to another thread) and `Sync` (may be shared by
    //! reference between threads). If someone adds an `Rc` or `RefCell` to
    //! one of these types, this module stops compiling.
    use super::*;

    fn assert_send<T: Send>() {}
    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn core_types_are_thread_safe() {
        assert_send_sync::<JobId>();
        assert_send_sync::<JobCommand>();
        assert_send_sync::<JobRecord>();
        assert_send_sync::<JobStatusEvent>();
        assert_send_sync::<EngineError>();
        assert_send_sync::<HandlerError>();
        assert_send_sync::<JobContext>();
        assert_send::<BoxFuture<'static, ()>>();
    }
}
