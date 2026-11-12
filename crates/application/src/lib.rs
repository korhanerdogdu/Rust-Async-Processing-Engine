//! # engine-application
//!
//! The worker engine's application layer – independent of gRPC and SQL:
//!
//! * [`engine::EngineHandle`] – submit / query / cancel; admission control.
//! * `dispatcher` – the **single MPSC consumer** + per-job execution.
//! * `reconciler` – lease recovery + DB-to-queue re-dispatch.
//! * [`bus::StatusBus`] – `broadcast` fan-out of committed transitions.
//! * [`memory::InMemoryJobStore`] – test double with the same guarded semantics.
//!
//! It is generic over `S: JobStore` (static dispatch): the worker binary
//! instantiates it with `PgJobStore`, tests with `InMemoryJobStore`.

pub mod bus;
pub mod config;
mod dispatcher;
pub mod engine;
pub mod memory;
pub mod queue;
mod reconciler;
pub mod registry;

pub use bus::StatusBus;
pub use config::{EngineConfig, KindTimeouts};
pub use engine::{
    ArtifactCleanup, EngineHandle, EngineState, EngineStats, SubmitOutcome, SubmitRequest,
};
pub use memory::InMemoryJobStore;
pub use reconciler::ReconcileReport;
pub use registry::HandlerRegistry;
