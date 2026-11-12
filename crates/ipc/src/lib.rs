//! # engine-ipc
//!
//! Everything both processes need to speak gRPC to each other:
//!
//! * [`pb`] – code generated from `proto/engine.proto` (messages, clients, servers)
//! * [`mapping`] – `engine-core` types <-> protobuf messages
//! * [`status`] – `EngineError` <-> `tonic::Status` (transport boundary mapping)
//! * [`metadata`] – W3C `traceparent` + `x-request-id` in gRPC metadata
//! * [`client`] – a configured, lazily connecting `WorkerEngineClient`
//!
//! This is *real* inter-process communication: protobuf over HTTP/2 over TCP.
//! Contrast with `tokio::sync::mpsc`, which only moves values between tasks
//! inside one process.

pub mod client;
pub mod mapping;
pub mod metadata;
pub mod status;

#[allow(clippy::all, clippy::pedantic)]
pub mod pb {
    tonic::include_proto!("engine.v1");

    /// Encoded descriptor set, used by gRPC server reflection (grpcurl).
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("engine_descriptor");
}
