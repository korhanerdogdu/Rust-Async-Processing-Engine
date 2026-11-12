//! Outbound gRPC metadata: W3C trace context + request id.

use engine_observability::propagation::{REQUEST_ID_HEADER, inject_current};
use opentelemetry::propagation::Injector;
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};

/// Lets the OpenTelemetry propagator write `traceparent`/`tracestate` into
/// tonic metadata.
pub struct MetadataInjector<'a>(pub &'a mut MetadataMap);

impl Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(k), Ok(v)) = (
            MetadataKey::from_bytes(key.as_bytes()),
            MetadataValue::try_from(value.as_str()),
        ) {
            self.0.insert(k, v);
        }
    }
}

/// Wraps a message in a `tonic::Request` carrying the current span's trace
/// context and the request id, so the worker's spans join the same trace.
pub fn outbound<T>(message: T, request_id: Option<&str>) -> tonic::Request<T> {
    let mut req = tonic::Request::new(message);
    inject_current(&mut MetadataInjector(req.metadata_mut()));
    if let Some(id) = request_id.and_then(|r| MetadataValue::try_from(r).ok()) {
        req.metadata_mut().insert(REQUEST_ID_HEADER, id);
    }
    req
}

/// Reads the request id from inbound metadata.
pub fn request_id<T>(req: &tonic::Request<T>) -> Option<String> {
    req.metadata()
        .get(REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}
