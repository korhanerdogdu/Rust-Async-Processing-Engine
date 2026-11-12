//! `json_format` – **short synchronous work**.
//!
//! Parsing and pretty-printing a bounded (<= 256 KiB by default) JSON text is
//! fast, CPU-light and never waits on I/O, so it runs *inline* in the async
//! job task. Spawning it onto another thread would cost more than it saves.

use engine_core::{
    BoxFuture, HandlerError, JobCommand, JobContext, JobHandler, JobKind, JobOutput,
    JsonFormatParams,
};
use serde::Serialize;
use serde_json::{Value, json};

pub struct JsonFormatHandler;

impl JobHandler for JsonFormatHandler {
    fn kind(&self) -> JobKind {
        JobKind::JsonFormat
    }

    fn execute<'a>(
        &'a self,
        _ctx: JobContext,
        command: JobCommand,
    ) -> BoxFuture<'a, Result<JobOutput, HandlerError>> {
        Box::pin(async move {
            let JobCommand::JsonFormat(params) = command else {
                return Err(HandlerError::Unsupported(command.kind()));
            };
            format_json(&params).map(JobOutput::inline)
        })
    }
}

/// Pure function: easy to unit-test, no async involved.
pub fn format_json(params: &JsonFormatParams) -> Result<Value, HandlerError> {
    let mut value: Value = serde_json::from_str(&params.input).map_err(|e| {
        HandlerError::invalid_input(
            "invalid_json",
            format!(
                "input is not valid JSON at line {} column {}: {e}",
                e.line(),
                e.column()
            ),
        )
    })?;
    if params.sort_keys {
        sort_keys(&mut value);
    }

    let formatted = if params.indent == 0 {
        serde_json::to_string(&value)
    } else {
        let indent = vec![b' '; params.indent as usize];
        let mut buf = Vec::with_capacity(params.input.len() * 2);
        let formatter = serde_json::ser::PrettyFormatter::with_indent(&indent);
        let mut ser = serde_json::Serializer::with_formatter(&mut buf, formatter);
        value
            .serialize(&mut ser)
            .map(|_| String::from_utf8(buf).unwrap_or_default())
    }
    .map_err(|e| HandlerError::failed("serialize_failed", e.to_string(), false))?;

    Ok(json!({
        "valid": true,
        "top_level_type": type_name(&value),
        "depth": depth(&value),
        "bytes_in": params.input.len(),
        "bytes_out": formatted.len(),
        "formatted": formatted,
    }))
}

fn sort_keys(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.sort_keys();
            map.values_mut().for_each(sort_keys);
        }
        Value::Array(items) => items.iter_mut().for_each(sort_keys),
        _ => {}
    }
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn depth(v: &Value) -> usize {
    match v {
        Value::Array(a) => 1 + a.iter().map(depth).max().unwrap_or(0),
        Value::Object(o) => 1 + o.values().map(depth).max().unwrap_or(0),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(input: &str, indent: u8, sort: bool) -> JsonFormatParams {
        JsonFormatParams {
            input: input.into(),
            indent,
            sort_keys: sort,
        }
    }

    #[test]
    fn pretty_prints_deterministically() {
        let out = format_json(&p(r#"{"b":1,"a":[1,2]}"#, 2, false)).unwrap();
        assert_eq!(
            out["formatted"],
            "{\n  \"b\": 1,\n  \"a\": [\n    1,\n    2\n  ]\n}"
        );
        assert_eq!(out["top_level_type"], "object");
        assert_eq!(out["depth"], 2);
    }

    #[test]
    fn sorts_keys_recursively_and_compacts() {
        let out = format_json(&p(r#"{"b":{"z":1,"y":2},"a":0}"#, 0, true)).unwrap();
        assert_eq!(out["formatted"], r#"{"a":0,"b":{"y":2,"z":1}}"#);
    }

    #[test]
    fn malformed_json_is_a_typed_non_retryable_error() {
        let err = format_json(&p("{\"a\": [1, 2,]}", 2, false)).unwrap_err();
        assert_eq!(err.code(), "invalid_json");
        assert!(!err.is_retryable());
        assert!(err.to_string().contains("line 1"));
    }
}
