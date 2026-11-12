//! `text_transform` – short synchronous, Unicode-aware string work.
//!
//! Unicode notes (all covered by tests):
//! * `str::to_uppercase` is *full* case mapping: `"ß"` becomes `"SS"`, so the
//!   output may be longer than the input.
//! * A user-perceived character (grapheme cluster) can be several `char`s:
//!   `"e\u{301}"` (e + combining acute) or the family emoji 👨‍👩‍👧 (joined by ZWJ).
//!   `reverse` reverses graphemes (correct); `reverse_chars` reverses `char`s
//!   and visibly breaks such sequences (kept for teaching).

use engine_core::{
    BoxFuture, HandlerError, JobCommand, JobContext, JobHandler, JobKind, JobOutput, TextOperation,
    TextTransformParams,
};
use serde_json::{Value, json};
use unicode_segmentation::UnicodeSegmentation;

pub struct TextTransformHandler;

impl JobHandler for TextTransformHandler {
    fn kind(&self) -> JobKind {
        JobKind::TextTransform
    }

    fn execute<'a>(
        &'a self,
        _ctx: JobContext,
        command: JobCommand,
    ) -> BoxFuture<'a, Result<JobOutput, HandlerError>> {
        Box::pin(async move {
            let JobCommand::TextTransform(params) = command else {
                return Err(HandlerError::Unsupported(command.kind()));
            };
            Ok(JobOutput::inline(transform(&params)))
        })
    }
}

/// Takes `&TextTransformParams` (a borrow): it only reads the input and
/// returns a newly allocated `String` inside the JSON result.
pub fn transform(params: &TextTransformParams) -> Value {
    let input = params.text.as_str();
    let output: String = match params.operation {
        TextOperation::Uppercase => input.to_uppercase(),
        TextOperation::Lowercase => input.to_lowercase(),
        TextOperation::Reverse => input.graphemes(true).rev().collect(),
        TextOperation::ReverseChars => input.chars().rev().collect(),
    };
    json!({
        "operation": params.operation,
        "output": output,
        "input_stats": stats(input),
        "output_stats": stats(&output),
    })
}

fn stats(s: &str) -> Value {
    json!({
        "bytes": s.len(),
        "chars": s.chars().count(),
        "graphemes": s.graphemes(true).count(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(text: &str, operation: TextOperation) -> Value {
        transform(&TextTransformParams {
            text: text.into(),
            operation,
        })
    }

    #[test]
    fn upper_and_lower_case_are_full_unicode_mappings() {
        assert_eq!(run("straße", TextOperation::Uppercase)["output"], "STRASSE");
        assert_eq!(
            run("ÀÉÎ İ", TextOperation::Lowercase)["output"],
            "àéî i\u{307}"
        );
    }

    #[test]
    fn reverse_respects_grapheme_clusters() {
        let family = "👨\u{200d}👩\u{200d}👧";
        let text = format!("ab{family}e\u{301}");
        let out = run(&text, TextOperation::Reverse);
        assert_eq!(out["output"], format!("e\u{301}{family}ba"));
        assert_eq!(out["input_stats"]["graphemes"], 4);
        // 2 ('a','b') + 5 (man, ZWJ, woman, ZWJ, girl) + 2 ('e', U+0301)
        assert_eq!(out["input_stats"]["chars"], 9);
    }

    #[test]
    fn naive_char_reverse_breaks_combining_marks() {
        let out = run("e\u{301}x", TextOperation::ReverseChars);
        // The accent now precedes 'e' and attaches to 'x' instead.
        assert_eq!(out["output"], "x\u{301}e");
        assert_ne!(
            out["output"],
            run("e\u{301}x", TextOperation::Reverse)["output"]
        );
    }

    #[test]
    fn deterministic() {
        let a = run("Hello, Wörld", TextOperation::Reverse);
        let b = run("Hello, Wörld", TextOperation::Reverse);
        assert_eq!(a, b);
    }
}
