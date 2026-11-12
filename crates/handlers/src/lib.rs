//! # engine-handlers
//!
//! One module per execution style (see `docs/execution-model.md`):
//!
//! | handler          | style                         | mechanism                                   |
//! |------------------|-------------------------------|---------------------------------------------|
//! | `json_format`    | short synchronous             | plain `fn`, inline in the async task        |
//! | `text_transform` | short synchronous             | plain `fn`, inline in the async task        |
//! | `simulate_delay` | async waiting                 | `tokio::time::sleep` + cancellation         |
//! | `html_to_pdf`    | external process + blocking IO| `tokio::process` + `spawn_blocking`         |
//! | `cpu_compute`    | CPU-parallel                  | dedicated Rayon pool + `oneshot` hand-off   |

use std::sync::Arc;

use engine_core::JobHandler;

pub mod artifacts;
pub mod cpu;
pub mod delay;
pub mod diagnostics;
pub mod json;
pub mod pdf;
pub mod text;

pub use artifacts::ArtifactStore;
pub use cpu::{CpuComputeHandler, CpuConfig};
pub use delay::DelayHandler;
pub use json::JsonFormatHandler;
pub use pdf::{HtmlToPdfHandler, PdfConfig};
pub use text::TextTransformHandler;

#[derive(Debug, Clone, Default)]
pub struct HandlersConfig {
    pub pdf: PdfConfig,
    pub cpu: CpuConfig,
}

/// Builds the standard handler set. Returned as trait objects so the
/// dispatcher can select one at runtime by `JobKind`.
pub fn default_handlers(
    cfg: &HandlersConfig,
    artifacts: ArtifactStore,
) -> Result<Vec<Arc<dyn JobHandler>>, rayon::ThreadPoolBuildError> {
    Ok(vec![
        Arc::new(JsonFormatHandler),
        Arc::new(TextTransformHandler),
        Arc::new(DelayHandler),
        Arc::new(HtmlToPdfHandler::new(cfg.pdf.clone(), artifacts)),
        Arc::new(CpuComputeHandler::new(cfg.cpu.clone())?),
    ])
}
