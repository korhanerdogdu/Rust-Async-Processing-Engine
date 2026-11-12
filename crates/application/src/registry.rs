use std::collections::HashMap;
use std::sync::Arc;

use engine_core::{
    BoxFuture, HandlerError, JobCommand, JobContext, JobHandler, JobKind, JobOutput,
};

/// Maps job kinds to handler trait objects. Built once at startup, then
/// shared immutably (`Arc<Shared>`), so no lock is needed to look a handler up.
#[derive(Default, Clone)]
pub struct HandlerRegistry {
    handlers: HashMap<JobKind, Arc<dyn JobHandler>>,
}

impl HandlerRegistry {
    pub fn new(handlers: impl IntoIterator<Item = Arc<dyn JobHandler>>) -> Self {
        let mut reg = Self::default();
        for h in handlers {
            reg.register(h);
        }
        reg
    }

    pub fn register(&mut self, handler: Arc<dyn JobHandler>) {
        self.handlers.insert(handler.kind(), handler);
    }

    pub fn kinds(&self) -> Vec<JobKind> {
        let mut k: Vec<_> = self.handlers.keys().copied().collect();
        k.sort();
        k
    }

    /// Dynamic dispatch: one vtable call selects the handler at runtime.
    pub fn execute(
        &self,
        ctx: JobContext,
        command: JobCommand,
    ) -> BoxFuture<'_, Result<JobOutput, HandlerError>> {
        match self.handlers.get(&command.kind()) {
            Some(handler) => handler.execute(ctx, command),
            None => {
                let kind = command.kind();
                Box::pin(async move { Err(HandlerError::Unsupported(kind)) })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_core::{JobId, TextOperation, TextTransformParams};
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    struct Echo;
    impl JobHandler for Echo {
        fn kind(&self) -> JobKind {
            JobKind::TextTransform
        }
        fn execute<'a>(
            &'a self,
            _ctx: JobContext,
            _c: JobCommand,
        ) -> BoxFuture<'a, Result<JobOutput, HandlerError>> {
            Box::pin(async { Ok(JobOutput::inline(json!("echo"))) })
        }
    }

    fn ctx() -> JobContext {
        JobContext {
            job_id: JobId::generate(),
            attempt: 1,
            cancel: CancellationToken::new(),
        }
    }

    #[tokio::test]
    async fn dispatches_by_kind_and_reports_unsupported() {
        let reg = HandlerRegistry::new([Arc::new(Echo) as Arc<dyn JobHandler>]);
        let cmd = JobCommand::TextTransform(TextTransformParams {
            text: "x".into(),
            operation: TextOperation::Uppercase,
        });
        assert_eq!(reg.execute(ctx(), cmd).await.unwrap().result, json!("echo"));

        let other = JobCommand::SimulateDelay(engine_core::DelayParams {
            duration_ms: 1,
            fail_first_attempts: 0,
        });
        assert_eq!(
            reg.execute(ctx(), other).await.unwrap_err(),
            HandlerError::Unsupported(JobKind::SimulateDelay)
        );
        assert_eq!(reg.kinds(), vec![JobKind::TextTransform]);
    }
}
