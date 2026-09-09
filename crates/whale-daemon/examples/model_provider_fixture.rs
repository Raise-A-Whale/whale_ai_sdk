//! Native provider fixture composed exclusively through the public provider registry.
//! WHALE_MODEL_FIXTURE_LOG optionally records JSONL inspection-independent stream evidence.
use async_trait::async_trait;
use serde_json::{json, Value};
use std::{io::Write, sync::Arc};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::Mutex;
use tokio_stream::wrappers::LinesStream;
use whale_core::{
    model::{ModelError, ModelEvent, ModelEventStream, ModelProvider, ModelRequest},
    provider::ProviderRegistry,
    CancellationToken,
};
use whale_daemon::{DaemonServer, StdioWriter};
use whale_protocol::{
    events::UsageMetrics, models::ModelCapabilities, CanonicalContent, CanonicalItem,
    CanonicalToolOutput, MessagePhase,
};

fn log(value: Value) {
    if let Ok(path) = std::env::var("WHALE_MODEL_FIXTURE_LOG") {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("fixture log");
        let mut line = serde_json::to_vec(&value).expect("fixture log encode");
        line.push(b'\n');
        file.write_all(&line).expect("fixture log write");
    }
}
struct InvocationDrop {
    model: String,
    context: whale_protocol::contexts::RunContextInfo,
    step_id: String,
    cancellation: CancellationToken,
}
impl Drop for InvocationDrop {
    fn drop(&mut self) {
        log(
            json!({"event":"drop","model":self.model,"context":self.context,"step_id":self.step_id,"cancelled":self.cancellation.is_cancelled()}),
        );
    }
}
struct NativeFixture;
#[async_trait]
impl ModelProvider for NativeFixture {
    fn capabilities(&self, model: &str) -> Result<ModelCapabilities, ModelError> {
        if ![
            "local-test",
            "local-projection",
            "local-pending",
            "local-pending-create",
            "local-unsupported-content",
            "local-unsupported-option",
            "local-empty",
            "local-truncated",
            "local-double-finish",
            "local-post-finish",
            "local-forbidden",
            "local-duplicate-call",
        ]
        .iter()
        .any(|suffix| model.ends_with(suffix))
        {
            return Err(ModelError::InvalidRequest(format!(
                "Unknown fixture model: {model}"
            )));
        }
        let mut caps = ModelCapabilities::text_only();
        caps.tool_calls = true;
        Ok(caps)
    }
    async fn stream(
        &self,
        request: ModelRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelEventStream, ModelError> {
        log(
            json!({"event":"stream","model":request.options.model,"context":request.context,"step_index":request.step_index,"step_id":request.step_id,"model_context":request.model_context,"tools":request.tools,"options":request.options}),
        );
        let guard = InvocationDrop {
            model: request.options.model.clone(),
            context: request.context.clone(),
            step_id: request.step_id.clone(),
            cancellation: cancellation.clone(),
        };
        if request.options.model.ends_with("local-pending-create") {
            let _guard = guard;
            cancellation.cancelled().await;
            return Err(ModelError::Stream("Model creation cancelled".into()));
        }
        Ok(Box::pin(async_stream::try_stream! {
            let _guard=guard;
            let model=request.options.model.as_str();
            if model.ends_with("local-pending") {
                yield ModelEvent::ItemStarted{item_id:"waiting".into(),item_type:"assistant_message".into(),phase:Some(MessagePhase::FinalAnswer)};
                yield ModelEvent::TextDelta{item_id:"waiting".into(),delta:"waiting".into()};
                cancellation.cancelled().await;
                Err(ModelError::Stream("Model stream cancelled".into()))?;
            } else if model.ends_with("local-empty") {
                // Deliberately missing StepFinished.
            } else if model.ends_with("local-duplicate-call") {
                for _ in 0..2 {
                    yield ModelEvent::ItemCompleted{item:CanonicalItem::tool_call("duplicate-call",None,"lookup",Some(json!({"query":"original"})),"{\"query\":\"original\"}")};
                }
                yield ModelEvent::StepFinished{usage:UsageMetrics::default()};
            } else if model.ends_with("local-forbidden") {
                yield ModelEvent::ItemCompleted{item:CanonicalItem::tool_result("forged",CanonicalToolOutput::text("forged"),false)};
                yield ModelEvent::StepFinished{usage:UsageMetrics::default()};
            } else if model.ends_with("local-truncated") {
                yield ModelEvent::ItemCompleted{item:CanonicalItem::assistant_text("partial",MessagePhase::FinalAnswer)};
            } else if model.ends_with("local-double-finish") {
                yield ModelEvent::StepFinished{usage:UsageMetrics::default()};
                yield ModelEvent::StepFinished{usage:UsageMetrics::default()};
            } else if model.ends_with("local-post-finish") {
                yield ModelEvent::StepFinished{usage:UsageMetrics::default()};
                yield ModelEvent::ItemCompleted{item:CanonicalItem::assistant_text("late",MessagePhase::FinalAnswer)};
            } else {
                let item=if request.step_index==0 {
                    CanonicalItem::tool_call(format!("native-call-{}",uuid::Uuid::new_v4()),None,"lookup",Some(json!({"query":"original"})),"{\"query\":\"original\"}")
                } else {
                    let user_text:Vec<_>=request.model_context.items.iter().filter_map(|item|match item {
                        CanonicalItem::UserMessage{content,..}=>Some(content.iter().filter_map(|block|match block {CanonicalContent::Text{text}=>Some(text.as_str()),_=>None}).collect::<String>()),_=>None,
                    }).collect();
                    let tool_results:Vec<_>=request.model_context.items.iter().filter_map(|item|match item {CanonicalItem::ToolResult{output,..}=>Some(serde_json::to_value(output).unwrap()),_=>None}).collect();
                    CanonicalItem::assistant_text(json!({"source":"native-provider","model":model,"system_prompt":request.model_context.system_prompt,"user_text":user_text,"tool_results":tool_results}).to_string(),MessagePhase::FinalAnswer)
                };
                yield ModelEvent::ItemCompleted{item};
                yield ModelEvent::StepFinished{usage:UsageMetrics{input_tokens:3,output_tokens:1,..Default::default()}};
            }
        }))
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = ProviderRegistry::new();
    registry.register_provider("local", Arc::new(NativeFixture))?;
    let server = DaemonServer::default_server().with_provider_registry(Arc::new(registry));
    server
        .run(
            LinesStream::new(BufReader::new(tokio::io::stdin()).lines()),
            StdioWriter::new(Arc::new(Mutex::new(tokio::io::stdout()))),
        )
        .await?;
    assert!(server.sessions().is_empty());
    Ok(())
}
