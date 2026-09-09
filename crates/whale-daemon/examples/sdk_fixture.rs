//! Deterministic model fixture behind the production daemon/stdin/stdout stack.
//!
//! Build: cargo build -p whale-daemon --example sdk_fixture
//! Latest prompt `pending` never completes; `long` emits 1000 text deltas.
//! Other prompts request host tool `lookup({"query":"original"})`, then finish.
//! The usual SDK daemon CLI flags are accepted and ignored; no network is used.

use serde_json::json;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::Mutex;
use tokio_stream::wrappers::LinesStream;
use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
use whale_daemon::{DaemonServer, StdioWriter};
use whale_protocol::canonical::{CanonicalContent, CanonicalItem, MessagePhase};
use whale_protocol::events::{AgentStreamEvent, UsageMetrics};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gate = Arc::new(ApprovalGate::new());
    let coordinator = Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    ));
    let engine = AgentEngine::new(coordinator).with_stream_provider(Arc::new(|session, step| {
        let prompt = session
            .history()
            .iter()
            .rev()
            .find_map(|item| match item {
                CanonicalItem::UserMessage { content, .. } => Some(
                    content
                        .iter()
                        .filter_map(|block| match block {
                            CanonicalContent::Text { text } => Some(text.as_str()),
                            _ => None,
                        })
                        .collect::<String>(),
                ),
                _ => None,
            })
            .unwrap_or_default();
        if prompt == "pending" {
            return Ok(Box::pin(futures::stream::pending()));
        }
        let mut events = Vec::new();
        if step > 0 && prompt == "long" {
            for index in 0..1000 {
                events.push(Ok(AgentStreamEvent::TextDelta {
                    turn_id: "provider-turn".into(),
                    item_id: "fixture-text".into(),
                    delta: format!("{index} "),
                }));
            }
        }
        let item = if step == 0 {
            CanonicalItem::tool_call(
                format!("fixture-call-{}", uuid::Uuid::new_v4()),
                None,
                "lookup",
                Some(json!({"query":"original"})),
                "{\"query\":\"original\"}",
            )
        } else {
            CanonicalItem::assistant_text("fixture complete", MessagePhase::FinalAnswer)
        };
        events.push(Ok(AgentStreamEvent::ItemCompleted {
            turn_id: "provider-turn".into(),
            item,
        }));
        events.push(Ok(AgentStreamEvent::TurnCompleted {
            turn_id: "provider-turn".into(),
            thread_id: "provider-thread".into(),
            usage: UsageMetrics {
                input_tokens: 3,
                output_tokens: 1,
                ..Default::default()
            },
        }));
        Ok(Box::pin(futures::stream::iter(events)))
    }));
    let server = DaemonServer::new(Arc::new(engine), gate);
    let lines = LinesStream::new(BufReader::new(tokio::io::stdin()).lines());
    let writer = StdioWriter::new(Arc::new(Mutex::new(tokio::io::stdout())));
    server.run(lines, writer).await?;
    assert!(server.sessions().is_empty(), "sessions leaked after EOF");
    assert_eq!(
        server.approval_gate().pending_count(),
        0,
        "approval waits leaked after EOF"
    );
    assert!(
        server.pending_host_tool_calls().is_empty(),
        "host calls leaked after EOF"
    );
    Ok(())
}
