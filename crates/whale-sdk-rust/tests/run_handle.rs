use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use whale_core::{
    AgentEngine, ApprovalGate, ThreadSession, ToolExecutionCoordinator, ToolRegistry,
};
use whale_daemon::DaemonServer;
use whale_protocol::runs::{RunApprovalDecision, RunEventPayload, RunStatus};
use whale_protocol::{AgentStreamEvent, CanonicalItem, CanonicalToolOutput, MessagePhase};
use whale_sdk_rust::{HostTool, WhaleClient};

fn client_with_provider(
    provider: impl Fn(&ThreadSession, usize) -> Result<whale_adapters::BoxedEventStream, whale_core::CoreError>
        + Send
        + Sync
        + 'static,
) -> WhaleClient {
    let gate = Arc::new(ApprovalGate::new());
    let coordinator = Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    ));
    let engine = Arc::new(AgentEngine::new(coordinator).with_stream_provider(Arc::new(provider)));
    WhaleClient::in_process(Arc::new(DaemonServer::new(engine, gate)))
}

#[tokio::test]
async fn handle_returns_before_model_and_cancel_releases_session() {
    let block = Arc::new(Notify::new());
    let client = client_with_provider(move |_, _| {
        let block = block.clone();
        Ok(Box::pin(async_stream::try_stream! {
            block.notified().await;
            yield AgentStreamEvent::ItemCompleted { turn_id:"provider".into(), item:CanonicalItem::assistant_text("done",MessagePhase::FinalAnswer) };
            yield AgentStreamEvent::TurnCompleted { turn_id:"provider".into(), thread_id:"provider".into(), usage:Default::default() };
        }))
    });
    let thread = client.create_thread("mock", None).await.unwrap();
    let run = tokio::time::timeout(Duration::from_secs(1), thread.start_turn("go"))
        .await
        .expect("start must not wait for model")
        .unwrap();
    assert_eq!(run.snapshot().await.unwrap().status, RunStatus::Running);
    assert!(thread.start_turn("overlap").await.is_err());
    run.cancel().await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), run.result())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.status, whale_protocol::TurnStatus::Interrupted);
    assert_eq!(run.snapshot().await.unwrap().status, RunStatus::Cancelled);
    let next = thread.start_turn("next").await.unwrap();
    next.cancel().await.unwrap();
    next.result().await.unwrap();
    client.close().await;
}

#[tokio::test]
async fn result_only_consumer_does_not_deadlock_on_thousand_events() {
    let client = client_with_provider(|_, _| {
        Ok(Box::pin(async_stream::try_stream! {
            for _ in 0..1000 {
                yield AgentStreamEvent::TextDelta { turn_id:"provider".into(), item_id:"item".into(), delta:"x".into() };
            }
            yield AgentStreamEvent::ItemCompleted { turn_id:"provider".into(), item:CanonicalItem::assistant_text("complete",MessagePhase::FinalAnswer) };
            yield AgentStreamEvent::TurnCompleted { turn_id:"provider".into(), thread_id:"provider".into(), usage:Default::default() };
        }))
    });
    let thread = client.create_thread("mock", None).await.unwrap();
    let run = thread.start_turn("go").await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), run.result())
        .await
        .expect("unused event buffer must not block result")
        .unwrap();
    assert_eq!(result.status, whale_protocol::TurnStatus::Completed);
    let retrieved = client.get_run(thread.id(), run.id()).await.unwrap();
    assert_eq!(retrieved.result().await.unwrap().turn_id, run.id());
    let mut events = run.events().unwrap();
    assert!(events.recv().await.is_err(), "lag must be explicit");
    client.close().await;
}

#[tokio::test]
async fn legacy_result_keeps_every_event_of_a_long_turn() {
    let client = client_with_provider(|_, _| {
        Ok(Box::pin(async_stream::try_stream! {
            for _ in 0..1000 {
                yield AgentStreamEvent::TextDelta { turn_id:"provider".into(), item_id:"item".into(), delta:"x".into() };
            }
            yield AgentStreamEvent::ItemCompleted { turn_id:"provider".into(), item:CanonicalItem::assistant_text("complete",MessagePhase::FinalAnswer) };
            yield AgentStreamEvent::TurnCompleted { turn_id:"provider".into(), thread_id:"provider".into(), usage:Default::default() };
        }))
    });
    let thread = client.create_thread("mock", None).await.unwrap();
    let (result, mut events) = tokio::time::timeout(Duration::from_secs(2), thread.run_turn("go"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.status, whale_protocol::TurnStatus::Completed);
    let mut deltas = 0;
    let mut terminals = 0;
    while let Some(event) = events.recv().await {
        match event {
            AgentStreamEvent::TextDelta { .. } => deltas += 1,
            AgentStreamEvent::TurnCompleted { .. } => terminals += 1,
            _ => {}
        }
    }
    assert_eq!(deltas, 1000);
    assert_eq!(terminals, 1);
    client.close().await;
}

struct ScopedTool {
    output: &'static str,
    approval: bool,
}
#[async_trait]
impl HostTool for ScopedTool {
    fn name(&self) -> &str {
        "lookup"
    }
    fn description(&self) -> &str {
        "session scoped test tool"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object","properties":{"target":{"type":"string"}}})
    }
    fn require_approval(&self) -> bool {
        self.approval
    }
    async fn execute(&self, args: Value) -> Result<CanonicalToolOutput, String> {
        Ok(CanonicalToolOutput::text(format!(
            "{}:{}",
            self.output,
            args["target"].as_str().unwrap_or("none")
        )))
    }
}

fn tool_client() -> WhaleClient {
    client_with_provider(|_, step| {
        Ok(Box::pin(async_stream::try_stream! {
            if step == 0 {
                yield AgentStreamEvent::ItemCompleted { turn_id:"provider".into(), item:CanonicalItem::tool_call("call",None,"lookup",Some(json!({"target":"original"})),"{\"target\":\"original\"}") };
            } else {
                yield AgentStreamEvent::ItemCompleted { turn_id:"provider".into(), item:CanonicalItem::assistant_text("finished",MessagePhase::FinalAnswer) };
            }
            yield AgentStreamEvent::TurnCompleted { turn_id:"provider".into(), thread_id:"provider".into(), usage:Default::default() };
        }))
    })
}

#[tokio::test]
async fn public_handle_approves_modified_arguments_without_core_access() {
    let client = tool_client();
    let thread = client.create_thread("mock", None).await.unwrap();
    thread
        .register_tool(Arc::new(ScopedTool {
            output: "scope",
            approval: true,
        }))
        .await
        .unwrap();
    let run = thread.start_turn("lookup").await.unwrap();
    let mut events = run.events().unwrap();
    let mut terminal_count = 0;
    while let Some(event) = tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .unwrap()
        .unwrap()
    {
        assert_eq!(event.turn_id, run.id());
        match event.payload {
            RunEventPayload::Stream {
                event: AgentStreamEvent::ApprovalRequested { request_id, .. },
            } => {
                let snapshot = run.snapshot().await.unwrap();
                assert_eq!(snapshot.status, RunStatus::WaitingApproval);
                assert_eq!(snapshot.pending_approvals.len(), 1);
                run.resolve_approval(
                    &request_id,
                    RunApprovalDecision::ModifyArguments,
                    Some(json!({"target":"approved"})),
                    None,
                )
                .await
                .unwrap();
            }
            RunEventPayload::Finished { snapshot } => {
                terminal_count += 1;
                assert_eq!(snapshot.status, RunStatus::Completed);
            }
            _ => {}
        }
    }
    assert_eq!(terminal_count, 1);
    assert!(run.result().await.unwrap().items.iter().any(|item| matches!(item,CanonicalItem::ToolResult{output:CanonicalToolOutput::Text{text},is_error:false,..} if text=="scope:approved")));
    client.close().await;
}

#[tokio::test]
async fn same_named_tools_are_bound_to_their_session() {
    let client = tool_client();
    let a = client.create_thread("mock", None).await.unwrap();
    let b = client.create_thread("mock", None).await.unwrap();
    a.register_tool(Arc::new(ScopedTool {
        output: "A",
        approval: false,
    }))
    .await
    .unwrap();
    b.register_tool(Arc::new(ScopedTool {
        output: "B",
        approval: false,
    }))
    .await
    .unwrap();
    let ar = a.start_turn("go").await.unwrap();
    let br = b.start_turn("go").await.unwrap();
    for (run, expected) in [(ar, "A:original"), (br, "B:original")] {
        assert!(run.result().await.unwrap().items.iter().any(|item| matches!(item,CanonicalItem::ToolResult{output:CanonicalToolOutput::Text{text},..} if text==expected)));
    }
    client.close().await;
}
