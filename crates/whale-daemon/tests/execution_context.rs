mod common;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
use whale_daemon::{AnyTransportWriter, DaemonServer, OutgoingTransport};
use whale_protocol::{AgentStreamEvent, CanonicalItem};
struct Capture(mpsc::UnboundedSender<Value>);
#[async_trait]
impl OutgoingTransport for Capture {
    async fn send_line(&self, line: &str) -> std::io::Result<()> {
        self.0
            .send(serde_json::from_str(line).unwrap())
            .map_err(|_| std::io::Error::other("closed"))
    }
}
fn fixture() -> (
    DaemonServer,
    AnyTransportWriter,
    mpsc::UnboundedReceiver<Value>,
) {
    let gate = Arc::new(ApprovalGate::new());
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    )))
    .with_stream_provider(Arc::new(|session, step| {
        let prior_calls = session
            .history()
            .iter()
            .filter(|item| matches!(item, CanonicalItem::ToolCall { .. }))
            .count();
        let call_id = if prior_calls == 0 {
            "model-call".to_owned()
        } else {
            format!("model-call-{}", prior_calls + 1)
        };
        let mut events = if step == 0 {
            vec![Ok(AgentStreamEvent::ItemCompleted {
                turn_id: "provider-id".into(),
                item: CanonicalItem::tool_call(
                    call_id,
                    None,
                    "lookup",
                    Some(json!({"query":"original"})),
                    "{}",
                ),
            })]
        } else {
            vec![]
        };
        events.push(Ok(AgentStreamEvent::TurnCompleted {
            turn_id: "provider".into(),
            thread_id: "provider".into(),
            usage: Default::default(),
        }));
        Ok(Box::pin(futures::stream::iter(events)))
    }));
    let (tx, rx) = mpsc::unbounded_channel();
    (
        DaemonServer::new(Arc::new(engine), gate),
        AnyTransportWriter::new(Arc::new(Capture(tx))),
        rx,
    )
}
async fn send(s: &DaemonServer, w: &AnyTransportWriter, id: u64, method: &str, params: Value) {
    s.handle_message(
        &json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}).to_string(),
        w,
    )
    .await;
}
async fn until(
    rx: &mut mpsc::UnboundedReceiver<Value>,
    predicate: impl Fn(&Value) -> bool,
) -> Value {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let v = rx.recv().await.unwrap();
            if predicate(&v) {
                return v;
            }
        }
    })
    .await
    .unwrap()
}
async fn start(
    s: &DaemonServer,
    w: &AnyTransportWriter,
    rx: &mut mpsc::UnboundedReceiver<Value>,
    policy: Value,
) {
    send(s,w,1,"session.start_thread",json!({"session_id":"s","agent_name":"worker","model":"test","context_policy":policy,"tools":[{"name":"lookup","description":"Lookup","parameters":{"type":"object","properties":{"query":{"type":"string"}},"required":["query"]},"is_host_tool":true}]})).await;
    assert!(until(rx, |v| v["id"] == 1).await.get("error").is_none());
    send(
        s,
        w,
        2,
        "thread.start_turn",
        json!({"thread_id":"s","turn_id":"r","input_items":[],"timeout_ms":5000}),
    )
    .await;
    assert!(until(rx, |v| v["id"] == 2).await.get("error").is_none());
}
#[tokio::test]
async fn host_context_progress_owner_and_cancellation() {
    let (s, w, mut rx) = fixture();
    common::initialize(&s, &w, Some(&mut rx)).await;
    start(&s, &w, &mut rx, json!({"type":"full_history"})).await;
    let request = until(&mut rx, |v| v["method"] == "tool.execute_host").await;
    assert_eq!(request["params"]["context"]["call_id"], "model-call");
    assert_eq!(request["params"]["context"]["thread_id"], "s");
    assert_eq!(request["params"]["context"]["turn_id"], "r");
    assert_eq!(request["params"]["context"]["agent_name"], "worker");
    assert!(request["params"]["context"]["deadline_unix_ms"].is_u64());
    let call = request["params"]["call_id"].clone();
    let (_, foreign, mut foreign_rx) = fixture();
    common::initialize(&s, &foreign, Some(&mut foreign_rx)).await;
    send(
        &s,
        &foreign,
        3,
        "tool.report_progress",
        json!({"call_id":call,"message":"foreign","progress":0.2}),
    )
    .await;
    assert_eq!(
        until(&mut foreign_rx, |v| v["id"] == 3).await["result"]["accepted"],
        false
    );
    send(
        &s,
        &w,
        4,
        "tool.report_progress",
        json!({"call_id":call,"message":"half","progress":0.5}),
    )
    .await;
    assert_eq!(
        until(&mut rx, |v| v["id"] == 4).await["result"]["accepted"],
        true
    );
    let event = until(&mut rx, |v| v["params"]["event"]["type"] == "tool_progress").await;
    assert_eq!(event["params"]["event"]["call_id"], "model-call");
    send(
        &s,
        &w,
        5,
        "turn.cancel",
        json!({"thread_id":"s","turn_id":"r"}),
    )
    .await;
    until(&mut rx, |v| v["id"] == 5).await;
    let cancel = until(&mut rx, |v| v["method"] == "tool.cancel_host").await;
    assert_eq!(cancel["params"]["call_id"], call);
    send(
        &s,
        &w,
        6,
        "tool.report_progress",
        json!({"call_id":call,"message":"late"}),
    )
    .await;
    assert_eq!(
        until(&mut rx, |v| v["id"] == 6).await["result"]["accepted"],
        false
    );
    assert!(s.pending_host_tool_calls().is_empty());
}
#[tokio::test]
async fn host_context_policy_request_and_cancel_are_correlated() {
    let (s, w, mut rx) = fixture();
    common::initialize(&s, &w, Some(&mut rx)).await;
    start(&s, &w, &mut rx, json!({"type":"host"})).await;
    let request = until(&mut rx, |v| v["method"] == "context.build_host").await;
    assert_eq!(request["params"]["context"]["turn_id"], "r");
    assert_eq!(request["params"]["step_index"], 0);
    send(
        &s,
        &w,
        5,
        "turn.cancel",
        json!({"thread_id":"s","turn_id":"r"}),
    )
    .await;
    until(&mut rx, |v| v["id"] == 5).await;
    let cancel = until(&mut rx, |v| v["method"] == "context.cancel_host").await;
    assert_eq!(cancel["params"]["request_id"], request["id"]);
}
#[tokio::test]
async fn invalid_schemas_and_policy_do_not_reserve_session() {
    let (s, w, mut rx) = fixture();
    common::initialize(&s, &w, Some(&mut rx)).await;
    for (index, parameters) in [
        json!({"type":23}),
        json!({"$ref":"https://example.invalid/schema"}),
        json!({"$ref":"file:///tmp/schema"}),
    ]
    .into_iter()
    .enumerate()
    {
        send(&s,&w,index as u64,"session.start_thread",json!({"session_id":"s","model":"test","tools":[{"name":"lookup","description":"bad","parameters":parameters,"is_host_tool":true}]})).await;
        assert!(until(&mut rx, |v| v["id"] == index as u64)
            .await
            .get("error")
            .is_some());
        assert!(!s.sessions().contains_key("s"));
    }
    send(&s,&w,10,"session.start_thread",json!({"session_id":"s","model":"test","context_policy":{"type":"recent_turns","max_turns":0}})).await;
    assert!(until(&mut rx, |v| v["id"] == 10)
        .await
        .get("error")
        .is_some());
    assert!(!s.sessions().contains_key("s"));
    send(
        &s,
        &w,
        11,
        "session.start_thread",
        json!({"session_id":"s","model":"test"}),
    )
    .await;
    assert!(until(&mut rx, |v| v["id"] == 11)
        .await
        .get("error")
        .is_none());
}

#[tokio::test]
async fn successful_host_callback_is_not_cancelled_and_late_progress_is_rejected() {
    let (s, w, mut rx) = fixture();
    common::initialize(&s, &w, Some(&mut rx)).await;
    start(&s, &w, &mut rx, json!({"type":"full_history"})).await;
    let request = until(&mut rx, |v| v["method"] == "tool.execute_host").await;
    let call = request["params"]["call_id"].clone();
    send(
        &s,
        &w,
        10,
        "tool.report_progress",
        json!({"call_id":call,"message":"invalid","progress":1.1}),
    )
    .await;
    assert!(until(&mut rx, |v| v["id"] == 10)
        .await
        .get("error")
        .is_some());
    s.handle_message(&json!({"jsonrpc":"2.0","id":request["id"],"result":{"call_id":call,"output":{"type":"text","text":"done"},"is_error":false}}).to_string(),&w).await;
    let finished = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let v = rx.recv().await.unwrap();
            assert_ne!(
                v["method"], "tool.cancel_host",
                "successful tool received cancellation"
            );
            if v["params"]["type"] == "finished" {
                break v;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(finished["params"]["snapshot"]["status"], "completed");
    assert_eq!(
        finished["params"]["snapshot"]["tool_executions"][0]["arguments"]["query"],
        "original"
    );
    send(
        &s,
        &w,
        11,
        "tool.report_progress",
        json!({"call_id":call,"message":"late"}),
    )
    .await;
    assert_eq!(
        until(&mut rx, |v| v["id"] == 11).await["result"]["accepted"],
        false
    );
}
#[tokio::test]
async fn context_reply_owner_validation_and_malformed_projection_fail_before_tool_dispatch() {
    let (s, w, mut rx) = fixture();
    common::initialize(&s, &w, Some(&mut rx)).await;
    start(&s, &w, &mut rx, json!({"type":"host"})).await;
    let request = until(&mut rx, |v| v["method"] == "context.build_host").await;
    let (_, foreign, mut foreign_rx) = fixture();
    common::initialize(&s, &foreign, Some(&mut foreign_rx)).await;
    let good =
        json!({"jsonrpc":"2.0","id":request["id"],"result":{"system_prompt":null,"items":[]}});
    s.handle_message(&good.to_string(), &foreign).await;
    send(
        &s,
        &w,
        10,
        "turn.get",
        json!({"thread_id":"s","turn_id":"r"}),
    )
    .await;
    assert_eq!(
        until(&mut rx, |v| v["id"] == 10).await["result"]["status"],
        "running"
    );
    let orphan = CanonicalItem::tool_result(
        "orphan",
        whale_protocol::CanonicalToolOutput::text("bad"),
        false,
    );
    s.handle_message(&json!({"jsonrpc":"2.0","id":request["id"],"result":{"system_prompt":null,"items":[orphan]}}).to_string(),&w).await;
    let final_event = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let v = rx.recv().await.unwrap();
            assert_ne!(v["method"], "tool.execute_host");
            assert_ne!(v["method"], "context.cancel_host");
            if v["params"]["type"] == "finished" {
                break v;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(final_event["params"]["snapshot"]["status"], "failed");
    assert!(final_event["params"]["snapshot"]["error"]["message"]
        .as_str()
        .unwrap()
        .contains("orphan"));
}
#[tokio::test]
async fn batch_registration_validates_all_before_publishing() {
    let (s, w, mut rx) = fixture();
    common::initialize(&s, &w, Some(&mut rx)).await;
    send(
        &s,
        &w,
        1,
        "session.start_thread",
        json!({"session_id":"s","model":"test"}),
    )
    .await;
    until(&mut rx, |v| v["id"] == 1).await;
    send(&s,&w,2,"session.register_tools",json!({"thread_id":"s","tools":[{"name":"good","description":"good","parameters":{},"is_host_tool":true},{"name":"bad","description":"bad","parameters":{"type":22},"is_host_tool":true}]})).await;
    assert!(until(&mut rx, |v| v["id"] == 2)
        .await
        .get("error")
        .is_some());
    let session = s.sessions().get("s").unwrap().value().clone();
    assert!(!session.lock().await.tools().contains("good"));
}

#[tokio::test]
async fn approved_arguments_are_audited_and_invalid_modification_never_reaches_host() {
    for (arguments, valid) in [
        (json!({"query":"changed"}), true),
        (json!({"query":42}), false),
    ] {
        let (s, w, mut rx) = fixture();
        common::initialize(&s, &w, Some(&mut rx)).await;
        send(&s,&w,1,"session.start_thread",json!({"session_id":"s","model":"test","tools":[{"name":"lookup","description":"Lookup","parameters":{"type":"object","properties":{"query":{"type":"string"}},"required":["query"]},"is_host_tool":true,"require_approval":true}]})).await;
        until(&mut rx, |v| v["id"] == 1).await;
        send(
            &s,
            &w,
            2,
            "thread.start_turn",
            json!({"thread_id":"s","turn_id":"r","input_items":[]}),
        )
        .await;
        until(&mut rx, |v| v["id"] == 2).await;
        let approval = until(&mut rx, |v| {
            v["params"]["event"]["type"] == "approval_requested"
        })
        .await;
        send(&s,&w,3,"turn.resolve_approval",json!({"thread_id":"s","turn_id":"r","request_id":approval["params"]["event"]["request_id"],"decision":"modify_arguments","arguments":arguments})).await;
        assert!(until(&mut rx, |v| v["id"] == 3)
            .await
            .get("error")
            .is_none());
        let mut host_calls = 0;
        let snapshot=tokio::time::timeout(Duration::from_secs(2),async{loop{let event=rx.recv().await.unwrap();if event["method"]=="tool.execute_host" {host_calls+=1;assert!(valid,"invalid approved arguments reached host");assert_eq!(event["params"]["arguments"],arguments);s.handle_message(&json!({"jsonrpc":"2.0","id":event["id"],"result":{"call_id":event["params"]["call_id"],"output":{"type":"text","text":"done"},"is_error":false}}).to_string(),&w).await;}if event["params"]["type"]=="finished"{break event["params"]["snapshot"].clone();}}}).await.unwrap();
        assert_eq!(snapshot["status"], "completed");
        assert_eq!(host_calls, usize::from(valid));
        if valid {
            assert_eq!(
                snapshot["tool_executions"][0]["original_arguments"],
                json!({"query":"original"})
            );
            assert_eq!(snapshot["tool_executions"][0]["arguments"], arguments);
        } else {
            assert!(snapshot["tool_executions"].as_array().unwrap().is_empty());
            assert!(snapshot["items"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["type"] == "tool_result" && item["is_error"] == true));
        }
    }
}
#[tokio::test]
async fn disconnect_cancels_context_callbacks_and_releases_owned_sessions() {
    let (s, w, mut rx) = fixture();
    common::initialize(&s, &w, Some(&mut rx)).await;
    start(&s, &w, &mut rx, json!({"type":"host"})).await;
    let request = until(&mut rx, |v| v["method"] == "context.build_host").await;
    s.disconnect_connection(&w).await;
    let cancel = until(&mut rx, |v| v["method"] == "context.cancel_host").await;
    assert_eq!(cancel["params"]["request_id"], request["id"]);
    assert!(s.sessions().is_empty());
    assert!(s.pending_host_tool_calls().is_empty());
    s.handle_message(
        &json!({"jsonrpc":"2.0","id":request["id"],"result":{"system_prompt":null,"items":[]}})
            .to_string(),
        &w,
    )
    .await;
    assert!(s.sessions().is_empty());
}

#[tokio::test]
async fn reverse_tool_request_echoes_captured_binding_version() {
    let (server, writer, mut receiver) = fixture();
    common::initialize(&server, &writer, Some(&mut receiver)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({
            "session_id": "versioned", "model": "test", "tools": [{
                "name": "lookup", "binding_id": "binding-A", "description": "Lookup A",
                "parameters": {}, "is_host_tool": true
            }]
        }),
    )
    .await;
    assert!(until(&mut receiver, |value| value["id"] == 1)
        .await
        .get("error")
        .is_none());
    send(
        &server,
        &writer,
        2,
        "thread.start_turn",
        json!({
            "thread_id": "versioned", "turn_id": "turn-A", "input_items": []
        }),
    )
    .await;
    until(&mut receiver, |value| value["id"] == 2).await;
    let old_request = until(&mut receiver, |value| {
        value["method"] == "tool.execute_host"
    })
    .await;
    assert_eq!(old_request["params"]["binding_id"], "binding-A");
    let registration = {
        let server = server.clone();
        let writer = writer.clone();
        tokio::spawn(async move {
            send(
                &server,
                &writer,
                3,
                "session.register_tools",
                json!({
                    "thread_id": "versioned", "tools": [{
                        "name": "lookup", "binding_id": "binding-B", "description": "Lookup B",
                        "parameters": {}, "is_host_tool": true
                    }]
                }),
            )
            .await;
        })
    };
    tokio::task::yield_now().await;
    assert!(
        !registration.is_finished(),
        "registration must wait for the running session"
    );
    server.handle_message(&json!({"jsonrpc":"2.0", "id": old_request["id"], "result": {
        "call_id": old_request["params"]["call_id"], "output": {"type":"text", "text":"A"}, "is_error":false
    }}).to_string(), &writer).await;
    registration.await.unwrap();
    assert!(until(&mut receiver, |value| value["id"] == 3)
        .await
        .get("error")
        .is_none());
    send(
        &server,
        &writer,
        4,
        "thread.start_turn",
        json!({
            "thread_id": "versioned", "turn_id": "turn-B", "input_items": []
        }),
    )
    .await;
    until(&mut receiver, |value| value["id"] == 4).await;
    let new_request = until(&mut receiver, |value| {
        value["method"] == "tool.execute_host"
    })
    .await;
    assert_eq!(new_request["params"]["binding_id"], "binding-B");
    assert_eq!(old_request["params"]["binding_id"], "binding-A");
    server.disconnect_connection(&writer).await;
}

#[tokio::test]
async fn empty_binding_version_is_rejected_without_partial_registration() {
    let (server, writer, mut receiver) = fixture();
    common::initialize(&server, &writer, Some(&mut receiver)).await;
    for (id, binding) in [(1, ""), (2, "   ")] {
        send(&server, &writer, id, "session.start_thread", json!({
            "session_id":"versioned", "model":"test", "tools":[{
                "name":"lookup", "binding_id":binding, "description":"Lookup", "parameters":{}, "is_host_tool":true
            }]
        })).await;
        assert!(until(&mut receiver, |value| value["id"] == id)
            .await
            .get("error")
            .is_some());
        assert!(!server.sessions().contains_key("versioned"));
    }
    send(
        &server,
        &writer,
        3,
        "session.start_thread",
        json!({"session_id":"versioned", "model":"test"}),
    )
    .await;
    assert!(until(&mut receiver, |value| value["id"] == 3)
        .await
        .get("error")
        .is_none());
    send(&server, &writer, 4, "session.register_tools", json!({
        "thread_id":"versioned", "tools":[
            {"name":"valid", "binding_id":"valid-version", "description":"Valid", "parameters":{}, "is_host_tool":true},
            {"name":"invalid", "binding_id":"", "description":"Invalid", "parameters":{}, "is_host_tool":true}
        ]
    })).await;
    assert!(until(&mut receiver, |value| value["id"] == 4)
        .await
        .get("error")
        .is_some());
    let session = server.sessions().get("versioned").unwrap().value().clone();
    assert!(!session.lock().await.tools().contains("valid"));
}
