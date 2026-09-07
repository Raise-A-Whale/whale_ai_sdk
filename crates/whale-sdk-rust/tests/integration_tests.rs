use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use async_trait::async_trait;
use serde_json::json;
use whale_core::{AgentEngine, ApprovalGate, ThreadSession, ToolExecutionCoordinator, ToolRegistry};
use whale_daemon::DaemonServer;
use whale_protocol::canonical::{CanonicalItem, CanonicalToolOutput, MessagePhase};
use whale_protocol::events::{AgentStreamEvent, UsageMetrics};
use whale_protocol::rpc::ApprovalDecision;
use whale_sdk_rust::{HostTool, WhaleClient};

struct LocalEchoTool;

#[async_trait]
impl HostTool for LocalEchoTool {
    fn name(&self) -> &str {
        "local_echo"
    }

    fn description(&self) -> &str {
        "Echoes text back from local host environment"
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "msg": { "type": "string" }
            }
        })
    }

    async fn execute(&self, arguments: serde_json::Value) -> Result<CanonicalToolOutput, String> {
        let msg = arguments
            .get("msg")
            .and_then(|v| v.as_str())
            .unwrap_or("empty");
        Ok(CanonicalToolOutput::text(format!("Host echoing: {}", msg)))
    }
}

struct SensitiveHostTool;

#[async_trait]
impl HostTool for SensitiveHostTool {
    fn name(&self) -> &str {
        "sensitive_delete"
    }

    fn description(&self) -> &str {
        "Deletes a file, requires approval"
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "target": { "type": "string" }
            }
        })
    }

    fn require_approval(&self) -> bool {
        true
    }

    async fn execute(&self, arguments: serde_json::Value) -> Result<CanonicalToolOutput, String> {
        let target = arguments
            .get("target")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        Ok(CanonicalToolOutput::text(format!("Deleted {}", target)))
    }
}

#[tokio::test]
async fn test_sdk_in_process_session_and_reverse_rpc() {
    let registry = Arc::new(ToolRegistry::new());
    let approval_gate = Arc::new(ApprovalGate::new());
    let coordinator = Arc::new(ToolExecutionCoordinator::new(
        Arc::clone(&registry),
        Arc::clone(&approval_gate),
    ));

    // Custom engine mocking stream provider
    let step_counter = Arc::new(AtomicUsize::new(0));
    let sc = Arc::clone(&step_counter);

    let engine = Arc::new(AgentEngine::new(coordinator).with_stream_provider(Arc::new(
        move |_session: &ThreadSession, step: usize| {
            sc.fetch_add(1, Ordering::SeqCst);
            let s: whale_adapters::BoxedEventStream = Box::pin(async_stream::try_stream! {
                if step == 0 {
                    // Step 0: Request tool call to host reverse tool
                    let call = CanonicalItem::tool_call(
                        "call_host_echo_1",
                        None,
                        "local_echo",
                        Some(json!({ "msg": "Hello from LLM" })),
                        "{\"msg\":\"Hello from LLM\"}",
                    );
                    yield AgentStreamEvent::ItemCompleted {
                        turn_id: "turn_ipc".to_string(),
                        item: call,
                    };
                } else {
                    // Step 1: Complete reply
                    yield AgentStreamEvent::ItemCompleted {
                        turn_id: "turn_ipc".to_string(),
                        item: CanonicalItem::assistant_text("Finished echoing!", MessagePhase::FinalAnswer),
                    };
                    yield AgentStreamEvent::TurnCompleted {
                        turn_id: "turn_ipc".to_string(),
                        thread_id: "th_ipc".to_string(),
                        usage: UsageMetrics {
                            input_tokens: 100,
                            output_tokens: 50,
                            reasoning_tokens: 0,
                            cache_creation_input_tokens: 0,
                            cache_read_input_tokens: 0,
                        },
                    };
                }
            });
            Ok(s)
        },
    )));

    let daemon_server = Arc::new(DaemonServer::new(engine, approval_gate));
    let client = WhaleClient::in_process(daemon_server);

    // Create thread session
    let thread = client
        .create_thread("mock-claude", Some("System prompt".to_string()))
        .await
        .expect("create thread should succeed");

    assert!(!thread.id().is_empty());

    // Register host reverse tool
    thread
        .register_tool(Arc::new(LocalEchoTool))
        .await
        .expect("register host tool should succeed");

    // Run turn
    let (run_result, mut event_rx) = thread
        .run_turn("Please call echo tool")
        .await
        .expect("turn should run");

    // Collect streaming events
    let mut received_events = Vec::new();
    while let Ok(event) = tokio::time::timeout(std::time::Duration::from_millis(500), event_rx.recv()).await {
        match event {
            Some(ev) => received_events.push(ev),
            None => break,
        }
    }

    assert_eq!(step_counter.load(Ordering::SeqCst), 2);
    assert_eq!(run_result.usage.input_tokens, 100);
    assert_eq!(run_result.usage.output_tokens, 50);

    // Verify generated items include user input, tool call, tool result, and assistant message
    let has_echo_result = run_result.items.iter().any(|item| {
        if let CanonicalItem::ToolResult { output, .. } = item {
            if let CanonicalToolOutput::Text { text } = output {
                return text.contains("Host echoing: Hello from LLM");
            }
        }
        false
    });
    assert!(has_echo_result, "Expected tool result from host tool execution");
}

#[tokio::test]
async fn test_sdk_approval_resolution_loop() {
    let registry = Arc::new(ToolRegistry::new());
    let approval_gate = Arc::new(ApprovalGate::new());
    let coordinator = Arc::new(ToolExecutionCoordinator::new(
        Arc::clone(&registry),
        Arc::clone(&approval_gate),
    ));

    let gate_for_task = Arc::clone(&approval_gate);

    let engine = Arc::new(AgentEngine::new(coordinator).with_stream_provider(Arc::new(
        move |_session: &ThreadSession, step: usize| {
            let s: whale_adapters::BoxedEventStream = Box::pin(async_stream::try_stream! {
                if step == 0 {
                    let call = CanonicalItem::tool_call(
                        "call_del_1",
                        None,
                        "sensitive_delete",
                        Some(json!({ "target": "/etc/hosts" })),
                        "{\"target\":\"/etc/hosts\"}",
                    );
                    yield AgentStreamEvent::ItemCompleted {
                        turn_id: "turn_del".to_string(),
                        item: call,
                    };
                } else {
                    yield AgentStreamEvent::ItemCompleted {
                        turn_id: "turn_del".to_string(),
                        item: CanonicalItem::assistant_text("Operation concluded", MessagePhase::FinalAnswer),
                    };
                    yield AgentStreamEvent::TurnCompleted {
                        turn_id: "turn_del".to_string(),
                        thread_id: "th_del".to_string(),
                        usage: UsageMetrics::default(),
                    };
                }
            });
            Ok(s)
        },
    )));

    let daemon_server = Arc::new(DaemonServer::new(engine, Arc::clone(&approval_gate)));
    let client = WhaleClient::in_process(daemon_server);

    let thread = client
        .create_thread("mock-claude", None)
        .await
        .expect("create thread");

    thread
        .register_tool(Arc::new(SensitiveHostTool))
        .await
        .expect("register sensitive tool");

    let client_for_resolve = client.clone();
    // Spawn background task to poll approval gate and approve via client.resolve_approval
    tokio::spawn(async move {
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let ids = gate_for_task.pending_request_ids();
            if let Some(req_id) = ids.first() {
                let _ = client_for_resolve
                    .resolve_approval(req_id, ApprovalDecision::Approve, None)
                    .await;
                break;
            }
        }
    });

    let (run_res, mut event_rx) = thread.run_turn("delete target").await.unwrap();

    let mut has_approval_event = false;
    while let Ok(Some(ev)) = tokio::time::timeout(std::time::Duration::from_millis(200), event_rx.recv()).await {
        if let AgentStreamEvent::ApprovalRequested { .. } = ev {
            has_approval_event = true;
        }
    }

    assert!(has_approval_event, "Must receive ApprovalRequested stream event");
    assert!(run_res.items.len() >= 2);
    let has_approved_res = run_res.items.iter().any(|item| {
        if let CanonicalItem::ToolResult { output, is_error, .. } = item {
            if !is_error {
                if let CanonicalToolOutput::Text { text } = output {
                    return text.contains("Deleted /etc/hosts");
                }
            }
        }
        false
    });
    assert!(has_approved_res, "Expected sensitive tool execution to complete after approval");
}

#[tokio::test]
async fn test_sdk_stdio_process_loop() {
    // Test UDS transport between server and client
    let tmp_dir = std::env::temp_dir();
    let socket_path = tmp_dir.join(format!("whale_test_{}.sock", uuid::Uuid::new_v4()));

    let registry = Arc::new(ToolRegistry::new());
    let approval_gate = Arc::new(ApprovalGate::new());
    let coordinator = Arc::new(ToolExecutionCoordinator::new(
        registry,
        approval_gate,
    ));

    let engine = Arc::new(AgentEngine::new(coordinator).with_stream_provider(Arc::new(
        move |_session: &ThreadSession, _step: usize| {
            let s: whale_adapters::BoxedEventStream = Box::pin(async_stream::try_stream! {
                yield AgentStreamEvent::ItemCompleted {
                    turn_id: "uds_turn".to_string(),
                    item: CanonicalItem::assistant_text("Hello from UDS daemon!", MessagePhase::FinalAnswer),
                };
                yield AgentStreamEvent::TurnCompleted {
                    turn_id: "uds_turn".to_string(),
                    thread_id: "uds_th".to_string(),
                    usage: UsageMetrics::default(),
                };
            });
            Ok(s)
        },
    )));

    let daemon_server = Arc::new(DaemonServer::new(engine, Arc::new(ApprovalGate::new())));
    let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();

    let server_task = tokio::spawn({
        let server = Arc::clone(&daemon_server);
        async move {
            if let Ok((stream, _)) = listener.accept().await {
                let (read_half, write_half) = stream.into_split();
                let reader = tokio::io::BufReader::new(read_half);
                let lines_stream = tokio_stream::wrappers::LinesStream::new(tokio::io::AsyncBufReadExt::lines(reader));
                let transport = whale_daemon::UnixStreamWriter::new(write_half);
                let _ = server.run(lines_stream, transport).await;
            }
        }
    });

    // Connect client over UDS
    let client = WhaleClient::connect_uds(&socket_path)
        .await
        .expect("connect_uds should succeed");

    let thread = client
        .create_thread("mock-model", None)
        .await
        .expect("create thread over UDS");

    let (run_res, _rx) = thread.run_turn("hello UDS").await.expect("run turn over UDS");
    assert!(run_res.items.iter().any(|i| {
        if let CanonicalItem::AssistantMessage { content, .. } = i {
            if let whale_protocol::canonical::CanonicalContent::Text { text } = &content[0] {
                return text.contains("Hello from UDS daemon!");
            }
        }
        false
    }));

    let _ = server_task.abort();
    let _ = std::fs::remove_file(&socket_path);
}

#[tokio::test]
async fn test_sdk_spawn_daemon_stdio_subprocess() {
    // Path to the compiled whale-daemon debug binary in workspace
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let daemon_bin = manifest_dir
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("target")
        .join("debug")
        .join("whale-daemon");

    if !daemon_bin.exists() {
        eprintln!("whale-daemon binary not found at {:?}, skipping spawn test", daemon_bin);
        return;
    }

    let client = WhaleClient::spawn_daemon(&daemon_bin)
        .await
        .expect("spawn_daemon should succeed");

    let thread = client
        .create_thread("claude-3-7-sonnet", Some("You are a helpful assistant".to_string()))
        .await
        .expect("create thread over subprocess stdio");

    assert!(!thread.id().is_empty());
}


