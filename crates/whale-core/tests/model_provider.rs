use async_trait::async_trait;
use futures::{stream, StreamExt};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, Notify};
use whale_adapters::SamplingOptions;
use whale_core::model::{ModelError, ModelEvent, ModelEventStream, ModelProvider, ModelRequest};
use whale_core::provider::ProviderRegistry;
use whale_core::{
    AgentEngine, ApprovalGate, CancellationToken, ContextPolicy, ThreadSession,
    ToolExecutionCoordinator, ToolHandler, ToolRegistry,
};
use whale_protocol::contexts::{ContextBuildRequest, ModelContext, RunContextInfo};
use whale_protocol::events::{AgentStreamEvent, UsageMetrics};
use whale_protocol::models::ModelCapabilities;
use whale_protocol::{CanonicalItem, CanonicalToolOutput, MessagePhase};

#[derive(Clone)]
enum Behavior {
    Loop,
    Creation,
    FirstEvent,
    MidStream,
    Empty,
    Truncated,
    Duplicate,
    AfterFinished,
    ErrorAfterFinished,
    Forbidden,
    DuplicateCall,
    DuplicateItem,
    UnfinishedTool,
    UnfinishedReasoning,
}
struct Provider {
    behavior: Behavior,
    requests: Mutex<Vec<ModelRequest>>,
    tokens: Mutex<Vec<CancellationToken>>,
    entered: Notify,
}
impl Provider {
    fn new(behavior: Behavior) -> Arc<Self> {
        Arc::new(Self {
            behavior,
            requests: Mutex::new(vec![]),
            tokens: Mutex::new(vec![]),
            entered: Notify::new(),
        })
    }
}
fn finish() -> ModelEvent {
    ModelEvent::StepFinished {
        usage: UsageMetrics {
            input_tokens: 2,
            output_tokens: 3,
            ..Default::default()
        },
    }
}
fn answer() -> ModelEvent {
    ModelEvent::ItemCompleted {
        item: CanonicalItem::assistant_text("done", MessagePhase::FinalAnswer),
    }
}
#[async_trait]
impl ModelProvider for Provider {
    fn capabilities(&self, model: &str) -> Result<ModelCapabilities, ModelError> {
        if model == "unknown" {
            return Err(ModelError::InvalidRequest("unknown model".into()));
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
        let step = request.step_index;
        self.requests.lock().unwrap().push(request);
        self.tokens.lock().unwrap().push(cancellation);
        self.entered.notify_one();
        Ok(match self.behavior {
            Behavior::Creation => futures::future::pending().await,
            Behavior::FirstEvent => Box::pin(stream::pending()),
            Behavior::MidStream => {
                Box::pin(stream::iter(vec![Ok(answer())]).chain(stream::pending()))
            }
            Behavior::Empty => Box::pin(stream::empty()),
            Behavior::Truncated => Box::pin(stream::iter(vec![Ok(answer())])),
            Behavior::Duplicate => Box::pin(stream::iter(vec![Ok(finish()), Ok(finish())])),
            Behavior::AfterFinished => Box::pin(stream::iter(vec![Ok(finish()), Ok(answer())])),
            Behavior::ErrorAfterFinished => Box::pin(stream::iter(vec![
                Ok(finish()),
                Err(ModelError::Stream("late error".into())),
            ])),
            Behavior::Forbidden => Box::pin(stream::iter(vec![
                Ok(ModelEvent::ItemCompleted {
                    item: CanonicalItem::user_text("forged"),
                }),
                Ok(finish()),
            ])),
            Behavior::DuplicateCall => Box::pin(stream::iter(vec![
                Ok(ModelEvent::ItemCompleted {
                    item: CanonicalItem::tool_call(
                        "duplicate",
                        None,
                        "lookup",
                        Some(json!({})),
                        "{}",
                    ),
                }),
                Ok(ModelEvent::ItemCompleted {
                    item: CanonicalItem::tool_call(
                        "duplicate",
                        None,
                        "lookup",
                        Some(json!({})),
                        "{}",
                    ),
                }),
                Ok(finish()),
            ])),
            Behavior::DuplicateItem => {
                let item = answer();
                Box::pin(stream::iter(vec![Ok(item.clone()), Ok(item), Ok(finish())]))
            }
            Behavior::UnfinishedTool => Box::pin(stream::iter(vec![
                Ok(ModelEvent::ToolCallDelta {
                    item_id: "unfinished".into(),
                    call_id: "call".into(),
                    delta: "{".into(),
                }),
                Ok(finish()),
            ])),
            Behavior::UnfinishedReasoning => Box::pin(stream::iter(vec![
                Ok(ModelEvent::ItemStarted {
                    item_id: "unfinished".into(),
                    item_type: "reasoning".into(),
                    phase: None,
                }),
                Ok(finish()),
            ])),
            Behavior::Loop => {
                let item = if step == 0 {
                    ModelEvent::ItemCompleted {
                        item: CanonicalItem::tool_call(
                            "call",
                            None,
                            "lookup",
                            Some(json!({})),
                            "{}",
                        ),
                    }
                } else {
                    answer()
                };
                Box::pin(stream::iter(vec![Ok(item), Ok(finish())]))
            }
        })
    }
}
struct Tool;
#[async_trait]
impl ToolHandler for Tool {
    fn name(&self) -> &str {
        "lookup"
    }
    fn description(&self) -> &str {
        "lookup"
    }
    fn parameters(&self) -> serde_json::Value {
        json!({"type":"object"})
    }
    fn supports_parallel(&self) -> bool {
        false
    }
    fn require_approval(&self) -> bool {
        false
    }
    async fn execute(&self, _: serde_json::Value) -> Result<CanonicalToolOutput, String> {
        Ok(CanonicalToolOutput::text("found"))
    }
}
struct Projection;
#[async_trait]
impl ContextPolicy for Projection {
    async fn build(
        &self,
        request: ContextBuildRequest,
        _: CancellationToken,
    ) -> Result<ModelContext, String> {
        Ok(ModelContext {
            system_prompt: Some("projected".into()),
            items: request.history[2..].to_vec(),
        })
    }
}
fn setup(provider: Arc<dyn ModelProvider>, id: &str) -> (AgentEngine, ThreadSession) {
    let tools = Arc::new(ToolRegistry::new());
    tools.register(Arc::new(Tool)).unwrap();
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        tools.clone(),
        Arc::new(ApprovalGate::new()),
    )));
    let session = ThreadSession::with_id_prompt_and_provider(
        id,
        Some("original".into()),
        provider,
        tools,
        SamplingOptions::new("local"),
    );
    (engine, session)
}
fn context(id: &str) -> RunContextInfo {
    RunContextInfo {
        agent_name: Some("agent".into()),
        thread_id: id.into(),
        turn_id: "outer".into(),
        deadline_unix_ms: None,
    }
}
async fn run(
    engine: &AgentEngine,
    session: &mut ThreadSession,
    token: CancellationToken,
) -> Result<whale_core::RunTurnResult, whale_core::CoreError> {
    let (tx, mut rx) = mpsc::channel(64);
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    engine
        .run_turn_with_context(
            session,
            context(session.id()),
            token,
            vec![CanonicalItem::user_text("new")],
            3,
            tx,
            Arc::default(),
        )
        .await
}
#[tokio::test]
async fn native_provider_receives_only_projection_and_unique_step_identity() {
    let provider = Provider::new(Behavior::Loop);
    let (engine, mut session) = setup(provider.clone(), "session");
    session.append_items([
        CanonicalItem::user_text("old"),
        CanonicalItem::assistant_text("old response", MessagePhase::FinalAnswer),
    ]);
    session.set_context_policy(Arc::new(Projection));
    let result = run(&engine, &mut session, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(result.steps_taken, 2);
    assert_eq!(result.total_usage.input_tokens, 4);
    let requests = provider.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].model_context.system_prompt.as_deref(),
        Some("projected")
    );
    assert_eq!(requests[0].model_context.items.len(), 1);
    assert_eq!(requests[1].model_context.items.len(), 3);
    assert!(
        matches!(&requests[1].model_context.items[2],CanonicalItem::ToolResult{call_id,output,..} if call_id=="call" && output==&CanonicalToolOutput::text("found"))
    );
    assert_ne!(requests[0].step_id, requests[1].step_id);
    assert!(!requests[0].step_id.is_empty());
    assert_eq!(requests[0].context, context("session"));
    assert_eq!(requests[1].step_index, 1);
    assert_eq!(requests[0].tools[0].name, "lookup");
    assert_eq!(session.history().len(), 6);
    assert!(session.adapter().is_none());
}
#[tokio::test]
async fn malformed_native_streams_fail_with_one_outer_terminal() {
    for behavior in [
        Behavior::Empty,
        Behavior::Truncated,
        Behavior::Duplicate,
        Behavior::AfterFinished,
        Behavior::ErrorAfterFinished,
        Behavior::Forbidden,
    ] {
        let provider = Provider::new(behavior);
        let (engine, mut session) = setup(provider, "session");
        let (tx, mut rx) = mpsc::channel(64);
        let result = engine
            .run_turn_with_context(
                &mut session,
                context("session"),
                CancellationToken::new(),
                vec![CanonicalItem::user_text("new")],
                3,
                tx,
                Arc::default(),
            )
            .await;
        assert!(result.is_err());
        let mut terminals = vec![];
        while let Some(event) = rx.recv().await {
            if matches!(
                event,
                AgentStreamEvent::TurnFailed { .. } | AgentStreamEvent::TurnCompleted { .. }
            ) {
                terminals.push(event);
            }
        }
        assert_eq!(terminals.len(), 1);
        assert!(matches!(terminals[0], AgentStreamEvent::TurnFailed { .. }));
        assert!(!session.history().iter().any(|item|matches!(item,CanonicalItem::UserMessage{content,..} if format!("{content:?}").contains("forged"))));
    }
}
#[tokio::test]
async fn cancellation_covers_stream_creation_first_event_and_mid_stream() {
    for behavior in [
        Behavior::Creation,
        Behavior::FirstEvent,
        Behavior::MidStream,
    ] {
        let provider = Provider::new(behavior);
        let (engine, mut session) = setup(provider.clone(), "session");
        let token = CancellationToken::new();
        let cancel = token.clone();
        let task = tokio::spawn(async move { run(&engine, &mut session, token).await });
        provider.entered.notified().await;
        cancel.cancel();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        assert!(provider.tokens.lock().unwrap()[0].is_cancelled());
    }
}
#[tokio::test]
async fn dropping_one_run_does_not_cancel_shared_provider_other_session() {
    let provider = Provider::new(Behavior::FirstEvent);
    let mut tasks = vec![];
    for id in ["a", "b"] {
        let (engine, mut session) = setup(provider.clone(), id);
        tasks.push(tokio::spawn(async move {
            run(&engine, &mut session, CancellationToken::new()).await
        }));
        provider.entered.notified().await;
    }
    tasks[0].abort();
    let _ = (&mut tasks[0]).await;
    {
        let tokens = provider.tokens.lock().unwrap();
        assert!(tokens[0].is_cancelled());
        assert!(!tokens[1].is_cancelled());
    }
    tasks[1].abort();
    let _ = (&mut tasks[1]).await;
    assert!(provider.tokens.lock().unwrap()[1].is_cancelled());
}
#[test]
fn registry_rejects_duplicate_unknown_reference_and_unknown_model() {
    let provider = Provider::new(Behavior::Loop);
    let mut registry = ProviderRegistry::new();
    registry
        .register_provider("local", provider.clone())
        .unwrap();
    assert!(registry
        .register_provider("local", provider.clone())
        .is_err());
    assert!(registry.register_provider(" bad", provider).is_err());
    assert!(registry.resolve("missing", "local").is_err());
    assert!(registry.resolve("local", "unknown").is_err());
    assert!(registry.resolve("local", "local").is_ok());
}

struct DropObservation {
    token: CancellationToken,
    observed: Arc<Mutex<Option<bool>>>,
}
impl Drop for DropObservation {
    fn drop(&mut self) {
        *self.observed.lock().unwrap() = Some(self.token.is_cancelled());
    }
}
struct DropProvider {
    creation: bool,
    observed: Arc<Mutex<Option<bool>>>,
    entered: Notify,
}
#[async_trait]
impl ModelProvider for DropProvider {
    fn capabilities(&self, _: &str) -> Result<ModelCapabilities, ModelError> {
        let mut caps = ModelCapabilities::text_only();
        caps.tool_calls = true;
        Ok(caps)
    }
    async fn stream(
        &self,
        _: ModelRequest,
        token: CancellationToken,
    ) -> Result<ModelEventStream, ModelError> {
        let probe = DropObservation {
            token,
            observed: self.observed.clone(),
        };
        if self.creation {
            let _probe = probe;
            self.entered.notify_one();
            futures::future::pending().await
        } else {
            struct Pending(DropObservation);
            impl futures::Stream for Pending {
                type Item = Result<ModelEvent, ModelError>;
                fn poll_next(
                    self: std::pin::Pin<&mut Self>,
                    _: &mut std::task::Context<'_>,
                ) -> std::task::Poll<Option<Self::Item>> {
                    let _ = &self.0;
                    std::task::Poll::Pending
                }
            }
            self.entered.notify_one();
            Ok(Box::pin(Pending(probe)))
        }
    }
}
#[tokio::test]
async fn provider_inner_drop_observes_cancelled_during_creation_and_consumption() {
    for creation in [true, false] {
        let observed = Arc::new(Mutex::new(None));
        let provider = Arc::new(DropProvider {
            creation,
            observed: observed.clone(),
            entered: Notify::new(),
        });
        let (engine, mut session) = setup(provider.clone(), "session");
        let task =
            tokio::spawn(async move { run(&engine, &mut session, CancellationToken::new()).await });
        provider.entered.notified().await;
        task.abort();
        let _ = task.await;
        assert_eq!(*observed.lock().unwrap(), Some(true));
    }
}
#[tokio::test]
async fn unsupported_native_inputs_and_options_never_invoke_stream() {
    use whale_protocol::CanonicalContent;
    for option in [true, false] {
        let provider = Provider::new(Behavior::Loop);
        let (engine, mut session) = setup(provider.clone(), "session");
        if option {
            session.sampling_options_mut().reasoning_effort = Some("high".into());
        } else {
            session.append_item(CanonicalItem::UserMessage {
                id: "audio".into(),
                content: vec![CanonicalContent::Audio {
                    mime_type: "audio/wav".into(),
                    data: Some("AA==".into()),
                    uri: None,
                }],
            });
        }
        assert!(run(&engine, &mut session, CancellationToken::new())
            .await
            .is_err());
        assert!(provider.requests.lock().unwrap().is_empty());
    }
}
#[tokio::test]
async fn compatibility_override_receives_projection_only() {
    let tools = Arc::new(ToolRegistry::new());
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        tools.clone(),
        Arc::new(ApprovalGate::new()),
    )))
    .with_stream_provider(Arc::new(|view, _| {
        assert_eq!(view.system_prompt(), Some("projected"));
        assert_eq!(view.history().len(), 1);
        Ok(Box::pin(stream::iter(vec![Ok(
            AgentStreamEvent::TurnCompleted {
                turn_id: "provider".into(),
                thread_id: "provider".into(),
                usage: UsageMetrics::default(),
            },
        )])))
    }));
    let mut session = ThreadSession::with_id_and_prompt(
        "session",
        Some("original".into()),
        Arc::new(whale_adapters::OpenAIAdapter::new("")),
        tools,
        SamplingOptions::new("model"),
    );
    session.append_items([
        CanonicalItem::user_text("old"),
        CanonicalItem::assistant_text("old", MessagePhase::FinalAnswer),
    ]);
    session.set_context_policy(Arc::new(Projection));
    run(&engine, &mut session, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(session.history().len(), 3);
}
#[tokio::test]
async fn unsupported_http_content_is_rejected_before_any_connection() {
    use whale_adapters::{AnthropicAdapter, OpenAIAdapter, OpenAIWireApi, ProtocolAdapter};
    use whale_core::http_provider::HttpModelProvider;
    use whale_protocol::CanonicalContent;
    for api in ["chat", "anthropic", "responses"] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let adapter: Arc<dyn ProtocolAdapter> = match api {
            "anthropic" => Arc::new(AnthropicAdapter::with_base_url("", base)),
            "responses" => Arc::new(OpenAIAdapter::with_options(
                "",
                base,
                OpenAIWireApi::Responses,
            )),
            _ => Arc::new(OpenAIAdapter::with_options(
                "",
                base,
                OpenAIWireApi::ChatCompletions,
            )),
        };
        let provider = HttpModelProvider::new(adapter);
        let items = vec![CanonicalItem::UserMessage {
            id: "audio".into(),
            content: vec![CanonicalContent::Audio {
                mime_type: "audio/wav".into(),
                data: Some("AA==".into()),
                uri: None,
            }],
        }];
        let request = ModelRequest {
            context: context("s"),
            step_index: 0,
            step_id: "step".into(),
            model_context: ModelContext {
                system_prompt: None,
                items,
            },
            tools: vec![],
            options: SamplingOptions::new("m"),
        };
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            provider.stream(request, CancellationToken::new()),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(ModelError::UnsupportedCapability(_))));
        assert!(
            matches!(listener.accept(),Err(error) if error.kind()==std::io::ErrorKind::WouldBlock)
        );
    }
}
#[tokio::test]
async fn legacy_custom_client_does_not_override_explicit_native_provider() {
    let provider = Provider::new(Behavior::Loop);
    let (_, mut session) = setup(provider.clone(), "s");
    let client = reqwest::Client::builder()
        .proxy(reqwest::Proxy::all("http://127.0.0.1:1").unwrap())
        .build()
        .unwrap();
    let engine = AgentEngine::with_client(
        client,
        Arc::new(ToolExecutionCoordinator::new(
            session.tools().clone(),
            Arc::new(ApprovalGate::new()),
        )),
    );
    run(&engine, &mut session, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(provider.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn duplicate_and_unfinished_model_items_fail_before_any_tool_execution() {
    for behavior in [
        Behavior::DuplicateCall,
        Behavior::DuplicateItem,
        Behavior::UnfinishedTool,
        Behavior::UnfinishedReasoning,
    ] {
        let provider = Provider::new(behavior);
        let (engine, mut session) = setup(provider, "s");
        assert!(run(&engine, &mut session, CancellationToken::new())
            .await
            .is_err());
        assert!(
            !session.history().iter().any(|item| matches!(
                item,
                CanonicalItem::ToolResult {
                    is_error: false,
                    ..
                }
            )),
            "a duplicate call executed before rejection"
        );
    }
}

#[tokio::test]
async fn legacy_custom_http_client_is_used_by_adapter_sessions() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = BufReader::new(socket);
        let mut headers = String::new();
        loop {
            let mut line = String::new();
            socket.read_line(&mut line).await.unwrap();
            if line == "\r\n" {
                break;
            }
            headers.push_str(&line);
        }
        let body="data: {\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
        let response=format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body);
        socket
            .get_mut()
            .write_all(response.as_bytes())
            .await
            .unwrap();
        headers
    });
    let tools = Arc::new(ToolRegistry::new());
    let client = reqwest::Client::builder()
        .user_agent("whale-legacy-provider-test")
        .build()
        .unwrap();
    let engine = AgentEngine::with_client(
        client,
        Arc::new(ToolExecutionCoordinator::new(
            tools.clone(),
            Arc::new(ApprovalGate::new()),
        )),
    );
    let mut session = ThreadSession::with_id_and_prompt(
        "session",
        None,
        Arc::new(whale_adapters::OpenAIAdapter::with_options(
            "",
            endpoint,
            whale_adapters::OpenAIWireApi::ChatCompletions,
        )),
        tools,
        SamplingOptions::new("model"),
    );
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        run(&engine, &mut session, CancellationToken::new()),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(peer.await.unwrap().contains("whale-legacy-provider-test"));
}

struct RejectedCreation(Mutex<Option<CancellationToken>>);
#[async_trait]
impl ModelProvider for RejectedCreation {
    fn capabilities(&self, _: &str) -> Result<ModelCapabilities, ModelError> {
        let mut capabilities = ModelCapabilities::text_only();
        capabilities.tool_calls = true;
        Ok(capabilities)
    }
    async fn stream(
        &self,
        _: ModelRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelEventStream, ModelError> {
        *self.0.lock().unwrap() = Some(cancellation);
        Err(ModelError::Transport("creation failed".into()))
    }
}
#[tokio::test]
async fn failed_stream_creation_cancels_the_provider_call_token() {
    let provider = Arc::new(RejectedCreation(Mutex::new(None)));
    let (engine, mut session) = setup(provider.clone(), "session");
    assert!(run(&engine, &mut session, CancellationToken::new())
        .await
        .is_err());
    assert!(provider.0.lock().unwrap().as_ref().unwrap().is_cancelled());
}

#[tokio::test]
async fn completed_model_stream_is_dropped_before_tool_execution() {
    struct ProviderWithDrop(Arc<Mutex<Option<bool>>>);
    struct StreamWithDrop {
        inner: ModelEventStream,
        _probe: DropObservation,
    }
    impl futures::Stream for StreamWithDrop {
        type Item = Result<ModelEvent, ModelError>;
        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            self.get_mut().inner.as_mut().poll_next(cx)
        }
    }
    #[async_trait]
    impl ModelProvider for ProviderWithDrop {
        fn capabilities(&self, _: &str) -> Result<ModelCapabilities, ModelError> {
            let mut capabilities = ModelCapabilities::text_only();
            capabilities.tool_calls = true;
            Ok(capabilities)
        }
        async fn stream(
            &self,
            request: ModelRequest,
            token: CancellationToken,
        ) -> Result<ModelEventStream, ModelError> {
            let item = if request.step_index == 0 {
                ModelEvent::ItemCompleted {
                    item: CanonicalItem::tool_call("check", None, "lookup", Some(json!({})), "{}"),
                }
            } else {
                answer()
            };
            Ok(Box::pin(StreamWithDrop {
                inner: Box::pin(stream::iter(vec![Ok(item), Ok(finish())])),
                _probe: DropObservation {
                    token,
                    observed: self.0.clone(),
                },
            }))
        }
    }
    struct CheckingTool(Arc<Mutex<Option<bool>>>, Arc<std::sync::atomic::AtomicBool>);
    #[async_trait]
    impl ToolHandler for CheckingTool {
        fn name(&self) -> &str {
            "lookup"
        }
        fn description(&self) -> &str {
            "verify model resources already released"
        }
        fn parameters(&self) -> serde_json::Value {
            json!({})
        }
        fn supports_parallel(&self) -> bool {
            false
        }
        fn require_approval(&self) -> bool {
            false
        }
        async fn execute(&self, _: serde_json::Value) -> Result<CanonicalToolOutput, String> {
            assert_eq!(
                *self.0.lock().unwrap(),
                Some(true),
                "model stream was still alive while tool execution began"
            );
            self.1.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(CanonicalToolOutput::text("checked"))
        }
    }
    let observed = Arc::new(Mutex::new(None));
    let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let tools = Arc::new(ToolRegistry::new());
    tools
        .register(Arc::new(CheckingTool(observed.clone(), called.clone())))
        .unwrap();
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        tools.clone(),
        Arc::new(ApprovalGate::new()),
    )));
    let mut session = ThreadSession::from_provider(
        Arc::new(ProviderWithDrop(observed)),
        tools,
        SamplingOptions::new("m"),
    );
    assert_eq!(
        run(&engine, &mut session, CancellationToken::new())
            .await
            .unwrap()
            .steps_taken,
        2
    );
    assert!(called.load(std::sync::atomic::Ordering::SeqCst));
}
