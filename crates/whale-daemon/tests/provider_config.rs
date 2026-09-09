mod common;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;
use whale_daemon::{AnyTransportWriter, DaemonServer, OutgoingTransport};
use whale_protocol::rpc::JSONRPCRequest;

struct Sink;
#[async_trait]
impl OutgoingTransport for Sink {
    async fn send_line(&self, _: &str) -> std::io::Result<()> {
        Ok(())
    }
}
fn setup() -> (DaemonServer, AnyTransportWriter) {
    (
        DaemonServer::default_server(),
        AnyTransportWriter::new(Arc::new(Sink)),
    )
}
async fn start(
    server: &DaemonServer,
    writer: &AnyTransportWriter,
    params: Value,
) -> whale_protocol::rpc::JSONRPCResponse {
    server
        .dispatch_request(
            JSONRPCRequest::new(1, "session.start_thread", Some(params)).unwrap(),
            writer,
        )
        .await
}

#[tokio::test]
async fn rejects_unknown_legacy_provider_without_reserving_session_id() {
    let (server, writer) = setup();
    common::initialize(&server, &writer, None).await;
    let result = start(
        &server,
        &writer,
        json!({"session_id":"retry","model":"test","provider":"typo"}),
    )
    .await;
    assert!(result.error.is_some(), "unknown provider accepted");
    assert!(server.sessions().is_empty());
    let retry = start(
        &server,
        &writer,
        json!({"session_id":"retry","model":"test","provider":"openai"}),
    )
    .await;
    assert!(retry.error.is_none(), "failed config occupied session id");
}

#[tokio::test]
async fn explicit_config_rejects_invalid_urls_and_provider_conflicts() {
    let (server, writer) = setup();
    common::initialize(&server, &writer, None).await;
    for base_url in [
        "not-a-url",
        "file:///tmp/model",
        "ftp://localhost/model",
        "https://user:password@example.com/v1",
        "http://localhost/v1?token=secret",
        "http://localhost/v1#fragment",
    ] {
        let result=start(&server,&writer,json!({"session_id":"retry","model":"test","provider_config":{"api":"openai_responses","base_url":base_url,"auth":{"type":"none"}}})).await;
        assert!(
            result.error.is_some(),
            "invalid base_url accepted: {base_url}"
        );
        assert!(server.sessions().is_empty());
    }
    let conflict=start(&server,&writer,json!({"model":"test","provider":"anthropic","provider_config":{"api":"openai_responses","auth":{"type":"none"}}})).await;
    assert!(
        conflict.error.is_some(),
        "conflicting protocol families accepted"
    );
    let unknown=start(&server,&writer,json!({"model":"test","provider":"typo","provider_config":{"api":"openai_responses","auth":{"type":"none"}}})).await;
    assert!(
        unknown.error.is_some(),
        "unknown legacy provider bypassed by explicit config"
    );
}

#[tokio::test]
async fn selects_each_wire_api_and_applies_session_option_defaults() {
    let (server, writer) = setup();
    common::initialize(&server, &writer, None).await;
    for (index, (api, suffix, family)) in [
        ("openai_chat_completions", "chat/completions", "openai"),
        ("openai_responses", "responses", "openai"),
        ("anthropic_messages", "messages", "anthropic"),
    ]
    .into_iter()
    .enumerate()
    {
        let thread = format!("s{index}");
        let effort = (family == "openai").then_some("high");
        let result=start(&server,&writer,json!({"session_id":thread,"model":"base","provider":family,"provider_config":{"api":api,"base_url":"http://127.0.0.1:9876/custom/v1/","auth":{"type":"none"}},"options":{"model":"override-default","temperature":0.25,"max_tokens":123,"reasoning_effort":effort}})).await;
        assert!(result.error.is_none(), "{result:?}");
        let session = server.sessions().get(&thread).unwrap().value().clone();
        let session = session.lock().await;
        assert_eq!(
            session
                .adapter()
                .expect("HTTP-backed session")
                .endpoint_url(),
            format!("http://127.0.0.1:9876/custom/v1/{suffix}")
        );
        let options = session.sampling_options();
        assert_eq!(options.model, "override-default");
        assert_eq!(options.temperature, Some(0.25));
        assert_eq!(options.max_tokens, Some(123));
        assert_eq!(options.reasoning_effort.as_deref(), effort);
        let (_, headers) = session
            .adapter()
            .expect("HTTP-backed session")
            .serialize_request(None, &[], &[], options)
            .unwrap();
        assert!(!headers.contains_key("authorization"));
        assert!(!headers.contains_key("x-api-key"));
    }
}

#[tokio::test]
async fn explicit_env_auth_rejects_missing_empty_or_invalid_secrets_and_uses_the_requested_variable(
) {
    let (server, writer) = setup();
    common::initialize(&server, &writer, None).await;
    let variable = format!("WHALE_PROVIDER_TEST_{}", uuid::Uuid::new_v4().simple());
    let config = json!({"session_id":"retry","model":"test","provider_config":{"api":"openai_responses","auth":{"type":"env","variable":variable}}});
    assert!(start(&server, &writer, config.clone())
        .await
        .error
        .is_some());
    assert!(server.sessions().is_empty());
    std::env::set_var(&variable, "");
    assert!(start(&server, &writer, config.clone())
        .await
        .error
        .is_some());
    std::env::set_var(&variable, "bad\nheader");
    assert!(start(&server, &writer, config.clone())
        .await
        .error
        .is_some());
    std::env::set_var(&variable, "test-key-only");
    let result = start(&server, &writer, config).await;
    std::env::remove_var(&variable);
    assert!(result.error.is_none());
    let session = server.sessions().get("retry").unwrap().value().clone();
    let session = session.lock().await;
    let (_, headers) = session
        .adapter()
        .expect("HTTP-backed session")
        .serialize_request(None, &[], &[], session.sampling_options())
        .unwrap();
    assert_eq!(headers["authorization"], "Bearer test-key-only");
}

struct RestoreEnvironment {
    name: String,
    original: Option<std::ffi::OsString>,
}
impl RestoreEnvironment {
    fn take(name: &str) -> Self {
        Self {
            name: name.into(),
            original: std::env::var_os(name),
        }
    }
}
impl Drop for RestoreEnvironment {
    fn drop(&mut self) {
        match &self.original {
            Some(value) => std::env::set_var(&self.name, value),
            None => std::env::remove_var(&self.name),
        }
    }
}

#[tokio::test]
async fn omitted_explicit_auth_uses_the_conventional_provider_variable() {
    let (server, writer) = setup();
    common::initialize(&server, &writer, None).await;
    for (api, variable, header) in [
        ("openai_responses", "OPENAI_API_KEY", "authorization"),
        ("anthropic_messages", "ANTHROPIC_API_KEY", "x-api-key"),
    ] {
        let _restore = RestoreEnvironment::take(variable);
        std::env::remove_var(variable);
        let params = json!({"session_id":api,"model":"test","provider_config":{"api":api}});
        let missing = start(&server, &writer, params.clone()).await;
        assert!(missing
            .error
            .as_ref()
            .is_some_and(|error| error.message.contains(variable)));
        assert!(!server.sessions().contains_key(api));
        std::env::set_var(variable, "conventional-test-only");
        assert!(start(&server, &writer, params).await.error.is_none());
        let session = server.sessions().get(api).unwrap().value().clone();
        let session = session.lock().await;
        let (_, headers) = session
            .adapter()
            .expect("HTTP-backed session")
            .serialize_request(None, &[], &[], session.sampling_options())
            .unwrap();
        assert_eq!(
            headers[header],
            if header == "authorization" {
                "Bearer conventional-test-only"
            } else {
                "conventional-test-only"
            }
        );
    }
}

#[tokio::test]
async fn invalid_sampling_defaults_do_not_reserve_a_session() {
    let (server, writer) = setup();
    common::initialize(&server, &writer, None).await;
    for options in [
        json!({"model":""}),
        json!({"max_tokens":0}),
        json!({"temperature":-1}),
        json!({"temperature":2.5}),
    ] {
        assert!(start(
            &server,
            &writer,
            json!({"session_id":"retry","model":"test","options":options})
        )
        .await
        .error
        .is_some());
        assert!(server.sessions().is_empty());
    }
    assert!(start(
        &server,
        &writer,
        json!({"session_id":"retry","model":"test"})
    )
    .await
    .error
    .is_none());
}

#[tokio::test]
async fn per_run_overrides_preserve_all_session_defaults() {
    use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
    let (observed, mut observations) = tokio::sync::mpsc::unbounded_channel();
    let gate = Arc::new(ApprovalGate::new());
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    )))
    .with_stream_provider(Arc::new(move |session, _| {
        observed.send(session.sampling_options().clone()).unwrap();
        Ok(Box::pin(futures::stream::iter([Ok(
            whale_protocol::events::AgentStreamEvent::TurnCompleted {
                turn_id: "provider".into(),
                thread_id: "provider".into(),
                usage: Default::default(),
            },
        )])))
    }));
    let server = DaemonServer::new(Arc::new(engine), gate);
    let writer = AnyTransportWriter::new(Arc::new(Sink));
    common::initialize(&server, &writer, None).await;
    assert!(start(&server,&writer,json!({"session_id":"s","model":"base","options":{"temperature":0.5,"max_tokens":456,"reasoning_effort":"medium"}})).await.error.is_none());
    for (turn, options) in [
        (
            "override",
            json!({"model":"other","temperature":0.75,"max_tokens":789,"reasoning_effort":"high"}),
        ),
        ("default", json!({})),
    ] {
        let request = JSONRPCRequest::new(
            2,
            "thread.start_turn",
            Some(json!({"thread_id":"s","turn_id":turn,"input_items":[],"options":options})),
        )
        .unwrap();
        server
            .handle_message(&serde_json::to_string(&request).unwrap(), &writer)
            .await;
        let actual = tokio::time::timeout(std::time::Duration::from_secs(1), observations.recv())
            .await
            .unwrap()
            .unwrap();
        if turn == "override" {
            assert_eq!(actual.model, "other");
            assert_eq!(actual.temperature, Some(0.75));
            assert_eq!(actual.max_tokens, Some(789));
            assert_eq!(actual.reasoning_effort.as_deref(), Some("high"));
        } else {
            assert_eq!(actual.model, "base");
            assert_eq!(actual.temperature, Some(0.5));
            assert_eq!(actual.max_tokens, Some(456));
            assert_eq!(actual.reasoning_effort.as_deref(), Some("medium"));
        }
        let session = server.sessions().get("s").unwrap().value().clone();
        let session = session.lock().await;
        assert_eq!(session.sampling_options().model, "base");
        assert_eq!(session.sampling_options().temperature, Some(0.5));
        assert_eq!(session.sampling_options().max_tokens, Some(456));
        assert_eq!(
            session.sampling_options().reasoning_effort.as_deref(),
            Some("medium")
        );
    }
}

#[tokio::test]
async fn thinking_and_false_caching_overrides_reach_model_and_restore_session_defaults() {
    use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
    let (observed, mut observations) = tokio::sync::mpsc::unbounded_channel();
    let gate = Arc::new(ApprovalGate::new());
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    )))
    .with_stream_provider(Arc::new(move |session, _| {
        observed.send(session.sampling_options().clone()).unwrap();
        Ok(Box::pin(futures::stream::iter([Ok(
            whale_protocol::AgentStreamEvent::TurnCompleted {
                turn_id: "provider".into(),
                thread_id: "provider".into(),
                usage: Default::default(),
            },
        )])))
    }));
    let server = DaemonServer::new(Arc::new(engine), gate);
    let writer = AnyTransportWriter::new(Arc::new(Sink));
    common::initialize(&server, &writer, None).await;
    let response=start(&server,&writer,json!({"session_id":"s","model":"claude","provider_config":{"api":"anthropic_messages","auth":{"type":"none"}},"options":{"max_tokens":4096,"thinking_budget":1024,"prompt_caching":true}})).await;
    assert!(response.error.is_none(), "{response:?}");
    for (turn, options, budget, caching) in [
        (
            "override",
            json!({"thinking_budget":2048,"prompt_caching":false}),
            2048,
            false,
        ),
        ("default", json!({}), 1024, true),
    ] {
        let request = JSONRPCRequest::new(
            2,
            "thread.start_turn",
            Some(json!({"thread_id":"s","turn_id":turn,"input_items":[],"options":options})),
        )
        .unwrap();
        server
            .handle_message(&serde_json::to_string(&request).unwrap(), &writer)
            .await;
        let actual = tokio::time::timeout(std::time::Duration::from_secs(2), observations.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual.thinking_budget, Some(budget));
        assert_eq!(actual.prompt_caching, caching);
        let session = server.sessions().get("s").unwrap().value().clone();
        let session = session.lock().await;
        assert_eq!(session.sampling_options().thinking_budget, Some(1024));
        assert!(session.sampling_options().prompt_caching);
    }
}
