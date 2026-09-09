mod common;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;
use whale_daemon::{AnyTransportWriter, DaemonServer, OutgoingTransport};
use whale_protocol::rpc::{JSONRPCRequest, JSONRPCResponse};
struct Sink;
#[async_trait]
impl OutgoingTransport for Sink {
    async fn send_line(&self, _: &str) -> std::io::Result<()> {
        Ok(())
    }
}
fn fixture() -> (DaemonServer, AnyTransportWriter) {
    (
        DaemonServer::default_server(),
        AnyTransportWriter::new(Arc::new(Sink)),
    )
}
async fn rpc(
    server: &DaemonServer,
    writer: &AnyTransportWriter,
    method: &str,
    params: Value,
) -> JSONRPCResponse {
    server
        .dispatch_request(
            JSONRPCRequest::new(1, method, Some(params)).unwrap(),
            writer,
        )
        .await
}
#[tokio::test]
async fn unknown_and_conflicting_references_never_publish_sessions() {
    let (server, writer) = fixture();
    common::initialize(&server, &writer, None).await;
    for params in [
        json!({"session_id":"s","model":"test","provider_ref":"missing"}),
        json!({"session_id":"s","model":"test","provider_ref":"local","provider":"openai"}),
        json!({"session_id":"s","model":"test","provider_ref":"local","provider_config":{"api":"openai_responses","auth":{"type":"none"}}}),
        json!({"session_id":"s","model":"test","provider_ref":" local"}),
        json!({"session_id":"s","model":"test","provider_ref":""}),
    ] {
        let response = rpc(&server, &writer, "session.start_thread", params).await;
        assert!(
            response.error.is_some(),
            "reference silently selected a default HTTP provider"
        );
        assert!(
            server.sessions().is_empty(),
            "invalid reference published a session"
        );
    }
}
#[tokio::test]
async fn inspect_builtin_is_typed_and_does_not_publish_a_session() {
    let (server, writer) = fixture();
    common::initialize(&server, &writer, None).await;
    let result = rpc(
        &server,
        &writer,
        "provider.inspect",
        json!({"model":"test","provider_config":{"api":"openai_responses","auth":{"type":"none"}}}),
    )
    .await;
    assert!(result.error.is_none(), "{result:?}");
    let value = result.result.unwrap();
    assert_eq!(value["model"], "test");
    assert_eq!(value["capabilities"]["scope"], "protocol");
    assert!(value["capabilities"]["user_content"]
        .as_array()
        .unwrap()
        .contains(&json!("text")));
    assert!(server.sessions().is_empty());
}

use std::sync::atomic::{AtomicUsize, Ordering};
use whale_core::model::{ModelError, ModelEvent, ModelEventStream, ModelProvider, ModelRequest};
use whale_core::provider::ProviderRegistry;
use whale_core::CancellationToken;
use whale_protocol::models::ModelCapabilities;
struct Native(Arc<AtomicUsize>);
#[async_trait]
impl ModelProvider for Native {
    fn capabilities(&self, model: &str) -> Result<ModelCapabilities, ModelError> {
        if !matches!(model, "native" | "no-tools") {
            return Err(ModelError::InvalidRequest("Unknown model".into()));
        }
        let mut caps = ModelCapabilities::text_only();
        caps.tool_calls = model != "no-tools";
        Ok(caps)
    }
    async fn stream(
        &self,
        _: ModelRequest,
        _: CancellationToken,
    ) -> Result<ModelEventStream, ModelError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(futures::stream::iter([Ok(
            ModelEvent::StepFinished {
                usage: Default::default(),
            },
        )])))
    }
}
fn registered_fixture() -> (DaemonServer, AnyTransportWriter, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = ProviderRegistry::new();
    registry
        .register_provider("local", Arc::new(Native(calls.clone())))
        .unwrap();
    let (server, writer) = fixture();
    (
        server.with_provider_registry(Arc::new(registry)),
        writer,
        calls,
    )
}
#[tokio::test]
async fn registered_inspection_and_creation_never_invoke_a_model() {
    let (server, writer, calls) = registered_fixture();
    common::initialize(&server, &writer, None).await;
    let inspected = rpc(
        &server,
        &writer,
        "provider.inspect",
        json!({"model":"native","provider_ref":"local"}),
    )
    .await;
    assert!(inspected.error.is_none(), "{inspected:?}");
    let inspected = inspected.result.unwrap();
    assert_eq!(inspected["provider_ref"], "local");
    assert_eq!(inspected["capabilities"]["scope"], "model");
    let started = rpc(
        &server,
        &writer,
        "session.start_thread",
        json!({"session_id":"native-s","model":"native","provider_ref":"local"}),
    )
    .await;
    assert!(started.error.is_none(), "{started:?}");
    assert!(server
        .sessions()
        .get("native-s")
        .unwrap()
        .lock()
        .await
        .adapter()
        .is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn registered_model_capabilities_reject_defaults_and_tools_before_publication() {
    let (server, writer, calls) = registered_fixture();
    common::initialize(&server, &writer, None).await;
    for params in [
        json!({"session_id":"s","model":"unknown","provider_ref":"local"}),
        json!({"session_id":"s","model":"native","provider_ref":"local","options":{"temperature":0.5}}),
        json!({"session_id":"s","model":"no-tools","provider_ref":"local","tools":[{"name":"lookup","description":"x","parameters":{"type":"object"},"is_host_tool":true}]}),
    ] {
        let result = rpc(&server, &writer, "session.start_thread", params).await;
        assert!(result.error.is_some());
        assert!(server.sessions().is_empty());
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn http_configuration_combinations_fail_before_session_publication() {
    let (server, writer) = fixture();
    common::initialize(&server, &writer, None).await;
    let result=rpc(&server,&writer,"session.start_thread",json!({"session_id":"combo","model":"claude","provider_config":{"api":"anthropic_messages","auth":{"type":"none"}},"options":{"max_tokens":2048,"thinking_budget":1024,"temperature":0.3}})).await;
    assert!(
        result.error.is_some(),
        "HTTP-only option combination was published"
    );
    assert!(server.sessions().is_empty());
}
