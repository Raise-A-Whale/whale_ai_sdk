use async_trait::async_trait;
use serde_json::{json, Value};
use std::{io, sync::Arc, time::Duration};
use tokio::sync::{mpsc, Notify, Semaphore};
use whale_daemon::{AnyTransportWriter, DaemonServer, OutgoingTransport};

struct Capture {
    tx: mpsc::UnboundedSender<Value>,
    entered: Arc<Notify>,
    permit: Arc<Semaphore>,
    hold: bool,
    fail: bool,
}
#[async_trait]
impl OutgoingTransport for Capture {
    async fn send_line(&self, line: &str) -> io::Result<()> {
        let value: Value = serde_json::from_str(line).unwrap();
        if value["result"]["protocol_version"] == 1 {
            self.entered.notify_one();
            if self.hold {
                self.permit.acquire().await.unwrap().forget();
            }
            if self.fail {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "init failed"));
            }
        }
        self.tx.send(value).unwrap();
        Ok(())
    }
}
fn fixture(
    hold: bool,
    fail: bool,
) -> (
    DaemonServer,
    AnyTransportWriter,
    mpsc::UnboundedReceiver<Value>,
    Arc<Notify>,
    Arc<Semaphore>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    let entered = Arc::new(Notify::new());
    let permit = Arc::new(Semaphore::new(0));
    let writer = AnyTransportWriter::new(Arc::new(Capture {
        tx,
        entered: entered.clone(),
        permit: permit.clone(),
        hold,
        fail,
    }));
    (DaemonServer::default_server(), writer, rx, entered, permit)
}
fn init_params() -> Value {
    json!({"client":{"name":"test","version":"1"},"protocol_versions":[1],"required_capabilities":["runs.v1"]})
}
async fn send(
    server: &DaemonServer,
    writer: &AnyTransportWriter,
    id: u64,
    method: &str,
    params: Value,
) {
    server
        .handle_message(
            &json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}).to_string(),
            writer,
        )
        .await;
}
async fn create(server: &DaemonServer, writer: &AnyTransportWriter, id: u64) {
    send(server,writer,id,"session.start_thread",json!({"session_id":"s","model":"test","provider_config":{"api":"openai_responses","auth":{"type":"none"}}})).await;
}
async fn recv(rx: &mut mpsc::UnboundedReceiver<Value>) -> Value {
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn ordinary_requests_require_initialization_without_publishing_sessions() {
    let (server, writer, mut rx, _, _) = fixture(false, false);
    create(&server, &writer, 1).await;
    assert_eq!(recv(&mut rx).await["error"]["code"], -32010);
    assert!(server.sessions().is_empty());
    send(&server, &writer, 2, "protocol.initialize", init_params()).await;
    assert_eq!(recv(&mut rx).await["result"]["protocol_version"], 1);
    create(&server, &writer.clone(), 3).await;
    assert!(recv(&mut rx).await.get("error").is_none());
    assert_eq!(server.sessions().len(), 1);
}
#[tokio::test]
async fn invalid_or_incompatible_initialization_allows_corrected_retry() {
    let (server, writer, mut rx, _, _) = fixture(false, false);
    for (params, code) in [
        (
            json!({"client":{"name":"test","version":"1"},"protocol_versions":[true],"required_capabilities":[]}),
            -32602,
        ),
        (
            json!({"client":{"name":"test","version":"1"},"protocol_versions":[1,1],"required_capabilities":[]}),
            -32602,
        ),
        (
            json!({"client":{"name":"test","version":"1"},"protocol_versions":[2],"required_capabilities":[]}),
            -32011,
        ),
        (
            json!({"client":{"name":"test","version":"1"},"protocol_versions":[1],"required_capabilities":["unknown.v1"]}),
            -32011,
        ),
    ] {
        send(&server, &writer, 1, "protocol.initialize", params).await;
        assert_eq!(recv(&mut rx).await["error"]["code"], code);
        create(&server, &writer, 2).await;
        assert_eq!(recv(&mut rx).await["error"]["code"], -32010);
        assert!(server.sessions().is_empty());
    }
    send(&server, &writer, 3, "protocol.initialize", init_params()).await;
    assert_eq!(recv(&mut rx).await["result"]["protocol_version"], 1);
    send(&server, &writer, 4, "protocol.initialize", init_params()).await;
    assert_eq!(recv(&mut rx).await["error"]["code"], -32012);
    create(&server, &writer, 5).await;
    assert!(recv(&mut rx).await.get("error").is_none());
}
#[tokio::test]
async fn acknowledgement_precedes_business_and_concurrent_duplicate_is_rejected() {
    let (server, writer, mut rx, entered, permit) = fixture(true, false);
    let initializer = tokio::spawn({
        let (server, writer) = (server.clone(), writer.clone());
        async move { send(&server, &writer, 1, "protocol.initialize", init_params()).await }
    });
    entered.notified().await;
    send(&server, &writer, 2, "protocol.initialize", init_params()).await;
    assert_eq!(recv(&mut rx).await["error"]["code"], -32012);
    let mut business = tokio::spawn({
        let (server, writer) = (server.clone(), writer.clone());
        async move { create(&server, &writer, 3).await }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut business)
            .await
            .is_err()
    );
    assert!(server.sessions().is_empty());
    assert!(!business.is_finished());
    permit.add_permits(1);
    initializer.await.unwrap();
    business.await.unwrap();
    assert_eq!(recv(&mut rx).await["id"], 1);
    assert_eq!(recv(&mut rx).await["id"], 3);
}
#[tokio::test]
async fn canceled_initializer_releases_waiters_and_cannot_reinitialize() {
    let (server, writer, mut rx, entered, _) = fixture(true, false);
    let initializer = tokio::spawn({
        let (server, writer) = (server.clone(), writer.clone());
        async move { send(&server, &writer, 1, "protocol.initialize", init_params()).await }
    });
    entered.notified().await;
    let mut business = tokio::spawn({
        let (server, writer) = (server.clone(), writer.clone());
        async move { create(&server, &writer, 2).await }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut business)
            .await
            .is_err()
    );
    initializer.abort();
    let _ = initializer.await;
    tokio::time::timeout(Duration::from_secs(2), business)
        .await
        .unwrap()
        .unwrap();
    assert!(recv(&mut rx).await.get("error").is_some());
    assert!(server.sessions().is_empty());
    send(&server, &writer, 3, "protocol.initialize", init_params()).await;
    assert!(recv(&mut rx).await.get("error").is_some());
}
#[tokio::test]
async fn failed_acknowledgement_and_disconnect_never_unlock_business() {
    let (server, writer, mut rx, _, _) = fixture(false, true);
    send(&server, &writer, 1, "protocol.initialize", init_params()).await;
    create(&server, &writer, 2).await;
    assert!(recv(&mut rx).await.get("error").is_some());
    assert!(server.sessions().is_empty());
    let (_, other, mut other_rx, _, _) = fixture(false, false);
    create(&server, &other, 3).await;
    assert_eq!(recv(&mut other_rx).await["error"]["code"], -32010);
    send(&server, &other, 4, "protocol.initialize", init_params()).await;
    assert_eq!(recv(&mut other_rx).await["result"]["protocol_version"], 1);
    create(&server, &other, 5).await;
    assert!(recv(&mut other_rx).await.get("error").is_none());
}
#[tokio::test]
async fn disconnect_during_acknowledgement_releases_waiters_without_resurrection() {
    let (server, writer, mut rx, entered, permit) = fixture(true, false);
    let initializer = tokio::spawn({
        let (server, writer) = (server.clone(), writer.clone());
        async move { send(&server, &writer, 1, "protocol.initialize", init_params()).await }
    });
    entered.notified().await;
    let mut business = tokio::spawn({
        let (server, writer) = (server.clone(), writer.clone());
        async move { create(&server, &writer, 2).await }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut business)
            .await
            .is_err()
    );
    server.disconnect_connection(&writer).await;
    tokio::time::timeout(Duration::from_secs(2), business)
        .await
        .unwrap()
        .unwrap();
    assert!(recv(&mut rx).await.get("error").is_some());
    permit.add_permits(1);
    initializer.await.unwrap();
    assert_eq!(recv(&mut rx).await["id"], 1);
    create(&server, &writer, 3).await;
    assert!(recv(&mut rx).await.get("error").is_some());
    assert!(server.sessions().is_empty());
}

#[tokio::test]
async fn ready_writer_does_not_unlock_an_independent_connection() {
    let (server, writer, mut rx, _, _) = fixture(false, false);
    let (_, other, mut other_rx, _, _) = fixture(false, false);
    send(&server, &writer, 1, "protocol.initialize", init_params()).await;
    assert_eq!(recv(&mut rx).await["result"]["protocol_version"], 1);
    create(&server, &other, 2).await;
    assert_eq!(recv(&mut other_rx).await["error"]["code"], -32010);
    assert!(server.sessions().is_empty());
    create(&server, &writer.clone(), 3).await;
    assert!(recv(&mut rx).await.get("error").is_none());
}

#[tokio::test]
async fn failed_acknowledgement_releases_already_waiting_business() {
    let (server, writer, mut rx, entered, permit) = fixture(true, true);
    let initializer = tokio::spawn({
        let (server, writer) = (server.clone(), writer.clone());
        async move { send(&server, &writer, 1, "protocol.initialize", init_params()).await }
    });
    entered.notified().await;
    let mut business = tokio::spawn({
        let (server, writer) = (server.clone(), writer.clone());
        async move { create(&server, &writer, 2).await }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut business)
            .await
            .is_err()
    );
    assert!(server.sessions().is_empty());
    permit.add_permits(1);
    initializer.await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), business)
        .await
        .unwrap()
        .unwrap();
    assert!(recv(&mut rx).await.get("error").is_some());
    assert!(server.sessions().is_empty());
}

#[tokio::test]
async fn bootstrap_rejects_malformed_values_without_advancing_connection() {
    let (server, writer, mut rx, _, _) = fixture(false, false);
    for (key, value) in [
        ("protocol_versions", json!([])),
        ("protocol_versions", json!([0])),
        ("protocol_versions", json!([1.0])),
        ("protocol_versions", json!(["1"])),
        ("protocol_versions", json!([-1])),
        ("protocol_versions", json!([4294967296u64])),
        ("required_capabilities", json!(["runs.v1", "runs.v1"])),
        ("required_capabilities", json!([""])),
        ("required_capabilities", json!([" runs.v1"])),
        ("client", json!({"name":"","version":"1"})),
        ("client", json!({"name":"test","version":1})),
    ] {
        let mut params = init_params();
        params[key] = value;
        send(&server, &writer, 1, "protocol.initialize", params).await;
        assert_eq!(recv(&mut rx).await["error"]["code"], -32602);
    }
    send(&server, &writer, 2, "protocol.initialize", init_params()).await;
    assert_eq!(recv(&mut rx).await["result"]["protocol_version"], 1);
}

#[tokio::test]
async fn preinit_calls_do_not_reach_provider_or_install_host_bindings() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use whale_core::{
        model::{ModelError, ModelEventStream, ModelProvider, ModelRequest},
        provider::ProviderRegistry,
        CancellationToken,
    };
    use whale_protocol::models::ModelCapabilities;
    struct Probe(Arc<AtomicUsize>);
    #[async_trait]
    impl ModelProvider for Probe {
        fn capabilities(&self, _: &str) -> Result<ModelCapabilities, ModelError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            let mut caps = ModelCapabilities::text_only();
            caps.tool_calls = true;
            Ok(caps)
        }
        async fn stream(
            &self,
            _: ModelRequest,
            _: CancellationToken,
        ) -> Result<ModelEventStream, ModelError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(futures::stream::pending()))
        }
    }
    let (server, writer, mut rx, _, _) = fixture(false, false);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = ProviderRegistry::new();
    registry
        .register_provider("probe", Arc::new(Probe(calls.clone())))
        .unwrap();
    let server = server.with_provider_registry(Arc::new(registry));
    for (method, params) in [
        (
            "provider.inspect",
            json!({"provider_ref":"probe","model":"test"}),
        ),
        (
            "session.start_thread",
            json!({"session_id":"s","provider_ref":"probe","model":"test","tools":[{"name":"lookup","description":"lookup","parameters":{},"is_host_tool":true}]}),
        ),
        (
            "thread.start_turn",
            json!({"thread_id":"s","input_items":[]}),
        ),
        (
            "session.register_tools",
            json!({"thread_id":"s","tools":[]}),
        ),
    ] {
        send(&server, &writer, 1, method, params).await;
        assert_eq!(recv(&mut rx).await["error"]["code"], -32010);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(server.sessions().is_empty());
    assert!(server.pending_host_tool_calls().is_empty());
    send(&server, &writer, 2, "protocol.initialize", init_params()).await;
    assert_eq!(recv(&mut rx).await["result"]["protocol_version"], 1);
    send(
        &server,
        &writer,
        3,
        "provider.inspect",
        json!({"provider_ref":"probe","model":"test"}),
    )
    .await;
    assert!(recv(&mut rx).await.get("error").is_none());
    assert!(calls.load(Ordering::SeqCst) > 0);
}
