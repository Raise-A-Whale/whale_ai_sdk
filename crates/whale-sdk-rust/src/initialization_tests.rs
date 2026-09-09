use crate::*;
use serde_json::json;
use whale_protocol::initialization::{
    InitializeParams, InitializeResult, PeerInfo, METHOD_INITIALIZE,
};
fn fixture() -> (WhaleClient, mpsc::Receiver<String>) {
    let (tx, rx) = mpsc::channel(16);
    (
        WhaleClient {
            inner: Arc::new(ClientInner {
                state: ClientState::new(),
                writer: ManagedWriter::channel(tx),
                compatibility_owner: None,
            }),
        },
        rx,
    )
}
pub(crate) async fn next(rx: &mut mpsc::Receiver<String>) -> Value {
    serde_json::from_str(
        &tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("peer did not receive request")
            .unwrap(),
    )
    .unwrap()
}
pub(crate) fn valid_reply(request: &Value) -> Value {
    let params: InitializeParams = serde_json::from_value(request["params"].clone()).unwrap();
    serde_json::to_value(
        InitializeResult::negotiate(
            &params,
            PeerInfo {
                name: "test-daemon".into(),
                version: "test-version".into(),
            },
        )
        .unwrap(),
    )
    .unwrap()
}
fn respond(client: &WhaleClient, request: &Value, result: Value) {
    client.inner.state.incoming(
        &json!({"jsonrpc":"2.0","id":request["id"],"result":result}).to_string(),
        &client.inner.writer,
    );
}
/// Existing private peers explicitly exchange and consume a real handshake.
pub(crate) async fn initialize_peer(client: &WhaleClient, rx: &mut mpsc::Receiver<String>) {
    let initializing = client.clone();
    let pending = tokio::spawn(async move { initializing.initialize().await });
    let request = next(rx).await;
    assert_eq!(request["method"], METHOD_INITIALIZE);
    respond(client, &request, valid_reply(&request));
    pending.await.unwrap().unwrap();
}
#[tokio::test]
async fn concurrent_first_requests_share_one_validated_initialization() {
    let (client, mut rx) = fixture();
    let a = client.clone();
    let b = client.clone();
    let first =
        tokio::spawn(async move { a.request::<_, Value>("example.first", None::<Value>).await });
    let second =
        tokio::spawn(async move { b.request::<_, Value>("example.second", None::<Value>).await });
    let init = next(&mut rx).await;
    assert_eq!(init["method"], METHOD_INITIALIZE);
    tokio::task::yield_now().await;
    assert!(
        rx.try_recv().is_err(),
        "ordinary request preceded handshake"
    );
    let mut value = valid_reply(&init);
    value["capabilities"]
        .as_array_mut()
        .unwrap()
        .push(json!("future.optional"));
    respond(&client, &init, value);
    for _ in 0..2 {
        let request = next(&mut rx).await;
        assert!(request["method"].as_str().unwrap().starts_with("example."));
        respond(&client, &request, json!({"ok":true}));
    }
    assert!(first.await.unwrap().is_ok());
    assert!(second.await.unwrap().is_ok());
    assert_eq!(client.initialize().await.unwrap().protocol_version, 1);
    assert!(rx.try_recv().is_err());
    client.close().await;
}
#[tokio::test]
async fn aborting_first_initializer_waiter_does_not_restart_handshake() {
    let (client, mut rx) = fixture();
    let first = client.clone();
    let first = tokio::spawn(async move { first.initialize().await });
    let request = next(&mut rx).await;
    assert_eq!(request["method"], METHOD_INITIALIZE);
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let second = client.clone();
    let second = tokio::spawn(async move { second.initialize().await });
    tokio::task::yield_now().await;
    assert!(rx.try_recv().is_err());
    respond(&client, &request, valid_reply(&request));
    assert!(second.await.unwrap().is_ok());
    assert!(rx.try_recv().is_err());
    client.close().await;
}
#[tokio::test]
async fn incompatible_and_malformed_handshakes_close_client_without_business_requests() {
    for mode in [
        "old",
        "wrong",
        "zero",
        "bool",
        "float",
        "string",
        "missing-caps",
        "duplicate-caps",
        "bad-peer",
        "missing-peer",
    ] {
        let (client, mut rx) = fixture();
        let c = client.clone();
        let operation = tokio::spawn(async move { c.create_thread("model", None).await });
        let request = next(&mut rx).await;
        assert_eq!(request["method"], METHOD_INITIALIZE);
        let mut response = valid_reply(&request);
        match mode {
            "old"=>client.inner.state.incoming(&json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32601,"message":"Unknown method"}}).to_string(),&client.inner.writer),
            _=>{
                match mode {
                    "wrong"=>response["protocol_version"]=json!(2),"zero"=>response["protocol_version"]=json!(0),
                    "bool"=>response["protocol_version"]=json!(true),"float"=>response["protocol_version"]=json!(1.0),"string"=>response["protocol_version"]=json!("1"),
                    "missing-caps"=>response["capabilities"]=json!([]),"duplicate-caps"=>response["capabilities"].as_array_mut().unwrap().push(json!("runs.v1")),
                    "bad-peer"=>response["server"]["name"]=json!(""),"missing-peer"=>{response.as_object_mut().unwrap().remove("server");},_=>unreachable!(),
                }
                respond(&client,&request,response);
            }
        }
        assert!(
            matches!(
                operation.await.unwrap(),
                Err(SdkError::ProtocolCompatibility(_))
            ),
            "{mode}"
        );
        assert!(client.inner.state.closed.load(Ordering::SeqCst));
        assert!(client.inner.state.pending.is_empty());
        assert!(client.inner.writer.target.lock().await.is_none());
        assert!(client.create_thread("retry", None).await.is_err());
        assert!(client.initialize().await.is_err());
        assert!(rx.try_recv().is_err(), "post-failure request for {mode}");
    }
}
#[tokio::test]
async fn explicit_close_interrupts_initialization() {
    let (client, mut rx) = fixture();
    let c = client.clone();
    let operation = tokio::spawn(async move { c.initialize().await });
    let request = next(&mut rx).await;
    assert_eq!(request["method"], METHOD_INITIALIZE);
    client.close().await;
    assert!(tokio::time::timeout(Duration::from_secs(2), operation)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    assert!(client.inner.state.pending.is_empty());
    assert!(rx.try_recv().is_err());
}

#[tokio::test(start_paused = true)]
async fn initialization_timeout_closes_transport_and_clears_pending() {
    let (client, mut rx) = fixture();
    let c = client.clone();
    let initialization = tokio::spawn(async move { c.initialize().await });
    next(&mut rx).await;
    tokio::time::advance(Duration::from_secs(11)).await;
    assert!(
        matches!(initialization.await.unwrap(), Err(SdkError::ProtocolCompatibility(message)) if message.contains("10 seconds"))
    );
    assert!(client.inner.state.closed.load(Ordering::Acquire));
    assert!(client.inner.state.pending.is_empty());
    assert!(client.inner.writer.target.lock().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn runtime_initialization_uses_the_callers_deadline_instead_of_the_legacy_default() {
    let (client, mut rx) = fixture();
    let timeout = Duration::from_millis(75);
    let deadline = tokio::time::Instant::now() + timeout;
    let initializing = tokio::spawn({
        let client = client.clone();
        async move { client.initialize_until(deadline, timeout).await }
    });
    let request = next(&mut rx).await;
    assert_eq!(request["method"], METHOD_INITIALIZE);

    tokio::time::advance(Duration::from_millis(76)).await;

    assert_eq!(
        initializing.await.unwrap(),
        Err(crate::initialization::InitializationFailure::TimedOut { timeout })
    );
    assert!(client.inner.state.closed.load(Ordering::Acquire));
    assert!(client.inner.state.pending.is_empty());
}

#[tokio::test]
async fn eof_during_initialization_is_permanent_failure() {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let (connection, peer) = tokio::io::duplex(8192);
    let (read, write) = tokio::io::split(connection);
    let client = WhaleClient::with_io(read, ManagedWriter::io(write), None);
    let c = client.clone();
    let pending = tokio::spawn(async move { c.initialize().await });
    let mut peer = BufReader::new(peer);
    let mut line = String::new();
    peer.read_line(&mut line).await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&line).unwrap()["method"],
        METHOD_INITIALIZE
    );
    drop(peer);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), pending)
            .await
            .unwrap()
            .unwrap(),
        Err(SdkError::ProtocolCompatibility(_))
    ));
    assert!(client.inner.state.closed.load(Ordering::Acquire));
    assert!(client.inner.state.pending.is_empty());
}

#[tokio::test]
async fn shared_initialization_fixture_is_validated_through_the_client() {
    let shared: Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/initialization-v1.json"
    ))
    .unwrap();
    for case in shared["response_cases"].as_array().unwrap() {
        let (client, mut rx) = fixture();
        let c = client.clone();
        let pending = tokio::spawn(async move { c.initialize().await });
        let request = next(&mut rx).await;
        respond(&client, &request, case["result"].clone());
        let result = pending.await.unwrap();
        assert_eq!(
            result.is_ok(),
            case["valid"].as_bool().unwrap(),
            "{}: {result:?}",
            case["name"]
        );
        if result.is_err() {
            assert!(client.inner.state.closed.load(Ordering::Acquire));
        }
        client.close().await;
    }
}

struct IngressProbe(Arc<std::sync::atomic::AtomicUsize>);
#[async_trait]
impl HostTool for IngressProbe {
    fn name(&self) -> &str {
        "lookup"
    }
    fn description(&self) -> &str {
        "initialization ingress probe"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object"})
    }
    async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(CanonicalToolOutput::text("ready"))
    }
}
#[async_trait]
impl HostContextPolicy for IngressProbe {
    async fn build(
        &self,
        request: ContextBuildRequest,
        _: CancellationSignal,
    ) -> Result<ModelContext, String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ModelContext {
            system_prompt: request.system_prompt,
            items: request.history,
        })
    }
}

#[tokio::test]
async fn reverse_requests_are_rejected_before_ready_without_blocking_initialization() {
    let (client, mut rx) = fixture();
    let effects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    client.inner.state.tools.insert(
        ("session".into(), "lookup".into()),
        Arc::new(IngressProbe(effects.clone())),
    );
    client
        .inner
        .state
        .context_policies
        .insert("session".into(), Arc::new(IngressProbe(effects.clone())));
    let tool = json!({"jsonrpc":"2.0","id":"tool","method":"tool.execute_host","params":{
        "thread_id":"session","call_id":"call","name":"lookup","arguments":{},
        "context":{"thread_id":"session","turn_id":"turn","call_id":"call","agent_name":"probe"}
    }});
    let policy = json!({"jsonrpc":"2.0","id":"policy","method":"context.build_host","params":{
        "context":{"thread_id":"session","turn_id":"turn","agent_name":"probe"},
        "step_index":0,"model":"fixture","system_prompt":"system","history":[CanonicalItem::user_text("hello")]
    }});
    for message in [&tool, &policy] {
        client
            .inner
            .state
            .incoming(&message.to_string(), &client.inner.writer);
        let reply = next(&mut rx).await;
        assert_eq!(reply["id"], message["id"]);
        assert_eq!(
            reply["error"]["code"], -32010,
            "pre-initialization request executed: {reply}"
        );
    }
    let c = client.clone();
    let initialized = tokio::spawn(async move { c.initialize().await });
    let initialize = next(&mut rx).await;
    assert_eq!(initialize["method"], METHOD_INITIALIZE);
    for message in [&tool, &policy] {
        client
            .inner
            .state
            .incoming(&message.to_string(), &client.inner.writer);
        let reply = next(&mut rx).await;
        assert_eq!(reply["error"]["code"], -32010);
    }
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert!(client.inner.state.tool_callbacks.is_empty());
    assert!(client.inner.state.context_callbacks.is_empty());
    respond(&client, &initialize, valid_reply(&initialize));
    tokio::time::timeout(Duration::from_secs(2), initialized)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    for message in [&tool, &policy] {
        client
            .inner
            .state
            .incoming(&message.to_string(), &client.inner.writer);
        let reply = next(&mut rx).await;
        assert_eq!(reply["id"], message["id"]);
        assert!(
            reply.get("error").is_none(),
            "ready callback rejected: {reply}"
        );
    }
    assert_eq!(effects.load(Ordering::SeqCst), 2);
    client.close().await;
}

#[tokio::test]
async fn business_notifications_before_ready_do_not_mutate_or_disconnect_client() {
    let (client, mut rx) = fixture();
    // A malformed turn payload would disconnect a ready client. Before readiness
    // it is untrusted business traffic and must never reach the turn router.
    let event = json!({"jsonrpc":"2.0","method":"turn.event","params":{"invalid":true}});
    client
        .inner
        .state
        .incoming(&event.to_string(), &client.inner.writer);
    assert!(!client.inner.state.closed.load(Ordering::SeqCst));
    let c = client.clone();
    let initialized = tokio::spawn(async move { c.initialize().await });
    let initialize = next(&mut rx).await;
    client
        .inner
        .state
        .incoming(&event.to_string(), &client.inner.writer);
    assert!(!client.inner.state.closed.load(Ordering::SeqCst));
    respond(&client, &initialize, valid_reply(&initialize));
    assert!(initialized.await.unwrap().is_ok());
    assert!(rx.try_recv().is_err());
    client.close().await;
}

#[tokio::test]
async fn eof_before_first_initializer_still_reaps_owned_child() {
    let mut child = Command::new("/bin/sh")
        .args(["-c", "exec 1>&-; exec sleep 30"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let read = child.stdout.take().unwrap();
    let write = child.stdin.take().unwrap();
    let client = WhaleClient::with_io(read, ManagedWriter::io(write), Some(child));
    tokio::time::timeout(Duration::from_secs(2), async {
        while !client.inner.state.closed.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reader did not observe EOF");
    assert!(matches!(
        client.initialize().await,
        Err(SdkError::ProtocolCompatibility(_))
    ));
    assert!(
        client
            .inner
            .compatibility_owner
            .as_ref()
            .unwrap()
            .lock()
            .await
            .try_wait()
            .unwrap()
            .is_some(),
        "EOF before first initialization left the owned child alive"
    );
    client.close().await;
}
