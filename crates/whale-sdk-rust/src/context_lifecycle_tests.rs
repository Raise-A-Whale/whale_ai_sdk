//! Lifecycle tests exercise the same synchronous ingress used by the reader.
use crate::*;
use serde_json::json;
use std::sync::atomic::AtomicUsize;
use tokio::sync::mpsc::UnboundedSender;

struct FutureDrop(Option<oneshot::Sender<()>>);
impl Drop for FutureDrop {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

struct WaitingTool {
    entered: UnboundedSender<ToolContext>,
    dropped: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    panic: bool,
}
#[async_trait]
impl HostTool for WaitingTool {
    fn name(&self) -> &str {
        "lookup"
    }
    fn description(&self) -> &str {
        "lifecycle probe"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object"})
    }
    async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
        panic!("legacy dispatch must not bypass the context override")
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        _: Value,
    ) -> Result<CanonicalToolOutput, String> {
        let _drop = FutureDrop(self.dropped.lock().unwrap().take());
        self.entered.send(context).unwrap();
        if self.panic {
            panic!("business tool panic");
        }
        futures::future::pending().await
    }
}

struct WaitingPolicy {
    entered: UnboundedSender<(ContextBuildRequest, CancellationSignal)>,
    dropped: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    calls: Arc<AtomicUsize>,
    panic: bool,
}
#[async_trait]
impl HostContextPolicy for WaitingPolicy {
    async fn build(
        &self,
        request: ContextBuildRequest,
        cancellation: CancellationSignal,
    ) -> Result<ModelContext, String> {
        let _drop = FutureDrop(self.dropped.lock().unwrap().take());
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.send((request, cancellation)).unwrap();
        if self.panic {
            panic!("business policy panic");
        }
        futures::future::pending().await
    }
}

async fn fixture() -> (WhaleClient, mpsc::Receiver<String>) {
    let (tx, mut rx) = mpsc::channel(16);
    let client = WhaleClient {
        inner: Arc::new(ClientInner {
            state: ClientState::new(),
            writer: ManagedWriter::channel(tx),
            compatibility_owner: None,
        }),
    };
    crate::initialization_tests::initialize_peer(&client, &mut rx).await;
    (client, rx)
}
fn incoming(client: &WhaleClient, value: Value) {
    client
        .inner
        .state
        .incoming(&value.to_string(), &client.inner.writer);
}
fn tool_request(id: &str, call_id: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":"tool.execute_host","params":{
        "thread_id":"session","call_id":call_id,"name":"lookup","arguments":{},
        "context":{"thread_id":"session","turn_id":"turn","call_id":"model-call","agent_name":"business"}
    }})
}
fn policy_request(id: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":"context.build_host","params":{
        "context":{"thread_id":"session","turn_id":"turn","agent_name":"business"},
        "step_index":0,"model":"fixture","system_prompt":"system","history":[CanonicalItem::user_text("hello")]
    }})
}
async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(2), future)
        .await
        .expect("lifecycle operation stalled")
}
async fn callbacks_empty(state: &ClientState) {
    bounded(async {
        while !state.tool_callbacks.is_empty() || !state.context_callbacks.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

#[tokio::test]
async fn context_cancel_immediately_after_ingress_prevents_dispatch() {
    let (client, mut outgoing) = fixture().await;
    let (entered, mut entries) = mpsc::unbounded_channel();
    let (dropped, _drop_receiver) = oneshot::channel();
    let calls = Arc::new(AtomicUsize::new(0));
    client.inner.state.context_policies.insert(
        "session".into(),
        Arc::new(WaitingPolicy {
            entered,
            dropped: std::sync::Mutex::new(Some(dropped)),
            calls: calls.clone(),
            panic: false,
        }),
    );
    incoming(&client, policy_request("context-correlation"));
    let signal = client
        .inner
        .state
        .context_callbacks
        .get("context-correlation")
        .expect("request must synchronously register before the reader accepts its next frame")
        .clone();
    incoming(
        &client,
        json!({"jsonrpc":"2.0","method":"context.cancel_host","params":{"request_id":"context-correlation"}}),
    );
    assert!(signal.is_cancelled());
    callbacks_empty(&client.inner.state).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(entries.try_recv().is_err());
    assert!(
        outgoing.try_recv().is_err(),
        "cancelled callback must not send a late reply"
    );
    client.close().await;
}

#[tokio::test]
async fn close_cancels_and_drops_both_callback_futures_without_late_replies() {
    let (client, mut outgoing) = fixture().await;
    let (tool_entered, mut tool_entries) = mpsc::unbounded_channel();
    let (tool_dropped, tool_drop) = oneshot::channel();
    client.inner.state.tools.insert(
        ("session".into(), "lookup".into()),
        Arc::new(WaitingTool {
            entered: tool_entered,
            dropped: std::sync::Mutex::new(Some(tool_dropped)),
            panic: false,
        }),
    );
    let (policy_entered, mut policy_entries) = mpsc::unbounded_channel();
    let (policy_dropped, policy_drop) = oneshot::channel();
    client.inner.state.context_policies.insert(
        "session".into(),
        Arc::new(WaitingPolicy {
            entered: policy_entered,
            dropped: std::sync::Mutex::new(Some(policy_dropped)),
            calls: Arc::new(AtomicUsize::new(0)),
            panic: false,
        }),
    );
    let handler = client
        .inner
        .state
        .tools
        .get(&("session".into(), "lookup".into()))
        .unwrap()
        .value()
        .clone();
    client
        .inner
        .state
        .tool_bindings
        .insert(("session".into(), "version".into()), handler);
    let mut tool = tool_request("reverse-tool", "tool-correlation");
    tool["params"]["binding_id"] = json!("version");
    incoming(&client, tool);
    incoming(&client, policy_request("context-correlation"));
    assert!(client
        .inner
        .state
        .tool_callbacks
        .contains_key("tool-correlation"));
    assert!(client
        .inner
        .state
        .context_callbacks
        .contains_key("context-correlation"));
    let context = bounded(tool_entries.recv()).await.unwrap();
    let (request, cancellation) = bounded(policy_entries.recv()).await.unwrap();
    assert_eq!(context.info().unwrap().call_id, "model-call");
    assert_eq!(request.history.len(), 1);
    assert_eq!(request.context.thread_id, "session");
    client.close().await;
    assert!(context.is_cancelled());
    assert!(cancellation.is_cancelled());
    bounded(tool_drop).await.unwrap();
    bounded(policy_drop).await.unwrap();
    callbacks_empty(&client.inner.state).await;
    assert!(client.inner.state.tools.is_empty());
    assert!(client.inner.state.tool_bindings.is_empty());
    assert!(client.inner.state.context_policies.is_empty());
    assert!(!context
        .report_progress("after close", Some(0.5))
        .await
        .unwrap());
    assert!(
        outgoing.try_recv().is_err(),
        "closing must not emit callbacks' late replies"
    );
}

#[tokio::test]
async fn retained_tool_context_does_not_keep_the_client_or_callback_state_alive() {
    let (client, mut outgoing) = fixture().await;
    let (entered, mut entries) = mpsc::unbounded_channel();
    let (dropped, drop_receiver) = oneshot::channel();
    client.inner.state.tools.insert(
        ("session".into(), "lookup".into()),
        Arc::new(WaitingTool {
            entered,
            dropped: std::sync::Mutex::new(Some(dropped)),
            panic: false,
        }),
    );
    incoming(&client, tool_request("reverse-tool", "tool-correlation"));
    assert!(client
        .inner
        .state
        .tool_callbacks
        .contains_key("tool-correlation"));
    let context = bounded(entries.recv()).await.unwrap();
    let weak_client = Arc::downgrade(&client.inner);
    let weak_state = Arc::downgrade(&client.inner.state);
    // No explicit close: this checks that retaining the business-facing context
    // does not stop the last client handle's Drop from signalling shutdown.
    drop(client);
    assert!(weak_client.upgrade().is_none());
    assert!(context.is_cancelled());
    bounded(drop_receiver).await.unwrap();
    bounded(async {
        while weak_state.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!context
        .report_progress("after client drop", None)
        .await
        .unwrap());
    assert!(outgoing.try_recv().is_err());
}

#[tokio::test]
async fn business_panics_send_correlated_errors_and_release_invocation_state() {
    let (client, mut outgoing) = fixture().await;
    let (tool_entered, mut tool_entries) = mpsc::unbounded_channel();
    let (tool_dropped, tool_drop) = oneshot::channel();
    client.inner.state.tools.insert(
        ("session".into(), "lookup".into()),
        Arc::new(WaitingTool {
            entered: tool_entered,
            dropped: std::sync::Mutex::new(Some(tool_dropped)),
            panic: true,
        }),
    );
    let (policy_entered, mut policy_entries) = mpsc::unbounded_channel();
    let (policy_dropped, policy_drop) = oneshot::channel();
    client.inner.state.context_policies.insert(
        "session".into(),
        Arc::new(WaitingPolicy {
            entered: policy_entered,
            dropped: std::sync::Mutex::new(Some(policy_dropped)),
            calls: Arc::new(AtomicUsize::new(0)),
            panic: true,
        }),
    );
    incoming(&client, tool_request("reverse-panic", "tool-panic"));
    incoming(&client, policy_request("context-panic"));
    assert!(client.inner.state.tool_callbacks.contains_key("tool-panic"));
    assert!(client
        .inner
        .state
        .context_callbacks
        .contains_key("context-panic"));
    let context = bounded(tool_entries.recv()).await.unwrap();
    let _ = bounded(policy_entries.recv()).await.unwrap();
    bounded(tool_drop).await.unwrap();
    bounded(policy_drop).await.unwrap();
    let mut replies = std::collections::HashMap::new();
    for _ in 0..2 {
        let reply: Value = serde_json::from_str(&bounded(outgoing.recv()).await.unwrap()).unwrap();
        assert_eq!(reply["jsonrpc"], "2.0");
        replies.insert(reply["id"].as_str().unwrap().to_owned(), reply);
    }
    let tool_reply = &replies["reverse-panic"];
    assert_eq!(tool_reply["result"]["call_id"], "tool-panic");
    assert_eq!(tool_reply["result"]["is_error"], true);
    assert!(tool_reply["result"]["output"]
        .to_string()
        .contains("Host tool panicked"));
    assert_eq!(
        replies["context-panic"]["error"]["code"],
        JSONRPCError::INTERNAL_ERROR
    );
    assert!(replies["context-panic"]["error"]["message"]
        .as_str()
        .unwrap()
        .contains("Host context policy panicked"));
    callbacks_empty(&client.inner.state).await;
    assert!(!context
        .report_progress("late after panic", None)
        .await
        .unwrap());
    assert!(
        !client.inner.state.closed.load(Ordering::SeqCst),
        "business panic must not break reader connection"
    );
    // The same incoming path must remain usable after both caught panics.
    incoming(
        &client,
        json!({"jsonrpc":"2.0","id":"still-alive","method":"unknown.test"}),
    );
    let next: Value = serde_json::from_str(&bounded(outgoing.recv()).await.unwrap()).unwrap();
    assert_eq!(next["id"], "still-alive");
    assert_eq!(next["error"]["code"], JSONRPCError::METHOD_NOT_FOUND);
    client.close().await;
}

#[tokio::test]
async fn session_close_drops_only_its_callbacks_and_owned_resources() {
    let (client, mut outgoing) = fixture().await;
    let (tool_entered, mut tool_entries) = mpsc::unbounded_channel();
    let (tool_dropped, tool_drop) = oneshot::channel();
    let tool = Arc::new(WaitingTool {
        entered: tool_entered,
        dropped: std::sync::Mutex::new(Some(tool_dropped)),
        panic: false,
    });
    let weak_tool = Arc::downgrade(&tool);
    client
        .inner
        .state
        .tools
        .insert(("session".into(), "lookup".into()), tool.clone());
    client
        .inner
        .state
        .tool_bindings
        .insert(("session".into(), "version".into()), tool);
    let (entered, mut policy_entries) = mpsc::unbounded_channel();
    let (dropped, policy_drop) = oneshot::channel();
    let policy = Arc::new(WaitingPolicy {
        entered,
        dropped: std::sync::Mutex::new(Some(dropped)),
        calls: Arc::new(AtomicUsize::new(0)),
        panic: false,
    });
    let weak_policy = Arc::downgrade(&policy);
    client
        .inner
        .state
        .context_policies
        .insert("session".into(), policy);
    incoming(&client, tool_request("tool", "correlation"));
    incoming(&client, policy_request("policy"));
    let context = bounded(tool_entries.recv()).await.unwrap();
    let (_, signal) = bounded(policy_entries.recv()).await.unwrap();
    let c = client.clone();
    let closing = tokio::spawn(async move { c.close_session("session").await });
    let request: Value = serde_json::from_str(&bounded(outgoing.recv()).await.unwrap()).unwrap();
    assert_eq!(request["method"], "session.close");
    incoming(
        &client,
        json!({"jsonrpc":"2.0","id":request["id"],"result":{"thread_id":"session","closed":true}}),
    );
    assert!(bounded(closing).await.unwrap().unwrap());
    bounded(tool_drop).await.unwrap();
    bounded(policy_drop).await.unwrap();
    assert!(context.is_cancelled());
    assert!(signal.is_cancelled());
    assert!(client.inner.state.tool_callbacks.is_empty());
    assert!(client.inner.state.context_callbacks.is_empty());
    assert!(weak_tool.upgrade().is_none());
    assert!(weak_policy.upgrade().is_none());
    assert!(!client.inner.state.closed.load(Ordering::SeqCst));
    assert!(!context.report_progress("late", None).await.unwrap());
    incoming(&client, tool_request("late-tool", "late-correlation"));
    let reply: Value = serde_json::from_str(&bounded(outgoing.recv()).await.unwrap()).unwrap();
    assert!(reply["error"]["message"]
        .as_str()
        .unwrap()
        .contains("SessionClosed"));
    client.close().await;
}

struct FinishingCallback {
    entered: UnboundedSender<()>,
    release: tokio::sync::Notify,
}
#[async_trait]
impl HostTool for FinishingCallback {
    fn name(&self) -> &str {
        "lookup"
    }
    fn description(&self) -> &str {
        "reply transport probe"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object"})
    }
    async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
        self.entered.send(()).unwrap();
        self.release.notified().await;
        Ok(CanonicalToolOutput::text("ready"))
    }
}
#[async_trait]
impl HostContextPolicy for FinishingCallback {
    async fn build(
        &self,
        request: ContextBuildRequest,
        _: CancellationSignal,
    ) -> Result<ModelContext, String> {
        self.entered.send(()).unwrap();
        self.release.notified().await;
        Ok(ModelContext {
            system_prompt: request.system_prompt,
            items: request.history,
        })
    }
}
async fn close_during_reply_backpressure(policy: bool) {
    let (client, mut outgoing) = fixture().await;
    let (entered, mut entries) = mpsc::unbounded_channel();
    let callback = Arc::new(FinishingCallback {
        entered,
        release: tokio::sync::Notify::new(),
    });
    if policy {
        client
            .inner
            .state
            .context_policies
            .insert("session".into(), callback.clone());
        incoming(&client, policy_request("reply-policy"));
    } else {
        client
            .inner
            .state
            .tools
            .insert(("session".into(), "lookup".into()), callback.clone());
        incoming(&client, tool_request("reply-tool", "reply-call"));
    }
    bounded(entries.recv()).await.unwrap();
    let closer = client.clone();
    let closing = tokio::spawn(async move { closer.close_session("session").await });
    let request: Value = serde_json::from_str(&bounded(outgoing.recv()).await.unwrap()).unwrap();
    assert_eq!(request["method"], "session.close");
    // The close frame has reached the peer. Delay subsequent outgoing replies,
    // while still allowing the reader to receive the peer's close acknowledgement.
    let writer_backpressure = client.inner.writer.target.lock().await;
    callback.release.notify_one();
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    incoming(
        &client,
        json!({"jsonrpc":"2.0","id":request["id"],"result":{"thread_id":"session","closed":true}}),
    );
    assert!(bounded(closing).await.unwrap().unwrap());
    assert!(client.inner.state.tool_callbacks.is_empty());
    assert!(client.inner.state.context_callbacks.is_empty());
    assert!(!client.inner.state.closed.load(Ordering::SeqCst));
    drop(writer_backpressure);
    assert!(
        outgoing.try_recv().is_err(),
        "closed callback sent a late reply"
    );
    client.close().await;
}
#[tokio::test]
async fn session_close_interrupts_tool_reply_backpressure() {
    close_during_reply_backpressure(false).await;
}
#[tokio::test]
async fn session_close_interrupts_policy_reply_backpressure() {
    close_during_reply_backpressure(true).await;
}

mod pack {
    use super::*;

    struct OrderedCallback {
        events: Arc<std::sync::Mutex<Vec<String>>>,
        entered: UnboundedSender<()>,
        calls: Arc<AtomicUsize>,
    }

    struct CallbackDrop {
        events: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl Drop for CallbackDrop {
        fn drop(&mut self) {
            self.events.lock().unwrap().push("callback:drop:B".into());
        }
    }

    #[async_trait]
    impl HostTool for OrderedCallback {
        fn name(&self) -> &str {
            "lookup"
        }

        fn description(&self) -> &str {
            "ordered pack callback"
        }

        fn parameters(&self) -> Value {
            json!({"type":"object"})
        }

        async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
            unreachable!("context-aware dispatch is required")
        }

        async fn execute_with_context(
            &self,
            _: ToolContext,
            _: Value,
        ) -> Result<CanonicalToolOutput, String> {
            let _drop = CallbackDrop {
                events: self.events.clone(),
            };
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.events.lock().unwrap().push("callback:enter:B".into());
            self.entered.send(()).unwrap();
            futures::future::pending().await
        }
    }

    struct OrderedPack {
        id: &'static str,
        events: Arc<std::sync::Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl BoundToolPack for OrderedPack {
        fn tools(&self) -> Vec<Arc<dyn HostTool>> {
            Vec::new()
        }

        async fn close(&mut self) -> Result<(), ToolPackError> {
            self.events
                .lock()
                .unwrap()
                .push(format!("close:{}", self.id));
            Ok(())
        }
    }

    fn attach_owner(client: &WhaleClient, events: Arc<std::sync::Mutex<Vec<String>>>) {
        let lifecycle = client.inner.state.prepare_session("session").unwrap();
        let mut owner = crate::tool_packs::SessionPackOwner::default();
        for id in ["A", "B"] {
            owner.push(crate::tool_packs::BoundPackLease::new(
                id.into(),
                Box::new(OrderedPack {
                    id,
                    events: events.clone(),
                }),
            ));
        }
        let mut owner = Some(owner);
        client
            .inner
            .state
            .accept_prepared_session("session", &lifecycle, &mut owner)
            .unwrap();
    }

    #[tokio::test]
    async fn close_quiesces_pack_callback_before_reverse_pack_close() {
        let (client, mut outgoing) = fixture().await;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let (entered, mut entries) = mpsc::unbounded_channel();
        client.inner.state.tools.insert(
            ("session".into(), "lookup".into()),
            Arc::new(OrderedCallback {
                events: events.clone(),
                entered,
                calls: calls.clone(),
            }),
        );
        attach_owner(&client, events.clone());
        incoming(&client, tool_request("pack-tool", "pack-call"));
        bounded(entries.recv()).await.unwrap();

        let closer = client.clone();
        let closing = tokio::spawn(async move { closer.close_session("session").await });
        let close: Value = serde_json::from_str(&bounded(outgoing.recv()).await.unwrap()).unwrap();
        assert_eq!(close["method"], "session.close");
        incoming(&client, tool_request("late-pack-tool", "late-pack-call"));
        let rejected: Value =
            serde_json::from_str(&bounded(outgoing.recv()).await.unwrap()).unwrap();
        assert_eq!(rejected["id"], "late-pack-tool");
        assert!(rejected["error"]["message"]
            .as_str()
            .unwrap()
            .contains("SessionClosed"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        incoming(
            &client,
            json!({"jsonrpc":"2.0","id":close["id"],"result":{"thread_id":"session","closed":true}}),
        );
        assert!(bounded(closing).await.unwrap().unwrap());
        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["callback:enter:B", "callback:drop:B", "close:B", "close:A"]
        );
        assert!(client.inner.state.tool_callbacks.is_empty());
        client.close().await;
    }
}
