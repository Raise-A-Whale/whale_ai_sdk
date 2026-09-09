use crate::*;
use serde_json::json;

struct Tool;
#[async_trait]
impl HostTool for Tool {
    fn name(&self) -> &str {
        "lookup"
    }
    fn description(&self) -> &str {
        "business"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object"})
    }
    async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
        Ok(CanonicalToolOutput::text("ok"))
    }
}
async fn fixture() -> (WhaleThread, mpsc::Receiver<String>) {
    let (tx, mut rx) = mpsc::channel(32);
    let client = WhaleClient {
        inner: Arc::new(ClientInner {
            state: ClientState::new(),
            writer: ManagedWriter::channel(tx),
            compatibility_owner: None,
        }),
    };
    crate::initialization_tests::initialize_peer(&client, &mut rx).await;
    (
        WhaleThread {
            recovery_key: None,
            client,
            thread_id: "a".into(),
            max_steps: 10,
            timeout_ms: None,
        },
        rx,
    )
}
async fn bounded<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(2), f)
        .await
        .expect("session operation stalled")
}
async fn next(rx: &mut mpsc::Receiver<String>) -> Value {
    serde_json::from_str(&bounded(rx.recv()).await.unwrap()).unwrap()
}
fn respond(t: &WhaleThread, req: &Value, result: Value) {
    t.client.inner.state.incoming(
        &json!({"jsonrpc":"2.0","id":req["id"],"result":result}).to_string(),
        &t.client.inner.writer,
    );
}
fn closed_reply(t: &WhaleThread, req: &Value) {
    respond(t, req, json!({"thread_id":"a","closed":true}));
}
fn install(t: &WhaleThread, sid: &str) {
    t.client
        .inner
        .state
        .tools
        .insert((sid.into(), "lookup".into()), Arc::new(Tool));
    t.client
        .inner
        .state
        .tool_bindings
        .insert((sid.into(), "version".into()), Arc::new(Tool));
}

#[derive(Clone, Copy)]
enum PackCloseBehavior {
    Ok,
    Error,
    Panic,
}

struct LifecyclePack {
    id: &'static str,
    behavior: PackCloseBehavior,
    normal: Arc<std::sync::atomic::AtomicUsize>,
    emergency: Arc<std::sync::atomic::AtomicUsize>,
    events: Arc<std::sync::Mutex<Vec<String>>>,
}

#[async_trait]
impl BoundToolPack for LifecyclePack {
    fn tools(&self) -> Vec<Arc<dyn HostTool>> {
        Vec::new()
    }

    async fn close(&mut self) -> Result<(), ToolPackError> {
        self.normal.fetch_add(1, Ordering::SeqCst);
        self.events
            .lock()
            .unwrap()
            .push(format!("close:{}", self.id));
        match self.behavior {
            PackCloseBehavior::Ok => Ok(()),
            PackCloseBehavior::Error => Err(ToolPackError::new(format!("{} close error", self.id))),
            PackCloseBehavior::Panic => panic!("{} close panic", self.id),
        }
    }

    fn emergency_close(&mut self) {
        self.emergency.fetch_add(1, Ordering::SeqCst);
        self.events
            .lock()
            .unwrap()
            .push(format!("emergency:{}", self.id));
    }
}

struct PackCounters {
    normal: Arc<std::sync::atomic::AtomicUsize>,
    emergency: Arc<std::sync::atomic::AtomicUsize>,
}

fn attach_packs(
    thread: &WhaleThread,
    sid: &str,
    packs: &[(&'static str, PackCloseBehavior)],
    events: Arc<std::sync::Mutex<Vec<String>>>,
) -> Vec<PackCounters> {
    let lifecycle = thread.client.inner.state.prepare_session(sid).unwrap();
    let mut owner = crate::tool_packs::SessionPackOwner::default();
    let mut counters = Vec::new();
    for (id, behavior) in packs {
        let normal = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let emergency = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        owner.push(crate::tool_packs::BoundPackLease::new(
            (*id).into(),
            Box::new(LifecyclePack {
                id,
                behavior: *behavior,
                normal: normal.clone(),
                emergency: emergency.clone(),
                events: events.clone(),
            }),
        ));
        counters.push(PackCounters { normal, emergency });
    }
    let mut owner = Some(owner);
    thread
        .client
        .inner
        .state
        .accept_prepared_session(sid, &lifecycle, &mut owner)
        .unwrap();
    assert!(owner.is_none());
    counters
}

mod pack {
    use super::*;

    #[tokio::test]
    async fn concurrent_close_waiters_share_pack_failure_and_never_retry() {
        let (thread, mut outgoing) = fixture().await;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let counters = attach_packs(
            &thread,
            "a",
            &[
                ("A", PackCloseBehavior::Ok),
                ("B", PackCloseBehavior::Panic),
                ("C", PackCloseBehavior::Error),
            ],
            events.clone(),
        );

        let first_thread = thread.clone();
        let first = tokio::spawn(async move { first_thread.close().await });
        let request = next(&mut outgoing).await;
        let second_thread = thread.clone();
        let second = tokio::spawn(async move { second_thread.close().await });
        tokio::task::yield_now().await;
        assert!(
            outgoing.try_recv().is_err(),
            "single-flight sent a second RPC"
        );
        closed_reply(&thread, &request);

        let first_error = bounded(first).await.unwrap().unwrap_err();
        let second_error = bounded(second).await.unwrap().unwrap_err();
        assert!(matches!(first_error, SdkError::Internal(_)));
        assert!(matches!(second_error, SdkError::Internal(_)));
        assert_eq!(first_error.to_string(), second_error.to_string());
        let message = first_error.to_string();
        assert!(message.contains("B") && message.contains("C"));
        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["close:C", "close:B", "close:A"]
        );
        for counter in &counters {
            assert_eq!(counter.normal.load(Ordering::SeqCst), 1);
            assert_eq!(counter.emergency.load(Ordering::SeqCst), 0);
        }
        assert!(!thread.close().await.unwrap());
        for counter in counters {
            assert_eq!(counter.normal.load(Ordering::SeqCst), 1);
            assert_eq!(counter.emergency.load(Ordering::SeqCst), 0);
        }
        thread.client.close().await;
    }

    #[tokio::test]
    async fn abandoned_first_waiter_does_not_cancel_normal_pack_close() {
        let (thread, mut outgoing) = fixture().await;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let counters = attach_packs(
            &thread,
            "a",
            &[("A", PackCloseBehavior::Ok)],
            events.clone(),
        );

        let first_thread = thread.clone();
        let first = tokio::spawn(async move { first_thread.close().await });
        let request = next(&mut outgoing).await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let second_thread = thread.clone();
        let second = tokio::spawn(async move { second_thread.close().await });
        closed_reply(&thread, &request);

        assert!(!bounded(second).await.unwrap().unwrap());
        let observed = events.lock().unwrap().clone();
        assert_eq!(observed, ["close:A"]);
        assert_eq!(counters[0].normal.load(Ordering::SeqCst), 1);
        assert_eq!(counters[0].emergency.load(Ordering::SeqCst), 0);
        thread.client.close().await;
    }
}

#[tokio::test]
async fn close_fences_clones_cleans_only_target_and_is_idempotent() {
    let (t, mut rx) = fixture().await;
    install(&t, "a");
    install(&t, "b");
    t.client
        .inner
        .state
        .approval_sessions
        .insert("approval-a".into(), "a".into());
    t.client
        .inner
        .state
        .approval_sessions
        .insert("approval-b".into(), "b".into());
    let c = t.clone();
    let closing = tokio::spawn(async move { c.close().await });
    let req = next(&mut rx).await;
    assert_eq!(req["method"], "session.close");
    assert_eq!(req["params"], json!({"thread_id":"a"}));
    assert!(bounded(t.start_turn("blocked")).await.is_err());
    assert!(bounded(t.register_tool(Arc::new(Tool))).await.is_err());
    closed_reply(&t, &req);
    assert!(bounded(closing).await.unwrap().unwrap());
    assert!(!t.close().await.unwrap());
    assert!(!t
        .client
        .inner
        .state
        .approval_sessions
        .contains_key("approval-a"));
    assert!(t
        .client
        .inner
        .state
        .approval_sessions
        .contains_key("approval-b"));
    assert!(!t.client.inner.state.closed.load(Ordering::SeqCst));
    assert!(!t
        .client
        .inner
        .state
        .tools
        .contains_key(&("a".into(), "lookup".into())));
    assert!(!t
        .client
        .inner
        .state
        .tool_bindings
        .contains_key(&("a".into(), "version".into())));
    assert!(t
        .client
        .inner
        .state
        .tools
        .contains_key(&("b".into(), "lookup".into())));
    assert!(t.client.get_run("a", "old").await.is_err());
    assert!(rx.try_recv().is_err());
    t.client.close().await;
}
#[tokio::test]
async fn abandoned_close_waiter_still_cleans_the_session() {
    let (t, mut rx) = fixture().await;
    install(&t, "a");
    let c = t.clone();
    let closing = tokio::spawn(async move { c.close().await });
    let req = next(&mut rx).await;
    closing.abort();
    let _ = closing.await;
    closed_reply(&t, &req);
    bounded(async {
        while t
            .client
            .inner
            .state
            .tools
            .contains_key(&("a".into(), "lookup".into()))
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!t.close().await.unwrap());
    assert!(!t.client.inner.state.closed.load(Ordering::SeqCst));
    t.client.close().await;
}
#[tokio::test]
async fn explicit_unsupported_close_restores_open_and_preserves_bindings() {
    let (t, mut rx) = fixture().await;
    t.client.inner.state.ensure_session_open("a").unwrap();
    let lifecycle = t
        .client
        .inner
        .state
        .sessions
        .get("a")
        .unwrap()
        .value()
        .clone();
    let hub = t
        .client
        .inner
        .state
        .session_event_hubs
        .get("a")
        .unwrap()
        .value()
        .clone();
    install(&t, "a");
    let c = t.clone();
    let closing = tokio::spawn(async move { c.close().await });
    let req = next(&mut rx).await;
    t.client.inner.state.incoming(&json!({"jsonrpc":"2.0","id":req["id"],"error":{"code":-32601,"message":"Method not found"}}).to_string(),&t.client.inner.writer);
    assert!(matches!(
        closing.await.unwrap(),
        Err(SdkError::Rpc { code: -32601, .. })
    ));
    assert!(Arc::ptr_eq(
        t.client.inner.state.sessions.get("a").unwrap().value(),
        &lifecycle
    ));
    assert!(Arc::ptr_eq(
        t.client
            .inner
            .state
            .session_event_hubs
            .get("a")
            .unwrap()
            .value(),
        &hub
    ));
    let c = t.clone();
    let registering = tokio::spawn(async move { c.register_tool(Arc::new(Tool)).await });
    let registration = next(&mut rx).await;
    assert_eq!(registration["method"], "session.register_tools");
    respond(&t, &registration, json!({"registered_count":1}));
    registering.await.unwrap().unwrap();
    t.client.close().await;
}

#[tokio::test]
async fn concurrent_failed_unknown_close_does_not_leave_a_provisional_lifecycle() {
    let (t, mut rx) = fixture().await;
    t.client.inner.state.ensure_session_open("a").unwrap();
    let known = t
        .client
        .inner
        .state
        .sessions
        .get("a")
        .unwrap()
        .value()
        .clone();
    let first_client = t.client.clone();
    let first = tokio::spawn(async move { first_client.close_session("unknown").await });
    let request = next(&mut rx).await;
    let second_client = t.client.clone();
    let second = tokio::spawn(async move { second_client.close_session("unknown").await });
    tokio::task::yield_now().await;
    assert!(rx.try_recv().is_err(), "concurrent close sent a second RPC");
    t.client.inner.state.incoming(
        &json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32602,"message":"unknown"}})
            .to_string(),
        &t.client.inner.writer,
    );
    assert!(first.await.unwrap().is_err());
    assert!(second.await.unwrap().is_err());
    assert!(!t.client.inner.state.sessions.contains_key("unknown"));
    assert!(!t
        .client
        .inner
        .state
        .session_event_hubs
        .contains_key("unknown"));
    assert!(Arc::ptr_eq(
        t.client.inner.state.sessions.get("a").unwrap().value(),
        &known
    ));
    t.client.close().await;
}

#[tokio::test]
async fn unknown_close_reporting_false_does_not_commit_a_fake_closed_session() {
    let (t, mut rx) = fixture().await;
    let client = t.client.clone();
    let closing = tokio::spawn(async move { client.close_session("missing").await });
    let request = next(&mut rx).await;
    respond(&t, &request, json!({"thread_id":"missing","closed":false}));
    assert!(!closing.await.unwrap().unwrap());
    assert!(!t.client.inner.state.sessions.contains_key("missing"));
    assert!(!t
        .client
        .inner
        .state
        .session_event_hubs
        .contains_key("missing"));
    t.client.close().await;
}
#[tokio::test]
async fn close_bypasses_pending_registration_and_late_reply_cannot_restore_bindings() {
    let (t, mut rx) = fixture().await;
    install(&t, "a");
    let retired_handler = Arc::downgrade(
        t.client
            .inner
            .state
            .tools
            .get(&("a".into(), "lookup".into()))
            .unwrap()
            .value(),
    );
    let c = t.clone();
    let registering = tokio::spawn(async move { c.register_tool(Arc::new(Tool)).await });
    let registration = next(&mut rx).await;
    let c = t.clone();
    let closing = tokio::spawn(async move { c.close().await });
    let req = next(&mut rx).await;
    assert_eq!(req["method"], "session.close");
    closed_reply(&t, &req);
    closing.await.unwrap().unwrap();
    assert!(
        retired_handler.upgrade().is_none(),
        "close retained a handler captured by registration"
    );
    assert!(bounded(registering).await.unwrap().is_err());
    respond(&t, &registration, json!({"registered_count":1}));
    tokio::task::yield_now().await;
    assert!(t.client.inner.state.tools.is_empty());
    assert!(t.client.inner.state.tool_bindings.is_empty());
    assert!(t.client.inner.state.registration_locks.is_empty());
    assert!(!t.client.inner.state.closed.load(Ordering::SeqCst));
    t.client.close().await;
}
#[tokio::test]
async fn ambiguous_close_result_closes_the_connection() {
    let (t, mut rx) = fixture().await;
    let c = t.clone();
    let closing = tokio::spawn(async move { c.close().await });
    let req = next(&mut rx).await;
    respond(&t, &req, json!({"unexpected":true}));
    assert!(closing.await.unwrap().is_err());
    assert!(t.client.inner.state.closed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn closed_session_retains_completed_results_and_buffered_events_but_not_authoritative_queries(
) {
    let (t, mut rx) = fixture().await;
    let state = run::RunState::new("a".into(), "done".into());
    let result = RunTurnResult {
        thread_id: "a".into(),
        turn_id: "done".into(),
        status: TurnStatus::Completed,
        items: vec![],
        usage: Default::default(),
    };
    let snapshot = RunSnapshot {
        thread_id: "a".into(),
        turn_id: "done".into(),
        status: RunStatus::Completed,
        items: vec![],
        usage: Default::default(),
        pending_approvals: vec![],
        last_seq: 1,
        result: Some(result.clone()),
        error: None,
        tool_executions: vec![],
    };
    state.accept(RunEvent {
        thread_id: "a".into(),
        turn_id: "done".into(),
        seq: 1,
        payload: RunEventPayload::Finished { snapshot },
    });
    t.client
        .inner
        .state
        .runs
        .insert("done".into(), state.clone());
    let done = RunHandle {
        client: t.client.clone(),
        state,
    };
    let mut events = done.events().unwrap();
    let pending = RunHandle {
        client: t.client.clone(),
        state: run::RunState::new("a".into(), "pending".into()),
    };
    t.client
        .inner
        .state
        .runs
        .insert("pending".into(), pending.state.clone());
    let c = t.clone();
    let closing = tokio::spawn(async move { c.close().await });
    let req = next(&mut rx).await;
    closed_reply(&t, &req);
    closing.await.unwrap().unwrap();
    assert_eq!(done.result().await.unwrap(), result);
    assert!(matches!(
        events.recv().await.unwrap().unwrap().payload,
        RunEventPayload::Finished { .. }
    ));
    assert!(events.recv().await.unwrap().is_none());
    assert!(bounded(pending.result())
        .await
        .unwrap_err()
        .to_string()
        .contains("SessionClosed"));
    assert!(done.snapshot().await.is_err());
    assert!(done.cancel().await.is_err());
    assert!(done
        .resolve_approval("approval", RunApprovalDecision::Approve, None, None)
        .await
        .is_err());
    assert!(t.client.inner.state.runs.is_empty());
    assert!(rx.try_recv().is_err());
    t.client.close().await;
}

#[tokio::test]
async fn internal_rpc_close_error_cannot_reopen_a_possibly_mutated_session() {
    let (t, mut rx) = fixture().await;
    let c = t.clone();
    let closing = tokio::spawn(async move { c.close().await });
    let req = next(&mut rx).await;
    t.client.inner.state.incoming(&json!({"jsonrpc":"2.0","id":req["id"],"error":{"code":-32603,"message":"terminal delivery failed after cancellation"}}).to_string(),&t.client.inner.writer);
    assert!(closing.await.unwrap().is_err());
    assert!(t.client.inner.state.closed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn accepted_registration_during_rejected_close_stays_registered() {
    let (t, mut rx) = fixture().await;
    install(&t, "a");
    let c = t.clone();
    let registering = tokio::spawn(async move { c.register_tool(Arc::new(Tool)).await });
    let registration = next(&mut rx).await;
    let binding = registration["params"]["tools"][0]["binding_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let c = t.clone();
    let closing = tokio::spawn(async move { c.close().await });
    let close = next(&mut rx).await;
    respond(&t, &registration, json!({"registered_count":1}));
    assert!(registering.await.unwrap().is_ok());
    t.client.inner.state.incoming(
        &json!({"jsonrpc":"2.0","id":close["id"],"error":{"code":-32601,"message":"unsupported"}})
            .to_string(),
        &t.client.inner.writer,
    );
    assert!(closing.await.unwrap().is_err());
    assert!(t
        .client
        .inner
        .state
        .tool_bindings
        .contains_key(&("a".into(), binding)));
    assert!(!t.client.inner.state.closed.load(Ordering::SeqCst));
    t.client.close().await;
}
