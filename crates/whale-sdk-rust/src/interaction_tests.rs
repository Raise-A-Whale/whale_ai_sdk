use crate::*;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use whale_protocol::initialization::{
    InitializeParams, InitializeResult, PeerInfo, METHOD_INITIALIZE,
};
use whale_protocol::interactions::*;
use whale_protocol::recovery::{
    PersistentSessionResult, RecoveryKey, RecoverySnapshot, CAPABILITY_SESSION_RECOVERY,
    METHOD_RECOVERY_ATTACH, METHOD_RECOVERY_INSPECT, METHOD_SESSION_CREATE_PERSISTENT,
};
use whale_protocol::rpc::{
    ApprovalResolveResult, JSONRPCError, JSONRPCNotification, JSONRPCRequest, JSONRPCResponse,
    StartThreadParams, StartThreadResult, TurnStatus, METHOD_SESSION_START_THREAD,
};
use whale_protocol::runs::{
    RunApprovalDecision, RunSnapshot, RunStatus, METHOD_TURN_RESOLVE_APPROVAL,
};
use whale_protocol::sessions::{CloseSessionResult, METHOD_SESSION_CLOSE};

async fn next_request(outgoing: &mut mpsc::Receiver<String>) -> JSONRPCRequest {
    serde_json::from_str(
        &tokio::time::timeout(Duration::from_secs(2), outgoing.recv())
            .await
            .expect("fake peer did not receive a request")
            .expect("SDK request channel closed"),
    )
    .expect("SDK emitted invalid JSON-RPC")
}

fn respond<T: serde::Serialize>(client: &WhaleClient, request: JSONRPCRequest, value: T) {
    let response = JSONRPCResponse::success(request.id, value).unwrap();
    client.inner.state.incoming(
        &serde_json::to_string(&response).unwrap(),
        &client.inner.writer,
    );
}

fn reject(client: &WhaleClient, request: JSONRPCRequest, code: i64, message: &str) {
    let response = JSONRPCResponse::error(request.id, JSONRPCError::new(code, message, None));
    client.inner.state.incoming(
        &serde_json::to_string(&response).unwrap(),
        &client.inner.writer,
    );
}

fn notify(client: &WhaleClient, event: InteractionEventEnvelope) {
    let notification =
        JSONRPCNotification::new(METHOD_SESSION_INTERACTION_EVENT, Some(event)).unwrap();
    client.inner.state.incoming(
        &serde_json::to_string(&notification).unwrap(),
        &client.inner.writer,
    );
}

fn client_fixture() -> (WhaleClient, mpsc::Receiver<String>) {
    let (sender, receiver) = mpsc::channel(1024);
    let client = WhaleClient {
        inner: Arc::new(ClientInner {
            state: ClientState::new(),
            writer: ManagedWriter::channel(sender),
            compatibility_owner: None,
        }),
    };
    (client, receiver)
}

async fn initialize(
    client: &WhaleClient,
    outgoing: &mut mpsc::Receiver<String>,
    interactions: bool,
) {
    let waiting = tokio::spawn({
        let client = client.clone();
        async move { client.initialize().await }
    });
    let request = next_request(outgoing).await;
    assert_eq!(request.method, METHOD_INITIALIZE);
    let params: InitializeParams = serde_json::from_value(request.params.clone().unwrap()).unwrap();
    let mut result = InitializeResult::negotiate(
        &params,
        PeerInfo {
            name: "interaction-peer".into(),
            version: "test".into(),
        },
    )
    .unwrap();
    if interactions {
        result.capabilities.push(CAPABILITY_INTERACTIONS.into());
    }
    respond(client, request, result);
    waiting.await.unwrap().unwrap();
}

async fn open_interaction_thread() -> (WhaleThread, mpsc::Receiver<String>) {
    let (client, mut outgoing) = client_fixture();
    let agent = client
        .agent(AgentDefinition::new("interactive", "fixture"), Vec::new())
        .unwrap()
        .with_interactions_enabled();
    let opening = tokio::spawn(async move { agent.create_session().await });
    let initialize_request = next_request(&mut outgoing).await;
    assert_eq!(initialize_request.method, METHOD_INITIALIZE);
    let params: InitializeParams =
        serde_json::from_value(initialize_request.params.clone().unwrap()).unwrap();
    let mut result = InitializeResult::negotiate(
        &params,
        PeerInfo {
            name: "interaction-peer".into(),
            version: "test".into(),
        },
    )
    .unwrap();
    result.capabilities.push(CAPABILITY_INTERACTIONS.into());
    respond(&client, initialize_request, result);

    let start = next_request(&mut outgoing).await;
    assert_eq!(start.method, METHOD_SESSION_START_THREAD);
    assert_eq!(start.params.as_ref().unwrap()["interactions_enabled"], true);
    let thread_id = start.params.as_ref().unwrap()["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    respond(
        &client,
        start,
        StartThreadResult {
            thread_id,
            created_at: "now".into(),
        },
    );
    (opening.await.unwrap().unwrap(), outgoing)
}

fn cursor(thread_id: &str, stream_id: &str, seq: u64) -> InteractionCursor {
    InteractionCursor {
        thread_id: thread_id.into(),
        stream_id: stream_id.into(),
        seq,
    }
}

fn request(id: &str, turn_id: &str) -> PendingInteraction {
    PendingInteraction::new(
        id,
        turn_id,
        InteractionRequest::new(
            "app.question",
            "Question",
            json!({"prompt":"continue?"}),
            None,
        )
        .unwrap(),
    )
    .unwrap()
}

fn snapshot(thread_id: &str, stream_id: &str, seq: u64) -> InteractionSnapshot {
    InteractionSnapshot::new(thread_id, cursor(thread_id, stream_id, seq), Vec::new()).unwrap()
}

fn requested_event(
    thread_id: &str,
    stream_id: &str,
    seq: u64,
    request_id: &str,
) -> InteractionEventEnvelope {
    InteractionEventEnvelope::new(
        thread_id,
        cursor(thread_id, stream_id, seq),
        seq,
        InteractionEventPayload::Requested {
            interaction: request(request_id, "turn"),
        },
    )
}

fn removed_event(
    thread_id: &str,
    stream_id: &str,
    seq: u64,
    request_id: &str,
) -> InteractionEventEnvelope {
    InteractionEventEnvelope::new(
        thread_id,
        cursor(thread_id, stream_id, seq),
        seq,
        InteractionEventPayload::Removed {
            request_id: request_id.into(),
            turn_id: "turn".into(),
            cause: INTERACTION_REMOVAL_RESOLVED.into(),
        },
    )
}

#[test]
fn interaction_option_boundaries_and_defaults_are_frozen() {
    let subscription = InteractionSubscriptionOptions::default();
    assert_eq!(subscription.output_capacity(), 64);
    assert_eq!(subscription.replay_page_limit(), 128);
    assert!(InteractionSubscriptionOptions::new(0, 1).is_err());
    assert!(InteractionSubscriptionOptions::new(1, 0).is_err());
    assert!(InteractionSubscriptionOptions::new(4_097, 1).is_err());
    assert!(InteractionSubscriptionOptions::new(1, 257).is_err());
    let exact = InteractionSubscriptionOptions::new(4_096, 256).unwrap();
    assert_eq!(exact.output_capacity(), 4_096);
    assert_eq!(exact.replay_page_limit(), 256);

    let watch = InteractionWatchOptions::default();
    assert_eq!(watch.output_capacity(), 64);
    assert_eq!(watch.replay_page_limit(), 128);
    assert!(InteractionWatchOptions::new(0, 1).is_err());
    assert!(InteractionWatchOptions::new(1, 257).is_err());
    assert!(InteractionWatchOptions::new(4_096, 256).is_ok());
}

#[tokio::test]
async fn missing_capability_rejects_opt_in_before_any_business_rpc() {
    let (client, mut outgoing) = client_fixture();
    let agent = client
        .agent(AgentDefinition::new("interactive", "fixture"), Vec::new())
        .unwrap()
        .with_interactions_enabled();
    assert!(agent.interactions_enabled());
    let opening = tokio::spawn(async move { agent.create_session().await });

    let request = next_request(&mut outgoing).await;
    assert_eq!(request.method, METHOD_INITIALIZE);
    let params: InitializeParams = serde_json::from_value(request.params.clone().unwrap()).unwrap();
    respond(
        &client,
        request,
        InitializeResult::negotiate(
            &params,
            PeerInfo {
                name: "old-peer".into(),
                version: "test".into(),
            },
        )
        .unwrap(),
    );
    assert!(matches!(
        opening.await.unwrap(),
        Err(SdkError::ProtocolCompatibility(_))
    ));
    assert!(outgoing.try_recv().is_err(), "no business RPC may be sent");
}

#[tokio::test]
async fn non_opted_session_rejects_all_generic_entrypoints_without_business_rpc() {
    let (client, mut outgoing) = client_fixture();
    initialize(&client, &mut outgoing, true).await;
    client.inner.state.ensure_session_open("session").unwrap();
    let thread = WhaleThread {
        recovery_key: None,
        client: client.clone(),
        thread_id: "session".into(),
        max_steps: 10,
        timeout_ms: None,
    };
    assert!(matches!(
        thread.interaction_snapshot().await,
        Err(InteractionViewError::NotEnabled { .. })
    ));
    assert!(matches!(
        client
            .respond_interaction("session", "turn", "request", json!({}))
            .await,
        Err(SdkError::InvalidConfiguration(_))
    ));
    assert!(outgoing.try_recv().is_err(), "no business RPC may be sent");
}

#[tokio::test]
async fn legacy_agent_creation_json_stays_exact_and_opt_in_is_additive() {
    for enabled in [false, true] {
        let (client, mut outgoing) = client_fixture();
        let mut agent = client
            .agent(AgentDefinition::new("agent", "fixture"), Vec::new())
            .unwrap();
        if enabled {
            agent = agent.with_interactions_enabled();
        }
        let opening = tokio::spawn(async move { agent.create_session().await });
        let init = next_request(&mut outgoing).await;
        let params: InitializeParams =
            serde_json::from_value(init.params.clone().unwrap()).unwrap();
        let mut capabilities = InitializeResult::negotiate(
            &params,
            PeerInfo {
                name: "peer".into(),
                version: "test".into(),
            },
        )
        .unwrap();
        if enabled {
            capabilities
                .capabilities
                .push(CAPABILITY_INTERACTIONS.into());
        }
        respond(&client, init, capabilities);
        let start = next_request(&mut outgoing).await;
        let value = start.params.clone().unwrap();
        assert_eq!(
            value.get("interactions_enabled"),
            enabled.then_some(&json!(true))
        );
        let legacy: StartThreadParams = if enabled {
            serde_json::from_value::<StartThreadWithInteractionsParams>(value.clone())
                .unwrap()
                .session
        } else {
            serde_json::from_value(value.clone()).unwrap()
        };
        assert_eq!(legacy.agent_name.as_deref(), Some("agent"));
        let thread_id = legacy.session_id.unwrap();
        respond(
            &client,
            start,
            StartThreadResult {
                thread_id,
                created_at: "now".into(),
            },
        );
        let thread = opening.await.unwrap().unwrap();
        assert!(outgoing.try_recv().is_err());
        drop(thread);
    }
}

#[tokio::test]
async fn persistent_create_and_attach_use_additive_interaction_wrappers() {
    for attach in [false, true] {
        let (client, mut outgoing) = client_fixture();
        let agent = client
            .agent(AgentDefinition::new("persistent", "fixture"), Vec::new())
            .unwrap()
            .with_interactions_enabled();
        let key = RecoveryKey::new();
        let opening = tokio::spawn({
            let key = key.clone();
            async move {
                if attach {
                    agent.recover_session(&key).await
                } else {
                    agent.create_persistent_session(&key).await
                }
            }
        });
        let init = next_request(&mut outgoing).await;
        let params: InitializeParams =
            serde_json::from_value(init.params.clone().unwrap()).unwrap();
        let mut initialized = InitializeResult::negotiate(
            &params,
            PeerInfo {
                name: "peer".into(),
                version: "test".into(),
            },
        )
        .unwrap();
        initialized.capabilities.extend([
            CAPABILITY_INTERACTIONS.into(),
            CAPABILITY_SESSION_RECOVERY.into(),
        ]);
        respond(&client, init, initialized);

        if attach {
            let inspect = next_request(&mut outgoing).await;
            assert_eq!(inspect.method, METHOD_RECOVERY_INSPECT);
            respond(
                &client,
                inspect,
                RecoverySnapshot {
                    recovery_id: key.recovery_id.clone(),
                    revision: 2,
                    epoch: 1,
                    attached: false,
                    configuration: json!({}),
                    history: Vec::new(),
                    runs: Vec::new(),
                    unknown_executions: Vec::new(),
                },
            );
        }
        let request = next_request(&mut outgoing).await;
        assert_eq!(
            request.method,
            if attach {
                METHOD_RECOVERY_ATTACH
            } else {
                METHOD_SESSION_CREATE_PERSISTENT
            }
        );
        assert_eq!(
            request.params.as_ref().unwrap()["interactions_enabled"],
            true
        );
        let thread_id = request.params.as_ref().unwrap()["session"]["session_id"]
            .as_str()
            .unwrap()
            .to_owned();
        if attach {
            let wrapper: whale_protocol::interactions::AttachRecoveryWithInteractionsParams =
                serde_json::from_value(request.params.clone().unwrap()).unwrap();
            assert!(wrapper.interactions_enabled);
            assert_eq!(wrapper.recovery.expected_revision, 2);
        } else {
            let wrapper: CreatePersistentSessionWithInteractionsParams =
                serde_json::from_value(request.params.clone().unwrap()).unwrap();
            assert!(wrapper.interactions_enabled);
            assert_eq!(wrapper.persistent.key, key);
        }
        respond(
            &client,
            request,
            PersistentSessionResult {
                thread: StartThreadResult {
                    thread_id,
                    created_at: "now".into(),
                },
                key: key.clone(),
                epoch: if attach { 2 } else { 1 },
            },
        );
        assert!(opening.await.unwrap().is_ok());
        assert!(outgoing.try_recv().is_err());
    }
}

#[tokio::test]
async fn snapshot_barrier_merges_live_and_replay_once_in_order() {
    let (thread, mut outgoing) = open_interaction_thread().await;
    let thread_id = thread.id().to_owned();
    let client = thread.client.clone();
    let watching = tokio::spawn(async move {
        thread
            .watch_interactions(InteractionWatchOptions::default())
            .await
    });
    let get = next_request(&mut outgoing).await;
    assert_eq!(get.method, METHOD_SESSION_INTERACTIONS_GET);

    let event_two = requested_event(&thread_id, "stream", 2, "request-2");
    notify(&client, event_two.clone());
    respond(&client, get, snapshot(&thread_id, "stream", 1));
    let mut watch = watching.await.unwrap().unwrap();
    assert_eq!(watch.snapshot.cursor.seq, 1);

    let barrier = next_request(&mut outgoing).await;
    assert_eq!(barrier.method, METHOD_SESSION_INTERACTIONS_SUBSCRIBE);
    let params: SubscribeInteractionsParams =
        serde_json::from_value(barrier.params.clone().unwrap()).unwrap();
    assert_eq!(params.after.seq, 1);
    assert_eq!(params.limit, 128);
    respond(
        &client,
        barrier,
        SubscribeInteractionsResult {
            events: vec![event_two.clone()],
            resume_after: cursor(&thread_id, "stream", 2),
            through: cursor(&thread_id, "stream", 2),
            has_more: false,
            gap: None,
        },
    );
    assert_eq!(watch.events.recv().await.unwrap().unwrap(), event_two);

    let event_three = removed_event(&thread_id, "stream", 3, "request-2");
    notify(&client, event_three.clone());
    assert_eq!(watch.events.recv().await.unwrap().unwrap(), event_three);
    assert_eq!(watch.events.last_received().unwrap().seq, 3);
}

#[tokio::test]
async fn replay_pages_keep_one_fixed_high_watermark_and_merge_later_live_once() {
    let (thread, mut outgoing) = open_interaction_thread().await;
    let thread_id = thread.id().to_owned();
    let client = thread.client.clone();
    let opening = tokio::spawn({
        let thread = thread.clone();
        async move {
            thread
                .watch_interactions(InteractionWatchOptions::new(8, 1).unwrap())
                .await
        }
    });
    let get = next_request(&mut outgoing).await;
    respond(&client, get, snapshot(&thread_id, "stream", 0));
    let mut watch = opening.await.unwrap().unwrap();
    let first = next_request(&mut outgoing).await;
    let one = requested_event(&thread_id, "stream", 1, "request-1");
    respond(
        &client,
        first,
        SubscribeInteractionsResult {
            events: vec![one.clone()],
            resume_after: cursor(&thread_id, "stream", 1),
            through: cursor(&thread_id, "stream", 2),
            has_more: true,
            gap: None,
        },
    );
    let second = next_request(&mut outgoing).await;
    let second_params: SubscribeInteractionsParams =
        serde_json::from_value(second.params.clone().unwrap()).unwrap();
    assert_eq!(second_params.after.seq, 1);
    assert_eq!(second_params.through, Some(cursor(&thread_id, "stream", 2)));
    assert_eq!(second_params.limit, 1);
    let two = requested_event(&thread_id, "stream", 2, "request-2");
    let three = requested_event(&thread_id, "stream", 3, "request-3");
    notify(&client, two.clone());
    notify(&client, three.clone());
    respond(
        &client,
        second,
        SubscribeInteractionsResult {
            events: vec![two.clone()],
            resume_after: cursor(&thread_id, "stream", 2),
            through: cursor(&thread_id, "stream", 2),
            has_more: false,
            gap: None,
        },
    );
    assert_eq!(watch.events.recv().await.unwrap().unwrap(), one);
    assert_eq!(watch.events.recv().await.unwrap().unwrap(), two);
    assert_eq!(watch.events.recv().await.unwrap().unwrap(), three);
}

#[tokio::test]
async fn a_non_contiguous_live_event_recovers_through_fixed_replay() {
    let (thread, mut outgoing) = open_interaction_thread().await;
    let thread_id = thread.id().to_owned();
    let client = thread.client.clone();
    let watching = tokio::spawn(async move {
        thread
            .watch_interactions(InteractionWatchOptions::new(1, 128).unwrap())
            .await
    });
    let get = next_request(&mut outgoing).await;
    respond(&client, get, snapshot(&thread_id, "stream", 0));
    let mut watch = watching.await.unwrap().unwrap();
    let barrier = next_request(&mut outgoing).await;
    respond(
        &client,
        barrier,
        SubscribeInteractionsResult {
            events: Vec::new(),
            resume_after: cursor(&thread_id, "stream", 0),
            through: cursor(&thread_id, "stream", 0),
            has_more: false,
            gap: None,
        },
    );

    notify(
        &client,
        requested_event(&thread_id, "stream", 2, "request-2"),
    );
    let replay = next_request(&mut outgoing).await;
    let params: SubscribeInteractionsParams =
        serde_json::from_value(replay.params.clone().unwrap()).unwrap();
    assert_eq!(params.after.seq, 0);
    let one = requested_event(&thread_id, "stream", 1, "request-1");
    let two = requested_event(&thread_id, "stream", 2, "request-2");
    respond(
        &client,
        replay,
        SubscribeInteractionsResult {
            events: vec![one.clone(), two.clone()],
            resume_after: cursor(&thread_id, "stream", 2),
            through: cursor(&thread_id, "stream", 2),
            has_more: false,
            gap: None,
        },
    );
    assert_eq!(watch.events.recv().await.unwrap().unwrap(), one);
    assert_eq!(watch.events.recv().await.unwrap().unwrap(), two);
}

#[tokio::test]
async fn replay_gap_fetches_and_returns_an_authoritative_resnapshot() {
    let (thread, mut outgoing) = open_interaction_thread().await;
    let thread_id = thread.id().to_owned();
    let client = thread.client.clone();
    let old = cursor(&thread_id, "old-stream", 4);
    let subscribing = tokio::spawn(async move {
        thread
            .subscribe_interactions_from(old, InteractionSubscriptionOptions::default())
            .await
    });
    let replay = next_request(&mut outgoing).await;
    let current = cursor(&thread_id, "new-stream", 0);
    let requested = cursor(&thread_id, "old-stream", 4);
    let gap = InteractionReplayGap {
        reason: InteractionReplayGapReason::StreamReset,
        requested: requested.clone(),
        replay_floor: current.clone(),
        current: current.clone(),
    };
    respond(
        &client,
        replay,
        SubscribeInteractionsResult {
            events: Vec::new(),
            resume_after: requested,
            through: current.clone(),
            has_more: false,
            gap: Some(gap.clone()),
        },
    );
    let get = next_request(&mut outgoing).await;
    assert_eq!(get.method, METHOD_SESSION_INTERACTIONS_GET);
    let fresh = snapshot(&thread_id, "new-stream", 0);
    respond(&client, get, fresh.clone());
    match subscribing.await.unwrap() {
        Err(InteractionViewError::ResyncRequired {
            gap: actual,
            snapshot,
        }) => {
            assert_eq!(actual, gap);
            assert_eq!(snapshot, fresh);
        }
        Err(other) => panic!("unexpected error: {other:?}"),
        Ok(_) => panic!("a stream-reset gap must require a resnapshot"),
    }
}

#[tokio::test]
async fn interaction_watchers_are_independent_and_close_with_the_session() {
    let (thread, mut outgoing) = open_interaction_thread().await;
    let thread_id = thread.id().to_owned();
    let client = thread.client.clone();
    let mut watches = Vec::new();
    for _ in 0..2 {
        let next = thread.clone();
        let opening = tokio::spawn(async move {
            next.watch_interactions(InteractionWatchOptions::default())
                .await
        });
        let get = next_request(&mut outgoing).await;
        respond(&client, get, snapshot(&thread_id, "stream", 0));
        let watch = opening.await.unwrap().unwrap();
        let barrier = next_request(&mut outgoing).await;
        respond(
            &client,
            barrier,
            SubscribeInteractionsResult {
                events: Vec::new(),
                resume_after: cursor(&thread_id, "stream", 0),
                through: cursor(&thread_id, "stream", 0),
                has_more: false,
                gap: None,
            },
        );
        watches.push(watch);
    }
    let event = requested_event(&thread_id, "stream", 1, "request-1");
    notify(&client, event.clone());
    assert_eq!(watches[0].events.recv().await.unwrap().unwrap(), event);
    assert_eq!(watches[1].events.recv().await.unwrap().unwrap(), event);

    let closing = tokio::spawn({
        let thread = thread.clone();
        async move { thread.close().await }
    });
    let close = next_request(&mut outgoing).await;
    assert_eq!(close.method, METHOD_SESSION_CLOSE);
    respond(
        &client,
        close,
        CloseSessionResult {
            thread_id,
            closed: true,
        },
    );
    assert!(closing.await.unwrap().unwrap());
    assert!(watches[0].events.recv().await.is_none());
    assert!(watches[1].events.recv().await.is_none());
}

#[tokio::test]
async fn dropping_the_last_watcher_releases_its_weak_notification_route() {
    let (thread, mut outgoing) = open_interaction_thread().await;
    let thread_id = thread.id().to_owned();
    let client = thread.client.clone();
    let opening = tokio::spawn({
        let thread = thread.clone();
        async move {
            thread
                .watch_interactions(InteractionWatchOptions::default())
                .await
        }
    });
    let get = next_request(&mut outgoing).await;
    respond(&client, get, snapshot(&thread_id, "stream", 0));
    let watch = opening.await.unwrap().unwrap();
    let barrier = next_request(&mut outgoing).await;
    respond(
        &client,
        barrier,
        SubscribeInteractionsResult {
            events: Vec::new(),
            resume_after: cursor(&thread_id, "stream", 0),
            through: cursor(&thread_id, "stream", 0),
            has_more: false,
            gap: None,
        },
    );
    let route = client
        .inner
        .state
        .interaction_event_hubs
        .get(&thread_id)
        .unwrap()
        .clone();
    assert!(route.upgrade().is_some());
    drop(watch);
    tokio::time::timeout(Duration::from_secs(1), async {
        while route.upgrade().is_some()
            || client
                .inner
                .state
                .interaction_event_hubs
                .contains_key(&thread_id)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropped watcher retained its route");
    assert!(!client.inner.state.closed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn connection_loss_emits_one_stream_error_then_ends() {
    let (thread, mut outgoing) = open_interaction_thread().await;
    let thread_id = thread.id().to_owned();
    let client = thread.client.clone();
    let opening = tokio::spawn({
        let thread = thread.clone();
        async move {
            thread
                .watch_interactions(InteractionWatchOptions::default())
                .await
        }
    });
    let get = next_request(&mut outgoing).await;
    respond(&client, get, snapshot(&thread_id, "stream", 0));
    let mut watch = opening.await.unwrap().unwrap();
    let barrier = next_request(&mut outgoing).await;
    respond(
        &client,
        barrier,
        SubscribeInteractionsResult {
            events: Vec::new(),
            resume_after: cursor(&thread_id, "stream", 0),
            through: cursor(&thread_id, "stream", 0),
            has_more: false,
            gap: None,
        },
    );
    client.inner.state.disconnect("peer disappeared");
    assert!(matches!(
        watch.events.recv().await,
        Some(Err(InteractionViewError::Sdk(SdkError::ChannelClosed(message))))
            if message == "peer disappeared"
    ));
    assert!(watch.events.recv().await.is_none());
}

#[tokio::test]
async fn run_query_and_generic_response_use_independent_interaction_rpc() {
    let (client, mut outgoing) = client_fixture();
    initialize(&client, &mut outgoing, true).await;
    client.inner.state.ensure_session_open("session").unwrap();
    client.inner.state.enable_interactions("session").unwrap();
    let state = run::RunState::new("session".into(), "turn".into());
    let terminal = RunSnapshot {
        thread_id: "session".into(),
        turn_id: "turn".into(),
        status: RunStatus::Completed,
        items: Vec::new(),
        usage: Default::default(),
        pending_approvals: Vec::new(),
        tool_executions: Vec::new(),
        last_seq: 1,
        result: Some(whale_protocol::rpc::RunTurnResult {
            thread_id: "session".into(),
            turn_id: "turn".into(),
            status: TurnStatus::Completed,
            items: Vec::new(),
            usage: Default::default(),
        }),
        error: None,
    };
    state.complete(terminal.clone());
    let run = RunHandle {
        client: client.clone(),
        state,
    };

    let querying = tokio::spawn({
        let run = run.clone();
        async move { run.pending_interactions().await }
    });
    let query = next_request(&mut outgoing).await;
    assert_eq!(query.method, METHOD_TURN_INTERACTIONS_GET);
    respond(
        &client,
        query,
        TurnInteractionSnapshot::new(
            "session",
            "turn",
            cursor("session", "stream", 0),
            Vec::new(),
        )
        .unwrap(),
    );
    assert!(querying.await.unwrap().unwrap().pending.is_empty());
    assert_eq!(run.result().await.unwrap(), terminal.result.unwrap());

    let responding = tokio::spawn({
        let run = run.clone();
        async move {
            run.respond_interaction("request", json!({"answer":true}))
                .await
        }
    });
    let response_request = next_request(&mut outgoing).await;
    assert_eq!(response_request.method, METHOD_TURN_RESPOND_INTERACTION);
    assert_eq!(
        response_request.params.as_ref().unwrap()["thread_id"],
        "session"
    );
    assert_eq!(response_request.params.as_ref().unwrap()["turn_id"], "turn");
    assert_eq!(
        response_request.params.as_ref().unwrap()["request_id"],
        "request"
    );
    respond(
        &client,
        response_request,
        RespondInteractionResult {
            request_id: "request".into(),
            resolved: true,
        },
    );
    assert!(responding.await.unwrap().unwrap().resolved);
}

#[tokio::test]
async fn stable_interaction_rpc_errors_remain_sdk_rpc_errors() {
    let (client, mut outgoing) = client_fixture();
    initialize(&client, &mut outgoing, true).await;
    client.inner.state.ensure_session_open("session").unwrap();
    client.inner.state.enable_interactions("session").unwrap();
    let responding = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .respond_interaction("session", "turn", "request", json!({"answer":true}))
                .await
        }
    });
    let request = next_request(&mut outgoing).await;
    reject(&client, request, INTERACTION_CONFLICT, "different response");
    assert!(matches!(
        responding.await.unwrap(),
        Err(SdkError::Rpc {
            code: INTERACTION_CONFLICT,
            ..
        })
    ));
}

#[tokio::test]
async fn typed_approval_uses_generic_only_for_an_opted_in_capable_session() {
    for enabled in [false, true] {
        let (client, mut outgoing) = client_fixture();
        initialize(&client, &mut outgoing, enabled).await;
        client.inner.state.ensure_session_open("session").unwrap();
        if enabled {
            client.inner.state.enable_interactions("session").unwrap();
        }
        let run = RunHandle {
            client: client.clone(),
            state: run::RunState::new("session".into(), "turn".into()),
        };
        let resolving = tokio::spawn(async move {
            run.resolve_approval(
                "approval",
                RunApprovalDecision::Reject,
                None,
                Some("no".into()),
            )
            .await
        });
        let request = next_request(&mut outgoing).await;
        if enabled {
            assert_eq!(request.method, METHOD_TURN_RESPOND_INTERACTION);
            assert_eq!(
                request.params.as_ref().unwrap()["response"],
                json!({"decision":"reject","feedback":"no"})
            );
            respond(
                &client,
                request,
                RespondInteractionResult {
                    request_id: "approval".into(),
                    resolved: true,
                },
            );
        } else {
            assert_eq!(request.method, METHOD_TURN_RESOLVE_APPROVAL);
            respond(
                &client,
                request,
                ApprovalResolveResult {
                    request_id: "approval".into(),
                    resolved: true,
                },
            );
        }
        assert!(resolving.await.unwrap().unwrap());
    }
}

#[tokio::test]
async fn client_typed_approval_preserves_legacy_fallback_and_uses_the_generic_transaction_when_known(
) {
    for enabled in [false, true] {
        let (client, mut outgoing) = client_fixture();
        initialize(&client, &mut outgoing, enabled).await;
        client.inner.state.ensure_session_open("session").unwrap();
        client
            .inner
            .state
            .approval_sessions
            .insert("approval".into(), "session".into());
        client
            .inner
            .state
            .approval_turns
            .insert("approval".into(), "turn".into());
        if enabled {
            client.inner.state.enable_interactions("session").unwrap();
        }
        let resolving = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .resolve_approval(
                        "approval",
                        whale_protocol::rpc::ApprovalDecision::Approve,
                        None,
                    )
                    .await
            }
        });
        let request = next_request(&mut outgoing).await;
        if enabled {
            assert_eq!(request.method, METHOD_TURN_RESPOND_INTERACTION);
            assert_eq!(
                request.params.as_ref().unwrap()["response"],
                json!({"decision":"approve"})
            );
            respond(
                &client,
                request,
                RespondInteractionResult {
                    request_id: "approval".into(),
                    resolved: true,
                },
            );
        } else {
            assert_eq!(request.method, whale_protocol::rpc::METHOD_APPROVAL_RESOLVE);
            respond(
                &client,
                request,
                ApprovalResolveResult {
                    request_id: "approval".into(),
                    resolved: true,
                },
            );
        }
        assert!(resolving.await.unwrap().unwrap());
    }
}

struct InteractiveTool;

#[async_trait]
impl HostTool for InteractiveTool {
    fn name(&self) -> &str {
        "interactive"
    }
    fn description(&self) -> &str {
        "requests host input"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object"})
    }
    async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
        unreachable!()
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        _: Value,
    ) -> Result<CanonicalToolOutput, String> {
        let response = context
            .request_interaction(
                InteractionRequest::new("app.question", "Question", json!({}), None).unwrap(),
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(CanonicalToolOutput::text(
            response["answer"].as_str().unwrap(),
        ))
    }
}

struct CaptureInteractionContext(
    std::sync::Mutex<Option<tokio::sync::oneshot::Sender<ToolContext>>>,
);

#[async_trait]
impl HostTool for CaptureInteractionContext {
    fn name(&self) -> &str {
        "capture"
    }
    fn description(&self) -> &str {
        "captures the active callback context"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object"})
    }
    async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
        unreachable!()
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        _: Value,
    ) -> Result<CanonicalToolOutput, String> {
        assert!(self
            .0
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .send(context.clone())
            .is_ok());
        context.cancelled().await;
        Err("cancelled".into())
    }
}

#[tokio::test]
async fn nested_tool_interaction_keeps_reader_progress_and_cleans_its_route() {
    let (client, mut outgoing) = client_fixture();
    initialize(&client, &mut outgoing, true).await;
    client.inner.state.ensure_session_open("session").unwrap();
    client.inner.state.enable_interactions("session").unwrap();
    client.inner.state.tools.insert(
        ("session".into(), "interactive".into()),
        Arc::new(InteractiveTool),
    );

    client.inner.state.incoming(
        &json!({"jsonrpc":"2.0","id":"reverse","method":"tool.execute_host","params":{
            "thread_id":"session","call_id":"host-call","name":"interactive","arguments":{},
            "context":{"thread_id":"session","turn_id":"turn","call_id":"model-call"}
        }})
        .to_string(),
        &client.inner.writer,
    );
    let nested = next_request(&mut outgoing).await;
    assert_eq!(nested.method, METHOD_TURN_REQUEST_INTERACTION);
    let params: RequestInteractionParams =
        serde_json::from_value(nested.params.clone().unwrap()).unwrap();
    assert_eq!(params.host_call_id, "host-call");
    assert_eq!(params.thread_id, "session");
    assert_eq!(params.turn_id, "turn");
    respond(
        &client,
        nested,
        InteractionResponse::new(params.request_id, json!({"answer":"continue"})).unwrap(),
    );

    let reverse_response: JSONRPCResponse = serde_json::from_value(
        serde_json::from_str::<Value>(
            &tokio::time::timeout(Duration::from_secs(2), outgoing.recv())
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        reverse_response.id,
        whale_protocol::rpc::RequestId::String("reverse".into())
    );
    assert!(reverse_response.error.is_none());
    tokio::task::yield_now().await;
    assert!(client.inner.state.pending.is_empty());
    assert!(client.inner.state.tool_callbacks.is_empty());
}

#[tokio::test]
async fn cancelling_a_nested_interaction_future_leaks_no_response_route() {
    let (client, mut outgoing) = client_fixture();
    initialize(&client, &mut outgoing, true).await;
    client.inner.state.ensure_session_open("session").unwrap();
    client.inner.state.enable_interactions("session").unwrap();
    let (captured, context) = tokio::sync::oneshot::channel();
    client.inner.state.tools.insert(
        ("session".into(), "capture".into()),
        Arc::new(CaptureInteractionContext(std::sync::Mutex::new(Some(
            captured,
        )))),
    );
    client.inner.state.incoming(
        &json!({"jsonrpc":"2.0","id":"reverse-cancel","method":"tool.execute_host","params":{
            "thread_id":"session","call_id":"host-call-cancel","name":"capture","arguments":{},
            "context":{"thread_id":"session","turn_id":"turn","call_id":"model-call"}
        }})
        .to_string(),
        &client.inner.writer,
    );
    let context = tokio::time::timeout(Duration::from_secs(1), context)
        .await
        .unwrap()
        .unwrap();
    let waiting = tokio::spawn(async move {
        context
            .request_interaction(
                InteractionRequest::new("app.question", "Question", json!({}), None).unwrap(),
            )
            .await
    });
    let nested = next_request(&mut outgoing).await;
    assert_eq!(nested.method, METHOD_TURN_REQUEST_INTERACTION);
    assert_eq!(client.inner.state.pending.len(), 1);
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), async {
        while !client.inner.state.pending.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled nested request retained its response route");

    client.inner.state.incoming(
        &json!({"jsonrpc":"2.0","method":"tool.cancel_host","params":{"call_id":"host-call-cancel"}})
            .to_string(),
        &client.inner.writer,
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        while !client.inner.state.tool_callbacks.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled host callback retained its route");
}
