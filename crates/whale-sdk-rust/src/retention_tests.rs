use crate::initialization_tests::{next, valid_reply};
use crate::*;
use serde_json::json;

async fn fixture(limits: bool) -> (WhaleClient, mpsc::Receiver<String>) {
    let (tx, mut rx) = mpsc::channel(16);
    let client = WhaleClient {
        inner: Arc::new(ClientInner {
            state: ClientState::new(),
            writer: ManagedWriter::channel(tx),
            compatibility_owner: None,
        }),
    };
    let c = client.clone();
    let init = tokio::spawn(async move { c.initialize().await });
    let req = next(&mut rx).await;
    let mut value = valid_reply(&req);
    let caps = value["capabilities"].as_array_mut().unwrap();
    caps.push(json!(whale_protocol::recovery::CAPABILITY_SESSION_RECOVERY));
    if limits {
        caps.push(json!(whale_protocol::retention::CAPABILITY_SESSION_LIMITS));
    }
    reply(&client, &req, value);
    init.await.unwrap().unwrap();
    (client, rx)
}
fn reply(client: &WhaleClient, req: &Value, value: Value) {
    incoming(
        client,
        json!({"jsonrpc":"2.0","id":req["id"],"result":value}),
    );
}
fn incoming(client: &WhaleClient, value: Value) {
    client
        .inner
        .state
        .incoming(&value.to_string(), &client.inner.writer);
}
fn terminal(seq: u64) -> Value {
    json!({"thread_id":"s","turn_id":"r","status":"completed","items":[],"usage":whale_protocol::UsageMetrics::default(),
        "pending_approvals":[],"last_seq":seq,
        "result":{"thread_id":"s","turn_id":"r","status":"completed","items":[],"usage":whale_protocol::UsageMetrics::default()}})
}
fn finished(client: &WhaleClient, seq: u64) {
    incoming(
        client,
        json!({"jsonrpc":"2.0","method":"turn.event","params":{
        "thread_id":"s","turn_id":"r","seq":seq,"type":"finished","snapshot":terminal(seq)}}),
    );
}
fn live(client: &WhaleClient) -> RunHandle {
    client.inner.state.ensure_session_open("s").unwrap();
    let state = run::RunState::new("s".into(), "r".into());
    client.inner.state.cache_run(&state);
    client.inner.state.runs.insert("r".into(), state.clone());
    RunHandle {
        client: client.clone(),
        state,
    }
}

#[tokio::test]
async fn finished_releases_strong_route_but_keeps_held_result_and_buffer() {
    let (client, _) = fixture(false).await;
    let handle = live(&client);
    let weak = Arc::downgrade(&handle.state);
    finished(&client, 1);
    assert!(
        client.inner.state.runs.is_empty(),
        "finished routes must not retain payload"
    );
    let mut events = handle.events().unwrap();
    assert!(matches!(
        events.recv().await.unwrap().unwrap().payload,
        RunEventPayload::Finished { .. }
    ));
    assert!(events.recv().await.unwrap().is_none());
    assert_eq!(
        handle.result().await.unwrap().status,
        whale_protocol::TurnStatus::Completed
    );
    drop(handle);
    assert!(weak.upgrade().is_some(), "held event stream owns its state");
    drop(events);
    assert!(weak.upgrade().is_none());
    assert!(client.inner.state.run_handles.lock().unwrap().is_empty());
    finished(&client, 1);
    assert!(
        client.inner.state.runs.is_empty(),
        "late events must not resurrect payload"
    );
    client.close().await;
}

#[tokio::test]
async fn terminal_query_does_not_close_a_live_subscription_before_finished() {
    let (client, mut rx) = fixture(false).await;
    let handle = live(&client);
    let h = handle.clone();
    let query = tokio::spawn(async move { h.snapshot().await });
    let req = next(&mut rx).await;
    reply(&client, &req, terminal(2));
    query.await.unwrap().unwrap();
    assert!(client.inner.state.runs.contains_key("r"));
    incoming(
        &client,
        json!({"jsonrpc":"2.0","method":"turn.event","params":{
        "thread_id":"s","turn_id":"r","seq":1,"type":"stream",
        "event":{"type":"text_delta","turn_id":"r","item_id":"i","delta":"last"}}}),
    );
    finished(&client, 2);
    let mut events = handle.events().unwrap();
    assert_eq!(events.recv().await.unwrap().unwrap().seq, 1);
    assert_eq!(events.recv().await.unwrap().unwrap().seq, 2);
    assert!(events.recv().await.unwrap().is_none());
    client.close().await;
}

#[tokio::test]
async fn newly_queried_terminal_has_an_ended_subscription() {
    let (client, mut rx) = fixture(false).await;
    let c = client.clone();
    let query = tokio::spawn(async move { c.get_run("s", "r").await });
    let req = next(&mut rx).await;
    reply(&client, &req, terminal(1));
    let handle = query.await.unwrap().unwrap();
    assert!(client.inner.state.runs.is_empty());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), handle.events().unwrap().recv())
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
    client.close().await;
}

#[tokio::test]
async fn cached_handle_queries_are_authoritative_without_overwriting_delivered_result() {
    let (client, mut rx) = fixture(false).await;
    let handle = live(&client);
    finished(&client, 1);
    let c = client.clone();
    let query = tokio::spawn(async move { c.get_run("s", "r").await });
    let req = next(&mut rx).await;
    reply(&client, &req, terminal(1));
    let cached = query.await.unwrap().unwrap();
    assert!(Arc::ptr_eq(&cached.state, &handle.state));
    let c = client.clone();
    let query = tokio::spawn(async move { c.get_run("s", "r").await });
    let req = next(&mut rx).await;
    incoming(
        &client,
        json!({"jsonrpc":"2.0","id":req["id"],"error":{"code":-32030,"message":"expired"}}),
    );
    assert!(matches!(query.await.unwrap(), Err(SdkError::RunExpired(_))));
    assert_eq!(
        handle.result().await.unwrap().status,
        whale_protocol::TurnStatus::Completed
    );
    assert!(client.inner.state.runs.is_empty());
    let thread = WhaleThread {
        client: client.clone(),
        thread_id: "s".into(),
        recovery_key: None,
        max_steps: 10,
        timeout_ms: None,
    };
    let task = tokio::spawn(async move { thread.start_turn("too many").await });
    let req = next(&mut rx).await;
    incoming(
        &client,
        json!({"jsonrpc":"2.0","id":req["id"],"error":{"code":-32031,"message":"quota"}}),
    );
    assert!(matches!(
        task.await.unwrap(),
        Err(SdkError::LimitExceeded(_))
    ));
    assert!(client.inner.state.runs.is_empty());
    client.close().await;
    assert_eq!(
        handle.result().await.unwrap().status,
        whale_protocol::TurnStatus::Completed
    );
}

#[tokio::test]
async fn missing_limits_capability_rejects_all_entrypoints_before_any_binding_or_rpc() {
    for mode in 0..3 {
        let (client, mut rx) = fixture(false).await;
        let mut def = AgentDefinition::new("limited", "test");
        def.provider_ref = Some("native".into());
        def.limits = Some(SessionLimits {
            max_accepted_turns: Some(1),
            ..Default::default()
        });
        let agent = client.agent(def, vec![]).unwrap();
        let key = RecoveryKey::new();
        let task = async {
            match mode {
                0 => agent.create_session().await,
                1 => agent.create_persistent_session(&key).await,
                _ => agent.recover_session(&key).await,
            }
        };
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(100), task).await,
            Ok(Err(SdkError::ProtocolCompatibility(_)))
        ));
        assert!(rx.try_recv().is_err());
        assert!(client.inner.state.tools.is_empty());
        assert!(client.inner.state.context_policies.is_empty());
        client.close().await;
    }
}

#[tokio::test]
async fn limits_round_trip_in_ordinary_and_persistent_creation() {
    for persistent in [false, true] {
        let (client, mut rx) = fixture(true).await;
        let mut def = AgentDefinition::new("limited", "test");
        def.limits = Some(SessionLimits {
            max_accepted_turns: Some(u64::MAX),
            max_history_bytes: Some(4096),
            max_model_request_bytes: Some(8192),
        });
        let agent = client.agent(def, vec![]).unwrap();
        let task = tokio::spawn(async move {
            if persistent {
                agent.create_persistent_session(&RecoveryKey::new()).await
            } else {
                agent.create_session().await
            }
        });
        let req = next(&mut rx).await;
        let session = if persistent {
            &req["params"]["session"]
        } else {
            &req["params"]
        };
        assert_eq!(
            session["limits"],
            json!({"max_accepted_turns":u64::MAX,"max_history_bytes":4096,"max_model_request_bytes":8192})
        );
        let thread = json!({"thread_id":session["session_id"],"created_at":"now"});
        let result = if persistent {
            json!({"thread":thread,"key":req["params"]["key"],"epoch":1})
        } else {
            thread
        };
        reply(&client, &req, result);
        task.await.unwrap().unwrap();
        client.close().await;
    }
}

#[tokio::test]
async fn rejected_query_preserves_existing_live_route_but_drops_new_query_state() {
    let (client, mut rx) = fixture(false).await;
    let handle = live(&client);
    let lifecycle = client
        .inner
        .state
        .sessions
        .get("s")
        .unwrap()
        .value()
        .clone();
    let hub = client
        .inner
        .state
        .session_event_hubs
        .get("s")
        .unwrap()
        .value()
        .clone();
    for turn in ["r", "missing"] {
        let c = client.clone();
        let query = tokio::spawn(async move { c.get_run("s", turn).await });
        let req = next(&mut rx).await;
        incoming(
            &client,
            json!({"jsonrpc":"2.0","id":req["id"],"error":{"code":-32602,"message":"unavailable"}}),
        );
        assert!(query.await.unwrap().is_err());
        assert_eq!(client.inner.state.runs.len(), 1);
        assert!(client.inner.state.runs.contains_key("r"));
        assert_eq!(client.inner.state.run_handles.lock().unwrap().len(), 1);
        assert!(Arc::ptr_eq(
            client.inner.state.sessions.get("s").unwrap().value(),
            &lifecycle
        ));
        assert!(Arc::ptr_eq(
            client
                .inner
                .state
                .session_event_hubs
                .get("s")
                .unwrap()
                .value(),
            &hub
        ));
    }
    finished(&client, 1);
    assert_eq!(
        handle.result().await.unwrap().status,
        whale_protocol::TurnStatus::Completed
    );
    client.close().await;
}

#[tokio::test]
async fn abandoning_new_query_releases_its_strong_route() {
    let (client, mut rx) = fixture(false).await;
    let c = client.clone();
    let query = tokio::spawn(async move { c.get_run("s", "missing").await });
    let _request = next(&mut rx).await;
    assert!(client.inner.state.runs.contains_key("missing"));
    query.abort();
    let _ = query.await;
    assert!(
        client.inner.state.runs.is_empty(),
        "abandoned queries must release unpublished state"
    );
    assert!(client.inner.state.run_handles.lock().unwrap().is_empty());
    assert!(client.inner.state.pending.is_empty());
    client.close().await;
}

#[tokio::test]
async fn aborting_start_before_ack_releases_an_unclaimed_run_route() {
    let (client, mut rx) = fixture(false).await;
    client.inner.state.ensure_session_open("s").unwrap();
    let thread = WhaleThread {
        recovery_key: None,
        client: client.clone(),
        thread_id: "s".into(),
        max_steps: 10,
        timeout_ms: None,
    };
    let starting = tokio::spawn(async move { thread.start_turn("cancel me").await });
    let request = next(&mut rx).await;
    let turn_id = request["params"]["turn_id"].as_str().unwrap().to_owned();
    assert!(client.inner.state.runs.contains_key(&turn_id));
    starting.abort();
    let _ = starting.await;
    assert!(
        !client.inner.state.runs.contains_key(&turn_id),
        "aborted start retained an unclaimed strong route"
    );
    assert!(!client
        .inner
        .state
        .run_handles
        .lock()
        .unwrap()
        .contains_key(&turn_id));
    client.close().await;
}

#[tokio::test]
async fn aborting_start_does_not_remove_a_route_claimed_by_a_real_event() {
    let (client, mut rx) = fixture(false).await;
    client.inner.state.ensure_session_open("s").unwrap();
    let thread = WhaleThread {
        recovery_key: None,
        client: client.clone(),
        thread_id: "s".into(),
        max_steps: 10,
        timeout_ms: None,
    };
    let starting = tokio::spawn(async move { thread.start_turn("keep me").await });
    let request = next(&mut rx).await;
    let turn_id = request["params"]["turn_id"].as_str().unwrap().to_owned();
    let route = client
        .inner
        .state
        .runs
        .get(&turn_id)
        .unwrap()
        .value()
        .clone();
    incoming(
        &client,
        json!({"jsonrpc":"2.0","method":"turn.event","params":{
            "thread_id":"s","turn_id":turn_id,"seq":1,"type":"stream",
            "event":{"type":"text_delta","turn_id":turn_id,"item_id":"i","delta":"live"}}}),
    );
    starting.abort();
    let _ = starting.await;
    let retained = client.inner.state.runs.get(&turn_id).unwrap();
    assert!(Arc::ptr_eq(retained.value(), &route));
    drop(retained);
    client.close().await;
}

#[tokio::test]
async fn failed_concurrent_unknown_queries_remove_only_their_provisional_session() {
    let (client, mut rx) = fixture(false).await;
    client.inner.state.ensure_session_open("s").unwrap();
    let known_lifecycle = client
        .inner
        .state
        .sessions
        .get("s")
        .unwrap()
        .value()
        .clone();
    let known_hub = client
        .inner
        .state
        .session_event_hubs
        .get("s")
        .unwrap()
        .value()
        .clone();

    let first_client = client.clone();
    let first =
        tokio::spawn(async move { first_client.get_run("unknown-session", "missing-a").await });
    let first_request = next(&mut rx).await;
    let second_client = client.clone();
    let second =
        tokio::spawn(async move { second_client.get_run("unknown-session", "missing-b").await });
    let second_request = next(&mut rx).await;
    incoming(
        &client,
        json!({"jsonrpc":"2.0","id":first_request["id"],"error":{"code":-32602,"message":"unknown"}}),
    );
    assert!(first.await.unwrap().is_err());
    assert!(client.inner.state.sessions.contains_key("unknown-session"));
    incoming(
        &client,
        json!({"jsonrpc":"2.0","id":second_request["id"],"error":{"code":-32602,"message":"unknown"}}),
    );
    assert!(second.await.unwrap().is_err());
    assert!(!client.inner.state.sessions.contains_key("unknown-session"));
    assert!(!client
        .inner
        .state
        .session_event_hubs
        .contains_key("unknown-session"));
    assert!(Arc::ptr_eq(
        client.inner.state.sessions.get("s").unwrap().value(),
        &known_lifecycle
    ));
    assert!(Arc::ptr_eq(
        client
            .inner
            .state
            .session_event_hubs
            .get("s")
            .unwrap()
            .value(),
        &known_hub
    ));
    client.close().await;
}

#[tokio::test]
async fn successful_unknown_query_commits_the_session_against_a_concurrent_failure() {
    let (client, mut rx) = fixture(false).await;
    let successful_client = client.clone();
    let successful = tokio::spawn(async move {
        successful_client
            .get_run("discovered-session", "found")
            .await
    });
    let successful_request = next(&mut rx).await;
    let failing_client = client.clone();
    let failing = tokio::spawn(async move {
        failing_client
            .get_run("discovered-session", "missing")
            .await
    });
    let failing_request = next(&mut rx).await;
    reply(
        &client,
        &successful_request,
        json!({
            "thread_id":"discovered-session","turn_id":"found","status":"running",
            "items":[],"usage":whale_protocol::UsageMetrics::default(),
            "pending_approvals":[],"last_seq":0,"result":null
        }),
    );
    let handle = successful.await.unwrap().unwrap();
    incoming(
        &client,
        json!({"jsonrpc":"2.0","id":failing_request["id"],"error":{"code":-32602,"message":"unknown"}}),
    );
    assert!(failing.await.unwrap().is_err());
    assert!(client
        .inner
        .state
        .sessions
        .contains_key("discovered-session"));
    assert!(client
        .inner
        .state
        .session_event_hubs
        .contains_key("discovered-session"));
    assert!(client.inner.state.runs.contains_key(handle.id()));
    client.close().await;
}

#[tokio::test]
async fn one_failed_or_abandoned_query_cannot_break_another_pending_query() {
    for abort in [false, true] {
        let (client, mut rx) = fixture(false).await;
        let c = client.clone();
        let first = tokio::spawn(async move { c.get_run("s", "r").await });
        let a = next(&mut rx).await;
        let c = client.clone();
        let second = tokio::spawn(async move { c.get_run("s", "r").await });
        let b = next(&mut rx).await;
        if abort {
            first.abort();
            let _ = first.await;
        } else {
            incoming(
                &client,
                json!({"jsonrpc":"2.0","id":a["id"],"error":{"code":-32603,"message":"query unavailable"}}),
            );
            assert!(first.await.unwrap().is_err());
        }
        let mut running = terminal(0);
        running["status"] = json!("running");
        running["result"] = Value::Null;
        reply(&client, &b, running);
        let handle = second.await.unwrap().unwrap();
        assert!(
            client.inner.state.runs.contains_key("r"),
            "remaining successful query still owns live routing"
        );
        finished(&client, 1);
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(100), handle.result())
                .await
                .unwrap()
                .unwrap()
                .status,
            whale_protocol::TurnStatus::Completed
        );
        assert!(client.inner.state.runs.is_empty());
        client.close().await;
    }
}

#[tokio::test]
async fn real_stream_before_first_query_reply_makes_the_subscription_live() {
    let (client, mut rx) = fixture(false).await;
    let c = client.clone();
    let query = tokio::spawn(async move { c.get_run("s", "r").await });
    let request = next(&mut rx).await;
    incoming(
        &client,
        json!({"jsonrpc":"2.0","method":"turn.event","params":{
        "thread_id":"s","turn_id":"r","seq":1,"type":"stream",
        "event":{"type":"text_delta","turn_id":"r","item_id":"i","delta":"last"}}}),
    );
    reply(&client, &request, terminal(2));
    let handle = query.await.unwrap().unwrap();
    assert!(
        client.inner.state.runs.contains_key("r"),
        "an actual event establishes a live subscription before the reply"
    );
    let mut events = handle.events().unwrap();
    assert_eq!(events.recv().await.unwrap().unwrap().seq, 1);
    finished(&client, 2);
    assert!(matches!(
        events.recv().await.unwrap().unwrap().payload,
        RunEventPayload::Finished { .. }
    ));
    assert!(events.recv().await.unwrap().is_none());
    client.close().await;
}

#[tokio::test]
async fn retry_reacquires_a_query_state_still_held_by_an_incoming_callback() {
    let (client, mut rx) = fixture(false).await;
    let c = client.clone();
    let query = tokio::spawn(async move { c.get_run("s", "r").await });
    let request = next(&mut rx).await;
    // The reader clones a state before dispatching the event. Model that
    // in-flight ownership while the query fails and removes its tentative route.
    let incoming_state = client.inner.state.runs.get("r").unwrap().value().clone();
    incoming(
        &client,
        json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32603,"message":"unavailable"}}),
    );
    assert!(query.await.unwrap().is_err());
    assert!(client.inner.state.runs.is_empty());
    let c = client.clone();
    let retry = tokio::spawn(async move { c.get_run("s", "r").await });
    let request = next(&mut rx).await;
    let mut running = terminal(0);
    running["status"] = json!("running");
    running["result"] = Value::Null;
    reply(&client, &request, running);
    let handle = retry.await.unwrap().unwrap();
    assert!(Arc::ptr_eq(&incoming_state, &handle.state));
    assert!(
        client.inner.state.runs.contains_key("r"),
        "a pending cached state needs a new route lease"
    );
    drop(incoming_state);
    finished(&client, 1);
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), handle.result())
            .await
            .unwrap()
            .unwrap()
            .status,
        whale_protocol::TurnStatus::Completed
    );
    client.close().await;
}

#[tokio::test]
async fn terminal_query_waits_for_another_pending_query_that_observes_running() {
    let (client, mut rx) = fixture(false).await;
    let c = client.clone();
    let first = tokio::spawn(async move { c.get_run("s", "r").await });
    let a = next(&mut rx).await;
    let c = client.clone();
    let second = tokio::spawn(async move { c.get_run("s", "r").await });
    let b = next(&mut rx).await;

    reply(&client, &a, terminal(2));
    let handle = first.await.unwrap().unwrap();
    assert!(
        client.inner.state.runs.contains_key("r"),
        "a terminal reply cannot close a route while another query may observe running"
    );
    let mut running = terminal(0);
    running["status"] = json!("running");
    running["result"] = Value::Null;
    reply(&client, &b, running);
    let other = second.await.unwrap().unwrap();
    assert!(Arc::ptr_eq(&handle.state, &other.state));

    incoming(
        &client,
        json!({"jsonrpc":"2.0","method":"turn.event","params":{
            "thread_id":"s","turn_id":"r","seq":1,"type":"stream",
            "event":{"type":"text_delta","turn_id":"r","item_id":"i","delta":"late"}}}),
    );
    finished(&client, 2);
    let mut events = handle.events().unwrap();
    assert_eq!(events.recv().await.unwrap().unwrap().seq, 1);
    assert!(matches!(
        events.recv().await.unwrap().unwrap().payload,
        RunEventPayload::Finished { .. }
    ));
    assert!(events.recv().await.unwrap().is_none());
    client.close().await;
}
