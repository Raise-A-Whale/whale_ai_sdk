use std::{sync::Arc, time::Duration};
use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
use whale_sdk_rust::{
    AgentDefinition, DaemonServer, RecoveryKey, SessionEventPayloadV2, SessionHistoryAnchor,
    SessionHistoryOptions, SessionHistoryPageCursor, SessionLifecycleState, SessionListCursor,
    SessionListOptions, SessionManagementError, SessionManagementWatchOptions,
    SessionPersistenceV2, SubscriptionOptions, WhaleClient,
};
use whale_store::{MemoryStore, StoreRuntime};

#[test]
fn public_options_and_capability_guards() {
    let list = SessionListOptions::default();
    assert_eq!(list.limit(), 64);
    assert!(list.cursor().is_none());
    assert!(matches!(
        SessionListOptions::new(0),
        Err(SessionManagementError::InvalidOptions { field: "limit", .. })
    ));
    assert!(matches!(
        SessionListOptions::new(257),
        Err(SessionManagementError::InvalidOptions { field: "limit", .. })
    ));
    let list_cursor = SessionListCursor::new("list-token").unwrap();
    let continued = SessionListOptions::new(3)
        .unwrap()
        .with_cursor(list_cursor.clone());
    assert_eq!(continued.limit(), 3);
    assert_eq!(continued.cursor(), Some(&list_cursor));

    let history = SessionHistoryOptions::default();
    assert_eq!(history.limit(), 128);
    assert!(history.anchor().is_none());
    assert!(history.cursor().is_none());
    assert!(matches!(
        SessionHistoryOptions::new(0),
        Err(SessionManagementError::InvalidOptions { field: "limit", .. })
    ));
    assert!(matches!(
        SessionHistoryOptions::new(257),
        Err(SessionManagementError::InvalidOptions { field: "limit", .. })
    ));
    let anchor = SessionHistoryAnchor {
        thread_id: "session".into(),
        stream_id: "stream".into(),
        index: 9,
    };
    let anchored = SessionHistoryOptions::new(3)
        .unwrap()
        .before(anchor.clone())
        .unwrap();
    assert_eq!(anchored.anchor(), Some(&anchor));
    assert!(matches!(
        anchored.continue_from(SessionHistoryPageCursor::new("history-token").unwrap()),
        Err(SessionManagementError::InvalidOptions {
            field: "cursor",
            ..
        })
    ));
    let cursor = SessionHistoryPageCursor::new("history-token").unwrap();
    let continued = SessionHistoryOptions::new(3)
        .unwrap()
        .continue_from(cursor.clone())
        .unwrap();
    assert_eq!(continued.cursor(), Some(&cursor));
    assert!(matches!(
        continued.before(anchor),
        Err(SessionManagementError::InvalidOptions {
            field: "before",
            ..
        })
    ));

    let stream = SubscriptionOptions::default();
    let watch = SessionManagementWatchOptions::default();
    assert_eq!(watch.history_limit(), 256);
    assert_eq!(watch.subscription().buffer_capacity(), 64);
    assert_eq!(watch.subscription().replay_page_size(), 128);
    assert!(matches!(
        SessionManagementWatchOptions::new(0, stream.clone()),
        Err(SessionManagementError::InvalidOptions {
            field: "history_limit",
            ..
        })
    ));
    assert!(matches!(
        SessionManagementWatchOptions::new(1025, stream),
        Err(SessionManagementError::InvalidOptions {
            field: "history_limit",
            ..
        })
    ));
}

async fn real_server() -> Arc<DaemonServer> {
    let gate = Arc::new(ApprovalGate::new());
    let coordinator = Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    ));
    let engine = AgentEngine::new(coordinator).with_stream_provider(Arc::new(|_, _| {
        Ok(Box::pin(futures::stream::iter(vec![
            Ok(whale_protocol::AgentStreamEvent::ItemCompleted {
                turn_id: "provider".into(),
                item: whale_protocol::CanonicalItem::assistant_text(
                    "answer",
                    whale_protocol::MessagePhase::FinalAnswer,
                ),
            }),
            Ok(whale_protocol::AgentStreamEvent::TurnCompleted {
                turn_id: "provider".into(),
                thread_id: "provider".into(),
                usage: Default::default(),
            }),
        ])) as whale_adapters::BoxedEventStream)
    }));
    let store = Arc::new(
        StoreRuntime::open(Arc::new(MemoryStore::new()))
            .await
            .unwrap(),
    );
    Arc::new(DaemonServer::new(Arc::new(engine), gate).with_store_runtime(store))
}

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("real daemon Session management operation stalled")
}

#[tokio::test]
async fn real_daemon_owner_history_cas_and_lifecycle() {
    let server = real_server().await;
    let first = WhaleClient::in_process(server.clone());
    let second = WhaleClient::in_process(server);
    let thread = first.create_thread("fixture", None).await.unwrap();
    let sibling = first.create_thread("fixture", None).await.unwrap();
    let foreign = second.create_thread("fixture", None).await.unwrap();

    let first_page = first
        .list_sessions(SessionListOptions::default())
        .await
        .unwrap();
    assert_eq!(first_page.sessions.len(), 2);
    assert!(first_page
        .sessions
        .iter()
        .any(|session| session.summary.thread_id == thread.id()));
    let second_page = second
        .list_sessions(SessionListOptions::default())
        .await
        .unwrap();
    assert_eq!(second_page.sessions.len(), 1);
    assert_eq!(second_page.sessions[0].summary.thread_id, foreign.id());
    assert!(matches!(
        second.session_view(thread.id()).unwrap().snapshot().await,
        Err(SessionManagementError::SessionUnavailable)
    ));

    let fixed_first = first
        .list_sessions(SessionListOptions::new(1).unwrap())
        .await
        .unwrap();
    let fixed_cursor = fixed_first.next_cursor.clone().unwrap();
    let late = first.create_thread("fixture", None).await.unwrap();
    let fixed_second = first
        .list_sessions(
            SessionListOptions::new(1)
                .unwrap()
                .with_cursor(fixed_cursor),
        )
        .await
        .unwrap();
    assert_eq!(fixed_second.sessions.len(), 1);
    let fixed_ids = fixed_first
        .sessions
        .iter()
        .chain(&fixed_second.sessions)
        .map(|entry| entry.summary.thread_id.as_str())
        .collect::<Vec<_>>();
    assert!(!fixed_ids.contains(&late.id()));

    let view = thread.session_view();
    let initial = view.snapshot().await.unwrap();
    let expected = initial.summary.view_revision;
    let left = thread.clone();
    let right = thread.clone();
    let (left, right) = tokio::join!(
        left.replace_metadata(
            expected,
            serde_json::Map::from_iter([("winner".into(), "left".into())]),
        ),
        right.replace_metadata(
            expected,
            serde_json::Map::from_iter([("winner".into(), "right".into())]),
        ),
    );
    let outcomes = [left, right];
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, Err(SessionManagementError::RevisionConflict { .. })))
            .count(),
        1
    );

    let run = thread.start_turn("question").await.unwrap();
    bounded(run.result()).await.unwrap();
    let snapshot = view.snapshot().await.unwrap();
    assert!(snapshot.history.total_items >= 2);
    let history = view
        .history_page(SessionHistoryOptions::default())
        .await
        .unwrap();
    assert_eq!(history.start_index, 0);
    assert_eq!(history.end_index, snapshot.history.total_items);
    assert_eq!(history.items, snapshot.history.items);

    let before_append = SessionHistoryAnchor {
        thread_id: thread.id().into(),
        stream_id: snapshot.cursor.stream_id.clone(),
        index: snapshot.history.total_items,
    };
    bounded(thread.start_turn("later").await.unwrap().result())
        .await
        .unwrap();
    let fixed_history = view
        .history_page(
            SessionHistoryOptions::default()
                .before(before_append.clone())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(fixed_history.through, before_append);
    assert!(fixed_history.current_end.index > fixed_history.through.index);

    let mut watch = view
        .watch(SessionManagementWatchOptions::default())
        .await
        .unwrap();
    let before_close = watch.snapshot.cursor.clone();
    assert!(bounded(thread.close()).await.unwrap());
    let mut lifecycle = Vec::new();
    while let Some(event) = bounded(watch.events.recv()).await {
        if let SessionEventPayloadV2::LifecycleChanged { lifecycle: state } = event.unwrap().payload
        {
            lifecycle.push(state);
        }
    }
    assert_eq!(
        lifecycle,
        vec![
            SessionLifecycleState::Closing,
            SessionLifecycleState::Closed
        ]
    );

    let mut late_stream = view
        .subscribe_from(before_close, SubscriptionOptions::default())
        .await
        .unwrap();
    let mut late_lifecycle = Vec::new();
    while let Some(event) = bounded(late_stream.recv()).await {
        if let SessionEventPayloadV2::LifecycleChanged { lifecycle: state } = event.unwrap().payload
        {
            late_lifecycle.push(state);
        }
    }
    assert_eq!(late_lifecycle, lifecycle);

    assert_eq!(
        view.snapshot().await.unwrap().lifecycle,
        SessionLifecycleState::Closed
    );
    assert!(matches!(
        thread.replace_metadata(0, serde_json::Map::new()).await,
        Err(SessionManagementError::SessionNotOpen {
            lifecycle: SessionLifecycleState::Closed
        })
    ));
    let closed = first
        .list_sessions(SessionListOptions::default())
        .await
        .unwrap();
    assert!(closed.sessions.iter().any(|entry| {
        entry.summary.thread_id == thread.id() && entry.lifecycle == SessionLifecycleState::Closed
    }));

    assert!(sibling.close().await.unwrap());
    assert!(late.close().await.unwrap());
    assert!(foreign.close().await.unwrap());
    first.close().await;
    second.close().await;
}

#[tokio::test]
async fn real_daemon_connection_eof_removes_the_owner_namespace() {
    let server = real_server().await;
    let client = WhaleClient::in_process(server.clone());
    let session = client.create_thread("fixture", None).await.unwrap();
    let thread_id = session.id().to_owned();
    assert!(server.sessions().contains_key(&thread_id));

    client.close().await;
    bounded(async {
        while server.sessions().contains_key(&thread_id) {
            tokio::task::yield_now().await;
        }
    })
    .await;

    let replacement = WhaleClient::in_process(server);
    assert!(replacement
        .list_sessions(SessionListOptions::default())
        .await
        .unwrap()
        .sessions
        .is_empty());
    replacement.close().await;
}

#[tokio::test]
async fn persistent_metadata_survives_fresh_attachment_and_old_anchor_resets() {
    let server = real_server().await;
    let client = WhaleClient::in_process(server);
    let agent = client
        .agent(AgentDefinition::new("persistent", "fixture"), Vec::new())
        .unwrap();
    let key = RecoveryKey::new();
    let first = agent.create_persistent_session(&key).await.unwrap();
    let old_view = first.session_view();
    let initial = old_view.snapshot().await.unwrap();
    assert!(matches!(
        initial.persistence,
        SessionPersistenceV2::Persistent { ref recovery_id } if recovery_id == &key.recovery_id
    ));
    first
        .replace_metadata(
            initial.summary.view_revision,
            serde_json::Map::from_iter([("title".into(), "durable".into())]),
        )
        .await
        .unwrap();
    let old = old_view.snapshot().await.unwrap();
    assert!(first.close().await.unwrap());

    let attached = agent.recover_session(&key).await.unwrap();
    assert_ne!(attached.id(), first.id());
    let fresh = attached.session_view().snapshot().await.unwrap();
    assert_ne!(fresh.cursor.stream_id, old.cursor.stream_id);
    assert_eq!(fresh.summary.metadata["title"], "durable");
    let old_anchor = SessionHistoryAnchor {
        thread_id: attached.id().into(),
        stream_id: old.cursor.stream_id.clone(),
        index: old.history.total_items,
    };
    match attached
        .session_view()
        .history_page(
            SessionHistoryOptions::default()
                .before(old_anchor.clone())
                .unwrap(),
        )
        .await
    {
        Err(SessionManagementError::HistoryStreamReset { requested, .. }) => {
            assert_eq!(requested, old_anchor)
        }
        other => panic!("old attachment anchor did not report stream reset: {other:?}"),
    }
    assert_eq!(
        old_view.snapshot().await.unwrap().lifecycle,
        SessionLifecycleState::Closed
    );

    assert!(attached.close().await.unwrap());
    client.close().await;
}
