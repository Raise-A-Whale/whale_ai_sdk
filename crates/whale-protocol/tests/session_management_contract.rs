use serde_json::{json, Map};
use std::any::TypeId;
use whale_protocol::{
    initialization::PROTOCOL_CAPABILITIES,
    runs::{RunEvent, RunEventPayload, RunSnapshot, RunStatus},
    session_management::*,
    session_views::{
        SessionCursor, SessionEventEnvelope, SessionEventPayload, SessionHistoryWindow,
        SessionRunView, CAPABILITY_SESSION_EVENT_REPLAY, CAPABILITY_SESSION_VIEWS,
        METHOD_SESSION_EVENT, METHOD_SESSION_GET, METHOD_SESSION_SUBSCRIBE,
    },
    AgentStreamEvent, CanonicalItem, UsageMetrics,
};

fn cursor_v2(stream_id: &str, seq: u64) -> SessionCursorV2 {
    SessionCursorV2 {
        thread_id: "thread-a".into(),
        stream_id: stream_id.into(),
        seq,
    }
}

fn summary_v2(revision: u64) -> SessionSummaryV2 {
    let mut summary = SessionSummaryV2::new("thread-a", 10);
    summary.agent_name = Some("researcher".into());
    summary.metadata.insert("title".into(), json!("Demo"));
    summary.updated_at_ms = 10 + revision;
    summary.view_revision = revision;
    summary
}

fn snapshot_v2(lifecycle: SessionLifecycleState, revision: u64) -> SessionSnapshotV2 {
    SessionSnapshotV2::new(
        summary_v2(revision),
        lifecycle,
        SessionPersistenceV2::Ephemeral,
        SessionHistoryWindow::new(8).unwrap(),
        cursor_v2("stream-v2", revision),
    )
}

fn running_snapshot() -> RunSnapshot {
    RunSnapshot {
        tool_executions: Vec::new(),
        thread_id: "thread-a".into(),
        turn_id: "turn-a".into(),
        status: RunStatus::Running,
        items: Vec::new(),
        usage: UsageMetrics::default(),
        pending_approvals: Vec::new(),
        last_seq: 0,
        result: None,
        error: None,
    }
}

fn metadata_event(seq: u64, value: &str) -> SessionEventEnvelopeV2 {
    SessionEventEnvelopeV2::new(
        "thread-a",
        cursor_v2("stream-v2", seq),
        10 + seq,
        SessionEventPayloadV2::MetadataChanged {
            metadata: Map::from_iter([("title".into(), json!(value))]),
        },
    )
}

#[test]
fn v1_session_wire_remains_frozen() {
    let envelope = SessionEventEnvelope::new(
        "thread-a",
        SessionCursor {
            thread_id: "thread-a".into(),
            stream_id: "stream-v1".into(),
            seq: 1,
        },
        14,
        SessionEventPayload::RunEvent {
            event: RunEvent {
                thread_id: "thread-a".into(),
                turn_id: "turn-a".into(),
                seq: 1,
                payload: RunEventPayload::Stream {
                    event: AgentStreamEvent::TextDelta {
                        turn_id: "turn-a".into(),
                        item_id: "item-a".into(),
                        delta: "hello".into(),
                    },
                },
            },
        },
    );

    assert_eq!(
        serde_json::to_value(envelope).unwrap(),
        json!({
            "thread_id": "thread-a",
            "cursor": {"thread_id":"thread-a","stream_id":"stream-v1","seq":1},
            "occurred_at_ms": 14,
            "type": "run_event",
            "event": {
                "thread_id":"thread-a",
                "turn_id":"turn-a",
                "seq":1,
                "type":"stream",
                "event": {
                    "type":"text_delta",
                    "turn_id":"turn-a",
                    "item_id":"item-a",
                    "delta":"hello"
                }
            }
        })
    );
    assert_eq!(CAPABILITY_SESSION_VIEWS, "session_views.v1");
    assert_eq!(CAPABILITY_SESSION_EVENT_REPLAY, "session_event_replay.v1");
    assert_eq!(METHOD_SESSION_GET, "session.get");
    assert_eq!(METHOD_SESSION_SUBSCRIBE, "session.subscribe");
    assert_eq!(METHOD_SESSION_EVENT, "session.event");
    assert!(!PROTOCOL_CAPABILITIES.contains(&CAPABILITY_SESSION_VIEWS));
    assert!(!PROTOCOL_CAPABILITIES.contains(&CAPABILITY_SESSION_EVENT_REPLAY));
}

#[test]
fn v2_capabilities_and_methods_are_optional_and_versioned() {
    assert_eq!(CAPABILITY_SESSION_CATALOG, "session_catalog.v1");
    assert_eq!(CAPABILITY_SESSION_HISTORY, "session_history.v1");
    assert_eq!(CAPABILITY_SESSION_METADATA_CAS, "session_metadata_cas.v1");
    assert_eq!(
        CAPABILITY_SESSION_LIFECYCLE_REPLAY,
        "session_lifecycle_replay.v1"
    );
    for capability in [
        CAPABILITY_SESSION_CATALOG,
        CAPABILITY_SESSION_HISTORY,
        CAPABILITY_SESSION_METADATA_CAS,
        CAPABILITY_SESSION_LIFECYCLE_REPLAY,
    ] {
        assert!(!PROTOCOL_CAPABILITIES.contains(&capability));
    }
    assert_eq!(METHOD_SESSION_GET_V2, "session.get.v2");
    assert_eq!(METHOD_SESSION_SUBSCRIBE_V2, "session.subscribe.v2");
    assert_eq!(METHOD_SESSION_EVENT_V2, "session.event.v2");
    assert_eq!(METHOD_SESSION_LIST, "session.list");
    assert_eq!(METHOD_SESSION_HISTORY, "session.history");
    assert_eq!(METHOD_SESSION_METADATA_REPLACE, "session.metadata.replace");
    assert_ne!(
        TypeId::of::<SessionCursor>(),
        TypeId::of::<SessionCursorV2>()
    );
}

#[test]
fn management_capacity_and_rpc_code_constants_are_frozen() {
    assert_eq!(DEFAULT_SESSION_LIST_PAGE_LIMIT, 64);
    assert_eq!(MAX_SESSION_LIST_PAGE_LIMIT, 256);
    assert_eq!(DEFAULT_SESSION_HISTORY_PAGE_LIMIT, 128);
    assert_eq!(MAX_SESSION_HISTORY_PAGE_LIMIT, 256);
    assert_eq!(DEFAULT_SESSION_V2_REPLAY_PAGE_LIMIT, 128);
    assert_eq!(DEFAULT_SESSION_V2_SUBSCRIBER_OUTPUT, 64);
    assert_eq!(MAX_SESSION_V2_SUBSCRIBER_OUTPUT, 4096);
    assert_eq!(MAX_SESSION_V2_EVENT_JOURNAL_EVENTS, 1024);
    assert_eq!(MAX_SESSION_V2_EVENT_JOURNAL_BYTES, 4 * 1024 * 1024);
    assert_eq!(MAX_SESSION_MANAGEMENT_HISTORY_BYTES, 32 * 1024 * 1024);
    assert_eq!(MAX_CLOSED_SESSION_TOMBSTONES_PER_OWNER, 128);
    assert_eq!(
        MAX_CLOSED_SESSION_TOMBSTONE_BYTES_PER_OWNER,
        64 * 1024 * 1024
    );
    assert_eq!(CLOSED_SESSION_TOMBSTONE_TTL_MS, 10 * 60 * 1000);
    assert_eq!(SESSION_MANAGEMENT_CURSOR_TTL_MS, 5 * 60 * 1000);
    assert_eq!(SESSION_MANAGEMENT_STATE, -32040);
    assert_eq!(SESSION_REVISION_CONFLICT, -32041);
    assert_eq!(SESSION_CURSOR_REJECTED, -32042);
    assert_eq!(SESSION_HISTORY_GAP, -32043);
}

#[test]
fn metadata_replacement_validator_is_shared_at_the_protocol_boundary() {
    validate_session_metadata_replacement(&Map::from_iter([(
        "valid".into(),
        json!({"nested":[1, 2, 3]}),
    )]))
    .unwrap();
    assert!(validate_session_metadata_replacement(&Map::from_iter([(
        "large".into(),
        json!("x".repeat(MAX_SESSION_METADATA_BYTES)),
    )]))
    .is_err());
    assert!(validate_session_metadata_replacement(&Map::from_iter([(
        "deep".into(),
        json!([[[[[[[[[[[[[[[[[0]]]]]]]]]]]]]]]]]),
    )]))
    .is_err());
}

#[test]
fn v2_snapshot_and_events_have_frozen_json() {
    let snapshot = snapshot_v2(SessionLifecycleState::Open, 0);
    assert_eq!(
        serde_json::to_value(&snapshot).unwrap(),
        json!({
            "summary": {
                "thread_id":"thread-a",
                "agent_name":"researcher",
                "metadata":{"title":"Demo"},
                "created_at_ms":10,
                "updated_at_ms":10,
                "view_revision":0
            },
            "lifecycle":"open",
            "persistence":{"kind":"ephemeral"},
            "history":{"items":[],"start_index":0,"total_items":0,"capacity":8},
            "cursor":{"thread_id":"thread-a","stream_id":"stream-v2","seq":0}
        })
    );
    assert_eq!(
        serde_json::from_value::<SessionSnapshotV2>(serde_json::to_value(&snapshot).unwrap())
            .unwrap(),
        snapshot
    );

    let metadata = metadata_event(1, "Renamed");
    assert_eq!(
        serde_json::to_value(&metadata).unwrap(),
        json!({
            "thread_id":"thread-a",
            "cursor":{"thread_id":"thread-a","stream_id":"stream-v2","seq":1},
            "occurred_at_ms":11,
            "type":"metadata_changed",
            "metadata":{"title":"Renamed"}
        })
    );
    let closing = SessionEventEnvelopeV2::new(
        "thread-a",
        cursor_v2("stream-v2", 2),
        12,
        SessionEventPayloadV2::LifecycleChanged {
            lifecycle: SessionLifecycleState::Closing,
        },
    );
    assert_eq!(
        serde_json::to_value(&closing).unwrap()["type"],
        "lifecycle_changed"
    );
    assert_eq!(
        serde_json::to_value(&closing).unwrap()["lifecycle"],
        "closing"
    );

    let run_event = SessionEventEnvelopeV2::new(
        "thread-a",
        cursor_v2("stream-v2", 3),
        13,
        SessionEventPayloadV2::RunEvent {
            event: RunEvent {
                thread_id: "thread-a".into(),
                turn_id: "turn-a".into(),
                seq: 1,
                payload: RunEventPayload::Stream {
                    event: AgentStreamEvent::TextDelta {
                        turn_id: "turn-a".into(),
                        item_id: "item-a".into(),
                        delta: "hello".into(),
                    },
                },
            },
        },
    );
    assert_eq!(
        serde_json::to_value(&run_event).unwrap(),
        json!({
            "thread_id":"thread-a",
            "cursor":{"thread_id":"thread-a","stream_id":"stream-v2","seq":3},
            "occurred_at_ms":13,
            "type":"run_event",
            "event":{
                "thread_id":"thread-a",
                "turn_id":"turn-a",
                "seq":1,
                "type":"stream",
                "event":{
                    "type":"text_delta",
                    "turn_id":"turn-a",
                    "item_id":"item-a",
                    "delta":"hello"
                }
            }
        })
    );

    let closed = SessionEventEnvelopeV2::new(
        "thread-a",
        cursor_v2("stream-v2", 4),
        14,
        SessionEventPayloadV2::LifecycleChanged {
            lifecycle: SessionLifecycleState::Closed,
        },
    );
    assert_eq!(
        serde_json::to_value(closed).unwrap(),
        json!({
            "thread_id":"thread-a",
            "cursor":{"thread_id":"thread-a","stream_id":"stream-v2","seq":4},
            "occurred_at_ms":14,
            "type":"lifecycle_changed",
            "lifecycle":"closed"
        })
    );

    let persistence = SessionPersistenceV2::Persistent {
        recovery_id: "550e8400-e29b-41d4-a716-446655440000".into(),
    };
    assert_eq!(
        serde_json::to_value(persistence).unwrap(),
        json!({"kind":"persistent","recovery_id":"550e8400-e29b-41d4-a716-446655440000"})
    );
}

#[test]
fn v2_event_payload_is_a_closed_wire_family() {
    let unknown = json!({
        "thread_id":"thread-a",
        "cursor":{"thread_id":"thread-a","stream_id":"stream-v2","seq":1},
        "occurred_at_ms":11,
        "type":"future_event",
        "value":true
    });
    assert!(serde_json::from_value::<SessionEventEnvelopeV2>(unknown).is_err());
}

#[test]
fn v2_reducer_applies_run_metadata_and_terminal_lifecycle_in_order() {
    let mut snapshot = snapshot_v2(SessionLifecycleState::Open, 0);
    let accepted = SessionEventEnvelopeV2::new(
        "thread-a",
        cursor_v2("stream-v2", 1),
        11,
        SessionEventPayloadV2::RunChanged {
            run: SessionRunView::new(running_snapshot(), 11),
        },
    );
    assert!(snapshot.apply(&accepted).unwrap());
    assert_eq!(
        snapshot.active_run.as_ref().unwrap().snapshot.turn_id,
        "turn-a"
    );

    let item = CanonicalItem::assistant_text("done", whale_protocol::MessagePhase::FinalAnswer);
    let item_id = item.id().to_owned();
    let completed = SessionEventEnvelopeV2::new(
        "thread-a",
        cursor_v2("stream-v2", 2),
        12,
        SessionEventPayloadV2::RunEvent {
            event: RunEvent {
                thread_id: "thread-a".into(),
                turn_id: "turn-a".into(),
                seq: 1,
                payload: RunEventPayload::Stream {
                    event: AgentStreamEvent::ItemCompleted {
                        turn_id: "turn-a".into(),
                        item,
                    },
                },
            },
        },
    );
    assert!(snapshot.apply(&completed).unwrap());
    assert_eq!(snapshot.history.items[0].id(), item_id);

    let renamed = metadata_event(3, "Renamed");
    assert!(snapshot.apply(&renamed).unwrap());
    assert_eq!(
        snapshot.summary.metadata,
        json!({"title":"Renamed"}).as_object().unwrap().clone()
    );

    let closing = SessionEventEnvelopeV2::new(
        "thread-a",
        cursor_v2("stream-v2", 4),
        14,
        SessionEventPayloadV2::LifecycleChanged {
            lifecycle: SessionLifecycleState::Closing,
        },
    );
    assert!(snapshot.apply(&closing).unwrap());

    let mut terminal = running_snapshot();
    terminal.status = RunStatus::Cancelled;
    terminal.last_seq = 2;
    terminal.result = Some(whale_protocol::RunTurnResult {
        thread_id: "thread-a".into(),
        turn_id: "turn-a".into(),
        status: whale_protocol::TurnStatus::Interrupted,
        items: Vec::new(),
        usage: UsageMetrics::default(),
    });
    let finished = SessionEventEnvelopeV2::new(
        "thread-a",
        cursor_v2("stream-v2", 5),
        15,
        SessionEventPayloadV2::RunEvent {
            event: RunEvent {
                thread_id: "thread-a".into(),
                turn_id: "turn-a".into(),
                seq: 2,
                payload: RunEventPayload::Finished { snapshot: terminal },
            },
        },
    );
    assert!(snapshot.apply(&finished).unwrap());
    assert!(snapshot.active_run.is_none());

    let closed = SessionEventEnvelopeV2::new(
        "thread-a",
        cursor_v2("stream-v2", 6),
        16,
        SessionEventPayloadV2::LifecycleChanged {
            lifecycle: SessionLifecycleState::Closed,
        },
    );
    assert!(snapshot.apply(&closed).unwrap());
    assert_eq!(snapshot.lifecycle, SessionLifecycleState::Closed);
    assert_eq!(snapshot.summary.view_revision, 6);
    assert_eq!(snapshot.cursor.seq, 6);
    assert!(!snapshot.apply(&closed).unwrap());
    assert!(snapshot.apply(&metadata_event(7, "too-late")).is_err());
}

#[test]
fn v2_reducer_rejects_invalid_lifecycle_streams_and_sequence_gaps() {
    let mut snapshot = snapshot_v2(SessionLifecycleState::Open, 0);
    let direct_closed = SessionEventEnvelopeV2::new(
        "thread-a",
        cursor_v2("stream-v2", 1),
        11,
        SessionEventPayloadV2::LifecycleChanged {
            lifecycle: SessionLifecycleState::Closed,
        },
    );
    assert!(matches!(
        snapshot.apply(&direct_closed),
        Err(SessionProjectionErrorV2::InvalidState { .. })
    ));

    let mut wrong_stream = metadata_event(1, "x");
    wrong_stream.cursor.stream_id = "other".into();
    assert!(matches!(
        snapshot.apply(&wrong_stream),
        Err(SessionProjectionErrorV2::StreamMismatch { .. })
    ));
    assert!(matches!(
        snapshot.apply(&metadata_event(2, "x")),
        Err(SessionProjectionErrorV2::SequenceGap { .. })
    ));

    let open_again = SessionEventEnvelopeV2::new(
        "thread-a",
        cursor_v2("stream-v2", 1),
        11,
        SessionEventPayloadV2::LifecycleChanged {
            lifecycle: SessionLifecycleState::Open,
        },
    );
    assert!(snapshot.apply(&open_again).is_err());
}

#[test]
fn v2_replay_validates_fixed_windows_gaps_and_closed_empty_page() {
    let params = SubscribeSessionV2Params {
        thread_id: "thread-a".into(),
        after: cursor_v2("stream-v2", 0),
        through: None,
        limit: 2,
    };
    let events = vec![metadata_event(1, "one"), metadata_event(2, "two")];
    let page = SubscribeSessionV2Result {
        events,
        resume_after: cursor_v2("stream-v2", 2),
        through: cursor_v2("stream-v2", 3),
        lifecycle: SessionLifecycleState::Open,
        has_more: true,
        gap: None,
    };
    page.validate_for(&params).unwrap();

    let next = SubscribeSessionV2Params {
        thread_id: "thread-a".into(),
        after: cursor_v2("stream-v2", 2),
        through: Some(cursor_v2("stream-v2", 3)),
        limit: 2,
    };
    let closing = SessionEventEnvelopeV2::new(
        "thread-a",
        cursor_v2("stream-v2", 3),
        13,
        SessionEventPayloadV2::LifecycleChanged {
            lifecycle: SessionLifecycleState::Closing,
        },
    );
    SubscribeSessionV2Result {
        events: vec![closing],
        resume_after: cursor_v2("stream-v2", 3),
        through: cursor_v2("stream-v2", 3),
        lifecycle: SessionLifecycleState::Closing,
        has_more: false,
        gap: None,
    }
    .validate_for(&next)
    .unwrap();

    let closed_params = SubscribeSessionV2Params {
        thread_id: "thread-a".into(),
        after: cursor_v2("stream-v2", 4),
        through: None,
        limit: 1,
    };
    let closed_page = SubscribeSessionV2Result {
        events: Vec::new(),
        resume_after: cursor_v2("stream-v2", 4),
        through: cursor_v2("stream-v2", 4),
        lifecycle: SessionLifecycleState::Closed,
        has_more: false,
        gap: None,
    };
    closed_page.validate_for(&closed_params).unwrap();
    assert_eq!(
        serde_json::to_value(closed_page).unwrap(),
        json!({
            "events":[],
            "resume_after":{"thread_id":"thread-a","stream_id":"stream-v2","seq":4},
            "through":{"thread_id":"thread-a","stream_id":"stream-v2","seq":4},
            "lifecycle":"closed",
            "has_more":false,
            "gap":null
        })
    );

    let gap = ReplayGapV2 {
        reason: ReplayGapReasonV2::Retention,
        requested: cursor_v2("stream-v2", 0),
        replay_floor: cursor_v2("stream-v2", 2),
        current: cursor_v2("stream-v2", 4),
        view_revision: 4,
    };
    gap.validate().unwrap();
    let reset = ReplayGapV2 {
        reason: ReplayGapReasonV2::StreamReset,
        requested: cursor_v2("old-stream", 4),
        replay_floor: cursor_v2("stream-v2", 0),
        current: cursor_v2("stream-v2", 0),
        view_revision: 0,
    };
    reset.validate().unwrap();
}

#[test]
fn list_history_and_metadata_contracts_validate_limits_and_identity() {
    let list_cursor = SessionListCursor::new("list-token").unwrap();
    assert_eq!(list_cursor.as_str(), "list-token");
    assert_eq!(
        serde_json::to_value(&list_cursor).unwrap(),
        json!("list-token")
    );
    let history_cursor = SessionHistoryPageCursor::new("history-token").unwrap();
    assert_eq!(history_cursor.as_str(), "history-token");
    assert_ne!(
        TypeId::of::<SessionListCursor>(),
        TypeId::of::<SessionHistoryPageCursor>()
    );
    assert!(SessionListCursor::new("").is_err());
    assert!(SessionHistoryPageCursor::new(" x ").is_err());
    assert!(SessionListCursor::new("x".repeat(MAX_SESSION_CURSOR_TOKEN_BYTES + 1)).is_err());

    for limit in [0, MAX_SESSION_LIST_PAGE_LIMIT + 1] {
        assert!(ListSessionsParams {
            cursor: None,
            limit
        }
        .validate()
        .is_err());
    }
    let list_params = ListSessionsParams {
        cursor: Some(list_cursor.clone()),
        limit: 1,
    };
    list_params.validate().unwrap();
    let entry = SessionListEntry::new(
        summary_v2(0),
        SessionLifecycleState::Open,
        SessionPersistenceV2::Ephemeral,
        cursor_v2("stream-v2", 0),
        3,
    );
    ListSessionsResult {
        sessions: vec![entry],
        next_cursor: Some(list_cursor),
    }
    .validate_for(&list_params)
    .unwrap();

    let anchor = SessionHistoryAnchor {
        thread_id: "thread-a".into(),
        stream_id: "stream-v2".into(),
        index: 4,
    };
    let history_params = GetSessionHistoryParams {
        thread_id: "thread-a".into(),
        before: Some(anchor.clone()),
        cursor: None,
        limit: 2,
    };
    history_params.validate().unwrap();
    assert!(GetSessionHistoryParams {
        thread_id: "thread-a".into(),
        before: Some(anchor.clone()),
        cursor: Some(history_cursor.clone()),
        limit: 2,
    }
    .validate()
    .is_err());
    for limit in [0, MAX_SESSION_HISTORY_PAGE_LIMIT + 1] {
        assert!(GetSessionHistoryParams {
            thread_id: "thread-a".into(),
            before: None,
            cursor: None,
            limit,
        }
        .validate()
        .is_err());
    }
    let history_item = CanonicalItem::user_text("older");
    GetSessionHistoryResult {
        items: vec![history_item],
        start_index: 3,
        end_index: 4,
        through: SessionHistoryAnchor {
            index: 4,
            ..anchor.clone()
        },
        current_end: SessionHistoryAnchor {
            index: 6,
            ..anchor.clone()
        },
        next_cursor: Some(history_cursor),
    }
    .validate_for(&history_params)
    .unwrap();

    let metadata_params = ReplaceSessionMetadataParams {
        thread_id: "thread-a".into(),
        expected_view_revision: 2,
        metadata: Map::from_iter([("title".into(), json!("new"))]),
    };
    metadata_params.validate().unwrap();
    ReplaceSessionMetadataResult {
        summary: {
            let mut summary = summary_v2(3);
            summary.metadata = metadata_params.metadata.clone();
            summary
        },
        cursor: cursor_v2("stream-v2", 3),
        changed: true,
    }
    .validate_for(&metadata_params)
    .unwrap();
    let mut too_many = Map::new();
    for index in 0..=MAX_SESSION_METADATA_KEYS {
        too_many.insert(format!("k{index}"), json!(index));
    }
    assert!(ReplaceSessionMetadataParams {
        thread_id: "thread-a".into(),
        expected_view_revision: 0,
        metadata: too_many,
    }
    .validate()
    .is_err());
    assert!(ReplaceSessionMetadataParams {
        thread_id: "thread-a".into(),
        expected_view_revision: 0,
        metadata: Map::from_iter([("deep".into(), json!([[[[[[[[[[[[[[[[[0]]]]]]]]]]]]]]]]]))]),
    }
    .validate()
    .is_err());
    assert!(ReplaceSessionMetadataParams {
        thread_id: "thread-a".into(),
        expected_view_revision: 0,
        metadata: Map::from_iter([(
            "large".into(),
            json!("x".repeat(MAX_SESSION_METADATA_BYTES))
        )]),
    }
    .validate()
    .is_err());
}

#[test]
fn management_pages_enforce_byte_caps_and_canonical_history_identity() {
    let list_params = ListSessionsParams {
        cursor: None,
        limit: 1,
    };
    let mut oversized_summary = summary_v2(0);
    oversized_summary.metadata.insert(
        "large".into(),
        json!("x".repeat(MAX_SESSION_MANAGEMENT_PAGE_BYTES)),
    );
    let oversized_entry = SessionListEntry::new(
        oversized_summary,
        SessionLifecycleState::Open,
        SessionPersistenceV2::Ephemeral,
        cursor_v2("stream-v2", 0),
        0,
    );
    assert!(ListSessionsResult {
        sessions: vec![oversized_entry],
        next_cursor: None,
    }
    .validate_for(&list_params)
    .is_err());

    let history_params = GetSessionHistoryParams {
        thread_id: "thread-a".into(),
        before: None,
        cursor: None,
        limit: 2,
    };
    let anchor = SessionHistoryAnchor {
        thread_id: "thread-a".into(),
        stream_id: "stream-v2".into(),
        index: 2,
    };
    let repeated = CanonicalItem::user_text("same");
    assert!(GetSessionHistoryResult {
        items: vec![repeated.clone(), repeated],
        start_index: 0,
        end_index: 2,
        through: anchor.clone(),
        current_end: anchor.clone(),
        next_cursor: None,
    }
    .validate_for(&history_params)
    .is_err());

    let oversized = CanonicalItem::user_text("x".repeat(MAX_SESSION_MANAGEMENT_PAGE_BYTES));
    let one_item_params = GetSessionHistoryParams {
        limit: 1,
        ..history_params
    };
    let one = SessionHistoryAnchor { index: 1, ..anchor };
    assert!(GetSessionHistoryResult {
        items: vec![oversized],
        start_index: 0,
        end_index: 1,
        through: one.clone(),
        current_end: one,
        next_cursor: None,
    }
    .validate_for(&one_item_params)
    .is_err());
}

#[test]
fn typed_error_data_has_stable_json_and_validates_cross_fields() {
    let conflict = SessionManagementErrorData::RevisionConflict {
        expected_view_revision: 2,
        current_view_revision: 3,
        current: cursor_v2("stream-v2", 3),
    };
    conflict.validate().unwrap();
    assert_eq!(
        serde_json::to_value(&conflict).unwrap(),
        json!({
            "kind":"revision_conflict",
            "expected_view_revision":2,
            "current_view_revision":3,
            "current":{"thread_id":"thread-a","stream_id":"stream-v2","seq":3}
        })
    );
    assert_eq!(
        serde_json::to_value(SessionManagementErrorData::Unavailable).unwrap(),
        json!({"kind":"unavailable"})
    );
    let anchor = SessionHistoryAnchor {
        thread_id: "thread-a".into(),
        stream_id: "stream-v2".into(),
        index: 1,
    };
    SessionManagementErrorData::HistoryGap {
        requested: anchor.clone(),
        floor: SessionHistoryAnchor {
            index: 2,
            ..anchor.clone()
        },
        current_end: SessionHistoryAnchor {
            index: 5,
            ..anchor.clone()
        },
    }
    .validate()
    .unwrap();

    let valid_errors = [
        (
            SessionManagementErrorData::SessionNotOpen {
                lifecycle: SessionLifecycleState::Closing,
            },
            json!({"kind":"session_not_open","lifecycle":"closing"}),
        ),
        (
            SessionManagementErrorData::ListCursorInvalid,
            json!({"kind":"list_cursor_invalid"}),
        ),
        (
            SessionManagementErrorData::ListCursorExpired,
            json!({"kind":"list_cursor_expired"}),
        ),
        (
            SessionManagementErrorData::HistoryCursorInvalid,
            json!({"kind":"history_cursor_invalid"}),
        ),
        (
            SessionManagementErrorData::HistoryStreamReset {
                requested: anchor.clone(),
                current: SessionHistoryAnchor {
                    stream_id: "stream-new".into(),
                    index: 0,
                    ..anchor.clone()
                },
            },
            json!({
                "kind":"history_stream_reset",
                "requested":{"thread_id":"thread-a","stream_id":"stream-v2","index":1},
                "current":{"thread_id":"thread-a","stream_id":"stream-new","index":0}
            }),
        ),
        (
            SessionManagementErrorData::HistoryGap {
                requested: anchor.clone(),
                floor: SessionHistoryAnchor {
                    index: 2,
                    ..anchor.clone()
                },
                current_end: SessionHistoryAnchor {
                    index: 5,
                    ..anchor.clone()
                },
            },
            json!({
                "kind":"history_gap",
                "requested":{"thread_id":"thread-a","stream_id":"stream-v2","index":1},
                "floor":{"thread_id":"thread-a","stream_id":"stream-v2","index":2},
                "current_end":{"thread_id":"thread-a","stream_id":"stream-v2","index":5}
            }),
        ),
        (
            SessionManagementErrorData::TombstoneExpired {
                thread_id: "thread-a".into(),
            },
            json!({"kind":"tombstone_expired","thread_id":"thread-a"}),
        ),
        (
            SessionManagementErrorData::ResourceLimit {
                resource: "history_page".into(),
                actual: 11,
                limit: 10,
                item_index: Some(7),
            },
            json!({
                "kind":"resource_limit",
                "resource":"history_page",
                "actual":11,
                "limit":10,
                "item_index":7
            }),
        ),
        (
            SessionManagementErrorData::StorageFailure {
                outcome_unknown: true,
            },
            json!({"kind":"storage_failure","outcome_unknown":true}),
        ),
    ];
    for (error, expected_json) in valid_errors {
        error.validate().unwrap();
        assert_eq!(serde_json::to_value(error).unwrap(), expected_json);
    }
    assert!(SessionManagementErrorData::RevisionConflict {
        expected_view_revision: 2,
        current_view_revision: 4,
        current: cursor_v2("stream-v2", 3),
    }
    .validate()
    .is_err());
    assert!(SessionManagementErrorData::ResourceLimit {
        resource: "history_page".into(),
        actual: 10,
        limit: 10,
        item_index: None,
    }
    .validate()
    .is_err());
}
