use serde_json::{json, Map};
use whale_protocol::{
    initialization::PROTOCOL_CAPABILITIES,
    runs::{RunEvent, RunEventPayload, RunSnapshot, RunStatus},
    session_views::*,
    AgentStreamEvent, CanonicalItem, UsageMetrics,
};

fn cursor(stream_id: &str, seq: u64) -> SessionCursor {
    SessionCursor {
        thread_id: "thread-a".into(),
        stream_id: stream_id.into(),
        seq,
    }
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
        last_seq: 1,
        result: None,
        error: None,
    }
}

fn run_event(session_seq: u64) -> SessionEventEnvelope {
    SessionEventEnvelope::new(
        "thread-a",
        cursor("stream-a", session_seq),
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
    )
}

#[test]
fn session_view_methods_are_optional_without_changing_the_sdk_baseline() {
    assert_eq!(CAPABILITY_SESSION_VIEWS, "session_views.v1");
    assert_eq!(CAPABILITY_SESSION_EVENT_REPLAY, "session_event_replay.v1");
    assert_eq!(METHOD_SESSION_GET, "session.get");
    assert_eq!(METHOD_SESSION_SUBSCRIBE, "session.subscribe");
    assert_eq!(METHOD_SESSION_EVENT, "session.event");
    assert!(!PROTOCOL_CAPABILITIES.contains(&CAPABILITY_SESSION_VIEWS));
    assert!(!PROTOCOL_CAPABILITIES.contains(&CAPABILITY_SESSION_EVENT_REPLAY));
}

#[test]
fn cursor_and_snapshot_have_stable_json() {
    let session_cursor = cursor("stream-a", 7);
    assert_eq!(
        serde_json::to_string(&session_cursor).unwrap(),
        r#"{"thread_id":"thread-a","stream_id":"stream-a","seq":7}"#
    );

    let mut metadata = Map::new();
    metadata.insert("title".into(), json!("Demo"));
    let mut summary = SessionSummary::new("thread-a", 10);
    summary.agent_name = Some("researcher".into());
    summary.metadata = metadata;
    summary.updated_at_ms = 12;
    summary.revision = 7;
    let history_item = CanonicalItem::UserMessage {
        id: "item-user".into(),
        content: vec![whale_protocol::CanonicalContent::text("hello")],
    };
    let history =
        SessionHistoryWindow::from_history(std::slice::from_ref(&history_item), 8).unwrap();
    let snapshot = SessionSnapshot::new(summary, history, session_cursor);
    assert_eq!(
        serde_json::to_string(&snapshot).unwrap(),
        r#"{"summary":{"thread_id":"thread-a","agent_name":"researcher","metadata":{"title":"Demo"},"created_at_ms":10,"updated_at_ms":12,"revision":7},"history":{"items":[{"type":"user_message","id":"item-user","content":[{"type":"text","text":"hello"}]}],"start_index":0,"total_items":1,"capacity":8},"cursor":{"thread_id":"thread-a","stream_id":"stream-a","seq":7}}"#
    );
    let value = serde_json::to_value(&snapshot).unwrap();
    assert_eq!(value["summary"]["thread_id"], "thread-a");
    assert_eq!(value["summary"]["metadata"], json!({"title":"Demo"}));
    assert_eq!(value["history"]["items"][0]["type"], "user_message");
    assert_eq!(value["history"]["start_index"], 0);
    assert_eq!(value["history"]["total_items"], 1);
    assert_eq!(value["history"]["capacity"], 8);
    assert_eq!(
        value["cursor"],
        json!({"thread_id":"thread-a","stream_id":"stream-a","seq":7})
    );
    assert!(value.get("active_run").is_none());
    assert!(value.get("last_run").is_none());
    assert!(snapshot.validate().is_ok());
    assert_eq!(
        serde_json::from_value::<SessionSnapshot>(value).unwrap(),
        snapshot
    );
}

#[test]
fn event_envelope_and_replay_page_have_stable_json() {
    let envelope = run_event(1);
    assert_eq!(
        serde_json::to_string(&envelope).unwrap(),
        r#"{"thread_id":"thread-a","cursor":{"thread_id":"thread-a","stream_id":"stream-a","seq":1},"occurred_at_ms":14,"type":"run_event","event":{"thread_id":"thread-a","turn_id":"turn-a","seq":1,"type":"stream","event":{"type":"text_delta","turn_id":"turn-a","item_id":"item-a","delta":"hello"}}}"#
    );

    let params = SubscribeSessionParams {
        thread_id: "thread-a".into(),
        after: cursor("stream-a", 0),
        through: None,
        limit: 1,
    };
    let _: u32 = params.limit;
    let page = SubscribeSessionResult {
        events: vec![envelope],
        resume_after: cursor("stream-a", 1),
        through: cursor("stream-a", 2),
        has_more: true,
        gap: None,
    };
    assert_eq!(
        serde_json::to_value(&page).unwrap(),
        json!({
            "events":[{
                "thread_id":"thread-a",
                "cursor":{"thread_id":"thread-a","stream_id":"stream-a","seq":1},
                "occurred_at_ms":14,
                "type":"run_event",
                "event":{
                    "thread_id":"thread-a","turn_id":"turn-a","seq":1,"type":"stream",
                    "event":{"type":"text_delta","turn_id":"turn-a","item_id":"item-a","delta":"hello"}
                }
            }],
            "resume_after":{"thread_id":"thread-a","stream_id":"stream-a","seq":1},
            "through":{"thread_id":"thread-a","stream_id":"stream-a","seq":2},
            "has_more":true,
            "gap":null
        })
    );
    page.validate_for(&params).unwrap();
}

#[test]
fn run_changed_projection_preserves_partial_items() {
    let mut item = InProgressItemProjection::new("item-a", "tool_call");
    item.call_id = Some("call-a".into());
    item.raw_arguments = "{\"path\":".into();
    let mut run = SessionRunView::new(running_snapshot(), 10);
    run.in_progress_items.push(item);
    run.updated_at_ms = 20;
    let envelope = SessionEventEnvelope::new(
        "thread-a",
        cursor("stream-a", 2),
        20,
        SessionEventPayload::RunChanged { run },
    );
    let value = serde_json::to_value(&envelope).unwrap();
    assert_eq!(value["type"], "run_changed");
    assert_eq!(value["run"]["in_progress_items"][0]["call_id"], "call-a");
    assert_eq!(
        value["run"]["in_progress_items"][0]["raw_arguments"],
        "{\"path\":"
    );
    assert!(envelope.validate().is_ok());
    assert_eq!(
        serde_json::from_value::<SessionEventEnvelope>(value).unwrap(),
        envelope
    );
}

#[test]
fn request_validation_rejects_blank_identity_bad_limits_and_cursor_mismatch() {
    for invalid in [
        SessionCursor {
            thread_id: "".into(),
            stream_id: "stream-a".into(),
            seq: 0,
        },
        SessionCursor {
            thread_id: "thread-a".into(),
            stream_id: " \n".into(),
            seq: 0,
        },
    ] {
        assert!(invalid.validate().is_err());
    }
    assert!(cursor("stream-a", u64::MAX).checked_next().is_err());
    assert!(GetSessionParams {
        thread_id: " \t".into(),
        history_limit: 8
    }
    .validate()
    .is_err());
    for history_limit in [0, MAX_SESSION_HISTORY_LIMIT + 1] {
        assert!(GetSessionParams {
            thread_id: "thread-a".into(),
            history_limit,
        }
        .validate()
        .is_err());
    }

    for params in [
        SubscribeSessionParams {
            thread_id: "thread-a".into(),
            after: cursor("stream-a", 0),
            through: None,
            limit: 0,
        },
        SubscribeSessionParams {
            thread_id: "thread-a".into(),
            after: cursor("stream-a", 0),
            through: None,
            limit: MAX_SESSION_REPLAY_PAGE_LIMIT + 1,
        },
        SubscribeSessionParams {
            thread_id: "thread-b".into(),
            after: cursor("stream-a", 0),
            through: None,
            limit: 1,
        },
        SubscribeSessionParams {
            thread_id: "thread-a".into(),
            after: cursor("stream-a", 4),
            through: Some(cursor("stream-a", 3)),
            limit: 1,
        },
    ] {
        assert!(params.validate().is_err());
    }
}

#[test]
fn result_validation_distinguishes_future_retention_and_stream_reset() {
    let future_params = SubscribeSessionParams {
        thread_id: "thread-a".into(),
        after: cursor("stream-a", 5),
        through: None,
        limit: 4,
    };
    let future = SubscribeSessionResult {
        events: Vec::new(),
        resume_after: cursor("stream-a", 5),
        through: cursor("stream-a", 4),
        has_more: false,
        gap: None,
    };
    assert!(future.validate_for(&future_params).is_err());

    let retention_params = SubscribeSessionParams {
        thread_id: "thread-a".into(),
        after: cursor("stream-a", 0),
        through: None,
        limit: 4,
    };
    let retention = SubscribeSessionResult {
        events: Vec::new(),
        resume_after: cursor("stream-a", 0),
        through: cursor("stream-a", 5),
        has_more: false,
        gap: Some(ReplayGap {
            reason: ReplayGapReason::Retention,
            requested: cursor("stream-a", 0),
            replay_floor: cursor("stream-a", 2),
            current: cursor("stream-a", 5),
            session_revision: 5,
        }),
    };
    retention.validate_for(&retention_params).unwrap();
    assert_eq!(
        serde_json::to_value(&retention).unwrap()["gap"],
        json!({
            "reason":"retention",
            "requested":{"thread_id":"thread-a","stream_id":"stream-a","seq":0},
            "replay_floor":{"thread_id":"thread-a","stream_id":"stream-a","seq":2},
            "current":{"thread_id":"thread-a","stream_id":"stream-a","seq":5},
            "session_revision":5
        })
    );

    let reset_params = SubscribeSessionParams {
        thread_id: "thread-a".into(),
        after: cursor("old-stream", 9),
        through: None,
        limit: 4,
    };
    let reset = SubscribeSessionResult {
        events: Vec::new(),
        resume_after: cursor("old-stream", 9),
        through: cursor("new-stream", 0),
        has_more: false,
        gap: Some(ReplayGap {
            reason: ReplayGapReason::StreamReset,
            requested: cursor("old-stream", 9),
            replay_floor: cursor("new-stream", 0),
            current: cursor("new-stream", 0),
            session_revision: 0,
        }),
    };
    reset.validate_for(&reset_params).unwrap();

    let mut wrong_reason = reset.clone();
    wrong_reason.gap.as_mut().unwrap().reason = ReplayGapReason::Retention;
    assert!(wrong_reason.validate_for(&reset_params).is_err());
}

#[test]
fn subsequent_page_stream_reset_validates_as_gap() {
    let params = SubscribeSessionParams {
        thread_id: "thread-a".into(),
        after: cursor("old-stream", 2),
        through: Some(cursor("old-stream", 5)),
        limit: 4,
    };
    let current = cursor("new-stream", 0);
    let reset = SubscribeSessionResult {
        events: Vec::new(),
        resume_after: params.after.clone(),
        through: current.clone(),
        has_more: false,
        gap: Some(ReplayGap {
            reason: ReplayGapReason::StreamReset,
            requested: params.after.clone(),
            replay_floor: current.clone(),
            current,
            session_revision: 0,
        }),
    };

    reset.validate_for(&params).unwrap();

    let mut inconsistent = reset;
    inconsistent.through.seq = 1;
    assert!(inconsistent.validate_for(&params).is_err());
}

#[test]
fn replay_pages_keep_one_fixed_through_window_and_floor_is_continuous() {
    let first = SubscribeSessionParams {
        thread_id: "thread-a".into(),
        after: cursor("stream-a", 0),
        through: None,
        limit: 1,
    };
    let first_page = SubscribeSessionResult {
        events: vec![run_event(1)],
        resume_after: cursor("stream-a", 1),
        through: cursor("stream-a", 3),
        has_more: true,
        gap: None,
    };
    first_page.validate_for(&first).unwrap();

    let second = SubscribeSessionParams {
        thread_id: "thread-a".into(),
        after: first_page.resume_after.clone(),
        through: Some(first_page.through.clone()),
        limit: 2,
    };
    let second_page = SubscribeSessionResult {
        events: vec![run_event(2), run_event(3)],
        resume_after: cursor("stream-a", 3),
        through: cursor("stream-a", 3),
        has_more: false,
        gap: None,
    };
    second_page.validate_for(&second).unwrap();

    let mut crossed_window = second_page.clone();
    crossed_window.events.push(run_event(4));
    crossed_window.resume_after = cursor("stream-a", 4);
    assert!(crossed_window.validate_for(&second).is_err());

    let at_floor = SubscribeSessionParams {
        thread_id: "thread-a".into(),
        after: cursor("stream-a", 2),
        through: Some(cursor("stream-a", 3)),
        limit: 1,
    };
    let suffix = SubscribeSessionResult {
        events: vec![run_event(3)],
        resume_after: cursor("stream-a", 3),
        through: cursor("stream-a", 3),
        has_more: false,
        gap: None,
    };
    suffix.validate_for(&at_floor).unwrap();
}

#[test]
fn projection_reducer_returns_stable_typed_errors() {
    fn assert_error<T: std::error::Error>() {}
    assert_error::<SessionProjectionError>();

    let mut invalid_summary = SessionSummary::new("thread-a", 10);
    invalid_summary.revision = 1;
    let mut invalid = SessionSnapshot::new(
        invalid_summary,
        SessionHistoryWindow::new(1).unwrap(),
        cursor("stream-a", 0),
    );
    let invalid_error = invalid.apply(&run_event(1)).unwrap_err();
    assert!(invalid_error
        .to_string()
        .contains("invalid Session projection"));
    match invalid_error {
        SessionProjectionError::InvalidState { message } => {
            assert!(message.contains("revision"));
        }
        other => panic!("expected InvalidState, got {other:?}"),
    }

    let mut current = SessionSnapshot::new(
        SessionSummary::new("thread-a", 10),
        SessionHistoryWindow::new(1).unwrap(),
        cursor("stream-a", 0),
    );
    let mut wrong_stream = run_event(1);
    wrong_stream.cursor.stream_id = "stream-b".into();
    let stream_error = current.apply(&wrong_stream).unwrap_err();
    match stream_error {
        SessionProjectionError::StreamMismatch {
            expected_thread,
            expected_stream,
            actual_thread,
            actual_stream,
        } => {
            assert_eq!(expected_thread, "thread-a");
            assert_eq!(expected_stream, "stream-a");
            assert_eq!(actual_thread, "thread-a");
            assert_eq!(actual_stream, "stream-b");
        }
        other => panic!("expected StreamMismatch, got {other:?}"),
    }

    let mut foreign_run = running_snapshot();
    foreign_run.thread_id = "thread-b".into();
    let foreign_thread = SessionEventEnvelope::new(
        "thread-b",
        SessionCursor {
            thread_id: "thread-b".into(),
            stream_id: "stream-a".into(),
            seq: 1,
        },
        14,
        SessionEventPayload::RunChanged {
            run: SessionRunView::new(foreign_run, 10),
        },
    );
    match current.apply(&foreign_thread).unwrap_err() {
        SessionProjectionError::StreamMismatch {
            expected_thread,
            expected_stream,
            actual_thread,
            actual_stream,
        } => {
            assert_eq!(expected_thread, "thread-a");
            assert_eq!(expected_stream, "stream-a");
            assert_eq!(actual_thread, "thread-b");
            assert_eq!(actual_stream, "stream-a");
        }
        other => panic!("expected StreamMismatch, got {other:?}"),
    }

    let gap_error = current.apply(&run_event(2)).unwrap_err();
    match gap_error {
        SessionProjectionError::SequenceGap { expected, actual } => {
            assert_eq!(expected, cursor("stream-a", 1));
            assert_eq!(actual, cursor("stream-a", 2));
        }
        other => panic!("expected SequenceGap, got {other:?}"),
    }
}

#[test]
fn applying_envelopes_reconstructs_the_later_authoritative_snapshot() {
    let mut summary = SessionSummary::new("thread-a", 10);
    summary.agent_name = Some("researcher".into());
    let mut reconstructed = SessionSnapshot::new(
        summary.clone(),
        SessionHistoryWindow::new(1).unwrap(),
        cursor("stream-a", 0),
    );

    let accepted = SessionEventEnvelope::new(
        "thread-a",
        cursor("stream-a", 1),
        10,
        SessionEventPayload::RunChanged {
            run: SessionRunView::new(running_snapshot(), 10),
        },
    );
    let started = SessionEventEnvelope::new(
        "thread-a",
        cursor("stream-a", 2),
        20,
        SessionEventPayload::RunEvent {
            event: RunEvent {
                thread_id: "thread-a".into(),
                turn_id: "turn-a".into(),
                seq: 1,
                payload: RunEventPayload::Stream {
                    event: AgentStreamEvent::ItemStarted {
                        turn_id: "turn-a".into(),
                        item_id: "item-a".into(),
                        item_type: "assistant_message".into(),
                        phase: Some(whale_protocol::MessagePhase::FinalAnswer),
                    },
                },
            },
        },
    );
    let delta = SessionEventEnvelope::new(
        "thread-a",
        cursor("stream-a", 3),
        30,
        SessionEventPayload::RunEvent {
            event: RunEvent {
                thread_id: "thread-a".into(),
                turn_id: "turn-a".into(),
                seq: 2,
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
    let item = CanonicalItem::AssistantMessage {
        id: "item-a".into(),
        content: vec![whale_protocol::CanonicalContent::text("hello")],
        phase: whale_protocol::MessagePhase::FinalAnswer,
    };
    let completed_item = SessionEventEnvelope::new(
        "thread-a",
        cursor("stream-a", 4),
        40,
        SessionEventPayload::RunEvent {
            event: RunEvent {
                thread_id: "thread-a".into(),
                turn_id: "turn-a".into(),
                seq: 3,
                payload: RunEventPayload::Stream {
                    event: AgentStreamEvent::ItemCompleted {
                        turn_id: "turn-a".into(),
                        item: item.clone(),
                    },
                },
            },
        },
    );
    let usage = UsageMetrics {
        input_tokens: 2,
        output_tokens: 1,
        ..UsageMetrics::default()
    };
    let terminal_snapshot = RunSnapshot {
        status: RunStatus::Completed,
        items: vec![
            item.clone(),
            CanonicalItem::AssistantMessage {
                id: "item-b".into(),
                content: vec![whale_protocol::CanonicalContent::text("done")],
                phase: whale_protocol::MessagePhase::FinalAnswer,
            },
        ],
        usage: usage.clone(),
        last_seq: 4,
        result: Some(whale_protocol::RunTurnResult {
            thread_id: "thread-a".into(),
            turn_id: "turn-a".into(),
            status: whale_protocol::TurnStatus::Completed,
            items: vec![
                item.clone(),
                CanonicalItem::AssistantMessage {
                    id: "item-b".into(),
                    content: vec![whale_protocol::CanonicalContent::text("done")],
                    phase: whale_protocol::MessagePhase::FinalAnswer,
                },
            ],
            usage: usage.clone(),
        }),
        ..running_snapshot()
    };
    let finished = SessionEventEnvelope::new(
        "thread-a",
        cursor("stream-a", 5),
        50,
        SessionEventPayload::RunEvent {
            event: RunEvent {
                thread_id: "thread-a".into(),
                turn_id: "turn-a".into(),
                seq: 4,
                payload: RunEventPayload::Finished {
                    snapshot: terminal_snapshot,
                },
            },
        },
    );

    for envelope in [&accepted, &started, &delta, &completed_item, &finished] {
        assert!(reconstructed.apply(envelope).unwrap());
    }
    assert!(!reconstructed.apply(&finished).unwrap());

    summary.updated_at_ms = 50;
    summary.revision = 5;
    let final_item = CanonicalItem::AssistantMessage {
        id: "item-b".into(),
        content: vec![whale_protocol::CanonicalContent::text("done")],
        phase: whale_protocol::MessagePhase::FinalAnswer,
    };
    let expected_history = SessionHistoryWindow::from_history(&[item, final_item], 1).unwrap();
    let mut expected = SessionSnapshot::new(summary, expected_history, cursor("stream-a", 5));
    expected.last_run = Some(SessionRunSummary::new(
        "turn-a",
        RunStatus::Completed,
        usage,
        10,
        50,
    ));
    assert_eq!(reconstructed, expected);
}

#[test]
fn terminal_reconciliation_clamps_completion_when_wall_clock_moves_backwards() {
    let mut snapshot = SessionSnapshot::new(
        SessionSummary::new("thread-a", 50),
        SessionHistoryWindow::new(8).unwrap(),
        cursor("stream-a", 0),
    );
    snapshot.active_run = Some(SessionRunView::new(running_snapshot(), 100));
    let terminal = RunSnapshot {
        status: RunStatus::Completed,
        last_seq: 1,
        ..running_snapshot()
    };
    let envelope = SessionEventEnvelope::new(
        "thread-a",
        cursor("stream-a", 1),
        90,
        SessionEventPayload::RunEvent {
            event: RunEvent {
                thread_id: "thread-a".into(),
                turn_id: "turn-a".into(),
                seq: 1,
                payload: RunEventPayload::Finished { snapshot: terminal },
            },
        },
    );

    assert!(snapshot.apply(&envelope).unwrap());
    assert_eq!(snapshot.last_run.unwrap().completed_at_ms, 100);
}

#[test]
fn terminal_reconciliation_detects_history_counter_overflow_atomically() {
    let retained = CanonicalItem::UserMessage {
        id: "item-old".into(),
        content: vec![whale_protocol::CanonicalContent::text("old")],
    };
    let history: SessionHistoryWindow = serde_json::from_value(json!({
        "items": [retained],
        "start_index": u64::MAX - 1,
        "total_items": u64::MAX,
        "capacity": 1
    }))
    .unwrap();
    history.validate().unwrap();

    let mut summary = SessionSummary::new("thread-a", 10);
    summary.revision = 7;
    let mut snapshot = SessionSnapshot::new(summary, history, cursor("stream-a", 7));
    snapshot.active_run = Some(SessionRunView::new(running_snapshot(), 10));
    let before = snapshot.clone();
    let terminal = RunSnapshot {
        status: RunStatus::Completed,
        items: vec![CanonicalItem::AssistantMessage {
            id: "item-new".into(),
            content: vec![whale_protocol::CanonicalContent::text("new")],
            phase: whale_protocol::MessagePhase::FinalAnswer,
        }],
        last_seq: 2,
        ..running_snapshot()
    };
    let envelope = SessionEventEnvelope::new(
        "thread-a",
        cursor("stream-a", 8),
        20,
        SessionEventPayload::RunEvent {
            event: RunEvent {
                thread_id: "thread-a".into(),
                turn_id: "turn-a".into(),
                seq: 2,
                payload: RunEventPayload::Finished { snapshot: terminal },
            },
        },
    );

    let error = snapshot.apply(&envelope).unwrap_err();
    match error {
        SessionProjectionError::InvalidState { message } => {
            assert!(message.contains("count is exhausted"));
        }
        other => panic!("expected InvalidState, got {other:?}"),
    }
    assert_eq!(snapshot, before);
}

#[test]
fn history_window_keeps_a_deterministic_tail_and_absolute_indexes() {
    let history = vec![
        CanonicalItem::user_text("one"),
        CanonicalItem::user_text("two"),
        CanonicalItem::user_text("three"),
    ];
    let window = SessionHistoryWindow::from_history(&history, 2).unwrap();
    assert_eq!(window.items, history[1..]);
    assert_eq!(window.start_index, 1);
    assert_eq!(window.total_items, 3);
    assert_eq!(window.capacity, 2);
    window.validate().unwrap();

    for invalid in [
        json!({"items":[],"start_index":0,"total_items":0,"capacity":0}),
        json!({"items":[],"start_index":0,"total_items":0,"capacity":MAX_SESSION_HISTORY_LIMIT + 1}),
        json!({"items":history,"start_index":1,"total_items":3,"capacity":2}),
    ]
    .map(|value| serde_json::from_value::<SessionHistoryWindow>(value).unwrap())
    {
        assert!(invalid.validate().is_err());
    }
}

#[test]
fn legacy_run_event_json_is_byte_for_byte_unchanged() {
    let event = RunEvent {
        thread_id: "session".into(),
        turn_id: "run".into(),
        seq: 1,
        payload: RunEventPayload::Stream {
            event: AgentStreamEvent::TextDelta {
                turn_id: "run".into(),
                item_id: "item".into(),
                delta: "hello".into(),
            },
        },
    };
    assert_eq!(
        serde_json::to_string(&event).unwrap(),
        r#"{"thread_id":"session","turn_id":"run","seq":1,"type":"stream","event":{"type":"text_delta","turn_id":"run","item_id":"item","delta":"hello"}}"#
    );
}
