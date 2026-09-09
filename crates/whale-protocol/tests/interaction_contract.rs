use serde_json::{json, Map, Value};
use std::error::Error;
use whale_protocol::{
    agents::AgentDefinition,
    initialization::PROTOCOL_CAPABILITIES,
    interactions::*,
    recovery::{
        AttachRecoveryParams, CreatePersistentSessionParams, RecoveryKey, SessionRunDefaults,
    },
    rpc::{RunTurnOptions, StartThreadParams},
    runs::{RunEvent, RunEventPayload, RunSnapshot, RunStatus},
    session_views::{SessionCursor, SessionEventEnvelope, SessionEventPayload},
    AgentStreamEvent, UsageMetrics,
};

fn start_params() -> StartThreadParams {
    StartThreadParams {
        limits: None,
        provider_ref: None,
        agent_name: None,
        context_policy: None,
        provider_config: None,
        options: None,
        session_id: None,
        provider: None,
        model: "model-a".into(),
        system_prompt: None,
        tools: Vec::new(),
        metadata: Map::new(),
    }
}

fn interaction_cursor(stream_id: &str, seq: u64) -> InteractionCursor {
    InteractionCursor {
        thread_id: "thread-a".into(),
        stream_id: stream_id.into(),
        seq,
    }
}

fn request(kind: &str) -> InteractionRequest {
    InteractionRequest::new(
        kind,
        "Review request",
        json!({"digest":"sha256:abc","preview":"safe"}),
        Some(json!({
            "$schema":"https://json-schema.org/draft/2020-12/schema",
            "type":"object",
            "required":["decision"]
        })),
    )
    .unwrap()
}

fn pending(request_id: &str, turn_id: &str) -> PendingInteraction {
    PendingInteraction::new(request_id, turn_id, request("vendor.review")).unwrap()
}

fn requested(seq: u64, request_id: &str) -> InteractionEventEnvelope {
    InteractionEventEnvelope::new(
        "thread-a",
        interaction_cursor("stream-a", seq),
        100 + seq,
        InteractionEventPayload::Requested {
            interaction: pending(request_id, "turn-a"),
        },
    )
}

fn removed(seq: u64, request_id: &str) -> InteractionEventEnvelope {
    InteractionEventEnvelope::new(
        "thread-a",
        interaction_cursor("stream-a", seq),
        100 + seq,
        InteractionEventPayload::Removed {
            request_id: request_id.into(),
            turn_id: "turn-a".into(),
            cause: INTERACTION_REMOVAL_RESOLVED.into(),
        },
    )
}

#[test]
fn constants_and_optional_capability_are_frozen() {
    assert_eq!(CAPABILITY_INTERACTIONS, "interactions.v1");
    assert_eq!(METHOD_SESSION_INTERACTIONS_GET, "session.interactions.get");
    assert_eq!(
        METHOD_SESSION_INTERACTIONS_SUBSCRIBE,
        "session.interactions.subscribe"
    );
    assert_eq!(
        METHOD_SESSION_INTERACTION_EVENT,
        "session.interaction_event"
    );
    assert_eq!(METHOD_TURN_INTERACTIONS_GET, "turn.interactions.get");
    assert_eq!(METHOD_TURN_REQUEST_INTERACTION, "turn.request_interaction");
    assert_eq!(METHOD_TURN_RESPOND_INTERACTION, "turn.respond_interaction");
    assert_eq!(INTERACTION_NOT_FOUND, -32050);
    assert_eq!(INTERACTION_CONFLICT, -32051);
    assert_eq!(INTERACTION_RESPONSE_INVALID, -32052);
    assert_eq!(INTERACTION_UNAVAILABLE, -32053);
    assert!(!PROTOCOL_CAPABILITIES.contains(&CAPABILITY_INTERACTIONS));

    assert_eq!(MAX_INTERACTION_ID_BYTES, 128);
    assert_eq!(MAX_INTERACTION_KIND_BYTES, 128);
    assert_eq!(MAX_INTERACTION_TITLE_BYTES, 512);
    assert_eq!(MAX_INTERACTION_PAYLOAD_BYTES, 65_536);
    assert_eq!(MAX_INTERACTION_SCHEMA_BYTES, 65_536);
    assert_eq!(MAX_INTERACTION_RESPONSE_BYTES, 65_536);
    assert_eq!(MAX_PENDING_INTERACTIONS_PER_RUN, 32);
    assert_eq!(DEFAULT_INTERACTION_REPLAY_PAGE_LIMIT, 128);
    assert_eq!(MAX_INTERACTION_REPLAY_PAGE_LIMIT, 256);
    assert_eq!(DEFAULT_INTERACTION_SUBSCRIBER_OUTPUT_CAPACITY, 64);
    assert_eq!(MAX_INTERACTION_SUBSCRIBER_OUTPUT_CAPACITY, 4_096);
    assert_eq!(MAX_INTERACTION_JOURNAL_EVENTS, 1_024);
    assert_eq!(MAX_INTERACTION_JOURNAL_BYTES, 4 * 1_024 * 1_024);
}

#[test]
fn request_json_is_open_and_stable() {
    let request = request("vendor.review");
    assert_eq!(
        serde_json::to_value(&request).unwrap(),
        json!({
            "kind":"vendor.review",
            "title":"Review request",
            "payload":{"digest":"sha256:abc","preview":"safe"},
            "response_schema":{
                "$schema":"https://json-schema.org/draft/2020-12/schema",
                "type":"object",
                "required":["decision"]
            }
        })
    );
    assert_eq!(
        serde_json::from_value::<InteractionRequest>(serde_json::to_value(&request).unwrap())
            .unwrap(),
        request
    );

    for kind in [
        KIND_TOOL_APPROVAL,
        KIND_CLARIFICATION,
        KIND_FORM,
        KIND_AUTH,
        KIND_PERMISSION_FILE,
        KIND_PERMISSION_NETWORK,
        KIND_REVIEW,
        "acme.product/review:v2",
    ] {
        InteractionRequest::new(kind, "Title", json!({}), None).unwrap();
    }
    assert!(InteractionRequest::new("whale.future_kind", "Title", json!({}), None).is_err());
}

#[test]
fn request_text_fields_are_checked_by_utf8_bytes_and_namespace() {
    for invalid in ["", " padded", "padded ", ".bad", "bad kind", "bad?kind"] {
        assert!(
            InteractionRequest::new(invalid, "Title", json!({}), None).is_err(),
            "{invalid:?}"
        );
    }

    let max_kind = format!("a{}", "b".repeat(MAX_INTERACTION_KIND_BYTES - 1));
    InteractionRequest::new(&max_kind, "Title", json!({}), None).unwrap();
    assert!(InteractionRequest::new(format!("{max_kind}b"), "Title", json!({}), None).is_err());

    let max_title = "x".repeat(MAX_INTERACTION_TITLE_BYTES);
    InteractionRequest::new("vendor.kind", &max_title, json!({}), None).unwrap();
    assert!(
        InteractionRequest::new("vendor.kind", format!("{max_title}x"), json!({}), None).is_err()
    );
    let multibyte_overflow = "界".repeat(MAX_INTERACTION_TITLE_BYTES / "界".len() + 1);
    assert!(InteractionRequest::new("vendor.kind", multibyte_overflow, json!({}), None).is_err());
    for invalid in ["", " padded", "padded "] {
        assert!(InteractionRequest::new("vendor.kind", invalid, json!({}), None).is_err());
    }
}

#[test]
fn request_payload_and_schema_bounds_are_exact() {
    let payload_at_limit = Value::String("x".repeat(MAX_INTERACTION_PAYLOAD_BYTES - 2));
    assert_eq!(
        serde_json::to_vec(&payload_at_limit).unwrap().len(),
        MAX_INTERACTION_PAYLOAD_BYTES
    );
    InteractionRequest::new("vendor.kind", "Title", payload_at_limit, None).unwrap();
    assert!(InteractionRequest::new(
        "vendor.kind",
        "Title",
        Value::String("x".repeat(MAX_INTERACTION_PAYLOAD_BYTES - 1)),
        None
    )
    .is_err());

    let schema_at_limit = json!({"x":"x".repeat(MAX_INTERACTION_SCHEMA_BYTES - 8)});
    assert_eq!(
        serde_json::to_vec(&schema_at_limit).unwrap().len(),
        MAX_INTERACTION_SCHEMA_BYTES
    );
    InteractionRequest::new("vendor.kind", "Title", json!({}), Some(schema_at_limit)).unwrap();
    assert!(InteractionRequest::new(
        "vendor.kind",
        "Title",
        json!({}),
        Some(json!({"x":"x".repeat(MAX_INTERACTION_SCHEMA_BYTES - 7)}))
    )
    .is_err());
    assert!(InteractionRequest::new("vendor.kind", "Title", json!({}), Some(json!(true))).is_err());
    assert!(InteractionRequest::new(
        "vendor.kind",
        "Title",
        json!({}),
        Some(json!({"$ref":"https://example.invalid/schema.json"}))
    )
    .is_err());
    InteractionRequest::new(
        "vendor.kind",
        "Title",
        json!({}),
        Some(json!({"$ref":"#/$defs/answer","$defs":{"answer":{"type":"string"}}})),
    )
    .unwrap();
}

#[test]
fn pending_and_response_json_are_stable_and_bounded() {
    let pending = pending("request-a", "turn-a");
    pending.validate().unwrap();
    assert_eq!(
        serde_json::to_value(&pending).unwrap(),
        json!({
            "request_id":"request-a",
            "turn_id":"turn-a",
            "kind":"vendor.review",
            "title":"Review request",
            "payload":{"digest":"sha256:abc","preview":"safe"},
            "response_schema":{
                "$schema":"https://json-schema.org/draft/2020-12/schema",
                "type":"object",
                "required":["decision"]
            }
        })
    );
    assert!(PendingInteraction::new("", "turn-a", request("vendor.review")).is_err());
    PendingInteraction::new(
        "x".repeat(MAX_INTERACTION_ID_BYTES),
        "turn-a",
        request("vendor.review"),
    )
    .unwrap();
    assert!(PendingInteraction::new(
        "x".repeat(MAX_INTERACTION_ID_BYTES + 1),
        "turn-a",
        request("vendor.review")
    )
    .is_err());

    let response_at_limit =
        InteractionResponse::new("request-a", Value::String("x".repeat(65_534))).unwrap();
    assert_eq!(
        serde_json::to_vec(&response_at_limit.response)
            .unwrap()
            .len(),
        MAX_INTERACTION_RESPONSE_BYTES
    );
    assert!(InteractionResponse::new(
        "request-a",
        Value::String("x".repeat(MAX_INTERACTION_RESPONSE_BYTES - 1))
    )
    .is_err());
}

#[test]
fn response_debug_redacts_response_payloads() {
    let response = InteractionResponse::new(
        "request-a",
        json!({"credential":"super-secret-interaction-response"}),
    )
    .unwrap();
    let response_debug = format!("{response:?}");
    assert!(response_debug.contains("request-a"));
    assert!(!response_debug.contains("super-secret-interaction-response"));

    let params = RespondInteractionParams::new(
        "thread-a",
        "turn-a",
        "request-a",
        json!({"credential":"super-secret-respond-params"}),
    )
    .unwrap();
    let params_debug = format!("{params:?}");
    assert!(params_debug.contains("thread-a"));
    assert!(params_debug.contains("turn-a"));
    assert!(params_debug.contains("request-a"));
    assert!(!params_debug.contains("super-secret-respond-params"));
}

#[test]
fn cursor_validation_and_checked_next_never_wrap() {
    let cursor = interaction_cursor("stream-a", 4);
    cursor.validate().unwrap();
    assert_eq!(
        cursor.checked_next().unwrap(),
        interaction_cursor("stream-a", 5)
    );
    assert!(InteractionCursor {
        thread_id: " thread-a".into(),
        ..cursor.clone()
    }
    .validate()
    .is_err());
    assert!(InteractionCursor {
        seq: u64::MAX,
        ..cursor
    }
    .checked_next()
    .is_err());
}

#[test]
fn snapshot_reducer_converges_and_deduplicates_by_cursor() {
    let mut snapshot =
        InteractionSnapshot::new("thread-a", interaction_cursor("stream-a", 0), Vec::new())
            .unwrap();
    let requested = requested(1, "request-a");
    assert!(snapshot.apply(&requested).unwrap());
    assert!(!snapshot.apply(&requested).unwrap());
    assert_eq!(snapshot.pending, vec![pending("request-a", "turn-a")]);
    assert_eq!(snapshot.cursor, interaction_cursor("stream-a", 1));

    let removed = removed(2, "request-a");
    assert!(snapshot.apply(&removed).unwrap());
    assert!(snapshot.pending.is_empty());
    assert_eq!(snapshot.cursor, interaction_cursor("stream-a", 2));
}

#[test]
fn snapshot_reducer_reports_typed_stream_and_sequence_errors_atomically() {
    fn assert_error<T: Error>() {}
    assert_error::<InteractionProjectionError>();

    let mut snapshot =
        InteractionSnapshot::new("thread-a", interaction_cursor("stream-a", 0), Vec::new())
            .unwrap();
    let before = snapshot.clone();
    let gap = requested(2, "request-a");
    assert!(matches!(
        snapshot.apply(&gap),
        Err(InteractionProjectionError::SequenceGap { .. })
    ));
    assert_eq!(snapshot, before);

    let different_stream = InteractionEventEnvelope::new(
        "thread-a",
        interaction_cursor("stream-b", 1),
        1,
        InteractionEventPayload::Requested {
            interaction: pending("request-a", "turn-a"),
        },
    );
    assert!(matches!(
        snapshot.apply(&different_stream),
        Err(InteractionProjectionError::StreamMismatch { .. })
    ));
    assert_eq!(snapshot, before);

    let remove_unknown = removed(1, "missing");
    assert!(matches!(
        snapshot.apply(&remove_unknown),
        Err(InteractionProjectionError::InvalidState { .. })
    ));
    assert_eq!(snapshot, before);
}

#[test]
fn snapshots_validate_owner_identity_uniqueness_and_per_run_count() {
    let cursor = interaction_cursor("stream-a", 0);
    let duplicate = pending("same", "turn-a");
    assert!(InteractionSnapshot::new(
        "thread-a",
        cursor.clone(),
        vec![duplicate.clone(), duplicate]
    )
    .is_err());

    let at_limit: Vec<_> = (0..MAX_PENDING_INTERACTIONS_PER_RUN)
        .map(|index| pending(&format!("request-{index}"), "turn-a"))
        .collect();
    InteractionSnapshot::new("thread-a", cursor.clone(), at_limit.clone()).unwrap();
    let mut too_many = at_limit;
    too_many.push(pending("request-over-limit", "turn-a"));
    assert!(InteractionSnapshot::new("thread-a", cursor.clone(), too_many).is_err());

    let turn = TurnInteractionSnapshot::new(
        "thread-a",
        "turn-a",
        cursor,
        vec![pending("request-a", "turn-b")],
    );
    assert!(turn.is_err());
}

#[test]
fn event_json_and_validation_are_stable() {
    let requested = requested(1, "request-a");
    requested.validate().unwrap();
    assert_eq!(
        serde_json::to_value(&requested).unwrap(),
        json!({
            "thread_id":"thread-a",
            "cursor":{"thread_id":"thread-a","stream_id":"stream-a","seq":1},
            "occurred_at_ms":101,
            "type":"requested",
            "interaction":{
                "request_id":"request-a",
                "turn_id":"turn-a",
                "kind":"vendor.review",
                "title":"Review request",
                "payload":{"digest":"sha256:abc","preview":"safe"},
                "response_schema":{
                    "$schema":"https://json-schema.org/draft/2020-12/schema",
                    "type":"object",
                    "required":["decision"]
                }
            }
        })
    );
    removed(2, "request-a").validate().unwrap();
    assert!(InteractionEventEnvelope::new(
        "thread-a",
        interaction_cursor("stream-a", 0),
        1,
        InteractionEventPayload::Removed {
            request_id: "request-a".into(),
            turn_id: "turn-a".into(),
            cause: "".into(),
        }
    )
    .validate()
    .is_err());
}

#[test]
fn fixed_window_replay_page_validates() {
    let params = SubscribeInteractionsParams {
        thread_id: "thread-a".into(),
        after: interaction_cursor("stream-a", 0),
        through: None,
        limit: 2,
    };
    let page = SubscribeInteractionsResult {
        events: vec![requested(1, "request-a"), removed(2, "request-a")],
        resume_after: interaction_cursor("stream-a", 2),
        through: interaction_cursor("stream-a", 2),
        has_more: false,
        gap: None,
    };
    page.validate_for(&params).unwrap();

    let next = SubscribeInteractionsParams {
        thread_id: "thread-a".into(),
        after: interaction_cursor("stream-a", 2),
        through: Some(interaction_cursor("stream-a", 5)),
        limit: 2,
    };
    let next_page = SubscribeInteractionsResult {
        events: vec![requested(3, "request-b"), removed(4, "request-b")],
        resume_after: interaction_cursor("stream-a", 4),
        through: interaction_cursor("stream-a", 5),
        has_more: true,
        gap: None,
    };
    next_page.validate_for(&next).unwrap();
}

#[test]
fn replay_gaps_distinguish_retention_and_stream_reset() {
    let retention_params = SubscribeInteractionsParams {
        thread_id: "thread-a".into(),
        after: interaction_cursor("stream-a", 1),
        through: None,
        limit: 10,
    };
    let retention = SubscribeInteractionsResult {
        events: Vec::new(),
        resume_after: retention_params.after.clone(),
        through: interaction_cursor("stream-a", 5),
        has_more: false,
        gap: Some(InteractionReplayGap {
            reason: InteractionReplayGapReason::Retention,
            requested: retention_params.after.clone(),
            replay_floor: interaction_cursor("stream-a", 2),
            current: interaction_cursor("stream-a", 5),
        }),
    };
    retention.validate_for(&retention_params).unwrap();

    let reset_params = SubscribeInteractionsParams {
        thread_id: "thread-a".into(),
        after: interaction_cursor("old-stream", 8),
        through: Some(interaction_cursor("old-stream", 10)),
        limit: 10,
    };
    let reset = SubscribeInteractionsResult {
        events: Vec::new(),
        resume_after: reset_params.after.clone(),
        through: interaction_cursor("new-stream", 0),
        has_more: false,
        gap: Some(InteractionReplayGap {
            reason: InteractionReplayGapReason::StreamReset,
            requested: reset_params.after.clone(),
            replay_floor: interaction_cursor("new-stream", 0),
            current: interaction_cursor("new-stream", 0),
        }),
    };
    reset.validate_for(&reset_params).unwrap();
}

#[test]
fn replay_rejects_drifting_windows_noncontiguous_pages_and_bad_limits() {
    for limit in [0, MAX_INTERACTION_REPLAY_PAGE_LIMIT + 1] {
        assert!(SubscribeInteractionsParams {
            thread_id: "thread-a".into(),
            after: interaction_cursor("stream-a", 0),
            through: None,
            limit,
        }
        .validate()
        .is_err());
    }
    SubscribeInteractionsParams {
        thread_id: "thread-a".into(),
        after: interaction_cursor("stream-a", 0),
        through: None,
        limit: MAX_INTERACTION_REPLAY_PAGE_LIMIT,
    }
    .validate()
    .unwrap();

    let params = SubscribeInteractionsParams {
        thread_id: "thread-a".into(),
        after: interaction_cursor("stream-a", 0),
        through: Some(interaction_cursor("stream-a", 2)),
        limit: 2,
    };
    let drifting = SubscribeInteractionsResult {
        events: vec![requested(1, "request-a")],
        resume_after: interaction_cursor("stream-a", 1),
        through: interaction_cursor("stream-a", 3),
        has_more: true,
        gap: None,
    };
    assert!(drifting.validate_for(&params).is_err());

    let skipped = SubscribeInteractionsResult {
        events: vec![requested(2, "request-a")],
        resume_after: interaction_cursor("stream-a", 2),
        through: interaction_cursor("stream-a", 2),
        has_more: false,
        gap: None,
    };
    assert!(skipped.validate_for(&params).is_err());
}

#[test]
fn rpc_params_reject_unknown_fields_and_validate_identity() {
    let get: GetSessionInteractionsParams =
        serde_json::from_value(json!({"thread_id":"thread-a"})).unwrap();
    get.validate().unwrap();
    assert!(serde_json::from_value::<GetSessionInteractionsParams>(
        json!({"thread_id":"thread-a","unknown":true})
    )
    .is_err());

    let turn: GetTurnInteractionsParams = serde_json::from_value(json!({
        "thread_id":"thread-a",
        "turn_id":"turn-a"
    }))
    .unwrap();
    turn.validate().unwrap();

    let producer = RequestInteractionParams::new(
        "thread-a",
        "turn-a",
        "host-call-a",
        "00000000-0000-4000-8000-000000000001",
        request("vendor.review"),
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(&producer).unwrap()["host_call_id"],
        "host-call-a"
    );
    assert!(RequestInteractionParams::new(
        "thread-a",
        "turn-a",
        "host-call-a",
        "not-a-uuid",
        request("vendor.review")
    )
    .is_err());

    let respond = RespondInteractionParams::new(
        "thread-a",
        "turn-a",
        "request-a",
        json!({"decision":"approve"}),
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(&respond).unwrap(),
        json!({
            "thread_id":"thread-a",
            "turn_id":"turn-a",
            "request_id":"request-a",
            "response":{"decision":"approve"}
        })
    );
    RespondInteractionResult {
        request_id: "request-a".into(),
        resolved: true,
    }
    .validate()
    .unwrap();
}

#[test]
fn start_wrapper_is_exact_and_rejects_unknown_legacy_keys() {
    let wrapper = StartThreadWithInteractionsParams::new(start_params(), true);
    assert_eq!(
        serde_json::to_string(&wrapper).unwrap(),
        r#"{"model":"model-a","interactions_enabled":true}"#
    );
    assert_eq!(
        serde_json::from_str::<StartThreadWithInteractionsParams>(
            r#"{"model":"model-a","interactions_enabled":true}"#
        )
        .unwrap(),
        wrapper
    );
    assert!(
        serde_json::from_value::<StartThreadWithInteractionsParams>(json!({
            "model":"model-a",
            "interactions_enabled":true,
            "future_field":"must not be ignored"
        }))
        .is_err()
    );
    assert!(serde_json::from_value::<StartThreadWithInteractionsParams>(
        json!({"model":"model-a"})
    )
    .is_err());
    assert!(serde_json::from_str::<StartThreadWithInteractionsParams>(
        r#"{"model":"model-a","interactions_enabled":false,"interactions_enabled":true}"#
    )
    .is_err());
}

#[test]
fn persistent_wrappers_are_checked_without_changing_legacy_params() {
    let key = RecoveryKey {
        recovery_id: "00000000-0000-4000-8000-000000000001".into(),
        secret: "a".repeat(64),
    };
    let mut session = start_params();
    session.session_id = Some("00000000-0000-4000-8000-000000000002".into());
    let create = CreatePersistentSessionParams {
        key: key.clone(),
        session: session.clone(),
        run_defaults: SessionRunDefaults::default(),
    };
    let create_wrapper = CreatePersistentSessionWithInteractionsParams::new(create.clone(), true);
    create_wrapper.validate().unwrap();
    let create_json = serde_json::to_value(&create_wrapper).unwrap();
    assert_eq!(create_json["interactions_enabled"], true);
    assert!(
        serde_json::from_value::<CreatePersistentSessionWithInteractionsParams>(json!({
            "key": key,
            "session": session,
            "run_defaults":{"max_steps":10},
            "interactions_enabled":true,
            "unknown":true
        }))
        .is_err()
    );

    let attach = AttachRecoveryParams {
        key: create.key,
        expected_revision: 1,
        session: create.session,
        run_defaults: create.run_defaults,
    };
    let attach_wrapper = AttachRecoveryWithInteractionsParams::new(attach, false);
    attach_wrapper.validate().unwrap();
    let round_trip: AttachRecoveryWithInteractionsParams =
        serde_json::from_value(serde_json::to_value(&attach_wrapper).unwrap()).unwrap();
    assert_eq!(round_trip, attach_wrapper);
}

#[test]
fn local_subscriber_and_journal_bounds_are_checked_exactly() {
    for capacity in [1, DEFAULT_INTERACTION_SUBSCRIBER_OUTPUT_CAPACITY, 4_096] {
        validate_interaction_subscriber_output_capacity(capacity).unwrap();
    }
    for capacity in [0, MAX_INTERACTION_SUBSCRIBER_OUTPUT_CAPACITY + 1] {
        assert!(validate_interaction_subscriber_output_capacity(capacity).is_err());
    }

    validate_interaction_journal_usage(
        MAX_INTERACTION_JOURNAL_EVENTS,
        MAX_INTERACTION_JOURNAL_BYTES,
    )
    .unwrap();
    assert!(validate_interaction_journal_usage(MAX_INTERACTION_JOURNAL_EVENTS + 1, 0).is_err());
    assert!(validate_interaction_journal_usage(0, MAX_INTERACTION_JOURNAL_BYTES + 1).is_err());

    let oversized = MAX_INTERACTION_JOURNAL_BYTES + 1;
    assert_eq!(
        interaction_event_retention(oversized),
        InteractionEventRetention::AdvanceReplayFloorWithoutRetaining
    );
    assert_eq!(
        interaction_event_retention(MAX_INTERACTION_JOURNAL_BYTES),
        InteractionEventRetention::Retain
    );

    let bounded_request = InteractionRequest::new(
        "vendor.boundary",
        "Boundary",
        Value::String("x".repeat(MAX_INTERACTION_PAYLOAD_BYTES - 2)),
        Some(json!({"x":"x".repeat(MAX_INTERACTION_SCHEMA_BYTES - 8)})),
    )
    .unwrap();
    let bounded_event = InteractionEventEnvelope::new(
        "thread-a",
        interaction_cursor("stream-a", 1),
        1,
        InteractionEventPayload::Requested {
            interaction: PendingInteraction::new("request-a", "turn-a", bounded_request).unwrap(),
        },
    );
    let event_bytes = serde_json::to_vec(&bounded_event).unwrap().len();
    assert!(event_bytes < MAX_INTERACTION_JOURNAL_BYTES);
    assert_eq!(
        interaction_event_retention(event_bytes),
        InteractionEventRetention::Retain
    );
}

#[test]
fn public_struct_literals_and_legacy_json_remain_unchanged() {
    let _definition = AgentDefinition {
        limits: None,
        context_policy: None,
        name: "agent-a".into(),
        model: "model-a".into(),
        system_prompt: None,
        provider_config: None,
        provider_ref: None,
        tool_names: Vec::new(),
        default_options: RunTurnOptions::default(),
        max_steps: 10,
        timeout_ms: None,
    };
    let thread = start_params();
    assert_eq!(
        serde_json::to_string(&thread).unwrap(),
        r#"{"model":"model-a"}"#
    );
    let _snapshot = RunSnapshot {
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
    };

    let run = RunEvent {
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
    };
    assert_eq!(
        serde_json::to_string(&run).unwrap(),
        r#"{"thread_id":"thread-a","turn_id":"turn-a","seq":1,"type":"stream","event":{"type":"text_delta","turn_id":"turn-a","item_id":"item-a","delta":"hello"}}"#
    );

    let session = SessionEventEnvelope::new(
        "thread-a",
        SessionCursor {
            thread_id: "thread-a".into(),
            stream_id: "session-stream".into(),
            seq: 1,
        },
        9,
        SessionEventPayload::RunEvent { event: run },
    );
    assert_eq!(
        serde_json::to_string(&session).unwrap(),
        r#"{"thread_id":"thread-a","cursor":{"thread_id":"thread-a","stream_id":"session-stream","seq":1},"occurred_at_ms":9,"type":"run_event","event":{"thread_id":"thread-a","turn_id":"turn-a","seq":1,"type":"stream","event":{"type":"text_delta","turn_id":"turn-a","item_id":"item-a","delta":"hello"}}}"#
    );
}
