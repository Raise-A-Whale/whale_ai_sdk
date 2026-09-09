use serde_json::json;
use whale_core::context::{
    validate_model_context, ContextPolicy, FullHistoryContext, RecentTurnsContext,
};
use whale_core::execution::CancellationToken;
use whale_protocol::canonical::{CanonicalItem, CanonicalToolOutput, MessagePhase};
use whale_protocol::contexts::{ContextBuildRequest, ModelContext, RunContextInfo};

fn call(id: &str) -> CanonicalItem {
    CanonicalItem::tool_call(id, None, "lookup", Some(json!({"id":id})), "")
}
fn result(id: &str) -> CanonicalItem {
    CanonicalItem::tool_result(id, CanonicalToolOutput::text(format!("result {id}")), false)
}
fn assistant(text: &str) -> CanonicalItem {
    CanonicalItem::assistant_text(text, MessagePhase::FinalAnswer)
}
fn request(history: Vec<CanonicalItem>) -> ContextBuildRequest {
    ContextBuildRequest {
        context: RunContextInfo {
            agent_name: Some("analyst".into()),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            deadline_unix_ms: None,
        },
        step_index: 2,
        model: "model".into(),
        system_prompt: Some("Business instructions".into()),
        history,
    }
}
fn projected(items: Vec<CanonicalItem>) -> ModelContext {
    ModelContext {
        system_prompt: Some("Business instructions".into()),
        items,
    }
}

#[tokio::test]
async fn full_history_preserves_all_items_and_input_snapshot() {
    let original = request(vec![
        CanonicalItem::user_text("start"),
        call("a"),
        call("b"),
        result("b"),
        result("a"),
        assistant("done"),
    ]);
    let before = serde_json::to_value(&original).unwrap();
    let result = FullHistoryContext
        .build(original.clone(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(result.items, original.history);
    assert_eq!(result.system_prompt, original.system_prompt);
    assert_eq!(serde_json::to_value(&original).unwrap(), before);
}

#[tokio::test]
async fn recent_turns_keep_the_current_turn_and_every_intermediate_tool_step() {
    let older = vec![
        CanonicalItem::user_text("old"),
        call("old"),
        result("old"),
        assistant("old answer"),
    ];
    let retained = vec![
        CanonicalItem::user_text("current"),
        CanonicalItem::reasoning("summary", None, None),
        call("a"),
        call("b"),
        result("b"),
        result("a"),
        assistant("intermediate"),
        call("c"),
        result("c"),
    ];
    let mut history = older;
    history.extend(retained.clone());
    let input = request(history.clone());
    let result = RecentTurnsContext::new(1)
        .unwrap()
        .build(input.clone(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(result.items, retained);
    assert_eq!(result.system_prompt, input.system_prompt);
    assert_eq!(input.history, history);
    validate_model_context(&result).unwrap();
}

#[tokio::test]
async fn recent_turns_count_user_messages_rather_than_model_steps() {
    let prefix = assistant("imported preamble");
    let first = CanonicalItem::user_text("first");
    let second = CanonicalItem::user_text("second");
    let third = CanonicalItem::user_text("third");
    let history = vec![
        prefix,
        first,
        assistant("one"),
        second.clone(),
        call("a"),
        result("a"),
        third.clone(),
    ];
    let output = RecentTurnsContext::new(2)
        .unwrap()
        .build(request(history.clone()), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(output.items, history[3..]);
    let all = RecentTurnsContext::new(3)
        .unwrap()
        .build(request(history.clone()), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(all.items, history);
}

#[tokio::test]
async fn empty_and_no_user_histories_are_preserved_and_zero_limit_is_rejected() {
    assert!(RecentTurnsContext::new(0).is_err());
    for history in [vec![], vec![assistant("imported"), call("a"), result("a")]] {
        let output = RecentTurnsContext::new(1)
            .unwrap()
            .build(request(history.clone()), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(output.items, history);
    }
}

#[test]
fn validation_rejects_orphan_duplicate_and_unanswered_calls() {
    for (label, history) in [
        ("orphan result", vec![result("missing")]),
        ("duplicate call", vec![call("a"), call("a"), result("a")]),
        (
            "duplicate result",
            vec![call("a"), result("a"), result("a")],
        ),
        (
            "reused call across complete batches",
            vec![
                call("a"),
                result("a"),
                CanonicalItem::user_text("new"),
                call("a"),
                result("a"),
            ],
        ),
        ("unanswered call", vec![call("a")]),
        (
            "partially answered batch",
            vec![call("a"), call("b"), result("a")],
        ),
        ("unknown result", vec![call("a"), result("other")]),
        ("empty call id", vec![call(""), result("")]),
    ] {
        assert!(
            validate_model_context(&projected(history)).is_err(),
            "accepted {label}"
        );
    }
}

#[test]
fn validation_does_not_allow_unfinished_groups_to_cross_message_or_step_boundaries() {
    for boundary in [
        CanonicalItem::user_text("next"),
        assistant("reply"),
        CanonicalItem::reasoning("reasoning", None, None),
    ] {
        assert!(
            validate_model_context(&projected(vec![call("a"), boundary, result("a")])).is_err()
        );
    }
    assert!(validate_model_context(&projected(vec![
        call("a"),
        call("b"),
        result("a"),
        call("c"),
        result("b"),
        result("c")
    ]))
    .is_err());
}

#[test]
fn validation_accepts_multiple_complete_batches_and_failed_tool_results() {
    let mut denied = result("b");
    if let CanonicalItem::ToolResult { is_error, .. } = &mut denied {
        *is_error = true;
    }
    validate_model_context(&projected(vec![
        CanonicalItem::user_text("user"),
        call("a"),
        call("b"),
        denied,
        result("a"),
        assistant("continued"),
        call("c"),
        result("c"),
    ]))
    .unwrap();
}

#[tokio::test]
async fn builtins_reject_cancelled_work() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(FullHistoryContext
        .build(request(vec![]), cancellation.clone())
        .await
        .is_err());
    assert!(RecentTurnsContext::new(1)
        .unwrap()
        .build(request(vec![]), cancellation)
        .await
        .is_err());
}

#[tokio::test]
async fn full_history_does_not_repair_unanswered_calls() {
    assert!(FullHistoryContext
        .build(
            request(vec![CanonicalItem::user_text("user"), call("a")]),
            CancellationToken::new()
        )
        .await
        .is_err());
}

#[tokio::test]
async fn recent_turns_cannot_hide_a_malformed_discarded_turn() {
    let history = vec![
        CanonicalItem::user_text("old"),
        call("unanswered"),
        CanonicalItem::user_text("new"),
    ];
    assert!(RecentTurnsContext::new(1)
        .unwrap()
        .build(request(history), CancellationToken::new())
        .await
        .is_err());
}
