use serde_json::{json, Value};
use whale_adapters::{OpenAIAdapter, ProtocolAdapter, SamplingOptions};
use whale_protocol::canonical::{CanonicalItem, CanonicalToolOutput, MessagePhase};

fn call(id: &str) -> CanonicalItem {
    CanonicalItem::tool_call(id, None, "lookup", Some(json!({"query":id})), "")
}
fn result(id: &str) -> CanonicalItem {
    CanonicalItem::tool_result(id, CanonicalToolOutput::text(format!("result {id}")), false)
}
fn messages(history: &[CanonicalItem]) -> Vec<Value> {
    OpenAIAdapter::new("key")
        .serialize_request(None, history, &[], &SamplingOptions::new("model"))
        .unwrap()
        .0["messages"]
        .as_array()
        .unwrap()
        .clone()
}

#[test]
fn consecutive_tool_calls_form_one_assistant_message_before_matched_results() {
    let messages = messages(&[
        CanonicalItem::user_text("look up both"),
        call("a"),
        call("b"),
        result("b"),
        result("a"),
    ]);
    assert_eq!(messages.len(), 4);
    assert_eq!(messages[1]["role"], "assistant");
    assert_eq!(messages[1]["tool_calls"].as_array().unwrap().len(), 2);
    assert_eq!(messages[1]["tool_calls"][0]["id"], "a");
    assert_eq!(messages[1]["tool_calls"][1]["id"], "b");
    assert_eq!(
        messages[1]["tool_calls"][1]["function"]["arguments"],
        "{\"query\":\"b\"}"
    );
    assert_eq!(messages[2]["role"], "tool");
    assert_eq!(messages[2]["tool_call_id"], "b");
    assert_eq!(messages[3]["tool_call_id"], "a");
}

#[test]
fn adjacent_call_groups_do_not_merge_across_any_canonical_boundary() {
    for boundary in [
        CanonicalItem::user_text("new instruction"),
        CanonicalItem::assistant_text("commentary", MessagePhase::Commentary),
        CanonicalItem::reasoning("summary", None, None),
        result("a"),
    ] {
        let messages = messages(&[call("a"), call("b"), boundary, call("c"), call("d")]);
        assert_eq!(messages.len(), 3, "{messages:?}");
        assert_eq!(messages[0]["tool_calls"].as_array().unwrap().len(), 2);
        assert_eq!(messages[2]["tool_calls"].as_array().unwrap().len(), 2);
        assert_eq!(messages[0]["tool_calls"][0]["id"], "a");
        assert_eq!(messages[2]["tool_calls"][0]["id"], "c");
    }
}
