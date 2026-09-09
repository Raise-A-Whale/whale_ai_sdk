use serde_json::json;
use whale_adapters::{SamplingOptions, ToolDefinition};
use whale_core::model::ModelRequest;
use whale_protocol::{
    contexts::{ModelContext, RunContextInfo},
    CanonicalItem,
};

#[test]
fn model_request_record_preserves_actual_projection_options_tools_and_identity() {
    let request = ModelRequest {
        context: RunContextInfo {
            agent_name: Some("audit".into()),
            thread_id: "live".into(),
            turn_id: "turn".into(),
            deadline_unix_ms: Some(123),
        },
        step_index: 2,
        step_id: "step".into(),
        model_context: ModelContext {
            system_prompt: Some("projected prompt".into()),
            items: vec![CanonicalItem::user_text("projected history")],
        },
        tools: vec![ToolDefinition::new(
            "lookup",
            "Read data",
            json!({"type":"object"}),
        )],
        options: SamplingOptions::new("effective-model"),
    };
    let saved = serde_json::to_value(&request).unwrap();
    assert_eq!(saved["context"]["turn_id"], "turn");
    assert_eq!(saved["step_id"], "step");
    assert_eq!(saved["step_index"], 2);
    assert_eq!(saved["model_context"]["system_prompt"], "projected prompt");
    assert_eq!(
        saved["model_context"]["items"],
        serde_json::to_value(&request.model_context.items).unwrap()
    );
    assert_eq!(saved["tools"][0]["name"], "lookup");
    assert_eq!(saved["options"]["model"], "effective-model");
    let keys = saved.as_object().unwrap();
    assert_eq!(
        keys.len(),
        6,
        "only the owned model request crosses the journal boundary"
    );
}
