use serde_json::json;
use whale_protocol::{
    agents::AgentDefinition,
    models::{
        InspectProviderParams, InspectProviderResult, ModelCapabilities, ModelCapabilityScope,
        ModelContentKind, ModelOption, METHOD_PROVIDER_INSPECT,
    },
    rpc::StartThreadParams,
};

#[test]
fn provider_reference_is_portable_and_omitted_for_legacy_requests() {
    let mut definition = AgentDefinition::new("local-agent", "native-model");
    definition.provider_ref = Some("native".into());
    definition.validate().unwrap();
    let wire = serde_json::to_value(&definition).unwrap();
    assert_eq!(wire["provider_ref"], "native");
    assert_eq!(
        serde_json::from_value::<AgentDefinition>(wire).unwrap(),
        definition
    );
    let request: StartThreadParams = serde_json::from_value(json!({
        "model":"native-model", "provider_ref":"native"
    }))
    .unwrap();
    assert_eq!(request.provider_ref.as_deref(), Some("native"));
    let legacy: StartThreadParams = serde_json::from_value(json!({"model":"gpt-test"})).unwrap();
    assert_eq!(legacy.provider_ref, None);
    assert!(serde_json::to_value(legacy)
        .unwrap()
        .get("provider_ref")
        .is_none());
}

#[test]
fn reference_selection_rejects_ambiguous_or_empty_values() {
    for reference in ["", " ", " native", "native\n"] {
        let mut definition = AgentDefinition::new("agent", "model");
        definition.provider_ref = Some(reference.into());
        assert!(definition.validate().is_err());
    }
    for fields in [
        json!({"provider":"openai"}),
        json!({"provider_config":{"api":"openai_responses","auth":{"type":"none"}}}),
    ] {
        let mut value = json!({"model":"model", "provider_ref":"native"});
        value
            .as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        let params: InspectProviderParams = serde_json::from_value(value).unwrap();
        assert!(params.validate().is_err());
    }
    let legacy: InspectProviderParams = serde_json::from_value(json!({
        "model":"model", "provider":"openai",
        "provider_config":{"api":"openai_responses","auth":{"type":"none"}}
    }))
    .unwrap();
    legacy.validate().unwrap();
    let mut definition = AgentDefinition::new("agent", "model");
    definition.provider_ref = Some("native".into());
    definition.provider_config = legacy.provider_config;
    assert!(definition.validate().is_err());
}

#[test]
fn inspection_describes_capability_scope_and_exact_selection() {
    assert_eq!(METHOD_PROVIDER_INSPECT, "provider.inspect");
    let mut capabilities = ModelCapabilities::text_only();
    capabilities.scope = ModelCapabilityScope::Protocol;
    capabilities.user_content.push(ModelContentKind::Image);
    capabilities.options.push(ModelOption::MaxTokens);
    let result = InspectProviderResult {
        model: "model".into(),
        provider_ref: Some("native".into()),
        capabilities,
    };
    let wire = serde_json::to_value(&result).unwrap();
    assert_eq!(wire["model"], "model");
    assert_eq!(wire["provider_ref"], "native");
    assert_eq!(wire["capabilities"]["scope"], "protocol");
    assert_eq!(
        wire["capabilities"]["user_content"],
        json!(["text", "image"])
    );
    assert_eq!(wire["capabilities"]["options"], json!(["max_tokens"]));
    assert_eq!(
        serde_json::from_value::<InspectProviderResult>(wire.clone()).unwrap(),
        result
    );
    let mut malformed = wire.clone();
    malformed["capabilities"]["tool_calls"] = json!("false");
    assert!(serde_json::from_value::<InspectProviderResult>(malformed).is_err());
    let mut missing = wire;
    missing["capabilities"]
        .as_object_mut()
        .unwrap()
        .remove("scope");
    assert!(serde_json::from_value::<InspectProviderResult>(missing).is_err());
}

#[test]
fn inspection_requires_a_nonempty_model_and_typed_reference() {
    for model in ["", "\t"] {
        let params: InspectProviderParams = serde_json::from_value(json!({"model":model})).unwrap();
        assert!(params.validate().is_err());
    }
    for invalid in [json!({}), json!({"model":"m","provider_ref":7})] {
        assert!(serde_json::from_value::<InspectProviderParams>(invalid).is_err());
    }
}

#[test]
fn model_options_preserve_thinking_budget_and_explicit_cache_disable() {
    let value = json!({"thinking_budget":2048,"prompt_caching":false});
    let options: whale_protocol::rpc::RunTurnOptions =
        serde_json::from_value(value.clone()).unwrap();
    assert_eq!(options.thinking_budget, Some(2048));
    assert_eq!(options.prompt_caching, Some(false));
    assert_eq!(serde_json::to_value(options).unwrap(), value);
}

#[test]
fn unknown_or_mistyped_options_are_rejected_instead_of_ignored() {
    for value in [
        json!({"future_option":true}),
        json!({"prompt_caching":"true"}),
        json!({"thinking_budget":-1}),
    ] {
        assert!(serde_json::from_value::<whale_protocol::rpc::RunTurnOptions>(value).is_err());
    }
    let mut definition = AgentDefinition::new("a", "m");
    definition.default_options.thinking_budget = Some(0);
    assert!(definition.validate().is_err());
}
