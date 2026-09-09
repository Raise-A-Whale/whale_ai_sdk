use serde_json::json;
use whale_protocol::agents::{AgentDefinition, ProviderApi, ProviderAuth, ProviderConfig};
use whale_protocol::rpc::StartThreadParams;

#[test]
fn provider_config_roundtrips_without_secret_values() {
    let config = ProviderConfig {
        api: ProviderApi::OpenaiResponses,
        base_url: Some("http://127.0.0.1:1234/v1".into()),
        auth: Some(ProviderAuth::Env {
            variable: "MY_PROVIDER_KEY".into(),
        }),
    };
    let value = serde_json::to_value(&config).unwrap();
    assert_eq!(
        value,
        json!({"api":"openai_responses","base_url":"http://127.0.0.1:1234/v1","auth":{"type":"env","variable":"MY_PROVIDER_KEY"}})
    );
    assert_eq!(
        serde_json::from_value::<ProviderConfig>(value).unwrap(),
        config
    );
    assert!(serde_json::from_value::<ProviderConfig>(json!({"api":"typo"})).is_err());
}

#[test]
fn definition_defaults_are_portable_and_invalid_limits_rejected() {
    let mut definition: AgentDefinition =
        serde_json::from_value(json!({"name":"analysis","model":"model"})).unwrap();
    assert_eq!(definition.max_steps, 10);
    assert!(definition.tool_names.is_empty());
    definition.validate().unwrap();
    definition.max_steps = 0;
    assert!(definition.validate().is_err());
    definition.max_steps = 10;
    definition.tool_names = vec!["lookup".into(), "lookup".into()];
    assert!(definition.validate().is_err());
}

#[test]
fn legacy_thread_request_remains_valid_without_new_options() {
    let legacy: StartThreadParams = serde_json::from_value(json!({"model":"legacy"})).unwrap();
    assert!(legacy.provider_config.is_none());
    assert!(legacy.options.is_none());
    let configured: StartThreadParams = serde_json::from_value(json!({
        "model":"local", "provider_config":{"api":"openai_chat_completions","auth":{"type":"none"}},
        "options":{"temperature":0.2,"max_tokens":512}
    }))
    .unwrap();
    assert_eq!(configured.options.unwrap().max_tokens, Some(512));
    assert_eq!(
        configured.provider_config.unwrap().auth,
        Some(ProviderAuth::None)
    );
}
