use whale_adapters::{
    AnthropicAdapter, OpenAIAdapter, OpenAIWireApi, ProtocolAdapter, SamplingOptions,
};

#[test]
fn openai_adapter_debug_redacts_api_key() {
    let secret = "openai-super-secret-value";
    let adapter = OpenAIAdapter::with_options(
        secret,
        "https://gateway.example/v1",
        OpenAIWireApi::Responses,
    );

    let debug = format!("{adapter:?}");

    assert!(!debug.contains(secret));
    assert!(debug.contains("api_key: \"[REDACTED]\""));
    assert!(debug.contains("gateway.example"));
    assert!(debug.contains("Responses"));
}

#[test]
fn anthropic_adapter_debug_redacts_api_key() {
    let secret = "anthropic-super-secret-value";
    let adapter = AnthropicAdapter::with_base_url(secret, "https://gateway.example/v1");

    let debug = format!("{adapter:?}");

    assert!(!debug.contains(secret));
    assert!(debug.contains("api_key: \"[REDACTED]\""));
    assert!(debug.contains("gateway.example"));
}

#[test]
fn openai_auth_header_debug_redacts_api_key() {
    let secret = "openai-header-super-secret-value";
    let adapter = OpenAIAdapter::new(secret);
    let (_, headers) = adapter
        .serialize_request(None, &[], &[], &SamplingOptions::new("gpt-test-model"))
        .expect("empty request serializes");

    let authorization = headers
        .get("authorization")
        .expect("credential produces authorization header");
    assert!(authorization.is_sensitive());
    assert!(!format!("{headers:?}").contains(secret));
}

#[test]
fn anthropic_auth_header_debug_redacts_api_key() {
    let secret = "anthropic-header-super-secret-value";
    let adapter = AnthropicAdapter::new(secret);
    let (_, headers) = adapter
        .serialize_request(None, &[], &[], &SamplingOptions::new("claude-test-model"))
        .expect("empty request serializes");

    let api_key = headers
        .get("x-api-key")
        .expect("credential produces x-api-key header");
    assert!(api_key.is_sensitive());
    assert!(!format!("{headers:?}").contains(secret));
}
