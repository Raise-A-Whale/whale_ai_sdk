use serde_json::json;
use whale_adapters::{
    AnthropicAdapter, OpenAIAdapter, OpenAIWireApi, ProtocolAdapter, SamplingOptions,
};
use whale_protocol::models::{ModelCapabilityScope, ModelContentKind};
use whale_protocol::{CanonicalContent, CanonicalItem, CanonicalToolOutput, MessagePhase};

fn audio() -> CanonicalContent {
    CanonicalContent::Audio {
        mime_type: "audio/wav".into(),
        data: Some("AA==".into()),
        uri: None,
    }
}
#[test]
fn unsupported_role_content_and_options_are_rejected_by_wire_adapters() {
    let adapters: Vec<Box<dyn ProtocolAdapter>> = vec![
        Box::new(OpenAIAdapter::new("")),
        Box::new(AnthropicAdapter::new("")),
        Box::new(OpenAIAdapter::with_options(
            "",
            "http://localhost",
            OpenAIWireApi::Responses,
        )),
    ];
    for adapter in adapters {
        assert_eq!(adapter.capabilities().scope, ModelCapabilityScope::Protocol);
        let cases = vec![
            CanonicalItem::UserMessage {
                id: "u".into(),
                content: vec![audio()],
            },
            CanonicalItem::AssistantMessage {
                id: "a".into(),
                content: vec![CanonicalContent::image_uri(
                    "image/png",
                    "https://example.test/i.png",
                )],
                phase: MessagePhase::FinalAnswer,
            },
            CanonicalItem::tool_result("c", CanonicalToolOutput::blocks(vec![audio()]), false),
        ];
        for item in cases {
            assert!(
                adapter
                    .serialize_request(None, &[item], &[], &SamplingOptions::new("m"))
                    .is_err(),
                "{} accepted unsupported content",
                adapter.provider_name()
            );
        }
        let mut options = SamplingOptions::new("m");
        options.max_tokens = Some(0);
        assert!(adapter.serialize_request(None, &[], &[], &options).is_err());
    }
    let chat = OpenAIAdapter::new("");
    assert!(chat
        .serialize_request(
            None,
            &[CanonicalItem::tool_result(
                "c",
                CanonicalToolOutput::blocks(vec![CanonicalContent::image_uri(
                    "image/png",
                    "https://example.test/i.png"
                )]),
                false
            )],
            &[],
            &SamplingOptions::new("m")
        )
        .is_err());
}
#[test]
fn declared_anthropic_tool_result_image_uri_is_not_silently_dropped() {
    let adapter = AnthropicAdapter::new("");
    assert!(adapter
        .capabilities()
        .tool_result_content
        .contains(&ModelContentKind::Image));
    let item = CanonicalItem::tool_result(
        "c",
        CanonicalToolOutput::blocks(vec![CanonicalContent::image_uri(
            "image/png",
            "https://example.test/result.png",
        )]),
        false,
    );
    let (body, _) = adapter
        .serialize_request(None, &[item], &[], &SamplingOptions::new("m"))
        .unwrap();
    assert_eq!(
        body["messages"][0]["content"][0]["content"],
        json!([{"type":"image","source":{"type":"url","url":"https://example.test/result.png"}}])
    );
}
#[test]
fn images_without_payload_are_rejected_instead_of_disappearing() {
    for adapter in [
        Box::new(OpenAIAdapter::new("")) as Box<dyn ProtocolAdapter>,
        Box::new(AnthropicAdapter::new("")),
    ] {
        let item = CanonicalItem::UserMessage {
            id: "u".into(),
            content: vec![CanonicalContent::Image {
                mime_type: "image/png".into(),
                data: None,
                uri: None,
            }],
        };
        assert!(adapter
            .serialize_request(None, &[item], &[], &SamplingOptions::new("m"))
            .is_err());
    }
}

#[test]
fn incompatible_anthropic_temperature_is_rejected_without_silent_option_loss() {
    let adapter = AnthropicAdapter::new("");
    let mut options = SamplingOptions::new("m");
    options.thinking_budget = Some(2048);
    options.temperature = Some(0.2);
    assert!(adapter.serialize_request(None, &[], &[], &options).is_err());
    options.temperature = Some(1.0);
    let (body, _) = adapter.serialize_request(None, &[], &[], &options).unwrap();
    assert_eq!(body["temperature"], json!(1.0));
}

#[test]
fn openai_protocols_preserve_explicit_temperature_with_reasoning_effort() {
    for api in [OpenAIWireApi::ChatCompletions, OpenAIWireApi::Responses] {
        let adapter = OpenAIAdapter::with_options("", "http://localhost", api);
        let mut options = SamplingOptions::new("m");
        options.temperature = Some(0.5);
        options.reasoning_effort = Some("high".into());
        let (body, _) = adapter.serialize_request(None, &[], &[], &options).unwrap();
        assert_eq!(body["temperature"], json!(0.5));
        if api == OpenAIWireApi::Responses {
            assert_eq!(body["reasoning"]["effort"], "high");
        } else {
            assert_eq!(body["reasoning_effort"], "high");
        }
    }
}
