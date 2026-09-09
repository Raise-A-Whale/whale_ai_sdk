//! Boundary fixtures follow the official Responses SSE contract:
//! https://developers.openai.com/api/reference/resources/responses/streaming-events
use bytes::Bytes;
use futures::{stream, StreamExt};
use serde_json::{json, Value};
use whale_adapters::{
    AnthropicAdapter, OpenAIAdapter, OpenAIWireApi, ProtocolAdapter, SamplingOptions,
};
use whale_protocol::canonical::{
    CanonicalContent, CanonicalItem, CanonicalToolOutput, MessagePhase,
};
use whale_protocol::events::AgentStreamEvent;

fn adapter() -> OpenAIAdapter {
    OpenAIAdapter::with_options("", "http://localhost/v1", OpenAIWireApi::Responses)
}
async fn parse(
    values: Vec<Value>,
    fragment: usize,
) -> Vec<Result<AgentStreamEvent, whale_adapters::AdapterError>> {
    let wire = values
        .iter()
        .map(|v| {
            format!(
                "event: {}\r\ndata: {}\r\n\r\n",
                v["type"].as_str().unwrap(),
                v
            )
        })
        .collect::<String>();
    parse_wire(wire, fragment).await
}
async fn parse_wire(
    wire: String,
    fragment: usize,
) -> Vec<Result<AgentStreamEvent, whale_adapters::AdapterError>> {
    let chunks: Vec<Result<Bytes, reqwest::Error>> = wire
        .as_bytes()
        .chunks(fragment)
        .map(|b| Ok(Bytes::copy_from_slice(b)))
        .collect();
    adapter()
        .parse_stream(Box::pin(stream::iter(chunks)))
        .collect()
        .await
}
fn message(text: &str) -> Value {
    json!({"id":"msg_a", "type":"message", "role":"assistant", "phase":"commentary", "status":"completed", "content":[{"type":"output_text", "text":text, "annotations":[]}]})
}
fn completed(output: Vec<Value>) -> Value {
    json!({"type":"response.completed", "response":{"status":"completed","output":output,"usage":{"input_tokens":7,"output_tokens":5,"input_tokens_details":{"cached_tokens":2},"output_tokens_details":{"reasoning_tokens":3}}}})
}
fn items(events: &[AgentStreamEvent]) -> Vec<CanonicalItem> {
    events
        .iter()
        .filter_map(|e| {
            if let AgentStreamEvent::ItemCompleted { item, .. } = e {
                Some(item.clone())
            } else {
                None
            }
        })
        .collect()
}

#[tokio::test]
async fn fragmented_unicode_text_done_is_not_duplicated_by_item_and_response_done() {
    let events = parse(vec![
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"msg_a","type":"message","role":"assistant","phase":"commentary","content":[]}}),
        json!({"type":"response.output_text.delta","item_id":"msg_a","output_index":0,"content_index":0,"delta":"鲸🐋"}),
        json!({"type":"response.output_text.done","item_id":"msg_a","output_index":0,"content_index":0,"text":"鲸🐋"}),
        json!({"type":"response.output_item.done","output_index":0,"item":message("鲸🐋")}),
        completed(vec![message("鲸🐋")]),
    ], 1).await.into_iter().collect::<Result<Vec<_>,_>>().unwrap();
    assert!(events
        .iter()
        .any(|e| matches!(e, AgentStreamEvent::TextDelta {delta,..} if delta=="鲸🐋")));
    assert_eq!(items(&events).len(), 1);
    assert!(
        matches!(&items(&events)[0], CanonicalItem::AssistantMessage{phase:MessagePhase::Commentary,content,..} if content==&vec![CanonicalContent::text("鲸🐋")])
    );
    assert!(
        matches!(events.last().unwrap(), AgentStreamEvent::TurnCompleted {usage,..} if usage.input_tokens==7 && usage.output_tokens==5 && usage.reasoning_tokens==3 && usage.cache_read_input_tokens==2)
    );
}

#[tokio::test]
async fn tool_and_encrypted_reasoning_round_trip_into_second_request() {
    let reasoning = json!({"id":"rs_a","type":"reasoning","summary":[{"type":"summary_text","text":"Need lookup"}],"encrypted_content":"opaque-token"});
    let call = json!({"id":"fc_a","type":"function_call","call_id":"call_a","name":"lookup","arguments":"{\"query\":\"whale\"}","status":"completed"});
    let events = parse(vec![
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"rs_a","type":"reasoning","summary":[]}}),
        json!({"type":"response.reasoning_summary_text.delta","item_id":"rs_a","output_index":0,"summary_index":0,"delta":"Need lookup"}),
        json!({"type":"response.output_item.done","output_index":0,"item":reasoning}),
        json!({"type":"response.output_item.added","output_index":1,"item":{"id":"fc_a","type":"function_call","call_id":"call_a","name":"lookup","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","item_id":"fc_a","output_index":1,"delta":"{\"query\":"}),
        json!({"type":"response.function_call_arguments.done","item_id":"fc_a","output_index":1,"arguments":"{\"query\":\"whale\"}"}),
        json!({"type":"response.output_item.done","output_index":1,"item":call}),
        completed(vec![reasoning,call]),
    ], 3).await.into_iter().collect::<Result<Vec<_>,_>>().unwrap();
    assert!(events.iter().any(|e| matches!(e, AgentStreamEvent::ToolCallDelta{call_id,delta,..} if call_id=="call_a" && delta=="{\"query\":")));
    assert!(events
        .iter()
        .any(|e| matches!(e, AgentStreamEvent::ReasoningDelta{delta,..} if delta=="Need lookup")));
    let mut history = items(&events);
    assert_eq!(history.len(), 2);
    history.push(CanonicalItem::tool_result(
        "call_a",
        CanonicalToolOutput::text("found"),
        false,
    ));
    let (body, _) = adapter()
        .serialize_request(None, &history, &[], &SamplingOptions::new("model"))
        .unwrap();
    assert_eq!(body["input"][0]["id"], "rs_a");
    assert_eq!(body["input"][0]["encrypted_content"], "opaque-token");
    assert_eq!(body["input"][1]["call_id"], "call_a");
    assert_eq!(body["input"][1]["arguments"], "{\"query\":\"whale\"}");
    assert_eq!(body["input"][2]["output"], "found");
    assert_eq!(body["store"], false);
}

#[tokio::test]
async fn terminal_snapshot_alone_emits_items_in_output_order() {
    let events = parse(vec![completed(vec![message("complete")])], 1024)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(items(&events).len(), 1);
}

#[tokio::test]
async fn error_failed_and_incomplete_never_report_success() {
    for terminal in [
        json!({"type":"error","code":"server_error","message":"broken"}),
        json!({"type":"response.failed","response":{"status":"failed","error":{"code":"server_error","message":"broken"}}}),
        json!({"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}}),
    ] {
        let events = parse(vec![terminal], 5)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(
            matches!(events.last().unwrap(), AgentStreamEvent::TurnFailed { .. }),
            "{events:?}"
        );
        assert!(!events
            .iter()
            .any(|e| matches!(e, AgentStreamEvent::TurnCompleted { .. })));
    }
}

#[tokio::test]
async fn eof_before_terminal_and_truncated_json_are_errors() {
    for wire in [
        String::new(),
        "data: {\"type\":\"response.created\",\"response\":{}}\n\n".into(),
        "data: {\"type\":\"response.completed\"".into(),
        "data: [DONE]\n\n".into(),
    ] {
        let events = parse_wire(wire, 1).await;
        assert!(events.iter().any(Result::is_err), "{events:?}");
        assert!(!events
            .iter()
            .any(|e| matches!(e, Ok(AgentStreamEvent::TurnCompleted { .. }))));
    }
}

#[tokio::test]
async fn unsupported_output_item_is_explicit_error() {
    let events = parse(
        vec![completed(vec![
            json!({"id":"img_a","type":"image_generation_call","result":"base64"}),
        ])],
        50,
    )
    .await;
    assert!(events.iter().any(Result::is_err));
}

#[test]
fn requests_preserve_multimodal_and_function_arguments_and_phase() {
    let history = vec![
        CanonicalItem::UserMessage {
            id: "u".into(),
            content: vec![
                CanonicalContent::text("look"),
                CanonicalContent::image_uri("image/png", "https://example.com/image.png"),
            ],
        },
        CanonicalItem::assistant_text("checking", MessagePhase::Commentary),
        CanonicalItem::tool_call("call_a", None, "lookup", Some(json!({"q":1})), ""),
        CanonicalItem::tool_result(
            "call_a",
            CanonicalToolOutput::blocks(vec![
                CanonicalContent::text("chart"),
                CanonicalContent::image_base64("image/png", "abc"),
            ]),
            false,
        ),
    ];
    let mut options = SamplingOptions::new("model");
    options.temperature = Some(0.4);
    options.max_tokens = Some(99);
    let (body, _) = adapter()
        .serialize_request(Some("help"), &history, &[], &options)
        .unwrap();
    assert_eq!(body["input"][0]["content"].as_array().unwrap().len(), 2);
    assert_eq!(body["input"][0]["content"][1]["type"], "input_image");
    assert_eq!(body["input"][1]["phase"], "commentary");
    assert_eq!(body["input"][2]["arguments"], "{\"q\":1}");
    assert_eq!(
        body["input"][3]["output"][1]["image_url"],
        "data:image/png;base64,abc"
    );
    assert!(body["temperature"].as_f64().is_some());
    assert_eq!(body["max_output_tokens"], 99);
}

#[test]
fn unsupported_audio_or_cross_provider_signature_is_not_silently_dropped() {
    for item in [
        CanonicalItem::UserMessage {
            id: "u".into(),
            content: vec![CanonicalContent::Audio {
                mime_type: "audio/wav".into(),
                data: Some("abc".into()),
                uri: None,
            }],
        },
        CanonicalItem::reasoning("thinking", Some("anthropic-signature".into()), None),
        CanonicalItem::UserMessage {
            id: "u".into(),
            content: vec![CanonicalContent::Image {
                mime_type: "image/png".into(),
                data: None,
                uri: None,
            }],
        },
    ] {
        assert!(adapter()
            .serialize_request(None, &[item], &[], &SamplingOptions::new("model"))
            .is_err());
    }
}

#[test]
fn no_auth_omits_headers_but_existing_credentials_are_kept() {
    for (adapter, header, expected) in [
        (
            Box::new(adapter()) as Box<dyn ProtocolAdapter>,
            "authorization",
            None,
        ),
        (Box::new(OpenAIAdapter::new("")), "authorization", None),
        (Box::new(AnthropicAdapter::new("")), "x-api-key", None),
        (
            Box::new(OpenAIAdapter::new("secret")),
            "authorization",
            Some("Bearer secret"),
        ),
        (
            Box::new(AnthropicAdapter::new("secret")),
            "x-api-key",
            Some("secret"),
        ),
    ] {
        let (_, headers) = adapter
            .serialize_request(None, &[], &[], &SamplingOptions::new("model"))
            .unwrap();
        assert_eq!(headers.get(header).map(|v| v.to_str().unwrap()), expected);
    }
}

#[tokio::test]
async fn parallel_calls_keep_distinct_ids_and_complete_without_argument_deltas() {
    let call_a = json!({"id":"fc_a","type":"function_call","call_id":"call_a","name":"lookup","arguments":"{\"q\":1}"});
    let call_b = json!({"id":"fc_b","type":"function_call","call_id":"call_b","name":"lookup","arguments":"{\"q\":2}"});
    let events=parse(vec![
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"fc_a","type":"function_call","call_id":"call_a","name":"lookup","arguments":""}}),
        json!({"type":"response.output_item.added","output_index":1,"item":{"id":"fc_b","type":"function_call","call_id":"call_b","name":"lookup","arguments":""}}),
        json!({"type":"response.function_call_arguments.done","item_id":"fc_a","output_index":0,"arguments":"{\"q\":1}"}),
        json!({"type":"response.function_call_arguments.done","item_id":"fc_b","output_index":1,"arguments":"{\"q\":2}"}),
        completed(vec![call_a,call_b]),
    ],2).await.into_iter().collect::<Result<Vec<_>,_>>().unwrap();
    let result = items(&events);
    assert_eq!(result.len(), 2);
    assert!(
        matches!(&result[0],CanonicalItem::ToolCall{call_id,arguments:Some(args),..} if call_id=="call_a" && args["q"]==1)
    );
    assert!(
        matches!(&result[1],CanonicalItem::ToolCall{call_id,arguments:Some(args),..} if call_id=="call_b" && args["q"]==2)
    );
}

#[tokio::test]
async fn malformed_or_inconsistent_stream_cannot_finish_successfully() {
    let cases = vec![
        vec![
            json!({"type":"response.output_text.delta","item_id":"unknown","output_index":0,"content_index":0,"delta":"x"}),
            completed(vec![]),
        ],
        vec![
            json!({"type":"response.output_item.added","output_index":0,"item":message("")}),
            json!({"type":"response.output_text.delta","item_id":"different","output_index":0,"content_index":0,"delta":"x"}),
            completed(vec![message("x")]),
        ],
        vec![
            json!({"type":"response.output_item.done","output_index":0,"item":message("first")}),
            completed(vec![message("changed")]),
        ],
        vec![
            json!({"type":"response.output_item.added","output_index":0,"item":message("")}),
            completed(vec![]),
        ],
        vec![json!({"type":"response.completed","response":{"status":"incomplete","output":[]}})],
    ];
    for values in cases {
        let events = parse(values, 3).await;
        assert!(events.iter().any(Result::is_err), "{events:?}");
        assert!(!events
            .iter()
            .any(|e| matches!(e, Ok(AgentStreamEvent::TurnCompleted { .. }))));
    }
    assert!(parse_wire("data: broken-json\n\n".into(), 1)
        .await
        .iter()
        .any(Result::is_err));
}

#[test]
fn tool_failure_remains_model_visible_in_responses_history() {
    let history = [CanonicalItem::tool_result(
        "call_a",
        CanonicalToolOutput::text("permission denied"),
        true,
    )];
    let (body, _) = adapter()
        .serialize_request(None, &history, &[], &SamplingOptions::new("model"))
        .unwrap();
    let output: Value = serde_json::from_str(body["input"][0]["output"].as_str().unwrap()).unwrap();
    assert_eq!(output["is_error"], true);
    assert_eq!(output["output"], "permission denied");
}

#[tokio::test]
async fn function_namespace_survives_response_history_round_trip() {
    let call = json!({"id":"fc_a","type":"function_call","call_id":"call_a","name":"lookup","namespace":"catalog","arguments":"{}"});
    let events = parse(vec![completed(vec![call])], 5)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let history = items(&events);
    assert!(matches!(&history[0],CanonicalItem::ToolCall{namespace:Some(ns),..} if ns=="catalog"));
    let (body, _) = adapter()
        .serialize_request(None, &history, &[], &SamplingOptions::new("model"))
        .unwrap();
    assert_eq!(body["input"][0]["namespace"], "catalog");
}
