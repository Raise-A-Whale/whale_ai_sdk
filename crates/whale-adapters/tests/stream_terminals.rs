use bytes::Bytes;
use futures::{stream, StreamExt};
use whale_adapters::{AdapterError, AnthropicAdapter, OpenAIAdapter, ProtocolAdapter};
use whale_protocol::events::AgentStreamEvent;

async fn parse(
    adapter: &dyn ProtocolAdapter,
    wire: &str,
) -> Vec<Result<AgentStreamEvent, AdapterError>> {
    let chunks: Vec<Result<Bytes, reqwest::Error>> = wire
        .as_bytes()
        .chunks(1)
        .map(|v| Ok(Bytes::copy_from_slice(v)))
        .collect();
    adapter
        .parse_stream(Box::pin(stream::iter(chunks)))
        .collect()
        .await
}
fn failed(events: &[Result<AgentStreamEvent, AdapterError>]) {
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Err(_) | Ok(AgentStreamEvent::TurnFailed { .. }))),
        "{events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Ok(AgentStreamEvent::TurnCompleted { .. }))),
        "{events:?}"
    );
}

#[tokio::test]
async fn chat_requires_explicit_finish_reason_but_not_done_sentinel() {
    let adapter = OpenAIAdapter::new("");
    for wire in [
        "",
        "data: [DONE]\n\n",
        "data: {\"choices\":[{\"delta\":{\"content\":\"unfinished\"},\"finish_reason\":null}]}\n\n",
        "data: {\"choices\":",
    ] {
        failed(&parse(&adapter, wire).await);
    }
    let wire =
        "data: {\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\n";
    let events = parse(&adapter, wire).await;
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Ok(AgentStreamEvent::TurnCompleted { .. })))
            .count(),
        1
    );
    assert!(events.iter().all(Result::is_ok));
}

#[tokio::test]
async fn chat_incomplete_finish_reasons_and_provider_errors_are_failures() {
    let adapter = OpenAIAdapter::new("");
    for reason in ["length", "content_filter", "unknown_reason"] {
        let wire=format!("data: {{\"choices\":[{{\"delta\":{{\"content\":\"partial\"}},\"finish_reason\":\"{reason}\"}}]}}\n\ndata: [DONE]\n\n");
        failed(&parse(&adapter, &wire).await);
    }
    failed(
        &parse(
            &adapter,
            "data: {\"error\":{\"code\":\"server_error\",\"message\":\"broken\"}}\n\n",
        )
        .await,
    );
}

#[tokio::test]
async fn chat_trailing_error_after_finish_is_not_hidden() {
    let wire="data: {\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"error\":{\"code\":\"server_error\",\"message\":\"broken\"}}\n\n";
    failed(&parse(&OpenAIAdapter::new(""), wire).await);
}

#[tokio::test]
async fn anthropic_requires_message_stop_and_errors_terminate_immediately() {
    let adapter = AnthropicAdapter::new("");
    for wire in ["", "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"usage\":{}}}\n\n", "event: message_stop\ndata: {\"type\":", "data: [DONE]\n\n"] {
        failed(&parse(&adapter,wire).await);
    }
    failed(&parse(&adapter,"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"broken\"}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n").await);
    let events=parse(&adapter,"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n").await;
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Ok(AgentStreamEvent::TurnCompleted { .. })))
            .count(),
        1
    );
}

#[tokio::test]
async fn anthropic_max_tokens_cannot_report_completed() {
    let wire="event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"max_tokens\"},\"usage\":{\"output_tokens\":4}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
    failed(&parse(&AnthropicAdapter::new(""), wire).await);
}

#[tokio::test]
async fn chat_cannot_accept_further_content_after_finish() {
    let wire="data: {\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"unfinished second message\"},\"finish_reason\":null}]}\n\n";
    failed(&parse(&OpenAIAdapter::new(""), wire).await);
}

#[tokio::test]
async fn anthropic_message_stop_cannot_complete_an_unclosed_content_block() {
    let wire="data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\ndata: {\"type\":\"message_stop\"}\n\n";
    failed(&parse(&AnthropicAdapter::new(""), wire).await);
}
