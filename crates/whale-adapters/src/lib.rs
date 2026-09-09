//! whale-adapters: Provider protocol adapters for Anthropic, OpenAI, and future LLM backends.
//!
//! Provides bidirectional conversion between canonical representations (`whale-protocol`)
//! and provider-specific wire schemas and SSE streams.

pub mod anthropic;
pub mod capabilities;
pub mod openai;
mod responses;
pub mod traits;

pub use anthropic::AnthropicAdapter;
pub use openai::{OpenAIAdapter, OpenAIWireApi};
pub use traits::{
    AdapterError, BoxedEventStream, ProtocolAdapter, SamplingOptions, ToolDefinition,
};

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::stream;
    use futures::StreamExt;
    use serde_json::json;
    use whale_protocol::canonical::{
        CanonicalContent, CanonicalItem, CanonicalToolOutput, MessagePhase,
    };
    use whale_protocol::events::AgentStreamEvent;

    #[test]
    fn test_anthropic_request_serialization() {
        let adapter = AnthropicAdapter::new("sk-ant-test-key");
        let history = vec![
            CanonicalItem::user_text("What is 2+2?"),
            CanonicalItem::reasoning(
                "User is asking for basic arithmetic.",
                Some("sig_123".into()),
                None,
            ),
            CanonicalItem::assistant_text("It is 4.", MessagePhase::FinalAnswer),
            CanonicalItem::tool_call(
                "call_calc_1",
                None,
                "calculator",
                Some(json!({"expr": "2+2"})),
                "{\"expr\":\"2+2\"}",
            ),
            CanonicalItem::tool_result("call_calc_1", CanonicalToolOutput::text("4"), false),
        ];

        let tools = vec![ToolDefinition::new(
            "calculator",
            "Evaluate arithmetic expression",
            json!({
                "type": "object",
                "properties": {
                    "expr": { "type": "string" }
                },
                "required": ["expr"]
            }),
        )];

        let mut options = SamplingOptions::new("claude-3-7-sonnet-20250219");
        options.temperature = Some(1.0);
        options.thinking_budget = Some(2048);
        options.prompt_caching = true;

        let (body, headers) = adapter
            .serialize_request(
                Some("You are an expert mathematician."),
                &history,
                &tools,
                &options,
            )
            .expect("serialization succeeded");

        assert_eq!(headers.get("x-api-key").unwrap(), "sk-ant-test-key");
        assert_eq!(headers.get("anthropic-version").unwrap(), "2023-06-01");
        assert!(headers
            .get("anthropic-beta")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("prompt-caching"));

        // System prompt check with cache_control
        let sys = body.get("system").unwrap().as_array().unwrap();
        assert_eq!(
            sys[0].get("text").unwrap(),
            "You are an expert mathematician."
        );
        assert_eq!(
            sys[0].get("cache_control").unwrap().get("type").unwrap(),
            "ephemeral"
        );

        // Thinking configuration check
        let thinking = body.get("thinking").unwrap();
        assert_eq!(thinking.get("type").unwrap(), "enabled");
        assert_eq!(thinking.get("budget_tokens").unwrap(), 2048);

        // Tools check
        let body_tools = body.get("tools").unwrap().as_array().unwrap();
        assert_eq!(body_tools.len(), 1);
        assert_eq!(body_tools[0].get("name").unwrap(), "calculator");
        assert_eq!(
            body_tools[0]
                .get("input_schema")
                .unwrap()
                .get("type")
                .unwrap(),
            "object"
        );

        // Messages check
        let messages = body.get("messages").unwrap().as_array().unwrap();
        // user ("What is 2+2?"), assistant (thinking + text + tool_use), user (tool_result) -> merged into alternating roles
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].get("role").unwrap(), "user");
        assert_eq!(messages[1].get("role").unwrap(), "assistant");
        assert_eq!(messages[2].get("role").unwrap(), "user");

        // The tool_result should have cache_control attached because prompt_caching is true
        let last_user_content = messages[2].get("content").unwrap().as_array().unwrap();
        assert_eq!(last_user_content[0].get("type").unwrap(), "tool_result");
        assert_eq!(
            last_user_content[0]
                .get("cache_control")
                .unwrap()
                .get("type")
                .unwrap(),
            "ephemeral"
        );
    }

    #[tokio::test]
    async fn test_anthropic_sse_stream_parsing() {
        let adapter = AnthropicAdapter::new("test-key");

        let sse_data = [
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-3-7-sonnet-20250219\",\"usage\":{\"input_tokens\":50,\"cache_creation_input_tokens\":10,\"cache_read_input_tokens\":20}}}\n\n",
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"Let me \"}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"think.\"}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"sig_xyz\"}}\n\n",
            "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello world\"}}\n\n",
            "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":25}}\n\n",
            "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        ];

        let chunks: Vec<Result<Bytes, reqwest::Error>> =
            sse_data.into_iter().map(|s| Ok(Bytes::from(s))).collect();

        let byte_stream = Box::pin(stream::iter(chunks));
        let mut event_stream = adapter.parse_stream(byte_stream);

        let mut events = Vec::new();
        while let Some(ev) = event_stream.next().await {
            events.push(ev.expect("event should parse"));
        }

        assert!(matches!(events[0], AgentStreamEvent::TurnStarted { .. }));
        assert!(matches!(
            events[1],
            AgentStreamEvent::ItemStarted {
                ref item_type,
                phase: Some(MessagePhase::Commentary),
                ..
            } if item_type == "reasoning"
        ));
        assert!(
            matches!(events[2], AgentStreamEvent::ReasoningDelta { ref delta, .. } if delta == "Let me ")
        );
        assert!(
            matches!(events[3], AgentStreamEvent::ReasoningDelta { ref delta, .. } if delta == "think.")
        );
        assert!(
            matches!(events[4], AgentStreamEvent::ReasoningSignature { ref signature, .. } if signature == "sig_xyz")
        );
        assert!(matches!(
            events[5],
            AgentStreamEvent::ItemCompleted {
                item: CanonicalItem::Reasoning { ref thinking, ref signature, .. },
                ..
            } if thinking == "Let me think." && signature.as_deref() == Some("sig_xyz")
        ));
        assert!(matches!(
            events[6],
            AgentStreamEvent::ItemStarted {
                ref item_type,
                phase: Some(MessagePhase::FinalAnswer),
                ..
            } if item_type == "assistant_message"
        ));
        assert!(
            matches!(events[7], AgentStreamEvent::TextDelta { ref delta, .. } if delta == "Hello world")
        );
        assert!(matches!(
            events[8],
            AgentStreamEvent::ItemCompleted {
                item: CanonicalItem::AssistantMessage { ref content, .. },
                ..
            } if matches!(&content[0], CanonicalContent::Text { text } if text == "Hello world")
        ));
        assert!(matches!(
            events[9],
            AgentStreamEvent::TurnCompleted { ref usage, .. }
            if usage.input_tokens == 50
                && usage.output_tokens == 25
                && usage.cache_creation_input_tokens == 10
                && usage.cache_read_input_tokens == 20
        ));
    }

    #[test]
    fn test_openai_chat_completions_serialization() {
        let adapter = OpenAIAdapter::new("sk-openai-key");
        let history = vec![
            CanonicalItem::user_text("Search for whale species"),
            CanonicalItem::tool_call(
                "call_search_1",
                None,
                "web_search",
                Some(json!({"q": "whale species"})),
                "{\"q\":\"whale species\"}",
            ),
            CanonicalItem::tool_result(
                "call_search_1",
                CanonicalToolOutput::text("Blue whale, Humpback whale, Orca"),
                false,
            ),
        ];

        let tools = vec![ToolDefinition::new(
            "web_search",
            "Search web",
            json!({
                "type": "object",
                "properties": { "q": { "type": "string" } }
            }),
        )];

        let mut options = SamplingOptions::new("o3-mini");
        options.reasoning_effort = Some("high".to_string());
        options.max_tokens = Some(2048);

        let (body, headers) = adapter
            .serialize_request(
                Some("You are a marine biologist."),
                &history,
                &tools,
                &options,
            )
            .expect("serialize success");

        assert_eq!(
            headers.get("Authorization").unwrap(),
            "Bearer sk-openai-key"
        );

        assert_eq!(body.get("model").unwrap(), "o3-mini");
        assert_eq!(body.get("reasoning_effort").unwrap(), "high");
        assert_eq!(body.get("max_completion_tokens").unwrap(), 2048);
        assert_eq!(
            body.get("stream_options")
                .unwrap()
                .get("include_usage")
                .unwrap(),
            true
        );

        let msgs = body.get("messages").unwrap().as_array().unwrap();
        // system/developer + user + assistant(tool_call) + tool(result)
        assert_eq!(msgs.len(), 4);
        assert_eq!(msgs[0].get("role").unwrap(), "system");
        assert_eq!(msgs[1].get("role").unwrap(), "user");
        assert_eq!(msgs[2].get("role").unwrap(), "assistant");
        assert_eq!(msgs[3].get("role").unwrap(), "tool");

        let tools_arr = body.get("tools").unwrap().as_array().unwrap();
        assert_eq!(tools_arr[0].get("type").unwrap(), "function");
        assert_eq!(
            tools_arr[0].get("function").unwrap().get("name").unwrap(),
            "web_search"
        );
    }

    #[test]
    fn test_openai_responses_api_serialization() {
        let adapter = OpenAIAdapter::with_options(
            "sk-openai-key",
            "https://api.openai.com/v1",
            OpenAIWireApi::Responses,
        );

        let history = vec![
            CanonicalItem::user_text("Run diagnostics"),
            CanonicalItem::tool_call("call_diag_1", None, "run_diag", None, "{}"),
            CanonicalItem::tool_result("call_diag_1", CanonicalToolOutput::text("All ok"), false),
        ];

        let tools = vec![ToolDefinition::new(
            "run_diag",
            "Run diagnostic check",
            json!({}),
        )];
        let options = SamplingOptions::new("gpt-4o");

        let (body, _headers) = adapter
            .serialize_request(Some("System instructions"), &history, &tools, &options)
            .expect("serialize success");

        assert_eq!(body.get("instructions").unwrap(), "System instructions");
        let inputs = body.get("input").unwrap().as_array().unwrap();
        assert_eq!(inputs.len(), 3);
        assert_eq!(inputs[0].get("role").unwrap(), "user");
        assert_eq!(inputs[1].get("type").unwrap(), "function_call");
        assert_eq!(inputs[2].get("type").unwrap(), "function_call_output");

        let tools_arr = body.get("tools").unwrap().as_array().unwrap();
        assert_eq!(tools_arr[0].get("type").unwrap(), "function");
        assert_eq!(tools_arr[0].get("name").unwrap(), "run_diag");
    }

    #[tokio::test]
    async fn test_openai_sse_stream_parsing() {
        let adapter = OpenAIAdapter::new("test-key");

        let sse_data = [
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"reasoning_content\":\"Solving \"},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"puzzle.\"},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Answer \"},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"is 42.\"},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":15,\"completion_tokens\":10,\"completion_tokens_details\":{\"reasoning_tokens\":4}}}\n\n",
            "data: [DONE]\n\n",
        ];

        let chunks: Vec<Result<Bytes, reqwest::Error>> =
            sse_data.into_iter().map(|s| Ok(Bytes::from(s))).collect();

        let byte_stream = Box::pin(stream::iter(chunks));
        let mut event_stream = adapter.parse_stream(byte_stream);

        let mut events = Vec::new();
        while let Some(ev) = event_stream.next().await {
            events.push(ev.expect("event should parse"));
        }

        assert!(matches!(events[0], AgentStreamEvent::TurnStarted { .. }));
        assert!(matches!(
            events[1],
            AgentStreamEvent::ItemStarted {
                ref item_type,
                phase: Some(MessagePhase::Commentary),
                ..
            } if item_type == "reasoning"
        ));
        assert!(
            matches!(events[2], AgentStreamEvent::ReasoningDelta { ref delta, .. } if delta == "Solving ")
        );
        assert!(
            matches!(events[3], AgentStreamEvent::ReasoningDelta { ref delta, .. } if delta == "puzzle.")
        );
        assert!(matches!(
            events[4],
            AgentStreamEvent::ItemCompleted {
                item: CanonicalItem::Reasoning { ref thinking, .. },
                ..
            } if thinking == "Solving puzzle."
        ));
        assert!(matches!(
            events[5],
            AgentStreamEvent::ItemStarted {
                ref item_type,
                phase: Some(MessagePhase::FinalAnswer),
                ..
            } if item_type == "assistant_message"
        ));
        assert!(
            matches!(events[6], AgentStreamEvent::TextDelta { ref delta, .. } if delta == "Answer ")
        );
        assert!(
            matches!(events[7], AgentStreamEvent::TextDelta { ref delta, .. } if delta == "is 42.")
        );
        assert!(matches!(
            events[8],
            AgentStreamEvent::ItemCompleted {
                item: CanonicalItem::AssistantMessage { ref content, .. },
                ..
            } if matches!(&content[0], CanonicalContent::Text { text } if text == "Answer is 42.")
        ));
        assert!(matches!(
            events[9],
            AgentStreamEvent::TurnCompleted { ref usage, .. }
            if usage.input_tokens == 15 && usage.output_tokens == 10 && usage.reasoning_tokens == 4
        ));
    }

    #[tokio::test]
    async fn test_openai_tool_call_sse_parsing() {
        let adapter = OpenAIAdapter::new("test-key");

        let sse_data = [
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call_abc\",\"function\":{\"name\":\"calculator\",\"arguments\":\"\"}}]},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"a\\\": 1\"}}]},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\", \\\"b\\\": 2}\"}}]},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":20,\"completion_tokens\":15}}\n\n",
            "data: [DONE]\n\n",
        ];

        let chunks: Vec<Result<Bytes, reqwest::Error>> =
            sse_data.into_iter().map(|s| Ok(Bytes::from(s))).collect();

        let byte_stream = Box::pin(stream::iter(chunks));
        let mut event_stream = adapter.parse_stream(byte_stream);

        let mut events = Vec::new();
        while let Some(ev) = event_stream.next().await {
            events.push(ev.expect("event should parse"));
        }

        assert!(matches!(events[0], AgentStreamEvent::TurnStarted { .. }));
        assert!(matches!(
            events[1],
            AgentStreamEvent::ToolCallDelta { ref call_id, ref delta, .. } if call_id == "call_abc" && delta == "{\"a\": 1"
        ));
        assert!(matches!(
            events[2],
            AgentStreamEvent::ToolCallDelta { ref call_id, ref delta, .. } if call_id == "call_abc" && delta == ", \"b\": 2}"
        ));
        assert!(matches!(
            events[3],
            AgentStreamEvent::ItemCompleted {
                item: CanonicalItem::ToolCall {
                    ref call_id,
                    ref name,
                    ref arguments,
                    ref raw_arguments,
                    ..
                },
                ..
            } if call_id == "call_abc"
                && name == "calculator"
                && raw_arguments == "{\"a\": 1, \"b\": 2}"
                && arguments.is_some()
        ));
        assert!(matches!(
            events[4],
            AgentStreamEvent::TurnCompleted { ref usage, .. }
            if usage.input_tokens == 20 && usage.output_tokens == 15
        ));
    }
}
