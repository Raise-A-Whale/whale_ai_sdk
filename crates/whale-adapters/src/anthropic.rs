//! Anthropic Messages API protocol adapter.
//!
//! Handles serialization of canonical conversations to Anthropic Messages API schema,
//! prompt caching markers, extended thinking / reasoning config, and parsing SSE streams.

use async_stream::try_stream;
use eventsource_stream::Eventsource;
use futures::{Stream, StreamExt};
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::json;
use std::pin::Pin;
use uuid::Uuid;
use whale_protocol::canonical::{
    new_item_id, CanonicalContent, CanonicalItem, CanonicalToolOutput, MessagePhase,
};
use whale_protocol::events::{AgentStreamEvent, UsageMetrics};

use crate::traits::{
    AdapterError, BoxedEventStream, ProtocolAdapter, SamplingOptions, ToolDefinition,
};

/// Adapter for Anthropic Claude models using the Messages API.
#[derive(Clone)]
pub struct AnthropicAdapter {
    api_key: String,
    base_url: String,
}

impl std::fmt::Debug for AnthropicAdapter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AnthropicAdapter")
            .field("api_key", &"[REDACTED]")
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl AnthropicAdapter {
    /// Creates a new AnthropicAdapter with the specified API key and default base URL
    /// (<https://api.anthropic.com/v1>).
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: "https://api.anthropic.com/v1".to_string(),
        }
    }

    /// Creates an AnthropicAdapter with custom base URL.
    pub fn with_base_url(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: base_url.into(),
        }
    }

    /// Returns the API key.
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Returns the base URL.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }
}

impl ProtocolAdapter for AnthropicAdapter {
    fn capabilities(&self) -> whale_protocol::models::ModelCapabilities {
        let api = "anthropic";
        crate::capabilities::http_capabilities(api)
    }

    fn provider_name(&self) -> &'static str {
        "anthropic"
    }

    fn endpoint_url(&self) -> String {
        format!("{}/messages", self.base_url.trim_end_matches('/'))
    }

    fn serialize_request(
        &self,
        system_prompt: Option<&str>,
        history: &[CanonicalItem],
        tools: &[ToolDefinition],
        options: &SamplingOptions,
    ) -> Result<(serde_json::Value, HeaderMap), AdapterError> {
        let capabilities = self.capabilities();
        crate::capabilities::validate_configuration(&options.model, tools, options, &capabilities)?;
        crate::capabilities::validate_items(history, &capabilities)?;
        let mut headers = HeaderMap::new();
        if !self.api_key.is_empty() {
            let mut api_key = HeaderValue::from_str(&self.api_key).map_err(|e| {
                AdapterError::ProtocolError(format!("Invalid API key header: {}", e))
            })?;
            api_key.set_sensitive(true);
            headers.insert("x-api-key", api_key);
        }
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        headers.insert("content-type", HeaderValue::from_static("application/json"));

        // Required beta header for prompt caching and output extension
        headers.insert(
            "anthropic-beta",
            HeaderValue::from_static("prompt-caching-2024-07-31,output-128k-2025-02-19"),
        );

        let mut body = serde_json::Map::new();
        body.insert("model".to_string(), json!(options.model));
        body.insert(
            "max_tokens".to_string(),
            json!(options.max_tokens.unwrap_or(4096)),
        );
        body.insert("stream".to_string(), json!(true));

        if let Some(temp) = options.temperature {
            // Anthropic extended thinking requires temperature = 1.0 or omitted
            if options.thinking_budget.is_some() && temp != 1.0 {
                return Err(AdapterError::ProtocolError(
                    "Anthropic thinking requires temperature=1 or no explicit temperature".into(),
                ));
            }
            body.insert("temperature".to_string(), json!(temp));
        }

        // Handle Extended Thinking
        if let Some(budget) = options.thinking_budget {
            body.insert(
                "thinking".to_string(),
                json!({
                    "type": "enabled",
                    "budget_tokens": budget
                }),
            );
        }

        // Handle system prompt with optional caching
        if let Some(sys) = system_prompt {
            if options.prompt_caching {
                body.insert(
                    "system".to_string(),
                    json!([
                        {
                            "type": "text",
                            "text": sys,
                            "cache_control": { "type": "ephemeral" }
                        }
                    ]),
                );
            } else {
                body.insert(
                    "system".to_string(),
                    json!([
                        {
                            "type": "text",
                            "text": sys
                        }
                    ]),
                );
            }
        }

        // Handle tools
        if !tools.is_empty() {
            let anthropic_tools: Vec<serde_json::Value> = tools
                .iter()
                .map(|t| {
                    json!({
                        "name": t.name,
                        "description": t.description,
                        "input_schema": t.parameters
                    })
                })
                .collect();
            body.insert("tools".to_string(), json!(anthropic_tools));
        }

        // Group canonical history into Anthropic messages format:
        // Anthropic requires alternating user/assistant messages, and consecutive blocks of the same role
        // must be combined into a single message with multiple content blocks.
        // User tool results also go to role "user" with type "tool_result".
        let mut messages: Vec<serde_json::Value> = Vec::new();

        for item in history {
            match item {
                CanonicalItem::UserMessage { content, .. } => {
                    let mut blocks = Vec::new();
                    for c in content {
                        match c {
                            CanonicalContent::Text { text } => {
                                blocks.push(json!({
                                    "type": "text",
                                    "text": text
                                }));
                            }
                            CanonicalContent::Image {
                                mime_type,
                                data,
                                uri,
                            } => {
                                if let Some(base64_data) = data {
                                    blocks.push(json!({
                                        "type": "image",
                                        "source": {
                                            "type": "base64",
                                            "media_type": mime_type,
                                            "data": base64_data
                                        }
                                    }));
                                } else if let Some(u) = uri {
                                    blocks.push(json!({
                                        "type": "image",
                                        "source": {
                                            "type": "url",
                                            "url": u
                                        }
                                    }));
                                }
                            }
                            CanonicalContent::Audio { .. } => {
                                // Anthropic currently doesn't accept audio directly in messages
                            }
                        }
                    }

                    if !blocks.is_empty() {
                        push_or_merge_message(&mut messages, "user", blocks);
                    }
                }
                CanonicalItem::Reasoning {
                    thinking,
                    signature,
                    ..
                } => {
                    let mut block = json!({
                        "type": "thinking",
                        "thinking": thinking
                    });
                    if let Some(sig) = signature {
                        block["signature"] = json!(sig);
                    }
                    push_or_merge_message(&mut messages, "assistant", vec![block]);
                }
                CanonicalItem::AssistantMessage { content, .. } => {
                    let mut blocks = Vec::new();
                    for c in content {
                        if let CanonicalContent::Text { text } = c {
                            blocks.push(json!({
                                "type": "text",
                                "text": text
                            }));
                        }
                    }
                    if !blocks.is_empty() {
                        push_or_merge_message(&mut messages, "assistant", blocks);
                    }
                }
                CanonicalItem::ToolCall {
                    call_id,
                    name,
                    arguments,
                    raw_arguments,
                    ..
                } => {
                    let input_val = if let Some(args) = arguments {
                        args.clone()
                    } else if let Ok(parsed) =
                        serde_json::from_str::<serde_json::Value>(raw_arguments)
                    {
                        parsed
                    } else {
                        json!({})
                    };

                    let block = json!({
                        "type": "tool_use",
                        "id": call_id,
                        "name": name,
                        "input": input_val
                    });
                    push_or_merge_message(&mut messages, "assistant", vec![block]);
                }
                CanonicalItem::ToolResult {
                    call_id,
                    output,
                    is_error,
                    ..
                } => {
                    let content_val = match output {
                        CanonicalToolOutput::Text { text } => json!(text),
                        CanonicalToolOutput::Structured { data } => {
                            json!(serde_json::to_string(data).unwrap_or_else(|_| "{}".to_string()))
                        }
                        CanonicalToolOutput::Blocks { blocks } => {
                            let mut b_list = Vec::new();
                            for b in blocks {
                                match b {
                                    CanonicalContent::Text { text } => {
                                        b_list.push(json!({
                                            "type": "text",
                                            "text": text
                                        }));
                                    }
                                    CanonicalContent::Image {
                                        mime_type,
                                        data,
                                        uri,
                                    } => {
                                        if let Some(b64) = data {
                                            b_list.push(json!({
                                                "type": "image",
                                                "source": {
                                                    "type": "base64",
                                                    "media_type": mime_type,
                                                    "data": b64
                                                }
                                            }));
                                        } else if let Some(uri) = uri {
                                            b_list.push(json!({"type":"image","source":{"type":"url","url":uri}}));
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            json!(b_list)
                        }
                    };

                    let mut block = json!({
                        "type": "tool_result",
                        "tool_use_id": call_id,
                        "content": content_val
                    });
                    if *is_error {
                        block["is_error"] = json!(true);
                    }

                    // Anthropic requires tool_result to be placed in a user message
                    push_or_merge_message(&mut messages, "user", vec![block]);
                }
            }
        }

        // If prompt caching is enabled and there are messages, attach cache_control to the last block of the last message
        if options.prompt_caching {
            if let Some(last_msg) = messages.last_mut() {
                if let Some(content_array) =
                    last_msg.get_mut("content").and_then(|c| c.as_array_mut())
                {
                    if let Some(last_block) = content_array.last_mut() {
                        if let Some(obj) = last_block.as_object_mut() {
                            obj.insert("cache_control".to_string(), json!({ "type": "ephemeral" }));
                        }
                    }
                }
            }
        }

        body.insert("messages".to_string(), json!(messages));

        Ok((serde_json::Value::Object(body), headers))
    }

    fn parse_stream(
        &self,
        byte_stream: Pin<Box<dyn Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send>>,
    ) -> BoxedEventStream {
        let stream = try_stream! {
            let mut event_stream = byte_stream.eventsource();
            let turn_id = Uuid::new_v4().to_string();
            let thread_id = Uuid::new_v4().to_string();

            // Track state during the stream
            let mut current_item_id = new_item_id();
            let mut current_block_type = String::new();
            let mut block_open = false;
            let mut current_text_buf = String::new();
            let mut current_thinking_buf = String::new();
            let mut current_signature_buf = String::new();
            let mut current_tool_id = String::new();
            let mut current_tool_name = String::new();
            let mut current_tool_arg_buf = String::new();
            let mut current_usage = UsageMetrics::default();

            yield AgentStreamEvent::TurnStarted {
                turn_id: turn_id.clone(),
                thread_id: thread_id.clone(),
            };

            while let Some(event_result) = event_stream.next().await {
                let sse = event_result.map_err(|e| AdapterError::StreamParseError(e.to_string()))?;
                let data = sse.data.trim();
                if data.is_empty() || data == "[DONE]" {
                    continue;
                }

                let parsed: serde_json::Value = serde_json::from_str(data)
                    .map_err(|e| AdapterError::StreamParseError(format!("Failed to parse JSON: {} in '{}'", e, data)))?;

                let event_type = parsed.get("type").and_then(|t| t.as_str()).unwrap_or("");

                match event_type {
                    "message_start" => {
                        if let Some(message) = parsed.get("message") {
                            if let Some(usage) = message.get("usage") {
                                if let Some(it) = usage.get("input_tokens").and_then(|v| v.as_u64()) {
                                    current_usage.input_tokens = it;
                                }
                                if let Some(cr) = usage.get("cache_creation_input_tokens").and_then(|v| v.as_u64()) {
                                    current_usage.cache_creation_input_tokens = cr;
                                }
                                if let Some(rd) = usage.get("cache_read_input_tokens").and_then(|v| v.as_u64()) {
                                    current_usage.cache_read_input_tokens = rd;
                                }
                            }
                        }
                    }
                    "content_block_start" => {
                        if block_open {
                            Err(AdapterError::ProtocolError("Anthropic started a block before closing the previous block".into()))?;
                        }
                        block_open = true;
                        current_item_id = new_item_id();
                        current_text_buf.clear();
                        current_thinking_buf.clear();
                        current_signature_buf.clear();
                        current_tool_id.clear();
                        current_tool_name.clear();
                        current_tool_arg_buf.clear();

                        if let Some(block) = parsed.get("content_block") {
                            let b_type = block.get("type").and_then(|t| t.as_str()).unwrap_or("");
                            current_block_type = b_type.to_string();

                            match b_type {
                                "text" => {
                                    yield AgentStreamEvent::ItemStarted {
                                        turn_id: turn_id.clone(),
                                        item_id: current_item_id.clone(),
                                        item_type: "assistant_message".to_string(),
                                        phase: Some(MessagePhase::FinalAnswer),
                                    };
                                }
                                "thinking" => {
                                    yield AgentStreamEvent::ItemStarted {
                                        turn_id: turn_id.clone(),
                                        item_id: current_item_id.clone(),
                                        item_type: "reasoning".to_string(),
                                        phase: Some(MessagePhase::Commentary),
                                    };
                                }
                                "tool_use" => {
                                    if let Some(id) = block.get("id").and_then(|v| v.as_str()) {
                                        current_tool_id = id.to_string();
                                    }
                                    if let Some(name) = block.get("name").and_then(|v| v.as_str()) {
                                        current_tool_name = name.to_string();
                                    }

                                    yield AgentStreamEvent::ItemStarted {
                                        turn_id: turn_id.clone(),
                                        item_id: current_item_id.clone(),
                                        item_type: "tool_call".to_string(),
                                        phase: None,
                                    };
                                }
                                _ => {}
                            }
                        }
                    }
                    "content_block_delta" => {
                        if let Some(delta) = parsed.get("delta") {
                            let delta_type = delta.get("type").and_then(|t| t.as_str()).unwrap_or("");
                            match delta_type {
                                "text_delta" => {
                                    if let Some(text) = delta.get("text").and_then(|t| t.as_str()) {
                                        current_text_buf.push_str(text);
                                        yield AgentStreamEvent::TextDelta {
                                            turn_id: turn_id.clone(),
                                            item_id: current_item_id.clone(),
                                            delta: text.to_string(),
                                        };
                                    }
                                }
                                "thinking_delta" => {
                                    if let Some(thinking) = delta.get("thinking").and_then(|t| t.as_str()) {
                                        current_thinking_buf.push_str(thinking);
                                        yield AgentStreamEvent::ReasoningDelta {
                                            turn_id: turn_id.clone(),
                                            item_id: current_item_id.clone(),
                                            delta: thinking.to_string(),
                                        };
                                    }
                                }
                                "signature_delta" => {
                                    if let Some(sig) = delta.get("signature").and_then(|s| s.as_str()) {
                                        current_signature_buf.push_str(sig);
                                        yield AgentStreamEvent::ReasoningSignature {
                                            turn_id: turn_id.clone(),
                                            item_id: current_item_id.clone(),
                                            signature: sig.to_string(),
                                        };
                                    }
                                }
                                "input_json_delta" => {
                                    if let Some(partial_json) = delta.get("partial_json").and_then(|j| j.as_str()) {
                                        current_tool_arg_buf.push_str(partial_json);
                                        yield AgentStreamEvent::ToolCallDelta {
                                            turn_id: turn_id.clone(),
                                            item_id: current_item_id.clone(),
                                            call_id: current_tool_id.clone(),
                                            delta: partial_json.to_string(),
                                        };
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    "content_block_stop" => {
                        block_open = false;
                        match current_block_type.as_str() {
                            "text" => {
                                let item = CanonicalItem::AssistantMessage {
                                    id: current_item_id.clone(),
                                    content: vec![CanonicalContent::text(&current_text_buf)],
                                    phase: MessagePhase::FinalAnswer,
                                };
                                yield AgentStreamEvent::ItemCompleted {
                                    turn_id: turn_id.clone(),
                                    item,
                                };
                            }
                            "thinking" => {
                                let sig = if current_signature_buf.is_empty() {
                                    None
                                } else {
                                    Some(current_signature_buf.clone())
                                };
                                let item = CanonicalItem::Reasoning {
                                    id: current_item_id.clone(),
                                    thinking: current_thinking_buf.clone(),
                                    signature: sig,
                                    encrypted_content: None,
                                };
                                yield AgentStreamEvent::ItemCompleted {
                                    turn_id: turn_id.clone(),
                                    item,
                                };
                            }
                            "tool_use" => {
                                let parsed_args: Option<serde_json::Value> =
                                    serde_json::from_str(&current_tool_arg_buf).ok();
                                let item = CanonicalItem::ToolCall {
                                    id: current_item_id.clone(),
                                    call_id: current_tool_id.clone(),
                                    namespace: None,
                                    name: current_tool_name.clone(),
                                    arguments: parsed_args,
                                    raw_arguments: current_tool_arg_buf.clone(),
                                };
                                yield AgentStreamEvent::ItemCompleted {
                                    turn_id: turn_id.clone(),
                                    item,
                                };
                            }
                            _ => {}
                        }
                    }
                    "message_delta" => {
                        if let Some(reason) = parsed["delta"]["stop_reason"].as_str() {
                            if !matches!(reason, "end_turn" | "tool_use" | "stop_sequence" | "refusal") {
                                yield AgentStreamEvent::TurnFailed {
                                    turn_id: turn_id.clone(),
                                    thread_id: thread_id.clone(),
                                    error_code: "incomplete_completion".into(),
                                    error_message: format!("Anthropic generation stopped without a complete supported response: {reason}"),
                                };
                                return;
                            }
                        }
                        if let Some(usage) = parsed.get("usage") {
                            if let Some(ot) = usage.get("output_tokens").and_then(|v| v.as_u64()) {
                                current_usage.output_tokens = ot;
                            }
                        }
                    }
                    "message_stop" => {
                        if block_open {
                            Err(AdapterError::ProtocolError("Anthropic message_stop arrived before content_block_stop".into()))?;
                        }
                        yield AgentStreamEvent::TurnCompleted {
                            turn_id: turn_id.clone(),
                            thread_id: thread_id.clone(),
                            usage: current_usage.clone(),
                        };
                        return;
                    }
                    "error" => {
                        let err_obj = parsed.get("error");
                        let err_type = err_obj.and_then(|e| e.get("type")).and_then(|t| t.as_str()).unwrap_or("API_ERROR");
                        let err_msg = err_obj.and_then(|e| e.get("message")).and_then(|m| m.as_str()).unwrap_or("Unknown Anthropic API error");
                        yield AgentStreamEvent::TurnFailed {
                            turn_id: turn_id.clone(),
                            thread_id: thread_id.clone(),
                            error_code: err_type.to_string(),
                            error_message: err_msg.to_string(),
                        };
                        return;
                    }
                    _ => {}
                }
            }
            Err(AdapterError::StreamParseError(
                "Unexpected EOF before Anthropic message_stop".into(),
            ))?;
        };

        Box::pin(stream)
    }
}

/// Helper function to append or merge blocks into the last message if same role,
/// otherwise create a new message with the given role and blocks.
fn push_or_merge_message(
    messages: &mut Vec<serde_json::Value>,
    role: &'static str,
    mut blocks: Vec<serde_json::Value>,
) {
    if let Some(last_msg) = messages.last_mut() {
        if last_msg.get("role").and_then(|r| r.as_str()) == Some(role) {
            if let Some(content_arr) = last_msg.get_mut("content").and_then(|c| c.as_array_mut()) {
                content_arr.append(&mut blocks);
                return;
            }
        }
    }

    messages.push(json!({
        "role": role,
        "content": blocks
    }));
}
