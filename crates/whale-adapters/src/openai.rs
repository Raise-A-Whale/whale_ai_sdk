//! OpenAI protocol adapter supporting both Chat Completions API and the newer Responses API.

use async_stream::try_stream;
use eventsource_stream::Eventsource;
use futures::{Stream, StreamExt};
use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
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

/// Wire format / endpoint protocol for OpenAI.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpenAIWireApi {
    /// Standard /v1/chat/completions endpoint.
    #[default]
    ChatCompletions,
    /// Next-generation /v1/responses endpoint.
    Responses,
}

/// Adapter for OpenAI models supporting Chat Completions and Responses endpoints.
#[derive(Clone)]
pub struct OpenAIAdapter {
    api_key: String,
    base_url: String,
    wire_api: OpenAIWireApi,
}

impl std::fmt::Debug for OpenAIAdapter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenAIAdapter")
            .field("api_key", &"[REDACTED]")
            .field("base_url", &self.base_url)
            .field("wire_api", &self.wire_api)
            .finish()
    }
}

impl OpenAIAdapter {
    /// Creates a new OpenAIAdapter with ChatCompletions API by default.
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: "https://api.openai.com/v1".to_string(),
            wire_api: OpenAIWireApi::ChatCompletions,
        }
    }

    /// Creates an OpenAIAdapter with custom base URL and specified wire format.
    pub fn with_options(
        api_key: impl Into<String>,
        base_url: impl Into<String>,
        wire_api: OpenAIWireApi,
    ) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: base_url.into(),
            wire_api,
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

    /// Returns current wire API format.
    pub fn wire_api(&self) -> OpenAIWireApi {
        self.wire_api
    }
}

impl ProtocolAdapter for OpenAIAdapter {
    fn capabilities(&self) -> whale_protocol::models::ModelCapabilities {
        let api = if self.wire_api == OpenAIWireApi::Responses {
            "responses"
        } else {
            "openai"
        };
        crate::capabilities::http_capabilities(api)
    }

    fn provider_name(&self) -> &'static str {
        "openai"
    }

    fn endpoint_url(&self) -> String {
        let base = self.base_url.trim_end_matches('/');
        match self.wire_api {
            OpenAIWireApi::ChatCompletions => format!("{}/chat/completions", base),
            OpenAIWireApi::Responses => format!("{}/responses", base),
        }
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
            let mut authorization = HeaderValue::from_str(&format!("Bearer {}", self.api_key))
                .map_err(|e| {
                    AdapterError::ProtocolError(format!("Invalid API key header: {}", e))
                })?;
            authorization.set_sensitive(true);
            headers.insert("Authorization", authorization);
        }
        headers.insert("Content-Type", HeaderValue::from_static("application/json"));

        match self.wire_api {
            OpenAIWireApi::ChatCompletions => {
                let mut body = serde_json::Map::new();
                body.insert("model".to_string(), json!(options.model));
                body.insert("stream".to_string(), json!(true));

                // stream_options to include usage in streaming
                body.insert(
                    "stream_options".to_string(),
                    json!({ "include_usage": true }),
                );

                if let Some(temp) = options.temperature {
                    // Preserve explicit settings; remote models validate their supported combinations.
                    body.insert("temperature".to_string(), json!(temp));
                }

                if let Some(max_tokens) = options.max_tokens {
                    if options.model.starts_with("o1") || options.model.starts_with("o3") {
                        body.insert("max_completion_tokens".to_string(), json!(max_tokens));
                    } else {
                        body.insert("max_tokens".to_string(), json!(max_tokens));
                    }
                }

                // Handle reasoning effort for o-series models (e.g. o1, o3-mini)
                if let Some(ref effort) = options.reasoning_effort {
                    body.insert("reasoning_effort".to_string(), json!(effort));
                }

                // Messages array
                let mut messages: Vec<serde_json::Value> = Vec::new();

                if let Some(sys) = system_prompt {
                    let role = if options.model.starts_with("o1") {
                        "developer"
                    } else {
                        "system"
                    };
                    messages.push(json!({
                        "role": role,
                        "content": sys
                    }));
                }

                // Only consecutive canonical calls belong to the same assistant batch.
                let mut tool_group: Option<usize> = None;
                for item in history {
                    if !matches!(item, CanonicalItem::ToolCall { .. }) {
                        tool_group = None;
                    }
                    match item {
                        CanonicalItem::UserMessage { content, .. } => {
                            // If single text block, serialize as simple string, otherwise array of content objects
                            if content.len() == 1 {
                                if let CanonicalContent::Text { text } = &content[0] {
                                    messages.push(json!({
                                        "role": "user",
                                        "content": text
                                    }));
                                    continue;
                                }
                            }

                            let mut content_arr = Vec::new();
                            for c in content {
                                match c {
                                    CanonicalContent::Text { text } => {
                                        content_arr.push(json!({
                                            "type": "text",
                                            "text": text
                                        }));
                                    }
                                    CanonicalContent::Image {
                                        mime_type,
                                        data,
                                        uri,
                                    } => {
                                        let url = if let Some(b64) = data {
                                            format!("data:{};base64,{}", mime_type, b64)
                                        } else if let Some(u) = uri {
                                            u.clone()
                                        } else {
                                            continue;
                                        };
                                        content_arr.push(json!({
                                            "type": "image_url",
                                            "image_url": { "url": url }
                                        }));
                                    }
                                    CanonicalContent::Audio { .. } => {}
                                }
                            }
                            messages.push(json!({
                                "role": "user",
                                "content": content_arr
                            }));
                        }
                        CanonicalItem::AssistantMessage { content, .. } => {
                            let text = content
                                .iter()
                                .filter_map(|c| match c {
                                    CanonicalContent::Text { text } => Some(text.as_str()),
                                    _ => None,
                                })
                                .collect::<Vec<_>>()
                                .join("");
                            messages.push(json!({
                                "role": "assistant",
                                "content": text
                            }));
                        }
                        CanonicalItem::Reasoning { thinking, .. } => {
                            // OpenAI reasoning content in history is typically formatted as assistant reasoning_content
                            // or omitted for non-reasoning APIs
                            messages.push(json!({
                                "role": "assistant",
                                "reasoning_content": thinking
                            }));
                        }
                        CanonicalItem::ToolCall {
                            call_id,
                            name,
                            arguments,
                            raw_arguments,
                            ..
                        } => {
                            let args_str = if !raw_arguments.is_empty() {
                                raw_arguments.clone()
                            } else if let Some(args) = arguments {
                                args.to_string()
                            } else {
                                "{}".to_string()
                            };

                            let call = json!({
                                "id": call_id,
                                "type": "function",
                                "function": { "name": name, "arguments": args_str }
                            });
                            if let Some(index) = tool_group {
                                messages[index]["tool_calls"]
                                    .as_array_mut()
                                    .expect("tool group always contains an array")
                                    .push(call);
                            } else {
                                tool_group = Some(messages.len());
                                messages.push(json!({
                                    "role": "assistant",
                                    "tool_calls": [call]
                                }));
                            }
                        }
                        CanonicalItem::ToolResult {
                            call_id, output, ..
                        } => {
                            let content_str = match output {
                                CanonicalToolOutput::Text { text } => text.clone(),
                                CanonicalToolOutput::Structured { data } => {
                                    serde_json::to_string(data).unwrap_or_else(|_| "{}".to_string())
                                }
                                CanonicalToolOutput::Blocks { blocks } => {
                                    let mut s = String::new();
                                    for b in blocks {
                                        if let CanonicalContent::Text { text } = b {
                                            s.push_str(text);
                                        }
                                    }
                                    s
                                }
                            };

                            messages.push(json!({
                                "role": "tool",
                                "tool_call_id": call_id,
                                "content": content_str
                            }));
                        }
                    }
                }

                body.insert("messages".to_string(), json!(messages));

                // Tools
                if !tools.is_empty() {
                    let openai_tools: Vec<serde_json::Value> = tools
                        .iter()
                        .map(|t| {
                            json!({
                                "type": "function",
                                "function": {
                                    "name": t.name,
                                    "description": t.description,
                                    "parameters": t.parameters
                                }
                            })
                        })
                        .collect();
                    body.insert("tools".to_string(), json!(openai_tools));
                }

                Ok((serde_json::Value::Object(body), headers))
            }
            OpenAIWireApi::Responses => Ok((
                crate::responses::serialize_request(system_prompt, history, tools, options)?,
                headers,
            )),
        }
    }

    fn parse_stream(
        &self,
        byte_stream: Pin<Box<dyn Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send>>,
    ) -> BoxedEventStream {
        if self.wire_api == OpenAIWireApi::Responses {
            return crate::responses::parse_stream(byte_stream);
        }
        let stream = try_stream! {
            let mut event_stream = byte_stream.eventsource();
            let turn_id = Uuid::new_v4().to_string();
            let thread_id = Uuid::new_v4().to_string();

            yield AgentStreamEvent::TurnStarted {
                turn_id: turn_id.clone(),
                thread_id: thread_id.clone(),
            };

            let mut current_text_item_id: Option<String> = None;
            let mut current_reasoning_item_id: Option<String> = None;
            let mut current_text_buf = String::new();
            let mut current_reasoning_buf = String::new();

            // Active tool calls being streamed: index -> (item_id, call_id, name, args_buf)
            let mut active_tool_calls: std::collections::HashMap<usize, (String, String, String, String)> =
                std::collections::HashMap::new();

            let mut current_usage = UsageMetrics::default();
            let mut finished = false;

            while let Some(event_result) = event_stream.next().await {
                let sse = event_result.map_err(|e| AdapterError::StreamParseError(e.to_string()))?;
                let data = sse.data.trim();
                if data.is_empty() {
                    continue;
                }
                if data == "[DONE]" {
                    break;
                }

                let parsed: serde_json::Value = serde_json::from_str(data)
                    .map_err(|e| AdapterError::StreamParseError(format!("Failed to parse JSON: {} in '{}'", e, data)))?;

                // Check for API errors
                if let Some(err) = parsed.get("error") {
                    let code = err.get("code").and_then(|c| c.as_str()).unwrap_or("OPENAI_ERROR");
                    let msg = err.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown OpenAI error");
                    yield AgentStreamEvent::TurnFailed {
                        turn_id: turn_id.clone(),
                        thread_id: thread_id.clone(),
                        error_code: code.to_string(),
                        error_message: msg.to_string(),
                    };
                    return;
                }

                // Check top-level usage (OpenAI returns usage in the final chunk when include_usage: true)
                if let Some(usage) = parsed.get("usage") {
                    if let Some(pt) = usage.get("prompt_tokens").and_then(|v| v.as_u64()) {
                        current_usage.input_tokens = pt;
                    }
                    if let Some(ct) = usage.get("completion_tokens").and_then(|v| v.as_u64()) {
                        current_usage.output_tokens = ct;
                    }
                    if let Some(details) = usage.get("completion_tokens_details") {
                        if let Some(rt) = details.get("reasoning_tokens").and_then(|v| v.as_u64()) {
                            current_usage.reasoning_tokens = rt;
                        }
                    }
                    if let Some(details) = usage.get("prompt_tokens_details") {
                        if let Some(cached) = details.get("cached_tokens").and_then(|v| v.as_u64()) {
                            current_usage.cache_read_input_tokens = cached;
                        }
                    }
                }

                let choices = match parsed.get("choices").and_then(|c| c.as_array()) {
                    Some(c) => c,
                    None => continue,
                };

                if choices.is_empty() {
                    continue;
                }
                if finished {
                    Err(AdapterError::ProtocolError(
                        "Chat Completions received a choice after finish_reason".into(),
                    ))?;
                }

                let choice = &choices[0];
                let delta = choice.get("delta");
                let finish_reason = choice.get("finish_reason").and_then(|f| f.as_str());

                if let Some(d) = delta {
                    // 1. Check reasoning_content (DeepSeek / OpenAI reasoning)
                    if let Some(reasoning) = d.get("reasoning_content").and_then(|r| r.as_str()) {
                        if !reasoning.is_empty() {
                            let item_id = current_reasoning_item_id.get_or_insert_with(|| {
                                new_item_id()
                            }).clone();

                            if current_reasoning_buf.is_empty() {
                                yield AgentStreamEvent::ItemStarted {
                                    turn_id: turn_id.clone(),
                                    item_id: item_id.clone(),
                                    item_type: "reasoning".to_string(),
                                    phase: Some(MessagePhase::Commentary),
                                };
                            }

                            current_reasoning_buf.push_str(reasoning);
                            yield AgentStreamEvent::ReasoningDelta {
                                turn_id: turn_id.clone(),
                                item_id: item_id.clone(),
                                delta: reasoning.to_string(),
                            };
                        }
                    }

                    // 2. Check standard text content
                    if let Some(content) = d.get("content").and_then(|c| c.as_str()) {
                        if !content.is_empty() {
                            // If we were reasoning and now switched to text, complete reasoning item
                            if let Some(r_id) = current_reasoning_item_id.take() {
                                yield AgentStreamEvent::ItemCompleted {
                                    turn_id: turn_id.clone(),
                                    item: CanonicalItem::Reasoning {
                                        id: r_id,
                                        thinking: current_reasoning_buf.clone(),
                                        signature: None,
                                        encrypted_content: None,
                                    },
                                };
                            }

                            let item_id = current_text_item_id.get_or_insert_with(|| {
                                new_item_id()
                            }).clone();

                            if current_text_buf.is_empty() {
                                yield AgentStreamEvent::ItemStarted {
                                    turn_id: turn_id.clone(),
                                    item_id: item_id.clone(),
                                    item_type: "assistant_message".to_string(),
                                    phase: Some(MessagePhase::FinalAnswer),
                                };
                            }

                            current_text_buf.push_str(content);
                            yield AgentStreamEvent::TextDelta {
                                turn_id: turn_id.clone(),
                                item_id: item_id.clone(),
                                delta: content.to_string(),
                            };
                        }
                    }

                    // 3. Check tool calls
                    if let Some(tool_calls) = d.get("tool_calls").and_then(|t| t.as_array()) {
                        for tc in tool_calls {
                            let index = tc.get("index").and_then(|i| i.as_u64()).unwrap_or(0) as usize;

                            let entry = active_tool_calls.entry(index).or_insert_with(|| {
                                (new_item_id(), String::new(), String::new(), String::new())
                            });

                            if let Some(id) = tc.get("id").and_then(|v| v.as_str()) {
                                entry.1 = id.to_string();
                            }
                            if let Some(func) = tc.get("function") {
                                if let Some(name) = func.get("name").and_then(|v| v.as_str()) {
                                    entry.2 = name.to_string();
                                }
                                if let Some(arg_chunk) = func.get("arguments").and_then(|v| v.as_str()) {
                                    if !arg_chunk.is_empty() {
                                        entry.3.push_str(arg_chunk);

                                        yield AgentStreamEvent::ToolCallDelta {
                                            turn_id: turn_id.clone(),
                                            item_id: entry.0.clone(),
                                            call_id: entry.1.clone(),
                                            delta: arg_chunk.to_string(),
                                        };
                                    }
                                }
                            }
                        }
                    }
                }

                // A socket EOF or [DONE] alone is not evidence of model completion.
                // Continue reading after finish_reason to retain the final usage chunk.
                if let Some(reason) = finish_reason {
                    if !matches!(reason, "stop" | "tool_calls") {
                        yield AgentStreamEvent::TurnFailed {
                            turn_id: turn_id.clone(),
                            thread_id: thread_id.clone(),
                            error_code: "incomplete_completion".into(),
                            error_message: format!("Chat completion stopped without a complete supported response: {reason}"),
                        };
                        return;
                    }
                    finished = true;
                    // Finalize reasoning item if open
                    if let Some(r_id) = current_reasoning_item_id.take() {
                        yield AgentStreamEvent::ItemCompleted {
                            turn_id: turn_id.clone(),
                            item: CanonicalItem::Reasoning {
                                id: r_id,
                                thinking: current_reasoning_buf.clone(),
                                signature: None,
                                encrypted_content: None,
                            },
                        };
                    }

                    // Finalize text item if open
                    if let Some(item_id) = current_text_item_id.take() {
                        yield AgentStreamEvent::ItemCompleted {
                            turn_id: turn_id.clone(),
                            item: CanonicalItem::AssistantMessage {
                                id: item_id,
                                content: vec![CanonicalContent::text(&current_text_buf)],
                                phase: MessagePhase::FinalAnswer,
                            },
                        };
                    }

                    // Finalize all active tool calls
                    let mut sorted_indices: Vec<_> = active_tool_calls.keys().cloned().collect();
                    sorted_indices.sort_unstable();
                    for idx in sorted_indices {
                        if let Some((item_id, call_id, name, raw_args)) = active_tool_calls.remove(&idx) {
                            let parsed_args: Option<serde_json::Value> = serde_json::from_str(&raw_args).ok();
                            yield AgentStreamEvent::ItemCompleted {
                                turn_id: turn_id.clone(),
                                item: CanonicalItem::ToolCall {
                                    id: item_id,
                                    call_id,
                                    namespace: None,
                                    name,
                                    arguments: parsed_args,
                                    raw_arguments: raw_args,
                                },
                            };
                        }
                    }
                }
            }

            if !finished {
                Err(AdapterError::StreamParseError(
                    "Unexpected EOF before Chat Completions finish_reason".into(),
                ))?;
            }
            yield AgentStreamEvent::TurnCompleted {
                turn_id: turn_id.clone(),
                thread_id: thread_id.clone(),
                usage: current_usage,
            };
        };

        Box::pin(stream)
    }
}
