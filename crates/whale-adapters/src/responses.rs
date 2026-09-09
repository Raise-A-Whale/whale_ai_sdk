//! OpenAI Responses wire conversion. This adapter owns model-step events, not the
//! outer agent turn. Reference:
//! <https://developers.openai.com/api/reference/resources/responses/streaming-events>
use crate::traits::{AdapterError, BoxedEventStream, SamplingOptions, ToolDefinition};
use async_stream::try_stream;
use eventsource_stream::Eventsource;
use futures::{Stream, StreamExt};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashSet},
    pin::Pin,
};
use uuid::Uuid;
use whale_protocol::{
    canonical::{CanonicalContent, CanonicalItem, CanonicalToolOutput, MessagePhase},
    events::{AgentStreamEvent, UsageMetrics},
};

fn protocol(message: impl Into<String>) -> AdapterError {
    AdapterError::ProtocolError(message.into())
}
fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str, AdapterError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| protocol(format!("Responses requires string field '{field}'")))
}
fn input_content(content: &[CanonicalContent]) -> Result<Vec<Value>, AdapterError> {
    content
        .iter()
        .map(|block| match block {
            CanonicalContent::Text { text } => Ok(json!({"type":"input_text", "text":text})),
            CanonicalContent::Image {
                mime_type,
                data,
                uri,
            } => {
                let image_url = match (data, uri) {
                    (Some(data), None) => format!("data:{mime_type};base64,{data}"),
                    (None, Some(uri)) => uri.clone(),
                    _ => {
                        return Err(protocol(
                            "Responses image requires exactly one of data or uri",
                        ))
                    }
                };
                Ok(json!({"type":"input_image", "image_url":image_url}))
            }
            CanonicalContent::Audio { .. } => Err(protocol(
                "Responses audio content is not supported by the canonical adapter",
            )),
        })
        .collect()
}

pub(crate) fn serialize_request(
    system_prompt: Option<&str>,
    history: &[CanonicalItem],
    tools: &[ToolDefinition],
    options: &SamplingOptions,
) -> Result<Value, AdapterError> {
    let mut input = Vec::with_capacity(history.len());
    for item in history {
        input.push(match item {
            CanonicalItem::UserMessage {content,..} => json!({"role":"user", "content":input_content(content)?}),
            CanonicalItem::AssistantMessage {content,phase,..} => {
                let text = content.iter().map(|part| match part {
                    CanonicalContent::Text{text} => Ok(text.as_str()),
                    _ => Err(protocol("Responses assistant history only supports text content")),
                }).collect::<Result<Vec<_>,_>>()?.join("");
                // EasyInputMessage accepts assistant phase and avoids inventing provider item IDs.
                json!({"role":"assistant", "content":text, "phase":phase})
            },
            CanonicalItem::Reasoning{id,thinking,signature,encrypted_content} => {
                if signature.is_some() { return Err(protocol("Responses cannot replay an Anthropic reasoning signature")); }
                let summary = if thinking.is_empty() {vec![]} else {vec![json!({"type":"summary_text", "text":thinking})]};
                let mut value=json!({"id":id,"type":"reasoning","summary":summary});
                if let Some(encrypted)=encrypted_content { value["encrypted_content"]=json!(encrypted); }
                value
            },
            CanonicalItem::ToolCall{call_id,name,namespace,arguments,raw_arguments,..} => {
                let arguments = if raw_arguments.is_empty() {arguments.as_ref().map(Value::to_string).unwrap_or_else(|| "{}".into())} else {raw_arguments.clone()};
                {
                    let mut call=json!({"type":"function_call", "call_id":call_id, "name":name, "arguments":arguments});
                    if let Some(namespace)=namespace {call["namespace"]=json!(namespace);}
                    call
                }
            },
            CanonicalItem::ToolResult{call_id,output,is_error,..} => {
                let mut output=match output {
                    CanonicalToolOutput::Text{text} => json!(text),
                    CanonicalToolOutput::Structured{data} => json!(data.to_string()),
                    CanonicalToolOutput::Blocks{blocks} => json!(input_content(blocks)?),
                };
                // The Responses wire type has no is_error field; expose that meaning
                // in the model-visible output instead of discarding it.
                if *is_error {output=json!(json!({"is_error":true,"output":output}).to_string());}
                json!({"type":"function_call_output", "call_id":call_id, "output":output})
            },
        });
    }
    // Whale owns the conversation. Encrypted reasoning permits stateless replay.
    let mut body = json!({"model":options.model,"stream":true,"store":false,"include":["reasoning.encrypted_content"],"input":input});
    if let Some(prompt) = system_prompt {
        body["instructions"] = json!(prompt);
    }
    if let Some(effort) = &options.reasoning_effort {
        body["reasoning"] = json!({"effort":effort});
    }
    if let Some(temperature) = options.temperature {
        body["temperature"] = json!(temperature);
    }
    if let Some(max) = options.max_tokens {
        body["max_output_tokens"] = json!(max);
    }
    if options.thinking_budget.is_some() {
        return Err(protocol(
            "Responses does not support thinking_budget; use reasoning_effort",
        ));
    }
    if !tools.is_empty() {
        body["tools"]=json!(tools.iter().map(|t|json!({"type":"function","name":t.name,"description":t.description,"parameters":t.parameters})).collect::<Vec<_>>());
    }
    Ok(body)
}

fn phase(value: &Value) -> Result<MessagePhase, AdapterError> {
    match value.get("phase").and_then(Value::as_str) {
        None | Some("final_answer") => Ok(MessagePhase::FinalAnswer),
        Some("commentary") => Ok(MessagePhase::Commentary),
        Some(other) => Err(protocol(format!(
            "Unsupported Responses message phase: {other}"
        ))),
    }
}
fn canonical(value: &Value) -> Result<CanonicalItem, AdapterError> {
    let id = string(value, "id")?.to_owned();
    match string(value, "type")? {
        "message" => {
            if string(value, "role")? != "assistant" {
                return Err(protocol(
                    "Responses output message must have assistant role",
                ));
            }
            let parts = value["content"]
                .as_array()
                .ok_or_else(|| protocol("Responses message requires content array"))?;
            let content = parts
                .iter()
                .map(|part| match string(part, "type")? {
                    "output_text" => Ok(CanonicalContent::text(string(part, "text")?)),
                    other => Err(protocol(format!(
                        "Unsupported Responses output content: {other}"
                    ))),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(CanonicalItem::AssistantMessage {
                id,
                content,
                phase: phase(value)?,
            })
        }
        "function_call" => {
            let raw_arguments = string(value, "arguments")?.to_owned();
            Ok(CanonicalItem::ToolCall {
                id,
                call_id: string(value, "call_id")?.to_owned(),
                namespace: value
                    .get("namespace")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                name: string(value, "name")?.to_owned(),
                arguments: serde_json::from_str(&raw_arguments).ok(),
                raw_arguments,
            })
        }
        "reasoning" => {
            if value
                .get("content")
                .and_then(Value::as_array)
                .is_some_and(|v| !v.is_empty())
            {
                return Err(protocol("Raw reasoning content cannot be replayed losslessly by the canonical Responses adapter; use reasoning summary/encrypted content"));
            }
            let summary = value["summary"]
                .as_array()
                .ok_or_else(|| protocol("Responses reasoning requires summary array"))?;
            let thinking = summary
                .iter()
                .map(|part| {
                    if string(part, "type")? != "summary_text" {
                        return Err(protocol("Unsupported Responses reasoning summary content"));
                    }
                    string(part, "text")
                })
                .collect::<Result<Vec<_>, _>>()?
                .join("\n");
            Ok(CanonicalItem::Reasoning {
                id,
                thinking,
                signature: None,
                encrypted_content: value
                    .get("encrypted_content")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            })
        }
        other => Err(protocol(format!(
            "Unsupported Responses output item: {other}"
        ))),
    }
}

struct ItemState {
    value: Value,
    completed: bool,
}
#[derive(Default)]
struct ResponseState {
    items: BTreeMap<u64, ItemState>,
    ids: HashSet<String>,
}
impl ResponseState {
    fn start(
        &mut self,
        index: u64,
        value: Value,
        turn_id: &str,
    ) -> Result<Option<AgentStreamEvent>, AdapterError> {
        if let Some(state) = self.items.get(&index) {
            if state.value["id"] != value["id"] {
                return Err(protocol(
                    "Responses reused output_index for a different item",
                ));
            }
            return Ok(None);
        }
        let id = string(&value, "id")?.to_owned();
        if !self.ids.insert(id.clone()) {
            return Err(protocol("Responses reused an output item ID"));
        }
        let (item_type, phase) = match string(&value, "type")? {
            "message" => ("assistant_message", Some(phase(&value)?)),
            "reasoning" => ("reasoning", Some(MessagePhase::Commentary)),
            "function_call" => ("tool_call", None),
            other => {
                return Err(protocol(format!(
                    "Unsupported Responses output item: {other}"
                )))
            }
        };
        self.items.insert(
            index,
            ItemState {
                value,
                completed: false,
            },
        );
        Ok(Some(AgentStreamEvent::ItemStarted {
            turn_id: turn_id.into(),
            item_id: id,
            item_type: item_type.into(),
            phase,
        }))
    }
    fn item_mut(&mut self, event: &Value) -> Result<&mut ItemState, AdapterError> {
        let index = event["output_index"]
            .as_u64()
            .ok_or_else(|| protocol("Responses event requires output_index"))?;
        let state = self
            .items
            .get_mut(&index)
            .ok_or_else(|| protocol("Responses delta/done arrived before output_item.added"))?;
        if string(event, "item_id")? != string(&state.value, "id")? {
            return Err(protocol(
                "Responses event item_id does not match output_index",
            ));
        }
        if state.completed {
            return Err(protocol(
                "Responses delta/done arrived after item completion",
            ));
        }
        Ok(state)
    }
    fn finish(
        &mut self,
        index: u64,
        value: Value,
        turn_id: &str,
    ) -> Result<Vec<AgentStreamEvent>, AdapterError> {
        let mut events = Vec::new();
        if let Some(event) = self.start(index, value.clone(), turn_id)? {
            events.push(event);
        }
        let state = self.items.get_mut(&index).unwrap();
        // Validate even repeated terminal snapshots so unsupported payloads are never ignored.
        let item = canonical(&value)?;
        if state.completed {
            if canonical(&state.value)? != item {
                return Err(protocol(
                    "Responses terminal snapshot changed a completed item",
                ));
            }
        } else {
            state.value = value;
            state.completed = true;
            events.push(AgentStreamEvent::ItemCompleted {
                turn_id: turn_id.into(),
                item,
            });
        }
        Ok(events)
    }
}

pub(crate) fn parse_stream(
    byte_stream: Pin<Box<dyn Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send>>,
) -> BoxedEventStream {
    Box::pin(try_stream! {
        let turn_id=Uuid::new_v4().to_string();
        let thread_id=Uuid::new_v4().to_string();
        yield AgentStreamEvent::TurnStarted{turn_id:turn_id.clone(),thread_id:thread_id.clone()};
        let mut state=ResponseState::default();
        let mut stream=byte_stream.eventsource();
        while let Some(event)=stream.next().await {
            let event=event.map_err(|e|AdapterError::StreamParseError(e.to_string()))?;
            if event.data.trim().is_empty() {continue;}
            let value:Value=serde_json::from_str(&event.data).map_err(|e|AdapterError::StreamParseError(format!("Invalid Responses SSE JSON: {e}")))?;
            let kind=string(&value,"type")?;
            match kind {
                "response.created"|"response.in_progress"|"response.queued" => {},
                "response.output_item.added" => {
                    let index=value["output_index"].as_u64().ok_or_else(||protocol("Responses item requires output_index"))?;
                    if let Some(event)=state.start(index,value["item"].clone(),&turn_id)? {yield event;}
                },
                "response.output_item.done" => {
                    let index=value["output_index"].as_u64().ok_or_else(||protocol("Responses item requires output_index"))?;
                    for event in state.finish(index,value["item"].clone(),&turn_id)? {yield event;}
                },
                "response.output_text.delta"|"response.reasoning_summary_text.delta"|"response.function_call_arguments.delta" => {
                    let item=state.item_mut(&value)?;
                    let item_id=string(&item.value,"id")?.to_owned();
                    let delta=string(&value,"delta")?.to_owned();
                    match kind {
                        "response.output_text.delta" => {
                            if string(&item.value,"type")?!="message" {Err(protocol("Text delta requires message item"))?;}
                            yield AgentStreamEvent::TextDelta{turn_id:turn_id.clone(),item_id,delta};
                        },
                        "response.reasoning_summary_text.delta" => {
                            if string(&item.value,"type")?!="reasoning" {Err(protocol("Reasoning delta requires reasoning item"))?;}
                            yield AgentStreamEvent::ReasoningDelta{turn_id:turn_id.clone(),item_id,delta};
                        },
                        _ => {
                            if string(&item.value,"type")?!="function_call" {Err(protocol("Arguments delta requires function_call item"))?;}
                            yield AgentStreamEvent::ToolCallDelta{turn_id:turn_id.clone(),item_id,call_id:string(&item.value,"call_id")?.into(),delta};
                        },
                    }
                },
                // These mark parts, not items. The full item.done (or terminal output
                // snapshot) supplies authoritative content, including encrypted reasoning.
                "response.output_text.done"|"response.function_call_arguments.done"|"response.reasoning_summary_text.done"|
                "response.content_part.added"|"response.content_part.done"|"response.reasoning_summary_part.added"|"response.reasoning_summary_part.done" => {
                    let _=state.item_mut(&value)?;
                },
                "response.completed" => {
                    let response=&value["response"];
                    if string(response,"status")?!="completed" {Err(protocol("response.completed has non-completed status"))?;}
                    let output=response["output"].as_array().ok_or_else(||protocol("response.completed requires output array"))?;
                    for (index,item) in output.iter().enumerate() {
                        for event in state.finish(index as u64,item.clone(),&turn_id)? {yield event;}
                    }
                    if state.items.len()!=output.len() || state.items.values().any(|s|!s.completed) {Err(protocol("Responses terminal output omitted a streamed item"))?;}
                    let usage=&response["usage"];
                    yield AgentStreamEvent::TurnCompleted{turn_id:turn_id.clone(),thread_id:thread_id.clone(),usage:UsageMetrics{
                        input_tokens:usage["input_tokens"].as_u64().unwrap_or(0),output_tokens:usage["output_tokens"].as_u64().unwrap_or(0),
                        reasoning_tokens:usage["output_tokens_details"]["reasoning_tokens"].as_u64().unwrap_or(0),
                        cache_read_input_tokens:usage["input_tokens_details"]["cached_tokens"].as_u64().unwrap_or(0),..Default::default()
                    }};
                    return;
                },
                "error"|"response.failed"|"response.incomplete" => {
                    let (code,message)=if kind=="error" {
                        (value["code"].as_str().unwrap_or("provider_error").to_owned(),value["message"].as_str().unwrap_or("Responses stream error").to_owned())
                    } else if kind=="response.failed" {
                        let error=&value["response"]["error"];
                        (error["code"].as_str().unwrap_or("response_failed").to_owned(),error["message"].as_str().unwrap_or("Responses generation failed").to_owned())
                    } else {
                        let reason=value["response"]["incomplete_details"]["reason"].as_str().unwrap_or("unknown");
                        ("response_incomplete".to_owned(),format!("Responses generation incomplete: {reason}"))
                    };
                    yield AgentStreamEvent::TurnFailed{turn_id:turn_id.clone(),thread_id:thread_id.clone(),error_code:code,error_message:message};
                    return;
                },
                // Annotation metadata is outside CanonicalContent. Native tool/audio/refusal
                // events, unlike metadata, must fail rather than produce false success.
                "response.output_text.annotation.added" => {},
                other => Err(protocol(format!("Unsupported Responses SSE event: {other}")))?,
            }
        }
        Err(AdapterError::StreamParseError("Unexpected EOF before Responses terminal event".into()))?;
    })
}
