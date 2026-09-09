//! Validation of the implemented wire subset before transport dispatch.
use crate::{AdapterError, SamplingOptions, ToolDefinition};
use whale_protocol::models::{
    ModelCapabilities, ModelCapabilityScope, ModelContentKind as Kind, ModelOption,
};
use whale_protocol::{CanonicalContent, CanonicalItem, CanonicalToolOutput};

pub fn http_capabilities(api: &str) -> ModelCapabilities {
    let mut caps = ModelCapabilities::text_only();
    caps.scope = ModelCapabilityScope::Protocol;
    caps.user_content.push(Kind::Image);
    caps.tool_calls = true;
    caps.reasoning_text = true;
    caps.options = vec![ModelOption::Temperature, ModelOption::MaxTokens];
    match api {
        "anthropic" => {
            caps.reasoning_signatures = true;
            caps.tool_result_content.push(Kind::Image);
            caps.options
                .extend([ModelOption::ThinkingBudget, ModelOption::PromptCaching]);
        }
        "responses" => {
            caps.encrypted_reasoning = true;
            caps.tool_result_content.push(Kind::Image);
            caps.options.push(ModelOption::ReasoningEffort);
        }
        _ => caps.options.push(ModelOption::ReasoningEffort),
    }
    caps
}

pub fn validate_configuration(
    model: &str,
    tools: &[ToolDefinition],
    options: &SamplingOptions,
    caps: &ModelCapabilities,
) -> Result<(), AdapterError> {
    let invalid = |message: &str| AdapterError::ProtocolError(message.into());
    if model.trim().is_empty() || model.trim() != model || options.model != model {
        return Err(invalid("Invalid or mismatched model identity"));
    }
    if options
        .temperature
        .is_some_and(|value| !value.is_finite() || !(0.0..=2.0).contains(&value))
    {
        return Err(invalid("temperature must be finite and between 0 and 2"));
    }
    if options.max_tokens == Some(0) || options.thinking_budget == Some(0) {
        return Err(invalid("Token limits must be positive"));
    }
    if options
        .reasoning_effort
        .as_ref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err(invalid("reasoning_effort must not be blank"));
    }
    if !tools.is_empty() && !caps.tool_calls {
        return Err(invalid("Unsupported capability: tool calls"));
    }
    for (present, option) in [
        (options.temperature.is_some(), ModelOption::Temperature),
        (options.max_tokens.is_some(), ModelOption::MaxTokens),
        (
            options.reasoning_effort.is_some(),
            ModelOption::ReasoningEffort,
        ),
        (
            options.thinking_budget.is_some(),
            ModelOption::ThinkingBudget,
        ),
        (options.prompt_caching, ModelOption::PromptCaching),
    ] {
        if present && !caps.options.contains(&option) {
            return Err(AdapterError::ProtocolError(format!(
                "Unsupported sampling option: {option:?}"
            )));
        }
    }
    Ok(())
}

pub fn validate_items(
    items: &[CanonicalItem],
    caps: &ModelCapabilities,
) -> Result<(), AdapterError> {
    let invalid = |message: &str| AdapterError::ProtocolError(message.into());
    let content = |blocks: &[CanonicalContent], supported: &[Kind], position: &str| {
        for block in blocks {
            let kind = match block {
                CanonicalContent::Text { .. } => Kind::Text,
                CanonicalContent::Image { .. } => Kind::Image,
                CanonicalContent::Audio { .. } => Kind::Audio,
            };
            if let CanonicalContent::Image { data, uri, .. } = block {
                if data.as_ref().is_none_or(|value| value.is_empty())
                    && uri.as_ref().is_none_or(|value| value.is_empty())
                {
                    return Err(AdapterError::ProtocolError(
                        "Image requires data or URI".into(),
                    ));
                }
            }
            if !supported.contains(&kind) {
                return Err(AdapterError::ProtocolError(format!(
                    "Unsupported {position} content: {kind:?}"
                )));
            }
        }
        Ok(())
    };
    for item in items {
        match item {
            CanonicalItem::UserMessage {
                content: blocks, ..
            } => content(blocks, &caps.user_content, "user")?,
            CanonicalItem::AssistantMessage {
                content: blocks, ..
            } => content(blocks, &caps.assistant_content, "assistant")?,
            CanonicalItem::Reasoning {
                thinking,
                signature,
                encrypted_content,
                ..
            } => {
                if !thinking.is_empty() && !caps.reasoning_text {
                    return Err(invalid("Unsupported reasoning text"));
                }
                if signature.is_some() && !caps.reasoning_signatures {
                    return Err(invalid("Unsupported reasoning signature"));
                }
                if encrypted_content.is_some() && !caps.encrypted_reasoning {
                    return Err(invalid("Unsupported encrypted reasoning"));
                }
            }
            CanonicalItem::ToolCall { .. } => {
                if !caps.tool_calls {
                    return Err(invalid("Unsupported capability: tool calls"));
                }
            }
            CanonicalItem::ToolResult { output, .. } => {
                if !caps.tool_calls {
                    return Err(invalid("Unsupported capability: tool results"));
                }
                match output {
                    CanonicalToolOutput::Blocks { blocks } => {
                        content(blocks, &caps.tool_result_content, "tool result")?
                    }
                    _ if !caps.tool_result_content.contains(&Kind::Text) => {
                        return Err(invalid("Unsupported text tool result"))
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}
