//! Validated provider construction shared by daemon session configuration.

use crate::CoreError;
use reqwest::{header::HeaderValue, Url};
use std::sync::Arc;
use whale_adapters::{AnthropicAdapter, OpenAIAdapter, OpenAIWireApi, ProtocolAdapter};
use whale_protocol::agents::{ProviderApi, ProviderAuth, ProviderConfig};

/// Resolves provider configuration before a session is made visible. Explicit
/// configurations require credentials unless unauthenticated use is requested.
/// Legacy inferred configuration retains its existing optional environment key.
pub fn build_adapter(
    model: &str,
    legacy_provider: Option<&str>,
    config: Option<&ProviderConfig>,
) -> Result<Arc<dyn ProtocolAdapter>, CoreError> {
    let invalid = |message: String| CoreError::InvalidConfiguration(message);
    let legacy_family = match legacy_provider {
        Some("openai") => Some("openai"),
        Some("anthropic") => Some("anthropic"),
        Some(other) => return Err(invalid(format!("Unknown provider '{other}'"))),
        None => None,
    };
    let api = config.map(|config| config.api).unwrap_or_else(|| {
        let family = legacy_family.unwrap_or(if model.to_lowercase().contains("claude") {
            "anthropic"
        } else {
            "openai"
        });
        if family == "anthropic" {
            ProviderApi::AnthropicMessages
        } else {
            ProviderApi::OpenaiChatCompletions
        }
    });
    let (family, default_base, conventional_variable) = match api {
        ProviderApi::AnthropicMessages => (
            "anthropic",
            "https://api.anthropic.com/v1",
            "ANTHROPIC_API_KEY",
        ),
        ProviderApi::OpenaiChatCompletions | ProviderApi::OpenaiResponses => {
            ("openai", "https://api.openai.com/v1", "OPENAI_API_KEY")
        }
    };
    if legacy_family.is_some_and(|legacy| legacy != family) {
        return Err(invalid(
            "provider and provider_config name conflicting protocol families".into(),
        ));
    }
    let base_url = config
        .and_then(|config| config.base_url.as_deref())
        .unwrap_or(default_base);
    let url = Url::parse(base_url)
        .map_err(|_| invalid("Provider base_url must be an absolute HTTP(S) URL".into()))?;
    if base_url.trim() != base_url
        || !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid("Provider base_url must be HTTP(S), with a host and without userinfo, query or fragment".into()));
    }
    let api_key = match config.map(|config| config.auth.as_ref()) {
        Some(Some(ProviderAuth::None)) => String::new(),
        Some(auth) => {
            let variable = match auth {
                Some(ProviderAuth::Env { variable }) => variable.as_str(),
                _ => conventional_variable,
            };
            if variable.is_empty() || variable.contains(['=', '\0']) {
                return Err(invalid(
                    "Provider credential environment variable name is invalid".into(),
                ));
            }
            let key = std::env::var(variable).map_err(|_| invalid(format!("Provider credential environment variable '{variable}' is missing or not Unicode")))?;
            if key.trim().is_empty() {
                return Err(invalid(format!(
                    "Provider credential environment variable '{variable}' is empty"
                )));
            }
            key
        }
        None => std::env::var(conventional_variable).unwrap_or_default(),
    };
    HeaderValue::from_str(&api_key).map_err(|_| {
        invalid("Provider credential cannot be represented as an HTTP header".into())
    })?;
    let base_url = url.to_string();
    Ok(match api {
        ProviderApi::AnthropicMessages => {
            Arc::new(AnthropicAdapter::with_base_url(api_key, base_url))
        }
        ProviderApi::OpenaiChatCompletions => Arc::new(OpenAIAdapter::with_options(
            api_key,
            base_url,
            OpenAIWireApi::ChatCompletions,
        )),
        ProviderApi::OpenaiResponses => Arc::new(OpenAIAdapter::with_options(
            api_key,
            base_url,
            OpenAIWireApi::Responses,
        )),
    })
}

pub use crate::model::ProviderFactory;
use crate::model::{ModelError, ModelProvider};
use std::collections::HashMap;

/// Startup-only registrations. A resolved session retains its selected instance.
#[derive(Default)]
pub struct ProviderRegistry {
    factories: HashMap<String, Arc<dyn ProviderFactory>>,
}
impl ProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn register_factory(
        &mut self,
        id: impl Into<String>,
        factory: Arc<dyn ProviderFactory>,
    ) -> Result<(), ModelError> {
        let id = id.into();
        if id.trim().is_empty() || id.trim() != id {
            return Err(ModelError::InvalidRequest(
                "Invalid provider reference".into(),
            ));
        }
        if self.factories.contains_key(&id) {
            return Err(ModelError::InvalidRequest(format!(
                "Provider reference '{id}' already registered"
            )));
        }
        self.factories.insert(id, factory);
        Ok(())
    }
    pub fn register_provider(
        &mut self,
        id: impl Into<String>,
        provider: Arc<dyn ModelProvider>,
    ) -> Result<(), ModelError> {
        struct Fixed(Arc<dyn ModelProvider>);
        impl ProviderFactory for Fixed {
            fn create(&self, _: &str) -> Result<Arc<dyn ModelProvider>, ModelError> {
                Ok(self.0.clone())
            }
        }
        self.register_factory(id, Arc::new(Fixed(provider)))
    }
    pub fn resolve(&self, id: &str, model: &str) -> Result<Arc<dyn ModelProvider>, ModelError> {
        if model.trim().is_empty() || model.trim() != model {
            return Err(ModelError::InvalidRequest("Invalid model identity".into()));
        }
        let factory = self.factories.get(id).ok_or_else(|| {
            ModelError::InvalidRequest(format!("Unknown provider reference '{id}'"))
        })?;
        let provider = factory.create(model)?;
        provider.capabilities(model)?;
        Ok(provider)
    }
}
