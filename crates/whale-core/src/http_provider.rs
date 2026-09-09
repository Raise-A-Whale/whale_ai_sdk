//! HTTP transport and SSE ownership for legacy wire adapters.
use crate::{model::*, CancellationToken};
use async_trait::async_trait;
use std::sync::Arc;
use whale_adapters::ProtocolAdapter;

pub struct HttpModelProvider {
    adapter: Arc<dyn ProtocolAdapter>,
    client: reqwest::Client,
}
impl HttpModelProvider {
    pub fn new(adapter: Arc<dyn ProtocolAdapter>) -> Self {
        Self::with_client(adapter, reqwest::Client::new())
    }
    pub fn with_client(adapter: Arc<dyn ProtocolAdapter>, client: reqwest::Client) -> Self {
        Self { adapter, client }
    }
}
#[async_trait]
impl ModelProvider for HttpModelProvider {
    fn capabilities(&self, model: &str) -> Result<ModelCapabilities, ModelError> {
        if model.trim().is_empty() {
            return Err(ModelError::InvalidRequest("model must not be blank".into()));
        }
        Ok(self.adapter.capabilities())
    }
    async fn stream(
        &self,
        request: ModelRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelEventStream, ModelError> {
        let caps = self.capabilities(&request.options.model)?;
        validate_model_request(&request, &caps)?;
        let (body, headers) = self.adapter.serialize_request(
            request.model_context.system_prompt.as_deref(),
            &request.model_context.items,
            &request.tools,
            &request.options,
        )?;
        let response = tokio::select! {
            biased;
            _=cancellation.cancelled()=>return Err(ModelError::Transport("Model request cancelled".into())),
            response=self.client.post(self.adapter.endpoint_url()).headers(headers).json(&body).send()=>response.map_err(|error|ModelError::Transport(error.to_string()))?,
        };
        if !response.status().is_success() {
            let status = response.status();
            let body = tokio::select! {_=cancellation.cancelled()=>return Err(ModelError::Transport("Model request cancelled".into())),body=response.text()=>body.unwrap_or_default()};
            return Err(ModelError::Transport(format!("API error {status}: {body}")));
        }
        let mut events = adapt_stream(self.adapter.parse_stream(Box::pin(response.bytes_stream())));
        Ok(Box::pin(async_stream::try_stream! {
            loop {
                let next=tokio::select! {biased;_=cancellation.cancelled()=>break,next=futures::StreamExt::next(&mut events)=>next};
                match next {Some(event)=>yield event?,None=>break}
            }
        }))
    }
}
