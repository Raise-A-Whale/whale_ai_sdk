//! Transport-neutral suspension bridge for host interactions.

use async_trait::async_trait;
use serde_json::Value;
use std::{future::Future, pin::Pin};
use whale_protocol::{
    contexts::RunContextInfo,
    interactions::{InteractionRequest, InteractionResponse},
};

use crate::CoreError;

type ResponseFuture = Pin<Box<dyn Future<Output = Result<Value, CoreError>> + Send + 'static>>;
type Cleanup = Box<dyn FnOnce() + Send + 'static>;

struct NoExternalSchemas;

impl jsonschema::Retrieve for NoExternalSchemas {
    fn retrieve(
        &self,
        _: &jsonschema::Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err("External Interaction schema references are disabled".into())
    }
}

/// Validates a request before a bridge may publish it.
///
/// Response schemas are always compiled as Draft 2020-12 with external
/// retrieval disabled. This is stricter than merely accepting a bounded JSON
/// object at the protocol decoding boundary.
pub fn validate_interaction_request(request: &InteractionRequest) -> Result<(), CoreError> {
    request
        .validate()
        .map_err(CoreError::InteractionRequestInvalid)?;
    if let Some(schema) = &request.response_schema {
        compile_response_schema(schema).map_err(CoreError::InteractionRequestInvalid)?;
    }
    Ok(())
}

/// Validates one response attempt without consuming the pending Interaction.
///
/// Bridge implementations must call this before their first-commit-wins state
/// transition. An invalid attempt returns `InteractionResponseInvalid` and must
/// leave the same request pending for a later corrected response.
pub fn validate_interaction_response(
    request: &InteractionRequest,
    response: &Value,
) -> Result<(), CoreError> {
    InteractionResponse::new("response-validation", response.clone())
        .map_err(CoreError::InteractionResponseInvalid)?;
    if let Some(schema) = &request.response_schema {
        let validator =
            compile_response_schema(schema).map_err(CoreError::InteractionRequestInvalid)?;
        validator.validate(response).map_err(|_| {
            CoreError::InteractionResponseInvalid(
                "Interaction response does not match response_schema".into(),
            )
        })?;
    }
    Ok(())
}

fn compile_response_schema(schema: &Value) -> Result<jsonschema::Validator, String> {
    jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .with_retriever(NoExternalSchemas)
        .build(schema)
        .map_err(|error| format!("Invalid Interaction response_schema: {error}"))
}

/// A published Interaction and its exact suspended continuation.
///
/// Dropping the ticket invokes its cleanup callback exactly once. Bridges use
/// that callback to remove only the identity created by the corresponding
/// `begin` call.
pub struct InteractionTicket {
    request_id: String,
    response: Option<ResponseFuture>,
    cleanup: Option<Cleanup>,
}

impl InteractionTicket {
    pub fn new<F, C>(request_id: String, response: F, cleanup: C) -> Self
    where
        F: Future<Output = Result<Value, CoreError>> + Send + 'static,
        C: FnOnce() + Send + 'static,
    {
        Self {
            request_id,
            response: Some(Box::pin(response)),
            cleanup: Some(Box::new(cleanup)),
        }
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub async fn response(mut self) -> Result<Value, CoreError> {
        self.response
            .take()
            .expect("Interaction response future is consumed once")
            .await
    }
}

impl Drop for InteractionTicket {
    fn drop(&mut self) {
        // Close the response receiver before asking the bridge to clean up. A
        // concurrent responder can then observe a vanished continuation and
        // must not acknowledge an unconsumed response.
        drop(self.response.take());
        if let Some(cleanup) = self.cleanup.take() {
            cleanup();
        }
    }
}

/// Owns provisional bridge state while `InteractionBridge::begin` is pending.
///
/// A bridge that allocates an identity before its last cancellable publication
/// await should create this guard immediately. `complete` transfers the exact
/// cleanup into the returned ticket; dropping the begin future runs it instead.
pub struct InteractionBeginGuard {
    request_id: String,
    cleanup: Option<Cleanup>,
}

impl InteractionBeginGuard {
    pub fn new<C>(request_id: String, cleanup: C) -> Self
    where
        C: FnOnce() + Send + 'static,
    {
        Self {
            request_id,
            cleanup: Some(Box::new(cleanup)),
        }
    }

    pub fn complete<F>(mut self, response: F) -> InteractionTicket
    where
        F: Future<Output = Result<Value, CoreError>> + Send + 'static,
    {
        InteractionTicket {
            request_id: self.request_id.clone(),
            response: Some(Box::pin(response)),
            cleanup: self.cleanup.take(),
        }
    }
}

impl Drop for InteractionBeginGuard {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup();
        }
    }
}

/// Publishes an Interaction and returns the exact suspended continuation.
///
/// Implementations must not return until publication succeeds. Cancellation
/// of `begin` itself must clean up any identity allocated before publication.
#[async_trait]
pub trait InteractionBridge: Send + Sync {
    async fn begin(
        &self,
        context: &RunContextInfo,
        request_id: String,
        request: InteractionRequest,
    ) -> Result<InteractionTicket, CoreError>;

    /// Resolves through the bridge's single response transaction.
    ///
    /// Implementations supporting typed approval compatibility validate the
    /// response before committing, preserve pending state on invalid attempts,
    /// and arbitrate generic and typed responders with the same first-commit-
    /// wins state. This synchronous method may take only a short state lock and
    /// enqueue publication; it must not perform transport I/O or block on a
    /// consumer. The default keeps existing custom bridge implementations source-
    /// compatible while declining typed delegation.
    fn respond(
        &self,
        _context: &RunContextInfo,
        _request_id: &str,
        _response: Value,
    ) -> Result<(), CoreError> {
        Err(CoreError::InteractionUnavailable(
            "Interaction bridge does not support response delegation".into(),
        ))
    }

    /// Clears one exact producer-owned request after a downstream publication
    /// boundary fails.
    ///
    /// The default remains a no-op for source compatibility. Bridges that
    /// retain resumable pending state should override this and remove only the
    /// request bound to `context` and `request_id`. Dropping the corresponding
    /// ticket still runs its exact-generation cleanup as a final safeguard.
    fn clear(&self, _context: &RunContextInfo, _request_id: &str, _cause: &str) {}
}
