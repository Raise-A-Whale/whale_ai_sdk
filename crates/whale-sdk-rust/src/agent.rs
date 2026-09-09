//! Reusable business Agent definitions with separately bound host functions.

use super::{
    ContextPolicyConfig, HostContextPolicy, HostTool, SdkError, ToolContext, WhaleClient,
    WhaleThread,
};
use crate::tool_packs::{
    BoundPackLease, PackPlan, SessionBindContext, SessionBindKind, SessionPackOwner, ToolPack,
    ToolPackTool,
};
use futures::FutureExt;
use std::collections::{HashMap, HashSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use whale_core::execution::compile_tool_schema;
use whale_protocol::interactions::{
    AttachRecoveryWithInteractionsParams, CreatePersistentSessionWithInteractionsParams,
    StartThreadWithInteractionsParams, CAPABILITY_INTERACTIONS,
};
use whale_protocol::recovery::*;
use whale_protocol::rpc::{
    RegisterToolDefinition, StartThreadParams, StartThreadResult, METHOD_SESSION_START_THREAD,
};

pub use whale_protocol::agents::{AgentDefinition, ProviderApi, ProviderAuth, ProviderConfig};

#[derive(Clone)]
pub struct Agent {
    client: WhaleClient,
    definition: Arc<AgentDefinition>,
    tools: Vec<Arc<BoundTool>>,
    pub(crate) packs: Vec<Arc<PackPlan>>,
    context_policy: Option<Arc<dyn HostContextPolicy>>,
    interactions_enabled: bool,
}

impl WhaleClient {
    /// Inspects a model selection without creating a session or invoking a model.
    /// Identity is checked so older or mismatched peers cannot silently select
    /// another provider when a registered reference is requested.
    pub async fn inspect_provider(
        &self,
        params: whale_protocol::models::InspectProviderParams,
    ) -> Result<whale_protocol::models::InspectProviderResult, SdkError> {
        params.validate().map_err(SdkError::InvalidConfiguration)?;
        let result: whale_protocol::models::InspectProviderResult = self
            .request(
                whale_protocol::models::METHOD_PROVIDER_INSPECT,
                Some(params.clone()),
            )
            .await?;
        if result.model != params.model || result.provider_ref != params.provider_ref {
            return Err(SdkError::InvalidConfiguration(
                "Provider inspection returned a different model or provider reference".into(),
            ));
        }
        Ok(result)
    }
    /// Captures configuration and exact host tool bindings without starting a session.
    pub fn agent(
        &self,
        definition: AgentDefinition,
        tools: Vec<Arc<dyn HostTool>>,
    ) -> Result<Agent, SdkError> {
        self.agent_with_tool_packs(definition, tools, Vec::new())
    }

    /// Captures static tools and reusable ToolPack factories without binding a Session.
    pub fn agent_with_tool_packs(
        &self,
        definition: AgentDefinition,
        tools: Vec<Arc<dyn HostTool>>,
        packs: Vec<Arc<dyn ToolPack>>,
    ) -> Result<Agent, SdkError> {
        definition
            .validate()
            .map_err(SdkError::InvalidConfiguration)?;
        let tools: Vec<_> = tools
            .into_iter()
            .map(|handler| {
                Arc::new(BoundTool {
                    definition: RegisterToolDefinition {
                        binding_id: None,
                        name: handler.name().into(),
                        description: handler.description().into(),
                        parameters: handler.parameters(),
                        supports_parallel: handler.supports_parallel(),
                        require_approval: handler.require_approval(),
                        is_host_tool: true,
                    },
                    handler,
                })
            })
            .collect();
        let expected: HashSet<_> = definition.tool_names.iter().cloned().collect();
        let mut actual = HashSet::new();
        for tool in &tools {
            if !actual.insert(tool.name().to_owned()) {
                return Err(SdkError::InvalidConfiguration(format!(
                    "Duplicate host tool binding: {}",
                    tool.name()
                )));
            }
        }
        let mut pack_ids = HashSet::new();
        let mut pack_plans = Vec::with_capacity(packs.len());
        for factory in packs {
            let manifest = catch_unwind(AssertUnwindSafe(|| factory.manifest())).map_err(|_| {
                SdkError::InvalidConfiguration(
                    "ToolPack manifest panicked while Agent configuration was captured".into(),
                )
            })?;
            if manifest.id.trim().is_empty() || manifest.id.trim() != manifest.id {
                return Err(SdkError::InvalidConfiguration(
                    "ToolPack IDs must be trimmed and nonempty".into(),
                ));
            }
            if !pack_ids.insert(manifest.id.clone()) {
                return Err(SdkError::InvalidConfiguration(format!(
                    "Duplicate ToolPack ID: {}",
                    manifest.id
                )));
            }
            if manifest.tools.is_empty() {
                return Err(SdkError::InvalidConfiguration(format!(
                    "ToolPack {} must declare at least one tool",
                    manifest.id
                )));
            }
            for tool in &manifest.tools {
                if tool.name.trim().is_empty() || tool.name.trim() != tool.name {
                    return Err(SdkError::InvalidConfiguration(format!(
                        "ToolPack {} tool names must be trimmed and nonempty",
                        manifest.id
                    )));
                }
                if tool.description.trim().is_empty() {
                    return Err(SdkError::InvalidConfiguration(format!(
                        "ToolPack {} tool {} must have a nonempty description",
                        manifest.id, tool.name
                    )));
                }
                compile_tool_schema(&tool.parameters).map_err(|error| {
                    SdkError::InvalidConfiguration(format!(
                        "ToolPack {} tool {} has an invalid parameter schema: {error}",
                        manifest.id, tool.name
                    ))
                })?;
                if !actual.insert(tool.name.clone()) {
                    return Err(SdkError::InvalidConfiguration(format!(
                        "Duplicate tool binding: {}",
                        tool.name
                    )));
                }
            }
            pack_plans.push(Arc::new(PackPlan { manifest, factory }));
        }
        if actual != expected {
            return Err(SdkError::InvalidConfiguration(
                "Host tool bindings must exactly match AgentDefinition.tool_names".into(),
            ));
        }
        Ok(Agent {
            client: self.clone(),
            definition: Arc::new(definition),
            tools,
            packs: pack_plans,
            context_policy: None,
            interactions_enabled: false,
        })
    }

    /// Creates a session using an explicit provider protocol, endpoint and credential reference.
    pub async fn create_thread_with_provider(
        &self,
        model: impl Into<String>,
        system_prompt: Option<String>,
        provider_config: ProviderConfig,
    ) -> Result<WhaleThread, SdkError> {
        let mut definition = AgentDefinition::new("session", model);
        definition.system_prompt = system_prompt;
        definition.provider_config = Some(provider_config);
        self.agent(definition, Vec::new())?.create_session().await
    }
}

enum PackBindFailure {
    Rollback(SdkError),
    ConnectionClosed(SdkError),
    WaiterDropped,
}

enum SessionCreationKind {
    Ephemeral,
    PersistentCreate {
        key: RecoveryKey,
    },
    PersistentAttach {
        key: RecoveryKey,
        expected_revision: u64,
    },
}

impl SessionCreationKind {
    fn live_session_id(&self) -> String {
        match self {
            Self::Ephemeral => format!("th_{}", uuid::Uuid::new_v4()),
            Self::PersistentCreate { .. } | Self::PersistentAttach { .. } => {
                uuid::Uuid::new_v4().to_string()
            }
        }
    }

    fn bind_kind(&self) -> SessionBindKind {
        match self {
            Self::Ephemeral => SessionBindKind::Ephemeral,
            Self::PersistentCreate { key } => SessionBindKind::PersistentCreate {
                recovery_id: key.recovery_id.clone(),
            },
            Self::PersistentAttach { key, .. } => SessionBindKind::PersistentAttach {
                recovery_id: key.recovery_id.clone(),
            },
        }
    }
}

struct SessionDelivery {
    thread: Option<WhaleThread>,
    return_to_owner: Option<tokio::sync::oneshot::Sender<WhaleThread>>,
}

impl SessionDelivery {
    fn new(
        thread: WhaleThread,
        return_to_owner: tokio::sync::oneshot::Sender<WhaleThread>,
    ) -> Self {
        Self {
            thread: Some(thread),
            return_to_owner: Some(return_to_owner),
        }
    }

    fn claim(mut self) -> WhaleThread {
        self.return_to_owner.take();
        self.thread
            .take()
            .expect("an undelivered Session retains its thread")
    }
}

impl Drop for SessionDelivery {
    fn drop(&mut self) {
        if let (Some(thread), Some(return_to_owner)) =
            (self.thread.take(), self.return_to_owner.take())
        {
            let _ = return_to_owner.send(thread);
        }
    }
}

fn validate_bound_pack_tools(
    plan: &PackPlan,
    handlers: Vec<Arc<dyn HostTool>>,
) -> Result<Vec<Arc<BoundTool>>, SdkError> {
    let mut captured = HashMap::new();
    for handler in handlers {
        let metadata = catch_unwind(AssertUnwindSafe(|| ToolPackTool {
            name: handler.name().to_owned(),
            description: handler.description().to_owned(),
            parameters: handler.parameters(),
            supports_parallel: handler.supports_parallel(),
            require_approval: handler.require_approval(),
        }))
        .map_err(|_| {
            SdkError::Internal(format!(
                "ToolPack {} handler metadata panicked",
                plan.manifest.id
            ))
        })?;
        if captured
            .insert(metadata.name.clone(), (metadata, handler))
            .is_some()
        {
            return Err(SdkError::InvalidConfiguration(format!(
                "ToolPack {} returned duplicate handler names",
                plan.manifest.id
            )));
        }
    }
    let mut bound = Vec::with_capacity(plan.manifest.tools.len());
    for expected in &plan.manifest.tools {
        let Some((actual, handler)) = captured.remove(&expected.name) else {
            return Err(SdkError::InvalidConfiguration(format!(
                "ToolPack {} handlers do not match its frozen manifest",
                plan.manifest.id
            )));
        };
        if actual != *expected {
            return Err(SdkError::InvalidConfiguration(format!(
                "ToolPack {} handler metadata does not match its frozen manifest",
                plan.manifest.id
            )));
        }
        bound.push(Arc::new(BoundTool {
            definition: RegisterToolDefinition {
                binding_id: None,
                name: expected.name.clone(),
                description: expected.description.clone(),
                parameters: expected.parameters.clone(),
                supports_parallel: expected.supports_parallel,
                require_approval: expected.require_approval,
                is_host_tool: true,
            },
            handler,
        }));
    }
    if !captured.is_empty() {
        return Err(SdkError::InvalidConfiguration(format!(
            "ToolPack {} handlers do not match its frozen manifest",
            plan.manifest.id
        )));
    }
    Ok(bound)
}

impl Agent {
    /// Opts Sessions created by this Agent into the generic Interaction surface.
    /// The capability is checked before any Session business request is sent.
    pub fn with_interactions_enabled(mut self) -> Self {
        self.interactions_enabled = true;
        self
    }

    pub fn interactions_enabled(&self) -> bool {
        self.interactions_enabled
    }

    async fn require_interactions(&self) -> Result<(), SdkError> {
        if self.interactions_enabled
            && !self
                .client
                .initialize()
                .await?
                .capabilities
                .iter()
                .any(|capability| capability == CAPABILITY_INTERACTIONS)
        {
            return Err(SdkError::ProtocolCompatibility(
                "Daemon does not advertise interactions.v1".into(),
            ));
        }
        Ok(())
    }

    async fn bind_session_packs(
        &self,
        context: SessionBindContext,
        build: &mut SessionBuildGuard,
        waiter: &tokio::sync::oneshot::Sender<Result<SessionDelivery, SdkError>>,
    ) -> Result<Vec<Arc<BoundTool>>, PackBindFailure> {
        let mut tools = Vec::new();
        for plan in &self.packs {
            let mut shutdown = self.client.inner.state.shutdown.subscribe();
            if *shutdown.borrow() {
                return Err(PackBindFailure::ConnectionClosed(SdkError::ChannelClosed(
                    "Client is closed".into(),
                )));
            }
            let binding = AssertUnwindSafe(plan.factory.bind(context.clone())).catch_unwind();
            let result = tokio::select! {
                biased;
                _ = async {
                    while !*shutdown.borrow_and_update() {
                        if shutdown.changed().await.is_err() {
                            break;
                        }
                    }
                } => return Err(PackBindFailure::ConnectionClosed(
                    SdkError::ChannelClosed("Client closed during ToolPack bind".into()),
                )),
                result = binding => result,
            };
            let bound = match result {
                Ok(Ok(bound)) => bound,
                Ok(Err(error)) => {
                    return Err(PackBindFailure::Rollback(SdkError::Internal(format!(
                        "ToolPack {} bind failed: {}",
                        plan.manifest.id, error
                    ))))
                }
                Err(_) => {
                    return Err(PackBindFailure::Rollback(SdkError::Internal(format!(
                        "ToolPack {} bind panicked",
                        plan.manifest.id
                    ))))
                }
            };
            build
                .packs
                .as_mut()
                .expect("an Agent with packs has a build owner")
                .push(BoundPackLease::new(plan.manifest.id.clone(), bound));
            let handlers = build
                .packs
                .as_ref()
                .expect("an Agent with packs has a build owner")
                .last_tools()
                .map_err(|message| PackBindFailure::Rollback(SdkError::Internal(message)))?;
            tools.extend(
                validate_bound_pack_tools(plan, handlers).map_err(PackBindFailure::Rollback)?,
            );
            if waiter.is_closed() {
                return Err(PackBindFailure::WaiterDropped);
            }
        }
        Ok(tools)
    }

    async fn prepare_session_build(
        &self,
        creation: &SessionCreationKind,
        waiter: &tokio::sync::oneshot::Sender<Result<SessionDelivery, SdkError>>,
    ) -> Result<(String, SessionBuildGuard, Vec<RegisterToolDefinition>), SdkError> {
        let thread_id = creation.live_session_id();
        let lifecycle = self.client.inner.state.prepare_session(&thread_id)?;
        let packs = (!self.packs.is_empty()).then(|| {
            SessionPackOwner::new(
                self.packs
                    .iter()
                    .flat_map(|plan| plan.manifest.tools.iter().map(|tool| tool.name.clone())),
            )
        });
        let mut build =
            SessionBuildGuard::new(self.client.clone(), thread_id.clone(), lifecycle, packs);
        let bind_context = SessionBindContext::new(
            thread_id.clone(),
            self.definition.name.clone(),
            creation.bind_kind(),
            self.client.inner.state.session_released(&thread_id),
        );
        let pack_tools = match self
            .bind_session_packs(bind_context, &mut build, waiter)
            .await
        {
            Ok(tools) => tools,
            Err(PackBindFailure::Rollback(error)) => return Err(build.rollback(error).await),
            Err(PackBindFailure::WaiterDropped) => {
                return Err(build
                    .rollback(SdkError::ChannelClosed(
                        "Session creation waiter dropped".into(),
                    ))
                    .await)
            }
            Err(PackBindFailure::ConnectionClosed(error)) => return Err(error),
        };
        if waiter.is_closed() {
            return Err(build
                .rollback(SdkError::ChannelClosed(
                    "Session creation waiter dropped".into(),
                ))
                .await);
        }
        if let Some(policy) = &self.context_policy {
            build.stage_context(policy.clone());
        }
        let mut tools = Vec::with_capacity(self.tools.len() + pack_tools.len());
        for tool in self.tools.iter().cloned().chain(pack_tools) {
            tools.push(build.stage_tool(tool));
        }
        if waiter.is_closed() {
            return Err(build
                .rollback(SdkError::ChannelClosed(
                    "Session creation waiter dropped".into(),
                ))
                .await);
        }
        Ok((thread_id, build, tools))
    }

    async fn require_limits(&self) -> Result<(), SdkError> {
        if self.definition.limits.is_some()
            && !self
                .client
                .initialize()
                .await?
                .capabilities
                .iter()
                .any(|cap| cap == whale_protocol::retention::CAPABILITY_SESSION_LIMITS)
        {
            return Err(SdkError::ProtocolCompatibility(
                "Daemon does not advertise session_limits.v1".into(),
            ));
        }
        Ok(())
    }

    /// Creates explicitly persistent history. Allocate and retain `key` before
    /// this call so an uncertain response can be inspected after reconnecting.
    /// Once dispatched, dropping the waiter still settles the transaction; an
    /// unclaimed successful attachment is closed and retained for later recovery.
    pub async fn create_persistent_session(
        &self,
        key: &RecoveryKey,
    ) -> Result<WhaleThread, SdkError> {
        self.persistent_session(key, None).await
    }

    /// Restores committed history with fresh live identity and callback bindings.
    /// This does not execute a model, replay a tool or reactivate old approvals.
    pub async fn recover_session(&self, key: &RecoveryKey) -> Result<WhaleThread, SdkError> {
        self.require_interactions().await?;
        self.require_limits().await?;
        let snapshot = self.client.inspect_recovery(key).await?;
        self.persistent_session(key, Some(snapshot.revision)).await
    }

    async fn persistent_session(
        &self,
        key: &RecoveryKey,
        revision: Option<u64>,
    ) -> Result<WhaleThread, SdkError> {
        self.require_interactions().await?;
        self.client.require_recovery(key).await?;
        self.require_limits().await?;
        if self.definition.context_policy == Some(ContextPolicyConfig::Host)
            && self.context_policy.is_none()
        {
            return Err(SdkError::InvalidConfiguration(
                "Host context policy requires a callback binding".into(),
            ));
        }
        if self.definition.provider_ref.is_some() {
            self.client
                .inspect_provider(whale_protocol::models::InspectProviderParams {
                    model: self
                        .definition
                        .default_options
                        .model
                        .clone()
                        .unwrap_or_else(|| self.definition.model.clone()),
                    provider_ref: self.definition.provider_ref.clone(),
                    provider: None,
                    provider_config: self.definition.provider_config.clone(),
                })
                .await?;
        }
        let agent = self.clone();
        let key = key.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            match agent.attach_persistent_owned(key, revision, &sender).await {
                Ok(thread) => {
                    let (return_to_owner, returned) = tokio::sync::oneshot::channel();
                    let delivery = SessionDelivery::new(thread, return_to_owner);
                    if let Err(undelivered) = sender.send(Ok(delivery)) {
                        drop(undelivered);
                    }
                    if let Ok(unclaimed) = returned.await {
                        agent.close_unclaimed_session(unclaimed).await;
                    }
                }
                Err(error) => {
                    let _ = sender.send(Err(error));
                }
            }
        });
        let delivery = receiver
            .await
            .map_err(|_| SdkError::ChannelClosed("Persistent session task stopped".into()))??;
        Ok(delivery.claim())
    }

    async fn attach_persistent_owned(
        &self,
        key: RecoveryKey,
        revision: Option<u64>,
        waiter: &tokio::sync::oneshot::Sender<Result<SessionDelivery, SdkError>>,
    ) -> Result<WhaleThread, SdkError> {
        let creation = match revision {
            Some(expected_revision) => SessionCreationKind::PersistentAttach {
                key: key.clone(),
                expected_revision,
            },
            None => SessionCreationKind::PersistentCreate { key: key.clone() },
        };
        let (thread_id, mut build, tools) = self.prepare_session_build(&creation, waiter).await?;
        let session = StartThreadParams {
            limits: self.definition.limits.clone(),
            session_id: Some(thread_id.clone()),
            agent_name: Some(self.definition.name.clone()),
            context_policy: self.definition.context_policy.clone(),
            provider: None,
            provider_ref: self.definition.provider_ref.clone(),
            provider_config: self.definition.provider_config.clone(),
            options: Some(self.definition.default_options.clone()),
            model: self.definition.model.clone(),
            system_prompt: self.definition.system_prompt.clone(),
            tools,
            metadata: Default::default(),
        };
        let run_defaults = SessionRunDefaults {
            max_steps: self.definition.max_steps,
            timeout_ms: self.definition.timeout_ms,
        };
        let response: Result<PersistentSessionResult, SdkError> = match &creation {
            SessionCreationKind::PersistentAttach {
                key,
                expected_revision,
            } => {
                let params = AttachRecoveryParams {
                    key: key.clone(),
                    expected_revision: *expected_revision,
                    session,
                    run_defaults,
                };
                if self.interactions_enabled {
                    self.client
                        .request(
                            METHOD_RECOVERY_ATTACH,
                            Some(AttachRecoveryWithInteractionsParams::new(params, true)),
                        )
                        .await
                } else {
                    self.client
                        .request(METHOD_RECOVERY_ATTACH, Some(params))
                        .await
                }
            }
            SessionCreationKind::PersistentCreate { key } => {
                let params = CreatePersistentSessionParams {
                    key: key.clone(),
                    session,
                    run_defaults,
                };
                if self.interactions_enabled {
                    self.client
                        .request(
                            METHOD_SESSION_CREATE_PERSISTENT,
                            Some(CreatePersistentSessionWithInteractionsParams::new(
                                params, true,
                            )),
                        )
                        .await
                } else {
                    self.client
                        .request(METHOD_SESSION_CREATE_PERSISTENT, Some(params))
                        .await
                }
            }
            SessionCreationKind::Ephemeral => unreachable!("persistent helper received ephemeral"),
        };
        match response {
            Ok(result)
                if result.thread.thread_id == thread_id
                    && result.key == key
                    && result.epoch > 0 =>
            {
                match build.commit() {
                    Ok(()) => {
                        if self.interactions_enabled {
                            self.client.inner.state.enable_interactions(&thread_id)?;
                        }
                        Ok(WhaleThread {
                            client: self.client.clone(),
                            thread_id,
                            recovery_key: Some(key),
                            max_steps: self.definition.max_steps,
                            timeout_ms: self.definition.timeout_ms,
                        })
                    }
                    Err(error) => {
                        let _ = self
                            .client
                            .inner
                            .state
                            .finish_preparation(&thread_id, false);
                        self.client.close().await;
                        Err(error)
                    }
                }
            }
            Err(error) if crate::recovery::definitive_rejection(&error) => {
                Err(build.rollback(error).await)
            }
            Err(error) => {
                let _ = self
                    .client
                    .inner
                    .state
                    .finish_preparation(&thread_id, false);
                self.client.close().await;
                Err(error)
            }
            Ok(_) => {
                let _ = self
                    .client
                    .inner
                    .state
                    .finish_preparation(&thread_id, false);
                self.client.close().await;
                Err(SdkError::Internal(
                    "Persistent session response identity or epoch mismatch".into(),
                ))
            }
        }
    }

    /// Binds host resources separately and selects the host context policy.
    pub fn with_context_policy(mut self, policy: Arc<dyn HostContextPolicy>) -> Self {
        Arc::make_mut(&mut self.definition).context_policy = Some(ContextPolicyConfig::Host);
        self.context_policy = Some(policy);
        self
    }
    pub fn definition(&self) -> &AgentDefinition {
        &self.definition
    }

    /// Creates independent history. Host function objects are intentionally shared
    /// references; bind per-session business resources through separate Agents.
    pub async fn create_session(&self) -> Result<WhaleThread, SdkError> {
        self.client.initialize().await?;
        self.require_interactions().await?;
        self.require_limits().await?;
        if self.definition.context_policy == Some(ContextPolicyConfig::Host)
            && self.context_policy.is_none()
        {
            return Err(SdkError::InvalidConfiguration(
                "Host context policy requires a callback binding".into(),
            ));
        }
        if self.definition.provider_ref.is_some() {
            self.client
                .inspect_provider(whale_protocol::models::InspectProviderParams {
                    model: self
                        .definition
                        .default_options
                        .model
                        .clone()
                        .unwrap_or_else(|| self.definition.model.clone()),
                    provider_ref: self.definition.provider_ref.clone(),
                    provider: None,
                    provider_config: self.definition.provider_config.clone(),
                })
                .await?;
        }
        let agent = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            match agent.create_session_owned(&sender).await {
                Ok(thread) => {
                    let (return_to_owner, returned) = tokio::sync::oneshot::channel();
                    let delivery = SessionDelivery::new(thread, return_to_owner);
                    if let Err(undelivered) = sender.send(Ok(delivery)) {
                        drop(undelivered);
                    }
                    if let Ok(unclaimed) = returned.await {
                        agent.close_unclaimed_session(unclaimed).await;
                    }
                }
                Err(error) => {
                    let _ = sender.send(Err(error));
                }
            }
        });
        let delivery = receiver
            .await
            .map_err(|_| SdkError::ChannelClosed("Session creation task stopped".into()))??;
        Ok(delivery.claim())
    }

    async fn create_session_owned(
        &self,
        waiter: &tokio::sync::oneshot::Sender<Result<SessionDelivery, SdkError>>,
    ) -> Result<WhaleThread, SdkError> {
        let (thread_id, mut build, tools) = self
            .prepare_session_build(&SessionCreationKind::Ephemeral, waiter)
            .await?;
        let params = StartThreadParams {
            limits: self.definition.limits.clone(),
            agent_name: Some(self.definition.name.clone()),
            context_policy: self.definition.context_policy.clone(),
            session_id: Some(thread_id.clone()),
            provider: None,
            provider_ref: self.definition.provider_ref.clone(),
            provider_config: self.definition.provider_config.clone(),
            options: Some(self.definition.default_options.clone()),
            model: self.definition.model.clone(),
            system_prompt: self.definition.system_prompt.clone(),
            tools,
            metadata: serde_json::Map::from_iter([(
                "agent_name".into(),
                serde_json::Value::String(self.definition.name.clone()),
            )]),
        };
        let result: Result<StartThreadResult, SdkError> = if self.interactions_enabled {
            self.client
                .request(
                    METHOD_SESSION_START_THREAD,
                    Some(StartThreadWithInteractionsParams::new(params, true)),
                )
                .await
        } else {
            self.client
                .request(METHOD_SESSION_START_THREAD, Some(params))
                .await
        };
        match result {
            Ok(accepted) if accepted.thread_id == thread_id => {
                build.commit()?;
                if self.interactions_enabled {
                    self.client.inner.state.enable_interactions(&thread_id)?;
                }
                Ok(WhaleThread {
                    recovery_key: None,
                    client: self.client.clone(),
                    thread_id,
                    max_steps: self.definition.max_steps,
                    timeout_ms: self.definition.timeout_ms,
                })
            }
            Err(error)
                if self
                    .client
                    .inner
                    .state
                    .closed
                    .load(std::sync::atomic::Ordering::SeqCst) =>
            {
                Err(error)
            }
            Err(error) => {
                let outcome_unknown =
                    !matches!(error, SdkError::Rpc { .. } | SdkError::LimitExceeded(_));
                let error = build.rollback(error).await;
                if outcome_unknown {
                    self.client.close().await;
                }
                Err(error)
            }
            Ok(_) => {
                let error = build
                    .rollback(SdkError::Internal(
                        "Daemon returned a different session identity".into(),
                    ))
                    .await;
                self.client.close().await;
                Err(error)
            }
        }
    }

    async fn close_unclaimed_session(&self, thread: WhaleThread) {
        let thread_id = thread.id().to_owned();
        if thread.close().await.is_err() {
            self.client.close().await;
            return;
        }
        if let Some(owner) = self.client.inner.state.take_attached_pack_owner(&thread_id) {
            if !owner.close_reverse().await.is_empty() {
                self.client.close().await;
            }
        }
    }
}

/// Metadata is configuration captured with the Agent; execution retains the host object.
struct BoundTool {
    definition: RegisterToolDefinition,
    handler: Arc<dyn HostTool>,
}

#[async_trait::async_trait]
impl HostTool for BoundTool {
    fn name(&self) -> &str {
        &self.definition.name
    }
    fn description(&self) -> &str {
        &self.definition.description
    }
    fn parameters(&self) -> serde_json::Value {
        self.definition.parameters.clone()
    }
    fn supports_parallel(&self) -> bool {
        self.definition.supports_parallel
    }
    fn require_approval(&self) -> bool {
        self.definition.require_approval
    }
    async fn execute(
        &self,
        arguments: serde_json::Value,
    ) -> Result<whale_protocol::CanonicalToolOutput, String> {
        self.handler.execute(arguments).await
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        arguments: serde_json::Value,
    ) -> Result<whale_protocol::CanonicalToolOutput, String> {
        self.handler.execute_with_context(context, arguments).await
    }
}

struct SessionBuildGuard {
    client: WhaleClient,
    thread_id: String,
    lifecycle: Arc<crate::sessions::SessionLifecycle>,
    staged_names: Vec<((String, String), Arc<dyn HostTool>)>,
    staged_versions: Vec<((String, String), Arc<dyn HostTool>)>,
    context_binding: Option<(String, Arc<dyn HostContextPolicy>)>,
    packs: Option<SessionPackOwner>,
    committed: bool,
}

impl SessionBuildGuard {
    fn new(
        client: WhaleClient,
        thread_id: String,
        lifecycle: Arc<crate::sessions::SessionLifecycle>,
        packs: Option<SessionPackOwner>,
    ) -> Self {
        Self {
            client,
            thread_id,
            lifecycle,
            staged_names: Vec::new(),
            staged_versions: Vec::new(),
            context_binding: None,
            packs,
            committed: false,
        }
    }

    fn stage_context(&mut self, policy: Arc<dyn HostContextPolicy>) {
        self.client
            .inner
            .state
            .context_policies
            .insert(self.thread_id.clone(), policy.clone());
        self.context_binding = Some((self.thread_id.clone(), policy));
    }

    fn stage_tool(&mut self, tool: Arc<BoundTool>) -> RegisterToolDefinition {
        let mut definition = tool.definition.clone();
        let binding_id = uuid::Uuid::new_v4().to_string();
        definition.binding_id = Some(binding_id.clone());
        let binding: Arc<dyn HostTool> = tool;
        let name_key = (self.thread_id.clone(), definition.name.clone());
        let version_key = (self.thread_id.clone(), binding_id);
        self.client
            .inner
            .state
            .tools
            .insert(name_key.clone(), binding.clone());
        self.client
            .inner
            .state
            .tool_bindings
            .insert(version_key.clone(), binding.clone());
        self.staged_names.push((name_key, binding.clone()));
        self.staged_versions.push((version_key, binding));
        definition
    }

    fn remove_exact_routes(&mut self) {
        if let Some((key, policy)) = self.context_binding.take() {
            self.client
                .inner
                .state
                .context_policies
                .remove_if(&key, |_, current| Arc::ptr_eq(current, &policy));
        }
        for (key, binding) in self.staged_versions.drain(..) {
            self.client
                .inner
                .state
                .tool_bindings
                .remove_if(&key, |_, current| Arc::ptr_eq(current, &binding));
        }
        for (key, binding) in self.staged_names.drain(..) {
            self.client
                .inner
                .state
                .tools
                .remove_if(&key, |_, current| Arc::ptr_eq(current, &binding));
        }
    }

    async fn rollback(&mut self, primary: SdkError) -> SdkError {
        self.lifecycle.cancel();
        self.remove_exact_routes();
        let failures = match self.packs.take() {
            Some(owner) => owner.close_reverse().await,
            None => Vec::new(),
        };
        self.client
            .inner
            .state
            .reject_prepared_session(&self.thread_id, &self.lifecycle);
        self.committed = true;
        if failures.is_empty() {
            primary
        } else {
            SdkError::Internal(format!(
                "{primary}; ToolPack rollback failures: {}",
                failures.join("; ")
            ))
        }
    }

    fn commit(&mut self) -> Result<(), SdkError> {
        self.client.inner.state.accept_prepared_session(
            &self.thread_id,
            &self.lifecycle,
            &mut self.packs,
        )?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for SessionBuildGuard {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        self.lifecycle.cancel();
        self.remove_exact_routes();
        drop(self.packs.take());
        self.client
            .inner
            .state
            .reject_prepared_session(&self.thread_id, &self.lifecycle);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    };
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use whale_protocol::CanonicalToolOutput;

    struct MutableTool {
        schema: Mutex<Value>,
        approval: AtomicBool,
        parallel: AtomicBool,
    }
    impl MutableTool {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                schema: Mutex::new(json!({"type":"object","description":"captured"})),
                approval: AtomicBool::new(true),
                parallel: AtomicBool::new(false),
            })
        }
    }
    #[async_trait::async_trait]
    impl HostTool for MutableTool {
        fn name(&self) -> &str {
            "lookup"
        }
        fn description(&self) -> &str {
            "business lookup"
        }
        fn parameters(&self) -> Value {
            self.schema.lock().unwrap().clone()
        }
        fn require_approval(&self) -> bool {
            self.approval.load(Ordering::SeqCst)
        }
        fn supports_parallel(&self) -> bool {
            self.parallel.load(Ordering::SeqCst)
        }
        async fn execute(&self, args: Value) -> Result<CanonicalToolOutput, String> {
            Ok(CanonicalToolOutput::structured(
                json!({"args":args,"live_approval":self.approval.load(Ordering::SeqCst)}),
            ))
        }
    }
    struct DropPolicy(Arc<AtomicBool>);
    impl Drop for DropPolicy {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    #[async_trait::async_trait]
    impl HostContextPolicy for DropPolicy {
        async fn build(
            &self,
            request: crate::ContextBuildRequest,
            _: crate::CancellationSignal,
        ) -> Result<crate::ModelContext, String> {
            Ok(crate::ModelContext {
                system_prompt: request.system_prompt,
                items: request.history,
            })
        }
    }
    fn definition() -> AgentDefinition {
        let mut definition = AgentDefinition::new("business", "test-model");
        definition.tool_names = vec!["lookup".into()];
        definition.provider_config = Some(ProviderConfig {
            api: ProviderApi::OpenaiChatCompletions,
            base_url: None,
            auth: Some(ProviderAuth::None),
        });
        definition
    }

    #[tokio::test]
    async fn delivery_guard_returns_a_session_until_the_public_waiter_claims_it() {
        let client =
            WhaleClient::in_process(Arc::new(whale_daemon::DaemonServer::default_server()));
        let thread = WhaleThread {
            recovery_key: None,
            client: client.clone(),
            thread_id: "returned-session".into(),
            max_steps: 10,
            timeout_ms: None,
        };
        let (return_to_owner, returned) = tokio::sync::oneshot::channel();
        drop(SessionDelivery::new(thread, return_to_owner));
        assert_eq!(returned.await.unwrap().id(), "returned-session");

        let claimed = WhaleThread {
            recovery_key: None,
            client: client.clone(),
            thread_id: "claimed-session".into(),
            max_steps: 10,
            timeout_ms: None,
        };
        let (return_to_owner, returned) = tokio::sync::oneshot::channel();
        assert_eq!(
            SessionDelivery::new(claimed, return_to_owner).claim().id(),
            "claimed-session"
        );
        assert!(returned.await.is_err());
        client.close().await;
    }

    #[tokio::test]
    async fn abandoned_session_creation_settles_then_removes_host_bindings() {
        let (connection, peer) = tokio::io::duplex(8192);
        let (read, write) = tokio::io::split(connection);
        let client = WhaleClient::with_io(read, crate::ManagedWriter::io(write), None);
        let policy_dropped = Arc::new(AtomicBool::new(false));
        let agent = client
            .agent(definition(), vec![MutableTool::new()])
            .unwrap()
            .with_context_policy(Arc::new(DropPolicy(policy_dropped.clone())));
        let creation = tokio::spawn(async move { agent.create_session().await });
        let mut reader = BufReader::new(peer);
        let mut line = String::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            reader.read_line(&mut line),
        )
        .await
        .unwrap()
        .unwrap();
        let request: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(
            request["method"],
            whale_protocol::initialization::METHOD_INITIALIZE
        );
        let reply = json!({"jsonrpc":"2.0","id":request["id"],"result":crate::initialization_tests::valid_reply(&request)});
        reader
            .get_mut()
            .write_all(format!("{reply}\n").as_bytes())
            .await
            .unwrap();
        line.clear();
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            reader.read_line(&mut line),
        )
        .await
        .unwrap()
        .unwrap();
        let start: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(start["method"], "session.start_thread");
        let session_id = start["params"]["session_id"].as_str().unwrap().to_owned();
        assert_eq!(client.inner.state.tools.len(), 1);
        assert_eq!(client.inner.state.tool_bindings.len(), 1);
        assert_eq!(client.inner.state.context_policies.len(), 1);
        assert!(!policy_dropped.load(Ordering::SeqCst));
        creation.abort();
        assert!(matches!(creation.await, Err(error) if error.is_cancelled()));
        assert_eq!(client.inner.state.tools.len(), 1);
        let reply = json!({
            "jsonrpc": "2.0",
            "id": start["id"],
            "result": {"thread_id": session_id, "created_at": "fixture"}
        });
        reader
            .get_mut()
            .write_all(format!("{reply}\n").as_bytes())
            .await
            .unwrap();
        line.clear();
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            reader.read_line(&mut line),
        )
        .await
        .unwrap()
        .unwrap();
        let close: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(
            close["method"],
            whale_protocol::sessions::METHOD_SESSION_CLOSE
        );
        let reply = json!({
            "jsonrpc": "2.0",
            "id": close["id"],
            "result": {"thread_id": session_id, "closed": true}
        });
        reader
            .get_mut()
            .write_all(format!("{reply}\n").as_bytes())
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !client.inner.state.tools.is_empty()
                || !client.inner.state.tool_bindings.is_empty()
                || !client.inner.state.context_policies.is_empty()
                || !policy_dropped.load(Ordering::SeqCst)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owned creation cleanup did not finish");
        assert_eq!(
            client.inner.state.tools.len(),
            0,
            "abandoned create_session leaked host bindings"
        );
        assert!(client.inner.state.pending.is_empty());
        assert!(client.inner.state.tool_bindings.is_empty());
        assert!(client.inner.state.context_policies.is_empty());
        assert!(policy_dropped.load(Ordering::SeqCst));
        client.close().await;
    }

    #[tokio::test]
    async fn agent_captures_tool_metadata_for_every_later_session() {
        let server = Arc::new(whale_daemon::DaemonServer::default_server());
        let client = WhaleClient::in_process(server.clone());
        let tool = MutableTool::new();
        let original = tool.parameters();
        let agent = client.agent(definition(), vec![tool.clone()]).unwrap();
        let first = agent.create_session().await.unwrap();
        *tool.schema.lock().unwrap() = json!({"type":"string","description":"mutated"});
        tool.approval.store(false, Ordering::SeqCst);
        tool.parallel.store(true, Ordering::SeqCst);
        let second = agent.create_session().await.unwrap();
        for thread in [&first, &second] {
            let session = server.sessions().get(thread.id()).unwrap().value().clone();
            let session = session.lock().await;
            let captured = session.tools().get("lookup").unwrap();
            assert_eq!(
                captured.parameters(),
                original,
                "mutable metadata changed an existing Agent"
            );
            assert!(captured.require_approval());
            assert!(!captured.supports_parallel());
            let binding = client
                .inner
                .state
                .tools
                .get(&(thread.id().into(), "lookup".into()))
                .unwrap()
                .value()
                .clone();
            assert_eq!(binding.parameters(), original);
            assert_eq!(
                binding
                    .execute(json!({"live":"business execution"}))
                    .await
                    .unwrap(),
                CanonicalToolOutput::structured(
                    json!({"args":{"live":"business execution"},"live_approval":false})
                )
            );
        }
        client.close().await;
    }
}
