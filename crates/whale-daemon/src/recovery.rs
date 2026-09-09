//! Durable attachments use fresh live identities; archived runs stay in recovery inspection.
use super::*;
use whale_protocol::recovery::*;
use whale_store::{PersistedSessionConfigurationV2, StoreError, StoreRuntime};

pub(super) fn is_recovery_method(method: &str) -> bool {
    matches!(
        method,
        METHOD_SESSION_CREATE_PERSISTENT
            | METHOD_RECOVERY_ATTACH
            | METHOD_RECOVERY_INSPECT
            | METHOD_RECOVERY_ACKNOWLEDGE
            | METHOD_RECOVERY_FORGET
    )
}
pub(super) fn configuration(
    session: &StartThreadParams,
    defaults: &SessionRunDefaults,
    interactions_enabled: bool,
) -> Result<Value, String> {
    let metadata = session.metadata.clone();
    let mut session = serde_json::to_value(session).map_err(|e| e.to_string())?;
    session.as_object_mut().unwrap().remove("session_id");
    session.as_object_mut().unwrap().remove("metadata");
    session
        .as_object_mut()
        .unwrap()
        .entry("tools")
        .or_insert_with(|| json!([]));
    if let Some(tools) = session.get_mut("tools").and_then(Value::as_array_mut) {
        let mut names = std::collections::HashSet::new();
        for tool in tools.iter_mut() {
            tool.as_object_mut().unwrap().remove("binding_id");
            let name = tool["name"].as_str().ok_or("Missing tool name")?;
            if name.trim().is_empty() || !names.insert(name.to_owned()) {
                return Err("Tool names must be nonempty and unique".into());
            }
        }
        tools.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    }
    let value = json!({
        "version": 3,
        "session": session,
        "run_defaults": defaults,
        "metadata": metadata,
        "runtime_features": {"interactions_enabled": interactions_enabled},
    });
    PersistedSessionConfigurationV2::parse_and_migrate(value)
        .map(|(configuration, _)| configuration.into_value())
        .map_err(|error| error.to_string())
}
pub(super) fn store_error(error: StoreError) -> JSONRPCError {
    match error {
        StoreError::LimitExceeded(message) => JSONRPCError::new(
            whale_protocol::retention::SESSION_LIMIT_EXCEEDED,
            message,
            None,
        ),
        StoreError::Io(_) | StoreError::Poisoned(_) => JSONRPCError::new(
            STORE_FAILED,
            "Session storage commit failed; close and recover this attachment",
            None,
        ),
        other => JSONRPCError::new(RECOVERY_REJECTED, other.to_string(), None),
    }
}

/// Cancellation of the transport handler must not leave a successfully committed
/// but unacknowledged attachment alive. The recovery transaction itself is owned.
pub(super) struct RecoveryReplyGuard {
    server: DaemonServer,
    transport: AnyTransportWriter,
    delivered: bool,
}
impl RecoveryReplyGuard {
    pub(super) fn new(server: DaemonServer, transport: AnyTransportWriter) -> Self {
        Self {
            server,
            transport,
            delivered: false,
        }
    }
    pub(super) fn delivered(&mut self) {
        self.delivered = true;
    }
}
impl Drop for RecoveryReplyGuard {
    fn drop(&mut self) {
        if !self.delivered {
            let server = self.server.clone();
            let transport = self.transport.clone();
            tokio::spawn(async move {
                server.disconnect_connection(&transport).await;
            });
        }
    }
}

impl DaemonServer {
    /// Install a StoreRuntime only after its asynchronous startup recovery succeeded.
    pub fn with_store_runtime(mut self, runtime: Arc<StoreRuntime>) -> Self {
        self.store = Some(runtime);
        // Preserve builder ordering: an otherwise-default server may install its
        // Store before selecting a retention policy. Ordinary request handling
        // starts the always-on tombstone maintenance worker when no Run/Store
        // retention policy has been selected yet.
        if self.retention.policy.is_enabled() {
            self.retention.start(
                &self.runs,
                &self.session_views,
                &self.session_management,
                &self.interactions,
                self.store.as_ref(),
            );
        }
        self
    }

    pub(super) async fn handle_recovery(
        &self,
        req: JSONRPCRequest,
        transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let id = req.id.clone();
        let Some(store) = self.store.clone() else {
            return JSONRPCResponse::error(
                id,
                JSONRPCError::new(
                    RECOVERY_UNAVAILABLE,
                    "Session recovery is not configured",
                    None,
                ),
            );
        };
        let server = self.clone();
        let transport = transport.clone();
        // Every accepted mutation/cleanup proceeds even if an individual RPC waiter disappears.
        match tokio::spawn(async move { server.recovery_transaction(req, &transport, store).await })
            .await
        {
            Ok(response) => response,
            Err(_) => JSONRPCResponse::error(
                id,
                JSONRPCError::new(
                    STORE_FAILED,
                    "Session recovery transaction did not complete",
                    None,
                ),
            ),
        }
    }
    async fn recovery_transaction(
        &self,
        req: JSONRPCRequest,
        transport: &AnyTransportWriter,
        store: Arc<StoreRuntime>,
    ) -> JSONRPCResponse {
        let id = req.id;
        let value = req.params.unwrap_or(Value::Null);
        macro_rules! parse {
            ($kind:ty) => {{
                let params: $kind = match serde_json::from_value(value) {
                    Ok(params) => params,
                    Err(_) => {
                        return JSONRPCResponse::error(
                            id,
                            JSONRPCError::invalid_params("Invalid recovery request"),
                        )
                    }
                };
                if let Err(error) = params.validate() {
                    return JSONRPCResponse::error(id, JSONRPCError::invalid_params(error));
                }
                params
            }};
        }
        match req.method.as_str() {
            METHOD_SESSION_CREATE_PERSISTENT => {
                let (p, interactions_enabled) = if value.get("interactions_enabled").is_some() {
                    let wrapper = parse!(
                        whale_protocol::interactions::CreatePersistentSessionWithInteractionsParams
                    );
                    (wrapper.persistent, wrapper.interactions_enabled)
                } else {
                    (parse!(CreatePersistentSessionParams), false)
                };
                self.create_or_attach(
                    id,
                    transport,
                    store,
                    p.key,
                    p.session,
                    p.run_defaults,
                    None,
                    interactions_enabled,
                )
                .await
            }
            METHOD_RECOVERY_ATTACH => {
                let (p, interactions_enabled) = if value.get("interactions_enabled").is_some() {
                    let wrapper =
                        parse!(whale_protocol::interactions::AttachRecoveryWithInteractionsParams);
                    (wrapper.recovery, wrapper.interactions_enabled)
                } else {
                    (parse!(AttachRecoveryParams), false)
                };
                self.create_or_attach(
                    id,
                    transport,
                    store,
                    p.key,
                    p.session,
                    p.run_defaults,
                    Some(p.expected_revision),
                    interactions_enabled,
                )
                .await
            }
            METHOD_RECOVERY_INSPECT => {
                let p = parse!(InspectRecoveryParams);
                match store.inspect(&p.key).await {
                    Ok(snapshot) => JSONRPCResponse::success(id, snapshot).unwrap(),
                    Err(error) => JSONRPCResponse::error(id, store_error(error)),
                }
            }
            METHOD_RECOVERY_FORGET => {
                let p = parse!(ForgetRecoveryParams);
                match store.forget(&p.key, p.expected_revision).await {
                    Ok(forgotten) => JSONRPCResponse::success(
                        id,
                        ForgetRecoveryResult {
                            recovery_id: p.key.recovery_id,
                            forgotten,
                        },
                    )
                    .unwrap(),
                    Err(error) => JSONRPCResponse::error(id, store_error(error)),
                }
            }
            METHOD_RECOVERY_ACKNOWLEDGE => {
                let p = parse!(AcknowledgeUnknownParams);
                if let Err(error) = store.inspect(&p.key).await {
                    return JSONRPCResponse::error(id, store_error(error));
                }
                let attachment = self
                    .persistent_sessions
                    .iter()
                    .find(|entry| entry.value().recovery_id() == p.key.recovery_id)
                    .map(|entry| (entry.key().clone(), entry.value().clone()));
                let Some((thread, journal)) = attachment else {
                    return JSONRPCResponse::error(
                        id,
                        JSONRPCError::new(
                            RECOVERY_REJECTED,
                            "Recovery is not attached to this connection",
                            None,
                        ),
                    );
                };
                if self
                    .lifecycle
                    .guard_open(&thread, transport.connection_id())
                    .is_err()
                {
                    return JSONRPCResponse::error(
                        id,
                        JSONRPCError::new(
                            RECOVERY_REJECTED,
                            "Recovery is not attached to this connection",
                            None,
                        ),
                    );
                }
                match journal
                    .acknowledge(p.expected_revision, p.execution_ids)
                    .await
                {
                    Ok(snapshot) => JSONRPCResponse::success(id, snapshot).unwrap(),
                    Err(error) => JSONRPCResponse::error(id, store_error(error)),
                }
            }
            _ => unreachable!(),
        }
    }
    async fn create_or_attach(
        &self,
        id: RequestId,
        transport: &AnyTransportWriter,
        store: Arc<StoreRuntime>,
        key: RecoveryKey,
        params: StartThreadParams,
        defaults: SessionRunDefaults,
        revision: Option<u64>,
        interactions_enabled: bool,
    ) -> JSONRPCResponse {
        if params
            .agent_name
            .as_ref()
            .is_some_and(|name| name.trim().is_empty() || name.trim() != name)
        {
            return JSONRPCResponse::error(
                id,
                JSONRPCError::invalid_params("agent_name must be nonempty and unpadded"),
            );
        }
        if revision.is_none() {
            if let Err(error) =
                whale_protocol::session_management::validate_session_metadata_replacement(
                    &params.metadata,
                )
            {
                return JSONRPCResponse::error(
                    id,
                    JSONRPCError::new(
                        whale_protocol::retention::SESSION_LIMIT_EXCEEDED,
                        error,
                        None,
                    ),
                );
            }
        }
        let agent_name = params.agent_name.clone();
        let configuration = match configuration(&params, &defaults, interactions_enabled) {
            Ok(value) => value,
            Err(error) => return JSONRPCResponse::error(id, JSONRPCError::invalid_params(error)),
        };
        let (thread, mut session) = match self.prepare_thread(id.clone(), params, transport) {
            Ok(prepared) => prepared,
            Err(error) => return error,
        };
        let mut reservation = match self.lifecycle.reserve(&thread, transport.connection_id()) {
            Ok(reservation) => reservation,
            Err(error) => {
                return JSONRPCResponse::error(
                    id,
                    JSONRPCError::new(RECOVERY_REJECTED, error, None),
                )
            }
        };
        let journal = match revision {
            Some(revision) => {
                store
                    .attach(
                        &key,
                        revision,
                        configuration,
                        transport.connection_id().into(),
                        thread.clone(),
                    )
                    .await
            }
            None => {
                store
                    .create(
                        key.clone(),
                        configuration,
                        transport.connection_id().into(),
                        thread.clone(),
                    )
                    .await
            }
        };
        let journal = match journal {
            Ok(journal) => journal,
            Err(error) => {
                let error = store_error(error);
                if error.code == STORE_FAILED {
                    reservation.fail(error.clone());
                }
                return JSONRPCResponse::error(id, error);
            }
        };
        let record = match journal.record().await {
            Ok(record) => record,
            Err(error) => {
                let error = store_error(journal.detach().await.err().unwrap_or(error));
                reservation.fail(error.clone());
                return JSONRPCResponse::error(id, error);
            }
        };
        let metadata = match PersistedSessionConfigurationV2::parse_and_migrate(
            record.configuration.clone(),
        ) {
            Ok((configuration, _)) => configuration.metadata().clone(),
            Err(error) => {
                let error = store_error(journal.detach().await.err().unwrap_or(error));
                reservation.fail(error.clone());
                return JSONRPCResponse::error(id, error);
            }
        };
        session.restore_accepted_turns(record.runs.len() as u64);
        let recovered_history = record.history;
        session.replace_history(recovered_history.clone());
        session.set_journal(Some(journal.clone()));
        let max_history_bytes = session.limits().and_then(|limits| limits.max_history_bytes);
        let created_at_ms = super::session_views::unix_ms();
        let persistence = whale_protocol::session_management::SessionPersistenceV2::Persistent {
            recovery_id: key.recovery_id.clone(),
        };
        let result = reservation.publish(|| {
            self.session_views
                .insert(
                    transport.connection_id(),
                    thread.clone(),
                    agent_name.clone(),
                    metadata.clone(),
                    recovered_history.clone(),
                    created_at_ms,
                    transport.clone(),
                )
                .expect("validated recovered Session view");
            self.session_management
                .insert(
                    transport.connection_id(),
                    thread.clone(),
                    agent_name.clone(),
                    metadata.clone(),
                    recovered_history.clone(),
                    created_at_ms,
                    persistence,
                    max_history_bytes,
                    transport.clone(),
                )
                .expect("validated recovered V2 Session view");
            self.interactions
                .insert(
                    transport.connection_id(),
                    thread.clone(),
                    interactions_enabled,
                    transport.clone(),
                )
                .expect("validated recovered Interaction view");
            self.persistent_sessions
                .insert(thread.clone(), journal.clone());
            self.sessions
                .insert(thread.clone(), Arc::new(Mutex::new(session)));
        });
        if let Err(error) = result {
            if let Err(storage) = journal.detach().await {
                let error = store_error(storage);
                reservation.fail(error.clone());
                return JSONRPCResponse::error(id, error);
            }
            return JSONRPCResponse::error(id, JSONRPCError::new(RECOVERY_REJECTED, error, None));
        }
        JSONRPCResponse::success(
            id,
            PersistentSessionResult {
                thread: StartThreadResult {
                    thread_id: thread,
                    created_at: chrono::DateTime::from_timestamp_millis(created_at_ms as i64)
                        .expect("nonnegative current timestamp")
                        .to_rfc3339(),
                },
                key,
                epoch: journal.epoch(),
            },
        )
        .unwrap()
    }
}
