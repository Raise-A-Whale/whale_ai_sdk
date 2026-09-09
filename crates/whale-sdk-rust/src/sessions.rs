//! Connection-local session fences and acknowledged close transactions.
use crate::tool_packs::SessionPackOwner;
use crate::*;
use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex as SyncMutex;
use whale_protocol::sessions::{CloseSessionParams, CloseSessionResult, METHOD_SESSION_CLOSE};

#[derive(Clone)]
enum CloseFailure {
    Rpc(i64, String),
    Unknown(String),
    LocalResource(String),
}
impl CloseFailure {
    fn sdk(self) -> SdkError {
        match self {
            Self::Rpc(code, message) => SdkError::Rpc { code, message },
            Self::Unknown(message) => SdkError::ChannelClosed(message),
            Self::LocalResource(message) => SdkError::Internal(message),
        }
    }
}
type CloseOutcome = Result<bool, CloseFailure>;
struct CloseAttempt(watch::Sender<Option<CloseOutcome>>);
enum SessionPhase {
    Preparing,
    Open,
    Closing(Arc<CloseAttempt>),
    Releasing(Arc<CloseAttempt>),
    Closed,
}
pub(crate) struct SessionLifecycle {
    phase: SyncMutex<SessionPhase>,
    released: CancellationSignal,
    pack_owner: SyncMutex<Option<SessionPackOwner>>,
    pack_names: SyncMutex<HashSet<String>>,
    provisional: AtomicBool,
    provisional_users: AtomicUsize,
}
impl SessionLifecycle {
    fn new() -> Arc<Self> {
        Self::new_with_provisional(false)
    }
    fn new_provisional() -> Arc<Self> {
        Self::new_with_provisional(true)
    }
    fn new_with_provisional(provisional: bool) -> Arc<Self> {
        Arc::new(Self {
            phase: SyncMutex::new(SessionPhase::Open),
            released: CancellationSignal::new(None),
            pack_owner: SyncMutex::new(None),
            pack_names: SyncMutex::new(HashSet::new()),
            provisional: AtomicBool::new(provisional),
            provisional_users: AtomicUsize::new(0),
        })
    }

    pub(crate) fn cancel(&self) {
        self.released.cancel();
    }
}

pub(crate) struct SessionProbe {
    state: Arc<ClientState>,
    sid: String,
    pub(crate) lifecycle: Arc<SessionLifecycle>,
    tracked: bool,
}

impl SessionProbe {
    pub(crate) fn commit(&mut self) {
        self.lifecycle.provisional.store(false, Ordering::SeqCst);
    }
}

impl Drop for SessionProbe {
    fn drop(&mut self) {
        if !self.tracked {
            return;
        }
        let previous = self
            .lifecycle
            .provisional_users
            .fetch_sub(1, Ordering::SeqCst);
        debug_assert!(previous > 0);
        if previous == 1 {
            self.state.sessions.remove_if(&self.sid, |_, current| {
                Arc::ptr_eq(current, &self.lifecycle)
                    && current.provisional.load(Ordering::SeqCst)
                    && current.provisional_users.load(Ordering::SeqCst) == 0
            });
        }
    }
}

impl ClientState {
    pub(crate) fn session_lifecycle_v2(
        &self,
        sid: &str,
    ) -> Option<whale_protocol::session_management::SessionLifecycleState> {
        let lifecycle = self.sessions.get(sid)?.value().clone();
        let state = match &*lifecycle.phase.lock().ok()? {
            SessionPhase::Preparing | SessionPhase::Open => {
                whale_protocol::session_management::SessionLifecycleState::Open
            }
            SessionPhase::Closing(_) | SessionPhase::Releasing(_) => {
                whale_protocol::session_management::SessionLifecycleState::Closing
            }
            SessionPhase::Closed => {
                whale_protocol::session_management::SessionLifecycleState::Closed
            }
        };
        Some(state)
    }

    pub(crate) fn prepare_session(&self, sid: &str) -> Result<Arc<SessionLifecycle>, SdkError> {
        let lifecycle = self.session_lifecycle(sid);
        let mut phase = lifecycle.phase.lock().unwrap();
        if self.closed.load(Ordering::SeqCst) || !matches!(*phase, SessionPhase::Open) {
            return Err(SdkError::SessionClosed(sid.into()));
        }
        self.ensure_session_event_hub(sid);
        *phase = SessionPhase::Preparing;
        drop(phase);
        Ok(lifecycle)
    }
    pub(crate) fn finish_preparation(&self, sid: &str, accepted: bool) -> Result<(), SdkError> {
        let lifecycle = self.session_lifecycle(sid);
        let mut phase = lifecycle.phase.lock().unwrap();
        if self.closed.load(Ordering::SeqCst) || !matches!(*phase, SessionPhase::Preparing) {
            return Err(SdkError::SessionClosed(sid.into()));
        }
        *phase = if accepted {
            SessionPhase::Open
        } else {
            self.close_session_event_hub(sid);
            SessionPhase::Closed
        };
        Ok(())
    }
    pub(crate) fn accept_prepared_session(
        &self,
        sid: &str,
        lifecycle: &Arc<SessionLifecycle>,
        packs: &mut Option<SessionPackOwner>,
    ) -> Result<(), SdkError> {
        let exact = self
            .sessions
            .get(sid)
            .is_some_and(|current| Arc::ptr_eq(current.value(), lifecycle));
        if !exact {
            return Err(SdkError::SessionClosed(sid.into()));
        }
        let mut phase = lifecycle.phase.lock().unwrap();
        if self.closed.load(Ordering::SeqCst) || !matches!(*phase, SessionPhase::Preparing) {
            return Err(SdkError::SessionClosed(sid.into()));
        }
        let mut owner = lifecycle.pack_owner.lock().unwrap();
        if owner.is_some() {
            return Err(SdkError::Internal(
                "Prepared Session already owns ToolPacks".into(),
            ));
        }
        if let Some(packs) = packs.as_ref() {
            *lifecycle.pack_names.lock().unwrap() = packs.owned_names().clone();
        }
        *owner = packs.take();
        *phase = SessionPhase::Open;
        Ok(())
    }
    pub(crate) fn reject_prepared_session(&self, sid: &str, lifecycle: &Arc<SessionLifecycle>) {
        lifecycle.released.cancel();
        if let Ok(mut phase) = lifecycle.phase.lock() {
            if matches!(*phase, SessionPhase::Preparing) {
                *phase = SessionPhase::Closed;
            }
        }
        self.close_session_event_hub(sid);
        self.sessions
            .remove_if(sid, |_, current| Arc::ptr_eq(current, lifecycle));
    }
    pub(crate) fn take_attached_pack_owner(&self, sid: &str) -> Option<SessionPackOwner> {
        let lifecycle = self.sessions.get(sid)?.value().clone();
        let owner = lifecycle.pack_owner.lock().unwrap().take();
        owner
    }
    fn take_lifecycle_pack_owner(
        &self,
        lifecycle: &Arc<SessionLifecycle>,
    ) -> Option<SessionPackOwner> {
        lifecycle.pack_owner.lock().unwrap().take()
    }
    pub(crate) fn emergency_drain_session_packs(&self) {
        let lifecycles: Vec<_> = self
            .sessions
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        let mut owners = Vec::new();
        for lifecycle in lifecycles {
            lifecycle.released.cancel();
            if let Ok(mut phase) = lifecycle.phase.lock() {
                *phase = SessionPhase::Closed;
            }
            if let Some(owner) = self.take_lifecycle_pack_owner(&lifecycle) {
                owners.push(owner);
            }
        }
        drop(owners);
    }
    #[cfg(test)]
    pub(crate) fn attached_pack_count(&self, sid: &str) -> usize {
        self.sessions
            .get(sid)
            .and_then(|lifecycle| {
                lifecycle
                    .pack_owner
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(SessionPackOwner::len)
            })
            .unwrap_or(0)
    }
    fn session_lifecycle(&self, sid: &str) -> Arc<SessionLifecycle> {
        let lifecycle = self
            .sessions
            .entry(sid.into())
            .or_insert_with(SessionLifecycle::new)
            .clone();
        lifecycle.provisional.store(false, Ordering::SeqCst);
        lifecycle
    }
    pub(crate) fn probe_session(self: &Arc<Self>, sid: &str) -> SessionProbe {
        let (lifecycle, tracked) = match self.sessions.entry(sid.into()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                let lifecycle = entry.get().clone();
                let tracked = lifecycle.provisional.load(Ordering::SeqCst);
                if tracked {
                    lifecycle.provisional_users.fetch_add(1, Ordering::SeqCst);
                }
                (lifecycle, tracked)
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let lifecycle = SessionLifecycle::new_provisional();
                lifecycle.provisional_users.store(1, Ordering::SeqCst);
                entry.insert(lifecycle.clone());
                (lifecycle, true)
            }
        };
        SessionProbe {
            state: self.clone(),
            sid: sid.into(),
            lifecycle,
            tracked,
        }
    }
    pub(crate) fn with_session_open<T>(
        &self,
        sid: &str,
        f: impl FnOnce() -> T,
    ) -> Result<T, SdkError> {
        let lifecycle = self.session_lifecycle(sid);
        self.with_open_lifecycle(sid, lifecycle, f)
    }
    pub(crate) fn with_existing_session_open<T>(
        &self,
        sid: &str,
        f: impl FnOnce() -> T,
    ) -> Result<T, SdkError> {
        let lifecycle = self
            .sessions
            .get(sid)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| SdkError::SessionClosed(sid.into()))?;
        self.with_open_lifecycle(sid, lifecycle, f)
    }
    fn with_open_lifecycle<T>(
        &self,
        sid: &str,
        lifecycle: Arc<SessionLifecycle>,
        f: impl FnOnce() -> T,
    ) -> Result<T, SdkError> {
        let phase = lifecycle.phase.lock().unwrap();
        if self.closed.load(Ordering::SeqCst) {
            return Err(SdkError::ChannelClosed("Client is closed".into()));
        }
        if !matches!(*phase, SessionPhase::Open) {
            return Err(SdkError::SessionClosed(sid.into()));
        }
        Ok(f())
    }
    pub(crate) fn session_released(&self, sid: &str) -> CancellationSignal {
        self.sessions
            .get(sid)
            .expect("session was checked before installing its release fence")
            .released
            .clone()
    }
    pub(crate) fn ensure_session_open(&self, sid: &str) -> Result<(), SdkError> {
        let lifecycle = self.session_lifecycle(sid);
        let phase = lifecycle.phase.lock().unwrap();
        if self.closed.load(Ordering::SeqCst) {
            return Err(SdkError::ChannelClosed("Client is closed".into()));
        }
        if !matches!(*phase, SessionPhase::Open) {
            return Err(SdkError::SessionClosed(sid.into()));
        }
        self.ensure_session_event_hub(sid);
        Ok(())
    }
    pub(crate) fn ensure_dynamic_tool_name_available(
        &self,
        sid: &str,
        name: &str,
    ) -> Result<(), SdkError> {
        let lifecycle = self
            .sessions
            .get(sid)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| SdkError::SessionClosed(sid.into()))?;
        let phase = lifecycle.phase.lock().unwrap();
        if self.closed.load(Ordering::SeqCst) {
            return Err(SdkError::ChannelClosed("Client is closed".into()));
        }
        if !matches!(*phase, SessionPhase::Open) {
            return Err(SdkError::SessionClosed(sid.into()));
        }
        if lifecycle.pack_names.lock().unwrap().contains(name) {
            return Err(SdkError::InvalidConfiguration(format!(
                "Tool '{name}' is owned by a ToolPack for Session {sid}"
            )));
        }
        Ok(())
    }
    pub(crate) fn session_closed(&self, sid: &str) -> bool {
        self.sessions.get(sid).is_some_and(|life| {
            matches!(
                *life.phase.lock().unwrap(),
                SessionPhase::Closed | SessionPhase::Releasing(_)
            )
        })
    }
    /// Rollback may finish during a rejected close, but never after successful teardown.
    pub(crate) fn while_session_exists(&self, sid: &str, f: impl FnOnce()) {
        let lifecycle = self.session_lifecycle(sid);
        let phase = lifecycle.phase.lock().unwrap();
        if !matches!(*phase, SessionPhase::Closed | SessionPhase::Releasing(_))
            && !self.closed.load(Ordering::SeqCst)
        {
            f();
        }
    }
    async fn release_session(&self, sid: &str, lifecycle: &Arc<SessionLifecycle>) -> Vec<String> {
        // Closed is committed before the daemon acknowledges close. Force V2
        // subscribers across replay in case the notification is delayed.
        self.nudge_session_event_hub_v2(sid);
        self.close_session_event_hub(sid);
        self.close_interaction_event_hub(sid);
        lifecycle.released.cancel();
        let pending: Vec<_> = self
            .pending_sessions
            .iter()
            .filter(|entry| entry.value() == sid)
            .map(|entry| entry.key().clone())
            .collect();
        for id in pending {
            self.pending_sessions.remove(&id);
            if let Some((_, reply)) = self.pending.remove(&id) {
                let _ = reply.send(Err(crate::InternalRequestError::Sdk(
                    SdkError::SessionClosed(sid.into()),
                )));
            }
        }
        let callbacks: Vec<_> = self
            .tool_callbacks
            .iter()
            .chain(self.context_callbacks.iter())
            .filter(|entry| entry.session_id.as_deref() == Some(sid))
            .map(|entry| entry.value().clone())
            .collect();
        for callback in &callbacks {
            callback.stop();
        }
        // Cancellation stops pending async host futures; guards release the registry.
        for callback in callbacks {
            callback.finished.cancelled().await;
        }
        let registrations: Vec<_> = self
            .registration_locks
            .iter()
            .filter(|entry| entry.key().0 == sid)
            .map(|entry| entry.value().clone())
            .collect();
        // Scoped requests already resolved locally, so these waits only allow
        // registration workers to drop captured handlers.
        for registration in registrations {
            drop(registration.lock().await);
        }
        self.tools.retain(|(thread, _), _| thread != sid);
        self.tool_bindings.retain(|(thread, _), _| thread != sid);
        self.registration_locks
            .retain(|(thread, _), _| thread != sid);
        self.context_policies.remove(sid);
        self.approval_sessions.retain(|_, thread| thread != sid);
        self.approval_turns
            .retain(|request_id, _| self.approval_sessions.contains_key(request_id));
        self.runs.retain(|_, run| {
            if run.thread_id == sid {
                run.fail(&format!("SessionClosed: {sid}"));
                false
            } else {
                true
            }
        });
        self.run_handles
            .lock()
            .unwrap()
            .retain(|_, (thread, _)| thread != sid);
        match self.take_lifecycle_pack_owner(lifecycle) {
            Some(owner) => owner.close_reverse().await,
            None => Vec::new(),
        }
    }
}
impl WhaleClient {
    /// Releases this session without closing other sessions or the client. Once
    /// dispatched, cleanup continues even if the caller stops awaiting it.
    /// Explicit RPC rejection restores the open state; an unknown remote outcome
    /// closes the connection. Already completed RunHandle results remain readable.
    pub async fn close_session(&self, thread_id: impl Into<String>) -> Result<bool, SdkError> {
        let sid = thread_id.into();
        let params = CloseSessionParams {
            thread_id: sid.clone(),
        };
        params.validate().map_err(SdkError::InvalidConfiguration)?;
        self.initialize().await?;
        if self.inner.state.closed.load(Ordering::SeqCst) {
            return Err(SdkError::ChannelClosed("Client is closed".into()));
        }
        let mut session_probe = self.inner.state.probe_session(&sid);
        let lifecycle = session_probe.lifecycle.clone();
        let (attempt, owner) = {
            let mut phase = lifecycle.phase.lock().unwrap();
            match &*phase {
                SessionPhase::Preparing => return Err(SdkError::SessionClosed(sid)),
                SessionPhase::Closed => return Ok(false),
                SessionPhase::Closing(attempt) | SessionPhase::Releasing(attempt) => {
                    (attempt.clone(), false)
                }
                SessionPhase::Open => {
                    let attempt = Arc::new(CloseAttempt(watch::channel(None).0));
                    *phase = SessionPhase::Closing(attempt.clone());
                    (attempt, true)
                }
            }
        };
        if owner {
            let client = self.clone();
            let complete = attempt.clone();
            tokio::spawn(async move {
                let response: Result<CloseSessionResult, SdkError> =
                    client.request(METHOD_SESSION_CLOSE, Some(params)).await;
                let outcome = match response {
                    Ok(result) if result.thread_id == sid => {
                        if result.closed {
                            session_probe.commit();
                        }
                        *lifecycle.phase.lock().unwrap() =
                            SessionPhase::Releasing(complete.clone());
                        let failures = client.inner.state.release_session(&sid, &lifecycle).await;
                        *lifecycle.phase.lock().unwrap() = SessionPhase::Closed;
                        if failures.is_empty() {
                            Ok(result.closed)
                        } else {
                            Err(CloseFailure::LocalResource(format!(
                                "Session {sid} closed with ToolPack cleanup failures: {}",
                                failures.join("; ")
                            )))
                        }
                    }
                    Err(SdkError::Rpc { code, message })
                        if matches!(
                            code,
                            JSONRPCError::METHOD_NOT_FOUND | JSONRPCError::INVALID_PARAMS
                        ) && !client.inner.state.closed.load(Ordering::SeqCst) =>
                    {
                        *lifecycle.phase.lock().unwrap() = SessionPhase::Open;
                        Err(CloseFailure::Rpc(code, message))
                    }
                    other => {
                        *lifecycle.phase.lock().unwrap() = SessionPhase::Closed;
                        let error = match other {
                            Err(error) => error.to_string(),
                            Ok(_) => "Daemon returned a different session identity".into(),
                        };
                        client.close().await;
                        Err(CloseFailure::Unknown(format!(
                            "Session close outcome unknown: {error}"
                        )))
                    }
                };
                drop(session_probe);
                complete.0.send_replace(Some(outcome));
            });
        }
        let mut result = attempt.0.subscribe();
        loop {
            let current = result.borrow_and_update().clone();
            if let Some(result) = current {
                return result
                    .map(|closed| owner && closed)
                    .map_err(CloseFailure::sdk);
            }
            result
                .changed()
                .await
                .map_err(|_| SdkError::ChannelClosed("Session close task stopped".into()))?;
        }
    }
}
