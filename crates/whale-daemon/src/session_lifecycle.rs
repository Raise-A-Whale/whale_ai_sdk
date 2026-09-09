//! Atomic session publication and explicit close, independent of session execution locks.
//!
//! A closed session retains only its owner/ID tombstone until that connection is
//! cleaned up. This prevents stale handles from attaching to a reused identity.
use super::{DaemonServer, RunRecord};
use crate::transport::AnyTransportWriter;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex as StdMutex, MutexGuard},
};
use tokio::sync::watch;
use whale_protocol::{
    rpc::{JSONRPCError, JSONRPCResponse, RequestId},
    sessions::{CloseSessionParams, CloseSessionResult},
};

type CloseOutcome = Result<(), JSONRPCError>;
#[derive(Clone)]
pub(super) struct CloseCompletion(watch::Sender<Option<CloseOutcome>>);
impl CloseCompletion {
    fn new() -> Self {
        Self(watch::channel(None).0)
    }
    async fn wait(&self) -> CloseOutcome {
        let mut state = self.0.subscribe();
        loop {
            if let Some(outcome) = state.borrow_and_update().clone() {
                return outcome;
            }
            if state.changed().await.is_err() {
                return Err(JSONRPCError::internal_error(
                    "Session close task ended unexpectedly",
                ));
            }
        }
    }
}
struct SessionOwner {
    owner: String,
    phase: SessionPhase,
}
enum SessionPhase {
    Preparing(CloseCompletion, bool),
    Open,
    Closing(CloseCompletion),
    Closed,
}
#[derive(Default)]
pub(super) struct LifecycleState {
    sessions: HashMap<String, SessionOwner>,
    disconnected: HashSet<String>,
}
#[derive(Default)]
pub(super) struct SessionLifecycle {
    state: StdMutex<LifecycleState>,
}
impl SessionLifecycle {
    pub(super) fn connection_closed(&self, owner: &str) -> bool {
        self.state.lock().unwrap().disconnected.contains(owner)
    }
    pub(super) fn is_foreign(&self, thread: &str, owner: &str) -> bool {
        self.state
            .lock()
            .unwrap()
            .sessions
            .get(thread)
            .is_some_and(|session| session.owner != owner)
    }
    /// Held only around non-awaiting publication; never while taking a session lock.
    pub(super) fn guard_open(
        &self,
        thread: &str,
        owner: &str,
    ) -> Result<MutexGuard<'_, LifecycleState>, String> {
        let state = self.state.lock().unwrap();
        if state.disconnected.contains(owner) {
            return Err("ConnectionClosed".into());
        }
        match state.sessions.get(thread) {
            Some(session) if session.owner == owner => match session.phase {
                SessionPhase::Open => Ok(state),
                _ => Err("SessionClosed".into()),
            },
            _ => Err("SessionNotFound".into()),
        }
    }
    pub(super) fn publish(
        &self,
        thread: &str,
        owner: &str,
        publish: impl FnOnce(),
    ) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        if state.disconnected.contains(owner) {
            return Err("ConnectionClosed".into());
        }
        if state.sessions.contains_key(thread) {
            return Err("SessionAlreadyExists".into());
        }
        state.sessions.insert(
            thread.into(),
            SessionOwner {
                owner: owner.into(),
                phase: SessionPhase::Open,
            },
        );
        publish();
        Ok(())
    }
    fn begin_close(
        &self,
        thread: &str,
        owner: &str,
    ) -> Result<(bool, Option<CloseCompletion>), String> {
        let mut state = self.state.lock().unwrap();
        let Some(session) = state.sessions.get_mut(thread) else {
            return Ok((false, None));
        };
        if session.owner != owner {
            return Err("SessionNotFound".into());
        }
        Ok(Self::mark_closing(session))
    }
    fn mark_closing(session: &mut SessionOwner) -> (bool, Option<CloseCompletion>) {
        match &session.phase {
            SessionPhase::Preparing(completion, _) => {
                let completion = completion.clone();
                session.phase = SessionPhase::Preparing(completion.clone(), true);
                (false, Some(completion))
            }
            SessionPhase::Open => {
                let completion = CloseCompletion::new();
                session.phase = SessionPhase::Closing(completion.clone());
                (true, Some(completion))
            }
            SessionPhase::Closing(completion) => (false, Some(completion.clone())),
            SessionPhase::Closed => (false, None),
        }
    }
    fn begin_disconnect(&self, owner: &str) -> Vec<(String, bool, Option<CloseCompletion>)> {
        let mut state = self.state.lock().unwrap();
        state.disconnected.insert(owner.into());
        state
            .sessions
            .iter_mut()
            .filter(|(_, session)| session.owner == owner)
            .map(|(id, session)| {
                let (first, completion) = Self::mark_closing(session);
                (id.clone(), first, completion)
            })
            .collect()
    }
    fn complete(&self, thread: &str, owner: &str) {
        if let Some(session) = self.state.lock().unwrap().sessions.get_mut(thread) {
            if session.owner == owner {
                session.phase = SessionPhase::Closed;
            }
        }
    }
    fn forget_owner(&self, owner: &str) {
        self.state
            .lock()
            .unwrap()
            .sessions
            .retain(|_, session| session.owner != owner);
        // Retain the small disconnected-owner set to reject already spawned RPC tasks.
    }
}

/// Reserves a live identity without holding a synchronous lock over storage I/O.
/// The owning recovery task outlives its RPC waiter; close/EOF join this completion.
pub(super) struct Preparation {
    lifecycle: Arc<SessionLifecycle>,
    thread: String,
    owner: String,
    completion: CloseCompletion,
    finished: bool,
}
impl SessionLifecycle {
    pub(super) fn reserve(
        self: &Arc<Self>,
        thread: &str,
        owner: &str,
    ) -> Result<Preparation, String> {
        let mut state = self.state.lock().unwrap();
        if state.disconnected.contains(owner) {
            return Err("ConnectionClosed".into());
        }
        if state.sessions.contains_key(thread) {
            return Err("SessionAlreadyExists".into());
        }
        let completion = CloseCompletion::new();
        state.sessions.insert(
            thread.into(),
            SessionOwner {
                owner: owner.into(),
                phase: SessionPhase::Preparing(completion.clone(), false),
            },
        );
        Ok(Preparation {
            lifecycle: self.clone(),
            thread: thread.into(),
            owner: owner.into(),
            completion,
            finished: false,
        })
    }
}
impl Preparation {
    pub(super) fn fail(&mut self, error: JSONRPCError) {
        self.lifecycle.complete(&self.thread, &self.owner);
        self.completion.0.send_replace(Some(Err(error)));
        self.finished = true;
    }
    pub(super) fn publish(&mut self, publish: impl FnOnce()) -> Result<(), String> {
        let mut state = self.lifecycle.state.lock().unwrap();
        if state.disconnected.contains(&self.owner) {
            return Err("ConnectionClosed".into());
        }
        let session = state
            .sessions
            .get_mut(&self.thread)
            .ok_or("SessionClosed")?;
        if session.owner != self.owner
            || !matches!(session.phase, SessionPhase::Preparing(_, false))
        {
            return Err("SessionClosed".into());
        }
        publish();
        session.phase = SessionPhase::Open;
        self.finished = true;
        self.completion.0.send_replace(Some(Ok(())));
        Ok(())
    }
}
impl Drop for Preparation {
    fn drop(&mut self) {
        if !self.finished {
            self.lifecycle.complete(&self.thread, &self.owner);
            // The preparation owner has already joined any necessary durable cleanup.
            self.completion.0.send_replace(Some(Ok(())));
        }
    }
}

impl DaemonServer {
    pub(super) async fn handle_close_session(
        &self,
        id: RequestId,
        params: Option<Value>,
        transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let params: CloseSessionParams = match serde_json::from_value(params.unwrap_or(Value::Null))
        {
            Ok(params) => params,
            Err(error) => {
                return JSONRPCResponse::error(id, JSONRPCError::invalid_params(error.to_string()))
            }
        };
        if let Err(error) = params.validate() {
            return JSONRPCResponse::error(id, JSONRPCError::invalid_params(error));
        }
        let (first, completion) = match self
            .lifecycle
            .begin_close(&params.thread_id, transport.connection_id())
        {
            Ok(action) => action,
            Err(error) => return JSONRPCResponse::error(id, JSONRPCError::invalid_params(error)),
        };
        if let Some(completion) = completion {
            if first {
                self.spawn_session_close(
                    params.thread_id.clone(),
                    transport.clone(),
                    completion.clone(),
                );
            }
            if let Err(error) = completion.wait().await {
                return JSONRPCResponse::error(id, error);
            }
        }
        JSONRPCResponse::success(
            id,
            CloseSessionResult {
                thread_id: params.thread_id,
                closed: first,
            },
        )
        .unwrap()
    }
    fn spawn_session_close(
        &self,
        thread: String,
        transport: AnyTransportWriter,
        completion: CloseCompletion,
    ) {
        let server = self.clone();
        // Keep cleanup alive if the requesting client abandons its close future.
        tokio::spawn(async move {
            let outcome = server.finish_session_close(&thread, &transport).await;
            let storage_failed = outcome
                .as_ref()
                .err()
                .is_some_and(|error| error.code == whale_protocol::recovery::STORE_FAILED);
            completion.0.send_replace(Some(outcome));
            if storage_failed
                && !server
                    .lifecycle
                    .connection_closed(transport.connection_id())
            {
                let cleanup = server.clone();
                let transport = transport.clone();
                tokio::spawn(async move {
                    cleanup.disconnect_connection(&transport).await;
                });
            }
        });
    }
    async fn finish_session_close(
        &self,
        thread: &str,
        transport: &AnyTransportWriter,
    ) -> CloseOutcome {
        self.session_management
            .begin_closing(transport.connection_id(), thread)
            .await
            .map_err(super::session_management::rpc_error)?;
        let interaction_cause = if self.lifecycle.connection_closed(transport.connection_id()) {
            whale_protocol::interactions::INTERACTION_REMOVAL_CONNECTION_CLOSED
        } else {
            whale_protocol::interactions::INTERACTION_REMOVAL_SESSION_CLOSED
        };
        self.interactions
            .close_session(transport.connection_id(), thread, interaction_cause);
        let runs: Vec<Arc<RunRecord>> = self
            .runs
            .lock()
            .await
            .values()
            .filter(|run| run.owner == transport.connection_id() && run.params.thread_id == thread)
            .cloned()
            .collect();
        for run in &runs {
            run.request_cancel().await;
            if self.lifecycle.connection_closed(transport.connection_id()) {
                run.acceptance.send_if_modified(|state| {
                    if state.is_none() {
                        *state = Some(false);
                        true
                    } else {
                        false
                    }
                });
            }
        }
        let mut outcome = Ok(());
        for run in &runs {
            let mut delivery = run.terminal_delivery.subscribe();
            loop {
                if let Some(result) = delivery.borrow_and_update().clone() {
                    if result.is_err() {
                        outcome = result.map_err(JSONRPCError::internal_error);
                    }
                    break;
                }
                if delivery.changed().await.is_err() {
                    outcome = Err(JSONRPCError::internal_error(
                        "Run ended before terminal delivery",
                    ));
                    break;
                }
            }
        }
        // Join an already accepted registration before detaching its journal.
        let session = self.sessions.get(thread).map(|entry| entry.value().clone());
        let session_guard = match &session {
            Some(session) => Some(session.lock().await),
            None => None,
        };
        drop(session_guard);
        // Execution and its pending-call guards finish before terminal_delivery.
        // Detach retains durable history/runs; forgetting is a separate authenticated RPC.
        let journal = self
            .persistent_sessions
            .get(thread)
            .map(|entry| entry.value().clone());
        let management_closed = match self
            .session_management
            .complete_closed(transport.connection_id(), thread, journal.clone())
            .await
        {
            Ok(()) => true,
            Err(error) => {
                outcome = Err(super::session_management::rpc_error(error));
                false
            }
        };
        if management_closed {
            self.persistent_sessions.remove(thread);
        }
        self.sessions.remove(thread);
        self.runs
            .lock()
            .await
            .remove_session(thread, transport.connection_id());
        self.session_views.remove(thread, transport.connection_id());
        if management_closed {
            self.lifecycle.complete(thread, transport.connection_id());
        }
        outcome
    }
    /// Cancels and joins every owned session, then drops per-session tombstones.
    pub async fn disconnect_connection(&self, transport: &AnyTransportWriter) {
        let owner = transport.connection_id();
        self.initializations.close(owner);
        self.session_management.mark_owner_disconnected(owner);
        let sessions = self.lifecycle.begin_disconnect(owner);
        // EOF owns the whole connection boundary. Clear and close each
        // Interaction feed before Run cancellation so any observable Removed
        // event carries the connection-scoped cause and precedes terminal
        // projection. `finish_session_close` repeats this idempotently.
        for (thread, _, _) in &sessions {
            self.interactions.close_session(
                owner,
                thread,
                whale_protocol::interactions::INTERACTION_REMOVAL_CONNECTION_CLOSED,
            );
        }
        // A close may already be waiting on an ACK whose writer is now gone.
        // Resolve every owned acceptance gate, including jobs we did not start.
        let runs: Vec<_> = self
            .runs
            .lock()
            .await
            .values()
            .filter(|run| run.owner == owner)
            .cloned()
            .collect();
        for run in runs {
            run.acceptance.send_if_modified(|state| {
                if state.is_none() {
                    *state = Some(false);
                    true
                } else {
                    false
                }
            });
            run.request_cancel().await;
        }
        for (thread, first, completion) in &sessions {
            if *first {
                self.spawn_session_close(
                    thread.clone(),
                    transport.clone(),
                    completion.clone().unwrap(),
                );
            }
        }
        let owned_threads: Vec<_> = sessions
            .iter()
            .map(|(thread, _, _)| thread.clone())
            .collect();
        for (_, _, completion) in sessions {
            if let Some(completion) = completion {
                let _ = completion.wait().await;
            }
        }
        for thread in owned_threads {
            if let Some((_, journal)) = self.persistent_sessions.remove(&thread) {
                let _ = journal.detach().await;
            }
            self.sessions.remove(&thread);
            self.runs.lock().await.remove_session(&thread, owner);
            self.session_views.remove(&thread, owner);
            self.interactions.close_session(
                owner,
                &thread,
                whale_protocol::interactions::INTERACTION_REMOVAL_CONNECTION_CLOSED,
            );
        }
        self.pending_host_tool_calls
            .retain(|id, _| !id.starts_with(&format!("{owner}:")));
        self.active_invocations.retain(|id, context| {
            if id.starts_with(&format!("{owner}:")) {
                context.finish();
                false
            } else {
                true
            }
        });
        self.pending_contexts
            .retain(|id, _| !id.starts_with(&format!("context_{owner}:")));
        self.session_management.remove_owner(owner).await;
        self.lifecycle.forget_owner(owner);
    }
}
