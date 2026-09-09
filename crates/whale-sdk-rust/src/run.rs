//! Independent result and event channels for one accepted turn.

use super::{
    RunEventSubscription, SdkError, SessionCursor, SessionViewError, SubscriptionOptions,
    WhaleClient,
};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex, OnceLock, Weak,
};
use tokio::sync::{mpsc, watch};
use whale_protocol::rpc::{ApprovalResolveResult, RunTurnResult};
use whale_protocol::runs::*;

const EVENT_CAPACITY: usize = 256;

pub(crate) struct RunState {
    pub(crate) owner: OnceLock<Weak<super::ClientState>>,
    pub thread_id: String,
    pub turn_id: String,
    sender: Mutex<Option<mpsc::Sender<RunEvent>>>,
    receiver: Mutex<Option<mpsc::Receiver<RunEvent>>>,
    terminal: watch::Sender<Option<Result<RunSnapshot, String>>>,
    last_seq: AtomicU64,
    event_route_claimed: AtomicBool,
    lagged: AtomicBool,
    legacy_events: Mutex<Option<Vec<RunEvent>>>,
    query: Option<Mutex<QueryOwnership>>,
}

#[derive(Default)]
struct QueryOwnership {
    waiters: usize,
    observed_live: bool,
    observed_terminal: bool,
}

impl RunState {
    pub fn new(thread_id: String, turn_id: String) -> Arc<Self> {
        Self::new_inner(thread_id, turn_id, false)
    }

    pub fn new_query(thread_id: String, turn_id: String) -> Arc<Self> {
        Self::new_inner(thread_id, turn_id, true)
    }

    fn new_inner(thread_id: String, turn_id: String, query: bool) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(EVENT_CAPACITY);
        let (terminal, _) = watch::channel(None);
        Arc::new(Self {
            owner: OnceLock::new(),
            thread_id,
            turn_id,
            sender: Mutex::new(Some(tx)),
            receiver: Mutex::new(Some(rx)),
            terminal,
            last_seq: AtomicU64::new(0),
            event_route_claimed: AtomicBool::new(false),
            lagged: AtomicBool::new(false),
            legacy_events: Mutex::new(None),
            query: query.then(|| Mutex::new(QueryOwnership::default())),
        })
    }

    pub fn buffer_legacy_events(&self) {
        *self.legacy_events.lock().unwrap() = Some(Vec::new());
        self.sender.lock().unwrap().take();
    }

    pub fn take_legacy_events(&self) -> Vec<RunEvent> {
        self.legacy_events
            .lock()
            .unwrap()
            .take()
            .unwrap_or_default()
    }

    pub fn fail(&self, error: &str) {
        self.terminal.send_if_modified(|value| {
            if value.is_some() {
                false
            } else {
                *value = Some(Err(error.into()));
                true
            }
        });
        self.sender.lock().unwrap().take();
    }

    pub fn complete(&self, snapshot: RunSnapshot) {
        if snapshot.thread_id != self.thread_id || snapshot.turn_id != self.turn_id {
            self.fail("Snapshot belongs to another run");
        } else if snapshot.status.is_terminal() {
            if snapshot.result.is_none() {
                self.fail("Terminal snapshot has no result");
                return;
            }
            self.terminal.send_if_modified(|value| {
                if value.is_some() {
                    false
                } else {
                    *value = Some(Ok(snapshot.clone()));
                    true
                }
            });
        }
    }

    pub fn finish_snapshot_only(&self) {
        self.sender.lock().unwrap().take();
    }

    pub fn pending_query(&self) -> bool {
        self.query.is_some() && self.terminal.borrow().is_none()
    }

    pub(crate) fn claim_event_route(&self) {
        self.event_route_claimed.store(true, Ordering::SeqCst);
    }

    fn event_route_claimed(&self) -> bool {
        self.event_route_claimed.load(Ordering::SeqCst)
    }

    /// Returns true only when this event ends the live route.
    pub fn accept(&self, event: RunEvent) -> bool {
        if event.thread_id != self.thread_id || event.turn_id != self.turn_id {
            self.fail("Event belongs to another run");
            return false;
        }
        let previous = self.last_seq.fetch_max(event.seq, Ordering::SeqCst);
        if event.seq <= previous {
            return false;
        }
        if matches!(event.payload, RunEventPayload::Stream { .. }) {
            if let Some(query) = &self.query {
                query.lock().unwrap().observed_live = true;
            }
        }
        if let Some(buffer) = self.legacy_events.lock().unwrap().as_mut() {
            buffer.push(event.clone());
        }
        let mut sender = self.sender.lock().unwrap();
        if event.seq != previous + 1 {
            self.lagged.store(true, Ordering::SeqCst);
            sender.take();
        }
        if let Some(tx) = sender.as_ref() {
            match tx.try_send(event.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.lagged.store(true, Ordering::SeqCst);
                    sender.take();
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    sender.take();
                }
            }
        }
        if let RunEventPayload::Finished { snapshot } = event.payload {
            sender.take();
            drop(sender);
            if snapshot.last_seq != event.seq || !snapshot.status.is_terminal() {
                self.fail("Invalid terminal event");
            } else {
                self.complete(snapshot);
            }
            return true;
        }
        false
    }
}

pub(crate) struct PendingStartRoute {
    client: Arc<super::ClientState>,
    state: Arc<RunState>,
    armed: bool,
}

impl PendingStartRoute {
    pub(crate) fn new(client: Arc<super::ClientState>, state: Arc<RunState>) -> Self {
        Self {
            client,
            state,
            armed: true,
        }
    }

    pub(crate) fn commit(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingStartRoute {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.client
            .runs
            .remove_if(&self.state.turn_id, |_, current| {
                Arc::ptr_eq(current, &self.state) && !self.state.event_route_claimed()
            });
    }
}

impl Drop for RunState {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.get().and_then(Weak::upgrade) {
            let mut cache = owner.run_handles.lock().unwrap();
            if cache
                .get(&self.turn_id)
                .is_some_and(|(_, weak)| std::ptr::eq(weak.as_ptr(), self))
            {
                cache.remove(&self.turn_id);
            }
        }
    }
}

/// A query owns its tentative route until another query observes a running
/// snapshot or the last waiter leaves. Always lock the registry before query
/// ownership, so cancellation and concurrent acquisition are atomic together.
pub(crate) struct QueryRouteLease {
    client: Arc<super::ClientState>,
    state: Arc<RunState>,
}
impl QueryRouteLease {
    /// Caller holds this run's registry entry while acquiring the lease.
    pub fn acquire(client: Arc<super::ClientState>, state: Arc<RunState>) -> Self {
        if let Some(query) = &state.query {
            query.lock().unwrap().waiters += 1;
        }
        Self { client, state }
    }

    pub fn observe(&self, snapshot: &RunSnapshot) {
        let Some(query) = &self.state.query else {
            return;
        };
        let mut query = query.lock().unwrap();
        if snapshot.thread_id != self.state.thread_id || snapshot.turn_id != self.state.turn_id {
            return;
        }
        if !snapshot.status.is_terminal() {
            query.observed_live = true;
        } else {
            // Another query may still return an earlier running snapshot. Only
            // the final waiter can decide this was an archive-only lookup.
            query.observed_terminal = true;
        }
    }
}
impl Drop for QueryRouteLease {
    fn drop(&mut self) {
        let Some(query) = &self.state.query else {
            return;
        };
        let entry = self.client.runs.entry(self.state.turn_id.clone());
        let mut query = query.lock().unwrap();
        query.waiters -= 1;
        if query.waiters == 0 && !query.observed_live {
            if query.observed_terminal {
                self.state.finish_snapshot_only();
            }
            if let dashmap::mapref::entry::Entry::Occupied(entry) = entry {
                if Arc::ptr_eq(entry.get(), &self.state) {
                    entry.remove();
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct RunHandle {
    pub(crate) client: WhaleClient,
    pub(crate) state: Arc<RunState>,
}

impl RunHandle {
    pub fn id(&self) -> &str {
        &self.state.turn_id
    }
    pub fn thread_id(&self) -> &str {
        &self.state.thread_id
    }

    /// Creates an independent replayable view of this Run's events.
    pub async fn subscribe_events(
        &self,
        after: Option<SessionCursor>,
        options: SubscriptionOptions,
    ) -> Result<RunEventSubscription, SessionViewError> {
        self.client
            .subscribe_run_events(self.thread_id(), self.id(), after, options)
            .await
    }

    /// Takes this handle's single buffered event subscription. Results remain
    /// available even if this stream is never taken or falls behind.
    pub fn events(&self) -> Result<RunEventStream, SdkError> {
        let receiver = self
            .state
            .receiver
            .lock()
            .unwrap()
            .take()
            .ok_or(SdkError::AlreadySubscribed)?;
        Ok(RunEventStream {
            receiver,
            state: self.state.clone(),
            ended: false,
        })
    }

    pub async fn result(&self) -> Result<RunTurnResult, SdkError> {
        let mut terminal = self.state.terminal.subscribe();
        loop {
            let current = terminal.borrow_and_update().clone();
            if let Some(outcome) = current {
                return match outcome {
                    Ok(snapshot) => snapshot.result.ok_or_else(|| {
                        SdkError::Internal("Terminal snapshot has no result".into())
                    }),
                    Err(message) => Err(SdkError::ChannelClosed(message)),
                };
            }
            terminal
                .changed()
                .await
                .map_err(|_| SdkError::ChannelClosed("Run result channel closed".into()))?;
        }
    }

    pub async fn snapshot(&self) -> Result<RunSnapshot, SdkError> {
        let snapshot: RunSnapshot = self
            .client
            .request_for_session(METHOD_TURN_GET, Some(self.reference()), self.thread_id())
            .await?;
        self.state.complete(snapshot.clone());
        Ok(snapshot)
    }

    /// Requests cancellation; use result/snapshot to observe the final state.
    pub async fn cancel(&self) -> Result<RunSnapshot, SdkError> {
        let snapshot: RunSnapshot = self
            .client
            .request_for_session(METHOD_TURN_CANCEL, Some(self.reference()), self.thread_id())
            .await?;
        self.state.complete(snapshot.clone());
        Ok(snapshot)
    }

    pub async fn resolve_approval(
        &self,
        request_id: &str,
        decision: RunApprovalDecision,
        arguments: Option<serde_json::Value>,
        feedback: Option<String>,
    ) -> Result<bool, SdkError> {
        if self
            .client
            .inner
            .state
            .interactions_enabled(self.thread_id())
            && self
                .client
                .initialize()
                .await?
                .capabilities
                .iter()
                .any(|capability| {
                    capability == whale_protocol::interactions::CAPABILITY_INTERACTIONS
                })
        {
            return Ok(self
                .respond_interaction(
                    request_id,
                    crate::interactions::run_approval_response(decision, arguments, feedback),
                )
                .await?
                .resolved);
        }
        let result: ApprovalResolveResult = self
            .client
            .request_for_session(
                METHOD_TURN_RESOLVE_APPROVAL,
                Some(RunApprovalParams {
                    thread_id: self.thread_id().into(),
                    turn_id: self.id().into(),
                    request_id: request_id.into(),
                    decision,
                    arguments,
                    feedback,
                }),
                self.thread_id(),
            )
            .await?;
        Ok(result.resolved)
    }

    fn reference(&self) -> RunRefParams {
        RunRefParams {
            thread_id: self.thread_id().into(),
            turn_id: self.id().into(),
        }
    }
}

pub struct RunEventStream {
    receiver: mpsc::Receiver<RunEvent>,
    state: Arc<RunState>,
    ended: bool,
}
impl RunEventStream {
    pub async fn recv(&mut self) -> Result<Option<RunEvent>, SdkError> {
        if self.ended {
            return Ok(None);
        }
        if self.state.lagged.load(Ordering::SeqCst) {
            self.ended = true;
            return Err(SdkError::EventLagged);
        }
        let event = self.receiver.recv().await;
        if self.state.lagged.load(Ordering::SeqCst) {
            self.ended = true;
            return Err(SdkError::EventLagged);
        }
        if event.is_none() {
            self.ended = true;
            if let Some(Err(message)) = self.state.terminal.borrow().as_ref() {
                return Err(SdkError::ChannelClosed(message.clone()));
            }
        }
        Ok(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use whale_protocol::AgentStreamEvent;

    #[tokio::test]
    async fn old_duplicate_does_not_move_sequence_backwards() {
        let state = RunState::new("s".into(), "r".into());
        for seq in [1, 2, 1, 3] {
            state.accept(RunEvent {
                thread_id: "s".into(),
                turn_id: "r".into(),
                seq,
                payload: RunEventPayload::Stream {
                    event: AgentStreamEvent::TextDelta {
                        turn_id: "r".into(),
                        item_id: "i".into(),
                        delta: "x".into(),
                    },
                },
            });
        }
        assert!(!state.lagged.load(Ordering::SeqCst));
        let mut receiver = state.receiver.lock().unwrap().take().unwrap();
        for seq in 1..=3 {
            assert_eq!(receiver.recv().await.unwrap().seq, seq);
        }
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn compatibility_collector_accepts_burst_before_start_returns() {
        let state = RunState::new("s".into(), "r".into());
        state.buffer_legacy_events();
        for seq in 1..=1000 {
            state.accept(RunEvent {
                thread_id: "s".into(),
                turn_id: "r".into(),
                seq,
                payload: RunEventPayload::Stream {
                    event: AgentStreamEvent::TextDelta {
                        turn_id: "r".into(),
                        item_id: "i".into(),
                        delta: "x".into(),
                    },
                },
            });
        }
        assert!(!state.lagged.load(Ordering::SeqCst));
        assert_eq!(state.take_legacy_events().len(), 1000);
    }
}
