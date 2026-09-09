//! Owner-scoped Session catalog, canonical history, metadata CAS, and V2 views.

use crate::{
    ClientState, InternalRequestError, SdkError, SessionRequestAccess, SubscriptionOptions,
    WhaleClient, WhaleThread,
};
use serde_json::{Map, Value};
use std::num::NonZeroU32;
use std::sync::{Arc, Weak};
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use whale_protocol::session_management::{
    GetSessionHistoryParams, GetSessionHistoryResult, GetSessionV2Params, ListSessionsParams,
    ListSessionsResult, ReplaceSessionMetadataParams, ReplaceSessionMetadataResult,
    SessionCursorV2, SessionEventEnvelopeV2, SessionHistoryAnchor, SessionHistoryPageCursor,
    SessionLifecycleState, SessionListCursor, SessionManagementErrorData, SessionSnapshotV2,
    SubscribeSessionV2Params, SubscribeSessionV2Result, CAPABILITY_SESSION_CATALOG,
    CAPABILITY_SESSION_HISTORY, CAPABILITY_SESSION_LIFECYCLE_REPLAY,
    CAPABILITY_SESSION_METADATA_CAS, DEFAULT_SESSION_HISTORY_PAGE_LIMIT,
    DEFAULT_SESSION_LIST_PAGE_LIMIT, MAX_SESSION_HISTORY_PAGE_LIMIT, MAX_SESSION_LIST_PAGE_LIMIT,
    METHOD_SESSION_GET_V2, METHOD_SESSION_HISTORY, METHOD_SESSION_LIST,
    METHOD_SESSION_METADATA_REPLACE, METHOD_SESSION_SUBSCRIBE_V2, SESSION_CURSOR_REJECTED,
    SESSION_HISTORY_GAP, SESSION_MANAGEMENT_STATE, SESSION_REVISION_CONFLICT,
};
use whale_protocol::session_views::MAX_SESSION_HISTORY_LIMIT;

const SESSION_EVENT_HUB_V2_CAPACITY: usize = 64;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SessionManagementError {
    #[error(transparent)]
    Sdk(#[from] SdkError),
    #[error("Daemon does not advertise required capability {capability}")]
    UnsupportedCapability { capability: &'static str },
    #[error("Invalid Session management option {field}: {message}")]
    InvalidOptions {
        field: &'static str,
        message: String,
    },
    #[error("Session is unavailable")]
    SessionUnavailable,
    #[error("Session is not open ({lifecycle:?})")]
    SessionNotOpen { lifecycle: SessionLifecycleState },
    #[error(
        "Session view revision conflict: expected {expected_view_revision}, current {current_view_revision}"
    )]
    RevisionConflict {
        expected_view_revision: u64,
        current_view_revision: u64,
        current: SessionCursorV2,
    },
    #[error("Session list cursor is invalid")]
    ListCursorInvalid,
    #[error("Session list cursor has expired")]
    ListCursorExpired,
    #[error("Session history cursor is invalid")]
    HistoryCursorInvalid,
    #[error("Session history belongs to an earlier attachment")]
    HistoryStreamReset {
        requested: SessionHistoryAnchor,
        current: SessionHistoryAnchor,
    },
    #[error("Requested Session history is no longer retained")]
    HistoryGap {
        requested: SessionHistoryAnchor,
        floor: SessionHistoryAnchor,
        current_end: SessionHistoryAnchor,
    },
    #[error("Closed Session tombstone expired: {thread_id}")]
    TombstoneExpired { thread_id: String },
    #[error("Session resource limit exceeded for {resource}: {actual} > {limit}")]
    ResourceLimit {
        resource: String,
        actual: u64,
        limit: u64,
        item_index: Option<u64>,
    },
    #[error("Session storage operation failed (outcome_unknown={outcome_unknown})")]
    StorageFailure { outcome_unknown: bool },
    #[error("Invalid Session management projection: {message}")]
    InvalidProjection { message: String },
}

pub type SessionListPage = ListSessionsResult;
pub type SessionHistoryPage = GetSessionHistoryResult;

#[derive(Clone)]
pub struct SessionViewHandle {
    client: WhaleClient,
    thread_id: String,
}

impl SessionViewHandle {
    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub async fn snapshot(&self) -> Result<SessionSnapshotV2, SessionManagementError> {
        self.client
            .require_management_capability(CAPABILITY_SESSION_LIFECYCLE_REPLAY)
            .await?;
        self.client
            .get_session_snapshot_v2(&self.thread_id, 256)
            .await
    }

    pub async fn history_page(
        &self,
        options: SessionHistoryOptions,
    ) -> Result<SessionHistoryPage, SessionManagementError> {
        self.client
            .require_management_capability(CAPABILITY_SESSION_HISTORY)
            .await?;
        let params = GetSessionHistoryParams {
            thread_id: self.thread_id.clone(),
            before: options.before,
            cursor: options.cursor,
            limit: options.limit.get(),
        };
        params
            .validate()
            .map_err(|message| invalid_option("history", message))?;
        let result: GetSessionHistoryResult = self
            .client
            .request_management(
                METHOD_SESSION_HISTORY,
                Some(params.clone()),
                SessionRequestAccess::Viewable(&self.thread_id),
            )
            .await?;
        result.validate_for(&params).map_err(invalid_projection)?;
        Ok(result)
    }

    pub async fn watch(
        &self,
        options: SessionManagementWatchOptions,
    ) -> Result<SessionWatchV2, SessionManagementError> {
        self.client
            .require_management_capability(CAPABILITY_SESSION_LIFECYCLE_REPLAY)
            .await?;
        let (hub, live, termination, catch_up) = self
            .client
            .inner
            .state
            .subscribe_session_events_v2(&self.thread_id)?;
        let snapshot = self
            .client
            .get_session_snapshot_v2(&self.thread_id, options.history_limit())
            .await?;
        let events = spawn_session_stream_v2(
            self.client.clone(),
            self.thread_id.clone(),
            hub,
            snapshot.cursor.clone(),
            live,
            termination,
            catch_up,
            options.subscription,
            None,
        );
        Ok(SessionWatchV2 { snapshot, events })
    }

    pub async fn subscribe_from(
        &self,
        cursor: SessionCursorV2,
        options: SubscriptionOptions,
    ) -> Result<SessionEventStreamV2, SessionManagementError> {
        self.client
            .require_management_capability(CAPABILITY_SESSION_LIFECYCLE_REPLAY)
            .await?;
        cursor
            .validate()
            .map_err(|message| invalid_option("cursor", message))?;
        if cursor.thread_id != self.thread_id {
            return Err(invalid_option(
                "cursor",
                "V2 Session cursor belongs to another thread",
            ));
        }
        let (hub, live, termination, catch_up) = self
            .client
            .inner
            .state
            .subscribe_session_events_v2(&self.thread_id)?;
        let params = SubscribeSessionV2Params {
            thread_id: self.thread_id.clone(),
            after: cursor.clone(),
            through: None,
            limit: options.replay_page_size(),
        };
        let page = self.client.get_session_replay_page_v2(&params).await?;
        if let Some(gap) = &page.gap {
            return Err(replay_gap_error(gap));
        }
        Ok(spawn_session_stream_v2(
            self.client.clone(),
            self.thread_id.clone(),
            hub,
            cursor,
            live,
            termination,
            catch_up,
            options,
            Some((params, page)),
        ))
    }
}

pub struct SessionWatchV2 {
    pub snapshot: SessionSnapshotV2,
    pub events: SessionEventStreamV2,
}

pub struct SessionEventStreamV2 {
    receiver: mpsc::Receiver<Result<SessionEventEnvelopeV2, SessionManagementError>>,
    termination: watch::Receiver<Option<SessionHubTerminationV2>>,
    _hub: Arc<SessionEventHubV2>,
    worker: JoinHandle<()>,
    ended: bool,
    last_received: Option<SessionCursorV2>,
}

impl SessionEventStreamV2 {
    pub fn last_received(&self) -> Option<&SessionCursorV2> {
        self.last_received.as_ref()
    }

    pub async fn recv(&mut self) -> Option<Result<SessionEventEnvelopeV2, SessionManagementError>> {
        loop {
            if self.ended {
                return None;
            }
            if let Some(termination) = self.termination.borrow().clone() {
                self.ended = true;
                return termination.into_stream_item();
            }
            tokio::select! {
                biased;
                changed = self.termination.changed() => {
                    if changed.is_err() && self.termination.borrow().is_none() {
                        self.ended = true;
                        return Some(Err(SessionManagementError::Sdk(SdkError::ChannelClosed(
                            "V2 Session event hub stopped".into(),
                        ))));
                    }
                }
                item = self.receiver.recv() => {
                    match item {
                        Some(Ok(event)) => {
                            self.last_received = Some(event.cursor.clone());
                            return Some(Ok(event));
                        }
                        Some(Err(error)) => {
                            self.ended = true;
                            return Some(Err(error));
                        }
                        None => {
                            if let Some(termination) = self.termination.borrow().clone() {
                                self.ended = true;
                                return termination.into_stream_item();
                            }
                            self.ended = true;
                            return None;
                        }
                    }
                }
            }
        }
    }
}

impl Drop for SessionEventStreamV2 {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

#[derive(Clone, Debug)]
enum SessionHubTerminationV2 {
    ConnectionLost(String),
}

impl SessionHubTerminationV2 {
    fn into_stream_item(self) -> Option<Result<SessionEventEnvelopeV2, SessionManagementError>> {
        match self {
            Self::ConnectionLost(message) => Some(Err(SessionManagementError::Sdk(
                SdkError::ChannelClosed(message),
            ))),
        }
    }
}

pub(crate) struct SessionEventHubV2 {
    events: broadcast::Sender<SessionEventEnvelopeV2>,
    termination: watch::Sender<Option<SessionHubTerminationV2>>,
    catch_up: watch::Sender<u64>,
    owner: Weak<ClientState>,
    thread_id: String,
}

impl SessionEventHubV2 {
    fn new(owner: &Arc<ClientState>, thread_id: &str) -> Arc<Self> {
        let (events, _) = broadcast::channel(SESSION_EVENT_HUB_V2_CAPACITY);
        let (termination, _) = watch::channel(None);
        let (catch_up, _) = watch::channel(0);
        Arc::new(Self {
            events,
            termination,
            catch_up,
            owner: Arc::downgrade(owner),
            thread_id: thread_id.to_owned(),
        })
    }

    fn subscribe(
        &self,
    ) -> (
        broadcast::Receiver<SessionEventEnvelopeV2>,
        watch::Receiver<Option<SessionHubTerminationV2>>,
        watch::Receiver<u64>,
    ) {
        (
            self.events.subscribe(),
            self.termination.subscribe(),
            self.catch_up.subscribe(),
        )
    }

    fn nudge(&self) {
        let next = self.catch_up.borrow().wrapping_add(1);
        self.catch_up.send_replace(next);
    }

    fn close(&self, termination: SessionHubTerminationV2) {
        self.termination.send_replace(Some(termination));
    }
}

impl Drop for SessionEventHubV2 {
    fn drop(&mut self) {
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        let self_ptr = self as *const Self;
        owner
            .session_event_hubs_v2
            .remove_if(&self.thread_id, |_, registered| {
                registered.as_ptr() == self_ptr
            });
    }
}

impl ClientState {
    pub(crate) fn ensure_session_viewable(&self, _thread_id: &str) -> Result<(), SdkError> {
        if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
            Err(SdkError::ChannelClosed("Client is closed".into()))
        } else {
            Ok(())
        }
    }

    fn ensure_session_writable_v2(&self, thread_id: &str) -> Result<(), SessionManagementError> {
        match self.session_lifecycle_v2(thread_id) {
            Some(SessionLifecycleState::Open) => Ok(()),
            Some(lifecycle) => Err(SessionManagementError::SessionNotOpen { lifecycle }),
            None => Err(SessionManagementError::SessionUnavailable),
        }
    }

    fn session_not_open_error(&self, thread_id: &str) -> SessionManagementError {
        match self.session_lifecycle_v2(thread_id) {
            Some(SessionLifecycleState::Open) | None => SessionManagementError::SessionUnavailable,
            Some(lifecycle) => SessionManagementError::SessionNotOpen { lifecycle },
        }
    }

    fn ensure_session_event_hub_v2(self: &Arc<Self>, thread_id: &str) -> Arc<SessionEventHubV2> {
        match self.session_event_hubs_v2.entry(thread_id.to_owned()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                if let Some(hub) = entry.get().upgrade() {
                    hub
                } else {
                    let hub = SessionEventHubV2::new(self, thread_id);
                    entry.insert(Arc::downgrade(&hub));
                    hub
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let hub = SessionEventHubV2::new(self, thread_id);
                entry.insert(Arc::downgrade(&hub));
                hub
            }
        }
    }

    fn subscribe_session_events_v2(
        self: &Arc<Self>,
        thread_id: &str,
    ) -> Result<
        (
            Arc<SessionEventHubV2>,
            broadcast::Receiver<SessionEventEnvelopeV2>,
            watch::Receiver<Option<SessionHubTerminationV2>>,
            watch::Receiver<u64>,
        ),
        SdkError,
    > {
        self.ensure_session_viewable(thread_id)?;
        let hub = self.ensure_session_event_hub_v2(thread_id);
        let (live, termination, catch_up) = hub.subscribe();
        Ok((hub, live, termination, catch_up))
    }

    fn remove_session_event_hub_v2_if_same(
        &self,
        thread_id: &str,
        expected: &Arc<SessionEventHubV2>,
    ) {
        self.session_event_hubs_v2
            .remove_if(thread_id, |_, current| {
                current.as_ptr() == Arc::as_ptr(expected)
            });
    }

    pub(crate) fn route_session_event_v2(
        &self,
        event: SessionEventEnvelopeV2,
    ) -> Result<(), String> {
        event.validate()?;
        let hub = self
            .session_event_hubs_v2
            .get(&event.thread_id)
            .map(|registered| registered.value().clone())
            .and_then(|registered| registered.upgrade());
        if let Some(hub) = hub {
            #[cfg(test)]
            route_drop_regression::pause_after_upgrade(&event.thread_id);
            let _ = hub.events.send(event);
        }
        Ok(())
    }

    pub(crate) fn nudge_session_event_hub_v2(&self, thread_id: &str) {
        let hub = self
            .session_event_hubs_v2
            .get(thread_id)
            .map(|registered| registered.value().clone())
            .and_then(|registered| registered.upgrade());
        if let Some(hub) = hub {
            hub.nudge();
        }
    }

    pub(crate) fn close_all_session_event_hubs_v2(&self, error: Option<String>) {
        let hubs: Vec<_> = self
            .session_event_hubs_v2
            .iter()
            .filter_map(|entry| entry.value().upgrade())
            .collect();
        self.session_event_hubs_v2.clear();
        if let Some(message) = error {
            for hub in hubs {
                hub.close(SessionHubTerminationV2::ConnectionLost(message.clone()));
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_session_stream_v2(
    client: WhaleClient,
    thread_id: String,
    hub: Arc<SessionEventHubV2>,
    cursor: SessionCursorV2,
    live: broadcast::Receiver<SessionEventEnvelopeV2>,
    termination: watch::Receiver<Option<SessionHubTerminationV2>>,
    catch_up: watch::Receiver<u64>,
    options: SubscriptionOptions,
    first_page: Option<(SubscribeSessionV2Params, SubscribeSessionV2Result)>,
) -> SessionEventStreamV2 {
    let (output, receiver) = mpsc::channel(options.buffer_capacity());
    let worker_termination = termination.clone();
    let worker = tokio::spawn(run_subscription_worker_v2(
        client,
        thread_id,
        hub.clone(),
        cursor,
        live,
        worker_termination,
        catch_up,
        output,
        options.replay_page_size(),
        first_page,
    ));
    SessionEventStreamV2 {
        receiver,
        termination,
        _hub: hub,
        worker,
        ended: false,
        last_received: None,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_subscription_worker_v2(
    client: WhaleClient,
    thread_id: String,
    hub: Arc<SessionEventHubV2>,
    mut last_enqueued: SessionCursorV2,
    mut live: broadcast::Receiver<SessionEventEnvelopeV2>,
    mut termination: watch::Receiver<Option<SessionHubTerminationV2>>,
    mut catch_up: watch::Receiver<u64>,
    output: mpsc::Sender<Result<SessionEventEnvelopeV2, SessionManagementError>>,
    page_size: u32,
    first_page: Option<(SubscribeSessionV2Params, SubscribeSessionV2Result)>,
) {
    match replay_fixed_window_v2(
        &client,
        &thread_id,
        &hub,
        page_size,
        &mut last_enqueued,
        &output,
        &mut termination,
        first_page,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => return,
        Err(error) => {
            send_worker_error_v2(&output, &mut termination, error).await;
            return;
        }
    }

    loop {
        tokio::select! {
            biased;
            changed = termination.changed() => {
                if changed.is_err() || termination.borrow().is_some() {
                    return;
                }
            }
            changed = catch_up.changed() => {
                if changed.is_err() {
                    return;
                }
                match replay_fixed_window_v2(
                    &client,
                    &thread_id,
                    &hub,
                    page_size,
                    &mut last_enqueued,
                    &output,
                    &mut termination,
                    None,
                ).await {
                    Ok(true) => {}
                    Ok(false) => return,
                    Err(error) => {
                        send_worker_error_v2(&output, &mut termination, error).await;
                        return;
                    }
                }
            }
            received = live.recv() => {
                let catch_up_needed = match received {
                    Ok(event) if same_stream_v2(&event.cursor, &last_enqueued)
                        && event.cursor.seq <= last_enqueued.seq => false,
                    Ok(event) if is_next_cursor_v2(&last_enqueued, &event.cursor) => {
                        let closed = is_closed_event_v2(&event);
                        if !enqueue_event_v2(
                            &output,
                            &mut termination,
                            &mut last_enqueued,
                            event,
                        ).await {
                            return;
                        }
                        if closed {
                            client
                                .inner
                                .state
                                .remove_session_event_hub_v2_if_same(&thread_id, &hub);
                            return;
                        }
                        false
                    }
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => true,
                    Err(broadcast::error::RecvError::Closed) => return,
                };
                if catch_up_needed {
                    match replay_fixed_window_v2(
                        &client,
                        &thread_id,
                        &hub,
                        page_size,
                        &mut last_enqueued,
                        &output,
                        &mut termination,
                        None,
                    ).await {
                        Ok(true) => {}
                        Ok(false) => return,
                        Err(error) => {
                            send_worker_error_v2(&output, &mut termination, error).await;
                            return;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod route_drop_regression {
    use super::*;
    use crate::weak_route_drop_regressions::{run_isolated, AfterUpgrade};
    use std::sync::{Mutex, OnceLock};

    const CHILD_ENVIRONMENT: &str = "WHALE_SDK_SESSION_V2_WEAK_ROUTE_CHILD";
    const EXACT_TEST_NAME: &str =
        "session_management::route_drop_regression::session_v2_route_releases_dashmap_guard_before_last_arc_drop";

    type Hook = (String, Arc<AfterUpgrade>);

    fn hook_slot() -> &'static Mutex<Option<Hook>> {
        static HOOK: OnceLock<Mutex<Option<Hook>>> = OnceLock::new();
        HOOK.get_or_init(|| Mutex::new(None))
    }

    pub(super) fn pause_after_upgrade(thread_id: &str) {
        let hook = hook_slot()
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(target, _)| target == thread_id)
            .map(|(_, hook)| hook.clone());
        if let Some(hook) = hook {
            hook.pause_route();
        }
    }

    #[test]
    fn session_v2_route_releases_dashmap_guard_before_last_arc_drop() {
        run_isolated(CHILD_ENVIRONMENT, EXACT_TEST_NAME, || {
            let thread_id = "session-v2-weak-route";
            let state = ClientState::new();
            let hub = SessionEventHubV2::new(&state, thread_id);
            state
                .session_event_hubs_v2
                .insert(thread_id.into(), Arc::downgrade(&hub));

            let hook = AfterUpgrade::new();
            *hook_slot().lock().unwrap() = Some((thread_id.into(), hook.clone()));
            let route_state = state.clone();
            let route = std::thread::spawn(move || {
                route_state.route_session_event_v2(SessionEventEnvelopeV2::new(
                    thread_id,
                    SessionCursorV2 {
                        thread_id: thread_id.into(),
                        stream_id: "session-v2-stream".into(),
                        seq: 1,
                    },
                    1,
                    whale_protocol::session_management::SessionEventPayloadV2::MetadataChanged {
                        metadata: Map::new(),
                    },
                ))
            });

            hook.drop_last_external_arc(|| drop(hub));
            route
                .join()
                .expect("Session V2 route thread panicked")
                .unwrap();
            *hook_slot().lock().unwrap() = None;
            assert!(!state.session_event_hubs_v2.contains_key(thread_id));
        });
    }
}

async fn replay_fixed_window_v2(
    client: &WhaleClient,
    thread_id: &str,
    hub: &Arc<SessionEventHubV2>,
    page_size: u32,
    last_enqueued: &mut SessionCursorV2,
    output: &mpsc::Sender<Result<SessionEventEnvelopeV2, SessionManagementError>>,
    termination: &mut watch::Receiver<Option<SessionHubTerminationV2>>,
    first_page: Option<(SubscribeSessionV2Params, SubscribeSessionV2Result)>,
) -> Result<bool, SessionManagementError> {
    let (mut params, mut page) = match first_page {
        Some(first) => first,
        None => {
            let params = SubscribeSessionV2Params {
                thread_id: thread_id.to_owned(),
                after: last_enqueued.clone(),
                through: None,
                limit: page_size,
            };
            let Some(page) = request_replay_page_v2(client, &params, termination).await? else {
                return Ok(false);
            };
            (params, page)
        }
    };

    loop {
        page.validate_for(&params).map_err(invalid_projection)?;
        if let Some(gap) = &page.gap {
            return Err(replay_gap_error(gap));
        }
        if page.has_more && page.resume_after == params.after {
            return Err(invalid_projection(
                "V2 replay page claimed more events without advancing its cursor",
            ));
        }
        let through = page.through.clone();
        for event in page.events {
            if !is_next_cursor_v2(last_enqueued, &event.cursor) {
                return Err(invalid_projection(format!(
                    "V2 replay event is not contiguous after Session cursor {}",
                    last_enqueued.seq
                )));
            }
            if !enqueue_event_v2(output, termination, last_enqueued, event).await {
                return Ok(false);
            }
        }
        if !page.has_more {
            let open = page.lifecycle != SessionLifecycleState::Closed;
            if !open {
                client
                    .inner
                    .state
                    .remove_session_event_hub_v2_if_same(thread_id, hub);
            }
            return Ok(open);
        }
        params = SubscribeSessionV2Params {
            thread_id: thread_id.to_owned(),
            after: page.resume_after,
            through: Some(through),
            limit: page_size,
        };
        let Some(next) = request_replay_page_v2(client, &params, termination).await? else {
            return Ok(false);
        };
        page = next;
    }
}

async fn request_replay_page_v2(
    client: &WhaleClient,
    params: &SubscribeSessionV2Params,
    termination: &mut watch::Receiver<Option<SessionHubTerminationV2>>,
) -> Result<Option<SubscribeSessionV2Result>, SessionManagementError> {
    let request = client.get_session_replay_page_v2(params);
    tokio::pin!(request);
    tokio::select! {
        biased;
        _ = termination.changed() => Ok(None),
        result = &mut request => result.map(Some),
    }
}

async fn enqueue_event_v2(
    output: &mpsc::Sender<Result<SessionEventEnvelopeV2, SessionManagementError>>,
    termination: &mut watch::Receiver<Option<SessionHubTerminationV2>>,
    last_enqueued: &mut SessionCursorV2,
    event: SessionEventEnvelopeV2,
) -> bool {
    let next = event.cursor.clone();
    let send = output.send(Ok(event));
    tokio::pin!(send);
    tokio::select! {
        biased;
        _ = termination.changed() => false,
        result = &mut send => {
            if result.is_ok() {
                *last_enqueued = next;
                true
            } else {
                false
            }
        }
    }
}

async fn send_worker_error_v2(
    output: &mpsc::Sender<Result<SessionEventEnvelopeV2, SessionManagementError>>,
    termination: &mut watch::Receiver<Option<SessionHubTerminationV2>>,
    error: SessionManagementError,
) {
    let send = output.send(Err(error));
    tokio::pin!(send);
    tokio::select! {
        biased;
        _ = termination.changed() => {}
        _ = &mut send => {}
    }
}

fn same_stream_v2(left: &SessionCursorV2, right: &SessionCursorV2) -> bool {
    left.thread_id == right.thread_id && left.stream_id == right.stream_id
}

fn is_next_cursor_v2(previous: &SessionCursorV2, next: &SessionCursorV2) -> bool {
    same_stream_v2(previous, next)
        && previous
            .seq
            .checked_add(1)
            .is_some_and(|expected| expected == next.seq)
}

fn is_closed_event_v2(event: &SessionEventEnvelopeV2) -> bool {
    matches!(
        event.payload,
        whale_protocol::session_management::SessionEventPayloadV2::LifecycleChanged {
            lifecycle: SessionLifecycleState::Closed
        }
    )
}

fn replay_gap_error(
    gap: &whale_protocol::session_management::ReplayGapV2,
) -> SessionManagementError {
    invalid_projection(format!(
        "V2 Session replay requires a fresh snapshot ({:?})",
        gap.reason
    ))
}

impl WhaleClient {
    async fn require_management_capability(
        &self,
        capability: &'static str,
    ) -> Result<(), SessionManagementError> {
        let initialized = self.initialize().await?;
        if initialized
            .capabilities
            .iter()
            .any(|advertised| advertised == capability)
        {
            Ok(())
        } else {
            Err(SessionManagementError::UnsupportedCapability { capability })
        }
    }

    pub async fn list_sessions(
        &self,
        options: SessionListOptions,
    ) -> Result<SessionListPage, SessionManagementError> {
        self.require_management_capability(CAPABILITY_SESSION_CATALOG)
            .await?;
        let params = ListSessionsParams {
            cursor: options.cursor,
            limit: options.limit.get(),
        };
        params
            .validate()
            .map_err(|message| invalid_option("cursor", message))?;
        let result: ListSessionsResult = self
            .request_management(
                METHOD_SESSION_LIST,
                Some(params.clone()),
                SessionRequestAccess::Unscoped,
            )
            .await?;
        result.validate_for(&params).map_err(invalid_projection)?;
        Ok(result)
    }

    pub fn session_view(
        &self,
        thread_id: impl Into<String>,
    ) -> Result<SessionViewHandle, SessionManagementError> {
        let thread_id = thread_id.into();
        whale_protocol::session_management::GetSessionV2Params {
            thread_id: thread_id.clone(),
            history_limit: 1,
        }
        .validate()
        .map_err(|message| invalid_option("thread_id", message))?;
        Ok(SessionViewHandle {
            client: self.clone(),
            thread_id,
        })
    }
}

impl WhaleThread {
    pub fn session_view(&self) -> SessionViewHandle {
        SessionViewHandle {
            client: self.client.clone(),
            thread_id: self.thread_id.clone(),
        }
    }

    pub async fn replace_metadata(
        &self,
        expected_view_revision: u64,
        metadata: Map<String, Value>,
    ) -> Result<ReplaceSessionMetadataResult, SessionManagementError> {
        self.client
            .require_management_capability(CAPABILITY_SESSION_METADATA_CAS)
            .await?;
        self.client
            .inner
            .state
            .ensure_session_writable_v2(&self.thread_id)?;
        let params = ReplaceSessionMetadataParams {
            thread_id: self.thread_id.clone(),
            expected_view_revision,
            metadata,
        };
        params
            .validate()
            .map_err(|message| invalid_option("metadata", message))?;
        let result: ReplaceSessionMetadataResult = match self
            .client
            .request_management(
                METHOD_SESSION_METADATA_REPLACE,
                Some(params.clone()),
                SessionRequestAccess::Writable(&self.thread_id),
            )
            .await
        {
            Err(SessionManagementError::Sdk(SdkError::SessionClosed(_))) => {
                return Err(self
                    .client
                    .inner
                    .state
                    .session_not_open_error(&self.thread_id));
            }
            result => result?,
        };
        result.validate_for(&params).map_err(invalid_projection)?;
        Ok(result)
    }
}

impl WhaleClient {
    async fn request_management<P: serde::Serialize, R: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: Option<P>,
        access: SessionRequestAccess<'_>,
    ) -> Result<R, SessionManagementError> {
        self.initialize().await?;
        self.inner
            .state
            .request_with_remote_error(&self.inner.writer, method, params, access)
            .await
            .map_err(map_request_error)
    }

    async fn get_session_snapshot_v2(
        &self,
        thread_id: &str,
        history_limit: u32,
    ) -> Result<SessionSnapshotV2, SessionManagementError> {
        let params = GetSessionV2Params {
            thread_id: thread_id.to_owned(),
            history_limit,
        };
        params
            .validate()
            .map_err(|message| invalid_option("history_limit", message))?;
        let snapshot: SessionSnapshotV2 = self
            .request_management(
                METHOD_SESSION_GET_V2,
                Some(params),
                SessionRequestAccess::Viewable(thread_id),
            )
            .await?;
        snapshot.validate().map_err(invalid_projection)?;
        if snapshot.summary.thread_id != thread_id || snapshot.history.capacity != history_limit {
            return Err(invalid_projection(
                "Daemon returned a V2 snapshot for another Session or history capacity",
            ));
        }
        Ok(snapshot)
    }

    async fn get_session_replay_page_v2(
        &self,
        params: &SubscribeSessionV2Params,
    ) -> Result<SubscribeSessionV2Result, SessionManagementError> {
        params
            .validate()
            .map_err(|message| invalid_option("cursor", message))?;
        let result: SubscribeSessionV2Result = self
            .request_management(
                METHOD_SESSION_SUBSCRIBE_V2,
                Some(params.clone()),
                SessionRequestAccess::Viewable(&params.thread_id),
            )
            .await?;
        result.validate_for(params).map_err(invalid_projection)?;
        Ok(result)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionListOptions {
    cursor: Option<SessionListCursor>,
    limit: NonZeroU32,
}

impl SessionListOptions {
    pub fn new(limit: u32) -> Result<Self, SessionManagementError> {
        let limit = checked_limit(limit, MAX_SESSION_LIST_PAGE_LIMIT, "limit")?;
        Ok(Self {
            cursor: None,
            limit,
        })
    }

    pub fn with_cursor(mut self, cursor: SessionListCursor) -> Self {
        self.cursor = Some(cursor);
        self
    }

    pub fn limit(&self) -> u32 {
        self.limit.get()
    }

    pub fn cursor(&self) -> Option<&SessionListCursor> {
        self.cursor.as_ref()
    }
}

impl Default for SessionListOptions {
    fn default() -> Self {
        Self::new(DEFAULT_SESSION_LIST_PAGE_LIMIT)
            .expect("default Session list page limit is valid")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionHistoryOptions {
    before: Option<SessionHistoryAnchor>,
    cursor: Option<SessionHistoryPageCursor>,
    limit: NonZeroU32,
}

impl SessionHistoryOptions {
    pub fn new(limit: u32) -> Result<Self, SessionManagementError> {
        let limit = checked_limit(limit, MAX_SESSION_HISTORY_PAGE_LIMIT, "limit")?;
        Ok(Self {
            before: None,
            cursor: None,
            limit,
        })
    }

    pub fn before(mut self, anchor: SessionHistoryAnchor) -> Result<Self, SessionManagementError> {
        if self.cursor.is_some() {
            return Err(invalid_option(
                "before",
                "history anchor and page cursor are mutually exclusive",
            ));
        }
        anchor
            .validate()
            .map_err(|message| invalid_option("before", message))?;
        self.before = Some(anchor);
        Ok(self)
    }

    pub fn continue_from(
        mut self,
        cursor: SessionHistoryPageCursor,
    ) -> Result<Self, SessionManagementError> {
        if self.before.is_some() {
            return Err(invalid_option(
                "cursor",
                "history page cursor and anchor are mutually exclusive",
            ));
        }
        cursor
            .validate()
            .map_err(|message| invalid_option("cursor", message))?;
        self.cursor = Some(cursor);
        Ok(self)
    }

    pub fn limit(&self) -> u32 {
        self.limit.get()
    }

    pub fn anchor(&self) -> Option<&SessionHistoryAnchor> {
        self.before.as_ref()
    }

    pub fn cursor(&self) -> Option<&SessionHistoryPageCursor> {
        self.cursor.as_ref()
    }
}

impl Default for SessionHistoryOptions {
    fn default() -> Self {
        Self::new(DEFAULT_SESSION_HISTORY_PAGE_LIMIT)
            .expect("default Session history page limit is valid")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionManagementWatchOptions {
    history_limit: NonZeroU32,
    subscription: SubscriptionOptions,
}

impl SessionManagementWatchOptions {
    pub fn new(
        history_limit: u32,
        subscription: SubscriptionOptions,
    ) -> Result<Self, SessionManagementError> {
        let history_limit =
            checked_limit(history_limit, MAX_SESSION_HISTORY_LIMIT, "history_limit")?;
        Ok(Self {
            history_limit,
            subscription,
        })
    }

    pub fn history_limit(&self) -> u32 {
        self.history_limit.get()
    }

    pub fn subscription(&self) -> &SubscriptionOptions {
        &self.subscription
    }
}

impl Default for SessionManagementWatchOptions {
    fn default() -> Self {
        Self::new(256, SubscriptionOptions::default())
            .expect("default V2 Session watch options are valid")
    }
}

fn checked_limit(
    value: u32,
    maximum: u32,
    field: &'static str,
) -> Result<NonZeroU32, SessionManagementError> {
    if value == 0 || value > maximum {
        return Err(invalid_option(
            field,
            format!("must be between 1 and {maximum}"),
        ));
    }
    Ok(NonZeroU32::new(value).expect("positive checked limit"))
}

fn invalid_option(field: &'static str, message: impl Into<String>) -> SessionManagementError {
    SessionManagementError::InvalidOptions {
        field,
        message: message.into(),
    }
}

fn invalid_projection(message: impl Into<String>) -> SessionManagementError {
    SessionManagementError::InvalidProjection {
        message: message.into(),
    }
}

fn map_request_error(error: InternalRequestError) -> SessionManagementError {
    let remote = match error {
        InternalRequestError::Sdk(error) => return SessionManagementError::Sdk(error),
        InternalRequestError::Remote(remote) => remote,
    };
    let Some(value) = remote.data else {
        return invalid_projection(format!(
            "Session management RPC {} omitted typed error data",
            remote.code
        ));
    };
    let data: SessionManagementErrorData = match serde_json::from_value(value) {
        Ok(data) => data,
        Err(error) => return invalid_projection(format!("Malformed Session error data: {error}")),
    };
    if let Err(message) = data.validate() {
        return invalid_projection(format!("Invalid Session error data: {message}"));
    }
    match (remote.code, data) {
        (SESSION_MANAGEMENT_STATE, SessionManagementErrorData::Unavailable) => {
            SessionManagementError::SessionUnavailable
        }
        (SESSION_MANAGEMENT_STATE, SessionManagementErrorData::SessionNotOpen { lifecycle }) => {
            SessionManagementError::SessionNotOpen { lifecycle }
        }
        (
            SESSION_REVISION_CONFLICT,
            SessionManagementErrorData::RevisionConflict {
                expected_view_revision,
                current_view_revision,
                current,
            },
        ) => SessionManagementError::RevisionConflict {
            expected_view_revision,
            current_view_revision,
            current,
        },
        (SESSION_CURSOR_REJECTED, SessionManagementErrorData::ListCursorInvalid) => {
            SessionManagementError::ListCursorInvalid
        }
        (SESSION_CURSOR_REJECTED, SessionManagementErrorData::ListCursorExpired) => {
            SessionManagementError::ListCursorExpired
        }
        (SESSION_CURSOR_REJECTED, SessionManagementErrorData::HistoryCursorInvalid) => {
            SessionManagementError::HistoryCursorInvalid
        }
        (
            SESSION_CURSOR_REJECTED,
            SessionManagementErrorData::HistoryStreamReset { requested, current },
        ) => SessionManagementError::HistoryStreamReset { requested, current },
        (
            SESSION_HISTORY_GAP,
            SessionManagementErrorData::HistoryGap {
                requested,
                floor,
                current_end,
            },
        ) => SessionManagementError::HistoryGap {
            requested,
            floor,
            current_end,
        },
        (SESSION_MANAGEMENT_STATE, SessionManagementErrorData::TombstoneExpired { thread_id }) => {
            SessionManagementError::TombstoneExpired { thread_id }
        }
        (
            SESSION_MANAGEMENT_STATE,
            SessionManagementErrorData::ResourceLimit {
                resource,
                actual,
                limit,
                item_index,
            },
        ) => SessionManagementError::ResourceLimit {
            resource,
            actual,
            limit,
            item_index,
        },
        (
            whale_protocol::recovery::STORE_FAILED,
            SessionManagementErrorData::StorageFailure { outcome_unknown },
        ) => SessionManagementError::StorageFailure { outcome_unknown },
        (code, _) => invalid_projection(format!(
            "Session error data does not match JSON-RPC code {code}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClientInner, ClientState, ManagedWriter, WhaleClient, WhaleThread};
    use serde_json::Map;
    use std::{sync::Arc, time::Duration};
    use tokio::sync::mpsc;
    use whale_protocol::{
        initialization::{InitializeParams, InitializeResult, PeerInfo},
        rpc::{JSONRPCError, JSONRPCNotification, JSONRPCRequest, JSONRPCResponse},
        session_management::{
            GetSessionHistoryParams, GetSessionHistoryResult, GetSessionV2Params,
            ListSessionsParams, ListSessionsResult, ReplaceSessionMetadataResult,
            SessionEventEnvelopeV2, SessionHistoryAnchor, SessionLifecycleState,
            SessionPersistenceV2, SessionSnapshotV2, SessionSummaryV2, SubscribeSessionV2Params,
            SubscribeSessionV2Result, CAPABILITY_SESSION_CATALOG, CAPABILITY_SESSION_HISTORY,
            CAPABILITY_SESSION_LIFECYCLE_REPLAY, CAPABILITY_SESSION_METADATA_CAS,
            METHOD_SESSION_EVENT_V2, METHOD_SESSION_GET_V2, METHOD_SESSION_HISTORY,
            METHOD_SESSION_LIST, METHOD_SESSION_METADATA_REPLACE, METHOD_SESSION_SUBSCRIBE_V2,
            SESSION_CURSOR_REJECTED,
        },
        session_views::SessionHistoryWindow,
        sessions::{CloseSessionResult, METHOD_SESSION_CLOSE},
    };

    async fn next_request(rx: &mut mpsc::Receiver<String>) -> JSONRPCRequest {
        serde_json::from_str(
            &tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .expect("fake peer did not receive request")
                .expect("SDK request channel closed"),
        )
        .expect("SDK emitted invalid JSON-RPC")
    }

    fn respond<T: serde::Serialize>(client: &WhaleClient, request: JSONRPCRequest, value: T) {
        let response = JSONRPCResponse::success(request.id, value).unwrap();
        client.inner.state.incoming(
            &serde_json::to_string(&response).unwrap(),
            &client.inner.writer,
        );
    }

    fn reject(client: &WhaleClient, request: JSONRPCRequest, error: JSONRPCError) {
        let response = JSONRPCResponse::error(request.id, error);
        client.inner.state.incoming(
            &serde_json::to_string(&response).unwrap(),
            &client.inner.writer,
        );
    }

    fn notify_v2(client: &WhaleClient, event: SessionEventEnvelopeV2) {
        let notification = JSONRPCNotification::new(METHOD_SESSION_EVENT_V2, Some(event)).unwrap();
        client.inner.state.incoming(
            &serde_json::to_string(&notification).unwrap(),
            &client.inner.writer,
        );
    }

    async fn fixture(capabilities: &[&str]) -> (WhaleThread, mpsc::Receiver<String>) {
        let (tx, mut rx) = mpsc::channel(256);
        let client = WhaleClient {
            inner: Arc::new(ClientInner {
                state: ClientState::new(),
                writer: ManagedWriter::channel(tx),
                compatibility_owner: None,
            }),
        };
        let initializing = tokio::spawn({
            let client = client.clone();
            async move { client.initialize().await }
        });
        let request = next_request(&mut rx).await;
        let params: InitializeParams =
            serde_json::from_value(request.params.clone().unwrap()).unwrap();
        let mut result = InitializeResult::negotiate(
            &params,
            PeerInfo {
                name: "session-management-peer".into(),
                version: "test".into(),
            },
        )
        .unwrap();
        result.capabilities.extend(
            capabilities
                .iter()
                .map(|capability| (*capability).to_owned()),
        );
        respond(&client, request, result);
        initializing.await.unwrap().unwrap();
        client.inner.state.ensure_session_open("session").unwrap();
        (
            WhaleThread {
                recovery_key: None,
                client,
                thread_id: "session".into(),
                max_steps: 10,
                timeout_ms: None,
            },
            rx,
        )
    }

    fn assert_unsupported(result: Result<(), SessionManagementError>, capability: &'static str) {
        assert!(matches!(
            result,
            Err(SessionManagementError::UnsupportedCapability {
                capability: actual
            }) if actual == capability
        ));
    }

    fn v2_cursor(seq: u64) -> SessionCursorV2 {
        SessionCursorV2 {
            thread_id: "session".into(),
            stream_id: "stream-v2".into(),
            seq,
        }
    }

    fn v2_snapshot(seq: u64, history_limit: u32) -> SessionSnapshotV2 {
        let mut summary = SessionSummaryV2::new("session", 1);
        summary.view_revision = seq;
        summary.updated_at_ms = 1 + seq;
        SessionSnapshotV2::new(
            summary,
            SessionLifecycleState::Open,
            SessionPersistenceV2::Ephemeral,
            SessionHistoryWindow::new(history_limit).unwrap(),
            v2_cursor(seq),
        )
    }

    fn metadata_event(seq: u64, value: &str) -> SessionEventEnvelopeV2 {
        SessionEventEnvelopeV2::new(
            "session",
            v2_cursor(seq),
            1 + seq,
            whale_protocol::session_management::SessionEventPayloadV2::MetadataChanged {
                metadata: Map::from_iter([("title".into(), value.into())]),
            },
        )
    }

    fn lifecycle_event(seq: u64, lifecycle: SessionLifecycleState) -> SessionEventEnvelopeV2 {
        SessionEventEnvelopeV2::new(
            "session",
            v2_cursor(seq),
            1 + seq,
            whale_protocol::session_management::SessionEventPayloadV2::LifecycleChanged {
                lifecycle,
            },
        )
    }

    fn replay_v2(
        after: SessionCursorV2,
        through: SessionCursorV2,
        lifecycle: SessionLifecycleState,
        events: Vec<SessionEventEnvelopeV2>,
    ) -> SubscribeSessionV2Result {
        let resume_after = events
            .last()
            .map(|event| event.cursor.clone())
            .unwrap_or(after);
        let has_more = resume_after.seq < through.seq;
        SubscribeSessionV2Result {
            events,
            resume_after,
            through,
            lifecycle,
            has_more,
            gap: None,
        }
    }

    async fn subscribe_v2_empty(
        view: &SessionViewHandle,
        requests: &mut mpsc::Receiver<String>,
        after: SessionCursorV2,
        options: SubscriptionOptions,
    ) -> SessionEventStreamV2 {
        let client = view.client.clone();
        let subscribing = tokio::spawn({
            let view = view.clone();
            let after = after.clone();
            async move { view.subscribe_from(after, options).await }
        });
        let request = next_request(requests).await;
        assert_eq!(request.method, METHOD_SESSION_SUBSCRIBE_V2);
        respond(
            &client,
            request,
            replay_v2(
                after.clone(),
                after,
                SessionLifecycleState::Open,
                Vec::new(),
            ),
        );
        subscribing.await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn every_missing_capability_fails_before_a_business_request() {
        let (thread, mut requests) = fixture(&[]).await;
        let view = thread.session_view();

        assert_unsupported(
            thread
                .client
                .list_sessions(SessionListOptions::default())
                .await
                .map(|_| ()),
            CAPABILITY_SESSION_CATALOG,
        );
        assert!(requests.try_recv().is_err());

        assert_unsupported(
            view.snapshot().await.map(|_| ()),
            CAPABILITY_SESSION_LIFECYCLE_REPLAY,
        );
        assert!(requests.try_recv().is_err());

        assert_unsupported(
            view.history_page(SessionHistoryOptions::default())
                .await
                .map(|_| ()),
            CAPABILITY_SESSION_HISTORY,
        );
        assert!(requests.try_recv().is_err());

        assert_unsupported(
            thread.replace_metadata(0, Map::new()).await.map(|_| ()),
            CAPABILITY_SESSION_METADATA_CAS,
        );
        assert!(requests.try_recv().is_err());

        assert_unsupported(
            view.watch(SessionManagementWatchOptions::default())
                .await
                .map(|_| ()),
            CAPABILITY_SESSION_LIFECYCLE_REPLAY,
        );
        assert!(requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn management_routes_emit_checked_wire_and_validate_successes() {
        let capabilities = [
            CAPABILITY_SESSION_CATALOG,
            CAPABILITY_SESSION_HISTORY,
            CAPABILITY_SESSION_LIFECYCLE_REPLAY,
            CAPABILITY_SESSION_METADATA_CAS,
        ];
        let (thread, mut requests) = fixture(&capabilities).await;
        let client = thread.client.clone();
        let view = thread.session_view();

        let listing = tokio::spawn({
            let client = client.clone();
            async move { client.list_sessions(SessionListOptions::default()).await }
        });
        let request = next_request(&mut requests).await;
        assert_eq!(request.method, METHOD_SESSION_LIST);
        let params: ListSessionsParams =
            serde_json::from_value(request.params.clone().unwrap()).unwrap();
        assert_eq!(params.limit, 64);
        assert!(params.cursor.is_none());
        respond(
            &client,
            request,
            ListSessionsResult {
                sessions: Vec::new(),
                next_cursor: None,
            },
        );
        assert!(listing.await.unwrap().unwrap().sessions.is_empty());

        let snapshotting = tokio::spawn({
            let view = view.clone();
            async move { view.snapshot().await }
        });
        let request = next_request(&mut requests).await;
        assert_eq!(request.method, METHOD_SESSION_GET_V2);
        let params: GetSessionV2Params =
            serde_json::from_value(request.params.clone().unwrap()).unwrap();
        assert_eq!(params.thread_id, "session");
        assert_eq!(params.history_limit, 256);
        respond(&client, request, v2_snapshot(0, 256));
        assert_eq!(snapshotting.await.unwrap().unwrap().cursor, v2_cursor(0));

        let paging = tokio::spawn({
            let view = view.clone();
            async move { view.history_page(SessionHistoryOptions::default()).await }
        });
        let request = next_request(&mut requests).await;
        assert_eq!(request.method, METHOD_SESSION_HISTORY);
        let params: GetSessionHistoryParams =
            serde_json::from_value(request.params.clone().unwrap()).unwrap();
        assert_eq!(params.limit, 128);
        let end = SessionHistoryAnchor {
            thread_id: "session".into(),
            stream_id: "stream-v2".into(),
            index: 0,
        };
        respond(
            &client,
            request,
            GetSessionHistoryResult {
                items: Vec::new(),
                start_index: 0,
                end_index: 0,
                through: end.clone(),
                current_end: end,
                next_cursor: None,
            },
        );
        assert!(paging.await.unwrap().unwrap().items.is_empty());

        let metadata = serde_json::Map::from_iter([("title".into(), "demo".into())]);
        let replacing = tokio::spawn({
            let thread = thread.clone();
            let metadata = metadata.clone();
            async move { thread.replace_metadata(0, metadata).await }
        });
        let request = next_request(&mut requests).await;
        assert_eq!(request.method, METHOD_SESSION_METADATA_REPLACE);
        let mut summary = SessionSummaryV2::new("session", 1);
        summary.metadata = metadata;
        summary.view_revision = 1;
        summary.updated_at_ms = 2;
        respond(
            &client,
            request,
            ReplaceSessionMetadataResult {
                summary,
                cursor: v2_cursor(1),
                changed: true,
            },
        );
        assert!(replacing.await.unwrap().unwrap().changed);
    }

    #[tokio::test]
    async fn typed_remote_data_is_mapped_but_legacy_rpc_shape_is_unchanged() {
        let (thread, mut requests) = fixture(&[CAPABILITY_SESSION_CATALOG]).await;
        let client = thread.client.clone();

        let listing = tokio::spawn({
            let client = client.clone();
            async move { client.list_sessions(SessionListOptions::default()).await }
        });
        let request = next_request(&mut requests).await;
        reject(
            &client,
            request,
            JSONRPCError::new(
                SESSION_CURSOR_REJECTED,
                "diagnostic text must not be parsed",
                Some(serde_json::json!({"kind":"list_cursor_expired"})),
            ),
        );
        assert!(matches!(
            listing.await.unwrap(),
            Err(SessionManagementError::ListCursorExpired)
        ));

        let legacy = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .request::<_, serde_json::Value>("legacy.test", None::<()>)
                    .await
            }
        });
        let request = next_request(&mut requests).await;
        reject(
            &client,
            request,
            JSONRPCError::new(
                -32999,
                "legacy diagnostic",
                Some(serde_json::json!({"secret":"not exposed"})),
            ),
        );
        assert!(matches!(
            legacy.await.unwrap(),
            Err(SdkError::Rpc { code: -32999, message }) if message == "legacy diagnostic"
        ));
    }

    #[tokio::test]
    async fn inflight_management_request_reports_connection_loss_as_channel_closed() {
        let (thread, mut requests) = fixture(&[CAPABILITY_SESSION_CATALOG]).await;
        let client = thread.client.clone();
        let listing = tokio::spawn({
            let client = client.clone();
            async move { client.list_sessions(SessionListOptions::default()).await }
        });
        let request = next_request(&mut requests).await;
        assert_eq!(request.method, METHOD_SESSION_LIST);

        client.inner.state.disconnect("fixture EOF");
        assert!(matches!(
            listing.await.unwrap(),
            Err(SessionManagementError::Sdk(SdkError::ChannelClosed(message)))
                if message == "fixture EOF"
        ));
    }

    #[tokio::test]
    async fn invalid_list_cursor_is_rejected_before_a_business_request() {
        let (thread, mut requests) = fixture(&[CAPABILITY_SESSION_CATALOG]).await;
        let invalid: SessionListCursor = serde_json::from_value(serde_json::json!("")).unwrap();
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            thread
                .client
                .list_sessions(SessionListOptions::default().with_cursor(invalid)),
        )
        .await
        .expect("invalid list cursor reached the transport");

        assert!(matches!(
            result,
            Err(SessionManagementError::InvalidOptions {
                field: "cursor",
                ..
            })
        ));
        assert!(requests.try_recv().is_err());
    }

    #[test]
    fn every_typed_remote_error_maps_by_code_and_kind() {
        fn mapped(code: i64, data: SessionManagementErrorData) -> SessionManagementError {
            map_request_error(InternalRequestError::Remote(JSONRPCError::new(
                code,
                "diagnostic text is intentionally ignored",
                Some(serde_json::to_value(data).unwrap()),
            )))
        }

        fn anchor(stream_id: &str, index: u64) -> SessionHistoryAnchor {
            SessionHistoryAnchor {
                thread_id: "session".into(),
                stream_id: stream_id.into(),
                index,
            }
        }

        assert!(matches!(
            mapped(
                SESSION_MANAGEMENT_STATE,
                SessionManagementErrorData::Unavailable
            ),
            SessionManagementError::SessionUnavailable
        ));
        assert!(matches!(
            mapped(
                SESSION_MANAGEMENT_STATE,
                SessionManagementErrorData::SessionNotOpen {
                    lifecycle: SessionLifecycleState::Closing,
                }
            ),
            SessionManagementError::SessionNotOpen {
                lifecycle: SessionLifecycleState::Closing
            }
        ));
        assert!(matches!(
            mapped(
                SESSION_REVISION_CONFLICT,
                SessionManagementErrorData::RevisionConflict {
                    expected_view_revision: 3,
                    current_view_revision: 4,
                    current: v2_cursor(4),
                }
            ),
            SessionManagementError::RevisionConflict {
                expected_view_revision: 3,
                current_view_revision: 4,
                current,
            } if current == v2_cursor(4)
        ));
        assert!(matches!(
            mapped(
                SESSION_CURSOR_REJECTED,
                SessionManagementErrorData::ListCursorInvalid
            ),
            SessionManagementError::ListCursorInvalid
        ));
        assert!(matches!(
            mapped(
                SESSION_CURSOR_REJECTED,
                SessionManagementErrorData::ListCursorExpired
            ),
            SessionManagementError::ListCursorExpired
        ));
        assert!(matches!(
            mapped(
                SESSION_CURSOR_REJECTED,
                SessionManagementErrorData::HistoryCursorInvalid
            ),
            SessionManagementError::HistoryCursorInvalid
        ));
        assert!(matches!(
            mapped(
                SESSION_CURSOR_REJECTED,
                SessionManagementErrorData::HistoryStreamReset {
                    requested: anchor("old", 2),
                    current: anchor("current", 0),
                }
            ),
            SessionManagementError::HistoryStreamReset { requested, current }
                if requested == anchor("old", 2) && current == anchor("current", 0)
        ));
        assert!(matches!(
            mapped(
                SESSION_HISTORY_GAP,
                SessionManagementErrorData::HistoryGap {
                    requested: anchor("current", 1),
                    floor: anchor("current", 2),
                    current_end: anchor("current", 4),
                }
            ),
            SessionManagementError::HistoryGap {
                requested,
                floor,
                current_end,
            } if requested == anchor("current", 1)
                && floor == anchor("current", 2)
                && current_end == anchor("current", 4)
        ));
        assert!(matches!(
            mapped(
                SESSION_MANAGEMENT_STATE,
                SessionManagementErrorData::TombstoneExpired {
                    thread_id: "session".into(),
                }
            ),
            SessionManagementError::TombstoneExpired { thread_id }
                if thread_id == "session"
        ));
        assert!(matches!(
            mapped(
                SESSION_MANAGEMENT_STATE,
                SessionManagementErrorData::ResourceLimit {
                    resource: "tombstones".into(),
                    actual: 5,
                    limit: 4,
                    item_index: Some(3),
                }
            ),
            SessionManagementError::ResourceLimit {
                resource,
                actual: 5,
                limit: 4,
                item_index: Some(3),
            } if resource == "tombstones"
        ));
        assert!(matches!(
            mapped(
                whale_protocol::recovery::STORE_FAILED,
                SessionManagementErrorData::StorageFailure {
                    outcome_unknown: true,
                }
            ),
            SessionManagementError::StorageFailure {
                outcome_unknown: true
            }
        ));

        assert!(matches!(
            map_request_error(InternalRequestError::Remote(JSONRPCError::new(
                SESSION_MANAGEMENT_STATE,
                "missing data",
                None,
            ))),
            SessionManagementError::InvalidProjection { .. }
        ));
        assert!(matches!(
            map_request_error(InternalRequestError::Remote(JSONRPCError::new(
                SESSION_MANAGEMENT_STATE,
                "malformed data",
                Some(serde_json::json!({"kind":"future_kind"})),
            ))),
            SessionManagementError::InvalidProjection { .. }
        ));
        assert!(matches!(
            mapped(
                SESSION_REVISION_CONFLICT,
                SessionManagementErrorData::Unavailable
            ),
            SessionManagementError::InvalidProjection { .. }
        ));
    }

    #[tokio::test]
    async fn v2_watch_installs_route_before_snapshot_and_merges_replay_once() {
        let (thread, mut requests) = fixture(&[CAPABILITY_SESSION_LIFECYCLE_REPLAY]).await;
        let client = thread.client.clone();
        let view = thread.session_view();
        let watching =
            tokio::spawn(async move { view.watch(SessionManagementWatchOptions::default()).await });

        let get = next_request(&mut requests).await;
        assert_eq!(get.method, METHOD_SESSION_GET_V2);
        notify_v2(&client, metadata_event(2, "two"));
        respond(&client, get, v2_snapshot(1, 256));
        let mut watch = watching.await.unwrap().unwrap();

        let replay = next_request(&mut requests).await;
        assert_eq!(replay.method, METHOD_SESSION_SUBSCRIBE_V2);
        let params: SubscribeSessionV2Params =
            serde_json::from_value(replay.params.clone().unwrap()).unwrap();
        assert_eq!(params.after, v2_cursor(1));
        assert!(params.through.is_none());
        respond(
            &client,
            replay,
            replay_v2(
                v2_cursor(1),
                v2_cursor(2),
                SessionLifecycleState::Open,
                vec![metadata_event(2, "two")],
            ),
        );

        let event = watch.events.recv().await.unwrap().unwrap();
        assert_eq!(event.cursor, v2_cursor(2));
        assert_eq!(watch.events.last_received(), Some(&v2_cursor(2)));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), watch.events.recv())
                .await
                .is_err()
        );
        notify_v2(&client, metadata_event(3, "three"));
        assert_eq!(
            watch.events.recv().await.unwrap().unwrap().cursor,
            v2_cursor(3)
        );
    }

    #[tokio::test]
    async fn v2_closed_is_delivered_once_and_closed_cursor_replay_ends() {
        let (thread, mut requests) = fixture(&[CAPABILITY_SESSION_LIFECYCLE_REPLAY]).await;
        let client = thread.client.clone();
        let view = thread.session_view();
        let subscribing = tokio::spawn({
            let view = view.clone();
            async move {
                view.subscribe_from(v2_cursor(0), SubscriptionOptions::default())
                    .await
            }
        });
        let request = next_request(&mut requests).await;
        respond(
            &client,
            request,
            replay_v2(
                v2_cursor(0),
                v2_cursor(2),
                SessionLifecycleState::Closed,
                vec![
                    lifecycle_event(1, SessionLifecycleState::Closing),
                    lifecycle_event(2, SessionLifecycleState::Closed),
                ],
            ),
        );
        let mut stream = subscribing.await.unwrap().unwrap();
        assert_eq!(stream.recv().await.unwrap().unwrap().cursor, v2_cursor(1));
        assert_eq!(stream.recv().await.unwrap().unwrap().cursor, v2_cursor(2));
        assert!(stream.recv().await.is_none());
        assert!(stream.recv().await.is_none());
        assert!(!client
            .inner
            .state
            .session_event_hubs_v2
            .contains_key("session"));

        let late = tokio::spawn(async move {
            view.subscribe_from(v2_cursor(2), SubscriptionOptions::default())
                .await
        });
        let request = next_request(&mut requests).await;
        respond(
            &client,
            request,
            replay_v2(
                v2_cursor(2),
                v2_cursor(2),
                SessionLifecycleState::Closed,
                Vec::new(),
            ),
        );
        let mut late = late.await.unwrap().unwrap();
        assert!(late.recv().await.is_none());
    }

    #[tokio::test]
    async fn cancelled_v2_recv_keeps_worker_and_disconnect_errors_once() {
        let (thread, mut requests) = fixture(&[CAPABILITY_SESSION_LIFECYCLE_REPLAY]).await;
        let client = thread.client.clone();
        let view = thread.session_view();
        let subscribing = tokio::spawn(async move {
            view.subscribe_from(v2_cursor(0), SubscriptionOptions::default())
                .await
        });
        let request = next_request(&mut requests).await;
        respond(
            &client,
            request,
            replay_v2(
                v2_cursor(0),
                v2_cursor(0),
                SessionLifecycleState::Open,
                Vec::new(),
            ),
        );
        let mut stream = subscribing.await.unwrap().unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), stream.recv())
                .await
                .is_err()
        );
        notify_v2(&client, metadata_event(1, "after-cancel"));
        assert_eq!(stream.recv().await.unwrap().unwrap().cursor, v2_cursor(1));

        client.inner.state.disconnect("peer lost");
        assert!(matches!(
            stream.recv().await,
            Some(Err(SessionManagementError::Sdk(SdkError::ChannelClosed(message))))
                if message == "peer lost"
        ));
        assert!(stream.recv().await.is_none());
    }

    #[tokio::test]
    async fn close_response_forces_v2_replay_and_view_remains_read_only() {
        let (thread, mut requests) = fixture(&[
            CAPABILITY_SESSION_LIFECYCLE_REPLAY,
            CAPABILITY_SESSION_METADATA_CAS,
        ])
        .await;
        let client = thread.client.clone();
        let view = thread.session_view();
        let subscribing = tokio::spawn({
            let view = view.clone();
            async move {
                view.subscribe_from(v2_cursor(0), SubscriptionOptions::default())
                    .await
            }
        });
        let request = next_request(&mut requests).await;
        respond(
            &client,
            request,
            replay_v2(
                v2_cursor(0),
                v2_cursor(0),
                SessionLifecycleState::Open,
                Vec::new(),
            ),
        );
        let mut stream = subscribing.await.unwrap().unwrap();

        let closing = tokio::spawn({
            let thread = thread.clone();
            async move { thread.close().await }
        });
        let request = next_request(&mut requests).await;
        assert_eq!(request.method, METHOD_SESSION_CLOSE);
        respond(
            &client,
            request,
            CloseSessionResult {
                thread_id: "session".into(),
                closed: true,
            },
        );
        assert!(closing.await.unwrap().unwrap());

        let replay = next_request(&mut requests).await;
        assert_eq!(replay.method, METHOD_SESSION_SUBSCRIBE_V2);
        respond(
            &client,
            replay,
            replay_v2(
                v2_cursor(0),
                v2_cursor(2),
                SessionLifecycleState::Closed,
                vec![
                    lifecycle_event(1, SessionLifecycleState::Closing),
                    lifecycle_event(2, SessionLifecycleState::Closed),
                ],
            ),
        );
        assert_eq!(stream.recv().await.unwrap().unwrap().cursor, v2_cursor(1));
        assert_eq!(stream.recv().await.unwrap().unwrap().cursor, v2_cursor(2));
        assert!(stream.recv().await.is_none());

        let reading = tokio::spawn({
            let view = view.clone();
            async move { view.snapshot().await }
        });
        let request = next_request(&mut requests).await;
        assert_eq!(request.method, METHOD_SESSION_GET_V2);
        let mut closed = v2_snapshot(2, 256);
        closed.lifecycle = SessionLifecycleState::Closed;
        respond(&client, request, closed);
        assert_eq!(
            reading.await.unwrap().unwrap().lifecycle,
            SessionLifecycleState::Closed
        );

        assert!(matches!(
            thread.replace_metadata(2, Map::new()).await,
            Err(SessionManagementError::SessionNotOpen {
                lifecycle: SessionLifecycleState::Closed
            })
        ));
        assert!(requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn v2_replay_keeps_fixed_through_and_repairs_output_and_hub_lag() {
        let (thread, mut requests) = fixture(&[CAPABILITY_SESSION_LIFECYCLE_REPLAY]).await;
        let client = thread.client.clone();
        let view = thread.session_view();
        let subscribing = tokio::spawn({
            let view = view.clone();
            async move {
                view.subscribe_from(v2_cursor(0), SubscriptionOptions::new(1, 128).unwrap())
                    .await
            }
        });
        let first = next_request(&mut requests).await;
        notify_v2(&client, metadata_event(4, "four"));
        respond(
            &client,
            first,
            replay_v2(
                v2_cursor(0),
                v2_cursor(3),
                SessionLifecycleState::Open,
                vec![metadata_event(1, "one")],
            ),
        );
        let mut stream = subscribing.await.unwrap().unwrap();

        let second = next_request(&mut requests).await;
        let params: SubscribeSessionV2Params =
            serde_json::from_value(second.params.clone().unwrap()).unwrap();
        assert_eq!(params.after, v2_cursor(1));
        assert_eq!(params.through, Some(v2_cursor(3)));
        respond(
            &client,
            second,
            replay_v2(
                v2_cursor(1),
                v2_cursor(3),
                SessionLifecycleState::Open,
                vec![metadata_event(2, "two"), metadata_event(3, "three")],
            ),
        );
        for expected in 1..=4 {
            assert_eq!(
                stream.recv().await.unwrap().unwrap().cursor,
                v2_cursor(expected)
            );
        }

        notify_v2(&client, metadata_event(5, "five"));
        while stream.receiver.len() == 0 {
            tokio::task::yield_now().await;
        }
        notify_v2(&client, metadata_event(6, "six"));
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        for seq in 7..=74 {
            notify_v2(&client, metadata_event(seq, &seq.to_string()));
        }
        assert_eq!(stream.recv().await.unwrap().unwrap().cursor, v2_cursor(5));
        let replay = next_request(&mut requests).await;
        let params: SubscribeSessionV2Params =
            serde_json::from_value(replay.params.clone().unwrap()).unwrap();
        assert_eq!(params.after, v2_cursor(6));
        assert!(params.through.is_none());
        respond(
            &client,
            replay,
            replay_v2(
                v2_cursor(6),
                v2_cursor(74),
                SessionLifecycleState::Open,
                (7..=74)
                    .map(|seq| metadata_event(seq, &seq.to_string()))
                    .collect(),
            ),
        );
        for expected in 6..=74 {
            assert_eq!(
                stream.recv().await.unwrap().unwrap().cursor,
                v2_cursor(expected)
            );
        }
        assert_eq!(stream.last_received(), Some(&v2_cursor(74)));
    }

    #[tokio::test]
    async fn v2_subscribers_are_independent_and_drop_stops_only_its_worker() {
        let (thread, mut requests) = fixture(&[CAPABILITY_SESSION_LIFECYCLE_REPLAY]).await;
        let client = thread.client.clone();
        let view = thread.session_view();
        let first = subscribe_v2_empty(
            &view,
            &mut requests,
            v2_cursor(0),
            SubscriptionOptions::new(1, 128).unwrap(),
        )
        .await;
        let mut second = subscribe_v2_empty(
            &view,
            &mut requests,
            v2_cursor(0),
            SubscriptionOptions::new(4, 128).unwrap(),
        )
        .await;
        let first_worker = first.worker.abort_handle();
        drop(first);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !first_worker.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropped V2 stream worker stayed alive");

        notify_v2(&client, metadata_event(1, "one"));
        notify_v2(&client, metadata_event(2, "two"));
        assert_eq!(second.recv().await.unwrap().unwrap().cursor, v2_cursor(1));
        assert_eq!(second.recv().await.unwrap().unwrap().cursor, v2_cursor(2));
    }

    #[tokio::test]
    async fn dropping_the_last_v2_watcher_releases_its_notification_route() {
        let (thread, mut requests) = fixture(&[CAPABILITY_SESSION_LIFECYCLE_REPLAY]).await;
        let client = thread.client.clone();
        let view = thread.session_view();
        let stream = subscribe_v2_empty(
            &view,
            &mut requests,
            v2_cursor(0),
            SubscriptionOptions::default(),
        )
        .await;
        assert!(client
            .inner
            .state
            .session_event_hubs_v2
            .contains_key("session"));

        drop(stream);
        tokio::time::timeout(Duration::from_secs(1), async {
            while client
                .inner
                .state
                .session_event_hubs_v2
                .contains_key("session")
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping the last V2 watcher retained its notification route");
    }
}
