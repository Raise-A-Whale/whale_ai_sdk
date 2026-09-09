//! Authoritative Session snapshots and replayable, independent subscriptions.

use crate::{ClientState, SdkError, WhaleClient, WhaleThread};
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use whale_protocol::runs::RunEvent;
use whale_protocol::session_views::{
    GetSessionParams, ReplayGap, SessionCursor, SessionEventEnvelope, SessionEventPayload,
    SessionSnapshot, SubscribeSessionParams, SubscribeSessionResult,
    CAPABILITY_SESSION_EVENT_REPLAY, CAPABILITY_SESSION_VIEWS, MAX_SESSION_HISTORY_LIMIT,
    MAX_SESSION_REPLAY_PAGE_LIMIT, METHOD_SESSION_GET, METHOD_SESSION_SUBSCRIBE,
};

const DEFAULT_SUBSCRIPTION_BUFFER_CAPACITY: usize = 64;
const DEFAULT_REPLAY_PAGE_SIZE: u32 = 128;
const DEFAULT_SESSION_HISTORY_LIMIT: u32 = 256;
const MAX_SUBSCRIPTION_BUFFER_CAPACITY: usize = 4096;
const SESSION_EVENT_HUB_CAPACITY: usize = 64;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SessionViewError {
    #[error(transparent)]
    Sdk(#[from] SdkError),
    #[error("Daemon does not advertise required capability {capability}")]
    UnsupportedCapability { capability: &'static str },
    #[error("Invalid Session cursor: {message}")]
    InvalidCursor { message: String },
    #[error("Session replay requires a fresh snapshot")]
    ResyncRequired { gap: ReplayGap },
    #[error("Invalid Session projection: {message}")]
    InvalidProjection { message: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionOptions {
    buffer_capacity: NonZeroUsize,
    replay_page_size: NonZeroU32,
}

impl SubscriptionOptions {
    pub fn new(buffer_capacity: usize, replay_page_size: u32) -> Result<Self, SessionViewError> {
        if buffer_capacity == 0 || buffer_capacity > MAX_SUBSCRIPTION_BUFFER_CAPACITY {
            return Err(invalid_configuration(format!(
                "subscription buffer capacity must be between 1 and {MAX_SUBSCRIPTION_BUFFER_CAPACITY}"
            )));
        }
        if replay_page_size == 0 || replay_page_size > MAX_SESSION_REPLAY_PAGE_LIMIT {
            return Err(invalid_configuration(format!(
                "replay page size must be between 1 and {MAX_SESSION_REPLAY_PAGE_LIMIT}"
            )));
        }
        Ok(Self {
            buffer_capacity: NonZeroUsize::new(buffer_capacity).expect("positive buffer capacity"),
            replay_page_size: NonZeroU32::new(replay_page_size).expect("positive replay page size"),
        })
    }

    pub fn buffer_capacity(&self) -> usize {
        self.buffer_capacity.get()
    }

    pub fn replay_page_size(&self) -> u32 {
        self.replay_page_size.get()
    }
}

impl Default for SubscriptionOptions {
    fn default() -> Self {
        Self::new(
            DEFAULT_SUBSCRIPTION_BUFFER_CAPACITY,
            DEFAULT_REPLAY_PAGE_SIZE,
        )
        .expect("default Session subscription options are valid")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionWatchOptions {
    history_limit: NonZeroU32,
    subscription: SubscriptionOptions,
}

impl SessionWatchOptions {
    pub fn new(
        history_limit: u32,
        subscription: SubscriptionOptions,
    ) -> Result<Self, SessionViewError> {
        if history_limit == 0 || history_limit > MAX_SESSION_HISTORY_LIMIT {
            return Err(invalid_configuration(format!(
                "Session history limit must be between 1 and {MAX_SESSION_HISTORY_LIMIT}"
            )));
        }
        Ok(Self {
            history_limit: NonZeroU32::new(history_limit).expect("positive history limit"),
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

impl Default for SessionWatchOptions {
    fn default() -> Self {
        Self::new(
            DEFAULT_SESSION_HISTORY_LIMIT,
            SubscriptionOptions::default(),
        )
        .expect("default Session watch options are valid")
    }
}

pub struct SessionWatch {
    pub snapshot: SessionSnapshot,
    pub events: SessionEventStream,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RunEventEnvelope {
    pub cursor: SessionCursor,
    pub event: RunEvent,
}

pub struct RunEventSubscription {
    events: SessionEventStream,
    turn_id: String,
    last_received: Option<SessionCursor>,
    ended: bool,
}

impl RunEventSubscription {
    pub fn last_received(&self) -> Option<&SessionCursor> {
        self.last_received.as_ref()
    }

    pub async fn recv(&mut self) -> Option<Result<RunEventEnvelope, SessionViewError>> {
        if self.ended {
            return None;
        }
        loop {
            match self.events.recv().await? {
                Err(error) => return Some(Err(error)),
                Ok(envelope) => {
                    let SessionEventPayload::RunEvent { event } = envelope.payload else {
                        continue;
                    };
                    if event.turn_id != self.turn_id {
                        continue;
                    }
                    if matches!(
                        &event.payload,
                        whale_protocol::runs::RunEventPayload::Finished { .. }
                    ) {
                        self.ended = true;
                        self.events.worker.abort();
                    }
                    self.last_received = Some(envelope.cursor.clone());
                    return Some(Ok(RunEventEnvelope {
                        cursor: envelope.cursor,
                        event,
                    }));
                }
            }
        }
    }
}

pub struct SessionEventStream {
    receiver: mpsc::Receiver<Result<SessionEventEnvelope, SessionViewError>>,
    termination: watch::Receiver<Option<SessionHubTermination>>,
    worker: JoinHandle<()>,
    ended: bool,
    last_received: Option<SessionCursor>,
}

impl SessionEventStream {
    /// Last cursor returned successfully by [`Self::recv`].
    pub fn last_received(&self) -> Option<&SessionCursor> {
        self.last_received.as_ref()
    }

    pub async fn recv(&mut self) -> Option<Result<SessionEventEnvelope, SessionViewError>> {
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
                        return Some(Err(SessionViewError::Sdk(SdkError::ChannelClosed(
                            "Session event hub stopped".into(),
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

impl Drop for SessionEventStream {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

#[derive(Clone, Debug)]
enum SessionHubTermination {
    SessionClosed,
    ConnectionLost(String),
}

impl SessionHubTermination {
    fn into_stream_item(self) -> Option<Result<SessionEventEnvelope, SessionViewError>> {
        match self {
            Self::SessionClosed => None,
            Self::ConnectionLost(message) => {
                Some(Err(SessionViewError::Sdk(SdkError::ChannelClosed(message))))
            }
        }
    }
}

pub(crate) struct SessionEventHub {
    events: broadcast::Sender<SessionEventEnvelope>,
    termination: watch::Sender<Option<SessionHubTermination>>,
}

impl SessionEventHub {
    fn new() -> Arc<Self> {
        let (events, _) = broadcast::channel(SESSION_EVENT_HUB_CAPACITY);
        let (termination, _) = watch::channel(None);
        Arc::new(Self {
            events,
            termination,
        })
    }

    fn subscribe(
        &self,
    ) -> (
        broadcast::Receiver<SessionEventEnvelope>,
        watch::Receiver<Option<SessionHubTermination>>,
    ) {
        (self.events.subscribe(), self.termination.subscribe())
    }

    fn close(&self, termination: SessionHubTermination) {
        self.termination.send_replace(Some(termination));
    }
}

impl ClientState {
    pub(crate) fn ensure_session_event_hub(&self, thread_id: &str) -> Arc<SessionEventHub> {
        self.session_event_hubs
            .entry(thread_id.to_owned())
            .or_insert_with(SessionEventHub::new)
            .clone()
    }

    fn subscribe_session_events(
        &self,
        thread_id: &str,
    ) -> Result<
        (
            broadcast::Receiver<SessionEventEnvelope>,
            watch::Receiver<Option<SessionHubTermination>>,
        ),
        SdkError,
    > {
        self.with_session_open(thread_id, || {
            self.ensure_session_event_hub(thread_id).subscribe()
        })
    }

    pub(crate) fn route_session_event(&self, event: SessionEventEnvelope) -> Result<(), String> {
        event.validate()?;
        if let Some(hub) = self.session_event_hubs.get(&event.thread_id) {
            let _ = hub.events.send(event);
        }
        Ok(())
    }

    pub(crate) fn close_session_event_hub(&self, thread_id: &str) {
        if let Some((_, hub)) = self.session_event_hubs.remove(thread_id) {
            hub.close(SessionHubTermination::SessionClosed);
        }
    }

    pub(crate) fn close_all_session_event_hubs(&self, error: Option<String>) {
        let hubs: Vec<_> = self
            .session_event_hubs
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        self.session_event_hubs.clear();
        for hub in hubs {
            hub.close(match &error {
                Some(message) => SessionHubTermination::ConnectionLost(message.clone()),
                None => SessionHubTermination::SessionClosed,
            });
        }
    }
}

impl WhaleClient {
    async fn require_session_capability(
        &self,
        capability: &'static str,
    ) -> Result<(), SessionViewError> {
        let initialized = self.initialize().await?;
        if initialized
            .capabilities
            .iter()
            .any(|advertised| advertised == capability)
        {
            Ok(())
        } else {
            Err(SessionViewError::UnsupportedCapability { capability })
        }
    }

    async fn get_session_snapshot(
        &self,
        thread_id: &str,
        history_limit: u32,
    ) -> Result<SessionSnapshot, SessionViewError> {
        let snapshot: SessionSnapshot = self
            .request_for_session(
                METHOD_SESSION_GET,
                Some(GetSessionParams {
                    thread_id: thread_id.to_owned(),
                    history_limit,
                }),
                thread_id,
            )
            .await?;
        snapshot
            .validate()
            .map_err(|message| SessionViewError::InvalidProjection { message })?;
        if snapshot.summary.thread_id != thread_id {
            return Err(SessionViewError::InvalidProjection {
                message: "Daemon returned a snapshot for another Session".into(),
            });
        }
        if snapshot.history.capacity != history_limit {
            return Err(SessionViewError::InvalidProjection {
                message: "Daemon returned a different Session history window capacity".into(),
            });
        }
        Ok(snapshot)
    }

    async fn get_session_replay_page(
        &self,
        params: &SubscribeSessionParams,
    ) -> Result<SubscribeSessionResult, SessionViewError> {
        let result: Result<SubscribeSessionResult, SdkError> = self
            .request_for_session(
                METHOD_SESSION_SUBSCRIBE,
                Some(params.clone()),
                &params.thread_id,
            )
            .await;
        let result = match result {
            Ok(result) => result,
            Err(SdkError::Rpc { code, message })
                if code == whale_protocol::rpc::JSONRPCError::INVALID_PARAMS =>
            {
                return Err(SessionViewError::InvalidCursor { message });
            }
            Err(error) => return Err(error.into()),
        };
        result
            .validate_for(params)
            .map_err(|message| SessionViewError::InvalidProjection { message })?;
        Ok(result)
    }

    async fn watch_session_with_options(
        &self,
        thread_id: &str,
        options: SessionWatchOptions,
    ) -> Result<SessionWatch, SessionViewError> {
        self.require_session_capability(CAPABILITY_SESSION_VIEWS)
            .await?;
        self.require_session_capability(CAPABILITY_SESSION_EVENT_REPLAY)
            .await?;
        let (live, termination) = self.inner.state.subscribe_session_events(thread_id)?;
        let snapshot = self
            .get_session_snapshot(thread_id, options.history_limit())
            .await?;
        let events = spawn_session_stream(
            self.clone(),
            thread_id.to_owned(),
            snapshot.cursor.clone(),
            live,
            termination,
            options.subscription,
            None,
        );
        Ok(SessionWatch { snapshot, events })
    }

    async fn subscribe_session_from(
        &self,
        thread_id: &str,
        cursor: SessionCursor,
        options: SubscriptionOptions,
    ) -> Result<SessionEventStream, SessionViewError> {
        cursor
            .validate()
            .map_err(|message| SessionViewError::InvalidCursor { message })?;
        if cursor.thread_id != thread_id {
            return Err(SessionViewError::InvalidCursor {
                message: "Session cursor belongs to another thread".into(),
            });
        }
        self.require_session_capability(CAPABILITY_SESSION_VIEWS)
            .await?;
        self.require_session_capability(CAPABILITY_SESSION_EVENT_REPLAY)
            .await?;
        let (live, termination) = self.inner.state.subscribe_session_events(thread_id)?;
        let params = SubscribeSessionParams {
            thread_id: thread_id.to_owned(),
            after: cursor.clone(),
            through: None,
            limit: options.replay_page_size(),
        };
        let page = self.get_session_replay_page(&params).await?;
        if let Some(gap) = page.gap.clone() {
            return Err(SessionViewError::ResyncRequired { gap });
        }
        Ok(spawn_session_stream(
            self.clone(),
            thread_id.to_owned(),
            cursor,
            live,
            termination,
            options,
            Some((params, page)),
        ))
    }

    pub(crate) async fn subscribe_run_events(
        &self,
        thread_id: &str,
        turn_id: &str,
        after: Option<SessionCursor>,
        options: SubscriptionOptions,
    ) -> Result<RunEventSubscription, SessionViewError> {
        let events = match after {
            Some(cursor) => {
                self.subscribe_session_from(thread_id, cursor, options)
                    .await?
            }
            None => {
                self.watch_session_with_options(
                    thread_id,
                    SessionWatchOptions::new(DEFAULT_SESSION_HISTORY_LIMIT, options)?,
                )
                .await?
                .events
            }
        };
        Ok(RunEventSubscription {
            events,
            turn_id: turn_id.to_owned(),
            last_received: None,
            ended: false,
        })
    }
}

impl WhaleThread {
    pub async fn snapshot(&self) -> Result<SessionSnapshot, SessionViewError> {
        self.snapshot_with_history_limit(
            NonZeroU32::new(DEFAULT_SESSION_HISTORY_LIMIT).expect("positive default"),
        )
        .await
    }

    pub async fn snapshot_with_history_limit(
        &self,
        history_limit: NonZeroU32,
    ) -> Result<SessionSnapshot, SessionViewError> {
        if history_limit.get() > MAX_SESSION_HISTORY_LIMIT {
            return Err(invalid_configuration(format!(
                "Session history limit must be between 1 and {MAX_SESSION_HISTORY_LIMIT}"
            )));
        }
        self.client
            .require_session_capability(CAPABILITY_SESSION_VIEWS)
            .await?;
        self.client
            .get_session_snapshot(&self.thread_id, history_limit.get())
            .await
    }

    pub async fn watch(&self) -> Result<SessionWatch, SessionViewError> {
        self.watch_with_options(SessionWatchOptions::default())
            .await
    }

    pub async fn watch_with_options(
        &self,
        options: SessionWatchOptions,
    ) -> Result<SessionWatch, SessionViewError> {
        self.client
            .watch_session_with_options(&self.thread_id, options)
            .await
    }

    pub async fn subscribe_from(
        &self,
        cursor: SessionCursor,
        options: SubscriptionOptions,
    ) -> Result<SessionEventStream, SessionViewError> {
        self.client
            .subscribe_session_from(&self.thread_id, cursor, options)
            .await
    }
}

fn spawn_session_stream(
    client: WhaleClient,
    thread_id: String,
    cursor: SessionCursor,
    live: broadcast::Receiver<SessionEventEnvelope>,
    termination: watch::Receiver<Option<SessionHubTermination>>,
    options: SubscriptionOptions,
    first_page: Option<(SubscribeSessionParams, SubscribeSessionResult)>,
) -> SessionEventStream {
    let (output, receiver) = mpsc::channel(options.buffer_capacity());
    let worker_termination = termination.clone();
    let worker = tokio::spawn(run_subscription_worker(
        client,
        thread_id,
        cursor,
        live,
        worker_termination,
        output,
        options.replay_page_size(),
        first_page,
    ));
    SessionEventStream {
        receiver,
        termination,
        worker,
        ended: false,
        last_received: None,
    }
}

async fn run_subscription_worker(
    client: WhaleClient,
    thread_id: String,
    mut last_enqueued: SessionCursor,
    mut live: broadcast::Receiver<SessionEventEnvelope>,
    mut termination: watch::Receiver<Option<SessionHubTermination>>,
    output: mpsc::Sender<Result<SessionEventEnvelope, SessionViewError>>,
    page_size: u32,
    first_page: Option<(SubscribeSessionParams, SubscribeSessionResult)>,
) {
    match replay_fixed_window(
        &client,
        &thread_id,
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
            send_worker_error(&output, &mut termination, error).await;
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
            received = live.recv() => {
                let catch_up = match received {
                    Ok(event) if same_stream(&event.cursor, &last_enqueued)
                        && event.cursor.seq <= last_enqueued.seq => false,
                    Ok(event) if is_next_cursor(&last_enqueued, &event.cursor) => {
                        if !enqueue_event(&output, &mut termination, &mut last_enqueued, event).await {
                            return;
                        }
                        false
                    }
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => true,
                    Err(broadcast::error::RecvError::Closed) => return,
                };
                if catch_up {
                    match replay_fixed_window(
                        &client,
                        &thread_id,
                        page_size,
                        &mut last_enqueued,
                        &output,
                        &mut termination,
                        None,
                    ).await {
                        Ok(true) => {}
                        Ok(false) => return,
                        Err(error) => {
                            send_worker_error(&output, &mut termination, error).await;
                            return;
                        }
                    }
                }
            }
        }
    }
}

async fn replay_fixed_window(
    client: &WhaleClient,
    thread_id: &str,
    page_size: u32,
    last_enqueued: &mut SessionCursor,
    output: &mpsc::Sender<Result<SessionEventEnvelope, SessionViewError>>,
    termination: &mut watch::Receiver<Option<SessionHubTermination>>,
    first_page: Option<(SubscribeSessionParams, SubscribeSessionResult)>,
) -> Result<bool, SessionViewError> {
    let (mut params, mut page) = match first_page {
        Some(first) => first,
        None => {
            let params = SubscribeSessionParams {
                thread_id: thread_id.to_owned(),
                after: last_enqueued.clone(),
                through: None,
                limit: page_size,
            };
            let Some(page) = request_replay_page(client, &params, termination).await? else {
                return Ok(false);
            };
            (params, page)
        }
    };

    loop {
        page.validate_for(&params)
            .map_err(|message| SessionViewError::InvalidProjection { message })?;
        if let Some(gap) = page.gap.clone() {
            return Err(SessionViewError::ResyncRequired { gap });
        }
        if page.has_more && page.resume_after == params.after {
            return Err(SessionViewError::InvalidProjection {
                message: "Replay page claimed more events without advancing its cursor".into(),
            });
        }
        let through = page.through.clone();
        for event in page.events {
            if !is_next_cursor(last_enqueued, &event.cursor) {
                return Err(SessionViewError::InvalidProjection {
                    message: format!(
                        "Replay event is not contiguous after Session cursor {}",
                        last_enqueued.seq
                    ),
                });
            }
            if !enqueue_event(output, termination, last_enqueued, event).await {
                return Ok(false);
            }
        }
        if !page.has_more {
            return Ok(true);
        }
        params = SubscribeSessionParams {
            thread_id: thread_id.to_owned(),
            after: page.resume_after,
            through: Some(through),
            limit: page_size,
        };
        let Some(next) = request_replay_page(client, &params, termination).await? else {
            return Ok(false);
        };
        page = next;
    }
}

async fn request_replay_page(
    client: &WhaleClient,
    params: &SubscribeSessionParams,
    termination: &mut watch::Receiver<Option<SessionHubTermination>>,
) -> Result<Option<SubscribeSessionResult>, SessionViewError> {
    let request = client.get_session_replay_page(params);
    tokio::pin!(request);
    tokio::select! {
        biased;
        _ = termination.changed() => Ok(None),
        result = &mut request => result.map(Some),
    }
}

async fn enqueue_event(
    output: &mpsc::Sender<Result<SessionEventEnvelope, SessionViewError>>,
    termination: &mut watch::Receiver<Option<SessionHubTermination>>,
    last_enqueued: &mut SessionCursor,
    event: SessionEventEnvelope,
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

async fn send_worker_error(
    output: &mpsc::Sender<Result<SessionEventEnvelope, SessionViewError>>,
    termination: &mut watch::Receiver<Option<SessionHubTermination>>,
    error: SessionViewError,
) {
    let send = output.send(Err(error));
    tokio::pin!(send);
    tokio::select! {
        biased;
        _ = termination.changed() => {}
        _ = &mut send => {}
    }
}

fn same_stream(left: &SessionCursor, right: &SessionCursor) -> bool {
    left.thread_id == right.thread_id && left.stream_id == right.stream_id
}

fn is_next_cursor(previous: &SessionCursor, next: &SessionCursor) -> bool {
    same_stream(previous, next)
        && previous
            .seq
            .checked_add(1)
            .is_some_and(|expected| expected == next.seq)
}

fn invalid_configuration(message: String) -> SessionViewError {
    SessionViewError::Sdk(SdkError::InvalidConfiguration(message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        run::RunState, ClientInner, ClientState, ManagedWriter, RunHandle, WhaleClient, WhaleThread,
    };
    use std::{sync::Arc, time::Duration};
    use tokio::sync::mpsc;
    use whale_protocol::{
        initialization::{InitializeParams, InitializeResult, PeerInfo},
        rpc::{JSONRPCNotification, JSONRPCRequest, JSONRPCResponse},
        runs::{RunEvent, RunEventPayload, RunSnapshot, RunStatus},
        session_views::{
            ReplayGap, ReplayGapReason, SessionCursor, SessionEventEnvelope, SessionEventPayload,
            SessionHistoryWindow, SessionRunView, SessionSnapshot, SessionSummary,
            SubscribeSessionParams, SubscribeSessionResult, CAPABILITY_SESSION_EVENT_REPLAY,
            CAPABILITY_SESSION_VIEWS, METHOD_SESSION_EVENT, METHOD_SESSION_GET,
            METHOD_SESSION_SUBSCRIBE,
        },
        sessions::{CloseSessionResult, METHOD_SESSION_CLOSE},
        AgentStreamEvent,
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

    fn notify(client: &WhaleClient, event: SessionEventEnvelope) {
        let notification = JSONRPCNotification::new(METHOD_SESSION_EVENT, Some(event)).unwrap();
        client.inner.state.incoming(
            &serde_json::to_string(&notification).unwrap(),
            &client.inner.writer,
        );
    }

    async fn fixture(view_capabilities: bool) -> (WhaleThread, mpsc::Receiver<String>) {
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
                name: "view-peer".into(),
                version: "test".into(),
            },
        )
        .unwrap();
        if view_capabilities {
            result.capabilities.extend([
                CAPABILITY_SESSION_VIEWS.into(),
                CAPABILITY_SESSION_EVENT_REPLAY.into(),
            ]);
        }
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

    fn cursor(seq: u64) -> SessionCursor {
        SessionCursor {
            thread_id: "session".into(),
            stream_id: "stream".into(),
            seq,
        }
    }

    fn snapshot(seq: u64) -> SessionSnapshot {
        let mut summary = SessionSummary::new("session", 1);
        summary.revision = seq;
        summary.updated_at_ms = 1 + seq;
        SessionSnapshot::new(
            summary,
            SessionHistoryWindow::new(256).unwrap(),
            cursor(seq),
        )
    }

    fn active_event(seq: u64, turn_id: &str) -> SessionEventEnvelope {
        let run = RunSnapshot {
            thread_id: "session".into(),
            turn_id: turn_id.into(),
            status: RunStatus::Running,
            items: Vec::new(),
            usage: Default::default(),
            pending_approvals: Vec::new(),
            tool_executions: Vec::new(),
            last_seq: 0,
            result: None,
            error: None,
        };
        SessionEventEnvelope::new(
            "session",
            cursor(seq),
            1 + seq,
            SessionEventPayload::RunChanged {
                run: SessionRunView::new(run, 1 + seq),
            },
        )
    }

    fn run_event(session_seq: u64, turn_id: &str, run_seq: u64) -> SessionEventEnvelope {
        SessionEventEnvelope::new(
            "session",
            cursor(session_seq),
            1 + session_seq,
            SessionEventPayload::RunEvent {
                event: RunEvent {
                    thread_id: "session".into(),
                    turn_id: turn_id.into(),
                    seq: run_seq,
                    payload: RunEventPayload::Stream {
                        event: AgentStreamEvent::TextDelta {
                            turn_id: turn_id.into(),
                            item_id: format!("item-{turn_id}"),
                            delta: format!("event-{session_seq}"),
                        },
                    },
                },
            },
        )
    }

    fn finished_run_event(session_seq: u64, turn_id: &str, run_seq: u64) -> SessionEventEnvelope {
        let result = whale_protocol::rpc::RunTurnResult {
            thread_id: "session".into(),
            turn_id: turn_id.into(),
            status: whale_protocol::TurnStatus::Completed,
            items: Vec::new(),
            usage: Default::default(),
        };
        SessionEventEnvelope::new(
            "session",
            cursor(session_seq),
            1 + session_seq,
            SessionEventPayload::RunEvent {
                event: RunEvent {
                    thread_id: "session".into(),
                    turn_id: turn_id.into(),
                    seq: run_seq,
                    payload: RunEventPayload::Finished {
                        snapshot: RunSnapshot {
                            thread_id: "session".into(),
                            turn_id: turn_id.into(),
                            status: RunStatus::Completed,
                            items: Vec::new(),
                            usage: Default::default(),
                            pending_approvals: Vec::new(),
                            tool_executions: Vec::new(),
                            last_seq: run_seq,
                            result: Some(result),
                            error: None,
                        },
                    },
                },
            },
        )
    }

    fn replay_page(
        after: SessionCursor,
        through: SessionCursor,
        events: Vec<SessionEventEnvelope>,
    ) -> SubscribeSessionResult {
        let resume_after = events
            .last()
            .map(|event| event.cursor.clone())
            .unwrap_or(after);
        let has_more = resume_after.seq < through.seq;
        SubscribeSessionResult {
            events,
            resume_after,
            through,
            has_more,
            gap: None,
        }
    }

    async fn subscribe_empty(
        thread: &WhaleThread,
        requests: &mut mpsc::Receiver<String>,
        after: SessionCursor,
        options: SubscriptionOptions,
    ) -> SessionEventStream {
        let client = thread.client.clone();
        let subscribing = tokio::spawn({
            let thread = thread.clone();
            let after = after.clone();
            async move { thread.subscribe_from(after, options).await }
        });
        let request = next_request(requests).await;
        assert_eq!(request.method, METHOD_SESSION_SUBSCRIBE);
        respond(
            &client,
            request,
            replay_page(after.clone(), after, Vec::new()),
        );
        subscribing.await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn missing_optional_capability_fails_before_session_get_is_sent() {
        let (thread, mut requests) = fixture(false).await;

        assert!(matches!(
            thread.snapshot().await,
            Err(SessionViewError::UnsupportedCapability {
                capability: CAPABILITY_SESSION_VIEWS
            })
        ));
        assert!(requests.try_recv().is_err());
    }

    #[test]
    fn session_preparation_installs_the_hub_before_the_business_request() {
        let state = ClientState::new();
        state.prepare_session("new-session").unwrap();
        assert!(state.session_event_hubs.contains_key("new-session"));
    }

    #[tokio::test]
    async fn watch_routes_an_event_received_before_the_snapshot_response_once() {
        let (thread, mut requests) = fixture(true).await;
        let client = thread.client.clone();
        let watching = tokio::spawn(async move { thread.watch().await });
        let request = next_request(&mut requests).await;
        assert_eq!(request.method, METHOD_SESSION_GET);
        assert_eq!(request.params.as_ref().unwrap()["history_limit"], 256);

        notify(&client, active_event(2, "run-a"));
        respond(&client, request, snapshot(1));

        let mut watch = watching.await.unwrap().unwrap();
        assert_eq!(watch.snapshot.cursor, cursor(1));
        let barrier = next_request(&mut requests).await;
        let barrier_params: SubscribeSessionParams =
            serde_json::from_value(barrier.params.clone().unwrap()).unwrap();
        assert_eq!(barrier_params.after, cursor(1));
        assert_eq!(barrier_params.through, None);
        respond(
            &client,
            barrier,
            replay_page(cursor(1), cursor(2), vec![active_event(2, "run-a")]),
        );

        assert_eq!(
            watch.events.recv().await.unwrap().unwrap().cursor,
            cursor(2)
        );
        assert_eq!(watch.events.last_received(), Some(&cursor(2)));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), watch.events.recv())
                .await
                .is_err()
        );
        notify(&client, active_event(3, "run-a"));
        assert_eq!(
            watch.events.recv().await.unwrap().unwrap().cursor,
            cursor(3)
        );
        assert_eq!(watch.events.last_received(), Some(&cursor(3)));
    }

    #[tokio::test]
    async fn watch_replays_a_journaled_event_when_the_live_notification_is_missing() {
        let (thread, mut requests) = fixture(true).await;
        let client = thread.client.clone();
        let watching = tokio::spawn(async move { thread.watch().await });
        let snapshot_request = next_request(&mut requests).await;
        respond(&client, snapshot_request, snapshot(1));

        let mut watch = watching.await.unwrap().unwrap();
        let barrier = next_request(&mut requests).await;
        let params: SubscribeSessionParams =
            serde_json::from_value(barrier.params.clone().unwrap()).unwrap();
        assert_eq!(params.after, cursor(1));
        assert_eq!(params.through, None);
        respond(
            &client,
            barrier,
            replay_page(cursor(1), cursor(2), vec![active_event(2, "run-a")]),
        );

        assert_eq!(
            watch.events.recv().await.unwrap().unwrap().cursor,
            cursor(2)
        );
    }

    #[tokio::test]
    async fn subscribe_reuses_fixed_through_and_merges_concurrent_live_once() {
        let (thread, mut requests) = fixture(true).await;
        let client = thread.client.clone();
        let subscribing = tokio::spawn(async move {
            thread
                .subscribe_from(cursor(0), SubscriptionOptions::default())
                .await
        });
        let first = next_request(&mut requests).await;
        assert_eq!(first.method, METHOD_SESSION_SUBSCRIBE);
        let first_params: SubscribeSessionParams =
            serde_json::from_value(first.params.clone().unwrap()).unwrap();
        assert_eq!(first_params.after, cursor(0));
        assert_eq!(first_params.through, None);
        assert_eq!(first_params.limit, 128);

        notify(&client, active_event(4, "run-a"));
        respond(
            &client,
            first,
            replay_page(cursor(0), cursor(3), vec![active_event(1, "run-a")]),
        );
        let mut stream = subscribing.await.unwrap().unwrap();

        let second = next_request(&mut requests).await;
        let second_params: SubscribeSessionParams =
            serde_json::from_value(second.params.clone().unwrap()).unwrap();
        assert_eq!(second_params.after, cursor(1));
        assert_eq!(second_params.through, Some(cursor(3)));
        respond(
            &client,
            second,
            replay_page(
                cursor(1),
                cursor(3),
                vec![active_event(2, "run-a"), active_event(3, "run-a")],
            ),
        );

        let mut observed = Vec::new();
        for _ in 0..4 {
            observed.push(stream.recv().await.unwrap().unwrap().cursor.seq);
        }
        assert_eq!(observed, vec![1, 2, 3, 4]);
        assert!(requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn initial_replay_gap_returns_typed_resync() {
        let (thread, mut requests) = fixture(true).await;
        let client = thread.client.clone();
        let subscribing = tokio::spawn(async move {
            thread
                .subscribe_from(cursor(0), SubscriptionOptions::default())
                .await
        });
        let request = next_request(&mut requests).await;
        let gap = ReplayGap {
            reason: ReplayGapReason::Retention,
            requested: cursor(0),
            replay_floor: cursor(3),
            current: cursor(5),
            session_revision: 5,
        };
        respond(
            &client,
            request,
            SubscribeSessionResult {
                events: Vec::new(),
                resume_after: cursor(0),
                through: cursor(5),
                has_more: false,
                gap: Some(gap.clone()),
            },
        );
        match subscribing.await.unwrap() {
            Err(SessionViewError::ResyncRequired { gap: observed }) => {
                assert_eq!(observed, gap)
            }
            _ => panic!("initial replay gap did not produce typed resync"),
        }
    }

    #[tokio::test]
    async fn run_subscription_scans_other_runs_and_resumes_from_the_exposed_cursor() {
        let (thread, mut requests) = fixture(true).await;
        let client = thread.client.clone();
        let handle = RunHandle {
            client: client.clone(),
            state: RunState::new("session".into(), "run-a".into()),
        };

        let subscribing = tokio::spawn({
            let handle = handle.clone();
            async move {
                handle
                    .subscribe_events(Some(cursor(0)), SubscriptionOptions::default())
                    .await
            }
        });
        let request = next_request(&mut requests).await;
        respond(
            &client,
            request,
            replay_page(cursor(0), cursor(0), Vec::new()),
        );
        let mut events = subscribing.await.unwrap().unwrap();

        notify(&client, run_event(1, "run-b", 1));
        notify(&client, run_event(2, "run-a", 1));
        let first = events.recv().await.unwrap().unwrap();
        assert_eq!(first.cursor, cursor(2));
        assert_eq!(first.event.turn_id, "run-a");

        drop(events);
        let resuming = tokio::spawn(async move {
            handle
                .subscribe_events(Some(cursor(2)), SubscriptionOptions::default())
                .await
        });
        let request = next_request(&mut requests).await;
        let params: SubscribeSessionParams =
            serde_json::from_value(request.params.clone().unwrap()).unwrap();
        assert_eq!(params.after, cursor(2));
        respond(
            &client,
            request,
            replay_page(
                cursor(2),
                cursor(4),
                vec![run_event(3, "run-b", 2), run_event(4, "run-a", 2)],
            ),
        );
        let mut resumed = resuming.await.unwrap().unwrap();
        let second = resumed.recv().await.unwrap().unwrap();
        assert_eq!(second.cursor, cursor(4));
        assert_eq!(second.event.turn_id, "run-a");
        assert_eq!(resumed.last_received(), Some(&cursor(4)));
    }

    #[tokio::test]
    async fn run_subscription_without_a_cursor_starts_after_a_fresh_snapshot() {
        let (thread, mut requests) = fixture(true).await;
        let client = thread.client.clone();
        let handle = RunHandle {
            client: client.clone(),
            state: RunState::new("session".into(), "run-a".into()),
        };
        let subscribing = tokio::spawn(async move {
            handle
                .subscribe_events(None, SubscriptionOptions::default())
                .await
        });

        let get = next_request(&mut requests).await;
        assert_eq!(get.method, METHOD_SESSION_GET);
        respond(&client, get, snapshot(1));
        let mut events = subscribing.await.unwrap().unwrap();

        let barrier = next_request(&mut requests).await;
        let params: SubscribeSessionParams =
            serde_json::from_value(barrier.params.clone().unwrap()).unwrap();
        assert_eq!(params.after, cursor(1));
        respond(
            &client,
            barrier,
            replay_page(
                cursor(1),
                cursor(3),
                vec![run_event(2, "run-b", 1), run_event(3, "run-a", 1)],
            ),
        );
        let received = events.recv().await.unwrap().unwrap();
        assert_eq!(received.cursor, cursor(3));
        assert_eq!(received.event.turn_id, "run-a");
    }

    #[tokio::test]
    async fn run_subscription_ends_after_returning_its_matching_finished_event() {
        let (thread, mut requests) = fixture(true).await;
        let client = thread.client.clone();
        let handle = RunHandle {
            client: client.clone(),
            state: RunState::new("session".into(), "run-a".into()),
        };
        let subscribing = tokio::spawn(async move {
            handle
                .subscribe_events(Some(cursor(0)), SubscriptionOptions::default())
                .await
        });
        let request = next_request(&mut requests).await;
        respond(
            &client,
            request,
            replay_page(
                cursor(0),
                cursor(1),
                vec![finished_run_event(1, "run-a", 1)],
            ),
        );
        let mut events = subscribing.await.unwrap().unwrap();
        let finished = events.recv().await.unwrap().unwrap();
        assert_eq!(finished.cursor, cursor(1));
        assert!(matches!(
            finished.event.payload,
            RunEventPayload::Finished { .. }
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), events.recv())
                .await
                .expect("Run subscription stayed alive after Finished")
                .is_none()
        );
    }

    #[tokio::test]
    async fn an_unpolled_subscriber_does_not_delay_an_independent_subscriber() {
        let (thread, mut requests) = fixture(true).await;
        let client = thread.client.clone();
        let _slow = subscribe_empty(
            &thread,
            &mut requests,
            cursor(0),
            SubscriptionOptions::new(1, 128).unwrap(),
        )
        .await;
        let mut fast = subscribe_empty(
            &thread,
            &mut requests,
            cursor(0),
            SubscriptionOptions::new(4, 128).unwrap(),
        )
        .await;

        notify(&client, active_event(1, "run-a"));
        while fast.receiver.len() == 0 {
            tokio::task::yield_now().await;
        }
        notify(&client, active_event(2, "run-a"));

        for expected in 1..=2 {
            let received = tokio::time::timeout(Duration::from_secs(1), fast.recv())
                .await
                .expect("fast subscriber was delayed")
                .unwrap()
                .unwrap();
            assert_eq!(received.cursor, cursor(expected));
        }
    }

    #[tokio::test]
    async fn output_backlog_and_hub_lag_replay_each_event_once() {
        let (thread, mut requests) = fixture(true).await;
        let client = thread.client.clone();
        let mut stream = subscribe_empty(
            &thread,
            &mut requests,
            cursor(0),
            SubscriptionOptions::new(1, 128).unwrap(),
        )
        .await;

        notify(&client, active_event(1, "run-a"));
        while stream.receiver.len() == 0 {
            tokio::task::yield_now().await;
        }
        notify(&client, active_event(2, "run-a"));
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        for seq in 3..=70 {
            notify(&client, active_event(seq, "run-a"));
        }

        assert_eq!(stream.recv().await.unwrap().unwrap().cursor, cursor(1));
        let replay = next_request(&mut requests).await;
        let params: SubscribeSessionParams =
            serde_json::from_value(replay.params.clone().unwrap()).unwrap();
        assert_eq!(params.after, cursor(2));
        assert_eq!(params.through, None);
        respond(
            &client,
            replay,
            replay_page(
                cursor(2),
                cursor(70),
                (3..=70).map(|seq| active_event(seq, "run-a")).collect(),
            ),
        );

        let mut observed = vec![1];
        for _ in 2..=70 {
            observed.push(stream.recv().await.unwrap().unwrap().cursor.seq);
        }
        assert_eq!(observed, (1..=70).collect::<Vec<_>>());
        assert_eq!(stream.last_received(), Some(&cursor(70)));
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert!(requests.try_recv().is_err());

        notify(&client, active_event(71, "run-a"));
        assert_eq!(stream.recv().await.unwrap().unwrap().cursor, cursor(71));
    }

    #[tokio::test]
    async fn a_gap_during_lag_recovery_is_emitted_once_and_ends_the_stream() {
        let (thread, mut requests) = fixture(true).await;
        let client = thread.client.clone();
        let mut stream = subscribe_empty(
            &thread,
            &mut requests,
            cursor(0),
            SubscriptionOptions::default(),
        )
        .await;

        notify(&client, active_event(2, "run-a"));
        let request = next_request(&mut requests).await;
        let gap = ReplayGap {
            reason: ReplayGapReason::Retention,
            requested: cursor(0),
            replay_floor: cursor(1),
            current: cursor(2),
            session_revision: 2,
        };
        respond(
            &client,
            request,
            SubscribeSessionResult {
                events: Vec::new(),
                resume_after: cursor(0),
                through: cursor(2),
                has_more: false,
                gap: Some(gap.clone()),
            },
        );

        match stream.recv().await {
            Some(Err(SessionViewError::ResyncRequired { gap: observed })) => {
                assert_eq!(observed, gap)
            }
            _ => panic!("lag recovery did not expose its replay gap"),
        }
        assert!(stream.recv().await.is_none());
    }

    #[tokio::test]
    async fn explicit_session_close_ends_stream_without_an_error() {
        let (thread, mut requests) = fixture(true).await;
        let client = thread.client.clone();
        let mut stream = subscribe_empty(
            &thread,
            &mut requests,
            cursor(0),
            SubscriptionOptions::default(),
        )
        .await;

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
        assert!(stream.recv().await.is_none());
        assert!(stream.recv().await.is_none());
    }

    #[tokio::test]
    async fn unexpected_disconnect_emits_one_error_then_ends() {
        let (thread, mut requests) = fixture(true).await;
        let mut stream = subscribe_empty(
            &thread,
            &mut requests,
            cursor(0),
            SubscriptionOptions::default(),
        )
        .await;

        thread.client.inner.state.disconnect("peer lost");
        assert!(matches!(
            stream.recv().await,
            Some(Err(SessionViewError::Sdk(SdkError::ChannelClosed(message))))
                if message == "peer lost"
        ));
        assert!(stream.recv().await.is_none());
    }

    #[tokio::test]
    async fn dropping_a_stream_cancels_its_worker() {
        let (thread, mut requests) = fixture(true).await;
        let stream = subscribe_empty(
            &thread,
            &mut requests,
            cursor(0),
            SubscriptionOptions::default(),
        )
        .await;
        let worker = stream.worker.abort_handle();
        assert!(!worker.is_finished());

        drop(stream);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !worker.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("subscription worker did not stop after stream drop");
    }
}
