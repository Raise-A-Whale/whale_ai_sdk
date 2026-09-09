//! Optional, session-scoped Interaction snapshots and resumable event streams.

use crate::{ClientState, SdkError, WhaleClient, WhaleThread};
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::{Arc, Weak};
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use whale_protocol::interactions::*;

const INTERACTION_EVENT_HUB_CAPACITY: usize = 64;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum InteractionViewError {
    #[error(transparent)]
    Sdk(#[from] SdkError),
    #[error("Daemon does not advertise required capability {capability}")]
    UnsupportedCapability { capability: &'static str },
    #[error("Interactions are not enabled for Session {thread_id}")]
    NotEnabled { thread_id: String },
    #[error("Invalid Interaction cursor: {message}")]
    InvalidCursor { message: String },
    #[error("Interaction replay requires the attached authoritative snapshot")]
    ResyncRequired {
        gap: InteractionReplayGap,
        snapshot: InteractionSnapshot,
    },
    #[error("Invalid Interaction projection: {message}")]
    InvalidProjection { message: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InteractionSubscriptionOptions {
    output_capacity: NonZeroUsize,
    replay_page_limit: NonZeroU32,
}

impl InteractionSubscriptionOptions {
    pub fn new(
        output_capacity: usize,
        replay_page_limit: u32,
    ) -> Result<Self, InteractionViewError> {
        validate_interaction_subscriber_output_capacity(output_capacity)
            .map_err(invalid_configuration)?;
        validate_interaction_replay_page_limit(replay_page_limit).map_err(invalid_configuration)?;
        Ok(Self {
            output_capacity: NonZeroUsize::new(output_capacity)
                .expect("validated Interaction output capacity is positive"),
            replay_page_limit: NonZeroU32::new(replay_page_limit)
                .expect("validated Interaction replay limit is positive"),
        })
    }

    pub fn output_capacity(&self) -> usize {
        self.output_capacity.get()
    }

    pub fn replay_page_limit(&self) -> u32 {
        self.replay_page_limit.get()
    }
}

impl Default for InteractionSubscriptionOptions {
    fn default() -> Self {
        Self::new(
            DEFAULT_INTERACTION_SUBSCRIBER_OUTPUT_CAPACITY,
            DEFAULT_INTERACTION_REPLAY_PAGE_LIMIT,
        )
        .expect("default Interaction subscription options are valid")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InteractionWatchOptions {
    subscription: InteractionSubscriptionOptions,
}

impl InteractionWatchOptions {
    pub fn new(
        output_capacity: usize,
        replay_page_limit: u32,
    ) -> Result<Self, InteractionViewError> {
        Ok(Self {
            subscription: InteractionSubscriptionOptions::new(output_capacity, replay_page_limit)?,
        })
    }

    pub fn output_capacity(&self) -> usize {
        self.subscription.output_capacity()
    }

    pub fn replay_page_limit(&self) -> u32 {
        self.subscription.replay_page_limit()
    }

    pub fn subscription(&self) -> &InteractionSubscriptionOptions {
        &self.subscription
    }
}

impl Default for InteractionWatchOptions {
    fn default() -> Self {
        Self {
            subscription: InteractionSubscriptionOptions::default(),
        }
    }
}

pub struct InteractionWatch {
    pub snapshot: InteractionSnapshot,
    pub events: InteractionEventStream,
}

pub struct InteractionEventStream {
    receiver: mpsc::Receiver<Result<InteractionEventEnvelope, InteractionViewError>>,
    termination: watch::Receiver<Option<InteractionHubTermination>>,
    worker: JoinHandle<()>,
    _hub: Arc<InteractionEventHub>,
    ended: bool,
    last_received: Option<InteractionCursor>,
}

impl InteractionEventStream {
    pub fn last_received(&self) -> Option<&InteractionCursor> {
        self.last_received.as_ref()
    }

    pub async fn recv(&mut self) -> Option<Result<InteractionEventEnvelope, InteractionViewError>> {
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
                        return Some(Err(InteractionViewError::Sdk(SdkError::ChannelClosed(
                            "Interaction event hub stopped".into(),
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
                            self.ended = true;
                            return self
                                .termination
                                .borrow()
                                .clone()
                                .and_then(InteractionHubTermination::into_stream_item);
                        }
                    }
                }
            }
        }
    }
}

impl Drop for InteractionEventStream {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

#[derive(Clone, Debug)]
enum InteractionHubTermination {
    SessionClosed,
    ConnectionLost(String),
}

impl InteractionHubTermination {
    fn into_stream_item(self) -> Option<Result<InteractionEventEnvelope, InteractionViewError>> {
        match self {
            Self::SessionClosed => None,
            Self::ConnectionLost(message) => Some(Err(InteractionViewError::Sdk(
                SdkError::ChannelClosed(message),
            ))),
        }
    }
}

pub(crate) struct InteractionEventHub {
    events: broadcast::Sender<InteractionEventEnvelope>,
    termination: watch::Sender<Option<InteractionHubTermination>>,
    owner: Weak<ClientState>,
    thread_id: String,
}

impl InteractionEventHub {
    fn new(owner: &Arc<ClientState>, thread_id: &str) -> Arc<Self> {
        let (events, _) = broadcast::channel(INTERACTION_EVENT_HUB_CAPACITY);
        let (termination, _) = watch::channel(None);
        Arc::new(Self {
            events,
            termination,
            owner: Arc::downgrade(owner),
            thread_id: thread_id.to_owned(),
        })
    }

    fn subscribe(
        &self,
    ) -> (
        broadcast::Receiver<InteractionEventEnvelope>,
        watch::Receiver<Option<InteractionHubTermination>>,
    ) {
        (self.events.subscribe(), self.termination.subscribe())
    }

    fn close(&self, termination: InteractionHubTermination) {
        self.termination.send_replace(Some(termination));
    }
}

impl Drop for InteractionEventHub {
    fn drop(&mut self) {
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        let self_ptr = self as *const Self;
        owner
            .interaction_event_hubs
            .remove_if(&self.thread_id, |_, registered| {
                registered.as_ptr() == self_ptr
            });
    }
}

impl ClientState {
    pub(crate) fn enable_interactions(&self, thread_id: &str) -> Result<(), SdkError> {
        self.with_existing_session_open(thread_id, || {
            self.interaction_sessions.insert(thread_id.to_owned(), ());
        })
    }

    pub(crate) fn interactions_enabled(&self, thread_id: &str) -> bool {
        self.interaction_sessions.contains_key(thread_id)
    }

    pub(crate) fn supports_interactions(&self) -> bool {
        self.initialization
            .get()
            .and_then(|completion| completion.borrow().clone())
            .and_then(Result::ok)
            .is_some_and(|initialized| {
                initialized
                    .capabilities
                    .iter()
                    .any(|capability| capability == CAPABILITY_INTERACTIONS)
            })
    }

    fn ensure_interaction_event_hub(self: &Arc<Self>, thread_id: &str) -> Arc<InteractionEventHub> {
        match self.interaction_event_hubs.entry(thread_id.to_owned()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                if let Some(hub) = entry.get().upgrade() {
                    hub
                } else {
                    let hub = InteractionEventHub::new(self, thread_id);
                    entry.insert(Arc::downgrade(&hub));
                    hub
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let hub = InteractionEventHub::new(self, thread_id);
                entry.insert(Arc::downgrade(&hub));
                hub
            }
        }
    }

    fn subscribe_interaction_events(
        self: &Arc<Self>,
        thread_id: &str,
    ) -> Result<
        (
            Arc<InteractionEventHub>,
            broadcast::Receiver<InteractionEventEnvelope>,
            watch::Receiver<Option<InteractionHubTermination>>,
        ),
        InteractionViewError,
    > {
        self.with_existing_session_open(thread_id, || {
            if !self.interactions_enabled(thread_id) {
                return Err(InteractionViewError::NotEnabled {
                    thread_id: thread_id.to_owned(),
                });
            }
            let hub = self.ensure_interaction_event_hub(thread_id);
            let (events, termination) = hub.subscribe();
            Ok((hub, events, termination))
        })?
    }

    pub(crate) fn route_interaction_event(
        &self,
        event: InteractionEventEnvelope,
    ) -> Result<(), String> {
        event.validate()?;
        // Release the DashMap shard guard before upgrading the route. If this
        // temporary Arc becomes the last owner, InteractionEventHub::drop may
        // remove the same map entry without recursively locking the shard.
        let hub = self
            .interaction_event_hubs
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

    pub(crate) fn close_interaction_event_hub(&self, thread_id: &str) {
        if let Some((_, registered)) = self.interaction_event_hubs.remove(thread_id) {
            if let Some(hub) = registered.upgrade() {
                hub.close(InteractionHubTermination::SessionClosed);
            }
        }
        self.interaction_sessions.remove(thread_id);
    }

    pub(crate) fn close_all_interaction_event_hubs(&self, error: Option<String>) {
        let hubs: Vec<_> = self
            .interaction_event_hubs
            .iter()
            .filter_map(|entry| entry.value().upgrade())
            .collect();
        self.interaction_event_hubs.clear();
        self.interaction_sessions.clear();
        for hub in hubs {
            hub.close(match &error {
                Some(message) => InteractionHubTermination::ConnectionLost(message.clone()),
                None => InteractionHubTermination::SessionClosed,
            });
        }
    }
}

impl WhaleClient {
    async fn require_interaction_capability(&self) -> Result<(), InteractionViewError> {
        let initialized = self.initialize().await?;
        if initialized
            .capabilities
            .iter()
            .any(|capability| capability == CAPABILITY_INTERACTIONS)
        {
            Ok(())
        } else {
            Err(InteractionViewError::UnsupportedCapability {
                capability: CAPABILITY_INTERACTIONS,
            })
        }
    }

    pub(crate) async fn require_interaction_session(
        &self,
        thread_id: &str,
    ) -> Result<(), SdkError> {
        let initialized = self.initialize().await?;
        if !initialized
            .capabilities
            .iter()
            .any(|capability| capability == CAPABILITY_INTERACTIONS)
        {
            return Err(SdkError::ProtocolCompatibility(
                "Daemon does not advertise interactions.v1".into(),
            ));
        }
        self.inner
            .state
            .with_existing_session_open(thread_id, || ())?;
        if !self.inner.state.interactions_enabled(thread_id) {
            return Err(SdkError::InvalidConfiguration(format!(
                "Interactions are not enabled for Session {thread_id}"
            )));
        }
        Ok(())
    }

    async fn require_interaction_view(&self, thread_id: &str) -> Result<(), InteractionViewError> {
        self.require_interaction_capability().await?;
        self.inner
            .state
            .with_existing_session_open(thread_id, || ())?;
        if !self.inner.state.interactions_enabled(thread_id) {
            return Err(InteractionViewError::NotEnabled {
                thread_id: thread_id.to_owned(),
            });
        }
        Ok(())
    }

    async fn get_interaction_snapshot(
        &self,
        thread_id: &str,
    ) -> Result<InteractionSnapshot, InteractionViewError> {
        let snapshot: InteractionSnapshot = self
            .request_for_session(
                METHOD_SESSION_INTERACTIONS_GET,
                Some(GetSessionInteractionsParams {
                    thread_id: thread_id.to_owned(),
                }),
                thread_id,
            )
            .await?;
        snapshot
            .validate()
            .map_err(|message| InteractionViewError::InvalidProjection { message })?;
        if snapshot.thread_id != thread_id {
            return Err(InteractionViewError::InvalidProjection {
                message: "Daemon returned an Interaction snapshot for another Session".into(),
            });
        }
        Ok(snapshot)
    }

    async fn get_interaction_replay_page(
        &self,
        params: &SubscribeInteractionsParams,
    ) -> Result<SubscribeInteractionsResult, InteractionViewError> {
        let result: Result<SubscribeInteractionsResult, SdkError> = self
            .request_for_session(
                METHOD_SESSION_INTERACTIONS_SUBSCRIBE,
                Some(params.clone()),
                &params.thread_id,
            )
            .await;
        let result = match result {
            Ok(result) => result,
            Err(SdkError::Rpc { code, message })
                if code == whale_protocol::rpc::JSONRPCError::INVALID_PARAMS =>
            {
                return Err(InteractionViewError::InvalidCursor { message });
            }
            Err(error) => return Err(error.into()),
        };
        result
            .validate_for(params)
            .map_err(|message| InteractionViewError::InvalidProjection { message })?;
        Ok(result)
    }

    pub async fn respond_interaction(
        &self,
        thread_id: &str,
        turn_id: &str,
        request_id: &str,
        response: serde_json::Value,
    ) -> Result<RespondInteractionResult, SdkError> {
        let params = RespondInteractionParams::new(thread_id, turn_id, request_id, response)
            .map_err(SdkError::InvalidConfiguration)?;
        self.require_interaction_session(thread_id).await?;
        let result: RespondInteractionResult = self
            .request_for_session(METHOD_TURN_RESPOND_INTERACTION, Some(params), thread_id)
            .await?;
        result.validate().map_err(SdkError::Internal)?;
        if result.request_id != request_id {
            return Err(SdkError::Internal(
                "Daemon returned a different Interaction response identity".into(),
            ));
        }
        Ok(result)
    }
}

impl WhaleThread {
    pub async fn interaction_snapshot(&self) -> Result<InteractionSnapshot, InteractionViewError> {
        self.client
            .require_interaction_view(&self.thread_id)
            .await?;
        self.client.get_interaction_snapshot(&self.thread_id).await
    }

    pub async fn watch_interactions(
        &self,
        options: InteractionWatchOptions,
    ) -> Result<InteractionWatch, InteractionViewError> {
        self.client
            .require_interaction_view(&self.thread_id)
            .await?;
        let (hub, live, termination) = self
            .client
            .inner
            .state
            .subscribe_interaction_events(&self.thread_id)?;
        let snapshot = self
            .client
            .get_interaction_snapshot(&self.thread_id)
            .await?;
        let events = spawn_interaction_stream(
            self.client.clone(),
            self.thread_id.clone(),
            snapshot.cursor.clone(),
            hub,
            live,
            termination,
            options.subscription,
            None,
        );
        Ok(InteractionWatch { snapshot, events })
    }

    pub async fn subscribe_interactions_from(
        &self,
        cursor: InteractionCursor,
        options: InteractionSubscriptionOptions,
    ) -> Result<InteractionEventStream, InteractionViewError> {
        cursor
            .validate()
            .map_err(|message| InteractionViewError::InvalidCursor { message })?;
        if cursor.thread_id != self.thread_id {
            return Err(InteractionViewError::InvalidCursor {
                message: "Interaction cursor belongs to another Session".into(),
            });
        }
        self.client
            .require_interaction_view(&self.thread_id)
            .await?;
        let (hub, live, termination) = self
            .client
            .inner
            .state
            .subscribe_interaction_events(&self.thread_id)?;
        let params = SubscribeInteractionsParams {
            thread_id: self.thread_id.clone(),
            after: cursor.clone(),
            through: None,
            limit: options.replay_page_limit(),
        };
        let page = self.client.get_interaction_replay_page(&params).await?;
        if let Some(gap) = page.gap.clone() {
            let snapshot = self
                .client
                .get_interaction_snapshot(&self.thread_id)
                .await?;
            return Err(InteractionViewError::ResyncRequired { gap, snapshot });
        }
        Ok(spawn_interaction_stream(
            self.client.clone(),
            self.thread_id.clone(),
            cursor,
            hub,
            live,
            termination,
            options,
            Some((params, page)),
        ))
    }
}

impl crate::RunHandle {
    pub async fn pending_interactions(&self) -> Result<TurnInteractionSnapshot, SdkError> {
        self.client
            .require_interaction_session(self.thread_id())
            .await?;
        let params = GetTurnInteractionsParams {
            thread_id: self.thread_id().to_owned(),
            turn_id: self.id().to_owned(),
        };
        params.validate().map_err(SdkError::InvalidConfiguration)?;
        let snapshot: TurnInteractionSnapshot = self
            .client
            .request_for_session(METHOD_TURN_INTERACTIONS_GET, Some(params), self.thread_id())
            .await?;
        snapshot.validate().map_err(SdkError::Internal)?;
        if snapshot.thread_id != self.thread_id() || snapshot.turn_id != self.id() {
            return Err(SdkError::Internal(
                "Daemon returned an Interaction snapshot for another Run".into(),
            ));
        }
        Ok(snapshot)
    }

    pub async fn respond_interaction(
        &self,
        request_id: &str,
        response: serde_json::Value,
    ) -> Result<RespondInteractionResult, SdkError> {
        self.client
            .respond_interaction(self.thread_id(), self.id(), request_id, response)
            .await
    }
}

fn spawn_interaction_stream(
    client: WhaleClient,
    thread_id: String,
    cursor: InteractionCursor,
    hub: Arc<InteractionEventHub>,
    live: broadcast::Receiver<InteractionEventEnvelope>,
    termination: watch::Receiver<Option<InteractionHubTermination>>,
    options: InteractionSubscriptionOptions,
    first_page: Option<(SubscribeInteractionsParams, SubscribeInteractionsResult)>,
) -> InteractionEventStream {
    let (output, receiver) = mpsc::channel(options.output_capacity());
    let worker_termination = termination.clone();
    let worker = tokio::spawn(run_subscription_worker(
        client,
        thread_id,
        cursor,
        live,
        worker_termination,
        output,
        options.replay_page_limit(),
        first_page,
    ));
    InteractionEventStream {
        receiver,
        termination,
        worker,
        _hub: hub,
        ended: false,
        last_received: None,
    }
}

async fn run_subscription_worker(
    client: WhaleClient,
    thread_id: String,
    mut last_enqueued: InteractionCursor,
    mut live: broadcast::Receiver<InteractionEventEnvelope>,
    mut termination: watch::Receiver<Option<InteractionHubTermination>>,
    output: mpsc::Sender<Result<InteractionEventEnvelope, InteractionViewError>>,
    page_limit: u32,
    first_page: Option<(SubscribeInteractionsParams, SubscribeInteractionsResult)>,
) {
    match replay_fixed_window(
        &client,
        &thread_id,
        page_limit,
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
                        page_limit,
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
    page_limit: u32,
    last_enqueued: &mut InteractionCursor,
    output: &mpsc::Sender<Result<InteractionEventEnvelope, InteractionViewError>>,
    termination: &mut watch::Receiver<Option<InteractionHubTermination>>,
    first_page: Option<(SubscribeInteractionsParams, SubscribeInteractionsResult)>,
) -> Result<bool, InteractionViewError> {
    let (mut params, mut page) = match first_page {
        Some(first) => first,
        None => {
            let params = SubscribeInteractionsParams {
                thread_id: thread_id.to_owned(),
                after: last_enqueued.clone(),
                through: None,
                limit: page_limit,
            };
            let Some(page) = request_replay_page(client, &params, termination).await? else {
                return Ok(false);
            };
            (params, page)
        }
    };

    loop {
        page.validate_for(&params)
            .map_err(|message| InteractionViewError::InvalidProjection { message })?;
        if let Some(gap) = page.gap.clone() {
            let snapshot = client.get_interaction_snapshot(thread_id).await?;
            return Err(InteractionViewError::ResyncRequired { gap, snapshot });
        }
        if page.has_more && page.resume_after == params.after {
            return Err(InteractionViewError::InvalidProjection {
                message: "Interaction replay claimed more events without advancing".into(),
            });
        }
        let through = page.through.clone();
        for event in page.events {
            if !is_next_cursor(last_enqueued, &event.cursor) {
                return Err(InteractionViewError::InvalidProjection {
                    message: format!(
                        "Interaction replay event is not contiguous after cursor {}",
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
        params = SubscribeInteractionsParams {
            thread_id: thread_id.to_owned(),
            after: page.resume_after,
            through: Some(through),
            limit: page_limit,
        };
        let Some(next) = request_replay_page(client, &params, termination).await? else {
            return Ok(false);
        };
        page = next;
    }
}

async fn request_replay_page(
    client: &WhaleClient,
    params: &SubscribeInteractionsParams,
    termination: &mut watch::Receiver<Option<InteractionHubTermination>>,
) -> Result<Option<SubscribeInteractionsResult>, InteractionViewError> {
    let request = client.get_interaction_replay_page(params);
    tokio::pin!(request);
    tokio::select! {
        biased;
        _ = termination.changed() => Ok(None),
        result = &mut request => result.map(Some),
    }
}

async fn enqueue_event(
    output: &mpsc::Sender<Result<InteractionEventEnvelope, InteractionViewError>>,
    termination: &mut watch::Receiver<Option<InteractionHubTermination>>,
    last_enqueued: &mut InteractionCursor,
    event: InteractionEventEnvelope,
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

#[cfg(test)]
mod route_drop_regression {
    use super::*;
    use crate::weak_route_drop_regressions::{run_isolated, AfterUpgrade};
    use std::sync::{Mutex, OnceLock};

    const CHILD_ENVIRONMENT: &str = "WHALE_SDK_INTERACTION_WEAK_ROUTE_CHILD";
    const EXACT_TEST_NAME: &str =
        "interactions::route_drop_regression::interaction_route_releases_dashmap_guard_before_last_arc_drop";

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
    fn interaction_route_releases_dashmap_guard_before_last_arc_drop() {
        run_isolated(CHILD_ENVIRONMENT, EXACT_TEST_NAME, || {
            let thread_id = "interaction-weak-route";
            let state = ClientState::new();
            let hub = InteractionEventHub::new(&state, thread_id);
            state
                .interaction_event_hubs
                .insert(thread_id.into(), Arc::downgrade(&hub));

            let hook = AfterUpgrade::new();
            *hook_slot().lock().unwrap() = Some((thread_id.into(), hook.clone()));
            let route_state = state.clone();
            let route = std::thread::spawn(move || {
                route_state.route_interaction_event(InteractionEventEnvelope::new(
                    thread_id,
                    InteractionCursor {
                        thread_id: thread_id.into(),
                        stream_id: "interaction-stream".into(),
                        seq: 1,
                    },
                    1,
                    InteractionEventPayload::Removed {
                        request_id: "request".into(),
                        turn_id: "turn".into(),
                        cause: INTERACTION_REMOVAL_RESOLVED.into(),
                    },
                ))
            });

            hook.drop_last_external_arc(|| drop(hub));
            route
                .join()
                .expect("Interaction route thread panicked")
                .unwrap();
            *hook_slot().lock().unwrap() = None;
            assert!(!state.interaction_event_hubs.contains_key(thread_id));
        });
    }
}

async fn send_worker_error(
    output: &mpsc::Sender<Result<InteractionEventEnvelope, InteractionViewError>>,
    termination: &mut watch::Receiver<Option<InteractionHubTermination>>,
    error: InteractionViewError,
) {
    let send = output.send(Err(error));
    tokio::pin!(send);
    tokio::select! {
        biased;
        _ = termination.changed() => {}
        _ = &mut send => {}
    }
}

fn same_stream(left: &InteractionCursor, right: &InteractionCursor) -> bool {
    left.thread_id == right.thread_id && left.stream_id == right.stream_id
}

fn is_next_cursor(previous: &InteractionCursor, next: &InteractionCursor) -> bool {
    same_stream(previous, next)
        && previous
            .seq
            .checked_add(1)
            .is_some_and(|expected| expected == next.seq)
}

fn invalid_configuration(message: String) -> InteractionViewError {
    InteractionViewError::Sdk(SdkError::InvalidConfiguration(message))
}

pub(crate) fn run_approval_response(
    decision: whale_protocol::runs::RunApprovalDecision,
    arguments: Option<serde_json::Value>,
    feedback: Option<String>,
) -> serde_json::Value {
    let mut response = serde_json::Map::new();
    response.insert(
        "decision".into(),
        serde_json::Value::String(
            match decision {
                whale_protocol::runs::RunApprovalDecision::Approve => "approve",
                whale_protocol::runs::RunApprovalDecision::Reject => "reject",
                whale_protocol::runs::RunApprovalDecision::ModifyArguments => "modify_arguments",
            }
            .into(),
        ),
    );
    if let Some(arguments) = arguments {
        response.insert("arguments".into(), arguments);
    }
    if let Some(feedback) = feedback {
        response.insert("feedback".into(), serde_json::Value::String(feedback));
    }
    serde_json::Value::Object(response)
}

pub(crate) fn legacy_approval_response(
    decision: whale_protocol::rpc::ApprovalDecision,
    feedback: Option<String>,
) -> serde_json::Value {
    let decision = match decision {
        whale_protocol::rpc::ApprovalDecision::Approve => {
            whale_protocol::runs::RunApprovalDecision::Approve
        }
        whale_protocol::rpc::ApprovalDecision::Reject => {
            whale_protocol::runs::RunApprovalDecision::Reject
        }
    };
    run_approval_response(decision, None, feedback)
}
