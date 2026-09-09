//! Owner-scoped V2 Session projections and the universal publication lane.

use super::DaemonServer;
use super::{
    session_catalog::{CursorCodec, CursorDecodeError, ListCursorClaims, LIST_CURSOR_DOMAIN},
    session_history::HistoryArchive,
};
use crate::transport::{AnyTransportWriter, OutgoingTransport};
use dashmap::{mapref::entry::Entry, DashMap};
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex as StdMutex,
    },
};
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use whale_protocol::{
    canonical::CanonicalItem,
    recovery::STORE_FAILED,
    retention::serialized_bytes,
    rpc::{JSONRPCError, JSONRPCNotification, JSONRPCResponse, RequestId},
    runs::{RunEvent, RunEventPayload, RunSnapshot},
    session_management::{
        GetSessionHistoryParams, GetSessionHistoryResult, GetSessionV2Params, ListSessionsParams,
        ListSessionsResult, ReplaceSessionMetadataParams, ReplaceSessionMetadataResult,
        ReplayGapReasonV2, ReplayGapV2, SessionCursorV2, SessionEventEnvelopeV2,
        SessionEventPayloadV2, SessionLifecycleState, SessionListCursor, SessionListEntry,
        SessionManagementErrorData, SessionPersistenceV2, SessionRunHeadline, SessionSnapshotV2,
        SessionSummaryV2, SubscribeSessionV2Params, SubscribeSessionV2Result,
        CLOSED_SESSION_TOMBSTONE_TTL_MS, MAX_CLOSED_SESSION_TOMBSTONES_PER_OWNER,
        MAX_CLOSED_SESSION_TOMBSTONE_BYTES_PER_OWNER, MAX_SESSION_MANAGEMENT_PAGE_BYTES,
        MAX_SESSION_V2_EVENT_JOURNAL_BYTES, MAX_SESSION_V2_EVENT_JOURNAL_EVENTS,
        METHOD_SESSION_EVENT_V2, SESSION_CURSOR_REJECTED, SESSION_HISTORY_GAP,
        SESSION_MANAGEMENT_STATE, SESSION_REVISION_CONFLICT,
    },
    session_views::{SessionHistoryWindow, SessionRunView, MAX_SESSION_HISTORY_LIMIT},
};
use whale_store::{SessionJournal, StoreError};

const DEFAULT_NOTIFICATION_QUEUE: usize = 128;

pub(super) struct SessionPublicationLane(Mutex<()>);

impl Default for SessionPublicationLane {
    fn default() -> Self {
        Self(Mutex::new(()))
    }
}

#[derive(Clone)]
struct JournalEntryV2 {
    envelope: SessionEventEnvelopeV2,
    bytes: usize,
    run_id: Option<String>,
}

#[derive(Clone)]
struct ManagementProjection {
    snapshot: SessionSnapshotV2,
    journal: VecDeque<JournalEntryV2>,
    journal_bytes: usize,
    replay_floor: SessionCursorV2,
    run_identities: HashMap<String, usize>,
}

impl ManagementProjection {
    fn prepare(
        &self,
        occurred_at_ms: u64,
        payload: SessionEventPayloadV2,
        run_identity: Option<(String, usize)>,
    ) -> Result<PreparedManagementPublication, SessionManagementFailure> {
        let cursor = self
            .snapshot
            .cursor
            .checked_next()
            .map_err(SessionManagementFailure::InvalidProjection)?;
        let envelope = SessionEventEnvelopeV2::new(
            self.snapshot.summary.thread_id.clone(),
            cursor,
            occurred_at_ms,
            payload,
        );
        envelope
            .validate()
            .map_err(SessionManagementFailure::InvalidProjection)?;
        let bytes = serde_json::to_vec(&envelope)
            .map_err(|error| SessionManagementFailure::InvalidProjection(error.to_string()))?
            .len();
        let run_id = event_turn_id(&envelope).map(str::to_owned).or_else(|| {
            self.snapshot
                .active_run
                .as_ref()
                .map(|run| run.snapshot.turn_id.clone())
        });
        let mut next = self.clone();
        next.snapshot
            .apply(&envelope)
            .map_err(|error| SessionManagementFailure::InvalidProjection(error.to_string()))?;
        next.retain(envelope.clone(), bytes, run_id);
        if let Some((turn_id, identity)) = run_identity {
            next.run_identities.insert(turn_id, identity);
        }
        Ok(PreparedManagementPublication {
            source: self.snapshot.cursor.clone(),
            next,
            envelope,
        })
    }

    fn retain(&mut self, envelope: SessionEventEnvelopeV2, bytes: usize, run_id: Option<String>) {
        if bytes > MAX_SESSION_V2_EVENT_JOURNAL_BYTES {
            self.journal.clear();
            self.journal_bytes = 0;
            self.replay_floor = envelope.cursor;
            return;
        }
        self.journal.push_back(JournalEntryV2 {
            envelope,
            bytes,
            run_id,
        });
        self.journal_bytes += bytes;
        while self.journal.len() > MAX_SESSION_V2_EVENT_JOURNAL_EVENTS
            || self.journal_bytes > MAX_SESSION_V2_EVENT_JOURNAL_BYTES
        {
            let evicted = self
                .journal
                .pop_front()
                .expect("nonempty bounded V2 journal");
            self.journal_bytes -= evicted.bytes;
            self.replay_floor = evicted.envelope.cursor;
        }
    }

    fn snapshot_with_history_limit(&self, history_limit: u32) -> SessionSnapshotV2 {
        let mut snapshot = self.snapshot.clone();
        let keep = snapshot.history.items.len().min(history_limit as usize);
        let start = snapshot.history.items.len() - keep;
        snapshot.history.items = snapshot.history.items[start..].to_vec();
        snapshot.history.start_index = snapshot.history.total_items - keep as u64;
        snapshot.history.capacity = history_limit;
        snapshot
    }

    fn replay(
        &self,
        params: &SubscribeSessionV2Params,
    ) -> Result<SubscribeSessionV2Result, SessionManagementFailure> {
        params
            .validate()
            .map_err(SessionManagementFailure::InvalidParams)?;
        let current = self.snapshot.cursor.clone();
        let response_through = params.through.clone().unwrap_or_else(|| current.clone());
        if params.after.stream_id != current.stream_id {
            return Ok(self.gap(params, current, ReplayGapReasonV2::StreamReset));
        }
        if params.after.seq > current.seq {
            return Err(SessionManagementFailure::InvalidParams(
                "Replay cursor is ahead of the current V2 Session cursor".into(),
            ));
        }
        if let Some(through) = &params.through {
            if through.stream_id != current.stream_id {
                return Ok(self.gap(params, current, ReplayGapReasonV2::StreamReset));
            }
            if through.seq > current.seq {
                return Err(SessionManagementFailure::InvalidParams(
                    "Fixed V2 replay high watermark is ahead of the current cursor".into(),
                ));
            }
        }
        if params.after.seq < self.replay_floor.seq {
            return Ok(self.gap(params, response_through, ReplayGapReasonV2::Retention));
        }
        let events: Vec<_> = self
            .journal
            .iter()
            .filter(|entry| {
                entry.envelope.cursor.seq > params.after.seq
                    && entry.envelope.cursor.seq <= response_through.seq
            })
            .take(params.limit as usize)
            .map(|entry| entry.envelope.clone())
            .collect();
        let resume_after = events
            .last()
            .map(|event| event.cursor.clone())
            .unwrap_or_else(|| params.after.clone());
        Ok(SubscribeSessionV2Result {
            events,
            resume_after: resume_after.clone(),
            through: response_through.clone(),
            lifecycle: lifecycle_at(self, response_through.seq),
            has_more: resume_after.seq < response_through.seq,
            gap: None,
        })
    }

    fn gap(
        &self,
        params: &SubscribeSessionV2Params,
        through: SessionCursorV2,
        reason: ReplayGapReasonV2,
    ) -> SubscribeSessionV2Result {
        SubscribeSessionV2Result {
            events: Vec::new(),
            resume_after: params.after.clone(),
            through,
            lifecycle: self.snapshot.lifecycle,
            has_more: false,
            gap: Some(ReplayGapV2 {
                reason,
                requested: params.after.clone(),
                replay_floor: self.replay_floor.clone(),
                current: self.snapshot.cursor.clone(),
                view_revision: self.snapshot.summary.view_revision,
            }),
        }
    }

    fn prepare_retire_run(
        &self,
        turn_id: &str,
        identity: usize,
    ) -> Option<PreparedManagementRetirement> {
        if self.run_identities.get(turn_id).copied() != Some(identity)
            || self
                .snapshot
                .active_run
                .as_ref()
                .is_some_and(|run| run.snapshot.turn_id == turn_id)
        {
            return None;
        }
        let mut next = self.clone();
        let through = next
            .journal
            .iter()
            .filter(|entry| entry.run_id.as_deref() == Some(turn_id))
            .map(|entry| entry.envelope.cursor.seq)
            .max();
        if let Some(through) = through {
            while next
                .journal
                .front()
                .is_some_and(|entry| entry.envelope.cursor.seq <= through)
            {
                let evicted = next
                    .journal
                    .pop_front()
                    .expect("checked nonempty V2 journal");
                next.journal_bytes -= evicted.bytes;
            }
            next.replay_floor.seq = next.replay_floor.seq.max(through);
        }
        next.run_identities.remove(turn_id);
        Some(PreparedManagementRetirement {
            source: self.snapshot.cursor.clone(),
            next,
        })
    }
}

fn lifecycle_at(state: &ManagementProjection, through: u64) -> SessionLifecycleState {
    let mut lifecycle = SessionLifecycleState::Open;
    for entry in &state.journal {
        if entry.envelope.cursor.seq > through {
            break;
        }
        if let SessionEventPayloadV2::LifecycleChanged { lifecycle: next } = entry.envelope.payload
        {
            lifecycle = next;
        }
    }
    if through == state.snapshot.cursor.seq {
        state.snapshot.lifecycle
    } else {
        lifecycle
    }
}

struct PreparedManagementPublication {
    source: SessionCursorV2,
    next: ManagementProjection,
    envelope: SessionEventEnvelopeV2,
}

struct PreparedManagementRetirement {
    source: SessionCursorV2,
    next: ManagementProjection,
}

struct NotificationWork {
    envelope: SessionEventEnvelopeV2,
    terminal: bool,
    completed: Option<oneshot::Sender<()>>,
}

#[derive(Default)]
struct NotificationOrder {
    enabled: bool,
    next_seq: Option<u64>,
    pending: BTreeMap<u64, SessionEventEnvelopeV2>,
}

struct ManagementNotifier {
    queue: mpsc::Sender<NotificationWork>,
    latest: watch::Sender<Option<SessionEventEnvelopeV2>>,
    order: StdMutex<NotificationOrder>,
    cancel: watch::Sender<bool>,
    task: tokio::task::AbortHandle,
}

impl ManagementNotifier {
    fn activate(&self, current_seq: u64) {
        let mut order = self.order.lock().unwrap();
        if order.enabled {
            return;
        }
        order.enabled = true;
        order.next_seq = current_seq.checked_add(1);
        order.pending.retain(|seq, _| *seq > current_seq);
    }

    fn ordered(&self, envelope: SessionEventEnvelopeV2) -> Vec<SessionEventEnvelopeV2> {
        let mut order = self.order.lock().unwrap();
        if !order.enabled {
            return Vec::new();
        }
        let Some(next) = order.next_seq else {
            return Vec::new();
        };
        if envelope.cursor.seq < next {
            return Vec::new();
        }
        order.pending.insert(envelope.cursor.seq, envelope);
        let mut ready = Vec::new();
        while let Some(next) = order.next_seq {
            let Some(envelope) = order.pending.remove(&next) else {
                break;
            };
            order.next_seq = next.checked_add(1);
            ready.push(envelope);
        }
        ready
    }

    fn publish(&self, envelope: SessionEventEnvelopeV2) {
        for envelope in self.ordered(envelope) {
            let terminal = is_closed(&envelope);
            let fallback = envelope.clone();
            if let Err(mpsc::error::TrySendError::Full(_)) = self.queue.try_send(NotificationWork {
                envelope,
                terminal,
                completed: None,
            }) {
                self.latest.send_replace(Some(fallback));
            }
        }
    }

    async fn publish_terminal(&self, envelope: SessionEventEnvelopeV2) {
        let ready = self.ordered(envelope);
        for envelope in ready {
            let terminal = is_closed(&envelope);
            let (completed, wait) = if terminal {
                let (tx, rx) = oneshot::channel();
                (Some(tx), Some(rx))
            } else {
                (None, None)
            };
            if self
                .queue
                .send(NotificationWork {
                    envelope,
                    terminal,
                    completed,
                })
                .await
                .is_err()
            {
                return;
            }
            if let Some(wait) = wait {
                let _ = wait.await;
            }
        }
    }

    fn close(&self) {
        self.cancel.send_replace(true);
        self.task.abort();
    }
}

impl Drop for ManagementNotifier {
    fn drop(&mut self) {
        self.close();
    }
}

struct ManagementRecord {
    owner: String,
    catalog_ordinal: u64,
    lane: SessionPublicationLane,
    accepting_metadata: AtomicBool,
    projection: StdMutex<ManagementProjection>,
    history: StdMutex<HistoryArchive>,
    closed_at: StdMutex<Option<tokio::time::Instant>>,
    notifier: ManagementNotifier,
}

fn retained_record_bytes(record: &ManagementRecord) -> usize {
    let projection = record.projection.lock().unwrap();
    let snapshot = serialized_bytes(&projection.snapshot)
        .ok()
        .and_then(|bytes| usize::try_from(bytes).ok())
        .unwrap_or(usize::MAX);
    let projection_bytes = snapshot.saturating_add(projection.journal_bytes);
    drop(projection);
    projection_bytes.saturating_add(record.history.lock().unwrap().retained_bytes())
}

struct OwnerCatalogState {
    generation: u64,
    generation_exhausted: bool,
    next_ordinal: u64,
    entries: BTreeMap<u64, String>,
    expired_tombstones: VecDeque<String>,
}

impl Default for OwnerCatalogState {
    fn default() -> Self {
        Self {
            generation: 0,
            generation_exhausted: false,
            next_ordinal: 1,
            entries: BTreeMap::new(),
            expired_tombstones: VecDeque::new(),
        }
    }
}

#[derive(Default)]
struct OwnerCatalog {
    state: StdMutex<OwnerCatalogState>,
}

#[derive(Clone, Copy)]
struct ManagementLimits {
    max_closed_tombstones: usize,
    max_closed_tombstone_bytes: usize,
    closed_tombstone_ttl_ms: u64,
}

impl Default for ManagementLimits {
    fn default() -> Self {
        Self {
            max_closed_tombstones: MAX_CLOSED_SESSION_TOMBSTONES_PER_OWNER,
            max_closed_tombstone_bytes: MAX_CLOSED_SESSION_TOMBSTONE_BYTES_PER_OWNER,
            closed_tombstone_ttl_ms: CLOSED_SESSION_TOMBSTONE_TTL_MS,
        }
    }
}

#[derive(Clone)]
pub(super) struct SessionManagementRegistry {
    records: Arc<DashMap<String, Arc<ManagementRecord>>>,
    owners: Arc<DashMap<String, Arc<OwnerCatalog>>>,
    cursors: CursorCodec,
    limits: ManagementLimits,
}

impl Default for SessionManagementRegistry {
    fn default() -> Self {
        Self {
            records: Arc::new(DashMap::new()),
            owners: Arc::new(DashMap::new()),
            cursors: CursorCodec::default(),
            limits: ManagementLimits::default(),
        }
    }
}

#[derive(Clone)]
pub(super) enum RunPublicationMutation {
    Begin {
        snapshot: RunSnapshot,
        identity: usize,
    },
    Changed(RunSnapshot),
    Event(RunEvent),
}

pub(super) struct CommittedDualPublication {
    pub(super) v1: whale_protocol::session_views::SessionEventEnvelope,
    pub(super) v2: SessionEventEnvelopeV2,
}

impl SessionManagementRegistry {
    #[cfg(test)]
    fn with_limits_for_test(
        max_closed_tombstones: usize,
        max_closed_tombstone_bytes: usize,
        closed_tombstone_ttl_ms: u64,
    ) -> Self {
        Self {
            records: Arc::new(DashMap::new()),
            owners: Arc::new(DashMap::new()),
            cursors: CursorCodec::fixed_for_test([9; 32]),
            limits: ManagementLimits {
                max_closed_tombstones,
                max_closed_tombstone_bytes,
                closed_tombstone_ttl_ms,
            },
        }
    }

    fn owner_catalog(&self, owner: &str) -> Arc<OwnerCatalog> {
        self.owners
            .entry(owner.to_owned())
            .or_insert_with(|| Arc::new(OwnerCatalog::default()))
            .clone()
    }

    fn prune_owner(&self, owner: &str, protected: Option<&Arc<ManagementRecord>>) {
        let Some(catalog) = self.owners.get(owner).map(|entry| entry.value().clone()) else {
            return;
        };
        let now = tokio::time::Instant::now();
        let mut state = catalog.state.lock().unwrap();
        let mut closed = Vec::new();
        for (ordinal, thread_id) in &state.entries {
            let Some(record) = self
                .records
                .get(thread_id)
                .map(|entry| entry.value().clone())
            else {
                continue;
            };
            let closed_at = *record.closed_at.lock().unwrap();
            if let Some(closed_at) = closed_at {
                closed.push((
                    *ordinal,
                    thread_id.clone(),
                    record.clone(),
                    closed_at,
                    retained_record_bytes(&record),
                ));
            }
        }
        closed.sort_by(|a, b| a.3.cmp(&b.3).then_with(|| a.0.cmp(&b.0)));

        let mut remove = Vec::new();
        for candidate in &closed {
            if protected.is_some_and(|record| Arc::ptr_eq(record, &candidate.2)) {
                continue;
            }
            if now.saturating_duration_since(candidate.3)
                >= std::time::Duration::from_millis(self.limits.closed_tombstone_ttl_ms)
            {
                remove.push(candidate.0);
            }
        }

        let mut retained_count = closed.len().saturating_sub(remove.len());
        let mut retained_bytes = closed
            .iter()
            .filter(|candidate| !remove.contains(&candidate.0))
            .fold(0_usize, |total, candidate| {
                total.saturating_add(candidate.4)
            });
        for candidate in &closed {
            if retained_count <= self.limits.max_closed_tombstones
                && retained_bytes <= self.limits.max_closed_tombstone_bytes
            {
                break;
            }
            if remove.contains(&candidate.0)
                || protected.is_some_and(|record| Arc::ptr_eq(record, &candidate.2))
            {
                continue;
            }
            remove.push(candidate.0);
            retained_count -= 1;
            retained_bytes = retained_bytes.saturating_sub(candidate.4);
        }

        for ordinal in remove {
            let Some(thread_id) = state.entries.remove(&ordinal) else {
                continue;
            };
            let removed = self.records.remove_if(&thread_id, |_, record| {
                record.owner == owner
                    && record.catalog_ordinal == ordinal
                    && record.closed_at.lock().unwrap().is_some()
            });
            let Some((_, record)) = removed else {
                state.entries.insert(ordinal, thread_id);
                continue;
            };
            record.notifier.close();
            state.expired_tombstones.push_back(thread_id);
            while state.expired_tombstones.len()
                > self.limits.max_closed_tombstones.saturating_mul(2).max(1)
            {
                state.expired_tombstones.pop_front();
            }
            match state.generation.checked_add(1) {
                Some(next) => state.generation = next,
                None => state.generation_exhausted = true,
            }
        }
    }

    pub(super) fn prune_closed(&self) {
        let owners: Vec<_> = self
            .owners
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        for owner in owners {
            self.prune_owner(&owner, None);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn insert(
        &self,
        owner: &str,
        thread_id: String,
        agent_name: Option<String>,
        metadata: Map<String, Value>,
        history: Vec<CanonicalItem>,
        created_at_ms: u64,
        persistence: SessionPersistenceV2,
        max_history_bytes: Option<u64>,
        transport: AnyTransportWriter,
    ) -> Result<(), SessionManagementFailure> {
        let mut summary = SessionSummaryV2::new(thread_id.clone(), created_at_ms);
        summary.agent_name = agent_name;
        summary.metadata = metadata;
        let snapshot_history =
            SessionHistoryWindow::from_history(&history, MAX_SESSION_HISTORY_LIMIT)
                .map_err(SessionManagementFailure::InvalidProjection)?;
        let cursor = SessionCursorV2 {
            thread_id: thread_id.clone(),
            stream_id: uuid::Uuid::new_v4().to_string(),
            seq: 0,
        };
        let snapshot = SessionSnapshotV2::new(
            summary,
            SessionLifecycleState::Open,
            persistence,
            snapshot_history,
            cursor.clone(),
        );
        snapshot
            .validate()
            .map_err(SessionManagementFailure::InvalidProjection)?;
        let history = HistoryArchive::new(
            thread_id.clone(),
            cursor.stream_id.clone(),
            history,
            max_history_bytes,
        )?;
        let catalog = self.owner_catalog(owner);
        let mut catalog_state = catalog.state.lock().unwrap();
        if catalog_state.generation_exhausted {
            return Err(SessionManagementFailure::InvalidProjection(
                "Session catalog generation is exhausted".into(),
            ));
        }
        if self.records.contains_key(&thread_id) {
            return Err(SessionManagementFailure::InvalidProjection(
                "SessionAlreadyExists".into(),
            ));
        }
        let catalog_ordinal = catalog_state.next_ordinal;
        catalog_state.next_ordinal = catalog_ordinal.checked_add(1).ok_or_else(|| {
            SessionManagementFailure::InvalidProjection(
                "Session catalog ordinal is exhausted".into(),
            )
        })?;
        let record = Arc::new(ManagementRecord {
            owner: owner.into(),
            catalog_ordinal,
            lane: SessionPublicationLane::default(),
            accepting_metadata: AtomicBool::new(true),
            projection: StdMutex::new(ManagementProjection {
                snapshot,
                journal: VecDeque::new(),
                journal_bytes: 0,
                replay_floor: cursor,
                run_identities: HashMap::new(),
            }),
            history: StdMutex::new(history),
            closed_at: StdMutex::new(None),
            notifier: spawn_notifier(transport),
        });
        match self.records.entry(thread_id.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(record);
                catalog_state.entries.insert(catalog_ordinal, thread_id);
                Ok(())
            }
            Entry::Occupied(_) => {
                // The fast duplicate check above is advisory. A concurrent insert
                // can still win before this entry API, so roll the owner-local
                // ordinal back while the catalog lock remains held.
                catalog_state.next_ordinal = catalog_ordinal;
                Err(SessionManagementFailure::InvalidProjection(
                    "SessionAlreadyExists".into(),
                ))
            }
        }
    }

    fn owned(
        &self,
        thread_id: &str,
        owner: &str,
    ) -> Result<Arc<ManagementRecord>, SessionManagementFailure> {
        self.prune_owner(owner, None);
        let record = self
            .records
            .get(thread_id)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| {
                if self.owners.get(owner).is_some_and(|catalog| {
                    catalog
                        .state
                        .lock()
                        .unwrap()
                        .expired_tombstones
                        .iter()
                        .any(|expired| expired == thread_id)
                }) {
                    SessionManagementFailure::TombstoneExpired(thread_id.into())
                } else {
                    SessionManagementFailure::Unavailable
                }
            })?;
        if record.owner != owner {
            return Err(SessionManagementFailure::Unavailable);
        }
        Ok(record)
    }

    pub(super) fn get_v2(
        &self,
        owner: &str,
        params: &GetSessionV2Params,
    ) -> Result<SessionSnapshotV2, SessionManagementFailure> {
        params
            .validate()
            .map_err(SessionManagementFailure::InvalidParams)?;
        let record = self.owned(&params.thread_id, owner)?;
        let snapshot = record
            .projection
            .lock()
            .unwrap()
            .snapshot_with_history_limit(params.history_limit);
        Ok(snapshot)
    }

    pub(super) fn subscribe_v2(
        &self,
        owner: &str,
        params: &SubscribeSessionV2Params,
    ) -> Result<SubscribeSessionV2Result, SessionManagementFailure> {
        let record = self.owned(&params.thread_id, owner)?;
        let projection = record.projection.lock().unwrap();
        let result = projection.replay(params)?;
        record.notifier.activate(projection.snapshot.cursor.seq);
        Ok(result)
    }

    pub(super) fn list_sessions(
        &self,
        owner: &str,
        params: &ListSessionsParams,
    ) -> Result<ListSessionsResult, SessionManagementFailure> {
        params
            .validate()
            .map_err(SessionManagementFailure::InvalidParams)?;
        let claims = if let Some(cursor) = &params.cursor {
            let claims: ListCursorClaims = self
                .cursors
                .decode(LIST_CURSOR_DOMAIN, owner, cursor.as_str())
                .map_err(|CursorDecodeError::Invalid| {
                    SessionManagementFailure::ListCursorInvalid
                })?;
            if !claims.valid_shape() {
                return Err(SessionManagementFailure::ListCursorInvalid);
            }
            if self.cursors.is_expired(claims.expires_at_ms) {
                return Err(SessionManagementFailure::ListCursorExpired);
            }
            Some(claims)
        } else {
            None
        };
        self.prune_owner(owner, None);
        let catalog = self.owner_catalog(owner);
        let state = catalog.state.lock().unwrap();
        if state.generation_exhausted {
            return Err(SessionManagementFailure::InvalidProjection(
                "Session catalog generation is exhausted".into(),
            ));
        }
        let (generation, after, through) = match claims {
            Some(claims) => {
                if claims.generation != state.generation {
                    return Err(SessionManagementFailure::ListCursorExpired);
                }
                if claims.through_ordinal >= state.next_ordinal {
                    return Err(SessionManagementFailure::ListCursorInvalid);
                }
                (
                    claims.generation,
                    claims.after_ordinal,
                    claims.through_ordinal,
                )
            }
            None => (
                state.generation,
                0,
                state.entries.keys().next_back().copied().unwrap_or(0),
            ),
        };

        let candidates: Vec<_> = state
            .entries
            .range((
                std::ops::Bound::Excluded(after),
                std::ops::Bound::Included(through),
            ))
            .filter_map(|(ordinal, thread_id)| {
                let record = self.records.get(thread_id)?;
                (record.owner == owner).then(|| (*ordinal, list_entry(&record)))
            })
            .collect();
        let mut sessions = Vec::new();
        let mut last = after;
        let mut result = ListSessionsResult {
            sessions: Vec::new(),
            next_cursor: None,
        };
        for (ordinal, entry) in candidates.iter().take(params.limit as usize) {
            let mut next_sessions = sessions.clone();
            next_sessions.push(entry.clone());
            let has_more = state
                .entries
                .range((
                    std::ops::Bound::Excluded(*ordinal),
                    std::ops::Bound::Included(through),
                ))
                .next()
                .is_some();
            let next_cursor = if has_more {
                Some(self.list_cursor(owner, generation, *ordinal, through)?)
            } else {
                None
            };
            let candidate = ListSessionsResult {
                sessions: next_sessions,
                next_cursor,
            };
            let actual = serialized_bytes(&candidate)
                .map_err(|error| SessionManagementFailure::InvalidProjection(error.to_string()))?;
            if actual > MAX_SESSION_MANAGEMENT_PAGE_BYTES as u64 {
                if sessions.is_empty() {
                    return Err(SessionManagementFailure::ResourceLimit {
                        resource: "session_list_page".into(),
                        actual,
                        limit: MAX_SESSION_MANAGEMENT_PAGE_BYTES as u64,
                        item_index: None,
                    });
                }
                break;
            }
            sessions = candidate.sessions.clone();
            last = *ordinal;
            result = candidate;
        }
        if sessions.is_empty() {
            result = ListSessionsResult {
                sessions,
                next_cursor: None,
            };
        } else if state
            .entries
            .range((
                std::ops::Bound::Excluded(last),
                std::ops::Bound::Included(through),
            ))
            .next()
            .is_some()
        {
            result.next_cursor = Some(self.list_cursor(owner, generation, last, through)?);
        }
        result
            .validate_for(params)
            .map_err(SessionManagementFailure::InvalidProjection)?;
        Ok(result)
    }

    fn list_cursor(
        &self,
        owner: &str,
        generation: u64,
        after: u64,
        through: u64,
    ) -> Result<SessionListCursor, SessionManagementFailure> {
        let claims = ListCursorClaims::new(generation, after, through, self.cursors.expiry()?);
        let token = self.cursors.encode(LIST_CURSOR_DOMAIN, owner, &claims)?;
        SessionListCursor::new(token).map_err(SessionManagementFailure::InvalidProjection)
    }

    pub(super) fn history_page(
        &self,
        owner: &str,
        params: &GetSessionHistoryParams,
    ) -> Result<GetSessionHistoryResult, SessionManagementFailure> {
        params
            .validate()
            .map_err(SessionManagementFailure::InvalidParams)?;
        let record = self.owned(&params.thread_id, owner)?;
        let result = record
            .history
            .lock()
            .unwrap()
            .page(&self.cursors, owner, params);
        result
    }

    pub(super) async fn publish_run(
        &self,
        views: &super::session_views::SessionViewRegistry,
        owner: &str,
        mutation: RunPublicationMutation,
        occurred_at_ms: u64,
    ) -> Result<CommittedDualPublication, SessionManagementFailure> {
        let thread_id = match &mutation {
            RunPublicationMutation::Begin { snapshot, .. }
            | RunPublicationMutation::Changed(snapshot) => &snapshot.thread_id,
            RunPublicationMutation::Event(event) => &event.thread_id,
        };
        let management = self.owned(thread_id, owner)?;
        let _lane = management.lane.0.lock().await;
        let committed_history = match &mutation {
            RunPublicationMutation::Event(RunEvent {
                payload: RunEventPayload::Finished { snapshot },
                ..
            }) => Some(snapshot.items.clone()),
            _ => None,
        };
        let v1 = match &mutation {
            RunPublicationMutation::Begin { snapshot, identity } => {
                views.prepare_begin_run(owner, snapshot.clone(), *identity, occurred_at_ms)
            }
            RunPublicationMutation::Changed(snapshot) => {
                views.prepare_run_changed(owner, snapshot.clone(), occurred_at_ms)
            }
            RunPublicationMutation::Event(event) => {
                views.prepare_run_event(owner, event.clone(), occurred_at_ms)
            }
        }
        .map_err(SessionManagementFailure::InvalidProjection)?;
        let v2 = {
            let projection = management.projection.lock().unwrap();
            match mutation {
                RunPublicationMutation::Begin { snapshot, identity } => {
                    let turn_id = snapshot.turn_id.clone();
                    projection.prepare(
                        occurred_at_ms,
                        SessionEventPayloadV2::RunChanged {
                            run: SessionRunView::new(snapshot, occurred_at_ms),
                        },
                        Some((turn_id, identity)),
                    )
                }
                RunPublicationMutation::Changed(snapshot) => {
                    let active = projection.snapshot.active_run.as_ref().ok_or_else(|| {
                        SessionManagementFailure::InvalidProjection(
                            "V2 Session has no active Run projection".into(),
                        )
                    })?;
                    if active.snapshot.turn_id != snapshot.turn_id {
                        return Err(SessionManagementFailure::InvalidProjection(
                            "V2 Session active Run identity changed".into(),
                        ));
                    }
                    let mut run = active.clone();
                    run.snapshot = snapshot;
                    run.updated_at_ms = run.updated_at_ms.max(occurred_at_ms);
                    projection.prepare(
                        occurred_at_ms,
                        SessionEventPayloadV2::RunChanged { run },
                        None,
                    )
                }
                RunPublicationMutation::Event(event) => projection.prepare(
                    occurred_at_ms,
                    SessionEventPayloadV2::RunEvent { event },
                    None,
                ),
            }?
        };
        let prepared_history = if let Some(items) = committed_history {
            let history = management.history.lock().unwrap();
            Some((history.end(), history.prepare_append(&items)?))
        } else {
            None
        };

        let v1_record = v1.record.clone();
        let mut v1_guard = v1_record.lock().unwrap();
        let mut v2_guard = management.projection.lock().unwrap();
        let mut history_guard = prepared_history
            .as_ref()
            .map(|_| management.history.lock().unwrap());
        if !v1.source_matches(&v1_guard)
            || v2_guard.snapshot.cursor != v2.source
            || prepared_history
                .as_ref()
                .zip(history_guard.as_ref())
                .is_some_and(|((source, _), current)| current.end() != *source)
        {
            return Err(SessionManagementFailure::InvalidProjection(
                "Session projection source changed during dual publication".into(),
            ));
        }
        let v1 = v1.install(&mut v1_guard);
        *v2_guard = v2.next;
        if let (Some((_, next)), Some(current)) = (prepared_history, history_guard.as_mut()) {
            **current = next;
        }
        let v2 = v2.envelope;
        drop(history_guard);
        drop(v2_guard);
        drop(v1_guard);
        Ok(CommittedDualPublication { v1, v2 })
    }

    pub(super) fn publish_committed(&self, owner: &str, envelope: SessionEventEnvelopeV2) {
        if let Ok(record) = self.owned(&envelope.thread_id, owner) {
            if record.projection.lock().unwrap().snapshot.cursor.stream_id
                == envelope.cursor.stream_id
            {
                record.notifier.publish(envelope);
            }
        }
    }

    pub(super) async fn replace_metadata(
        &self,
        owner: &str,
        params: ReplaceSessionMetadataParams,
        journal: Option<SessionJournal>,
    ) -> Result<ReplaceSessionMetadataResult, SessionManagementFailure> {
        params
            .validate()
            .map_err(SessionManagementFailure::InvalidParams)?;
        let record = self.owned(&params.thread_id, owner)?;
        let _lane = record.lane.0.lock().await;
        if !record.accepting_metadata.load(Ordering::Acquire) {
            return Err(SessionManagementFailure::Unavailable);
        }
        let prepared = {
            let projection = record.projection.lock().unwrap();
            if projection.snapshot.lifecycle != SessionLifecycleState::Open {
                return Err(SessionManagementFailure::NotOpen(
                    projection.snapshot.lifecycle,
                ));
            }
            let current = projection.snapshot.summary.view_revision;
            if params.expected_view_revision != current {
                return Err(SessionManagementFailure::RevisionConflict {
                    expected: params.expected_view_revision,
                    current,
                    cursor: projection.snapshot.cursor.clone(),
                });
            }
            if projection.snapshot.summary.metadata == params.metadata {
                return Ok(ReplaceSessionMetadataResult {
                    summary: projection.snapshot.summary.clone(),
                    cursor: projection.snapshot.cursor.clone(),
                    changed: false,
                });
            }
            projection.prepare(
                super::session_views::unix_ms(),
                SessionEventPayloadV2::MetadataChanged {
                    metadata: params.metadata.clone(),
                },
                None,
            )?
        };
        let persistent = {
            let projection = record.projection.lock().unwrap();
            matches!(
                projection.snapshot.persistence,
                SessionPersistenceV2::Persistent { .. }
            )
        };
        if persistent {
            let journal = journal.ok_or_else(|| {
                SessionManagementFailure::Storage(StoreError::Poisoned(
                    "Persistent Session journal is unavailable".into(),
                ))
            })?;
            journal
                .replace_metadata(params.metadata.clone())
                .await
                .map_err(SessionManagementFailure::Storage)?;
        }
        let mut projection = record.projection.lock().unwrap();
        if projection.snapshot.cursor != prepared.source {
            return Err(SessionManagementFailure::InvalidProjection(
                "Metadata projection changed while its publication lane was held".into(),
            ));
        }
        *projection = prepared.next;
        let envelope = prepared.envelope;
        let result = ReplaceSessionMetadataResult {
            summary: projection.snapshot.summary.clone(),
            cursor: projection.snapshot.cursor.clone(),
            changed: true,
        };
        drop(projection);
        drop(_lane);
        record.notifier.publish(envelope);
        Ok(result)
    }

    pub(super) async fn retire_run(
        &self,
        views: &super::session_views::SessionViewRegistry,
        owner: &str,
        thread_id: &str,
        turn_id: &str,
        identity: usize,
    ) -> bool {
        let Ok(management) = self.owned(thread_id, owner) else {
            return false;
        };
        let _lane = management.lane.0.lock().await;
        let Ok(Some(v1)) = views.prepare_retire_run(thread_id, turn_id, identity) else {
            return false;
        };
        let Some(v2) = management
            .projection
            .lock()
            .unwrap()
            .prepare_retire_run(turn_id, identity)
        else {
            return false;
        };
        let v1_record = v1.record.clone();
        let mut v1_guard = v1_record.lock().unwrap();
        let mut v2_guard = management.projection.lock().unwrap();
        if !v1.source_matches(&v1_guard) || v2_guard.snapshot.cursor != v2.source {
            return false;
        }
        v1.install(&mut v1_guard);
        *v2_guard = v2.next;
        true
    }

    pub(super) async fn begin_closing(
        &self,
        owner: &str,
        thread_id: &str,
    ) -> Result<(), SessionManagementFailure> {
        let record = self.owned(thread_id, owner)?;
        let _lane = record.lane.0.lock().await;
        let prepared = {
            let projection = record.projection.lock().unwrap();
            match projection.snapshot.lifecycle {
                SessionLifecycleState::Open => projection.prepare(
                    super::session_views::unix_ms(),
                    SessionEventPayloadV2::LifecycleChanged {
                        lifecycle: SessionLifecycleState::Closing,
                    },
                    None,
                )?,
                SessionLifecycleState::Closing | SessionLifecycleState::Closed => return Ok(()),
                _ => {
                    return Err(SessionManagementFailure::InvalidProjection(
                        "Unknown V2 Session lifecycle state".into(),
                    ))
                }
            }
        };
        let mut projection = record.projection.lock().unwrap();
        if projection.snapshot.cursor != prepared.source {
            return Err(SessionManagementFailure::InvalidProjection(
                "Lifecycle projection changed while its publication lane was held".into(),
            ));
        }
        *projection = prepared.next;
        let envelope = prepared.envelope;
        drop(projection);
        drop(_lane);
        record.notifier.publish(envelope);
        Ok(())
    }

    pub(super) async fn complete_closed(
        &self,
        owner: &str,
        thread_id: &str,
        journal: Option<SessionJournal>,
    ) -> Result<(), SessionManagementFailure> {
        let record = self.owned(thread_id, owner)?;
        let _lane = record.lane.0.lock().await;
        let prepared = {
            let projection = record.projection.lock().unwrap();
            match projection.snapshot.lifecycle {
                SessionLifecycleState::Closing => projection.prepare(
                    super::session_views::unix_ms(),
                    SessionEventPayloadV2::LifecycleChanged {
                        lifecycle: SessionLifecycleState::Closed,
                    },
                    None,
                )?,
                SessionLifecycleState::Closed => return Ok(()),
                SessionLifecycleState::Open => {
                    return Err(SessionManagementFailure::InvalidProjection(
                        "Session cannot become Closed before Closing".into(),
                    ))
                }
                _ => {
                    return Err(SessionManagementFailure::InvalidProjection(
                        "Unknown V2 Session lifecycle state".into(),
                    ))
                }
            }
        };
        let persistent = {
            let projection = record.projection.lock().unwrap();
            matches!(
                projection.snapshot.persistence,
                SessionPersistenceV2::Persistent { .. }
            )
        };
        if persistent {
            let journal = journal.ok_or_else(|| {
                SessionManagementFailure::Storage(StoreError::Poisoned(
                    "Persistent Session journal is unavailable".into(),
                ))
            })?;
            journal
                .detach()
                .await
                .map_err(SessionManagementFailure::Storage)?;
        }
        let envelope = {
            let mut projection = record.projection.lock().unwrap();
            if projection.snapshot.cursor != prepared.source {
                return Err(SessionManagementFailure::InvalidProjection(
                    "Closed projection changed while its publication lane was held".into(),
                ));
            }
            *projection = prepared.next;
            prepared.envelope
        };
        *record.closed_at.lock().unwrap() = Some(tokio::time::Instant::now());
        drop(_lane);
        // Preserve delivery of this Session's terminal envelope while enforcing
        // hard owner pressure against older tombstones.
        self.prune_owner(owner, Some(&record));
        record.notifier.publish_terminal(envelope).await;
        self.prune_owner(owner, None);
        Ok(())
    }

    pub(super) fn mark_owner_disconnected(&self, owner: &str) {
        for entry in self.records.iter() {
            if entry.value().owner == owner {
                entry
                    .value()
                    .accepting_metadata
                    .store(false, Ordering::Release);
            }
        }
    }

    pub(super) async fn remove_owner(&self, owner: &str) {
        let records: Vec<_> = self
            .records
            .iter()
            .filter(|entry| entry.value().owner == owner)
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        for (thread_id, record) in records {
            let _lane = record.lane.0.lock().await;
            self.records
                .remove_if(&thread_id, |_, current| Arc::ptr_eq(current, &record));
            record.notifier.close();
        }
        self.owners.remove(owner);
    }
}

fn list_entry(record: &ManagementRecord) -> SessionListEntry {
    let projection = record.projection.lock().unwrap();
    let mut entry = SessionListEntry::new(
        projection.snapshot.summary.clone(),
        projection.snapshot.lifecycle,
        projection.snapshot.persistence.clone(),
        projection.snapshot.cursor.clone(),
        record.history.lock().unwrap().end(),
    );
    entry.active_run = projection.snapshot.active_run.as_ref().map(|run| {
        SessionRunHeadline::new(
            run.snapshot.turn_id.clone(),
            run.snapshot.status,
            run.accepted_at_ms,
            run.updated_at_ms,
        )
    });
    entry.last_run = projection.snapshot.last_run.clone();
    entry
}

#[derive(Debug)]
pub(super) enum SessionManagementFailure {
    InvalidParams(String),
    Unavailable,
    NotOpen(SessionLifecycleState),
    RevisionConflict {
        expected: u64,
        current: u64,
        cursor: SessionCursorV2,
    },
    ListCursorInvalid,
    ListCursorExpired,
    HistoryCursorInvalid,
    HistoryStreamReset {
        requested: whale_protocol::session_management::SessionHistoryAnchor,
        current: whale_protocol::session_management::SessionHistoryAnchor,
    },
    HistoryGap {
        requested: whale_protocol::session_management::SessionHistoryAnchor,
        floor: whale_protocol::session_management::SessionHistoryAnchor,
        current: whale_protocol::session_management::SessionHistoryAnchor,
    },
    TombstoneExpired(String),
    ResourceLimit {
        resource: String,
        actual: u64,
        limit: u64,
        item_index: Option<u64>,
    },
    Storage(StoreError),
    InvalidProjection(String),
}

pub(super) fn rpc_error(error: SessionManagementFailure) -> JSONRPCError {
    match error {
        SessionManagementFailure::InvalidParams(message) => JSONRPCError::invalid_params(message),
        SessionManagementFailure::Unavailable => management_error(
            SESSION_MANAGEMENT_STATE,
            "SessionUnavailable",
            SessionManagementErrorData::Unavailable,
        ),
        SessionManagementFailure::NotOpen(lifecycle) => management_error(
            SESSION_MANAGEMENT_STATE,
            "SessionNotOpen",
            SessionManagementErrorData::SessionNotOpen { lifecycle },
        ),
        SessionManagementFailure::RevisionConflict {
            expected,
            current,
            cursor,
        } => management_error(
            SESSION_REVISION_CONFLICT,
            "SessionRevisionConflict",
            SessionManagementErrorData::RevisionConflict {
                expected_view_revision: expected,
                current_view_revision: current,
                current: cursor,
            },
        ),
        SessionManagementFailure::ListCursorInvalid => management_error(
            SESSION_CURSOR_REJECTED,
            "ListCursorInvalid",
            SessionManagementErrorData::ListCursorInvalid,
        ),
        SessionManagementFailure::ListCursorExpired => management_error(
            SESSION_CURSOR_REJECTED,
            "ListCursorExpired",
            SessionManagementErrorData::ListCursorExpired,
        ),
        SessionManagementFailure::HistoryCursorInvalid => management_error(
            SESSION_CURSOR_REJECTED,
            "HistoryCursorInvalid",
            SessionManagementErrorData::HistoryCursorInvalid,
        ),
        SessionManagementFailure::HistoryStreamReset { requested, current } => management_error(
            SESSION_CURSOR_REJECTED,
            "HistoryStreamReset",
            SessionManagementErrorData::HistoryStreamReset { requested, current },
        ),
        SessionManagementFailure::HistoryGap {
            requested,
            floor,
            current,
        } => management_error(
            SESSION_HISTORY_GAP,
            "HistoryGap",
            SessionManagementErrorData::HistoryGap {
                requested,
                floor,
                current_end: current,
            },
        ),
        SessionManagementFailure::TombstoneExpired(thread_id) => management_error(
            SESSION_MANAGEMENT_STATE,
            "TombstoneExpired",
            SessionManagementErrorData::TombstoneExpired { thread_id },
        ),
        SessionManagementFailure::ResourceLimit {
            resource,
            actual,
            limit,
            item_index,
        } => management_error(
            SESSION_MANAGEMENT_STATE,
            "SessionResourceLimit",
            SessionManagementErrorData::ResourceLimit {
                resource,
                actual,
                limit,
                item_index,
            },
        ),
        SessionManagementFailure::Storage(error) => {
            let outcome_unknown = matches!(
                error,
                StoreError::Io(_) | StoreError::Poisoned(_) | StoreError::Conflict
            );
            management_error(
                STORE_FAILED,
                "SessionStorageFailure",
                SessionManagementErrorData::StorageFailure { outcome_unknown },
            )
        }
        SessionManagementFailure::InvalidProjection(message) => {
            JSONRPCError::internal_error(message)
        }
    }
}

fn management_error(code: i64, message: &str, data: SessionManagementErrorData) -> JSONRPCError {
    debug_assert!(data.validate().is_ok());
    JSONRPCError::new(
        code,
        message,
        Some(serde_json::to_value(data).expect("Session management error data serializes")),
    )
}

impl DaemonServer {
    pub(super) fn handle_session_management_read(
        &self,
        id: RequestId,
        method: &str,
        params: Option<Value>,
        transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let value = params.unwrap_or(Value::Null);
        match method {
            whale_protocol::session_management::METHOD_SESSION_GET_V2 => {
                let params: GetSessionV2Params = match serde_json::from_value(value) {
                    Ok(params) => params,
                    Err(error) => {
                        return JSONRPCResponse::error(
                            id,
                            JSONRPCError::invalid_params(error.to_string()),
                        )
                    }
                };
                match self
                    .session_management
                    .get_v2(transport.connection_id(), &params)
                {
                    Ok(snapshot) => JSONRPCResponse::success(id, snapshot)
                        .expect("V2 Session snapshot serializes"),
                    Err(error) => JSONRPCResponse::error(id, rpc_error(error)),
                }
            }
            whale_protocol::session_management::METHOD_SESSION_SUBSCRIBE_V2 => {
                let params: SubscribeSessionV2Params = match serde_json::from_value(value) {
                    Ok(params) => params,
                    Err(error) => {
                        return JSONRPCResponse::error(
                            id,
                            JSONRPCError::invalid_params(error.to_string()),
                        )
                    }
                };
                match self
                    .session_management
                    .subscribe_v2(transport.connection_id(), &params)
                {
                    Ok(result) => JSONRPCResponse::success(id, result)
                        .expect("V2 Session replay result serializes"),
                    Err(error) => JSONRPCResponse::error(id, rpc_error(error)),
                }
            }
            _ => unreachable!("management reader only routes known methods"),
        }
    }

    pub(super) async fn handle_replace_session_metadata(
        &self,
        id: RequestId,
        params: Option<Value>,
        transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let params: ReplaceSessionMetadataParams =
            match serde_json::from_value(params.unwrap_or(Value::Null)) {
                Ok(params) => params,
                Err(error) => {
                    return JSONRPCResponse::error(
                        id,
                        JSONRPCError::invalid_params(error.to_string()),
                    )
                }
            };
        if let Err(error) = params.validate() {
            return JSONRPCResponse::error(id, JSONRPCError::invalid_params(error));
        }
        let owner = transport.connection_id().to_owned();
        let registry = self.session_management.clone();
        let journal = self
            .persistent_sessions
            .get(&params.thread_id)
            .map(|entry| entry.value().clone());
        let transaction =
            tokio::spawn(async move { registry.replace_metadata(&owner, params, journal).await });
        match transaction.await {
            Ok(Ok(result)) => JSONRPCResponse::success(id, result)
                .expect("Metadata replacement result serializes"),
            Ok(Err(error)) => JSONRPCResponse::error(id, rpc_error(error)),
            Err(_) => JSONRPCResponse::error(
                id,
                JSONRPCError::internal_error("Metadata transaction ended unexpectedly"),
            ),
        }
    }
}

fn spawn_notifier(transport: AnyTransportWriter) -> ManagementNotifier {
    let (queue, mut work) = mpsc::channel::<NotificationWork>(DEFAULT_NOTIFICATION_QUEUE);
    let (latest, mut latest_event) = watch::channel::<Option<SessionEventEnvelopeV2>>(None);
    let (cancel, mut cancelled) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut last_sent = 0;
        loop {
            let item = tokio::select! {
                biased;
                changed = cancelled.changed() => {
                    if changed.is_err() || *cancelled.borrow() {
                        break;
                    }
                    continue;
                }
                item = work.recv() => item,
                changed = latest_event.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    latest_event.borrow_and_update().clone().map(|envelope| NotificationWork {
                        terminal: is_closed(&envelope),
                        envelope,
                        completed: None,
                    })
                }
            };
            let Some(item) = item else {
                break;
            };
            if item.envelope.cursor.seq <= last_sent {
                if let Some(completed) = item.completed {
                    let _ = completed.send(());
                }
                continue;
            }
            if let Ok(notification) =
                JSONRPCNotification::new(METHOD_SESSION_EVENT_V2, Some(&item.envelope))
            {
                if let Ok(line) = serde_json::to_string(&notification) {
                    let _ = transport.send_line(&line).await;
                }
            }
            last_sent = item.envelope.cursor.seq;
            if let Some(completed) = item.completed {
                let _ = completed.send(());
            }
            if item.terminal {
                break;
            }
        }
    });
    ManagementNotifier {
        queue,
        latest,
        order: StdMutex::new(NotificationOrder::default()),
        cancel,
        task: task.abort_handle(),
    }
}

fn event_turn_id(envelope: &SessionEventEnvelopeV2) -> Option<&str> {
    match &envelope.payload {
        SessionEventPayloadV2::RunChanged { run } => Some(&run.snapshot.turn_id),
        SessionEventPayloadV2::RunEvent { event } => Some(&event.turn_id),
        SessionEventPayloadV2::MetadataChanged { .. }
        | SessionEventPayloadV2::LifecycleChanged { .. } => None,
        _ => None,
    }
}

fn is_closed(envelope: &SessionEventEnvelopeV2) -> bool {
    matches!(
        envelope.payload,
        SessionEventPayloadV2::LifecycleChanged {
            lifecycle: SessionLifecycleState::Closed
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::atomic::AtomicBool;
    use tokio::sync::{watch, Semaphore};
    use whale_protocol::{
        events::UsageMetrics,
        runs::{RunEventPayload, RunStatus},
        session_views::{GetSessionParams, SessionHistoryWindow},
    };

    struct NoopTransport;

    #[async_trait]
    impl OutgoingTransport for NoopTransport {
        async fn send_line(&self, _line: &str) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn writer() -> AnyTransportWriter {
        AnyTransportWriter::new(Arc::new(NoopTransport))
    }

    fn run_snapshot(status: RunStatus) -> RunSnapshot {
        RunSnapshot {
            tool_executions: Vec::new(),
            thread_id: "session".into(),
            turn_id: "run".into(),
            status,
            items: Vec::new(),
            usage: UsageMetrics::default(),
            pending_approvals: Vec::new(),
            last_seq: u64::from(status.is_terminal()),
            result: None,
            error: None,
        }
    }

    fn projection() -> ManagementProjection {
        let summary = SessionSummaryV2::new("session", 1);
        let cursor = SessionCursorV2 {
            thread_id: "session".into(),
            stream_id: "stream".into(),
            seq: 0,
        };
        ManagementProjection {
            snapshot: SessionSnapshotV2::new(
                summary,
                SessionLifecycleState::Open,
                SessionPersistenceV2::Ephemeral,
                SessionHistoryWindow::new(8).unwrap(),
                cursor.clone(),
            ),
            journal: VecDeque::new(),
            journal_bytes: 0,
            replay_floor: cursor,
            run_identities: HashMap::new(),
        }
    }

    #[test]
    fn run_retirement_recomputes_exact_remaining_journal_bytes_once() {
        let mut state = projection();
        state = state
            .prepare(
                2,
                SessionEventPayloadV2::RunChanged {
                    run: SessionRunView::new(run_snapshot(RunStatus::Running), 2),
                },
                Some(("run".into(), 7)),
            )
            .unwrap()
            .next;
        state = state
            .prepare(
                3,
                SessionEventPayloadV2::RunEvent {
                    event: RunEvent {
                        thread_id: "session".into(),
                        turn_id: "run".into(),
                        seq: 1,
                        payload: RunEventPayload::Finished {
                            snapshot: run_snapshot(RunStatus::Completed),
                        },
                    },
                },
                None,
            )
            .unwrap()
            .next;
        state = state
            .prepare(
                4,
                SessionEventPayloadV2::MetadataChanged {
                    metadata: Map::from_iter([("kept".into(), json!(true))]),
                },
                None,
            )
            .unwrap()
            .next;

        assert_eq!(
            state.journal_bytes,
            state.journal.iter().map(|entry| entry.bytes).sum::<usize>()
        );
        let retired = state.prepare_retire_run("run", 7).unwrap().next;
        assert_eq!(retired.replay_floor.seq, 2);
        assert_eq!(retired.journal.len(), 1);
        assert_eq!(retired.journal.front().unwrap().envelope.cursor.seq, 3);
        assert_eq!(
            retired.journal_bytes,
            retired
                .journal
                .iter()
                .map(|entry| entry.bytes)
                .sum::<usize>()
        );
    }

    #[tokio::test]
    async fn failed_second_projection_prepare_leaves_v1_unpublished() {
        let transport = writer();
        let owner = transport.connection_id().to_owned();
        let views = super::super::session_views::SessionViewRegistry::default();
        views
            .insert(
                &owner,
                "session".into(),
                None,
                Map::new(),
                Vec::new(),
                1,
                transport.clone(),
            )
            .unwrap();
        let management = SessionManagementRegistry::default();
        management
            .insert(
                &owner,
                "session".into(),
                None,
                Map::new(),
                Vec::new(),
                1,
                SessionPersistenceV2::Ephemeral,
                None,
                transport,
            )
            .unwrap();
        let record = management.records.get("session").unwrap().value().clone();
        {
            let mut state = record.projection.lock().unwrap();
            state.snapshot.cursor.seq = u64::MAX;
            state.snapshot.summary.view_revision = u64::MAX;
        }

        let result = management
            .publish_run(
                &views,
                &owner,
                RunPublicationMutation::Begin {
                    snapshot: run_snapshot(RunStatus::Running),
                    identity: 7,
                },
                2,
            )
            .await;
        assert!(matches!(
            result,
            Err(SessionManagementFailure::InvalidProjection(_))
        ));
        let v1 = views
            .get(
                &owner,
                &GetSessionParams {
                    thread_id: "session".into(),
                    history_limit: 8,
                },
            )
            .unwrap();
        assert_eq!(v1.cursor.seq, 0);
        assert!(v1.active_run.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn tombstone_byte_pressure_evicts_oldest_closed_only() {
        let transport = writer();
        let owner = transport.connection_id().to_owned();
        let mut management =
            SessionManagementRegistry::with_limits_for_test(usize::MAX, usize::MAX, u64::MAX);

        for (thread_id, payload) in [
            ("closed-a", "a".repeat(1_024)),
            ("closed-b", "b".repeat(1_024)),
            ("closing", "c".repeat(8_192)),
            ("open", "d".repeat(8_192)),
        ] {
            management
                .insert(
                    &owner,
                    thread_id.into(),
                    None,
                    Map::from_iter([("payload".into(), json!(payload))]),
                    Vec::new(),
                    1,
                    SessionPersistenceV2::Ephemeral,
                    None,
                    transport.clone(),
                )
                .unwrap();
        }

        let closed_a = management.records.get("closed-a").unwrap().value().clone();
        closed_a.projection.lock().unwrap().snapshot.lifecycle = SessionLifecycleState::Closed;
        *closed_a.closed_at.lock().unwrap() = Some(tokio::time::Instant::now());
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        let closed_b = management.records.get("closed-b").unwrap().value().clone();
        closed_b.projection.lock().unwrap().snapshot.lifecycle = SessionLifecycleState::Closed;
        *closed_b.closed_at.lock().unwrap() = Some(tokio::time::Instant::now());
        management
            .records
            .get("closing")
            .unwrap()
            .projection
            .lock()
            .unwrap()
            .snapshot
            .lifecycle = SessionLifecycleState::Closing;

        let retained_closed_bytes = retained_record_bytes(&closed_a)
            .checked_add(retained_record_bytes(&closed_b))
            .unwrap();
        management.limits.max_closed_tombstone_bytes = retained_closed_bytes - 1;
        management.prune_owner(&owner, None);

        assert!(!management.records.contains_key("closed-a"));
        assert!(management.records.contains_key("closed-b"));
        assert!(management.records.contains_key("closing"));
        assert!(management.records.contains_key("open"));
        let catalog = management.owners.get(&owner).unwrap();
        let state = catalog.state.lock().unwrap();
        assert_eq!(state.generation, 1);
        assert_eq!(
            state.expired_tombstones.front().map(String::as_str),
            Some("closed-a")
        );
    }

    struct BlockingCapture {
        block_first: AtomicBool,
        entered: Arc<Semaphore>,
        release: Arc<Semaphore>,
        delivered: watch::Sender<u64>,
        sent: Arc<StdMutex<Vec<u64>>>,
    }

    #[async_trait]
    impl OutgoingTransport for BlockingCapture {
        async fn send_line(&self, line: &str) -> std::io::Result<()> {
            if self.block_first.swap(false, Ordering::AcqRel) {
                self.entered.add_permits(1);
                let _permit = self.release.acquire().await.unwrap();
            }
            let value: Value = serde_json::from_str(line).unwrap();
            let seq = value["params"]["cursor"]["seq"].as_u64().unwrap();
            self.sent.lock().unwrap().push(seq);
            self.delivered.send_replace(seq);
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bounded_notifier_lag_preserves_monotonic_delivery_and_latest_cursor() {
        let entered = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let sent = Arc::new(StdMutex::new(Vec::new()));
        let (delivered, mut delivered_rx) = watch::channel(0);
        let notifier = spawn_notifier(AnyTransportWriter::new(Arc::new(BlockingCapture {
            block_first: AtomicBool::new(true),
            entered: entered.clone(),
            release: release.clone(),
            delivered,
            sent: sent.clone(),
        })));
        notifier.activate(0);
        for seq in 1..=300 {
            notifier.publish(SessionEventEnvelopeV2::new(
                "session",
                SessionCursorV2 {
                    thread_id: "session".into(),
                    stream_id: "stream".into(),
                    seq,
                },
                seq,
                SessionEventPayloadV2::MetadataChanged {
                    metadata: Map::from_iter([("seq".into(), json!(seq))]),
                },
            ));
        }
        let _entered = entered.acquire().await.unwrap();
        release.add_permits(1);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while *delivered_rx.borrow_and_update() != 300 {
                delivered_rx.changed().await.unwrap();
            }
        })
        .await
        .expect("latest V2 cursor remains observable after queue saturation");

        let sent = sent.lock().unwrap();
        assert_eq!(sent.last(), Some(&300));
        assert!(sent.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(
            sent.len() < 300,
            "bounded notifier must coalesce saturated work"
        );
        drop(sent);
        notifier.close();
    }
}
