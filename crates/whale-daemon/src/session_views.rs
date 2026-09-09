//! Short-lock authoritative Session projections and bounded live-attachment replay.

use super::DaemonServer;
use crate::transport::{AnyTransportWriter, OutgoingTransport};
use dashmap::{mapref::entry::Entry, DashMap};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex as StdMutex},
};
use tokio::sync::{broadcast, watch};
use whale_protocol::{
    canonical::CanonicalItem,
    rpc::{JSONRPCError, JSONRPCNotification, JSONRPCResponse, RequestId},
    runs::{RunEvent, RunSnapshot},
    session_views::{
        GetSessionParams, ReplayGap, ReplayGapReason, SessionCursor, SessionEventEnvelope,
        SessionEventPayload, SessionHistoryWindow, SessionRunView, SessionSnapshot, SessionSummary,
        SubscribeSessionParams, SubscribeSessionResult, MAX_SESSION_HISTORY_LIMIT,
        METHOD_SESSION_EVENT,
    },
};

const DEFAULT_JOURNAL_EVENTS: usize = 1024;
const DEFAULT_JOURNAL_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_NOTIFICATION_QUEUE: usize = 128;

#[derive(Clone, Copy)]
pub(super) struct EventJournalLimits {
    max_events: usize,
    max_bytes: usize,
}

impl EventJournalLimits {
    pub(super) fn new(max_events: usize, max_bytes: usize) -> Self {
        assert!(max_events > 0, "event journal count must be positive");
        assert!(max_bytes > 0, "event journal byte limit must be positive");
        Self {
            max_events,
            max_bytes,
        }
    }
}

impl Default for EventJournalLimits {
    fn default() -> Self {
        Self::new(DEFAULT_JOURNAL_EVENTS, DEFAULT_JOURNAL_BYTES)
    }
}

#[derive(Clone)]
struct JournalEntry {
    envelope: SessionEventEnvelope,
    bytes: usize,
    run_id: Option<String>,
}

struct SessionNotifier {
    events: broadcast::Sender<SessionEventEnvelope>,
    cancel: watch::Sender<bool>,
    task: tokio::task::AbortHandle,
}

impl SessionNotifier {
    fn publish(&self, envelope: SessionEventEnvelope) {
        let _ = self.events.send(envelope);
    }

    fn close(&self) {
        self.cancel.send_replace(true);
        self.task.abort();
    }
}

impl Drop for SessionNotifier {
    fn drop(&mut self) {
        self.close();
    }
}

pub(super) struct SessionViewRecord {
    owner: String,
    pub(super) snapshot: SessionSnapshot,
    journal: VecDeque<JournalEntry>,
    journal_bytes: usize,
    replay_floor: SessionCursor,
    limits: EventJournalLimits,
    notifier: Option<SessionNotifier>,
    run_identities: HashMap<String, usize>,
}

struct PreparedSessionViewState {
    snapshot: SessionSnapshot,
    journal: VecDeque<JournalEntry>,
    journal_bytes: usize,
    replay_floor: SessionCursor,
    run_identities: HashMap<String, usize>,
}

pub(super) struct PreparedSessionViewPublication {
    pub(super) record: Arc<StdMutex<SessionViewRecord>>,
    owner: String,
    source: SessionCursor,
    next: PreparedSessionViewState,
    envelope: SessionEventEnvelope,
}

pub(super) struct PreparedSessionViewRetirement {
    pub(super) record: Arc<StdMutex<SessionViewRecord>>,
    owner: String,
    source: SessionCursor,
    next: PreparedSessionViewState,
}

impl PreparedSessionViewPublication {
    pub(super) fn source_matches(&self, record: &SessionViewRecord) -> bool {
        record.owner == self.owner && record.snapshot.cursor == self.source
    }

    pub(super) fn install(self, record: &mut SessionViewRecord) -> SessionEventEnvelope {
        record.snapshot = self.next.snapshot;
        record.journal = self.next.journal;
        record.journal_bytes = self.next.journal_bytes;
        record.replay_floor = self.next.replay_floor;
        record.run_identities = self.next.run_identities;
        self.envelope
    }
}

impl PreparedSessionViewRetirement {
    pub(super) fn source_matches(&self, record: &SessionViewRecord) -> bool {
        record.owner == self.owner && record.snapshot.cursor == self.source
    }

    pub(super) fn install(self, record: &mut SessionViewRecord) {
        record.snapshot = self.next.snapshot;
        record.journal = self.next.journal;
        record.journal_bytes = self.next.journal_bytes;
        record.replay_floor = self.next.replay_floor;
        record.run_identities = self.next.run_identities;
    }
}

impl SessionViewRecord {
    fn with_owner(
        owner: String,
        summary: SessionSummary,
        history: SessionHistoryWindow,
        stream_id: String,
        limits: EventJournalLimits,
    ) -> Result<Self, String> {
        let cursor = SessionCursor {
            thread_id: summary.thread_id.clone(),
            stream_id,
            seq: 0,
        };
        let snapshot = SessionSnapshot::new(summary, history, cursor.clone());
        snapshot.validate()?;
        Ok(Self {
            owner,
            snapshot,
            journal: VecDeque::new(),
            journal_bytes: 0,
            replay_floor: cursor,
            limits,
            notifier: None,
            run_identities: HashMap::new(),
        })
    }

    #[cfg(test)]
    fn new(
        summary: SessionSummary,
        history: SessionHistoryWindow,
        stream_id: String,
        limits: EventJournalLimits,
    ) -> Result<Self, String> {
        Self::with_owner("test-owner".into(), summary, history, stream_id, limits)
    }

    #[cfg(test)]
    pub(super) fn commit(
        &mut self,
        occurred_at_ms: u64,
        payload: SessionEventPayload,
    ) -> Result<SessionEventEnvelope, String> {
        let (next, envelope) = self.prepare(occurred_at_ms, payload, None)?;
        self.snapshot = next.snapshot;
        self.journal = next.journal;
        self.journal_bytes = next.journal_bytes;
        self.replay_floor = next.replay_floor;
        self.run_identities = next.run_identities;
        Ok(envelope)
    }

    fn prepare(
        &self,
        occurred_at_ms: u64,
        payload: SessionEventPayload,
        run_identity: Option<(String, usize)>,
    ) -> Result<(PreparedSessionViewState, SessionEventEnvelope), String> {
        let cursor = self.snapshot.cursor.checked_next()?;
        let envelope = SessionEventEnvelope::new(
            self.snapshot.summary.thread_id.clone(),
            cursor,
            occurred_at_ms,
            payload,
        );
        envelope.validate()?;
        let bytes = serde_json::to_vec(&envelope)
            .map_err(|error| format!("Session event serialization failed: {error}"))?
            .len();
        let run_id = event_turn_id(&envelope).map(str::to_owned).or_else(|| {
            self.snapshot
                .active_run
                .as_ref()
                .map(|run| run.snapshot.turn_id.clone())
        });
        let mut next = self.snapshot.clone();
        next.apply(&envelope).map_err(|error| error.to_string())?;

        let mut state = PreparedSessionViewState {
            snapshot: next,
            journal: self.journal.clone(),
            journal_bytes: self.journal_bytes,
            replay_floor: self.replay_floor.clone(),
            run_identities: self.run_identities.clone(),
        };
        retain_prepared(
            &mut state.journal,
            &mut state.journal_bytes,
            &mut state.replay_floor,
            self.limits,
            envelope.clone(),
            bytes,
            run_id,
        );
        if let Some((turn_id, identity)) = run_identity {
            state.run_identities.insert(turn_id, identity);
        }
        Ok((state, envelope))
    }

    fn snapshot_with_history_limit(&self, history_limit: u32) -> SessionSnapshot {
        let mut snapshot = self.snapshot.clone();
        let keep = snapshot.history.items.len().min(history_limit as usize);
        let start = snapshot.history.items.len() - keep;
        snapshot.history.items = snapshot.history.items[start..].to_vec();
        snapshot.history.start_index = snapshot.history.total_items - keep as u64;
        snapshot.history.capacity = history_limit;
        snapshot
    }

    fn replay(&self, params: &SubscribeSessionParams) -> Result<SubscribeSessionResult, String> {
        params.validate()?;
        let current = self.snapshot.cursor.clone();
        let response_through = params.through.clone().unwrap_or_else(|| current.clone());

        if params.after.stream_id != current.stream_id {
            return Ok(self.gap(params, current, ReplayGapReason::StreamReset));
        }
        if params.after.seq > current.seq {
            return Err("Replay cursor is ahead of the current Session cursor".into());
        }
        if let Some(through) = &params.through {
            if through.stream_id != current.stream_id {
                return Ok(self.gap(params, current, ReplayGapReason::StreamReset));
            }
            if through.seq > current.seq {
                return Err(
                    "Fixed replay high watermark is ahead of the current Session cursor".into(),
                );
            }
        }
        if params.after.seq < self.replay_floor.seq {
            return Ok(self.gap(params, response_through, ReplayGapReason::Retention));
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
        Ok(SubscribeSessionResult {
            has_more: resume_after.seq < response_through.seq,
            events,
            resume_after,
            through: response_through,
            gap: None,
        })
    }

    fn gap(
        &self,
        params: &SubscribeSessionParams,
        through: SessionCursor,
        reason: ReplayGapReason,
    ) -> SubscribeSessionResult {
        SubscribeSessionResult {
            events: Vec::new(),
            resume_after: params.after.clone(),
            through,
            has_more: false,
            gap: Some(ReplayGap {
                reason,
                requested: params.after.clone(),
                replay_floor: self.replay_floor.clone(),
                current: self.snapshot.cursor.clone(),
                session_revision: self.snapshot.summary.revision,
            }),
        }
    }

    fn notify(&self, envelope: SessionEventEnvelope) {
        if let Some(notifier) = &self.notifier {
            notifier.publish(envelope);
        }
    }

    #[cfg(test)]
    fn retire_run(&mut self, turn_id: &str, identity: usize) -> bool {
        if self.run_identities.get(turn_id).copied() != Some(identity) {
            return false;
        }
        if self
            .snapshot
            .active_run
            .as_ref()
            .is_some_and(|run| run.snapshot.turn_id == turn_id)
        {
            return false;
        }
        let through = self
            .journal
            .iter()
            .filter(|entry| entry.run_id.as_deref() == Some(turn_id))
            .map(|entry| entry.envelope.cursor.seq)
            .max();
        if let Some(through) = through {
            while self
                .journal
                .front()
                .is_some_and(|entry| entry.envelope.cursor.seq <= through)
            {
                let evicted = self.journal.pop_front().expect("checked nonempty");
                self.journal_bytes -= evicted.bytes;
            }
            self.replay_floor.seq = self.replay_floor.seq.max(through);
        }
        self.run_identities.remove(turn_id);
        true
    }
}

fn retain_prepared(
    journal: &mut VecDeque<JournalEntry>,
    journal_bytes: &mut usize,
    replay_floor: &mut SessionCursor,
    limits: EventJournalLimits,
    envelope: SessionEventEnvelope,
    bytes: usize,
    run_id: Option<String>,
) {
    if bytes > limits.max_bytes {
        journal.clear();
        *journal_bytes = 0;
        *replay_floor = envelope.cursor;
        return;
    }
    journal.push_back(JournalEntry {
        envelope,
        bytes,
        run_id,
    });
    *journal_bytes += bytes;
    while journal.len() > limits.max_events || *journal_bytes > limits.max_bytes {
        let evicted = journal.pop_front().expect("nonempty bounded journal");
        *journal_bytes -= evicted.bytes;
        *replay_floor = evicted.envelope.cursor;
    }
}

fn scrub_run(
    journal: &mut VecDeque<JournalEntry>,
    journal_bytes: &mut usize,
    replay_floor: &mut SessionCursor,
    turn_id: &str,
) {
    let through = journal
        .iter()
        .filter(|entry| entry.run_id.as_deref() == Some(turn_id))
        .map(|entry| entry.envelope.cursor.seq)
        .max();
    if let Some(through) = through {
        while journal
            .front()
            .is_some_and(|entry| entry.envelope.cursor.seq <= through)
        {
            let evicted = journal.pop_front().expect("checked nonempty");
            *journal_bytes -= evicted.bytes;
        }
        replay_floor.seq = replay_floor.seq.max(through);
    }
}

#[derive(Clone)]
pub(super) struct SessionViewRegistry {
    records: Arc<DashMap<String, Arc<StdMutex<SessionViewRecord>>>>,
    limits: EventJournalLimits,
}

impl Default for SessionViewRegistry {
    fn default() -> Self {
        Self {
            records: Arc::new(DashMap::new()),
            limits: EventJournalLimits::default(),
        }
    }
}

impl SessionViewRegistry {
    #[cfg(test)]
    pub(super) fn with_limits(limits: EventJournalLimits) -> Self {
        Self {
            records: Arc::new(DashMap::new()),
            limits,
        }
    }

    pub(super) fn insert(
        &self,
        owner: &str,
        thread_id: String,
        agent_name: Option<String>,
        metadata: serde_json::Map<String, serde_json::Value>,
        history: Vec<CanonicalItem>,
        created_at_ms: u64,
        transport: AnyTransportWriter,
    ) -> Result<(), String> {
        let mut summary = SessionSummary::new(thread_id.clone(), created_at_ms);
        summary.agent_name = agent_name;
        summary.metadata = metadata;
        let history = SessionHistoryWindow::from_history(&history, MAX_SESSION_HISTORY_LIMIT)?;
        let mut record = SessionViewRecord::with_owner(
            owner.into(),
            summary,
            history,
            uuid::Uuid::new_v4().to_string(),
            self.limits,
        )?;
        record.notifier = Some(spawn_notifier(transport));
        match self.records.entry(thread_id) {
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(StdMutex::new(record)));
            }
            Entry::Occupied(_) => return Err("SessionAlreadyExists".into()),
        }
        Ok(())
    }

    fn owned(
        &self,
        thread_id: &str,
        owner: &str,
    ) -> Result<Arc<StdMutex<SessionViewRecord>>, String> {
        let record = self
            .records
            .get(thread_id)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| "SessionNotFound".to_string())?;
        if record.lock().unwrap().owner != owner {
            return Err("SessionNotFound".into());
        }
        Ok(record)
    }

    pub(super) fn get(
        &self,
        owner: &str,
        params: &GetSessionParams,
    ) -> Result<SessionSnapshot, String> {
        params.validate()?;
        let record = self.owned(&params.thread_id, owner)?;
        let snapshot = record
            .lock()
            .unwrap()
            .snapshot_with_history_limit(params.history_limit);
        Ok(snapshot)
    }

    pub(super) fn subscribe(
        &self,
        owner: &str,
        params: &SubscribeSessionParams,
    ) -> Result<SubscribeSessionResult, String> {
        let record = self.owned(&params.thread_id, owner)?;
        let result = record.lock().unwrap().replay(params);
        result
    }

    #[cfg(test)]
    pub(super) fn begin_run(
        &self,
        owner: &str,
        snapshot: RunSnapshot,
        identity: usize,
        occurred_at_ms: u64,
    ) -> Result<SessionEventEnvelope, String> {
        let prepared = self.prepare_begin_run(owner, snapshot, identity, occurred_at_ms)?;
        let record = prepared.record.clone();
        let mut record = record.lock().unwrap();
        if !prepared.source_matches(&record) {
            return Err("Session projection changed during publication".into());
        }
        Ok(prepared.install(&mut record))
    }

    pub(super) fn prepare_begin_run(
        &self,
        owner: &str,
        snapshot: RunSnapshot,
        identity: usize,
        occurred_at_ms: u64,
    ) -> Result<PreparedSessionViewPublication, String> {
        let record = self.owned(&snapshot.thread_id, owner)?;
        let turn_id = snapshot.turn_id.clone();
        let guard = record.lock().unwrap();
        let source = guard.snapshot.cursor.clone();
        let (next, envelope) = guard.prepare(
            occurred_at_ms,
            SessionEventPayload::RunChanged {
                run: SessionRunView::new(snapshot, occurred_at_ms),
            },
            Some((turn_id, identity)),
        )?;
        let owner = guard.owner.clone();
        drop(guard);
        Ok(PreparedSessionViewPublication {
            record,
            owner,
            source,
            next,
            envelope,
        })
    }

    pub(super) fn publish_committed(&self, owner: &str, envelope: SessionEventEnvelope) {
        if let Some(record) = self.records.get(&envelope.thread_id) {
            let record = record.value().lock().unwrap();
            if record.owner == owner
                && record.snapshot.cursor.stream_id == envelope.cursor.stream_id
            {
                record.notify(envelope);
            }
        }
    }

    #[cfg(test)]
    pub(super) fn apply_run_event(
        &self,
        owner: &str,
        event: RunEvent,
        occurred_at_ms: u64,
    ) -> Result<SessionEventEnvelope, String> {
        let prepared = self.prepare_run_event(owner, event, occurred_at_ms)?;
        let record = prepared.record.clone();
        let mut record = record.lock().unwrap();
        if !prepared.source_matches(&record) {
            return Err("Session projection changed during publication".into());
        }
        Ok(prepared.install(&mut record))
    }

    pub(super) fn prepare_run_event(
        &self,
        owner: &str,
        event: RunEvent,
        occurred_at_ms: u64,
    ) -> Result<PreparedSessionViewPublication, String> {
        let record = self.owned(&event.thread_id, owner)?;
        let guard = record.lock().unwrap();
        let source = guard.snapshot.cursor.clone();
        let (next, envelope) = guard.prepare(
            occurred_at_ms,
            SessionEventPayload::RunEvent { event },
            None,
        )?;
        let owner = guard.owner.clone();
        drop(guard);
        Ok(PreparedSessionViewPublication {
            record,
            owner,
            source,
            next,
            envelope,
        })
    }

    pub(super) fn prepare_run_changed(
        &self,
        owner: &str,
        snapshot: RunSnapshot,
        occurred_at_ms: u64,
    ) -> Result<PreparedSessionViewPublication, String> {
        let record = self.owned(&snapshot.thread_id, owner)?;
        let guard = record.lock().unwrap();
        let active = guard
            .snapshot
            .active_run
            .as_ref()
            .ok_or_else(|| "Session has no active Run projection".to_string())?;
        if active.snapshot.turn_id != snapshot.turn_id {
            return Err("Session active Run identity changed".into());
        }
        let mut run = active.clone();
        run.snapshot = snapshot;
        run.updated_at_ms = run.updated_at_ms.max(occurred_at_ms);
        let source = guard.snapshot.cursor.clone();
        let (next, envelope) = guard.prepare(
            occurred_at_ms,
            SessionEventPayload::RunChanged { run },
            None,
        )?;
        let owner = guard.owner.clone();
        drop(guard);
        Ok(PreparedSessionViewPublication {
            record,
            owner,
            source,
            next,
            envelope,
        })
    }

    #[cfg(test)]
    pub(super) fn retire_run(&self, thread_id: &str, turn_id: &str, identity: usize) -> bool {
        let Ok(Some(prepared)) = self.prepare_retire_run(thread_id, turn_id, identity) else {
            return false;
        };
        let record = prepared.record.clone();
        let mut guard = record.lock().unwrap();
        if !prepared.source_matches(&guard) {
            return false;
        }
        prepared.install(&mut guard);
        true
    }

    pub(super) fn prepare_retire_run(
        &self,
        thread_id: &str,
        turn_id: &str,
        identity: usize,
    ) -> Result<Option<PreparedSessionViewRetirement>, String> {
        let record = self
            .records
            .get(thread_id)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| "SessionNotFound".to_string())?;
        let guard = record.lock().unwrap();
        if guard.run_identities.get(turn_id).copied() != Some(identity)
            || guard
                .snapshot
                .active_run
                .as_ref()
                .is_some_and(|run| run.snapshot.turn_id == turn_id)
        {
            return Ok(None);
        }
        let mut next = PreparedSessionViewState {
            snapshot: guard.snapshot.clone(),
            journal: guard.journal.clone(),
            journal_bytes: guard.journal_bytes,
            replay_floor: guard.replay_floor.clone(),
            run_identities: guard.run_identities.clone(),
        };
        scrub_run(
            &mut next.journal,
            &mut next.journal_bytes,
            &mut next.replay_floor,
            turn_id,
        );
        next.run_identities.remove(turn_id);
        let owner = guard.owner.clone();
        let source = guard.snapshot.cursor.clone();
        drop(guard);
        Ok(Some(PreparedSessionViewRetirement {
            record,
            owner,
            source,
            next,
        }))
    }

    pub(super) fn remove(&self, thread_id: &str, owner: &str) {
        let Some(record) = self
            .records
            .get(thread_id)
            .map(|entry| entry.value().clone())
        else {
            return;
        };
        let guard = record.lock().unwrap();
        if guard.owner != owner {
            return;
        }
        if let Some(notifier) = &guard.notifier {
            notifier.close();
        }
        drop(guard);
        self.records
            .remove_if(thread_id, |_, current| Arc::ptr_eq(current, &record));
    }
}

fn spawn_notifier(transport: AnyTransportWriter) -> SessionNotifier {
    let (events, mut receiver) = broadcast::channel(DEFAULT_NOTIFICATION_QUEUE);
    let (cancel, mut cancelled) = watch::channel(false);
    let task = tokio::spawn(async move {
        loop {
            let envelope = tokio::select! {
                biased;
                changed = cancelled.changed() => {
                    if changed.is_err() || *cancelled.borrow() {
                        break;
                    }
                    continue;
                }
                received = receiver.recv() => match received {
                    Ok(envelope) => envelope,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            };
            let Ok(notification) = JSONRPCNotification::new(METHOD_SESSION_EVENT, Some(envelope))
            else {
                continue;
            };
            let Ok(line) = serde_json::to_string(&notification) else {
                continue;
            };
            loop {
                let delivered = tokio::select! {
                    biased;
                    changed = cancelled.changed() => {
                        if changed.is_err() || *cancelled.borrow() {
                            return;
                        }
                        false
                    }
                    result = tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        transport.send_line(&line),
                    ) => matches!(result, Ok(Ok(()))),
                };
                if delivered {
                    break;
                }
                tokio::select! {
                    biased;
                    changed = cancelled.changed() => {
                        if changed.is_err() || *cancelled.borrow() {
                            return;
                        }
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
                }
            }
        }
    });
    SessionNotifier {
        events,
        cancel,
        task: task.abort_handle(),
    }
}

fn event_turn_id(envelope: &SessionEventEnvelope) -> Option<&str> {
    match &envelope.payload {
        SessionEventPayload::RunChanged { run } => Some(&run.snapshot.turn_id),
        SessionEventPayload::RunEvent { event } => Some(&event.turn_id),
        _ => None,
    }
}

pub(super) fn rpc_error(error: String) -> JSONRPCError {
    JSONRPCError::invalid_params(error)
}

impl DaemonServer {
    pub(super) fn handle_session_view(
        &self,
        id: RequestId,
        method: &str,
        params: Option<serde_json::Value>,
        transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let value = params.unwrap_or(serde_json::Value::Null);
        match method {
            whale_protocol::session_views::METHOD_SESSION_GET => {
                let params: GetSessionParams = match serde_json::from_value(value) {
                    Ok(params) => params,
                    Err(error) => {
                        return JSONRPCResponse::error(
                            id,
                            JSONRPCError::invalid_params(error.to_string()),
                        )
                    }
                };
                match self.session_views.get(transport.connection_id(), &params) {
                    Ok(snapshot) => {
                        JSONRPCResponse::success(id, snapshot).expect("Session snapshot serializes")
                    }
                    Err(error) => JSONRPCResponse::error(id, rpc_error(error)),
                }
            }
            whale_protocol::session_views::METHOD_SESSION_SUBSCRIBE => {
                let params: SubscribeSessionParams = match serde_json::from_value(value) {
                    Ok(params) => params,
                    Err(error) => {
                        return JSONRPCResponse::error(
                            id,
                            JSONRPCError::invalid_params(error.to_string()),
                        )
                    }
                };
                match self
                    .session_views
                    .subscribe(transport.connection_id(), &params)
                {
                    Ok(result) => JSONRPCResponse::success(id, result)
                        .expect("Session replay result serializes"),
                    Err(error) => JSONRPCResponse::error(id, rpc_error(error)),
                }
            }
            _ => unreachable!("view dispatcher only routes known methods"),
        }
    }
}

pub(super) fn unix_ms() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Map;
    use whale_protocol::{
        canonical::{CanonicalContent, CanonicalItem, MessagePhase},
        events::AgentStreamEvent,
        runs::{RunEventPayload, RunSnapshot, RunStatus},
        session_views::{SessionEventPayload, SessionHistoryWindow, SessionSummary},
    };

    fn snapshot(thread: &str, turn: &str) -> RunSnapshot {
        RunSnapshot {
            thread_id: thread.into(),
            turn_id: turn.into(),
            status: RunStatus::Running,
            items: vec![],
            usage: Default::default(),
            pending_approvals: vec![],
            tool_executions: vec![],
            last_seq: 0,
            result: None,
            error: None,
        }
    }

    fn record(limits: EventJournalLimits) -> SessionViewRecord {
        SessionViewRecord::new(
            SessionSummary::new("session", 10),
            SessionHistoryWindow::new(8).unwrap(),
            "stream".into(),
            limits,
        )
        .unwrap()
    }

    fn changed(turn: &str, at: u64) -> SessionEventPayload {
        SessionEventPayload::RunChanged {
            run: SessionRunView::new(snapshot("session", turn), at),
        }
    }

    fn stream(turn: &str, seq: u64, event: AgentStreamEvent) -> SessionEventPayload {
        SessionEventPayload::RunEvent {
            event: RunEvent {
                thread_id: "session".into(),
                turn_id: turn.into(),
                seq,
                payload: RunEventPayload::Stream { event },
            },
        }
    }

    #[test]
    fn projection_starts_at_zero_and_two_runs_share_one_session_sequence() {
        let mut summary = SessionSummary::new("session", 10);
        summary.agent_name = Some("agent".into());
        summary.metadata = Map::new();
        let mut record = SessionViewRecord::new(
            summary,
            SessionHistoryWindow::new(8).unwrap(),
            "stream".into(),
            EventJournalLimits::new(8, 64 * 1024),
        )
        .unwrap();
        assert_eq!(record.snapshot.cursor.seq, 0);

        let first = record
            .commit(
                11,
                SessionEventPayload::RunChanged {
                    run: whale_protocol::session_views::SessionRunView::new(
                        snapshot("session", "one"),
                        11,
                    ),
                },
            )
            .unwrap();
        let second = record
            .commit(
                12,
                SessionEventPayload::RunChanged {
                    run: whale_protocol::session_views::SessionRunView::new(
                        snapshot("session", "two"),
                        12,
                    ),
                },
            )
            .unwrap();
        assert_eq!((first.cursor.seq, second.cursor.seq), (1, 2));
    }

    #[test]
    fn partial_drafts_complete_once_and_replay_reduces_to_authoritative_state() {
        let mut record = record(EventJournalLimits::new(32, 128 * 1024));
        record.commit(11, changed("run", 11)).unwrap();
        let events = [
            AgentStreamEvent::ItemStarted {
                turn_id: "run".into(),
                item_id: "draft".into(),
                item_type: "assistant_message".into(),
                phase: Some(MessagePhase::FinalAnswer),
            },
            AgentStreamEvent::TextDelta {
                turn_id: "run".into(),
                item_id: "draft".into(),
                delta: "hello".into(),
            },
            AgentStreamEvent::ReasoningDelta {
                turn_id: "run".into(),
                item_id: "draft".into(),
                delta: " thought".into(),
            },
            AgentStreamEvent::ReasoningSignature {
                turn_id: "run".into(),
                item_id: "draft".into(),
                signature: "signed".into(),
            },
            AgentStreamEvent::ItemStarted {
                turn_id: "run".into(),
                item_id: "tool-draft".into(),
                item_type: "tool_call".into(),
                phase: None,
            },
            AgentStreamEvent::ToolCallDelta {
                turn_id: "run".into(),
                item_id: "tool-draft".into(),
                call_id: "call".into(),
                delta: "{\"q\":".into(),
            },
            AgentStreamEvent::ToolExecutionStarted {
                turn_id: "run".into(),
                call_id: "call".into(),
                original_arguments: serde_json::json!({"q":"old"}),
                arguments: serde_json::json!({"q":"new"}),
            },
        ];
        let next_seq = events.len() as u64 + 1;
        for (offset, event) in events.into_iter().enumerate() {
            record
                .commit(12 + offset as u64, stream("run", offset as u64 + 1, event))
                .unwrap();
        }
        let active = record.snapshot.active_run.as_ref().unwrap();
        assert_eq!(active.in_progress_items[0].text, "hello thought");
        assert_eq!(
            active.in_progress_items[0].reasoning_signature.as_deref(),
            Some("signed")
        );
        let tool = active
            .in_progress_items
            .iter()
            .find(|draft| draft.item_id == "tool-draft")
            .unwrap();
        assert_eq!(tool.call_id.as_deref(), Some("call"));
        assert_eq!(tool.raw_arguments, "{\"q\":");
        assert_eq!(active.snapshot.tool_executions[0].arguments["q"], "new");

        let item = CanonicalItem::AssistantMessage {
            id: "draft".into(),
            content: vec![CanonicalContent::text("hello")],
            phase: MessagePhase::FinalAnswer,
        };
        record
            .commit(
                20,
                stream(
                    "run",
                    next_seq,
                    AgentStreamEvent::ItemCompleted {
                        turn_id: "run".into(),
                        item: item.clone(),
                    },
                ),
            )
            .unwrap();
        record
            .commit(
                21,
                stream(
                    "run",
                    next_seq + 1,
                    AgentStreamEvent::ItemCompleted {
                        turn_id: "run".into(),
                        item,
                    },
                ),
            )
            .unwrap();
        assert!(!record
            .snapshot
            .active_run
            .as_ref()
            .unwrap()
            .in_progress_items
            .iter()
            .any(|draft| draft.item_id == "draft"));
        assert_eq!(record.snapshot.history.items.len(), 1);

        let mut reconstructed = SessionSnapshot::new(
            SessionSummary::new("session", 10),
            SessionHistoryWindow::new(8).unwrap(),
            SessionCursor {
                thread_id: "session".into(),
                stream_id: "stream".into(),
                seq: 0,
            },
        );
        for entry in &record.journal {
            reconstructed.apply(&entry.envelope).unwrap();
        }
        assert_eq!(reconstructed, record.snapshot);
    }

    #[test]
    fn count_oversize_and_run_retirement_advance_one_contiguous_floor() {
        let mut count = record(EventJournalLimits::new(2, 128 * 1024));
        for (index, turn) in ["one", "two", "three"].into_iter().enumerate() {
            count
                .commit(11 + index as u64, changed(turn, 11 + index as u64))
                .unwrap();
        }
        assert_eq!(count.replay_floor.seq, 1);
        assert_eq!(count.journal.len(), 2);
        let gap = count
            .replay(&SubscribeSessionParams {
                thread_id: "session".into(),
                after: SessionCursor {
                    thread_id: "session".into(),
                    stream_id: "stream".into(),
                    seq: 0,
                },
                through: None,
                limit: 2,
            })
            .unwrap();
        assert_eq!(gap.gap.unwrap().reason, ReplayGapReason::Retention);

        let mut oversized = record(EventJournalLimits::new(8, 1));
        oversized.commit(11, changed("huge", 11)).unwrap();
        assert!(oversized.journal.is_empty());
        assert_eq!(oversized.replay_floor.seq, 1);

        let mut sized = record(EventJournalLimits::new(8, 128 * 1024));
        sized.commit(11, changed("same", 11)).unwrap();
        let one_event = sized.journal.front().unwrap().bytes;
        let mut byte_limited = record(EventJournalLimits::new(8, one_event));
        byte_limited.commit(11, changed("same", 11)).unwrap();
        byte_limited.commit(12, changed("next", 12)).unwrap();
        assert_eq!(byte_limited.replay_floor.seq, 1);
        assert_eq!(byte_limited.journal.len(), 1);

        let mut retired = record(EventJournalLimits::new(8, 128 * 1024));
        retired.commit(10, changed("earlier", 10)).unwrap();
        retired.commit(11, changed("old", 11)).unwrap();
        retired.commit(12, changed("new", 12)).unwrap();
        retired.run_identities.insert("old".into(), 7);
        assert!(retired.retire_run("old", 7));
        assert_eq!(retired.replay_floor.seq, 2);
        assert_eq!(retired.journal.front().unwrap().envelope.cursor.seq, 3);
        assert!(!retired.retire_run("old", 8));
    }

    #[test]
    fn replay_freezes_through_and_reports_stream_reset_and_future_cursor() {
        let mut record = record(EventJournalLimits::new(16, 128 * 1024));
        for (index, turn) in ["one", "two", "three"].into_iter().enumerate() {
            record
                .commit(11 + index as u64, changed(turn, 11 + index as u64))
                .unwrap();
        }
        let zero = SessionCursor {
            thread_id: "session".into(),
            stream_id: "stream".into(),
            seq: 0,
        };
        let first = record
            .replay(&SubscribeSessionParams {
                thread_id: "session".into(),
                after: zero.clone(),
                through: None,
                limit: 1,
            })
            .unwrap();
        assert_eq!(first.through.seq, 3);
        assert!(first.has_more);
        record.commit(20, changed("four", 20)).unwrap();
        let second = record
            .replay(&SubscribeSessionParams {
                thread_id: "session".into(),
                after: first.resume_after,
                through: Some(first.through),
                limit: 8,
            })
            .unwrap();
        assert_eq!(
            second
                .events
                .iter()
                .map(|event| event.cursor.seq)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert!(!second.has_more);

        let reset = record
            .replay(&SubscribeSessionParams {
                thread_id: "session".into(),
                after: SessionCursor {
                    stream_id: "old".into(),
                    ..zero.clone()
                },
                through: None,
                limit: 1,
            })
            .unwrap();
        assert_eq!(reset.through, record.snapshot.cursor);
        assert_eq!(reset.gap.unwrap().reason, ReplayGapReason::StreamReset);

        let reset_with_old_window = record
            .replay(&SubscribeSessionParams {
                thread_id: "session".into(),
                after: SessionCursor {
                    stream_id: "old".into(),
                    ..zero.clone()
                },
                through: Some(SessionCursor {
                    thread_id: "session".into(),
                    stream_id: "old".into(),
                    seq: 2,
                }),
                limit: 1,
            })
            .unwrap();
        assert_eq!(reset_with_old_window.through, record.snapshot.cursor);
        assert_eq!(
            reset_with_old_window.gap.unwrap().reason,
            ReplayGapReason::StreamReset
        );

        assert!(record
            .replay(&SubscribeSessionParams {
                thread_id: "session".into(),
                after: SessionCursor { seq: 99, ..zero },
                through: None,
                limit: 1,
            })
            .unwrap_err()
            .contains("ahead"));
    }

    #[test]
    fn bounded_get_keeps_tail_indexes_and_terminal_keeps_lightweight_summary() {
        let history = vec![
            CanonicalItem::user_text("one"),
            CanonicalItem::user_text("two"),
            CanonicalItem::user_text("three"),
        ];
        let mut record = SessionViewRecord::new(
            SessionSummary::new("session", 10),
            SessionHistoryWindow::from_history(&history, 8).unwrap(),
            "stream".into(),
            EventJournalLimits::new(8, 128 * 1024),
        )
        .unwrap();
        let bounded = record.snapshot_with_history_limit(2);
        assert_eq!(bounded.history.items, history[1..]);
        assert_eq!(bounded.history.start_index, 1);
        assert_eq!(bounded.history.total_items, 3);
        assert_eq!(bounded.history.capacity, 2);

        record.commit(11, changed("run", 11)).unwrap();
        let mut terminal = snapshot("session", "run");
        terminal.status = RunStatus::Completed;
        terminal.last_seq = 1;
        terminal.result = Some(whale_protocol::RunTurnResult {
            thread_id: "session".into(),
            turn_id: "run".into(),
            status: whale_protocol::TurnStatus::Completed,
            items: vec![],
            usage: Default::default(),
        });
        record
            .commit(
                12,
                SessionEventPayload::RunEvent {
                    event: RunEvent {
                        thread_id: "session".into(),
                        turn_id: "run".into(),
                        seq: 1,
                        payload: RunEventPayload::Finished { snapshot: terminal },
                    },
                },
            )
            .unwrap();
        assert!(record.snapshot.active_run.is_none());
        assert_eq!(record.snapshot.last_run.unwrap().turn_id, "run");
    }

    struct BlockFirstNotification {
        attempts: std::sync::atomic::AtomicUsize,
        entered: tokio::sync::Semaphore,
        release: tokio::sync::Semaphore,
        output: tokio::sync::mpsc::UnboundedSender<u64>,
    }

    #[async_trait::async_trait]
    impl OutgoingTransport for BlockFirstNotification {
        async fn send_line(&self, line: &str) -> std::io::Result<()> {
            if self
                .attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                == 0
            {
                self.entered.add_permits(1);
                self.release.acquire().await.unwrap().forget();
            }
            let value: serde_json::Value = serde_json::from_str(line).unwrap();
            self.output
                .send(value["params"]["cursor"]["seq"].as_u64().unwrap())
                .map_err(|_| std::io::Error::other("capture closed"))
        }
    }

    #[tokio::test]
    async fn bounded_notifier_lag_preserves_monotonic_delivery_and_the_latest_cursor() {
        let configured = SessionViewRegistry::with_limits(EventJournalLimits::new(3, 4096));
        assert_eq!(configured.limits.max_events, 3);

        let (output, mut received) = tokio::sync::mpsc::unbounded_channel();
        let transport = Arc::new(BlockFirstNotification {
            attempts: std::sync::atomic::AtomicUsize::new(0),
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            output,
        });
        let notifier = spawn_notifier(AnyTransportWriter::new(transport.clone()));
        let mut record = record(EventJournalLimits::new(
            DEFAULT_NOTIFICATION_QUEUE + 16,
            2 * 1024 * 1024,
        ));
        let first = record.commit(11, changed("run-1", 11)).unwrap();
        notifier.publish(first);
        transport.entered.acquire().await.unwrap().forget();

        let target = DEFAULT_NOTIFICATION_QUEUE as u64 + 10;
        for seq in 2..=target {
            let envelope = record
                .commit(10 + seq, changed(&format!("run-{seq}"), 10 + seq))
                .unwrap();
            notifier.publish(envelope);
        }
        transport.release.add_permits(1);

        let delivered = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let mut cursors = Vec::new();
            loop {
                let cursor = received.recv().await.unwrap();
                cursors.push(cursor);
                if cursor == target {
                    break cursors;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(delivered[0], 1);
        assert_eq!(delivered.last().copied(), Some(target));
        assert!(delivered.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(
            delivered.windows(2).any(|pair| pair[1] > pair[0] + 1),
            "bounded lag must be visible as a cursor gap"
        );
        drop(notifier);
    }
}
