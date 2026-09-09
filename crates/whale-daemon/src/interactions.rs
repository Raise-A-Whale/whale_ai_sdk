//! Session-scoped Interaction projection, replay, and response transaction.

use async_trait::async_trait;
use dashmap::{mapref::entry::Entry as DashEntry, DashMap};
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex as StdMutex, OnceLock, Weak},
};
use tokio::sync::{broadcast, oneshot, watch};
use whale_core::{
    interaction::{
        validate_interaction_request, validate_interaction_response, InteractionBeginGuard,
        InteractionBridge, InteractionTicket,
    },
    CoreError,
};
use whale_protocol::{
    contexts::RunContextInfo,
    interactions::{
        GetSessionInteractionsParams, GetTurnInteractionsParams, InteractionCursor,
        InteractionEventEnvelope, InteractionEventPayload, InteractionReplayGap,
        InteractionReplayGapReason, InteractionRequest, InteractionSnapshot, PendingInteraction,
        RespondInteractionParams, SubscribeInteractionsParams, SubscribeInteractionsResult,
        TurnInteractionSnapshot, INTERACTION_CONFLICT, INTERACTION_NOT_FOUND,
        INTERACTION_REMOVAL_ORIGIN_FINISHED, INTERACTION_REMOVAL_PUBLICATION_FAILED,
        INTERACTION_REMOVAL_RESOLVED, INTERACTION_REMOVAL_SESSION_CLOSED,
        INTERACTION_RESPONSE_INVALID, INTERACTION_UNAVAILABLE, KIND_TOOL_APPROVAL,
        MAX_INTERACTION_JOURNAL_BYTES, MAX_INTERACTION_JOURNAL_EVENTS,
        MAX_PENDING_INTERACTIONS_PER_RUN, METHOD_SESSION_INTERACTION_EVENT,
    },
    rpc::{JSONRPCError, JSONRPCNotification, JSONRPCResponse, RequestId},
};

use super::{DaemonServer, RunPublicationMutation, RunRecord};
use crate::transport::{AnyTransportWriter, OutgoingTransport};

const DEFAULT_NOTIFICATION_QUEUE: usize = 128;

#[derive(Debug, Clone, Copy)]
pub(super) struct InteractionJournalLimits {
    max_events: usize,
    max_bytes: usize,
}

impl InteractionJournalLimits {
    #[cfg(test)]
    fn new(max_events: usize, max_bytes: usize) -> Self {
        assert!(max_events > 0);
        assert!(max_bytes > 0);
        Self {
            max_events,
            max_bytes,
        }
    }
}

impl Default for InteractionJournalLimits {
    fn default() -> Self {
        Self {
            max_events: MAX_INTERACTION_JOURNAL_EVENTS,
            max_bytes: MAX_INTERACTION_JOURNAL_BYTES,
        }
    }
}

#[derive(Clone)]
struct JournalEntry {
    envelope: InteractionEventEnvelope,
    bytes: usize,
    turn_id: String,
}

struct InteractionNotifier {
    events: Option<broadcast::Sender<InteractionEventEnvelope>>,
    cancel: Option<watch::Sender<bool>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl InteractionNotifier {
    fn publish(&self, envelope: InteractionEventEnvelope) {
        if let Some(events) = &self.events {
            let _ = events.send(envelope);
        }
    }

    fn close(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            cancel.send_replace(true);
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
        self.events.take();
    }

    fn drain_queued(mut self) {
        // Closing the only producer makes the worker exit after it has sent the
        // retained queue. Keep cancellation alive until that completion ACK,
        // but cap the detached drain so a broken transport cannot leak a task.
        self.events.take();
        let cancel = self.cancel.take();
        let Some(mut task) = self.task.take() else {
            return;
        };
        tokio::spawn(async move {
            tokio::select! {
                _ = &mut task => {}
                _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => task.abort(),
            }
            drop(cancel);
        });
    }
}

impl Drop for InteractionNotifier {
    fn drop(&mut self) {
        self.close();
    }
}

#[derive(Clone, PartialEq, Eq)]
enum InteractionOrigin {
    Core,
    HostCall(String),
}

struct PendingEntry {
    request: InteractionRequest,
    sender: oneshot::Sender<Result<InteractionDelivery, CoreError>>,
    generation: u64,
    origin: InteractionOrigin,
    typed_approval: bool,
}

struct InteractionDelivery {
    response: Value,
    committed: oneshot::Receiver<Result<(), CoreError>>,
}

enum ResponseState {
    Pending(PendingEntry),
    Resolved {
        digest: [u8; 32],
        generation: u64,
        typed_approval: bool,
        completion: watch::Sender<Option<Result<(), InteractionFailure>>>,
    },
}

struct InteractionRecord {
    owner: String,
    enabled: bool,
    snapshot: InteractionSnapshot,
    journal: VecDeque<JournalEntry>,
    journal_bytes: usize,
    replay_floor: InteractionCursor,
    limits: InteractionJournalLimits,
    notifier: Option<InteractionNotifier>,
    entries: HashMap<(String, String), ResponseState>,
    run_identities: HashMap<String, usize>,
    next_generation: u64,
}

impl InteractionRecord {
    fn new(
        owner: String,
        thread_id: String,
        enabled: bool,
        limits: InteractionJournalLimits,
        transport: AnyTransportWriter,
    ) -> Result<Self, String> {
        let cursor = InteractionCursor {
            thread_id: thread_id.clone(),
            stream_id: uuid::Uuid::new_v4().to_string(),
            seq: 0,
        };
        let snapshot = InteractionSnapshot::new(thread_id, cursor.clone(), Vec::new())?;
        Ok(Self {
            owner,
            enabled,
            snapshot,
            journal: VecDeque::new(),
            journal_bytes: 0,
            replay_floor: cursor,
            limits,
            notifier: enabled.then(|| spawn_notifier(transport)),
            entries: HashMap::new(),
            run_identities: HashMap::new(),
            next_generation: 1,
        })
    }

    fn commit(
        &mut self,
        payload: InteractionEventPayload,
    ) -> Result<InteractionEventEnvelope, String> {
        let cursor = self.snapshot.cursor.checked_next()?;
        let envelope = InteractionEventEnvelope::new(
            self.snapshot.thread_id.clone(),
            cursor,
            super::session_views::unix_ms(),
            payload,
        );
        envelope.validate()?;
        let bytes = serde_json::to_vec(&envelope)
            .map_err(|error| format!("Interaction event serialization failed: {error}"))?
            .len();
        self.snapshot
            .apply(&envelope)
            .map_err(|error| error.to_string())?;
        let turn_id = event_turn_id(&envelope).to_owned();
        retain(
            &mut self.journal,
            &mut self.journal_bytes,
            &mut self.replay_floor,
            self.limits,
            envelope.clone(),
            bytes,
            turn_id,
        );
        if let Some(notifier) = &self.notifier {
            // Enqueue under the same short state lock that assigned the cursor.
            // The notifier performs transport I/O later, so producers never
            // wait on a slow connection and cursor order cannot be inverted.
            notifier.publish(envelope.clone());
        }
        Ok(envelope)
    }

    fn replay(
        &self,
        params: &SubscribeInteractionsParams,
    ) -> Result<SubscribeInteractionsResult, String> {
        params.validate()?;
        let current = self.snapshot.cursor.clone();
        let through = params.through.clone().unwrap_or_else(|| current.clone());
        if params.after.stream_id != current.stream_id {
            return Ok(self.gap(params, current, InteractionReplayGapReason::StreamReset));
        }
        if params.after.seq > current.seq {
            return Err("Interaction replay cursor is ahead of the current cursor".into());
        }
        if let Some(requested_through) = &params.through {
            if requested_through.stream_id != current.stream_id {
                return Ok(self.gap(params, current, InteractionReplayGapReason::StreamReset));
            }
            if requested_through.seq > current.seq {
                return Err("Interaction replay high watermark is ahead of current".into());
            }
        }
        if params.after.seq < self.replay_floor.seq {
            return Ok(self.gap(params, through, InteractionReplayGapReason::Retention));
        }
        let events: Vec<_> = self
            .journal
            .iter()
            .filter(|entry| {
                entry.envelope.cursor.seq > params.after.seq
                    && entry.envelope.cursor.seq <= through.seq
            })
            .take(params.limit as usize)
            .map(|entry| entry.envelope.clone())
            .collect();
        let resume_after = events
            .last()
            .map(|event| event.cursor.clone())
            .unwrap_or_else(|| params.after.clone());
        Ok(SubscribeInteractionsResult {
            has_more: resume_after.seq < through.seq,
            events,
            resume_after,
            through,
            gap: None,
        })
    }

    fn gap(
        &self,
        params: &SubscribeInteractionsParams,
        through: InteractionCursor,
        reason: InteractionReplayGapReason,
    ) -> SubscribeInteractionsResult {
        SubscribeInteractionsResult {
            events: Vec::new(),
            resume_after: params.after.clone(),
            through,
            has_more: false,
            gap: Some(InteractionReplayGap {
                reason,
                requested: params.after.clone(),
                replay_floor: self.replay_floor.clone(),
                current: self.snapshot.cursor.clone(),
            }),
        }
    }
}

#[derive(Clone)]
pub(super) struct InteractionRegistry {
    records: Arc<DashMap<String, Arc<StdMutex<InteractionRecord>>>>,
    limits: InteractionJournalLimits,
    digest_key: Arc<[u8; 32]>,
}

impl Default for InteractionRegistry {
    fn default() -> Self {
        let mut key = [0_u8; 32];
        getrandom::fill(&mut key).expect("operating-system Interaction key generation failed");
        Self {
            records: Arc::new(DashMap::new()),
            limits: InteractionJournalLimits::default(),
            digest_key: Arc::new(key),
        }
    }
}

impl InteractionRegistry {
    #[cfg(test)]
    fn with_limits(limits: InteractionJournalLimits) -> Self {
        Self {
            records: Arc::new(DashMap::new()),
            limits,
            digest_key: Arc::new([7; 32]),
        }
    }

    pub(super) fn insert(
        &self,
        owner: &str,
        thread_id: String,
        enabled: bool,
        transport: AnyTransportWriter,
    ) -> Result<(), String> {
        let record = InteractionRecord::new(
            owner.to_owned(),
            thread_id.clone(),
            enabled,
            self.limits,
            transport,
        )?;
        match self.records.entry(thread_id) {
            DashEntry::Vacant(entry) => {
                entry.insert(Arc::new(StdMutex::new(record)));
                Ok(())
            }
            DashEntry::Occupied(_) => Err("SessionAlreadyExists".into()),
        }
    }

    fn record(&self, thread_id: &str) -> Result<Arc<StdMutex<InteractionRecord>>, String> {
        self.records
            .get(thread_id)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| "SessionNotFound".to_string())
    }

    fn owned(
        &self,
        owner: &str,
        thread_id: &str,
    ) -> Result<Arc<StdMutex<InteractionRecord>>, String> {
        let record = self.record(thread_id)?;
        if record.lock().unwrap().owner != owner {
            return Err("SessionNotFound".into());
        }
        Ok(record)
    }

    pub(super) fn enabled(&self, owner: &str, thread_id: &str) -> Result<bool, String> {
        let record = self.owned(owner, thread_id)?;
        let enabled = record.lock().unwrap().enabled;
        Ok(enabled)
    }

    pub(super) fn get(
        &self,
        owner: &str,
        params: &GetSessionInteractionsParams,
    ) -> Result<InteractionSnapshot, InteractionFailure> {
        params.validate().map_err(InteractionFailure::invalid)?;
        let record = self
            .owned(owner, &params.thread_id)
            .map_err(InteractionFailure::unavailable)?;
        let record = record.lock().unwrap();
        if !record.enabled {
            return Err(InteractionFailure::unavailable(
                "Interactions are disabled for this Session",
            ));
        }
        Ok(record.snapshot.clone())
    }

    pub(super) fn get_turn(
        &self,
        owner: &str,
        params: &GetTurnInteractionsParams,
    ) -> Result<TurnInteractionSnapshot, InteractionFailure> {
        params.validate().map_err(InteractionFailure::invalid)?;
        let record = self
            .owned(owner, &params.thread_id)
            .map_err(InteractionFailure::unavailable)?;
        let record = record.lock().unwrap();
        if !record.enabled {
            return Err(InteractionFailure::unavailable(
                "Interactions are disabled for this Session",
            ));
        }
        let pending = record
            .snapshot
            .pending
            .iter()
            .filter(|pending| pending.turn_id == params.turn_id)
            .cloned()
            .collect();
        TurnInteractionSnapshot::new(
            params.thread_id.clone(),
            params.turn_id.clone(),
            record.snapshot.cursor.clone(),
            pending,
        )
        .map_err(InteractionFailure::invalid)
    }

    pub(super) fn subscribe(
        &self,
        owner: &str,
        params: &SubscribeInteractionsParams,
    ) -> Result<SubscribeInteractionsResult, InteractionFailure> {
        let record = self
            .owned(owner, &params.thread_id)
            .map_err(InteractionFailure::unavailable)?;
        let record = record.lock().unwrap();
        if !record.enabled {
            return Err(InteractionFailure::unavailable(
                "Interactions are disabled for this Session",
            ));
        }
        record.replay(params).map_err(InteractionFailure::invalid)
    }

    pub(super) fn register_run(
        &self,
        owner: &str,
        thread_id: &str,
        turn_id: &str,
        identity: usize,
    ) -> Result<(), String> {
        let record = self.owned(owner, thread_id)?;
        let mut record = record.lock().unwrap();
        match record.run_identities.get(turn_id) {
            Some(current) if *current == identity => Ok(()),
            Some(_) => Err("RunConflict".into()),
            None => {
                record.run_identities.insert(turn_id.to_owned(), identity);
                Ok(())
            }
        }
    }

    pub(super) fn unregister_run(
        &self,
        owner: &str,
        thread_id: &str,
        turn_id: &str,
        identity: usize,
    ) {
        let Ok(record) = self.owned(owner, thread_id) else {
            return;
        };
        let mut record = record.lock().unwrap();
        if record.run_identities.get(turn_id).copied() == Some(identity) {
            record.run_identities.remove(turn_id);
        }
    }

    fn begin(
        &self,
        context: &RunContextInfo,
        request_id: String,
        request: InteractionRequest,
        origin: InteractionOrigin,
    ) -> Result<InteractionTicket, CoreError> {
        validate_interaction_request(&request)?;
        let record = self
            .record(&context.thread_id)
            .map_err(CoreError::InteractionUnavailable)?;
        let (sender, receiver) = oneshot::channel();
        let generation = {
            let mut record = record.lock().unwrap();
            if !record.enabled && request.kind != KIND_TOOL_APPROVAL {
                return Err(CoreError::InteractionUnavailable(
                    "Interactions are disabled for this Session".into(),
                ));
            }
            if !record.run_identities.contains_key(&context.turn_id) {
                return Err(CoreError::InteractionUnavailable(
                    "Interaction Run is not active".into(),
                ));
            }
            if record
                .entries
                .keys()
                .any(|(_, existing)| existing == &request_id)
            {
                return Err(CoreError::InteractionUnavailable(
                    "Duplicate Interaction request_id".into(),
                ));
            }
            let pending_for_run = record
                .entries
                .iter()
                .filter(|((turn, _), state)| {
                    turn == &context.turn_id && matches!(state, ResponseState::Pending(_))
                })
                .count();
            if pending_for_run >= MAX_PENDING_INTERACTIONS_PER_RUN {
                return Err(CoreError::InteractionUnavailable(format!(
                    "A Run cannot contain more than {MAX_PENDING_INTERACTIONS_PER_RUN} pending Interactions"
                )));
            }
            let generation = record.next_generation;
            record.next_generation = record.next_generation.checked_add(1).ok_or_else(|| {
                CoreError::InteractionUnavailable("Interaction identity space is exhausted".into())
            })?;
            let typed_approval = request.kind == KIND_TOOL_APPROVAL;
            let pending = PendingInteraction::new(
                request_id.clone(),
                context.turn_id.clone(),
                request.clone(),
            )
            .map_err(CoreError::InteractionRequestInvalid)?;
            if record.enabled {
                record
                    .commit(InteractionEventPayload::Requested {
                        interaction: pending,
                    })
                    .map_err(CoreError::InteractionUnavailable)?;
            }
            record.entries.insert(
                (context.turn_id.clone(), request_id.clone()),
                ResponseState::Pending(PendingEntry {
                    request,
                    sender,
                    generation,
                    origin,
                    typed_approval,
                }),
            );
            generation
        };
        let registry = self.clone();
        let thread_id = context.thread_id.clone();
        let turn_id = context.turn_id.clone();
        let cleanup_id = request_id.clone();
        let guard = InteractionBeginGuard::new(request_id, move || {
            registry.clear_exact(
                &thread_id,
                &turn_id,
                &cleanup_id,
                generation,
                INTERACTION_REMOVAL_ORIGIN_FINISHED,
            );
        });
        Ok(guard.complete(async move {
            let delivery = receiver.await.unwrap_or_else(|_| {
                Err(CoreError::InteractionCancelled(
                    "Interaction continuation closed".into(),
                ))
            })?;
            delivery.committed.await.unwrap_or_else(|_| {
                Err(CoreError::InteractionUnavailable(
                    "Interaction response commit closed".into(),
                ))
            })?;
            Ok(delivery.response)
        }))
    }

    pub(super) fn begin_host(
        &self,
        context: &RunContextInfo,
        host_call_id: String,
        request_id: String,
        request: InteractionRequest,
    ) -> Result<InteractionTicket, CoreError> {
        self.begin(
            context,
            request_id,
            request,
            InteractionOrigin::HostCall(host_call_id),
        )
    }

    fn commit_response(
        &self,
        owner: Option<&str>,
        params: &RespondInteractionParams,
        require_enabled: bool,
    ) -> Result<ResponseCommit, InteractionFailure> {
        params
            .validate()
            .map_err(InteractionFailure::response_invalid)?;
        let digest = response_digest(&self.digest_key, &params.response)
            .map_err(InteractionFailure::response_invalid)?;
        let record = match owner {
            Some(owner) => self
                .owned(owner, &params.thread_id)
                .map_err(InteractionFailure::unavailable)?,
            None => self
                .record(&params.thread_id)
                .map_err(InteractionFailure::not_found)?,
        };
        let key = (params.turn_id.clone(), params.request_id.clone());

        let (request, generation) = {
            let record = record.lock().unwrap();
            if require_enabled && !record.enabled {
                return Err(InteractionFailure::unavailable(
                    "Interactions are disabled for this Session",
                ));
            }
            match record.entries.get(&key) {
                Some(ResponseState::Resolved {
                    digest: previous,
                    typed_approval,
                    completion,
                    ..
                }) => {
                    return if constant_time_eq(previous, &digest) {
                        Ok(ResponseCommit::idempotent(
                            self.clone(),
                            params,
                            *typed_approval,
                            completion.subscribe(),
                        ))
                    } else {
                        Err(InteractionFailure::conflict())
                    };
                }
                Some(ResponseState::Pending(entry)) => (entry.request.clone(), entry.generation),
                None => return Err(InteractionFailure::not_found("InteractionNotFound")),
            }
        };

        validate_interaction_response(&request, &params.response).map_err(|error| {
            InteractionFailure::response_invalid(match error {
                CoreError::InteractionResponseInvalid(message)
                | CoreError::InteractionRequestInvalid(message) => message,
                other => other.to_string(),
            })
        })?;

        let (sender, typed_approval, completion) = {
            let mut record = record.lock().unwrap();
            if require_enabled && !record.enabled {
                return Err(InteractionFailure::unavailable(
                    "Interactions are disabled for this Session",
                ));
            }
            match record.entries.get(&key) {
                Some(ResponseState::Resolved {
                    digest: previous,
                    typed_approval,
                    completion,
                    ..
                }) => {
                    return if constant_time_eq(previous, &digest) {
                        Ok(ResponseCommit::idempotent(
                            self.clone(),
                            params,
                            *typed_approval,
                            completion.subscribe(),
                        ))
                    } else {
                        Err(InteractionFailure::conflict())
                    };
                }
                Some(ResponseState::Pending(entry)) if entry.generation == generation => {
                    if entry.sender.is_closed() {
                        let _ = record.entries.remove(&key);
                        if record.enabled {
                            record
                                .commit(InteractionEventPayload::Removed {
                                    request_id: params.request_id.clone(),
                                    turn_id: params.turn_id.clone(),
                                    cause: INTERACTION_REMOVAL_ORIGIN_FINISHED.into(),
                                })
                                .map_err(InteractionFailure::unavailable)?;
                        }
                        return Err(InteractionFailure::not_found(
                            "Interaction continuation is no longer active",
                        ));
                    }
                }
                Some(ResponseState::Pending(_)) | None => {
                    return Err(InteractionFailure::not_found("InteractionNotFound"));
                }
            }
            let state = record
                .entries
                .remove(&key)
                .expect("validated pending Interaction");
            let ResponseState::Pending(entry) = state else {
                unreachable!("validated pending Interaction")
            };
            let typed_approval = entry.typed_approval;
            let sender = entry.sender;
            let (completion, _) = watch::channel(None);
            record.entries.insert(
                key.clone(),
                ResponseState::Resolved {
                    digest,
                    generation,
                    typed_approval,
                    completion: completion.clone(),
                },
            );
            (sender, typed_approval, completion)
        };
        Ok(ResponseCommit {
            registry: self.clone(),
            thread_id: params.thread_id.clone(),
            turn_id: params.turn_id.clone(),
            request_id: params.request_id.clone(),
            digest,
            generation,
            sender: Some(sender),
            response: Some(params.response.clone()),
            delivery_commit: None,
            typed_approval,
            newly_resolved: true,
            completion: Some(ResponseCompletion::Leader(completion)),
        })
    }

    pub(super) fn respond_owned(
        &self,
        owner: &str,
        params: &RespondInteractionParams,
        require_enabled: bool,
    ) -> Result<ResponseCommit, InteractionFailure> {
        self.commit_response(Some(owner), params, require_enabled)
    }

    fn commit_core_response(
        &self,
        context: &RunContextInfo,
        request_id: &str,
        response: Value,
    ) -> Result<ResponseCommit, CoreError> {
        let params = RespondInteractionParams {
            thread_id: context.thread_id.clone(),
            turn_id: context.turn_id.clone(),
            request_id: request_id.to_owned(),
            response,
        };
        self.commit_response(None, &params, false)
            .map_err(InteractionFailure::into_core)
    }

    pub(super) fn find_request(&self, owner: &str, request_id: &str) -> Option<(String, String)> {
        for entry in self.records.iter() {
            let thread_id = entry.key().clone();
            let record = entry.value().lock().unwrap();
            if record.owner != owner {
                continue;
            }
            if let Some((turn_id, _)) = record
                .entries
                .keys()
                .find(|(_, current)| current == request_id)
            {
                return Some((thread_id, turn_id.clone()));
            }
        }
        None
    }

    pub(super) fn clear_run(&self, owner: &str, thread_id: &str, turn_id: &str, cause: &str) {
        let Ok(record) = self.owned(owner, thread_id) else {
            return;
        };
        let senders = {
            let mut record = record.lock().unwrap();
            let keys: Vec<_> = record
                .entries
                .iter()
                .filter(|(key, state)| {
                    key.0 == turn_id && matches!(state, ResponseState::Pending(_))
                })
                .map(|(key, _)| key.clone())
                .collect();
            let mut senders = Vec::new();
            for key in keys {
                let Some(state) = record.entries.remove(&key) else {
                    continue;
                };
                if let ResponseState::Pending(entry) = state {
                    if record.enabled {
                        let _ = record.commit(InteractionEventPayload::Removed {
                            request_id: key.1,
                            turn_id: key.0,
                            cause: cause.to_owned(),
                        });
                    }
                    senders.push(entry.sender);
                }
            }
            senders
        };
        for sender in senders {
            let _ = sender.send(Err(CoreError::InteractionCancelled(cause.to_owned())));
        }
    }

    pub(super) fn clear_origin(
        &self,
        owner: &str,
        thread_id: &str,
        turn_id: &str,
        host_call_id: &str,
        cause: &str,
    ) {
        let Ok(record) = self.owned(owner, thread_id) else {
            return;
        };
        let identities: Vec<_> = {
            let record = record.lock().unwrap();
            record
                .entries
                .iter()
                .filter_map(|((turn, request), state)| match state {
                    ResponseState::Pending(entry)
                        if turn == turn_id
                            && entry.origin == InteractionOrigin::HostCall(host_call_id.into()) =>
                    {
                        Some((turn.clone(), request.clone(), entry.generation))
                    }
                    _ => None,
                })
                .collect()
        };
        for (turn, request, generation) in identities {
            self.clear_exact(thread_id, &turn, &request, generation, cause);
        }
    }

    fn clear_core_request(&self, context: &RunContextInfo, request_id: &str, cause: &str) {
        let Ok(record) = self.record(&context.thread_id) else {
            return;
        };
        let generation = {
            let record = record.lock().unwrap();
            match record
                .entries
                .get(&(context.turn_id.clone(), request_id.to_owned()))
            {
                Some(ResponseState::Pending(entry)) if entry.origin == InteractionOrigin::Core => {
                    Some(entry.generation)
                }
                _ => None,
            }
        };
        if let Some(generation) = generation {
            self.clear_exact(
                &context.thread_id,
                &context.turn_id,
                request_id,
                generation,
                cause,
            );
        }
    }

    fn clear_exact(
        &self,
        thread_id: &str,
        turn_id: &str,
        request_id: &str,
        generation: u64,
        cause: &str,
    ) {
        let Ok(record) = self.record(thread_id) else {
            return;
        };
        let sender = {
            let mut record = record.lock().unwrap();
            let key = (turn_id.to_owned(), request_id.to_owned());
            let matches = matches!(
                record.entries.get(&key),
                Some(ResponseState::Pending(entry)) if entry.generation == generation
            );
            if !matches {
                return;
            }
            let Some(ResponseState::Pending(entry)) = record.entries.remove(&key) else {
                unreachable!("checked pending identity")
            };
            if record.enabled {
                let _ = record.commit(InteractionEventPayload::Removed {
                    request_id: request_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    cause: cause.to_owned(),
                });
            }
            entry.sender
        };
        let _ = sender.send(Err(CoreError::InteractionCancelled(cause.to_owned())));
    }

    fn publish_resolved_exact(
        &self,
        thread_id: &str,
        turn_id: &str,
        request_id: &str,
        generation: u64,
        digest: &[u8; 32],
    ) -> Result<(), InteractionFailure> {
        let record = self
            .record(thread_id)
            .map_err(InteractionFailure::not_found)?;
        let mut record = record.lock().unwrap();
        let key = (turn_id.to_owned(), request_id.to_owned());
        let matches = matches!(
            record.entries.get(&key),
            Some(ResponseState::Resolved { digest: saved, generation: current, .. })
                if *current == generation && constant_time_eq(saved, digest)
        );
        if !matches {
            return Err(InteractionFailure::not_found(
                "Interaction response transaction is no longer active",
            ));
        }
        if record.enabled {
            record
                .commit(InteractionEventPayload::Removed {
                    request_id: request_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    cause: INTERACTION_REMOVAL_RESOLVED.into(),
                })
                .map_err(InteractionFailure::unavailable)?;
        }
        Ok(())
    }

    fn abort_resolved_exact(
        &self,
        thread_id: &str,
        turn_id: &str,
        request_id: &str,
        generation: u64,
        digest: &[u8; 32],
        cause: &str,
    ) {
        let Ok(record) = self.record(thread_id) else {
            return;
        };
        let mut record = record.lock().unwrap();
        let key = (turn_id.to_owned(), request_id.to_owned());
        let matches = matches!(
            record.entries.get(&key),
            Some(ResponseState::Resolved { digest: saved, generation: current, .. })
                if *current == generation && constant_time_eq(saved, digest)
        );
        if !matches {
            return;
        }
        if record.enabled
            && record
                .commit(InteractionEventPayload::Removed {
                    request_id: request_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    cause: cause.to_owned(),
                })
                .is_err()
        {
            // Cursor exhaustion or an invalid local projection must not leave
            // a failed transaction visible as pending even when no final
            // envelope can be committed.
            record
                .snapshot
                .pending
                .retain(|pending| pending.turn_id != turn_id || pending.request_id != request_id);
        }
        record.entries.remove(&key);
    }

    pub(super) fn retire_run(
        &self,
        owner: &str,
        thread_id: &str,
        turn_id: &str,
        identity: usize,
    ) -> bool {
        let Ok(record) = self.owned(owner, thread_id) else {
            return false;
        };
        let mut record = record.lock().unwrap();
        match record.run_identities.get(turn_id).copied() {
            Some(current) if current != identity => return false,
            None => {
                return !record.entries.keys().any(|(turn, _)| turn == turn_id)
                    && !record.journal.iter().any(|entry| entry.turn_id == turn_id);
            }
            Some(_) => {}
        }
        if record
            .entries
            .iter()
            .any(|((turn, _), state)| turn == turn_id && matches!(state, ResponseState::Pending(_)))
        {
            return false;
        }
        let InteractionRecord {
            journal,
            journal_bytes,
            replay_floor,
            ..
        } = &mut *record;
        scrub_run(journal, journal_bytes, replay_floor, turn_id);
        record.entries.retain(|(turn, _), _| turn != turn_id);
        record.run_identities.remove(turn_id);
        true
    }

    pub(super) fn close_session(&self, owner: &str, thread_id: &str, cause: &str) {
        let Ok(record) = self.owned(owner, thread_id) else {
            return;
        };
        let turns: Vec<_> = record
            .lock()
            .unwrap()
            .entries
            .keys()
            .map(|(turn, _)| turn.clone())
            .collect();
        for turn in turns {
            self.clear_run(owner, thread_id, &turn, cause);
        }
        let Some((_, record)) = self
            .records
            .remove_if(thread_id, |_, current| Arc::ptr_eq(current, &record))
        else {
            return;
        };
        let notifier = record.lock().unwrap().notifier.take();
        // A normal close keeps the final session_closed removal observable and
        // waits in a detached, bounded drain. Connection teardown cannot use
        // the writer again, so its emergency Drop cancels immediately.
        drop(record);
        if cause == INTERACTION_REMOVAL_SESSION_CLOSED {
            if let Some(notifier) = notifier {
                notifier.drain_queued();
            }
        }
    }
}

impl DaemonServer {
    pub(super) async fn handle_interaction_read(
        &self,
        id: RequestId,
        method: &str,
        params: Option<Value>,
        transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let value = params.unwrap_or(Value::Null);
        match method {
            whale_protocol::interactions::METHOD_SESSION_INTERACTIONS_GET => {
                let params: GetSessionInteractionsParams = match serde_json::from_value(value) {
                    Ok(params) => params,
                    Err(error) => {
                        return JSONRPCResponse::error(
                            id,
                            JSONRPCError::invalid_params(error.to_string()),
                        )
                    }
                };
                if let Err(error) = self
                    .lifecycle
                    .guard_open(&params.thread_id, transport.connection_id())
                {
                    return JSONRPCResponse::error(
                        id,
                        InteractionFailure::unavailable(error).rpc(),
                    );
                }
                match self.interactions.get(transport.connection_id(), &params) {
                    Ok(snapshot) => JSONRPCResponse::success(id, snapshot)
                        .expect("Interaction snapshot serializes"),
                    Err(error) => JSONRPCResponse::error(id, error.rpc()),
                }
            }
            whale_protocol::interactions::METHOD_TURN_INTERACTIONS_GET => {
                let params: GetTurnInteractionsParams = match serde_json::from_value(value) {
                    Ok(params) => params,
                    Err(error) => {
                        return JSONRPCResponse::error(
                            id,
                            JSONRPCError::invalid_params(error.to_string()),
                        )
                    }
                };
                let reference = whale_protocol::runs::RunRefParams {
                    thread_id: params.thread_id.clone(),
                    turn_id: params.turn_id.clone(),
                };
                if let Err(error) = self
                    .lifecycle
                    .guard_open(&params.thread_id, transport.connection_id())
                {
                    return JSONRPCResponse::error(
                        id,
                        InteractionFailure::unavailable(error).rpc(),
                    );
                }
                if let Err(error) = self.find_run(&reference, transport).await {
                    return JSONRPCResponse::error(id, error);
                }
                match self
                    .interactions
                    .get_turn(transport.connection_id(), &params)
                {
                    Ok(snapshot) => JSONRPCResponse::success(id, snapshot)
                        .expect("Run Interaction snapshot serializes"),
                    Err(error) => JSONRPCResponse::error(id, error.rpc()),
                }
            }
            whale_protocol::interactions::METHOD_SESSION_INTERACTIONS_SUBSCRIBE => {
                let params: SubscribeInteractionsParams = match serde_json::from_value(value) {
                    Ok(params) => params,
                    Err(error) => {
                        return JSONRPCResponse::error(
                            id,
                            JSONRPCError::invalid_params(error.to_string()),
                        )
                    }
                };
                if let Err(error) = self
                    .lifecycle
                    .guard_open(&params.thread_id, transport.connection_id())
                {
                    return JSONRPCResponse::error(
                        id,
                        InteractionFailure::unavailable(error).rpc(),
                    );
                }
                match self
                    .interactions
                    .subscribe(transport.connection_id(), &params)
                {
                    Ok(replay) => {
                        JSONRPCResponse::success(id, replay).expect("Interaction replay serializes")
                    }
                    Err(error) => JSONRPCResponse::error(id, error.rpc()),
                }
            }
            _ => unreachable!("Interaction dispatcher routes known methods"),
        }
    }

    pub(super) async fn handle_request_interaction(
        &self,
        id: RequestId,
        params: Option<Value>,
        transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let params: whale_protocol::interactions::RequestInteractionParams =
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
        if let Err(error) = self
            .lifecycle
            .guard_open(&params.thread_id, transport.connection_id())
        {
            return JSONRPCResponse::error(id, InteractionFailure::unavailable(error).rpc());
        }
        if !self
            .interactions
            .enabled(transport.connection_id(), &params.thread_id)
            .unwrap_or(false)
        {
            return JSONRPCResponse::error(
                id,
                InteractionFailure::unavailable("Interactions are disabled for this Session").rpc(),
            );
        }
        let reference = whale_protocol::runs::RunRefParams {
            thread_id: params.thread_id.clone(),
            turn_id: params.turn_id.clone(),
        };
        let run = match self.find_run(&reference, transport).await {
            Ok(run) => run,
            Err(error) => return JSONRPCResponse::error(id, error),
        };
        if run.snapshot.lock().await.status.is_terminal() {
            return JSONRPCResponse::error(
                id,
                InteractionFailure::not_found("Interaction Run has finished").rpc(),
            );
        }
        let prefix = format!("{}:", transport.connection_id());
        let context = if params.host_call_id.starts_with(&prefix) {
            self.active_invocations
                .get(&params.host_call_id)
                .map(|entry| entry.value().clone())
        } else {
            None
        };
        let Some(context) = context.filter(|context| {
            context.is_active()
                && context.info.run.thread_id == params.thread_id
                && context.info.run.turn_id == params.turn_id
        }) else {
            return JSONRPCResponse::error(
                id,
                InteractionFailure::unavailable("Host callback is no longer active").rpc(),
            );
        };
        let request_id = params.request_id.clone();
        let host_call_id = params.host_call_id.clone();
        let ticket = match self.interactions.begin_host(
            &run.context,
            host_call_id.clone(),
            params.request_id,
            params.request,
        ) {
            Ok(ticket) => ticket,
            Err(error) => {
                return JSONRPCResponse::error(id, core_failure(error).rpc());
            }
        };
        let response = ticket.response();
        tokio::pin!(response);
        let outcome = tokio::select! {
            biased;
            _ = context.finished() => {
                self.interactions.clear_origin(
                    transport.connection_id(),
                    &run.context.thread_id,
                    &run.context.turn_id,
                    &host_call_id,
                    INTERACTION_REMOVAL_ORIGIN_FINISHED,
                );
                Err(CoreError::InteractionCancelled("Host callback finished".into()))
            },
            response = &mut response => response,
        };
        match outcome {
            Ok(response) => JSONRPCResponse::success(
                id,
                whale_protocol::interactions::InteractionResponse::new(request_id, response)
                    .expect("validated Interaction response"),
            )
            .expect("Interaction response serializes"),
            Err(error) => JSONRPCResponse::error(id, core_failure(error).rpc()),
        }
    }

    pub(super) async fn handle_respond_interaction(
        &self,
        id: RequestId,
        params: Option<Value>,
        transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let params: RespondInteractionParams =
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
            return JSONRPCResponse::error(id, InteractionFailure::response_invalid(error).rpc());
        }
        if let Err(error) = self
            .lifecycle
            .guard_open(&params.thread_id, transport.connection_id())
        {
            return JSONRPCResponse::error(id, InteractionFailure::unavailable(error).rpc());
        }
        let reference = whale_protocol::runs::RunRefParams {
            thread_id: params.thread_id.clone(),
            turn_id: params.turn_id.clone(),
        };
        let run = match self.find_run(&reference, transport).await {
            Ok(run) => run,
            Err(error) => return JSONRPCResponse::error(id, error),
        };
        if let Err(error) = self
            .resolve_interaction_response(&run, &params, transport.connection_id(), true)
            .await
        {
            return JSONRPCResponse::error(id, error.rpc());
        }
        JSONRPCResponse::success(
            id,
            whale_protocol::interactions::RespondInteractionResult {
                request_id: params.request_id,
                resolved: true,
            },
        )
        .expect("Interaction response result serializes")
    }

    pub(super) async fn resolve_interaction_response(
        &self,
        run: &Arc<RunRecord>,
        params: &RespondInteractionParams,
        owner: &str,
        require_enabled: bool,
    ) -> Result<ResolveOutcome, InteractionFailure> {
        let commit = self
            .interactions
            .respond_owned(owner, params, require_enabled)?;
        finish_response_commit(run, &self.session_views, &self.session_management, commit).await
    }
}

async fn finish_response_commit(
    run: &Arc<RunRecord>,
    session_views: &super::session_views::SessionViewRegistry,
    session_management: &super::session_management::SessionManagementRegistry,
    mut commit: ResponseCommit,
) -> Result<ResolveOutcome, InteractionFailure> {
    if commit.newly_resolved() {
        commit.stage_delivery()?;
    }
    let request_id = commit.request_id.clone();
    if commit.typed_approval() && commit.newly_resolved() {
        let changed = {
            let mut snapshot = run.snapshot.lock().await;
            let before = snapshot.pending_approvals.len();
            snapshot
                .pending_approvals
                .retain(|approval| approval.request_id != request_id);
            if before != snapshot.pending_approvals.len() {
                if snapshot.pending_approvals.is_empty()
                    && snapshot.status == whale_protocol::runs::RunStatus::WaitingApproval
                {
                    snapshot.status = whale_protocol::runs::RunStatus::Running;
                }
                Some(snapshot.clone())
            } else {
                None
            }
        };
        if let Some(snapshot) = changed {
            if let Err(error) = run
                .commit_session_event(
                    session_views,
                    session_management,
                    RunPublicationMutation::Changed(snapshot),
                )
                .await
            {
                let failure = InteractionFailure::unavailable(format!(
                    "Typed Interaction projection publication failed: {error}"
                ));
                return Err(commit.abort(INTERACTION_REMOVAL_PUBLICATION_FAILED, failure));
            }
        }
    }
    commit.complete().await
}

pub(super) struct ResponseCommit {
    registry: InteractionRegistry,
    thread_id: String,
    turn_id: String,
    request_id: String,
    digest: [u8; 32],
    generation: u64,
    sender: Option<oneshot::Sender<Result<InteractionDelivery, CoreError>>>,
    response: Option<Value>,
    delivery_commit: Option<oneshot::Sender<Result<(), CoreError>>>,
    typed_approval: bool,
    newly_resolved: bool,
    completion: Option<ResponseCompletion>,
}

enum ResponseCompletion {
    Leader(watch::Sender<Option<Result<(), InteractionFailure>>>),
    Follower(watch::Receiver<Option<Result<(), InteractionFailure>>>),
}

impl ResponseCommit {
    fn idempotent(
        registry: InteractionRegistry,
        params: &RespondInteractionParams,
        typed_approval: bool,
        completion: watch::Receiver<Option<Result<(), InteractionFailure>>>,
    ) -> Self {
        Self {
            registry,
            thread_id: params.thread_id.clone(),
            turn_id: params.turn_id.clone(),
            request_id: params.request_id.clone(),
            digest: [0; 32],
            generation: 0,
            sender: None,
            response: None,
            delivery_commit: None,
            typed_approval,
            newly_resolved: false,
            completion: Some(ResponseCompletion::Follower(completion)),
        }
    }

    pub(super) fn typed_approval(&self) -> bool {
        self.typed_approval
    }

    pub(super) fn newly_resolved(&self) -> bool {
        self.newly_resolved
    }

    fn publish_deferred_resolution(&self) -> Result<(), InteractionFailure> {
        self.registry.publish_resolved_exact(
            &self.thread_id,
            &self.turn_id,
            &self.request_id,
            self.generation,
            &self.digest,
        )
    }

    fn stage_delivery(&mut self) -> Result<(), InteractionFailure> {
        if !self.newly_resolved || self.delivery_commit.is_some() {
            return Ok(());
        }
        let sender = self
            .sender
            .take()
            .expect("new response has one continuation sender");
        let response = self.response.take().expect("new response has value");
        let (commit, committed) = oneshot::channel();
        if sender
            .send(Ok(InteractionDelivery {
                response,
                committed,
            }))
            .is_err()
        {
            let failure =
                InteractionFailure::not_found("Interaction continuation is no longer active");
            self.registry.abort_resolved_exact(
                &self.thread_id,
                &self.turn_id,
                &self.request_id,
                self.generation,
                &self.digest,
                INTERACTION_REMOVAL_ORIGIN_FINISHED,
            );
            self.signal_completion(Err(failure.clone()));
            return Err(failure);
        }
        self.delivery_commit = Some(commit);
        Ok(())
    }

    fn abort(mut self, cause: &str, failure: InteractionFailure) -> InteractionFailure {
        self.registry.abort_resolved_exact(
            &self.thread_id,
            &self.turn_id,
            &self.request_id,
            self.generation,
            &self.digest,
            cause,
        );
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(Err(failure.clone().into_core()));
        }
        if let Some(commit) = self.delivery_commit.take() {
            let _ = commit.send(Err(failure.clone().into_core()));
        }
        self.response.take();
        self.signal_completion(Err(failure.clone()));
        failure
    }

    pub(super) fn release(mut self) -> Result<ResolveOutcome, InteractionFailure> {
        let outcome = ResolveOutcome {
            typed_approval: self.typed_approval,
            newly_resolved: self.newly_resolved,
        };
        if !self.newly_resolved {
            return Ok(outcome);
        }
        self.stage_delivery()?;
        if self
            .delivery_commit
            .as_ref()
            .is_some_and(oneshot::Sender::is_closed)
        {
            let failure =
                InteractionFailure::not_found("Interaction continuation is no longer active");
            return Err(self.abort(INTERACTION_REMOVAL_ORIGIN_FINISHED, failure));
        }
        if let Err(error) = self.publish_deferred_resolution() {
            let failure = InteractionFailure::unavailable(format!(
                "Interaction resolution publication failed: {}",
                error.message
            ));
            return Err(self.abort(INTERACTION_REMOVAL_PUBLICATION_FAILED, failure));
        }
        if let Some(commit) = self.delivery_commit.take() {
            // The liveness check above is the response/cancellation
            // linearization point. A concurrent drop after it loses to the
            // committed response even if it removes its local receiver before
            // this acknowledgement is observed.
            let _ = commit.send(Ok(()));
        }
        self.signal_completion(Ok(()));
        Ok(outcome)
    }

    async fn complete(mut self) -> Result<ResolveOutcome, InteractionFailure> {
        if self.newly_resolved {
            return self.release();
        }
        let outcome = ResolveOutcome {
            typed_approval: self.typed_approval,
            newly_resolved: false,
        };
        let Some(ResponseCompletion::Follower(mut completion)) = self.completion.take() else {
            return Ok(outcome);
        };
        loop {
            if let Some(result) = completion.borrow_and_update().clone() {
                return result.map(|_| outcome);
            }
            if completion.changed().await.is_err() {
                return Err(InteractionFailure::not_found(
                    "Interaction response transaction ended before completion",
                ));
            }
        }
    }

    fn signal_completion(&mut self, result: Result<(), InteractionFailure>) {
        if let Some(ResponseCompletion::Leader(completion)) = self.completion.take() {
            completion.send_replace(Some(result));
        }
    }
}

impl Drop for ResponseCommit {
    fn drop(&mut self) {
        if self.sender.is_none() && self.delivery_commit.is_none() {
            return;
        }
        if self.newly_resolved {
            self.registry.abort_resolved_exact(
                &self.thread_id,
                &self.turn_id,
                &self.request_id,
                self.generation,
                &self.digest,
                INTERACTION_REMOVAL_PUBLICATION_FAILED,
            );
        }
        let failure =
            InteractionFailure::unavailable("Interaction response transaction was abandoned");
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(Err(failure.clone().into_core()));
        }
        if let Some(commit) = self.delivery_commit.take() {
            let _ = commit.send(Err(failure.clone().into_core()));
        }
        self.response.take();
        self.signal_completion(Err(failure));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ResolveOutcome {
    pub(super) typed_approval: bool,
    pub(super) newly_resolved: bool,
}

#[derive(Debug, Clone)]
pub(super) struct InteractionFailure {
    pub(super) code: i64,
    pub(super) message: String,
}

impl InteractionFailure {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: JSONRPCError::INVALID_PARAMS,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            code: INTERACTION_NOT_FOUND,
            message: message.into(),
        }
    }

    fn conflict() -> Self {
        Self {
            code: INTERACTION_CONFLICT,
            message: "InteractionConflict".into(),
        }
    }

    fn response_invalid(message: impl Into<String>) -> Self {
        Self {
            code: INTERACTION_RESPONSE_INVALID,
            message: message.into(),
        }
    }

    fn unavailable(message: impl Into<String>) -> Self {
        Self {
            code: INTERACTION_UNAVAILABLE,
            message: message.into(),
        }
    }

    pub(super) fn rpc(self) -> JSONRPCError {
        JSONRPCError::new(self.code, self.message, None)
    }

    fn into_core(self) -> CoreError {
        match self.code {
            INTERACTION_RESPONSE_INVALID => CoreError::InteractionResponseInvalid(self.message),
            INTERACTION_NOT_FOUND => CoreError::InteractionCancelled(self.message),
            _ => CoreError::InteractionUnavailable(self.message),
        }
    }
}

#[derive(Clone)]
pub(super) struct DaemonInteractionBridge {
    registry: InteractionRegistry,
    runs: Weak<tokio::sync::Mutex<super::retention::RunRegistry>>,
    session_views: super::session_views::SessionViewRegistry,
    session_management: super::session_management::SessionManagementRegistry,
    runtime: Arc<OnceLock<tokio::runtime::Handle>>,
}

impl DaemonInteractionBridge {
    pub(super) fn new(
        registry: InteractionRegistry,
        runs: Weak<tokio::sync::Mutex<super::retention::RunRegistry>>,
        session_views: super::session_views::SessionViewRegistry,
        session_management: super::session_management::SessionManagementRegistry,
    ) -> Self {
        Self {
            registry,
            runs,
            session_views,
            session_management,
            runtime: Arc::new(OnceLock::new()),
        }
    }
}

#[async_trait]
impl InteractionBridge for DaemonInteractionBridge {
    async fn begin(
        &self,
        context: &RunContextInfo,
        request_id: String,
        request: InteractionRequest,
    ) -> Result<InteractionTicket, CoreError> {
        let _ = self.runtime.set(tokio::runtime::Handle::current());
        self.registry
            .begin(context, request_id, request, InteractionOrigin::Core)
    }

    fn respond(
        &self,
        context: &RunContextInfo,
        request_id: &str,
        response: Value,
    ) -> Result<(), CoreError> {
        let commit = self
            .registry
            .commit_core_response(context, request_id, response)?;
        if !commit.typed_approval() || !commit.newly_resolved() {
            return commit
                .release()
                .map(|_| ())
                .map_err(InteractionFailure::into_core);
        }
        let Some(runs) = self.runs.upgrade() else {
            drop(commit);
            return Err(CoreError::InteractionUnavailable(
                "Interaction Run registry is no longer active".into(),
            ));
        };
        let Some(runtime) = self.runtime.get().cloned() else {
            drop(commit);
            return Err(CoreError::InteractionUnavailable(
                "Interaction runtime is no longer active".into(),
            ));
        };
        let thread_id = context.thread_id.clone();
        let turn_id = context.turn_id.clone();
        let session_views = self.session_views.clone();
        let session_management = self.session_management.clone();
        runtime.spawn(async move {
            let run = runs
                .lock()
                .await
                .get(&(thread_id.clone(), turn_id.clone()))
                .cloned();
            let Some(run) = run else {
                drop(commit);
                return;
            };
            if let Err(error) =
                finish_response_commit(&run, &session_views, &session_management, commit).await
            {
                tracing::warn!(
                    thread_id,
                    turn_id,
                    code = error.code,
                    "Failed to finish delegated typed Interaction response"
                );
            }
        });
        Ok(())
    }

    fn clear(&self, context: &RunContextInfo, request_id: &str, cause: &str) {
        self.registry.clear_core_request(context, request_id, cause);
    }
}

fn response_digest(key: &[u8; 32], response: &Value) -> Result<[u8; 32], String> {
    let canonical = canonical_json(response);
    let bytes = serde_json::to_vec(&canonical)
        .map_err(|error| format!("Interaction response serialization failed: {error}"))?;
    let mut mac = Hmac::<Sha256>::new_from_slice(key)
        .map_err(|_| "Interaction response key is invalid".to_string())?;
    mac.update(&bytes);
    Ok(mac.finalize().into_bytes().into())
}

fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut keys: Vec<_> = object.keys().collect();
            keys.sort();
            let mut canonical = serde_json::Map::new();
            for key in keys {
                canonical.insert(key.clone(), canonical_json(&object[key]));
            }
            Value::Object(canonical)
        }
        Value::Array(values) => Value::Array(values.iter().map(canonical_json).collect()),
        other => other.clone(),
    }
}

fn constant_time_eq(left: &[u8; 32], right: &[u8; 32]) -> bool {
    // `verify_slice` compares authentication tags in constant time. Hashing
    // both fixed-size digests under the same local comparison key gives one
    // fixed-work equality check without exposing either response fingerprint.
    const COMPARISON_KEY: &[u8] = b"whale-interaction-digest-compare";
    let mut actual = Hmac::<Sha256>::new_from_slice(COMPARISON_KEY).expect("valid HMAC key");
    actual.update(left);
    let mut expected = Hmac::<Sha256>::new_from_slice(COMPARISON_KEY).expect("valid HMAC key");
    expected.update(right);
    actual
        .verify_slice(&expected.finalize().into_bytes())
        .is_ok()
}

fn core_failure(error: CoreError) -> InteractionFailure {
    match error {
        CoreError::InteractionResponseInvalid(message) => {
            InteractionFailure::response_invalid(message)
        }
        CoreError::InteractionRequestInvalid(message) => InteractionFailure::invalid(message),
        CoreError::InteractionCancelled(message) => InteractionFailure::not_found(message),
        CoreError::InteractionUnavailable(message) => InteractionFailure::unavailable(message),
        other => InteractionFailure::unavailable(other.to_string()),
    }
}

fn retain(
    journal: &mut VecDeque<JournalEntry>,
    journal_bytes: &mut usize,
    replay_floor: &mut InteractionCursor,
    limits: InteractionJournalLimits,
    envelope: InteractionEventEnvelope,
    bytes: usize,
    turn_id: String,
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
        turn_id,
    });
    *journal_bytes += bytes;
    while journal.len() > limits.max_events || *journal_bytes > limits.max_bytes {
        let evicted = journal.pop_front().expect("nonempty Interaction journal");
        *journal_bytes -= evicted.bytes;
        *replay_floor = evicted.envelope.cursor;
    }
}

fn scrub_run(
    journal: &mut VecDeque<JournalEntry>,
    journal_bytes: &mut usize,
    replay_floor: &mut InteractionCursor,
    turn_id: &str,
) {
    let through = journal
        .iter()
        .filter(|entry| entry.turn_id == turn_id)
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

fn event_turn_id(envelope: &InteractionEventEnvelope) -> &str {
    match &envelope.payload {
        InteractionEventPayload::Requested { interaction } => &interaction.turn_id,
        InteractionEventPayload::Removed { turn_id, .. } => turn_id,
        _ => unreachable!("daemon constructs only known Interaction event payloads"),
    }
}

fn spawn_notifier(transport: AnyTransportWriter) -> InteractionNotifier {
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
                    // The bounded receiver resumes at the oldest retained
                    // cursor. Clients observe the gap and replay it, while the
                    // retained tail (including the high watermark) is still
                    // delivered below.
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            };
            let Ok(notification) =
                JSONRPCNotification::new(METHOD_SESSION_INTERACTION_EVENT, Some(envelope))
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
    InteractionNotifier {
        events: Some(events),
        cancel: Some(cancel),
        task: Some(task),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Duration;
    use tokio::sync::mpsc;

    struct Capture(mpsc::UnboundedSender<Value>);

    #[async_trait]
    impl OutgoingTransport for Capture {
        async fn send_line(&self, line: &str) -> std::io::Result<()> {
            self.0
                .send(serde_json::from_str(line).unwrap())
                .map_err(|_| std::io::Error::other("capture closed"))
        }
    }

    struct FailFirstNotifications {
        attempts: std::sync::atomic::AtomicUsize,
        failures: usize,
        output: mpsc::UnboundedSender<u64>,
    }

    #[async_trait]
    impl OutgoingTransport for FailFirstNotifications {
        async fn send_line(&self, line: &str) -> std::io::Result<()> {
            let attempt = self
                .attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if attempt < self.failures {
                return Err(std::io::Error::other("transient notification failure"));
            }
            let value: Value = serde_json::from_str(line).unwrap();
            self.output
                .send(value["params"]["cursor"]["seq"].as_u64().unwrap())
                .map_err(|_| std::io::Error::other("capture closed"))
        }
    }

    struct BlockFirstNotification {
        attempts: std::sync::atomic::AtomicUsize,
        entered: tokio::sync::Semaphore,
        release: tokio::sync::Semaphore,
        output: mpsc::UnboundedSender<u64>,
    }

    #[async_trait]
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
            let value: Value = serde_json::from_str(line).unwrap();
            self.output
                .send(value["params"]["cursor"]["seq"].as_u64().unwrap())
                .map_err(|_| std::io::Error::other("capture closed"))
        }
    }

    struct PendingNotification {
        entered: Arc<tokio::sync::Semaphore>,
        lifetime: Arc<()>,
    }

    #[async_trait]
    impl OutgoingTransport for PendingNotification {
        async fn send_line(&self, _line: &str) -> std::io::Result<()> {
            let _lifetime = self.lifetime.clone();
            self.entered.add_permits(1);
            std::future::pending().await
        }
    }

    struct BlockFirstThenCapture {
        attempts: std::sync::atomic::AtomicUsize,
        entered: tokio::sync::Semaphore,
        output: mpsc::UnboundedSender<u64>,
    }

    #[async_trait]
    impl OutgoingTransport for BlockFirstThenCapture {
        async fn send_line(&self, line: &str) -> std::io::Result<()> {
            if self
                .attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                == 0
            {
                self.entered.add_permits(1);
                std::future::pending::<()>().await;
            }
            let value: Value = serde_json::from_str(line).unwrap();
            self.output
                .send(value["params"]["cursor"]["seq"].as_u64().unwrap())
                .map_err(|_| std::io::Error::other("capture closed"))
        }
    }

    fn context(turn_id: &str) -> RunContextInfo {
        RunContextInfo {
            agent_name: None,
            thread_id: "session".into(),
            turn_id: turn_id.into(),
            deadline_unix_ms: None,
        }
    }

    fn request(kind: &str) -> InteractionRequest {
        InteractionRequest::new(
            kind,
            "Need input",
            json!({"safe":"display"}),
            Some(json!({
                "$schema":"https://json-schema.org/draft/2020-12/schema",
                "type":"object",
                "properties":{"answer":{"type":"string"}},
                "required":["answer"],
                "additionalProperties":false
            })),
        )
        .unwrap()
    }

    fn notification_envelope(seq: u64) -> InteractionEventEnvelope {
        InteractionEventEnvelope::new(
            "session",
            InteractionCursor {
                thread_id: "session".into(),
                stream_id: "stream".into(),
                seq,
            },
            seq,
            InteractionEventPayload::Requested {
                interaction: PendingInteraction::new(
                    format!("request-{seq}"),
                    format!("turn-{seq}"),
                    request("vendor.question"),
                )
                .unwrap(),
            },
        )
    }

    fn fixture(
        limits: InteractionJournalLimits,
        enabled: bool,
    ) -> (
        InteractionRegistry,
        mpsc::UnboundedReceiver<Value>,
        AnyTransportWriter,
    ) {
        let registry = InteractionRegistry::with_limits(limits);
        let (tx, rx) = mpsc::unbounded_channel();
        let writer = AnyTransportWriter::new(Arc::new(Capture(tx)));
        registry
            .insert("owner", "session".into(), enabled, writer.clone())
            .unwrap();
        (registry, rx, writer)
    }

    fn register(registry: &InteractionRegistry, turn: &str, identity: usize) {
        registry
            .register_run("owner", "session", turn, identity)
            .unwrap();
    }

    fn response_failure(result: Result<ResponseCommit, InteractionFailure>) -> InteractionFailure {
        match result {
            Ok(commit) => {
                drop(commit);
                panic!("response unexpectedly committed")
            }
            Err(error) => error,
        }
    }

    #[tokio::test]
    async fn notifier_retries_one_event_until_transient_errors_clear_without_duplicates() {
        let (output, mut received) = mpsc::unbounded_channel();
        let transport = Arc::new(FailFirstNotifications {
            attempts: std::sync::atomic::AtomicUsize::new(0),
            failures: 2,
            output,
        });
        let notifier = spawn_notifier(AnyTransportWriter::new(transport.clone()));

        notifier.publish(notification_envelope(1));

        let cursor = tokio::time::timeout(Duration::from_secs(1), received.recv())
            .await
            .expect("notification retry timed out")
            .expect("notification retry worker closed");
        assert_eq!(cursor, 1);
        assert_eq!(
            transport.attempts.load(std::sync::atomic::Ordering::SeqCst),
            3
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(30), received.recv())
                .await
                .is_err(),
            "a successful retry must deliver the committed event exactly once"
        );
        drop(notifier);
    }

    #[tokio::test]
    async fn notifier_lag_preserves_monotonic_delivery_and_reaches_the_unique_tail() {
        let (output, mut received) = mpsc::unbounded_channel();
        let transport = Arc::new(BlockFirstNotification {
            attempts: std::sync::atomic::AtomicUsize::new(0),
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            output,
        });
        let notifier = spawn_notifier(AnyTransportWriter::new(transport.clone()));
        notifier.publish(notification_envelope(1));
        transport.entered.acquire().await.unwrap().forget();

        let target = DEFAULT_NOTIFICATION_QUEUE as u64 + 10;
        for seq in 2..=target {
            notifier.publish(notification_envelope(seq));
        }
        transport.release.add_permits(1);

        let delivered = tokio::time::timeout(Duration::from_secs(2), async {
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
        .expect("lagged notifier never converged to the final committed cursor");
        assert_eq!(delivered[0], 1);
        assert_eq!(delivered.last().copied(), Some(target));
        assert!(delivered.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(
            delivered.iter().filter(|&&cursor| cursor == target).count(),
            1,
            "the high-watermark event must be delivered exactly once"
        );
        assert!(
            delivered.windows(2).any(|pair| pair[1] > pair[0] + 1),
            "bounded lag must remain observable as a replayable cursor gap"
        );
        drop(notifier);
    }

    #[tokio::test(start_paused = true)]
    async fn notifier_retries_a_timed_out_send_and_delivers_the_event_once() {
        let (output, mut received) = mpsc::unbounded_channel();
        let transport = Arc::new(BlockFirstThenCapture {
            attempts: std::sync::atomic::AtomicUsize::new(0),
            entered: tokio::sync::Semaphore::new(0),
            output,
        });
        let notifier = spawn_notifier(AnyTransportWriter::new(transport.clone()));

        notifier.publish(notification_envelope(1));
        transport.entered.acquire().await.unwrap().forget();
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;

        assert_eq!(received.recv().await, Some(1));
        assert_eq!(
            transport.attempts.load(std::sync::atomic::Ordering::SeqCst),
            2
        );
        assert!(received.try_recv().is_err());
        drop(notifier);
    }

    #[tokio::test]
    async fn notifier_drop_cancels_a_permanently_blocked_retry_and_releases_transport() {
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let lifetime = Arc::new(());
        let weak_lifetime = Arc::downgrade(&lifetime);
        let transport = Arc::new(PendingNotification {
            entered: entered.clone(),
            lifetime: lifetime.clone(),
        });
        let notifier = spawn_notifier(AnyTransportWriter::new(transport.clone()));

        notifier.publish(notification_envelope(1));
        entered.acquire().await.unwrap().forget();
        drop(lifetime);
        drop(transport);
        drop(notifier);

        tokio::time::timeout(Duration::from_secs(1), async {
            while weak_lifetime.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping the notifier must abort its blocked transport task");
    }

    #[tokio::test]
    async fn snapshot_replay_freezes_through_filters_runs_and_notifies_in_order() {
        let (registry, mut output, _writer) = fixture(InteractionJournalLimits::default(), true);
        register(&registry, "turn-a", 1);
        register(&registry, "turn-b", 2);
        let start = registry
            .get(
                "owner",
                &GetSessionInteractionsParams {
                    thread_id: "session".into(),
                },
            )
            .unwrap()
            .cursor;
        let first = registry
            .begin(
                &context("turn-a"),
                "request-a".into(),
                request("vendor.question"),
                InteractionOrigin::Core,
            )
            .unwrap();
        let page = registry
            .subscribe(
                "owner",
                &SubscribeInteractionsParams {
                    thread_id: "session".into(),
                    after: start,
                    through: None,
                    limit: 1,
                },
            )
            .unwrap();
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.through.seq, 1);
        let second = registry
            .begin(
                &context("turn-b"),
                "request-b".into(),
                request("vendor.question"),
                InteractionOrigin::Core,
            )
            .unwrap();
        let fixed = registry
            .subscribe(
                "owner",
                &SubscribeInteractionsParams {
                    thread_id: "session".into(),
                    after: page.resume_after.clone(),
                    through: Some(page.through.clone()),
                    limit: 1,
                },
            )
            .unwrap();
        assert!(fixed.events.is_empty());
        assert!(!fixed.has_more);
        let turn = registry
            .get_turn(
                "owner",
                &GetTurnInteractionsParams {
                    thread_id: "session".into(),
                    turn_id: "turn-b".into(),
                },
            )
            .unwrap();
        assert_eq!(turn.cursor.seq, 2);
        assert_eq!(turn.pending.len(), 1);
        assert_eq!(turn.pending[0].request_id, "request-b");

        for expected in [1, 2] {
            let value = tokio::time::timeout(std::time::Duration::from_secs(1), output.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(value["method"], METHOD_SESSION_INTERACTION_EVENT);
            assert_eq!(value["params"]["cursor"]["seq"], expected);
        }
        drop((first, second));
    }

    #[tokio::test]
    async fn journal_count_byte_oversize_and_stream_reset_have_continuous_gaps() {
        let cursor = InteractionCursor {
            thread_id: "session".into(),
            stream_id: "stream".into(),
            seq: 0,
        };
        let pending = PendingInteraction::new(
            "request",
            "turn",
            InteractionRequest::new("vendor.question", "title", json!({}), None).unwrap(),
        )
        .unwrap();
        let envelope = |seq| {
            let interaction = PendingInteraction::new(
                format!("request-{seq}"),
                pending.turn_id.clone(),
                pending.request.clone(),
            )
            .unwrap();
            InteractionEventEnvelope::new(
                "session",
                InteractionCursor {
                    seq,
                    ..cursor.clone()
                },
                1,
                InteractionEventPayload::Requested { interaction },
            )
        };
        let mut journal = VecDeque::new();
        let mut bytes = 0;
        let mut floor = cursor.clone();
        let count_limits = InteractionJournalLimits::new(MAX_INTERACTION_JOURNAL_EVENTS, 10_000);
        for seq in 1..=(MAX_INTERACTION_JOURNAL_EVENTS as u64 + 1) {
            retain(
                &mut journal,
                &mut bytes,
                &mut floor,
                count_limits,
                envelope(seq),
                1,
                "turn".into(),
            );
        }
        assert_eq!(journal.len(), MAX_INTERACTION_JOURNAL_EVENTS);
        assert_eq!(floor.seq, 1);
        assert_eq!(bytes, MAX_INTERACTION_JOURNAL_EVENTS);

        let mut journal = VecDeque::new();
        let mut bytes = 0;
        let mut floor = cursor.clone();
        let byte_limits = InteractionJournalLimits::new(10, 10);
        retain(
            &mut journal,
            &mut bytes,
            &mut floor,
            byte_limits,
            envelope(1),
            10,
            "turn".into(),
        );
        assert_eq!(bytes, 10, "the exact byte boundary is retained");
        retain(
            &mut journal,
            &mut bytes,
            &mut floor,
            byte_limits,
            envelope(2),
            1,
            "turn".into(),
        );
        assert_eq!(floor.seq, 1);
        assert_eq!(bytes, 1);
        retain(
            &mut journal,
            &mut bytes,
            &mut floor,
            byte_limits,
            envelope(3),
            11,
            "turn".into(),
        );
        assert!(journal.is_empty());
        assert_eq!(bytes, 0);
        assert_eq!(floor.seq, 3);

        let (registry, _output, writer) = fixture(InteractionJournalLimits::default(), true);
        let old = registry
            .get(
                "owner",
                &GetSessionInteractionsParams {
                    thread_id: "session".into(),
                },
            )
            .unwrap()
            .cursor;
        registry.close_session("owner", "session", "session_closed");
        registry
            .insert("owner", "session".into(), true, writer)
            .unwrap();
        let reset = registry
            .subscribe(
                "owner",
                &SubscribeInteractionsParams {
                    thread_id: "session".into(),
                    after: old,
                    through: None,
                    limit: 1,
                },
            )
            .unwrap();
        assert_eq!(
            reset.gap.unwrap().reason,
            InteractionReplayGapReason::StreamReset
        );
    }

    #[tokio::test]
    async fn canonical_retries_conflicts_and_cancel_arbitrate_once() {
        let (registry, _output, _writer) = fixture(InteractionJournalLimits::default(), true);
        register(&registry, "turn-a", 1);
        let ticket = registry
            .begin(
                &context("turn-a"),
                "request-a".into(),
                request("vendor.question"),
                InteractionOrigin::Core,
            )
            .unwrap();
        let first_params = RespondInteractionParams::new(
            "session",
            "turn-a",
            "request-a",
            serde_json::from_str(r#"{"answer":"yes","unused":1}"#).unwrap(),
        )
        .unwrap();
        assert_eq!(
            response_failure(registry.respond_owned("owner", &first_params, true)).code,
            INTERACTION_RESPONSE_INVALID
        );
        let first_params = RespondInteractionParams::new(
            "session",
            "turn-a",
            "request-a",
            serde_json::from_str(r#"{"answer":"yes"}"#).unwrap(),
        )
        .unwrap();
        let first = registry
            .respond_owned("owner", &first_params, true)
            .unwrap();
        let equivalent = RespondInteractionParams::new(
            "session",
            "turn-a",
            "request-a",
            serde_json::from_str(r#"{"answer":"yes"}"#).unwrap(),
        )
        .unwrap();
        let retry = registry.respond_owned("owner", &equivalent, true).unwrap();
        assert!(!retry.release().unwrap().newly_resolved);
        let conflict =
            RespondInteractionParams::new("session", "turn-a", "request-a", json!({"answer":"no"}))
                .unwrap();
        assert_eq!(
            response_failure(registry.respond_owned("owner", &conflict, true)).code,
            INTERACTION_CONFLICT
        );
        assert!(first.release().unwrap().newly_resolved);
        assert_eq!(ticket.response().await.unwrap(), json!({"answer":"yes"}));

        let cancelled = registry
            .begin(
                &context("turn-a"),
                "request-cancelled".into(),
                request("vendor.question"),
                InteractionOrigin::Core,
            )
            .unwrap();
        registry.clear_run("owner", "session", "turn-a", "cancelled");
        let after_cancel = RespondInteractionParams::new(
            "session",
            "turn-a",
            "request-cancelled",
            json!({"answer":"late"}),
        )
        .unwrap();
        assert_eq!(
            response_failure(registry.respond_owned("owner", &after_cancel, true)).code,
            INTERACTION_NOT_FOUND
        );
        assert!(matches!(
            cancelled.response().await,
            Err(CoreError::InteractionCancelled(_))
        ));
    }

    #[tokio::test]
    async fn generic_ticket_drop_before_release_clears_without_resolved_audit() {
        let (registry, mut output, _writer) = fixture(InteractionJournalLimits::default(), true);
        register(&registry, "turn-a", 1);
        let start = registry
            .get(
                "owner",
                &GetSessionInteractionsParams {
                    thread_id: "session".into(),
                },
            )
            .unwrap()
            .cursor;
        let ticket = registry
            .begin(
                &context("turn-a"),
                "request-dropped".into(),
                request("vendor.question"),
                InteractionOrigin::Core,
            )
            .unwrap();
        let params = RespondInteractionParams::new(
            "session",
            "turn-a",
            "request-dropped",
            json!({"answer":"generic-response-secret-must-not-escape"}),
        )
        .unwrap();
        let leader = registry.respond_owned("owner", &params, true).unwrap();
        let follower = registry.respond_owned("owner", &params, true).unwrap();

        drop(ticket);
        let error = leader
            .release()
            .expect_err("a vanished generic continuation cannot resolve");
        assert_eq!(error.code, INTERACTION_NOT_FOUND);
        let follower_error = follower
            .complete()
            .await
            .expect_err("the equivalent follower must observe the same failure");
        assert_eq!(follower_error.code, error.code);
        assert_eq!(follower_error.message, error.message);
        assert_eq!(
            response_failure(registry.respond_owned("owner", &params, true)).code,
            INTERACTION_NOT_FOUND
        );

        let snapshot = registry
            .get(
                "owner",
                &GetSessionInteractionsParams {
                    thread_id: "session".into(),
                },
            )
            .unwrap();
        assert!(snapshot.pending.is_empty());
        let replay = registry
            .subscribe(
                "owner",
                &SubscribeInteractionsParams {
                    thread_id: "session".into(),
                    after: start,
                    through: None,
                    limit: 2,
                },
            )
            .unwrap();
        assert_eq!(replay.events.len(), 2);
        assert!(matches!(
            &replay.events[1].payload,
            InteractionEventPayload::Removed { request_id, cause, .. }
                if request_id == "request-dropped"
                    && cause == INTERACTION_REMOVAL_ORIGIN_FINISHED
        ));

        let requested_notification = tokio::time::timeout(Duration::from_secs(1), output.recv())
            .await
            .expect("requested notification timeout")
            .expect("requested notification missing");
        let removed_notification = tokio::time::timeout(Duration::from_secs(1), output.recv())
            .await
            .expect("removed notification timeout")
            .expect("removed notification missing");
        assert_eq!(removed_notification["params"]["type"], "removed");
        assert_eq!(
            removed_notification["params"]["cause"],
            INTERACTION_REMOVAL_ORIGIN_FINISHED
        );
        let observable = serde_json::to_string(&json!({
            "snapshot": snapshot,
            "replay": replay,
            "notifications": [requested_notification, removed_notification],
            "leader_error": error.message,
            "follower_error": follower_error.message,
        }))
        .unwrap();
        assert!(!observable.contains("generic-response-secret-must-not-escape"));
        assert!(!observable.contains("fingerprint"));
        assert!(!observable.contains("digest"));
        assert!(!observable.contains("hmac"));
        assert!(!observable.contains(INTERACTION_REMOVAL_RESOLVED));
    }

    #[tokio::test]
    async fn duplicate_count_owner_disable_and_vanished_continuation_are_rejected() {
        let (registry, _output, _writer) = fixture(InteractionJournalLimits::default(), true);
        register(&registry, "turn-a", 1);
        let vanished = registry
            .begin(
                &context("turn-a"),
                "vanished".into(),
                request("vendor.question"),
                InteractionOrigin::Core,
            )
            .unwrap();
        drop(vanished);
        let response = RespondInteractionParams::new(
            "session",
            "turn-a",
            "vanished",
            json!({"answer":"late"}),
        )
        .unwrap();
        assert_eq!(
            response_failure(registry.respond_owned("owner", &response, true)).code,
            INTERACTION_NOT_FOUND
        );
        assert_eq!(
            response_failure(registry.respond_owned("foreign", &response, true)).code,
            INTERACTION_UNAVAILABLE
        );

        let mut tickets = Vec::new();
        for index in 0..MAX_PENDING_INTERACTIONS_PER_RUN {
            tickets.push(
                registry
                    .begin(
                        &context("turn-a"),
                        format!("request-{index}"),
                        request("vendor.question"),
                        InteractionOrigin::Core,
                    )
                    .unwrap(),
            );
        }
        assert!(matches!(
            registry.begin(
                &context("turn-a"),
                "over-limit".into(),
                request("vendor.question"),
                InteractionOrigin::Core,
            ),
            Err(CoreError::InteractionUnavailable(_))
        ));
        assert!(matches!(
            registry.begin(
                &context("turn-a"),
                "request-0".into(),
                request("vendor.question"),
                InteractionOrigin::Core,
            ),
            Err(CoreError::InteractionUnavailable(_))
        ));
        drop(tickets);

        let (disabled, _output, _writer) = fixture(InteractionJournalLimits::default(), false);
        register(&disabled, "turn-a", 1);
        assert!(matches!(
            disabled.begin(
                &context("turn-a"),
                "custom".into(),
                request("vendor.question"),
                InteractionOrigin::Core,
            ),
            Err(CoreError::InteractionUnavailable(_))
        ));
        let typed = disabled
            .begin(
                &context("turn-a"),
                "typed".into(),
                InteractionRequest::new(KIND_TOOL_APPROVAL, "approval", json!({}), None).unwrap(),
                InteractionOrigin::Core,
            )
            .unwrap();
        drop(typed);
    }

    #[tokio::test]
    async fn retention_scrubs_exact_run_before_expiry_and_rejects_stale_identity() {
        let (registry, _output, _writer) = fixture(InteractionJournalLimits::default(), true);
        register(&registry, "turn-a", 7);
        register(&registry, "turn-b", 8);
        let start = registry
            .get(
                "owner",
                &GetSessionInteractionsParams {
                    thread_id: "session".into(),
                },
            )
            .unwrap()
            .cursor;
        let ticket = registry
            .begin(
                &context("turn-a"),
                "request-a".into(),
                request("vendor.question"),
                InteractionOrigin::Core,
            )
            .unwrap();
        let surviving = registry
            .begin(
                &context("turn-b"),
                "request-b".into(),
                request("vendor.question"),
                InteractionOrigin::Core,
            )
            .unwrap();
        let params = RespondInteractionParams::new(
            "session",
            "turn-a",
            "request-a",
            json!({"answer":"done"}),
        )
        .unwrap();
        registry
            .respond_owned("owner", &params, true)
            .unwrap()
            .release()
            .unwrap();
        assert!(ticket.response().await.is_ok());
        assert!(!registry.retire_run("owner", "session", "turn-a", 8));
        assert!(registry.retire_run("owner", "session", "turn-a", 7));
        let replay = registry
            .subscribe(
                "owner",
                &SubscribeInteractionsParams {
                    thread_id: "session".into(),
                    after: start,
                    through: None,
                    limit: 128,
                },
            )
            .unwrap();
        let gap = replay.gap.unwrap();
        assert_eq!(gap.reason, InteractionReplayGapReason::Retention);
        assert_eq!(gap.replay_floor.seq, 3);
        let snapshot = registry
            .get(
                "owner",
                &GetSessionInteractionsParams {
                    thread_id: "session".into(),
                },
            )
            .unwrap();
        assert_eq!(snapshot.pending.len(), 1);
        assert_eq!(snapshot.pending[0].request_id, "request-b");
        assert!(registry.retire_run("owner", "session", "turn-a", 7));
        drop(surviving);
    }

    #[tokio::test]
    async fn typed_publication_failure_clears_exact_daemon_identity_and_wakes_waiter() {
        let (registry, _output, _writer) = fixture(InteractionJournalLimits::default(), true);
        register(&registry, "turn-a", 1);
        let start = registry
            .get(
                "owner",
                &GetSessionInteractionsParams {
                    thread_id: "session".into(),
                },
            )
            .unwrap()
            .cursor;
        let context = context("turn-a");
        let ticket = registry
            .begin(
                &context,
                "request-publication".into(),
                InteractionRequest::new(KIND_TOOL_APPROVAL, "approval", json!({}), None).unwrap(),
                InteractionOrigin::Core,
            )
            .unwrap();
        registry.clear_core_request(
            &context,
            "request-publication",
            whale_protocol::interactions::INTERACTION_REMOVAL_PUBLICATION_FAILED,
        );
        assert!(matches!(
            ticket.response().await,
            Err(CoreError::InteractionCancelled(_))
        ));
        let replay = registry
            .subscribe(
                "owner",
                &SubscribeInteractionsParams {
                    thread_id: "session".into(),
                    after: start,
                    through: None,
                    limit: 2,
                },
            )
            .unwrap();
        assert_eq!(replay.events.len(), 2);
        assert!(matches!(
            &replay.events[1].payload,
            InteractionEventPayload::Removed { request_id, cause, .. }
                if request_id == "request-publication"
                    && cause == whale_protocol::interactions::INTERACTION_REMOVAL_PUBLICATION_FAILED
        ));
    }

    #[tokio::test]
    async fn typed_ticket_drop_before_finish_clears_without_resolved_audit() {
        let (registry, mut output, writer) = fixture(InteractionJournalLimits::default(), true);
        register(&registry, "turn-a", 1);
        let context = context("turn-a");
        let request_id = "request-typed-dropped";
        let start = registry
            .get(
                "owner",
                &GetSessionInteractionsParams {
                    thread_id: "session".into(),
                },
            )
            .unwrap()
            .cursor;
        let ticket = registry
            .begin(
                &context,
                request_id.into(),
                InteractionRequest::new(
                    KIND_TOOL_APPROVAL,
                    "approval",
                    json!({}),
                    Some(json!({
                        "$schema":"https://json-schema.org/draft/2020-12/schema",
                        "type":"object",
                        "properties":{
                            "decision":{"const":"reject"},
                            "feedback":{"type":"string"}
                        },
                        "required":["decision"],
                        "additionalProperties":false
                    })),
                )
                .unwrap(),
                InteractionOrigin::Core,
            )
            .unwrap();
        let params = RespondInteractionParams::new(
            "session",
            "turn-a",
            request_id,
            json!({
                "decision":"reject",
                "feedback":"typed-response-secret-must-not-escape"
            }),
        )
        .unwrap();
        let leader = registry.respond_owned("owner", &params, true).unwrap();
        let follower = registry.respond_owned("owner", &params, true).unwrap();

        let tool_call = whale_protocol::CanonicalItem::tool_call(
            "call-a",
            None,
            "lookup",
            Some(json!({})),
            "{}",
        );
        let initial_snapshot = whale_protocol::runs::RunSnapshot {
            thread_id: "session".into(),
            turn_id: "turn-a".into(),
            status: whale_protocol::runs::RunStatus::WaitingApproval,
            items: Vec::new(),
            usage: Default::default(),
            tool_executions: Vec::new(),
            pending_approvals: vec![whale_protocol::runs::PendingApproval {
                request_id: request_id.into(),
                tool_call,
                reason: None,
            }],
            last_seq: 0,
            result: None,
            error: None,
        };
        let session_views = super::super::session_views::SessionViewRegistry::default();
        session_views
            .insert(
                "owner",
                "session".into(),
                None,
                serde_json::Map::new(),
                Vec::new(),
                1,
                writer.clone(),
            )
            .unwrap();
        let session_management =
            super::super::session_management::SessionManagementRegistry::default();
        session_management
            .insert(
                "owner",
                "session".into(),
                None,
                serde_json::Map::new(),
                Vec::new(),
                1,
                whale_protocol::session_management::SessionPersistenceV2::Ephemeral,
                None,
                writer,
            )
            .unwrap();
        session_management
            .publish_run(
                &session_views,
                "owner",
                RunPublicationMutation::Begin {
                    snapshot: initial_snapshot.clone(),
                    identity: 1,
                },
                2,
            )
            .await
            .unwrap();
        let run = Arc::new(super::super::RunRecord {
            owner: "owner".into(),
            params: whale_protocol::runs::StartTurnParams {
                thread_id: "session".into(),
                turn_id: "turn-a".into(),
                input_items: Vec::new(),
                options: None,
                max_steps: 1,
                timeout_ms: None,
            },
            snapshot: tokio::sync::Mutex::new(initial_snapshot),
            cancel: watch::channel(false).0,
            cancellation: whale_core::CancellationToken::new(),
            context,
            acceptance: watch::channel(Some(true)).0,
            preparation: watch::channel(Some(Ok(()))).0,
            finished: tokio::sync::Notify::new(),
            terminal_delivery: watch::channel(None).0,
            publication: tokio::sync::Mutex::new(Default::default()),
            decisions: tokio::sync::Mutex::new(HashMap::new()),
            interactions: registry.clone(),
            legacy: std::sync::atomic::AtomicBool::new(false),
            legacy_events_finished: std::sync::atomic::AtomicBool::new(false),
            progress: Arc::new(whale_core::engine::RunProgress::default()),
            deadline: None,
            accepted_ordinal: 1,
        });

        drop(ticket);
        let error = finish_response_commit(&run, &session_views, &session_management, leader)
            .await
            .expect_err("a vanished typed continuation cannot resolve");
        assert_eq!(error.code, INTERACTION_NOT_FOUND);
        let follower_error = follower
            .complete()
            .await
            .expect_err("the equivalent follower must observe the same failure");
        assert_eq!(follower_error.code, error.code);
        assert_eq!(follower_error.message, error.message);
        assert_eq!(
            response_failure(registry.respond_owned("owner", &params, true)).code,
            INTERACTION_NOT_FOUND
        );

        let snapshot = registry
            .get(
                "owner",
                &GetSessionInteractionsParams {
                    thread_id: "session".into(),
                },
            )
            .unwrap();
        assert!(snapshot.pending.is_empty());
        let replay = registry
            .subscribe(
                "owner",
                &SubscribeInteractionsParams {
                    thread_id: "session".into(),
                    after: start,
                    through: None,
                    limit: 2,
                },
            )
            .unwrap();
        assert_eq!(replay.events.len(), 2);
        assert!(matches!(
            &replay.events[1].payload,
            InteractionEventPayload::Removed { request_id, cause, .. }
                if request_id == "request-typed-dropped"
                    && cause == INTERACTION_REMOVAL_ORIGIN_FINISHED
        ));

        let mut interaction_notifications = Vec::new();
        while interaction_notifications.len() < 2 {
            let notification = tokio::time::timeout(Duration::from_secs(1), output.recv())
                .await
                .expect("Interaction notification timeout")
                .expect("Interaction notification missing");
            if notification["method"] == METHOD_SESSION_INTERACTION_EVENT {
                interaction_notifications.push(notification);
            }
        }
        assert_eq!(interaction_notifications[1]["params"]["type"], "removed");
        assert_eq!(
            interaction_notifications[1]["params"]["cause"],
            INTERACTION_REMOVAL_ORIGIN_FINISHED
        );
        let observable = serde_json::to_string(&json!({
            "snapshot": snapshot,
            "replay": replay,
            "notifications": interaction_notifications,
            "leader_error": error.message,
            "follower_error": follower_error.message,
        }))
        .unwrap();
        assert!(!observable.contains("typed-response-secret-must-not-escape"));
        assert!(!observable.contains("fingerprint"));
        assert!(!observable.contains("digest"));
        assert!(!observable.contains("hmac"));
        assert!(!observable.contains(INTERACTION_REMOVAL_RESOLVED));
    }

    #[tokio::test]
    async fn typed_projection_publication_failure_aborts_response_before_release() {
        let (registry, mut output, _writer) = fixture(InteractionJournalLimits::default(), true);
        register(&registry, "turn-a", 1);
        let context = context("turn-a");
        let request_id = "request-projection";
        let start = registry
            .get(
                "owner",
                &GetSessionInteractionsParams {
                    thread_id: "session".into(),
                },
            )
            .unwrap()
            .cursor;
        let tool_call = whale_protocol::CanonicalItem::tool_call(
            "call-a",
            None,
            "lookup",
            Some(json!({})),
            "{}",
        );
        let ticket = registry
            .begin(
                &context,
                request_id.into(),
                InteractionRequest::new(
                    KIND_TOOL_APPROVAL,
                    "approval",
                    json!({}),
                    Some(json!({
                        "$schema":"https://json-schema.org/draft/2020-12/schema",
                        "type":"object",
                        "properties":{
                            "decision":{"const":"reject"},
                            "feedback":{"type":"string"}
                        },
                        "required":["decision"],
                        "additionalProperties":false
                    })),
                )
                .unwrap(),
                InteractionOrigin::Core,
            )
            .unwrap();
        let params = RespondInteractionParams::new(
            "session",
            "turn-a",
            request_id,
            json!({
                "decision":"reject",
                "feedback":"projection-response-secret-must-not-escape"
            }),
        )
        .unwrap();
        let commit = registry.respond_owned("owner", &params, true).unwrap();
        let follower = registry.respond_owned("owner", &params, true).unwrap();
        let run = Arc::new(super::super::RunRecord {
            owner: "owner".into(),
            params: whale_protocol::runs::StartTurnParams {
                thread_id: "session".into(),
                turn_id: "turn-a".into(),
                input_items: Vec::new(),
                options: None,
                max_steps: 1,
                timeout_ms: None,
            },
            snapshot: tokio::sync::Mutex::new(whale_protocol::runs::RunSnapshot {
                thread_id: "session".into(),
                turn_id: "turn-a".into(),
                status: whale_protocol::runs::RunStatus::WaitingApproval,
                items: Vec::new(),
                usage: Default::default(),
                tool_executions: Vec::new(),
                pending_approvals: vec![whale_protocol::runs::PendingApproval {
                    request_id: request_id.into(),
                    tool_call,
                    reason: None,
                }],
                last_seq: 0,
                result: None,
                error: None,
            }),
            cancel: watch::channel(false).0,
            cancellation: whale_core::CancellationToken::new(),
            context,
            acceptance: watch::channel(Some(true)).0,
            preparation: watch::channel(Some(Ok(()))).0,
            finished: tokio::sync::Notify::new(),
            terminal_delivery: watch::channel(None).0,
            publication: tokio::sync::Mutex::new(Default::default()),
            decisions: tokio::sync::Mutex::new(HashMap::new()),
            interactions: registry.clone(),
            legacy: std::sync::atomic::AtomicBool::new(false),
            legacy_events_finished: std::sync::atomic::AtomicBool::new(false),
            progress: Arc::new(whale_core::engine::RunProgress::default()),
            deadline: None,
            accepted_ordinal: 1,
        });
        let error = finish_response_commit(
            &run,
            &super::super::session_views::SessionViewRegistry::default(),
            &super::super::session_management::SessionManagementRegistry::default(),
            commit,
        )
        .await
        .expect_err("missing typed Session projection must abort response delivery");
        assert_eq!(error.code, INTERACTION_UNAVAILABLE);
        let follower_error = follower
            .complete()
            .await
            .expect_err("equivalent follower must observe the leader failure");
        assert_eq!(follower_error.code, error.code);
        assert_eq!(follower_error.message, error.message);
        let continuation_error = ticket
            .response()
            .await
            .expect_err("failed projection must wake the continuation with failure");
        assert!(matches!(
            continuation_error,
            CoreError::InteractionUnavailable(ref message) if message == &error.message
        ));
        assert_eq!(
            response_failure(registry.respond_owned("owner", &params, true)).code,
            INTERACTION_NOT_FOUND
        );
        assert!(run.snapshot.lock().await.pending_approvals.is_empty());

        let snapshot = registry
            .get(
                "owner",
                &GetSessionInteractionsParams {
                    thread_id: "session".into(),
                },
            )
            .unwrap();
        assert!(snapshot.pending.is_empty());
        let replay = registry
            .subscribe(
                "owner",
                &SubscribeInteractionsParams {
                    thread_id: "session".into(),
                    after: start,
                    through: None,
                    limit: 2,
                },
            )
            .unwrap();
        assert_eq!(replay.events.len(), 2);
        assert!(matches!(
            &replay.events[1].payload,
            InteractionEventPayload::Removed { request_id, cause, .. }
                if request_id == "request-projection"
                    && cause == whale_protocol::interactions::INTERACTION_REMOVAL_PUBLICATION_FAILED
        ));

        let requested_notification = tokio::time::timeout(Duration::from_secs(1), output.recv())
            .await
            .expect("requested notification timeout")
            .expect("requested notification missing");
        let removed_notification = tokio::time::timeout(Duration::from_secs(1), output.recv())
            .await
            .expect("removed notification timeout")
            .expect("removed notification missing");
        assert_eq!(requested_notification["params"]["type"], "requested");
        assert_eq!(removed_notification["params"]["type"], "removed");
        assert_eq!(
            removed_notification["params"]["cause"],
            whale_protocol::interactions::INTERACTION_REMOVAL_PUBLICATION_FAILED
        );

        let observable = serde_json::to_string(&json!({
            "snapshot": snapshot,
            "replay": replay,
            "notifications": [requested_notification, removed_notification],
            "leader_error": error.message,
            "follower_error": follower_error.message,
        }))
        .unwrap();
        assert!(!observable.contains("projection-response-secret-must-not-escape"));
        assert!(!observable.contains("fingerprint"));
        assert!(!observable.contains("digest"));
        assert!(!observable.contains("hmac"));
        assert!(!observable.contains(whale_protocol::interactions::INTERACTION_REMOVAL_RESOLVED));
    }
}
