use crate::*;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::{collections::HashSet, sync::Arc};
use tokio::sync::{mpsc, oneshot};
use whale_protocol::recovery::{RecoveryKey, RecoverySnapshot};
use whale_protocol::runs::{RunSnapshot, StartTurnParams};
#[derive(Clone)]
pub struct StoreRuntime {
    pub(crate) store: Arc<dyn SessionStore>,
    pub(crate) sweep_lock: Arc<tokio::sync::Mutex<()>>,
}
fn digest(secret: &str) -> String {
    format!("{:x}", Sha256::digest(secret.as_bytes()))
}
fn check_key(record: &SessionRecord, key: &RecoveryKey) -> Result<()> {
    key.validate().map_err(StoreError::Invalid)?;
    let expected = digest(&key.secret);
    let difference = expected
        .bytes()
        .zip(record.secret_digest.bytes())
        .fold(0u8, |n, (a, b)| n | (a ^ b));
    if record.recovery_id != key.recovery_id
        || expected.len() != record.secret_digest.len()
        || difference != 0
    {
        return Err(StoreError::Unauthorized);
    }
    Ok(())
}
fn identity(owner: &str, thread: &str, configuration: &Value) -> Result<()> {
    if owner.trim().is_empty() || thread.trim().is_empty() || !configuration.is_object() {
        return Err(StoreError::Invalid(
            "Invalid attachment/configuration".into(),
        ));
    }
    Ok(())
}

fn parse_versioned_configuration(
    configuration: &Value,
) -> Result<Option<(PersistedSessionConfigurationV2, bool)>> {
    if configuration.get("version").is_none() {
        return Ok(None);
    }
    PersistedSessionConfigurationV2::parse_and_migrate(configuration.clone()).map(Some)
}

fn normalize_new_configuration(configuration: Value) -> Result<Value> {
    match parse_versioned_configuration(&configuration)? {
        Some((configuration, _)) => Ok(configuration.into_value()),
        None => Ok(configuration),
    }
}

fn normalize_configuration_replacement(current: &Value, replacement: Value) -> Result<Value> {
    let replacement = normalize_new_configuration(replacement)?;
    match (
        parse_versioned_configuration(current)?,
        parse_versioned_configuration(&replacement)?,
    ) {
        (Some((current, _)), Some((replacement_configuration, _))) => {
            if current.metadata() != replacement_configuration.metadata() {
                return Err(StoreError::Invalid(
                    "Use replace_metadata to change persisted Session metadata".into(),
                ));
            }
        }
        (Some(_), None) => {
            return Err(StoreError::Invalid(
                "Versioned persisted Session configuration cannot become unversioned".into(),
            ));
        }
        _ => {}
    }
    Ok(replacement)
}
impl StoreRuntime {
    pub async fn open(store: Arc<dyn SessionStore>) -> Result<Self> {
        let now = crate::retention::now_ms();
        for mut record in store.list().await? {
            record.validate()?;
            let revision = record.revision;
            let configuration_migrated = if record.forgotten {
                false
            } else {
                match parse_versioned_configuration(&record.configuration)? {
                    Some((configuration, migrated)) => {
                        if migrated {
                            record.configuration = configuration.into_value();
                        }
                        migrated
                    }
                    None => false,
                }
            };
            let retention_migrated = record.migrate_retention(now);
            let attached = record.owner.is_some();
            let recovered = record.recover()?;
            if attached {
                record.mark_detached(now);
            }
            if configuration_migrated || retention_migrated || recovered {
                record.revision = revision.checked_add(1).ok_or(StoreError::Conflict)?;
                store
                    .compare_exchange(&record.recovery_id.clone(), revision, record)
                    .await?;
            }
        }
        Ok(Self {
            store,
            sweep_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }
    pub fn durable(&self) -> bool {
        self.store.durable()
    }
    pub async fn create(
        &self,
        key: RecoveryKey,
        configuration: Value,
        owner: String,
        thread_id: String,
    ) -> Result<SessionJournal> {
        key.validate().map_err(StoreError::Invalid)?;
        identity(&owner, &thread_id, &configuration)?;
        let configuration = normalize_new_configuration(configuration)?;
        let now = crate::retention::now_ms();
        let record = SessionRecord {
            schema_version: 2,
            created_at_ms: Some(now),
            updated_at_ms: Some(now),
            detached_since_ms: None,
            retired_at_ms: None,
            retirement_reason: None,
            recovery_id: key.recovery_id,
            secret_digest: digest(&key.secret),
            revision: 1,
            epoch: 1,
            owner: Some(owner),
            thread_id: Some(thread_id.clone()),
            historical_thread_ids: vec![thread_id],
            forgotten: false,
            configuration,
            history: vec![],
            runs: Default::default(),
            unknown_executions: vec![],
        };
        self.store.create(record.clone()).await?;
        Ok(SessionJournal::spawn(self.store.clone(), &record))
    }
    async fn authenticated(&self, key: &RecoveryKey) -> Result<SessionRecord> {
        key.validate().map_err(StoreError::Invalid)?;
        let record = self
            .store
            .load(&key.recovery_id)
            .await?
            .ok_or(StoreError::NotFound)?;
        check_key(&record, key)?;
        record.validate()?;
        Ok(record)
    }
    pub async fn inspect(&self, key: &RecoveryKey) -> Result<RecoverySnapshot> {
        let record = self.authenticated(key).await?;
        if record.forgotten {
            return Err(StoreError::Forgotten);
        }
        Ok(record.snapshot())
    }
    pub async fn attach(
        &self,
        key: &RecoveryKey,
        expected_revision: u64,
        configuration: Value,
        owner: String,
        fresh_thread_id: String,
    ) -> Result<SessionJournal> {
        identity(&owner, &fresh_thread_id, &configuration)?;
        let mut record = self.authenticated(key).await?;
        if record.forgotten {
            return Err(StoreError::Forgotten);
        }
        if record.owner.is_some() {
            return Err(StoreError::Active);
        }
        if record.revision != expected_revision {
            return Err(StoreError::Conflict);
        }
        let stored_configuration = parse_versioned_configuration(&record.configuration)?;
        let caller_configuration = parse_versioned_configuration(&configuration)?;
        let configuration_matches = match (&stored_configuration, &caller_configuration) {
            (Some((stored, _)), Some((caller, _))) => {
                stored.attachment_identity() == caller.attachment_identity()
                    && stored.interactions_enabled() == caller.interactions_enabled()
            }
            (None, None) => record.configuration == configuration,
            _ => false,
        };
        if !configuration_matches || record.historical_thread_ids.contains(&fresh_thread_id) {
            return Err(StoreError::Invalid(
                "Attachment configuration or fresh identity mismatch".into(),
            ));
        }
        if let Some((configuration, migrated)) = stored_configuration {
            if migrated {
                record.configuration = configuration.into_value();
            }
        }
        record.touch(crate::retention::now_ms());
        record.detached_since_ms = None;
        record.owner = Some(owner);
        record.thread_id = Some(fresh_thread_id.clone());
        record.historical_thread_ids.push(fresh_thread_id);
        record.epoch = record.epoch.checked_add(1).ok_or(StoreError::Conflict)?;
        record.revision = record.revision.checked_add(1).ok_or(StoreError::Conflict)?;
        self.store
            .compare_exchange(&key.recovery_id, expected_revision, record.clone())
            .await?;
        Ok(SessionJournal::spawn(self.store.clone(), &record))
    }
    pub async fn forget(&self, key: &RecoveryKey, expected_revision: u64) -> Result<bool> {
        let mut record = self.authenticated(key).await?;
        if record.forgotten {
            return Ok(false);
        }
        if record.owner.is_some() {
            return Err(StoreError::Active);
        }
        if record.revision != expected_revision {
            return Err(StoreError::Conflict);
        }
        record.retire(crate::retention::now_ms(), "explicit_forget");
        record.revision = record.revision.checked_add(1).ok_or(StoreError::Conflict)?;
        self.store
            .compare_exchange(&key.recovery_id, expected_revision, record)
            .await?;
        Ok(true)
    }
}
#[derive(Clone)]
pub struct SessionJournal {
    inner: Arc<JournalHandle>,
}
struct JournalHandle {
    id: String,
    epoch: u64,
    sender: mpsc::UnboundedSender<Command>,
}
struct Command {
    action: Action,
    reply: oneshot::Sender<Result<Response>>,
}
enum Action {
    Record,
    Begin(StartTurnParams, RunSnapshot, Value),
    Commit(String, RunMutation),
    Finalize(String, RunSnapshot),
    Replace(Value),
    ReplaceMetadata(Map<String, Value>),
    Acknowledge(u64, Vec<String>),
    Detach,
}
enum Response {
    Record(SessionRecord),
    Bool(bool),
    Receipt(CommitReceipt),
    Finalized(FinalizedRun),
    Snapshot(RecoverySnapshot),
    Metadata(Map<String, Value>),
    Unit,
}
impl SessionJournal {
    fn spawn(store: Arc<dyn SessionStore>, record: &SessionRecord) -> Self {
        let (sender, mut receiver) = mpsc::unbounded_channel::<Command>();
        let id = record.recovery_id.clone();
        let owner = record.owner.clone();
        let thread = record.thread_id.clone();
        let epoch = record.epoch;
        let worker_id = id.clone();
        tokio::spawn(async move {
            let mut poison: Option<String> = None;
            while let Some(command) = receiver.recv().await {
                if let Some(error) = &poison {
                    let _ = command.reply.send(Err(StoreError::Poisoned(error.clone())));
                    continue;
                }
                let loaded = store.load(&worker_id).await;
                let mut record = match loaded {
                    Ok(Some(record)) => record,
                    Ok(None) => {
                        poison = Some("Record disappeared".into());
                        let _ = command.reply.send(Err(StoreError::NotFound));
                        continue;
                    }
                    Err(error) => {
                        poison = Some(error.to_string());
                        let _ = command.reply.send(Err(error));
                        continue;
                    }
                };
                if record.epoch != epoch
                    || record.owner != owner
                    || record.thread_id != thread
                    || record.owner.is_none()
                    || record.forgotten
                {
                    let _ = command.reply.send(Err(StoreError::StaleLease));
                    continue;
                }
                if matches!(command.action, Action::Record) {
                    let _ = command.reply.send(Ok(Response::Record(record)));
                    continue;
                }
                let revision = record.revision;
                let response = apply(&mut record, command.action);
                let response = match response {
                    Ok(response) => response,
                    Err(error) => {
                        let _ = command.reply.send(Err(error));
                        continue;
                    }
                };
                let Some(next) = revision.checked_add(1) else {
                    let _ = command.reply.send(Err(StoreError::Conflict));
                    continue;
                };
                record.touch(crate::retention::now_ms());
                record.revision = next;
                match store
                    .compare_exchange(&worker_id, revision, record.clone())
                    .await
                {
                    Ok(()) => {
                        let response = match response {
                            Response::Receipt(mut r) => {
                                r.revision = next;
                                Response::Receipt(r)
                            }
                            Response::Snapshot(_) => Response::Snapshot(record.snapshot()),
                            other => other,
                        };
                        let _ = command.reply.send(Ok(response));
                    }
                    Err(error) => {
                        poison = Some(error.to_string());
                        // A backend CAS failure is not the same as rejecting an
                        // application's stale expected_revision before mutation.
                        let failure = match error {
                            StoreError::Io(_) | StoreError::Poisoned(_) => error,
                            other => StoreError::Poisoned(other.to_string()),
                        };
                        let _ = command.reply.send(Err(failure));
                    }
                }
            }
        });
        Self {
            inner: Arc::new(JournalHandle { id, epoch, sender }),
        }
    }
    pub fn recovery_id(&self) -> &str {
        &self.inner.id
    }
    pub fn epoch(&self) -> u64 {
        self.inner.epoch
    }
    async fn send(&self, action: Action) -> Result<Response> {
        let (reply, receive) = oneshot::channel();
        self.inner
            .sender
            .send(Command { action, reply })
            .map_err(|_| StoreError::Poisoned("Journal worker stopped".into()))?;
        receive
            .await
            .map_err(|_| StoreError::Poisoned("Journal worker stopped".into()))?
    }
    pub async fn record(&self) -> Result<SessionRecord> {
        match self.send(Action::Record).await? {
            Response::Record(r) => Ok(r),
            _ => unreachable!(),
        }
    }
    pub async fn begin_run(
        &self,
        params: StartTurnParams,
        snapshot: RunSnapshot,
        effective_options: Value,
    ) -> Result<bool> {
        match self
            .send(Action::Begin(params, snapshot, effective_options))
            .await?
        {
            Response::Bool(r) => Ok(r),
            _ => unreachable!(),
        }
    }
    pub async fn commit(&self, turn_id: &str, mutation: RunMutation) -> Result<CommitReceipt> {
        match self.send(Action::Commit(turn_id.into(), mutation)).await? {
            Response::Receipt(r) => Ok(r),
            _ => unreachable!(),
        }
    }
    pub async fn finalize(&self, turn_id: &str, candidate: RunSnapshot) -> Result<FinalizedRun> {
        match self
            .send(Action::Finalize(turn_id.into(), candidate))
            .await?
        {
            Response::Finalized(r) => Ok(r),
            _ => unreachable!(),
        }
    }
    pub async fn replace_configuration(&self, configuration: Value) -> Result<()> {
        self.send(Action::Replace(configuration)).await?;
        Ok(())
    }
    pub async fn replace_metadata(
        &self,
        metadata: Map<String, Value>,
    ) -> Result<Map<String, Value>> {
        match self.send(Action::ReplaceMetadata(metadata)).await? {
            Response::Metadata(metadata) => Ok(metadata),
            _ => unreachable!(),
        }
    }
    pub async fn acknowledge(
        &self,
        expected_revision: u64,
        execution_ids: Vec<String>,
    ) -> Result<RecoverySnapshot> {
        match self
            .send(Action::Acknowledge(expected_revision, execution_ids))
            .await?
        {
            Response::Snapshot(r) => Ok(r),
            _ => unreachable!(),
        }
    }
    pub async fn detach(&self) -> Result<()> {
        self.send(Action::Detach).await?;
        Ok(())
    }
}
fn apply(record: &mut SessionRecord, action: Action) -> Result<Response> {
    match action {
        Action::Record => unreachable!(),
        Action::Begin(params, snapshot, options) => {
            Ok(Response::Bool(record.begin(params, snapshot, options)?))
        }
        Action::Commit(turn, mutation) => Ok(Response::Receipt(CommitReceipt {
            revision: record.revision,
            result_item: record.mutate(&turn, mutation)?,
        })),
        Action::Finalize(turn, candidate) => {
            Ok(Response::Finalized(record.finalize(&turn, candidate)?))
        }
        Action::Replace(configuration) => {
            if record.active() {
                return Err(StoreError::Active);
            }
            if !configuration.is_object() {
                return Err(StoreError::Invalid(
                    "Configuration must be an object".into(),
                ));
            }
            record.configuration =
                normalize_configuration_replacement(&record.configuration, configuration)?;
            Ok(Response::Unit)
        }
        Action::ReplaceMetadata(metadata) => {
            let (mut configuration, _) =
                PersistedSessionConfigurationV2::parse_and_migrate(record.configuration.clone())?;
            configuration.replace_metadata(metadata.clone())?;
            record.configuration = configuration.into_value();
            Ok(Response::Metadata(metadata))
        }
        Action::Acknowledge(expected, ids) => {
            if record.revision != expected {
                return Err(StoreError::Conflict);
            }
            if record.active() {
                return Err(StoreError::Active);
            }
            let supplied = ids.iter().cloned().collect::<HashSet<_>>();
            let required = record
                .unknown_executions
                .iter()
                .filter(|u| !u.acknowledged)
                .map(|u| u.execution_id.clone())
                .collect::<HashSet<_>>();
            if supplied.len() != ids.len() || supplied != required {
                return Err(StoreError::Invalid(
                    "Acknowledgment must name the exact unresolved execution set".into(),
                ));
            }
            for unknown in &mut record.unknown_executions {
                unknown.acknowledged = true;
            }
            Ok(Response::Snapshot(record.snapshot()))
        }
        Action::Detach => {
            record.recover()?;
            record.mark_detached(crate::retention::now_ms());
            Ok(Response::Unit)
        }
    }
}
