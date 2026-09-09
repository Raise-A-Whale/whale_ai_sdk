use crate::{Result, RetentionMetadata, SessionRecord, StoreError};
use async_trait::async_trait;
use std::{collections::BTreeMap, sync::Mutex};
#[async_trait]
pub trait SessionStore: Send + Sync {
    fn durable(&self) -> bool;
    async fn create(&self, record: SessionRecord) -> Result<()>;
    async fn load(&self, id: &str) -> Result<Option<SessionRecord>>;
    async fn compare_exchange(
        &self,
        id: &str,
        expected_revision: u64,
        replacement: SessionRecord,
    ) -> Result<()>;
    async fn list(&self) -> Result<Vec<SessionRecord>>;
    /// Ascending recovery IDs, strictly after the cursor. Defaults preserve old
    /// backends; built-ins avoid cloning all payloads for maintenance scans.
    async fn metadata_page(
        &self,
        after_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RetentionMetadata>> {
        let mut records = self.list().await?;
        records.sort_by(|a, b| a.recovery_id.cmp(&b.recovery_id));
        records
            .iter()
            .filter(|r| after_id.is_none_or(|id| r.recovery_id.as_str() > id))
            .take(limit)
            .map(SessionRecord::retention_metadata)
            .collect()
    }
    /// Revision CAS protects the predicate check even for legacy backends.
    async fn retire_detached(
        &self,
        id: &str,
        expected_revision: u64,
        now_ms: u64,
        reason: &str,
    ) -> Result<bool> {
        let old = self.load(id).await?.ok_or(StoreError::NotFound)?;
        let Some(new) =
            crate::retention::retirement_replacement(old, expected_revision, now_ms, reason)?
        else {
            return Ok(false);
        };
        self.compare_exchange(id, expected_revision, new).await?;
        Ok(true)
    }
}
#[derive(Default)]
pub struct MemoryStore {
    records: Mutex<BTreeMap<String, SessionRecord>>,
}
impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}
pub(crate) fn validate_create(record: &SessionRecord) -> Result<()> {
    record.validate()?;
    if record.revision != 1 {
        return Err(StoreError::Invalid("Initial revision must be one".into()));
    }
    Ok(())
}
pub(crate) fn validate_replace(
    old: &SessionRecord,
    id: &str,
    expected: u64,
    new: &SessionRecord,
) -> Result<()> {
    new.validate()?;
    if old.revision != expected {
        return Err(StoreError::Conflict);
    }
    if new.recovery_id != id
        || new.secret_digest != old.secret_digest
        || new.revision != expected.checked_add(1).ok_or(StoreError::Conflict)?
        || new.epoch < old.epoch
        || (old.forgotten && !is_legacy_tombstone_migration(old, new))
    {
        return Err(StoreError::Conflict);
    }
    if old.schema_version == 2
        && (new.schema_version != 2
            || new.created_at_ms != old.created_at_ms
            || new.updated_at_ms < old.updated_at_ms)
    {
        return Err(StoreError::Invalid(
            "Retention timestamps cannot be rewritten".into(),
        ));
    }
    if !new.forgotten {
        if !new.history.starts_with(&old.history)
            || !new
                .historical_thread_ids
                .starts_with(&old.historical_thread_ids)
        {
            return Err(StoreError::Invalid(
                "Committed history and attachment identities are append-only".into(),
            ));
        }
        for (id, run) in &old.runs {
            let replacement = new
                .runs
                .get(id)
                .ok_or_else(|| StoreError::Invalid("Run record was removed".into()))?;
            if replacement.params != run.params
                || replacement.effective_options != run.effective_options
                || replacement.history_start != run.history_start
                || (run.snapshot.status.is_terminal() && replacement.snapshot != run.snapshot)
            {
                return Err(StoreError::Invalid(
                    "Committed run identity/result was changed".into(),
                ));
            }
            for call in &run.calls {
                let updated = replacement
                    .calls
                    .iter()
                    .find(|c| c.call_id == call.call_id)
                    .ok_or_else(|| StoreError::Invalid("Call record was removed".into()))?;
                if updated.result_item_id != call.result_item_id
                    || updated.execution_id != call.execution_id
                    || updated.step_id != call.step_id
                    || updated.tool_name != call.tool_name
                    || call
                        .intent
                        .as_ref()
                        .is_some_and(|i| updated.intent.as_ref() != Some(i))
                    || call
                        .outcome
                        .as_ref()
                        .is_some_and(|i| updated.outcome.as_ref() != Some(i))
                    || (call.committed && !updated.committed)
                {
                    return Err(StoreError::Invalid(
                        "Committed tool outcome/identity was changed".into(),
                    ));
                }
            }
        }
        if new.owner != old.owner || new.thread_id != old.thread_id {
            if new.owner.is_some()
                && (old.owner.is_some()
                    || new.epoch != old.epoch.checked_add(1).ok_or(StoreError::Conflict)?)
            {
                return Err(StoreError::StaleLease);
            }
            if new.owner.is_none() && new.epoch != old.epoch {
                return Err(StoreError::StaleLease);
            }
        } else if new.epoch != old.epoch {
            return Err(StoreError::StaleLease);
        }
    }
    Ok(())
}
#[async_trait]
impl SessionStore for MemoryStore {
    fn durable(&self) -> bool {
        false
    }
    async fn create(&self, record: SessionRecord) -> Result<()> {
        validate_create(&record)?;
        let mut records = self.records.lock().unwrap();
        if records.contains_key(&record.recovery_id) {
            return Err(StoreError::Conflict);
        }
        records.insert(record.recovery_id.clone(), record);
        Ok(())
    }
    async fn load(&self, id: &str) -> Result<Option<SessionRecord>> {
        Ok(self.records.lock().unwrap().get(id).cloned())
    }
    async fn compare_exchange(&self, id: &str, expected: u64, new: SessionRecord) -> Result<()> {
        let mut records = self.records.lock().unwrap();
        let old = records.get(id).ok_or(StoreError::NotFound)?;
        validate_replace(old, id, expected, &new)?;
        records.insert(id.into(), new);
        Ok(())
    }
    async fn metadata_page(
        &self,
        after_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RetentionMetadata>> {
        self.records
            .lock()
            .unwrap()
            .values()
            .filter(|r| after_id.is_none_or(|id| r.recovery_id.as_str() > id))
            .take(limit)
            .map(SessionRecord::retention_metadata)
            .collect()
    }
    async fn list(&self) -> Result<Vec<SessionRecord>> {
        Ok(self.records.lock().unwrap().values().cloned().collect())
    }
}

fn is_legacy_tombstone_migration(old: &SessionRecord, new: &SessionRecord) -> bool {
    if old.schema_version != 1 || new.schema_version != 2 {
        return false;
    }
    let Some(now) = new.created_at_ms else {
        return false;
    };
    let mut expected = old.clone();
    expected.migrate_retention(now);
    expected.revision = new.revision;
    serde_json::to_value(expected).ok() == serde_json::to_value(new).ok()
}
