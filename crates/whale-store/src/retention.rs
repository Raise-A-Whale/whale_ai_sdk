use crate::*;
use whale_protocol::retention::StoreRetentionPolicy;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionMetadata {
    pub recovery_id: String,
    pub revision: u64,
    pub detached_since_ms: Option<u64>,
    pub forgotten: bool,
    pub protected_active: bool,
    pub protected_unknown: bool,
    /// UTF-8 bytes of the complete serialized non-tombstone SessionRecord.
    pub payload_bytes: u64,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RetentionReport {
    pub examined: u64,
    pub retired: u64,
    pub protected_active: u64,
    pub protected_unknown: u64,
    pub remaining_sessions: u64,
    pub remaining_payload_bytes: u64,
    pub unmet_sessions: u64,
    pub unmet_payload_bytes: u64,
}
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
impl SessionRecord {
    pub(crate) fn touch(&mut self, now: u64) {
        self.updated_at_ms = Some(now.max(self.updated_at_ms.unwrap_or(now)));
    }
    pub(crate) fn mark_detached(&mut self, now: u64) {
        self.touch(now);
        self.detached_since_ms = self.updated_at_ms;
    }
    pub(crate) fn migrate_retention(&mut self, now: u64) -> bool {
        if self.schema_version != 1 {
            return false;
        }
        self.schema_version = 2;
        self.created_at_ms = Some(now);
        self.updated_at_ms = Some(now);
        self.detached_since_ms = if self.owner.is_none() && !self.forgotten {
            Some(now)
        } else {
            None
        };
        self.retired_at_ms = self.forgotten.then_some(now);
        self.retirement_reason = self.forgotten.then(|| "legacy_forget".into());
        true
    }
    pub(crate) fn retire(&mut self, now: u64, reason: &str) {
        self.touch(now);
        self.forgotten = true;
        self.retired_at_ms = self.updated_at_ms;
        self.retirement_reason = Some(reason.into());
        self.detached_since_ms = None;
        self.configuration = serde_json::json!({});
        self.history.clear();
        self.runs.clear();
        self.unknown_executions.clear();
        self.historical_thread_ids.clear();
    }
    pub fn retention_metadata(&self) -> Result<RetentionMetadata> {
        Ok(RetentionMetadata {
            recovery_id: self.recovery_id.clone(),
            revision: self.revision,
            detached_since_ms: self.detached_since_ms,
            forgotten: self.forgotten,
            protected_active: self.owner.is_some() || self.active(),
            protected_unknown: self.unresolved(),
            payload_bytes: if self.forgotten {
                0
            } else {
                whale_protocol::retention::serialized_bytes(self)
                    .map_err(|e| StoreError::Invalid(e.to_string()))?
            },
        })
    }
}
pub(crate) fn retirement_replacement(
    mut record: SessionRecord,
    expected: u64,
    now: u64,
    reason: &str,
) -> Result<Option<SessionRecord>> {
    record.validate()?;
    if record.revision != expected {
        return Err(StoreError::Conflict);
    }
    if record.forgotten {
        return Ok(None);
    }
    if record.owner.is_some() || record.active() {
        return Err(StoreError::Active);
    }
    if record.unresolved() {
        return Err(StoreError::UnknownExecutions);
    }
    // Never expire a legacy record before migration establishes its grace period.
    if record.schema_version != 2 || record.detached_since_ms.is_none() {
        return Err(StoreError::Invalid(
            "Retention metadata requires migration".into(),
        ));
    }
    record.retire(now, reason);
    record.revision = expected.checked_add(1).ok_or(StoreError::Conflict)?;
    Ok(Some(record))
}
impl StoreRuntime {
    /// Accepted sweeps are owned and serialized across runtime clones. Dropping
    /// this waiter does not interrupt an accepted backend mutation.
    pub async fn sweep_retention(
        &self,
        policy: &StoreRetentionPolicy,
        now_ms: u64,
    ) -> Result<RetentionReport> {
        policy.validate().map_err(StoreError::Invalid)?;
        let runtime = self.clone();
        let policy = policy.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _guard = runtime.sweep_lock.lock().await;
            let result = runtime.sweep_owned(&policy, now_ms).await;
            let _ = tx.send(result);
        });
        rx.await
            .map_err(|_| StoreError::Io("Retention worker stopped".into()))?
    }
    async fn metadata(&self) -> Result<Vec<RetentionMetadata>> {
        let mut rows = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = self.store.metadata_page(cursor.as_deref(), 128).await?;
            if page.is_empty() {
                break;
            }
            // Enforce progress even for third-party backends.
            for row in page {
                if cursor.as_ref().is_some_and(|old| row.recovery_id <= *old) {
                    return Err(StoreError::Invalid(
                        "Retention metadata pages must increase by ID".into(),
                    ));
                }
                cursor = Some(row.recovery_id.clone());
                if !row.forgotten {
                    rows.push(row);
                }
            }
        }
        Ok(rows)
    }
    async fn sweep_owned(
        &self,
        policy: &StoreRetentionPolicy,
        now: u64,
    ) -> Result<RetentionReport> {
        let mut rows = self.metadata().await?;
        let mut report = RetentionReport::default();
        let mut sessions = 0u64;
        let mut bytes = 0u64;
        for row in &rows {
            if !row.forgotten {
                report.examined += 1;
                sessions += 1;
                bytes = bytes.saturating_add(row.payload_bytes);
            }
        }
        rows.sort_by(|a, b| {
            (a.detached_since_ms, &a.recovery_id).cmp(&(b.detached_since_ms, &b.recovery_id))
        });
        for row in rows {
            if row.forgotten || row.protected_active || row.protected_unknown {
                continue;
            }
            let ttl = policy.detached_ttl_ms.is_some_and(|ttl| {
                row.detached_since_ms
                    .and_then(|since| now.checked_sub(since))
                    .is_some_and(|age| age >= ttl)
            });
            let excess = policy
                .max_retained_sessions
                .is_some_and(|max| sessions > max)
                || policy
                    .max_retained_payload_bytes
                    .is_some_and(|max| bytes > max);
            if !ttl && !excess {
                continue;
            }
            match self
                .store
                .retire_detached(
                    &row.recovery_id,
                    row.revision,
                    now,
                    if ttl {
                        "detached_ttl"
                    } else {
                        "retention_budget"
                    },
                )
                .await
            {
                Ok(true) => {
                    report.retired += 1;
                    sessions = sessions.saturating_sub(1);
                    bytes = bytes.saturating_sub(row.payload_bytes);
                }
                Ok(false)
                | Err(
                    StoreError::Conflict
                    | StoreError::Active
                    | StoreError::UnknownExecutions
                    | StoreError::NotFound
                    | StoreError::Forgotten,
                ) => {}
                Err(error) => return Err(error),
            }
        }
        // Refresh totals after conflicts/attachments; budgets are observations,
        // not hard caps while protected records can continue growing.
        for row in self.metadata().await? {
            if !row.forgotten {
                report.remaining_sessions += 1;
                report.remaining_payload_bytes = report
                    .remaining_payload_bytes
                    .saturating_add(row.payload_bytes);
                report.protected_active += u64::from(row.protected_active);
                report.protected_unknown += u64::from(row.protected_unknown);
            }
        }
        report.unmet_sessions = policy
            .max_retained_sessions
            .map_or(0, |max| report.remaining_sessions.saturating_sub(max));
        report.unmet_payload_bytes = policy
            .max_retained_payload_bytes
            .map_or(0, |max| report.remaining_payload_bytes.saturating_sub(max));
        Ok(report)
    }
}
