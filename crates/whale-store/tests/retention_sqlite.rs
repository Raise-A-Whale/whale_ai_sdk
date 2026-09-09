#![cfg(feature = "sqlite")]
use serde_json::json;
use std::sync::Arc;
use whale_protocol::{recovery::RecoveryKey, retention::StoreRetentionPolicy};
use whale_store::*;
async fn legacy_record(forgotten: bool) -> (RecoveryKey, serde_json::Value) {
    let b = Arc::new(MemoryStore::new());
    let rt = StoreRuntime::open(b.clone()).await.unwrap();
    let key = RecoveryKey::new();
    let j = rt
        .create(
            key.clone(),
            json!({"original":"旧记录数据".repeat(100)}),
            "owner".into(),
            "live".into(),
        )
        .await
        .unwrap();
    j.detach().await.unwrap();
    if forgotten {
        let r = rt.inspect(&key).await.unwrap();
        rt.forget(&key, r.revision).await.unwrap();
    }
    let mut value = serde_json::to_value(b.load(&key.recovery_id).await.unwrap().unwrap()).unwrap();
    value["schema_version"] = json!(1);
    value["revision"] = json!(1);
    for name in [
        "created_at_ms",
        "updated_at_ms",
        "detached_since_ms",
        "retired_at_ms",
        "retirement_reason",
    ] {
        value.as_object_mut().unwrap().remove(name);
    }
    (key, value)
}
#[tokio::test]
async fn actual_v1_sqlite_migration_reopen_and_payload_retirement() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.sqlite");
    let (key, old) = legacy_record(false).await;
    let (dead, forgot) = legacy_record(true).await;
    {
        let db = rusqlite::Connection::open(&path).unwrap();
        db.execute_batch("CREATE TABLE sessions (id TEXT PRIMARY KEY NOT NULL, revision INTEGER NOT NULL, record TEXT NOT NULL)").unwrap();
        for (k, v) in [(&key, old.clone()), (&dead, forgot)] {
            db.execute(
                "INSERT INTO sessions VALUES(?1,1,?2)",
                rusqlite::params![k.recovery_id, serde_json::to_string(&v).unwrap()],
            )
            .unwrap();
        }
    }
    let first;
    {
        let b = Arc::new(SQLiteStore::open(&path).unwrap());
        let rt = StoreRuntime::open(b.clone()).await.unwrap();
        first = b.load(&key.recovery_id).await.unwrap().unwrap();
        assert_eq!(first.schema_version, 2);
        assert_eq!(first.configuration, old["configuration"]);
        assert!(matches!(
            rt.inspect(&dead).await,
            Err(StoreError::Forgotten)
        ));
        assert_eq!(
            b.load(&dead.recovery_id)
                .await
                .unwrap()
                .unwrap()
                .schema_version,
            2
        );
        let pages = b.metadata_page(None, 1).await.unwrap();
        assert_eq!(pages.len(), 1);
        assert_eq!(
            b.metadata_page(Some(&pages[0].recovery_id), 1)
                .await
                .unwrap()
                .len(),
            1
        );
    }
    let b = Arc::new(SQLiteStore::open(&path).unwrap());
    let rt = StoreRuntime::open(b.clone()).await.unwrap();
    let reopened = b.load(&key.recovery_id).await.unwrap().unwrap();
    assert_eq!(reopened.detached_since_ms, first.detached_since_ms);
    assert_eq!(reopened.revision, first.revision);
    let rows = b.metadata_page(None, 10).await.unwrap();
    let size = rows
        .iter()
        .find(|r| r.recovery_id == key.recovery_id)
        .unwrap()
        .payload_bytes;
    assert_eq!(size, serde_json::to_vec(&reopened).unwrap().len() as u64);
    assert!(size > serde_json::to_string(&reopened).unwrap().chars().count() as u64);
    let policy = StoreRetentionPolicy {
        detached_ttl_ms: Some(100),
        ..Default::default()
    };
    let time = first.detached_since_ms.unwrap();
    assert_eq!(
        rt.sweep_retention(&policy, time + 99)
            .await
            .unwrap()
            .retired,
        0
    );
    assert_eq!(
        rt.sweep_retention(&policy, time + 100)
            .await
            .unwrap()
            .retired,
        1
    );
    let record = b.load(&key.recovery_id).await.unwrap().unwrap();
    assert!(record.forgotten);
    assert_eq!(record.configuration, json!({}));
    assert!(rt
        .create(key.clone(), json!({}), "owner".into(), "fresh".into())
        .await
        .is_err());
    let db = rusqlite::Connection::open(&path).unwrap();
    let text: String = db
        .query_row(
            "SELECT record FROM sessions WHERE id=?1",
            [key.recovery_id],
            |r| r.get(0),
        )
        .unwrap();
    assert!(!text.contains("旧记录数据"));
    assert!(text.len() < size as usize);
}
#[tokio::test]
async fn sqlite_retirement_failure_is_reported_and_does_not_erase_payload() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let b = Arc::new(SQLiteStore::open(&path).unwrap());
    let rt = StoreRuntime::open(b.clone()).await.unwrap();
    let key = RecoveryKey::new();
    let j = rt
        .create(
            key.clone(),
            json!({"data":"retain me"}),
            "owner".into(),
            "live".into(),
        )
        .await
        .unwrap();
    j.detach().await.unwrap();
    let original = b.load(&key.recovery_id).await.unwrap().unwrap();
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("CREATE TRIGGER fail_retention BEFORE UPDATE ON sessions BEGIN SELECT RAISE(ABORT,'disk fault'); END;").unwrap();
    let policy = StoreRetentionPolicy {
        detached_ttl_ms: Some(1),
        ..Default::default()
    };
    assert!(matches!(
        rt.sweep_retention(&policy, u64::MAX).await,
        Err(StoreError::Io(_))
    ));
    assert_eq!(rt.inspect(&key).await.unwrap().revision, original.revision);
    assert_eq!(
        rt.inspect(&key).await.unwrap().configuration,
        original.configuration
    );
    db.execute_batch("DROP TRIGGER fail_retention").unwrap();
    assert_eq!(
        rt.sweep_retention(&policy, u64::MAX).await.unwrap().retired,
        1
    );
}
#[tokio::test]
async fn sqlite_stale_candidate_cannot_retire_attached_record() {
    let dir = tempfile::tempdir().unwrap();
    let b = Arc::new(SQLiteStore::open(dir.path().join("state.sqlite")).unwrap());
    let rt = StoreRuntime::open(b.clone()).await.unwrap();
    let key = RecoveryKey::new();
    let j = rt
        .create(key.clone(), json!({}), "owner".into(), "live".into())
        .await
        .unwrap();
    j.detach().await.unwrap();
    let candidate = b.metadata_page(None, 1).await.unwrap().remove(0);
    let _fresh = rt
        .attach(
            &key,
            candidate.revision,
            json!({}),
            "other".into(),
            "fresh".into(),
        )
        .await
        .unwrap();
    assert!(matches!(
        b.retire_detached(&key.recovery_id, candidate.revision, u64::MAX, "test")
            .await,
        Err(StoreError::Conflict)
    ));
    let current = rt.inspect(&key).await.unwrap();
    assert!(matches!(
        b.retire_detached(&key.recovery_id, current.revision, u64::MAX, "test")
            .await,
        Err(StoreError::Active)
    ));
    assert!(rt.inspect(&key).await.unwrap().attached);
}

#[tokio::test]
async fn sqlite_metadata_protects_unknown_and_active_even_under_byte_pressure() {
    use whale_protocol::{
        contexts::ToolExecutionRecord,
        runs::{RunSnapshot, RunStatus, StartTurnParams},
        CanonicalItem,
    };
    let dir = tempfile::tempdir().unwrap();
    let b = Arc::new(SQLiteStore::open(dir.path().join("protected.sqlite")).unwrap());
    let rt = StoreRuntime::open(b.clone()).await.unwrap();
    let unknown = RecoveryKey::new();
    let j = rt
        .create(unknown.clone(), json!({}), "owner".into(), "live".into())
        .await
        .unwrap();
    j.begin_run(
        StartTurnParams {
            thread_id: "live".into(),
            turn_id: "turn".into(),
            input_items: vec![],
            options: None,
            max_steps: 2,
            timeout_ms: None,
        },
        RunSnapshot {
            thread_id: "live".into(),
            turn_id: "turn".into(),
            status: RunStatus::Running,
            items: vec![],
            usage: Default::default(),
            pending_approvals: vec![],
            tool_executions: vec![],
            last_seq: 0,
            result: None,
            error: None,
        },
        json!({}),
    )
    .await
    .unwrap();
    j.commit(
        "turn",
        RunMutation::ModelInput {
            step_id: "step".into(),
            step_index: 0,
            request: json!({}),
        },
    )
    .await
    .unwrap();
    j.commit(
        "turn",
        RunMutation::ModelItem {
            step_id: "step".into(),
            item: CanonicalItem::tool_call("call", None, "tool", Some(json!({})), "{}"),
        },
    )
    .await
    .unwrap();
    j.commit(
        "turn",
        RunMutation::ModelStepFinished {
            step_id: "step".into(),
            usage: Default::default(),
        },
    )
    .await
    .unwrap();
    j.commit(
        "turn",
        RunMutation::DispatchIntent {
            call_id: "call".into(),
            execution: ToolExecutionRecord {
                call_id: "call".into(),
                original_arguments: json!({}),
                arguments: json!({}),
            },
        },
    )
    .await
    .unwrap();
    j.detach().await.unwrap();
    let active = RecoveryKey::new();
    let _live = rt
        .create(active.clone(), json!({}), "owner".into(), "other".into())
        .await
        .unwrap();
    let report = rt
        .sweep_retention(
            &StoreRetentionPolicy {
                detached_ttl_ms: Some(1),
                max_retained_sessions: Some(1),
                max_retained_payload_bytes: Some(1),
            },
            u64::MAX,
        )
        .await
        .unwrap();
    assert_eq!(report.retired, 0);
    assert_eq!(report.protected_active, 1);
    assert_eq!(report.protected_unknown, 1);
    assert_eq!(report.unmet_sessions, 1);
    assert!(report.unmet_payload_bytes > 0);
    assert_eq!(
        rt.inspect(&unknown).await.unwrap().unknown_executions.len(),
        1
    );
    assert!(rt.inspect(&active).await.unwrap().attached);
}

struct ReplyPause {
    inner: SQLiteStore,
    committed: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}
#[async_trait::async_trait]
impl SessionStore for ReplyPause {
    fn durable(&self) -> bool {
        true
    }
    async fn create(&self, r: SessionRecord) -> Result<()> {
        self.inner.create(r).await
    }
    async fn load(&self, id: &str) -> Result<Option<SessionRecord>> {
        self.inner.load(id).await
    }
    async fn list(&self) -> Result<Vec<SessionRecord>> {
        self.inner.list().await
    }
    async fn compare_exchange(&self, id: &str, rev: u64, r: SessionRecord) -> Result<()> {
        self.inner.compare_exchange(id, rev, r).await
    }
    async fn metadata_page(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RetentionMetadata>> {
        self.inner.metadata_page(after, limit).await
    }
    async fn retire_detached(&self, id: &str, rev: u64, now: u64, reason: &str) -> Result<bool> {
        let result = self.inner.retire_detached(id, rev, now, reason).await?;
        self.committed.notify_one();
        self.release.acquire().await.unwrap().forget();
        Ok(result)
    }
}
#[tokio::test]
async fn cancelling_waiter_after_real_sqlite_commit_keeps_owned_sweep_until_reply() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("owned.sqlite");
    let b = Arc::new(ReplyPause {
        inner: SQLiteStore::open(&path).unwrap(),
        committed: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
    });
    let rt = StoreRuntime::open(b.clone()).await.unwrap();
    let key = RecoveryKey::new();
    let j = rt
        .create(
            key.clone(),
            json!({"data":"original"}),
            "owner".into(),
            "live".into(),
        )
        .await
        .unwrap();
    j.detach().await.unwrap();
    let r = rt.clone();
    let policy = StoreRetentionPolicy {
        detached_ttl_ms: Some(1),
        ..Default::default()
    };
    let p = policy.clone();
    let task = tokio::spawn(async move { r.sweep_retention(&p, u64::MAX).await });
    b.committed.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(matches!(rt.inspect(&key).await, Err(StoreError::Forgotten)));
    b.release.add_permits(1);
    assert_eq!(
        rt.sweep_retention(&policy, u64::MAX)
            .await
            .unwrap()
            .remaining_sessions,
        0
    );
    let db = rusqlite::Connection::open(&path).unwrap();
    let text: String = db
        .query_row(
            "SELECT record FROM sessions WHERE id=?1",
            [key.recovery_id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!text.contains("original"));
    assert_eq!(
        serde_json::from_str::<SessionRecord>(&text)
            .unwrap()
            .retirement_reason
            .as_deref(),
        Some("detached_ttl")
    );
}
