use async_trait::async_trait;
use serde_json::{json, Map, Value};
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};
use whale_protocol::{
    contexts::ToolExecutionRecord,
    recovery::RecoveryKey,
    runs::{RunSnapshot, RunStatus, StartTurnParams},
    session_management::MAX_SESSION_METADATA_BYTES,
    CanonicalItem,
};
use whale_store::{
    MemoryStore, PersistedSessionConfigurationV2, RunMutation, SessionRecord, SessionStore,
    StoreError, StoreRuntime,
};

fn metadata(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

fn configuration_v1(model: &str, metadata: Value) -> Value {
    json!({
        "version": 1,
        "session": {
            "model": model,
            "tools": [],
            "limits": {"max_accepted_turns": 8},
            "extension": {"keep": [1, 2, 3]},
            "metadata": metadata
        },
        "run_defaults": {"max_steps": 3, "timeout_ms": 100}
    })
}

fn configuration_v2(model: &str, metadata: Value) -> Value {
    json!({
        "version": 2,
        "session": {
            "model": model,
            "tools": [],
            "limits": {"max_accepted_turns": 8},
            "extension": {"keep": [1, 2, 3]}
        },
        "run_defaults": {"max_steps": 3, "timeout_ms": 100},
        "metadata": metadata
    })
}

fn configuration_v3(model: &str, metadata: Value, interactions_enabled: bool) -> Value {
    json!({
        "version": 3,
        "session": {
            "model": model,
            "tools": [],
            "limits": {"max_accepted_turns": 8},
            "extension": {"keep": [1, 2, 3]}
        },
        "run_defaults": {"max_steps": 3, "timeout_ms": 100},
        "metadata": metadata,
        "runtime_features": {"interactions_enabled": interactions_enabled}
    })
}

fn run_params() -> StartTurnParams {
    StartTurnParams {
        thread_id: "thread-a".into(),
        turn_id: "turn-a".into(),
        input_items: vec![CanonicalItem::user_text("remember")],
        options: None,
        max_steps: 3,
        timeout_ms: Some(100),
    }
}

fn run_snapshot() -> RunSnapshot {
    RunSnapshot {
        thread_id: "thread-a".into(),
        turn_id: "turn-a".into(),
        status: RunStatus::Running,
        items: Vec::new(),
        usage: Default::default(),
        tool_executions: Vec::new(),
        pending_approvals: Vec::new(),
        last_seq: 0,
        result: None,
        error: None,
    }
}

async fn begin_unknown_run(journal: &whale_store::SessionJournal) {
    journal
        .begin_run(run_params(), run_snapshot(), json!({"model":"model-a"}))
        .await
        .unwrap();
    journal
        .commit(
            "turn-a",
            RunMutation::ModelInput {
                step_id: "step-a".into(),
                step_index: 0,
                request: json!({"model_context":{"items":[]}}),
            },
        )
        .await
        .unwrap();
    journal
        .commit(
            "turn-a",
            RunMutation::ModelItem {
                step_id: "step-a".into(),
                item: CanonicalItem::tool_call(
                    "call-a",
                    None,
                    "lookup",
                    Some(json!({"query":"whale"})),
                    "{\"query\":\"whale\"}",
                ),
            },
        )
        .await
        .unwrap();
    journal
        .commit(
            "turn-a",
            RunMutation::ModelStepFinished {
                step_id: "step-a".into(),
                usage: Default::default(),
            },
        )
        .await
        .unwrap();
    journal
        .commit(
            "turn-a",
            RunMutation::DispatchIntent {
                call_id: "call-a".into(),
                execution: ToolExecutionRecord {
                    call_id: "call-a".into(),
                    original_arguments: json!({"query":"whale"}),
                    arguments: json!({"query":"whale"}),
                },
            },
        )
        .await
        .unwrap();
}

async fn rewrite_configuration(
    store: &Arc<dyn SessionStore>,
    key: &RecoveryKey,
    configuration: Value,
) -> SessionRecord {
    let mut record = store.load(&key.recovery_id).await.unwrap().unwrap();
    let revision = record.revision;
    record.configuration = configuration;
    record.revision += 1;
    store
        .compare_exchange(&key.recovery_id, revision, record.clone())
        .await
        .unwrap();
    record
}

async fn seed_detached(store: Arc<dyn SessionStore>) -> (RecoveryKey, SessionRecord) {
    let runtime = StoreRuntime::open(store.clone()).await.unwrap();
    let key = RecoveryKey::new();
    let journal = runtime
        .create(
            key.clone(),
            configuration_v2("model-a", json!({"label":"initial"})),
            "owner-a".into(),
            "thread-a".into(),
        )
        .await
        .unwrap();
    journal.detach().await.unwrap();
    let record = store.load(&key.recovery_id).await.unwrap().unwrap();
    (key, record)
}

#[tokio::test]
async fn opens_and_migrates_v1_configuration_exactly_once() {
    let store: Arc<dyn SessionStore> = Arc::new(MemoryStore::new());
    let runtime = StoreRuntime::open(store.clone()).await.unwrap();
    let key = RecoveryKey::new();
    let journal = runtime
        .create(
            key.clone(),
            configuration_v2("model-a", json!({"old":true})),
            "owner-a".into(),
            "thread-a".into(),
        )
        .await
        .unwrap();
    begin_unknown_run(&journal).await;
    journal.detach().await.unwrap();
    let before = rewrite_configuration(
        &store,
        &key,
        configuration_v1("model-a", json!({"nested":{"value":7}})),
    )
    .await;
    assert!(!before.history.is_empty());
    assert!(!before.runs.is_empty());
    assert!(!before.unknown_executions.is_empty());

    StoreRuntime::open(store.clone()).await.unwrap();
    let migrated = store.load(&key.recovery_id).await.unwrap().unwrap();
    let mut expected = before.clone();
    expected.revision += 1;
    expected.configuration = configuration_v3("model-a", json!({"nested":{"value":7}}), false);
    assert_eq!(
        serde_json::to_value(&migrated).unwrap(),
        serde_json::to_value(&expected).unwrap()
    );
    assert_eq!(migrated.schema_version, before.schema_version);

    StoreRuntime::open(store.clone()).await.unwrap();
    assert_eq!(
        serde_json::to_value(store.load(&key.recovery_id).await.unwrap().unwrap()).unwrap(),
        serde_json::to_value(migrated).unwrap()
    );
}

#[tokio::test]
async fn migration_and_interrupted_recovery_share_one_backend_cas() {
    let store: Arc<dyn SessionStore> = Arc::new(MemoryStore::new());
    let runtime = StoreRuntime::open(store.clone()).await.unwrap();
    let key = RecoveryKey::new();
    let journal = runtime
        .create(
            key.clone(),
            configuration_v2("model-a", json!({"label":"durable"})),
            "owner-a".into(),
            "thread-a".into(),
        )
        .await
        .unwrap();
    begin_unknown_run(&journal).await;
    let before = rewrite_configuration(
        &store,
        &key,
        configuration_v1("model-a", json!({"label":"durable"})),
    )
    .await;
    assert!(before.owner.is_some());

    StoreRuntime::open(store.clone()).await.unwrap();
    let after = store.load(&key.recovery_id).await.unwrap().unwrap();
    assert_eq!(after.revision, before.revision + 1);
    assert!(after.owner.is_none());
    assert_eq!(after.runs["turn-a"].snapshot.status, RunStatus::Failed);
    assert_eq!(after.unknown_executions.len(), 1);
    assert_eq!(after.configuration["version"], 3);
    assert_eq!(
        after.configuration["runtime_features"]["interactions_enabled"],
        false
    );
    assert_eq!(after.configuration["metadata"]["label"], "durable");
}

#[tokio::test]
async fn absent_v1_metadata_defaults_empty_and_invalid_versions_never_rewrite() {
    let store: Arc<dyn SessionStore> = Arc::new(MemoryStore::new());
    let (key, _) = seed_detached(store.clone()).await;
    let mut absent = configuration_v1("model-a", json!({}));
    absent["session"]
        .as_object_mut()
        .unwrap()
        .remove("metadata");
    let before = rewrite_configuration(&store, &key, absent).await;
    StoreRuntime::open(store.clone()).await.unwrap();
    let after = store.load(&key.recovery_id).await.unwrap().unwrap();
    assert_eq!(after.revision, before.revision + 1);
    assert_eq!(after.configuration["metadata"], json!({}));

    for malformed in [
        json!({"version":99,"session":{},"run_defaults":{},"metadata":{}}),
        json!({"version":2,"session":{},"run_defaults":{},"metadata":"bad"}),
        json!({"version":1,"session":{"metadata":"bad"},"run_defaults":{}}),
    ] {
        let invalid_store: Arc<dyn SessionStore> = Arc::new(MemoryStore::new());
        let (invalid_key, _) = seed_detached(invalid_store.clone()).await;
        let invalid_before = rewrite_configuration(&invalid_store, &invalid_key, malformed).await;
        assert!(matches!(
            StoreRuntime::open(invalid_store.clone()).await,
            Err(StoreError::Invalid(_))
        ));
        assert_eq!(
            serde_json::to_value(
                invalid_store
                    .load(&invalid_key.recovery_id)
                    .await
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            serde_json::to_value(invalid_before).unwrap()
        );
    }
}

#[tokio::test]
async fn forgotten_v1_and_v2_tombstones_skip_configuration_migration() {
    let v2_store: Arc<dyn SessionStore> = Arc::new(MemoryStore::new());
    let runtime = StoreRuntime::open(v2_store.clone()).await.unwrap();
    let key = RecoveryKey::new();
    let journal = runtime
        .create(
            key.clone(),
            configuration_v2("model-a", json!({"secret":"retire-me"})),
            "owner-a".into(),
            "thread-a".into(),
        )
        .await
        .unwrap();
    journal.detach().await.unwrap();
    let revision = runtime.inspect(&key).await.unwrap().revision;
    assert!(runtime.forget(&key, revision).await.unwrap());
    let v2_tombstone = v2_store.load(&key.recovery_id).await.unwrap().unwrap();
    assert!(v2_tombstone.forgotten);
    assert_eq!(v2_tombstone.configuration, json!({}));
    StoreRuntime::open(v2_store.clone()).await.unwrap();
    assert_eq!(
        serde_json::to_value(v2_store.load(&key.recovery_id).await.unwrap().unwrap()).unwrap(),
        serde_json::to_value(&v2_tombstone).unwrap()
    );

    let mut v1_tombstone = v2_tombstone;
    v1_tombstone.schema_version = 1;
    v1_tombstone.revision = 1;
    v1_tombstone.created_at_ms = None;
    v1_tombstone.updated_at_ms = None;
    v1_tombstone.detached_since_ms = None;
    v1_tombstone.retired_at_ms = None;
    v1_tombstone.retirement_reason = None;
    let v1_store: Arc<dyn SessionStore> = Arc::new(MemoryStore::new());
    v1_store.create(v1_tombstone).await.unwrap();
    StoreRuntime::open(v1_store.clone()).await.unwrap();
    let migrated = v1_store.load(&key.recovery_id).await.unwrap().unwrap();
    assert_eq!(migrated.schema_version, 2);
    assert_eq!(migrated.revision, 2);
    assert!(migrated.forgotten);
    assert_eq!(migrated.configuration, json!({}));
}

#[tokio::test]
async fn create_attach_and_metadata_replacement_keep_identity_and_store_revision_separate() {
    let store: Arc<dyn SessionStore> = Arc::new(MemoryStore::new());
    let runtime = StoreRuntime::open(store.clone()).await.unwrap();
    let key = RecoveryKey::new();
    let journal = runtime
        .create(
            key.clone(),
            configuration_v1("model-a", json!({"label":"stored"})),
            "owner-a".into(),
            "thread-a".into(),
        )
        .await
        .unwrap();
    let created = journal.record().await.unwrap();
    assert_eq!(
        created.configuration,
        configuration_v3("model-a", json!({"label":"stored"}), false)
    );

    begin_unknown_run(&journal).await;
    let before = journal.record().await.unwrap();
    let replacement = metadata(json!({"label":"updated","nested":{"x":1}}));
    assert_eq!(
        journal.replace_metadata(replacement.clone()).await.unwrap(),
        replacement
    );
    let after = journal.record().await.unwrap();
    assert_eq!(after.revision, before.revision + 1);
    assert_eq!(after.configuration["metadata"]["label"], "updated");
    assert_eq!(
        serde_json::to_value(&after.runs).unwrap(),
        serde_json::to_value(&before.runs).unwrap()
    );
    assert_eq!(
        runtime.inspect(&key).await.unwrap().revision,
        after.revision
    );
    let oversized = metadata(json!({"large":"x".repeat(MAX_SESSION_METADATA_BYTES)}));
    assert!(matches!(
        journal.replace_metadata(oversized).await,
        Err(StoreError::Invalid(_))
    ));
    assert_eq!(journal.record().await.unwrap().revision, after.revision);

    journal.detach().await.unwrap();
    let detached = runtime.inspect(&key).await.unwrap();
    let attached = runtime
        .attach(
            &key,
            detached.revision,
            configuration_v1("model-a", json!({"label":"stale-caller"})),
            "owner-b".into(),
            "thread-b".into(),
        )
        .await
        .unwrap();
    let attached_record = attached.record().await.unwrap();
    assert_eq!(
        attached_record.configuration["metadata"]["label"],
        "updated"
    );
    assert_eq!(attached_record.epoch, detached.epoch + 1);
    attached.detach().await.unwrap();

    let before_mismatch = runtime.inspect(&key).await.unwrap();
    assert!(matches!(
        runtime
            .attach(
                &key,
                before_mismatch.revision,
                configuration_v1("different-model", json!({"label":"updated"})),
                "owner-c".into(),
                "thread-c".into(),
            )
            .await,
        Err(StoreError::Invalid(_))
    ));
    assert_eq!(runtime.inspect(&key).await.unwrap(), before_mismatch);
}

#[tokio::test]
async fn full_configuration_replacement_cannot_bypass_the_metadata_command() {
    let store: Arc<dyn SessionStore> = Arc::new(MemoryStore::new());
    let runtime = StoreRuntime::open(store).await.unwrap();
    let key = RecoveryKey::new();
    let journal = runtime
        .create(
            key,
            configuration_v2("model-a", json!({"label":"stored"})),
            "owner-a".into(),
            "thread-a".into(),
        )
        .await
        .unwrap();
    let before = journal.record().await.unwrap();
    assert!(matches!(
        journal
            .replace_configuration(configuration_v2("model-a", json!({"label":"bypassed"})))
            .await,
        Err(StoreError::Invalid(_))
    ));
    assert_eq!(
        serde_json::to_value(journal.record().await.unwrap()).unwrap(),
        serde_json::to_value(before).unwrap()
    );
}

#[tokio::test]
async fn oversized_legacy_metadata_is_migrated_read_only() {
    let store: Arc<dyn SessionStore> = Arc::new(MemoryStore::new());
    let (key, _) = seed_detached(store.clone()).await;
    let large = "x".repeat(MAX_SESSION_METADATA_BYTES);
    rewrite_configuration(
        &store,
        &key,
        configuration_v1("model-a", json!({"large":large})),
    )
    .await;
    let runtime = StoreRuntime::open(store.clone()).await.unwrap();
    let migrated = runtime.inspect(&key).await.unwrap();
    assert_eq!(
        migrated.configuration["metadata"]["large"]
            .as_str()
            .unwrap()
            .len(),
        MAX_SESSION_METADATA_BYTES
    );
    let attached = runtime
        .attach(
            &key,
            migrated.revision,
            configuration_v1("model-a", json!({"large":"ignored"})),
            "owner-b".into(),
            "thread-b".into(),
        )
        .await
        .unwrap();
    assert!(matches!(
        attached
            .replace_metadata(metadata(json!({
                "large":"x".repeat(MAX_SESSION_METADATA_BYTES)
            })))
            .await,
        Err(StoreError::Invalid(_))
    ));
}

struct FailingCasStore {
    memory: MemoryStore,
    failure: AtomicU8,
}

impl FailingCasStore {
    fn new() -> Self {
        Self {
            memory: MemoryStore::new(),
            failure: AtomicU8::new(0),
        }
    }

    fn fail_next_with(&self, failure: u8) {
        self.failure.store(failure, Ordering::SeqCst);
    }
}

#[async_trait]
impl SessionStore for FailingCasStore {
    fn durable(&self) -> bool {
        false
    }

    async fn create(&self, record: SessionRecord) -> whale_store::Result<()> {
        self.memory.create(record).await
    }

    async fn load(&self, id: &str) -> whale_store::Result<Option<SessionRecord>> {
        self.memory.load(id).await
    }

    async fn compare_exchange(
        &self,
        id: &str,
        expected_revision: u64,
        replacement: SessionRecord,
    ) -> whale_store::Result<()> {
        match self.failure.swap(0, Ordering::SeqCst) {
            1 => Err(StoreError::Io("injected metadata write failure".into())),
            2 => Err(StoreError::Conflict),
            _ => {
                self.memory
                    .compare_exchange(id, expected_revision, replacement)
                    .await
            }
        }
    }

    async fn list(&self) -> whale_store::Result<Vec<SessionRecord>> {
        self.memory.list().await
    }
}

#[tokio::test]
async fn metadata_backend_failure_never_reports_success_and_poisons_the_journal() {
    for failure in [1, 2] {
        let store = Arc::new(FailingCasStore::new());
        let runtime = StoreRuntime::open(store.clone()).await.unwrap();
        let key = RecoveryKey::new();
        let journal = runtime
            .create(
                key.clone(),
                configuration_v2("model-a", json!({"label":"old"})),
                "owner-a".into(),
                "thread-a".into(),
            )
            .await
            .unwrap();
        let before = store.load(&key.recovery_id).await.unwrap().unwrap();
        store.fail_next_with(failure);
        assert!(journal
            .replace_metadata(metadata(json!({"label":"new"})))
            .await
            .is_err());
        assert_eq!(
            serde_json::to_value(store.load(&key.recovery_id).await.unwrap().unwrap()).unwrap(),
            serde_json::to_value(before).unwrap()
        );
        assert!(matches!(
            journal.record().await,
            Err(StoreError::Poisoned(_))
        ));
    }
}

async fn backend_parity(store: Arc<dyn SessionStore>) {
    let runtime = StoreRuntime::open(store.clone()).await.unwrap();
    let key = RecoveryKey::new();
    let journal = runtime
        .create(
            key.clone(),
            configuration_v1("model-a", json!({"label":"created"})),
            "owner-a".into(),
            "thread-a".into(),
        )
        .await
        .unwrap();
    assert_eq!(journal.record().await.unwrap().configuration["version"], 3);
    journal
        .replace_metadata(metadata(json!({"label":"replaced"})))
        .await
        .unwrap();
    journal.detach().await.unwrap();
    let detached = runtime.inspect(&key).await.unwrap();
    let attached = runtime
        .attach(
            &key,
            detached.revision,
            configuration_v1("model-a", json!({"label":"ignored"})),
            "owner-b".into(),
            "thread-b".into(),
        )
        .await
        .unwrap();
    assert_eq!(
        attached.record().await.unwrap().configuration["metadata"]["label"],
        "replaced"
    );
}

#[tokio::test]
async fn memory_backend_has_session_metadata_parity() {
    backend_parity(Arc::new(MemoryStore::new())).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_backend_has_session_metadata_parity() {
    let directory = tempfile::tempdir().unwrap();
    backend_parity(Arc::new(
        whale_store::SQLiteStore::open(directory.path().join("metadata.sqlite")).unwrap(),
    ))
    .await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_open_migrates_raw_v1_once_without_changing_history_or_runs() {
    let directory = tempfile::tempdir().unwrap();
    let store: Arc<dyn SessionStore> = Arc::new(
        whale_store::SQLiteStore::open(directory.path().join("migration.sqlite")).unwrap(),
    );
    let runtime = StoreRuntime::open(store.clone()).await.unwrap();
    let key = RecoveryKey::new();
    let journal = runtime
        .create(
            key.clone(),
            configuration_v2("model-a", json!({"label":"old"})),
            "owner-a".into(),
            "thread-a".into(),
        )
        .await
        .unwrap();
    begin_unknown_run(&journal).await;
    journal.detach().await.unwrap();
    let before = rewrite_configuration(
        &store,
        &key,
        configuration_v1("model-a", json!({"label":"migrated"})),
    )
    .await;

    StoreRuntime::open(store.clone()).await.unwrap();
    let migrated = store.load(&key.recovery_id).await.unwrap().unwrap();
    assert_eq!(migrated.revision, before.revision + 1);
    assert_eq!(
        migrated.configuration,
        configuration_v3("model-a", json!({"label":"migrated"}), false)
    );
    assert_eq!(
        serde_json::to_value(&migrated.history).unwrap(),
        serde_json::to_value(&before.history).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&migrated.runs).unwrap(),
        serde_json::to_value(&before.runs).unwrap()
    );
    StoreRuntime::open(store.clone()).await.unwrap();
    assert_eq!(
        store
            .load(&key.recovery_id)
            .await
            .unwrap()
            .unwrap()
            .revision,
        migrated.revision
    );
}

#[test]
fn typed_configuration_parser_keeps_attachment_identity_separate_from_metadata() {
    let (v1, migrated) = PersistedSessionConfigurationV2::parse_and_migrate(configuration_v1(
        "model-a",
        json!({"label":"old"}),
    ))
    .unwrap();
    assert!(migrated);
    assert_eq!(v1.metadata()["label"], "old");
    let (v2, migrated) = PersistedSessionConfigurationV2::parse_and_migrate(configuration_v2(
        "model-a",
        json!({"label":"new"}),
    ))
    .unwrap();
    assert!(migrated);
    assert!(!v1.interactions_enabled());
    assert!(!v2.interactions_enabled());
    assert_eq!(v1.attachment_identity(), v2.attachment_identity());
    assert_ne!(v1.metadata(), v2.metadata());
}
