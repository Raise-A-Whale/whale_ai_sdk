use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use whale_protocol::recovery::RecoveryKey;
use whale_store::{
    MemoryStore, PersistedSessionConfigurationV2, SessionRecord, SessionStore, StoreError,
    StoreRuntime,
};

fn configuration_v1(metadata: Value) -> Value {
    json!({
        "version": 1,
        "session": {
            "model": "model-a",
            "tools": [],
            "extension": {"keep": [1, 2, 3]},
            "metadata": metadata,
        },
        "run_defaults": {"max_steps": 3, "timeout_ms": 100},
    })
}

fn configuration_v2(metadata: Value) -> Value {
    json!({
        "version": 2,
        "session": {
            "model": "model-a",
            "tools": [],
            "extension": {"keep": [1, 2, 3]},
        },
        "run_defaults": {"max_steps": 3, "timeout_ms": 100},
        "metadata": metadata,
    })
}

fn configuration_v3(metadata: Value, interactions_enabled: bool) -> Value {
    json!({
        "version": 3,
        "session": {
            "model": "model-a",
            "tools": [],
            "extension": {"keep": [1, 2, 3]},
        },
        "run_defaults": {"max_steps": 3, "timeout_ms": 100},
        "metadata": metadata,
        "runtime_features": {"interactions_enabled": interactions_enabled},
    })
}

#[derive(Default)]
struct CountingStore {
    memory: MemoryStore,
    creates: AtomicUsize,
    exchanges: AtomicUsize,
}

impl CountingStore {
    fn reset_counts(&self) {
        self.creates.store(0, Ordering::SeqCst);
        self.exchanges.store(0, Ordering::SeqCst);
    }

    fn create_count(&self) -> usize {
        self.creates.load(Ordering::SeqCst)
    }

    fn exchange_count(&self) -> usize {
        self.exchanges.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl SessionStore for CountingStore {
    fn durable(&self) -> bool {
        self.memory.durable()
    }

    async fn create(&self, record: SessionRecord) -> whale_store::Result<()> {
        self.creates.fetch_add(1, Ordering::SeqCst);
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
        self.exchanges.fetch_add(1, Ordering::SeqCst);
        self.memory
            .compare_exchange(id, expected_revision, replacement)
            .await
    }

    async fn list(&self) -> whale_store::Result<Vec<SessionRecord>> {
        self.memory.list().await
    }
}

async fn seed_detached(store: Arc<CountingStore>) -> (RecoveryKey, SessionRecord) {
    let runtime = StoreRuntime::open(store.clone()).await.unwrap();
    let key = RecoveryKey::new();
    let journal = runtime
        .create(
            key.clone(),
            configuration_v3(json!({"stored": "metadata"}), false),
            "owner-a".into(),
            "thread-a".into(),
        )
        .await
        .unwrap();
    journal.detach().await.unwrap();
    drop(journal);
    let record = store.load(&key.recovery_id).await.unwrap().unwrap();
    (key, record)
}

async fn replace_raw_configuration(
    store: &Arc<CountingStore>,
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

#[test]
fn parser_normalizes_v1_and_v2_to_v3_and_reopens_v3_without_migration() {
    for legacy in [
        configuration_v1(json!({"label": "legacy"})),
        configuration_v2(json!({"label": "legacy"})),
    ] {
        let (configuration, migrated) =
            PersistedSessionConfigurationV2::parse_and_migrate(legacy).unwrap();
        assert!(migrated);
        assert!(!configuration.interactions_enabled());
        assert_eq!(
            configuration.into_value(),
            configuration_v3(json!({"label": "legacy"}), false)
        );
    }

    let expected = configuration_v3(json!({"label": "current"}), true);
    let (configuration, migrated) =
        PersistedSessionConfigurationV2::parse_and_migrate(expected.clone()).unwrap();
    assert!(!migrated);
    assert!(configuration.interactions_enabled());
    assert_eq!(configuration.into_value(), expected);
}

#[tokio::test]
async fn store_open_repairs_each_legacy_version_once_and_v3_zero_times() {
    for legacy in [
        configuration_v1(json!({"label": "legacy"})),
        configuration_v2(json!({"label": "legacy"})),
    ] {
        let store = Arc::new(CountingStore::default());
        let (key, _) = seed_detached(store.clone()).await;
        let before = replace_raw_configuration(&store, &key, legacy).await;
        store.reset_counts();

        StoreRuntime::open(store.clone()).await.unwrap();
        let migrated = store.load(&key.recovery_id).await.unwrap().unwrap();
        assert_eq!(store.exchange_count(), 1);
        assert_eq!(migrated.revision, before.revision + 1);
        assert_eq!(
            migrated.configuration,
            configuration_v3(json!({"label": "legacy"}), false)
        );

        StoreRuntime::open(store.clone()).await.unwrap();
        assert_eq!(store.exchange_count(), 1, "valid V3 must not be rewritten");
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

    let store = Arc::new(CountingStore::default());
    let (key, v3) = seed_detached(store.clone()).await;
    store.reset_counts();
    StoreRuntime::open(store.clone()).await.unwrap();
    assert_eq!(store.exchange_count(), 0);
    assert_eq!(
        store
            .load(&key.recovery_id)
            .await
            .unwrap()
            .unwrap()
            .revision,
        v3.revision
    );
}

#[tokio::test]
async fn malformed_or_unknown_configuration_is_never_rewritten() {
    for malformed in [
        json!({"version": 99, "session": {}, "run_defaults": {}, "metadata": {}}),
        json!({
            "version": 2,
            "session": {},
            "run_defaults": {},
            "metadata": {},
            "unknown": true,
        }),
        json!({
            "version": 3,
            "session": {},
            "run_defaults": {},
            "metadata": {},
        }),
        json!({
            "version": 3,
            "session": {},
            "run_defaults": {},
            "metadata": {},
            "runtime_features": "bad",
        }),
        json!({
            "version": 3,
            "session": {},
            "run_defaults": {},
            "metadata": {},
            "runtime_features": {"interactions_enabled": false, "unknown": true},
        }),
        json!({
            "version": 3,
            "session": {},
            "run_defaults": {},
            "metadata": {},
            "runtime_features": {"interactions_enabled": false},
            "pending_interactions": [],
        }),
    ] {
        let store = Arc::new(CountingStore::default());
        let (key, _) = seed_detached(store.clone()).await;
        let before = replace_raw_configuration(&store, &key, malformed).await;
        store.reset_counts();

        assert!(matches!(
            StoreRuntime::open(store.clone()).await,
            Err(StoreError::Invalid(_))
        ));
        assert_eq!(store.exchange_count(), 0);
        let after = store.load(&key.recovery_id).await.unwrap().unwrap();
        assert_eq!(
            serde_json::to_value(after).unwrap(),
            serde_json::to_value(before).unwrap()
        );
    }
}

#[tokio::test]
async fn attach_identity_includes_exact_runtime_feature_and_ignores_caller_metadata() {
    let store = Arc::new(CountingStore::default());
    let (key, _) = seed_detached(store.clone()).await;
    let legacy =
        replace_raw_configuration(&store, &key, configuration_v2(json!({"label": "retained"})))
            .await;

    let runtime = StoreRuntime::open(store.clone()).await.unwrap();
    let migrated = runtime.inspect(&key).await.unwrap();
    assert_eq!(migrated.revision, legacy.revision + 1);
    assert_eq!(
        migrated.configuration["runtime_features"]["interactions_enabled"],
        false
    );

    assert!(matches!(
        runtime
            .attach(
                &key,
                migrated.revision,
                configuration_v3(json!({"label": "ignored"}), true),
                "owner-b".into(),
                "thread-b".into(),
            )
            .await,
        Err(StoreError::Invalid(_))
    ));
    assert_eq!(runtime.inspect(&key).await.unwrap(), migrated);

    let attached = runtime
        .attach(
            &key,
            migrated.revision,
            configuration_v2(json!({"label": "ignored"})),
            "owner-b".into(),
            "thread-b".into(),
        )
        .await
        .unwrap();
    let record = attached.record().await.unwrap();
    assert_eq!(record.configuration["metadata"]["label"], "retained");
    assert_eq!(record.configuration["version"], 3);
    assert_eq!(
        record.configuration["runtime_features"]["interactions_enabled"],
        false
    );
}

#[tokio::test]
async fn persistent_create_writes_only_v3_runtime_enablement_not_live_interaction_state() {
    let store = Arc::new(CountingStore::default());
    let runtime = StoreRuntime::open(store.clone()).await.unwrap();
    store.reset_counts();
    let key = RecoveryKey::new();
    let journal = runtime
        .create(
            key,
            configuration_v3(json!({"label": "stored"}), true),
            "owner-a".into(),
            "thread-a".into(),
        )
        .await
        .unwrap();
    assert_eq!(store.create_count(), 1);
    let configuration = journal.record().await.unwrap().configuration;
    assert_eq!(
        configuration,
        configuration_v3(json!({"label": "stored"}), true)
    );
    let serialized = serde_json::to_string(&configuration).unwrap();
    for forbidden in [
        "pending_interactions",
        "interaction_request",
        "interaction_response",
        "interaction_fingerprint",
    ] {
        assert!(!serialized.contains(forbidden));
    }

    for forbidden_configuration in [
        json!({
            "version": 3,
            "session": {},
            "run_defaults": {},
            "metadata": {},
            "runtime_features": {"interactions_enabled": false},
            "interaction_response": {"secret": true},
        }),
        json!({
            "version": 3,
            "session": {},
            "run_defaults": {},
            "metadata": {},
            "runtime_features": {
                "interactions_enabled": false,
                "interaction_fingerprint": "secret",
            },
        }),
    ] {
        let before = store.create_count();
        assert!(matches!(
            runtime
                .create(
                    RecoveryKey::new(),
                    forbidden_configuration,
                    "owner-b".into(),
                    format!("thread-{before}"),
                )
                .await,
            Err(StoreError::Invalid(_))
        ));
        assert_eq!(store.create_count(), before);
    }
}
