use serde_json::{json, Value};
use std::sync::Arc;
use whale_protocol::{
    contexts::ToolExecutionRecord,
    recovery::RecoveryKey,
    runs::{PendingApproval, RunSnapshot, RunStatus, StartTurnParams},
    CanonicalItem,
};
#[cfg(feature = "sqlite")]
use whale_store::SessionStore;
use whale_store::{MemoryStore, RunMutation, SessionJournal, StoreRuntime};

fn configuration() -> Value {
    json!({
        "version": 3,
        "session": {"model": "fixture", "tools": []},
        "run_defaults": {"max_steps": 4},
        "metadata": {"label": "interaction-recovery"},
        "runtime_features": {"interactions_enabled": true}
    })
}

fn params() -> StartTurnParams {
    StartTurnParams {
        thread_id: "thread-before-restart".into(),
        turn_id: "turn-pending".into(),
        input_items: vec![CanonicalItem::user_text("persisted input")],
        options: None,
        max_steps: 4,
        timeout_ms: None,
    }
}

fn pending_snapshot() -> RunSnapshot {
    RunSnapshot {
        thread_id: "thread-before-restart".into(),
        turn_id: "turn-pending".into(),
        status: RunStatus::WaitingApproval,
        items: Vec::new(),
        usage: Default::default(),
        tool_executions: Vec::new(),
        pending_approvals: vec![PendingApproval {
            request_id: "typed-request".into(),
            tool_call: CanonicalItem::tool_call(
                "tool-item",
                None,
                "lookup",
                Some(json!({"query": "safe"})),
                r#"{"query":"safe"}"#,
            ),
            reason: Some("confirm".into()),
        }],
        last_seq: 1,
        result: None,
        error: None,
    }
}

async fn seed_pending(runtime: &StoreRuntime) -> (RecoveryKey, SessionJournal) {
    let key = RecoveryKey::new();
    let journal = runtime
        .create(
            key.clone(),
            configuration(),
            "owner-before-restart".into(),
            "thread-before-restart".into(),
        )
        .await
        .unwrap();
    journal
        .begin_run(params(), pending_snapshot(), json!({"model": "fixture"}))
        .await
        .unwrap();
    journal
        .commit(
            "turn-pending",
            RunMutation::ModelInput {
                step_id: "step-0".into(),
                step_index: 0,
                request: json!({"model_context": {"items": []}, "options": {"model": "fixture"}}),
            },
        )
        .await
        .unwrap();
    journal
        .commit(
            "turn-pending",
            RunMutation::ModelItem {
                step_id: "step-0".into(),
                item: CanonicalItem::tool_call(
                    "durable-item",
                    None,
                    "lookup",
                    Some(json!({"query": "durable"})),
                    r#"{"query":"durable"}"#,
                ),
            },
        )
        .await
        .unwrap();
    journal
        .commit(
            "turn-pending",
            RunMutation::ModelStepFinished {
                step_id: "step-0".into(),
                usage: Default::default(),
            },
        )
        .await
        .unwrap();
    journal
        .commit(
            "turn-pending",
            RunMutation::DispatchIntent {
                call_id: "durable-item".into(),
                execution: ToolExecutionRecord {
                    call_id: "durable-item".into(),
                    original_arguments: json!({"query": "durable"}),
                    arguments: json!({"query": "durable"}),
                },
            },
        )
        .await
        .unwrap();
    (key, journal)
}

fn assert_no_interaction_runtime_state(value: &impl serde::Serialize) {
    let encoded = serde_json::to_string(value).unwrap();
    for forbidden in [
        "interaction-secret-response",
        "interaction-secret-fingerprint",
        "pending_interactions",
        "interaction_response",
        "interaction_fingerprint",
        "interaction_waiter",
        "interaction_cursor",
    ] {
        assert!(
            !encoded.contains(forbidden),
            "persisted forbidden value: {forbidden}"
        );
    }
}

async fn assert_recovered(runtime: &StoreRuntime, key: &RecoveryKey) {
    let recovered = runtime.inspect(key).await.unwrap();
    assert!(!recovered.attached);
    assert_eq!(
        recovered.configuration["runtime_features"]["interactions_enabled"],
        true
    );
    assert_eq!(recovered.runs.len(), 1);
    let run = &recovered.runs[0].snapshot;
    assert_eq!(run.status, RunStatus::Failed);
    assert_eq!(run.error.as_ref().unwrap().code, "RECOVERY_INTERRUPTED");
    assert!(run.pending_approvals.is_empty());
    assert_eq!(recovered.unknown_executions.len(), 1);
    assert_eq!(recovered.unknown_executions[0].call_id, "durable-item");
    assert_no_interaction_runtime_state(&recovered);
}

#[tokio::test]
async fn memory_restart_interrupts_pending_run_without_persisting_interaction_state() {
    let backend = Arc::new(MemoryStore::new());
    let runtime = StoreRuntime::open(backend.clone()).await.unwrap();
    let (key, journal) = seed_pending(&runtime).await;
    assert_no_interaction_runtime_state(&journal.record().await.unwrap());
    drop(journal);
    drop(runtime);

    let reopened = StoreRuntime::open(backend).await.unwrap();
    assert_recovered(&reopened, &key).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_restart_interrupts_pending_run_without_persisting_interaction_state() {
    use std::time::{Duration, Instant};
    use whale_store::SQLiteStore;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("interaction-recovery.sqlite");
    let key = {
        let backend: Arc<dyn SessionStore> = Arc::new(SQLiteStore::open(&path).unwrap());
        let runtime = StoreRuntime::open(backend).await.unwrap();
        let (key, journal) = seed_pending(&runtime).await;
        assert_no_interaction_runtime_state(&journal.record().await.unwrap());
        key
    };

    let deadline = Instant::now() + Duration::from_secs(2);
    let backend: Arc<dyn SessionStore> = loop {
        match SQLiteStore::open(&path) {
            Ok(store) => break Arc::new(store),
            Err(error) => {
                assert!(Instant::now() < deadline, "{error}");
                tokio::task::yield_now().await;
            }
        }
    };
    let reopened = StoreRuntime::open(backend).await.unwrap();
    assert_recovered(&reopened, &key).await;
    let bytes = std::fs::read(&path).unwrap();
    let stored = String::from_utf8_lossy(&bytes);
    for forbidden in [
        "interaction-secret-response",
        "interaction-secret-fingerprint",
        "pending_interactions",
        "interaction_response",
        "interaction_fingerprint",
        "interaction_waiter",
        "interaction_cursor",
    ] {
        assert!(
            !stored.contains(forbidden),
            "persisted forbidden value: {forbidden}"
        );
    }
}
