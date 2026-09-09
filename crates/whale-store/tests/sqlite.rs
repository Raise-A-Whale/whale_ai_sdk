#![cfg(feature = "sqlite")]
use serde_json::json;
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use whale_protocol::recovery::RecoveryKey;
use whale_protocol::runs::{RunSnapshot, RunStatus, StartTurnParams};
use whale_protocol::CanonicalItem;
use whale_store::*;
fn pending() -> (StartTurnParams, RunSnapshot) {
    let params = StartTurnParams {
        thread_id: "thread".into(),
        turn_id: "turn".into(),
        input_items: vec![CanonicalItem::user_text("persisted input")],
        options: None,
        max_steps: 10,
        timeout_ms: None,
    };
    let snapshot = RunSnapshot {
        thread_id: "thread".into(),
        turn_id: "turn".into(),
        status: RunStatus::Running,
        items: vec![],
        usage: Default::default(),
        tool_executions: vec![],
        pending_approvals: vec![],
        last_seq: 0,
        result: None,
        error: None,
    };
    (params, snapshot)
}
#[tokio::test]
async fn sqlite_reopen_recovers_and_retains_authenticated_terminal_history() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state.sqlite");
    let key = RecoveryKey::new();
    {
        let backend = Arc::new(SQLiteStore::open(&path).unwrap());
        let runtime = StoreRuntime::open(backend).await.unwrap();
        assert!(runtime.durable());
        let journal = runtime
            .create(
                key.clone(),
                json!({"version":1}),
                "owner".into(),
                "thread".into(),
            )
            .await
            .unwrap();
        let (p, s) = pending();
        journal
            .begin_run(p, s, json!({"model":"fixture"}))
            .await
            .unwrap();
    }
    // The FIFO actor releases the database once its last sender is dropped and all
    // accepted jobs finish, so opening is retried only for that bounded drain.
    let deadline = Instant::now() + Duration::from_secs(2);
    let backend = loop {
        match SQLiteStore::open(&path) {
            Ok(store) => break Arc::new(store),
            Err(error) => {
                assert!(Instant::now() < deadline, "{error}");
                tokio::task::yield_now().await;
            }
        }
    };
    let runtime = StoreRuntime::open(backend).await.unwrap();
    let recovered = runtime.inspect(&key).await.unwrap();
    assert!(!recovered.attached);
    assert_eq!(recovered.history.len(), 1);
    assert_eq!(recovered.runs[0].snapshot.status, RunStatus::Failed);
    assert_eq!(
        recovered.runs[0].snapshot.error.as_ref().unwrap().code,
        "RECOVERY_INTERRUPTED"
    );
    let text = std::fs::read(&path).unwrap();
    assert!(!text
        .windows(key.secret.len())
        .any(|w| w == key.secret.as_bytes()));
}
#[tokio::test]
async fn actual_sqlite_write_failure_poisons_the_journal_and_prevents_followup() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state.sqlite");
    let backend = Arc::new(SQLiteStore::open(&path).unwrap());
    let runtime = StoreRuntime::open(backend).await.unwrap();
    let key = RecoveryKey::new();
    let journal = runtime
        .create(
            key.clone(),
            json!({"version":1}),
            "owner".into(),
            "thread".into(),
        )
        .await
        .unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER injected_disk_failure BEFORE UPDATE ON sessions BEGIN SELECT RAISE(ABORT,'controlled storage write failure'); END;").unwrap();
    let (p, s) = pending();
    assert!(matches!(
        journal.begin_run(p, s, json!({})).await,
        Err(StoreError::Io(_))
    ));
    assert!(matches!(
        journal.replace_configuration(json!({"version":1})).await,
        Err(StoreError::Poisoned(_))
    ));
    assert!(runtime.inspect(&key).await.unwrap().runs.is_empty());
}
#[test]
fn sqlite_lock_child() {
    let Ok(path) = std::env::var("WHALE_STORE_CHILD_DB") else {
        return;
    };
    let _store = SQLiteStore::open(&path).unwrap();
    std::fs::write(std::env::var("WHALE_STORE_CHILD_READY").unwrap(), "locked").unwrap();
    loop {
        std::thread::sleep(Duration::from_secs(10));
    }
}
#[test]
fn actual_process_lock_rejects_competing_writer_and_releases_after_death() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state.sqlite");
    let ready = directory.path().join("ready");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "sqlite_lock_child", "--nocapture"])
        .env("WHALE_STORE_CHILD_DB", &path)
        .env("WHALE_STORE_CHILD_READY", &ready)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while !ready.exists() {
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("child did not open database")
        };
        std::thread::sleep(Duration::from_millis(5));
    }
    let competing = SQLiteStore::open(&path);
    let rejected = competing.is_err();
    drop(competing);
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(rejected, "competing writer acquired store");
    assert!(SQLiteStore::open(&path).is_ok());
}
#[test]
fn same_inode_alias_cannot_bypass_process_lock() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state.sqlite");
    let alias = directory.path().join("alias.sqlite");
    let _store = SQLiteStore::open(&path).unwrap();
    std::fs::hard_link(&path, &alias).unwrap();
    assert!(SQLiteStore::open(&alias).is_err());
}
#[tokio::test]
async fn corrupt_schema_is_rejected_before_startup_publication() {
    let directory = tempfile::tempdir().unwrap();
    let path: PathBuf = directory.path().join("state.sqlite");
    let backend = Arc::new(SQLiteStore::open(&path).unwrap());
    let runtime = StoreRuntime::open(backend.clone()).await.unwrap();
    let key = RecoveryKey::new();
    let journal = runtime
        .create(
            key.clone(),
            json!({"version":1}),
            "owner".into(),
            "thread".into(),
        )
        .await
        .unwrap();
    let record = journal.record().await.unwrap();
    let mut value = serde_json::to_value(record).unwrap();
    value["schema_version"] = json!(900);
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute(
            "UPDATE sessions SET record=?1 WHERE id=?2",
            rusqlite::params![value.to_string(), key.recovery_id],
        )
        .unwrap();
    assert!(StoreRuntime::open(backend).await.is_err());
}
#[test]
fn sqlite_recovery_child() {
    let Ok(path) = std::env::var("WHALE_STORE_RECOVERY_DB") else {
        return;
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let key: RecoveryKey =
            serde_json::from_str(&std::env::var("WHALE_STORE_RECOVERY_KEY").unwrap()).unwrap();
        let store = StoreRuntime::open(Arc::new(SQLiteStore::open(path).unwrap()))
            .await
            .unwrap();
        let journal = store
            .create(
                key,
                json!({"version":1}),
                "dead-owner".into(),
                "thread".into(),
            )
            .await
            .unwrap();
        let (p, s) = pending();
        journal
            .begin_run(p, s, json!({"model":"fixture"}))
            .await
            .unwrap();
        journal
            .commit(
                "turn",
                RunMutation::ModelInput {
                    step_id: "step".into(),
                    step_index: 0,
                    request: json!({"model_context":{"items":["actual projection"]}}),
                },
            )
            .await
            .unwrap();
        for id in ["unknown", "known"] {
            journal
                .commit(
                    "turn",
                    RunMutation::ModelItem {
                        step_id: "step".into(),
                        item: CanonicalItem::ToolCall {
                            id: format!("item-{id}"),
                            call_id: id.into(),
                            name: "lookup".into(),
                            namespace: None,
                            arguments: Some(json!({})),
                            raw_arguments: "{}".into(),
                        },
                    },
                )
                .await
                .unwrap();
        }
        journal
            .commit(
                "turn",
                RunMutation::ModelStepFinished {
                    step_id: "step".into(),
                    usage: Default::default(),
                },
            )
            .await
            .unwrap();
        for id in ["unknown", "known"] {
            journal
                .commit(
                    "turn",
                    RunMutation::DispatchIntent {
                        call_id: id.into(),
                        execution: whale_protocol::contexts::ToolExecutionRecord {
                            call_id: id.into(),
                            original_arguments: json!({}),
                            arguments: json!({}),
                        },
                    },
                )
                .await
                .unwrap();
        }
        // Deliberate stand-in for two irreversible host effects; the parent checks
        // this external file is unchanged by every startup/inspect operation.
        std::fs::write(std::env::var("WHALE_STORE_RECOVERY_COUNTER").unwrap(), "2").unwrap();
        journal
            .commit(
                "turn",
                RunMutation::ToolOutcome {
                    call_id: "known".into(),
                    result: CanonicalItem::tool_result(
                        "known",
                        whale_protocol::CanonicalToolOutput::text("known result"),
                        false,
                    ),
                },
            )
            .await
            .unwrap();
        std::fs::write(
            std::env::var("WHALE_STORE_RECOVERY_READY").unwrap(),
            "committed",
        )
        .unwrap();
        std::future::pending::<()>().await;
    });
}
#[tokio::test]
async fn process_death_preserves_parallel_outcome_and_recovery_never_changes_external_counter() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state.sqlite");
    let ready = directory.path().join("ready");
    let counter = directory.path().join("counter");
    let key = RecoveryKey::new();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "sqlite_recovery_child", "--nocapture"])
        .env("WHALE_STORE_RECOVERY_DB", &path)
        .env(
            "WHALE_STORE_RECOVERY_KEY",
            serde_json::to_string(&key).unwrap(),
        )
        .env("WHALE_STORE_RECOVERY_READY", &ready)
        .env("WHALE_STORE_RECOVERY_COUNTER", &counter)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while !ready.exists() {
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("recovery child did not finish commits")
        };
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let recovered = {
        let store = StoreRuntime::open(Arc::new(SQLiteStore::open(&path).unwrap()))
            .await
            .unwrap();
        store.inspect(&key).await.unwrap()
    };
    assert_eq!(recovered.unknown_executions.len(), 1);
    assert_eq!(recovered.unknown_executions[0].call_id, "unknown");
    assert!(recovered.history.iter().any(|item|matches!(item,CanonicalItem::ToolResult{call_id,is_error:false,..}if call_id=="known")));
    let reopened = StoreRuntime::open(Arc::new(SQLiteStore::open(&path).unwrap()))
        .await
        .unwrap()
        .inspect(&key)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(&recovered).unwrap(),
        serde_json::to_value(&reopened).unwrap()
    );
    assert_eq!(std::fs::read_to_string(counter).unwrap(), "2");
}
