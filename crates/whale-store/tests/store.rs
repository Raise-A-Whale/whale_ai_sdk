use serde_json::{json, Value};
use std::sync::Arc;
use whale_protocol::contexts::ToolExecutionRecord;
use whale_protocol::recovery::RecoveryKey;
use whale_protocol::runs::{RunSnapshot, RunStatus, StartTurnParams};
use whale_protocol::{CanonicalItem, CanonicalToolOutput};
use whale_store::*;
fn config() -> Value {
    json!({"version":1,"model":"fixture"})
}
fn snapshot(thread: &str, turn: &str) -> RunSnapshot {
    RunSnapshot {
        thread_id: thread.into(),
        turn_id: turn.into(),
        status: RunStatus::Running,
        items: vec![],
        usage: Default::default(),
        tool_executions: vec![],
        pending_approvals: vec![],
        last_seq: 0,
        result: None,
        error: None,
    }
}
fn params(thread: &str, turn: &str) -> StartTurnParams {
    StartTurnParams {
        thread_id: thread.into(),
        turn_id: turn.into(),
        input_items: vec![CanonicalItem::user_text("original")],
        options: None,
        max_steps: 10,
        timeout_ms: None,
    }
}
fn call(id: &str) -> CanonicalItem {
    CanonicalItem::ToolCall {
        id: format!("item-{id}"),
        call_id: id.into(),
        name: "lookup".into(),
        namespace: None,
        arguments: Some(json!({"query":"original"})),
        raw_arguments: "{\"query\":\"original\"}".into(),
    }
}
fn intent(id: &str) -> RunMutation {
    RunMutation::DispatchIntent {
        call_id: id.into(),
        execution: ToolExecutionRecord {
            call_id: id.into(),
            original_arguments: json!({"query":"original"}),
            arguments: json!({"query":"changed"}),
        },
    }
}
async fn fixture() -> (Arc<MemoryStore>, StoreRuntime, RecoveryKey, SessionJournal) {
    let store = Arc::new(MemoryStore::new());
    let runtime = StoreRuntime::open(store.clone()).await.unwrap();
    let key = RecoveryKey::new();
    let journal = runtime
        .create(key.clone(), config(), "owner".into(), "thread".into())
        .await
        .unwrap();
    (store, runtime, key, journal)
}
async fn prepare(j: &SessionJournal, calls: &[&str]) {
    j.begin_run(
        params("thread", "turn"),
        snapshot("thread", "turn"),
        json!({"model":"fixture"}),
    )
    .await
    .unwrap();
    j.commit(
        "turn",
        RunMutation::ModelInput {
            step_id: "step".into(),
            step_index: 0,
            request: json!({"model_context":{"items":["projected"]},"options":{"model":"fixture"}}),
        },
    )
    .await
    .unwrap();
    for id in calls {
        j.commit(
            "turn",
            RunMutation::ModelItem {
                step_id: "step".into(),
                item: call(id),
            },
        )
        .await
        .unwrap();
    }
    j.commit(
        "turn",
        RunMutation::ModelStepFinished {
            step_id: "step".into(),
            usage: whale_protocol::events::UsageMetrics {
                input_tokens: 3,
                output_tokens: 1,
                ..Default::default()
            },
        },
    )
    .await
    .unwrap();
}
#[tokio::test]
async fn memory_runtime_opens_without_claiming_durability() {
    let (_, runtime, _, _) = fixture().await;
    assert!(!runtime.durable());
}
#[tokio::test]
async fn credentials_cas_and_authenticated_tombstone() {
    let (store, runtime, key, journal) = fixture().await;
    let record = journal.record().await.unwrap();
    assert_eq!(record.revision, 1);
    assert!(!format!("{record:?}").contains(&key.secret));
    assert!(!serde_json::to_string(&record)
        .unwrap()
        .contains(&key.secret));
    let mut invalid = record.clone();
    invalid.revision += 2;
    assert!(store
        .compare_exchange(&key.recovery_id, 1, invalid)
        .await
        .is_err());
    let mut wrong = key.clone();
    wrong.secret = RecoveryKey::new().secret;
    assert!(matches!(
        runtime.inspect(&wrong).await,
        Err(StoreError::Unauthorized)
    ));
    assert!(matches!(
        runtime.forget(&key, 1).await,
        Err(StoreError::Active)
    ));
    journal.detach().await.unwrap();
    let revision = runtime.inspect(&key).await.unwrap().revision;
    assert!(matches!(
        runtime.forget(&key, revision - 1).await,
        Err(StoreError::Conflict)
    ));
    assert!(runtime.forget(&key, revision).await.unwrap());
    assert!(!runtime.forget(&key, revision).await.unwrap());
    assert!(matches!(
        runtime.inspect(&key).await,
        Err(StoreError::Forgotten)
    ));
    assert!(runtime
        .create(key, config(), "owner".into(), "fresh".into())
        .await
        .is_err());
}
#[tokio::test]
async fn completed_parallel_outcome_survives_recovery_and_unknown_ids_are_stable() {
    let (store, runtime, key, journal) = fixture().await;
    prepare(&journal, &["first", "second", "never"]).await;
    journal.commit("turn", intent("first")).await.unwrap();
    journal.commit("turn", intent("second")).await.unwrap();
    let receipt = journal
        .commit(
            "turn",
            RunMutation::ToolOutcome {
                call_id: "second".into(),
                result: CanonicalItem::tool_result(
                    "second",
                    CanonicalToolOutput::text("external-effect-complete"),
                    false,
                ),
            },
        )
        .await
        .unwrap();
    let stable = receipt.result_item.unwrap();
    assert!(!journal.record().await.unwrap().history.contains(&stable));
    let reopened = StoreRuntime::open(store.clone()).await.unwrap();
    let state = reopened.inspect(&key).await.unwrap();
    assert!(!state.attached);
    assert_eq!(state.runs[0].snapshot.status, RunStatus::Failed);
    assert_eq!(
        state.runs[0].snapshot.error.as_ref().unwrap().code,
        "RECOVERY_INTERRUPTED"
    );
    assert_eq!(state.unknown_executions.len(), 1);
    assert_eq!(state.unknown_executions[0].call_id, "first");
    assert!(state.history.contains(&stable));
    assert_eq!(
        state.runs[0].model_inputs[0].request["model_context"]["items"][0],
        "projected"
    );
    assert_eq!(state.runs[0].snapshot.usage.input_tokens, 3);
    let again = StoreRuntime::open(store)
        .await
        .unwrap()
        .inspect(&key)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(&state).unwrap(),
        serde_json::to_value(&again).unwrap()
    );
    assert!(matches!(
        journal.commit("turn", intent("never")).await,
        Err(StoreError::StaleLease)
    ));
    let attached = reopened
        .attach(
            &key,
            state.revision,
            config(),
            "new-owner".into(),
            "fresh".into(),
        )
        .await
        .unwrap();
    assert!(matches!(
        attached
            .begin_run(params("fresh", "new"), snapshot("fresh", "new"), json!({}))
            .await,
        Err(StoreError::UnknownExecutions)
    ));
    let current = attached.record().await.unwrap();
    assert!(attached
        .acknowledge(current.revision, vec![])
        .await
        .is_err());
    let acknowledged = attached
        .acknowledge(
            current.revision,
            vec![state.unknown_executions[0].execution_id.clone()],
        )
        .await
        .unwrap();
    assert!(acknowledged.unknown_executions[0].acknowledged);
    assert!(attached
        .begin_run(params("fresh", "new"), snapshot("fresh", "new"), json!({}))
        .await
        .unwrap());
    drop(runtime);
}
#[tokio::test]
async fn dispatch_and_model_step_order_are_enforced() {
    let (_, _, _, j) = fixture().await;
    j.begin_run(
        params("thread", "turn"),
        snapshot("thread", "turn"),
        json!({}),
    )
    .await
    .unwrap();
    assert!(j.commit("turn", intent("one")).await.is_err());
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
            item: call("one"),
        },
    )
    .await
    .unwrap();
    assert!(j.commit("turn", intent("one")).await.is_err());
    j.commit(
        "turn",
        RunMutation::ModelStepFinished {
            step_id: "step".into(),
            usage: Default::default(),
        },
    )
    .await
    .unwrap();
    j.commit("turn", intent("one")).await.unwrap();
    assert!(j.commit("turn", intent("one")).await.is_err());
    assert!(j
        .commit(
            "turn",
            RunMutation::ToolOutcome {
                call_id: "one".into(),
                result: CanonicalItem::tool_result(
                    "other",
                    CanonicalToolOutput::text("wrong"),
                    false
                )
            }
        )
        .await
        .is_err());
    assert!(j
        .commit(
            "turn",
            RunMutation::ToolBatch {
                call_ids: vec!["one".into()]
            }
        )
        .await
        .is_err());
}
#[tokio::test]
async fn terminal_and_batch_results_are_authoritative_and_idempotent() {
    let (store, _, key, j) = fixture().await;
    prepare(&j, &["one", "two"]).await;
    for id in ["two", "one"] {
        j.commit(
            "turn",
            RunMutation::ToolOutcome {
                call_id: id.into(),
                result: CanonicalItem::tool_result(id, CanonicalToolOutput::text(id), true),
            },
        )
        .await
        .unwrap();
    }
    assert!(j
        .commit(
            "turn",
            RunMutation::ToolBatch {
                call_ids: vec!["two".into(), "one".into()]
            }
        )
        .await
        .is_err());
    j.commit(
        "turn",
        RunMutation::ToolBatch {
            call_ids: vec!["one".into(), "two".into()],
        },
    )
    .await
    .unwrap();
    let mut final_snapshot = snapshot("thread", "turn");
    final_snapshot.status = RunStatus::Completed;
    final_snapshot.last_seq = 22;
    let result = j.finalize("turn", final_snapshot.clone()).await.unwrap();
    assert_eq!(result.snapshot.items.len(), 5);
    assert_eq!(result.snapshot.last_seq, 22);
    assert_eq!(
        j.finalize("turn", final_snapshot).await.unwrap().snapshot,
        result.snapshot
    );
    let restored = StoreRuntime::open(store)
        .await
        .unwrap()
        .inspect(&key)
        .await
        .unwrap();
    assert_eq!(restored.runs[0].snapshot, result.snapshot);
}
#[tokio::test]
async fn attachment_configuration_and_old_leases_cannot_be_reused() {
    let (_, runtime, key, j) = fixture().await;
    assert!(runtime
        .attach(&key, 1, config(), "owner2".into(), "fresh".into())
        .await
        .is_err());
    j.detach().await.unwrap();
    let revision = runtime.inspect(&key).await.unwrap().revision;
    assert!(runtime
        .attach(
            &key,
            revision,
            json!({"version":1,"model":"changed"}),
            "owner2".into(),
            "fresh".into()
        )
        .await
        .is_err());
    assert!(runtime
        .attach(&key, revision, config(), "owner2".into(), "thread".into())
        .await
        .is_err());
    let new = runtime
        .attach(&key, revision, config(), "owner2".into(), "fresh".into())
        .await
        .unwrap();
    assert_eq!(new.epoch(), 2);
    assert!(matches!(
        j.replace_configuration(config()).await,
        Err(StoreError::StaleLease)
    ));
}
#[tokio::test]
async fn matching_begin_duplicate_does_not_append_inputs() {
    let (_, _, _, j) = fixture().await;
    let p = params("thread", "turn");
    assert!(j
        .begin_run(p.clone(), snapshot("thread", "turn"), json!({}))
        .await
        .unwrap());
    assert!(!j
        .begin_run(p, snapshot("thread", "turn"), json!({}))
        .await
        .unwrap());
    assert_eq!(j.record().await.unwrap().history.len(), 1);
}
#[tokio::test]
async fn backend_cas_rejects_history_loss_and_impossible_call_state() {
    let (store, _, key, j) = fixture().await;
    prepare(&j, &["one"]).await;
    let old = j.record().await.unwrap();
    let mut erased = old.clone();
    erased.revision += 1;
    erased.history.clear();
    assert!(
        store
            .compare_exchange(&key.recovery_id, old.revision, erased)
            .await
            .is_err(),
        "CAS allowed committed history loss"
    );
    let mut impossible = old.clone();
    impossible.revision += 1;
    impossible.runs.get_mut("turn").unwrap().calls[0].committed = true;
    assert!(
        store
            .compare_exchange(&key.recovery_id, old.revision, impossible)
            .await
            .is_err(),
        "CAS accepted a committed call with no outcome"
    );
}
#[tokio::test]
async fn tool_outcome_cannot_precede_validated_model_step_completion() {
    let (_, _, _, j) = fixture().await;
    j.begin_run(
        params("thread", "turn"),
        snapshot("thread", "turn"),
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
            item: call("one"),
        },
    )
    .await
    .unwrap();
    assert!(j
        .commit(
            "turn",
            RunMutation::ToolOutcome {
                call_id: "one".into(),
                result: CanonicalItem::tool_result(
                    "one",
                    CanonicalToolOutput::text("premature"),
                    false
                )
            }
        )
        .await
        .is_err());
}
use std::sync::atomic::{AtomicUsize, Ordering};
struct ControlledStore {
    inner: MemoryStore,
    mode: AtomicUsize,
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}
impl ControlledStore {
    fn new() -> Self {
        Self {
            inner: MemoryStore::new(),
            mode: AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        }
    }
}
#[async_trait::async_trait]
impl SessionStore for ControlledStore {
    fn durable(&self) -> bool {
        false
    }
    async fn create(&self, r: SessionRecord) -> whale_store::Result<()> {
        self.inner.create(r).await
    }
    async fn load(&self, id: &str) -> whale_store::Result<Option<SessionRecord>> {
        self.inner.load(id).await
    }
    async fn list(&self) -> whale_store::Result<Vec<SessionRecord>> {
        self.inner.list().await
    }
    async fn compare_exchange(
        &self,
        id: &str,
        rev: u64,
        r: SessionRecord,
    ) -> whale_store::Result<()> {
        match self.mode.swap(0, Ordering::SeqCst) {
            1 => {
                self.entered.notify_one();
                self.release.acquire().await.unwrap().forget();
                self.inner.compare_exchange(id, rev, r).await
            }
            2 => Err(StoreError::Io("controlled write failure".into())),
            4 => Err(StoreError::Conflict),
            3 => {
                self.inner.compare_exchange(id, rev, r).await?;
                Err(StoreError::Io("lost commit acknowledgment".into()))
            }
            _ => self.inner.compare_exchange(id, rev, r).await,
        }
    }
}
#[tokio::test]
async fn dropped_waiter_does_not_cancel_fifo_commit_and_read_joins_queue() {
    let store = Arc::new(ControlledStore::new());
    let runtime = StoreRuntime::open(store.clone()).await.unwrap();
    let j = runtime
        .create(
            RecoveryKey::new(),
            config(),
            "owner".into(),
            "thread".into(),
        )
        .await
        .unwrap();
    store.mode.store(1, Ordering::SeqCst);
    let writer = j.clone();
    let task = tokio::spawn(async move {
        writer
            .begin_run(
                params("thread", "turn"),
                snapshot("thread", "turn"),
                json!({}),
            )
            .await
    });
    store.entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let reader = j.clone();
    let mut read = tokio::spawn(async move { reader.record().await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut read)
            .await
            .is_err()
    );
    store.release.add_permits(1);
    let record = read.await.unwrap().unwrap();
    assert_eq!(record.history.len(), 1);
    assert!(record.runs.contains_key("turn"));
}
#[tokio::test]
async fn unknown_commit_outcome_poisons_live_journal_and_startup_recovers_actual_storage() {
    let store = Arc::new(ControlledStore::new());
    let runtime = StoreRuntime::open(store.clone()).await.unwrap();
    let key = RecoveryKey::new();
    let j = runtime
        .create(key.clone(), config(), "owner".into(), "thread".into())
        .await
        .unwrap();
    store.mode.store(3, Ordering::SeqCst);
    assert!(matches!(
        j.begin_run(
            params("thread", "turn"),
            snapshot("thread", "turn"),
            json!({})
        )
        .await,
        Err(StoreError::Io(_))
    ));
    assert!(matches!(
        j.commit(
            "turn",
            RunMutation::ModelInput {
                step_id: "late".into(),
                step_index: 0,
                request: json!({})
            }
        )
        .await,
        Err(StoreError::Poisoned(_))
    ));
    assert!(matches!(j.detach().await, Err(StoreError::Poisoned(_))));
    assert_eq!(runtime.inspect(&key).await.unwrap().history.len(), 1);
    let reopened = StoreRuntime::open(store)
        .await
        .unwrap()
        .inspect(&key)
        .await
        .unwrap();
    assert!(!reopened.attached);
    assert_eq!(reopened.runs[0].snapshot.status, RunStatus::Failed);
    assert!(reopened.runs[0].model_inputs.is_empty());
}
#[tokio::test]
async fn failed_dispatch_intent_never_commits_or_allows_later_dispatch() {
    let store = Arc::new(ControlledStore::new());
    let runtime = StoreRuntime::open(store.clone()).await.unwrap();
    let key = RecoveryKey::new();
    let j = runtime
        .create(key.clone(), config(), "owner".into(), "thread".into())
        .await
        .unwrap();
    prepare(&j, &["one", "two"]).await;
    store.mode.store(2, Ordering::SeqCst);
    assert!(j.commit("turn", intent("one")).await.is_err());
    assert!(matches!(
        j.commit("turn", intent("two")).await,
        Err(StoreError::Poisoned(_))
    ));
    let persisted = store.load(&key.recovery_id).await.unwrap().unwrap();
    assert!(persisted.runs["turn"]
        .calls
        .iter()
        .all(|c| c.intent.is_none()));
}
#[tokio::test]
async fn usage_overflow_is_an_error_not_a_dead_journal_worker() {
    let (_, _, _, j) = fixture().await;
    j.begin_run(
        params("thread", "turn"),
        snapshot("thread", "turn"),
        json!({}),
    )
    .await
    .unwrap();
    for index in 0..2 {
        let step = format!("step-{index}");
        j.commit(
            "turn",
            RunMutation::ModelInput {
                step_id: step.clone(),
                step_index: index,
                request: json!({}),
            },
        )
        .await
        .unwrap();
        let result = j
            .commit(
                "turn",
                RunMutation::ModelStepFinished {
                    step_id: step,
                    usage: whale_protocol::events::UsageMetrics {
                        input_tokens: u64::MAX,
                        ..Default::default()
                    },
                },
            )
            .await;
        if index == 0 {
            result.unwrap();
        } else {
            assert!(matches!(result, Err(StoreError::Invalid(_))));
        }
    }
    let mut candidate = snapshot("thread", "turn");
    candidate.status = RunStatus::Failed;
    assert_eq!(
        j.finalize("turn", candidate)
            .await
            .unwrap()
            .snapshot
            .usage
            .input_tokens,
        u64::MAX
    );
    assert!(
        j.record().await.is_ok(),
        "overflow killed the owned journal worker"
    );
}

#[tokio::test]
async fn finalized_run_rejects_late_tool_outcomes_without_changing_saved_result() {
    let (_, runtime, key, j) = fixture().await;
    prepare(&j, &["one"]).await;
    j.commit("turn", intent("one")).await.unwrap();
    let mut candidate = snapshot("thread", "turn");
    candidate.status = RunStatus::Cancelled;
    let final_result = j.finalize("turn", candidate).await.unwrap();
    assert!(j
        .commit(
            "turn",
            RunMutation::ToolOutcome {
                call_id: "one".into(),
                result: CanonicalItem::tool_result("one", CanonicalToolOutput::text("late"), false)
            }
        )
        .await
        .is_err());
    assert_eq!(
        runtime.inspect(&key).await.unwrap().runs[0].snapshot,
        final_result.snapshot
    );
}

#[tokio::test]
async fn backend_cas_conflict_is_poison_not_a_safe_application_rejection() {
    let store = Arc::new(ControlledStore::new());
    let runtime = StoreRuntime::open(store.clone()).await.unwrap();
    let j = runtime
        .create(
            RecoveryKey::new(),
            config(),
            "owner".into(),
            "thread".into(),
        )
        .await
        .unwrap();
    store.mode.store(4, Ordering::SeqCst);
    assert!(matches!(
        j.begin_run(
            params("thread", "turn"),
            snapshot("thread", "turn"),
            json!({})
        )
        .await,
        Err(StoreError::Poisoned(_))
    ));
    assert!(matches!(j.record().await, Err(StoreError::Poisoned(_))));
}
