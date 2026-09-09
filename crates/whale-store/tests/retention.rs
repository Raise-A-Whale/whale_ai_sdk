use serde_json::{json, Value};
use std::sync::Arc;
use whale_protocol::{
    recovery::RecoveryKey,
    retention::StoreRetentionPolicy,
    runs::{RunSnapshot, RunStatus, StartTurnParams},
    CanonicalItem,
};
use whale_store::*;
fn snap() -> RunSnapshot {
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
    }
}
fn params(input: Vec<CanonicalItem>) -> StartTurnParams {
    StartTurnParams {
        thread_id: "live".into(),
        turn_id: "turn".into(),
        input_items: input,
        options: None,
        max_steps: 3,
        timeout_ms: None,
    }
}
async fn create(rt: &StoreRuntime, config: Value) -> (RecoveryKey, SessionJournal) {
    let key = RecoveryKey::new();
    let j = rt
        .create(key.clone(), config, "owner".into(), "live".into())
        .await
        .unwrap();
    (key, j)
}
#[tokio::test]
async fn ttl_boundary_future_clock_and_tombstone_identity() {
    let b = Arc::new(MemoryStore::new());
    let rt = StoreRuntime::open(b.clone()).await.unwrap();
    let (k, j) = create(&rt, json!({"value":"保留数据"})).await;
    j.detach().await.unwrap();
    let before = b.load(&k.recovery_id).await.unwrap().unwrap();
    let time = before.detached_since_ms.unwrap();
    let p = StoreRetentionPolicy {
        detached_ttl_ms: Some(100),
        ..Default::default()
    };
    assert_eq!(
        rt.sweep_retention(&p, time.saturating_sub(1))
            .await
            .unwrap()
            .retired,
        0
    );
    assert_eq!(rt.sweep_retention(&p, time + 99).await.unwrap().retired, 0);
    assert_eq!(rt.sweep_retention(&p, time + 100).await.unwrap().retired, 1);
    let record = b.load(&k.recovery_id).await.unwrap().unwrap();
    assert!(record.forgotten);
    assert!(
        record.history.is_empty()
            && record.runs.is_empty()
            && record.historical_thread_ids.is_empty()
    );
    assert_eq!(record.configuration, json!({}));
    assert!(matches!(rt.inspect(&k).await, Err(StoreError::Forgotten)));
    assert!(rt
        .attach(
            &k,
            record.revision,
            json!({}),
            "owner".into(),
            "other".into()
        )
        .await
        .is_err());
    assert!(rt
        .create(k, json!({}), "owner".into(), "other".into())
        .await
        .is_err());
}
#[tokio::test]
async fn budgets_count_utf8_and_protect_active_payload() {
    let b = Arc::new(MemoryStore::new());
    let rt = StoreRuntime::open(b.clone()).await.unwrap();
    let (active, _j) = create(&rt, json!({"large":"字".repeat(100)})).await;
    let (k, j) = create(&rt, json!({"large":"字".repeat(100)})).await;
    j.detach().await.unwrap();
    let page = b.metadata_page(None, 10).await.unwrap();
    let rec = b.load(&k.recovery_id).await.unwrap().unwrap();
    assert_eq!(
        page.iter()
            .find(|m| m.recovery_id == k.recovery_id)
            .unwrap()
            .payload_bytes,
        serde_json::to_vec(&rec).unwrap().len() as u64
    );
    let report = rt
        .sweep_retention(
            &StoreRetentionPolicy {
                max_retained_sessions: Some(1),
                max_retained_payload_bytes: Some(1),
                ..Default::default()
            },
            u64::MAX,
        )
        .await
        .unwrap();
    assert_eq!(report.retired, 1);
    assert_eq!(report.protected_active, 1);
    assert_eq!(report.remaining_sessions, 1);
    assert!(report.unmet_payload_bytes > 0);
    assert!(rt.inspect(&active).await.unwrap().attached);
}
#[tokio::test]
async fn begin_limits_reject_without_acceptance_and_duplicate_is_idempotent() {
    let b = Arc::new(MemoryStore::new());
    let rt = StoreRuntime::open(b).await.unwrap();
    let (_, j) = create(&rt, json!({"session":{"limits":{"max_history_bytes":1}}})).await;
    let before = j.record().await.unwrap();
    assert!(matches!(
        j.begin_run(
            params(vec![CanonicalItem::user_text("x")]),
            snap(),
            json!({})
        )
        .await,
        Err(StoreError::LimitExceeded(_))
    ));
    let after = j.record().await.unwrap();
    assert_eq!(before.revision, after.revision);
    assert!(after.runs.is_empty() && after.history.is_empty());
    let (_, j) = create(&rt, json!({"session":{"limits":{"max_accepted_turns":1}}})).await;
    let p = params(vec![]);
    assert!(j.begin_run(p.clone(), snap(), json!({})).await.unwrap());
    assert!(!j.begin_run(p, snap(), json!({})).await.unwrap());
    let mut final_ = snap();
    final_.status = RunStatus::Completed;
    j.finalize("turn", final_).await.unwrap();
    let mut p = params(vec![]);
    p.turn_id = "next".into();
    let mut s = snap();
    s.turn_id = "next".into();
    assert!(matches!(
        j.begin_run(p, s, json!({})).await,
        Err(StoreError::LimitExceeded(_))
    ));
}
#[tokio::test]
async fn v1_migration_grace_is_persisted_once() {
    let b = Arc::new(MemoryStore::new());
    let rt = StoreRuntime::open(b.clone()).await.unwrap();
    let (k, j) = create(&rt, json!({"old":"payload"})).await;
    j.detach().await.unwrap();
    let record = b.load(&k.recovery_id).await.unwrap().unwrap();
    let mut json = serde_json::to_value(record).unwrap();
    json["schema_version"] = json!(1);
    json["revision"] = json!(1);
    for key in [
        "created_at_ms",
        "updated_at_ms",
        "detached_since_ms",
        "retired_at_ms",
        "retirement_reason",
    ] {
        json.as_object_mut().unwrap().remove(key);
    }
    let old = Arc::new(MemoryStore::new());
    old.create(serde_json::from_value(json).unwrap())
        .await
        .unwrap();
    let first = StoreRuntime::open(old.clone()).await.unwrap();
    let migrated = old.load(&k.recovery_id).await.unwrap().unwrap();
    assert_eq!(migrated.schema_version, 2);
    assert!(migrated.detached_since_ms.is_some());
    assert_eq!(
        first.inspect(&k).await.unwrap().configuration,
        json!({"old":"payload"})
    );
    let _second = StoreRuntime::open(old.clone()).await.unwrap();
    let reopened = old.load(&k.recovery_id).await.unwrap().unwrap();
    assert_eq!(reopened.detached_since_ms, migrated.detached_since_ms);
    assert_eq!(reopened.revision, migrated.revision);
}

#[tokio::test]
async fn unknown_effects_survive_all_retention_pressure() {
    let b = Arc::new(MemoryStore::new());
    let rt = StoreRuntime::open(b.clone()).await.unwrap();
    let (k, j) = create(&rt, json!({})).await;
    j.begin_run(params(vec![]), snap(), json!({}))
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
            item: CanonicalItem::tool_call("call", None, "effect", Some(json!({})), "{}"),
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
            execution: whale_protocol::contexts::ToolExecutionRecord {
                call_id: "call".into(),
                original_arguments: json!({}),
                arguments: json!({}),
            },
        },
    )
    .await
    .unwrap();
    j.detach().await.unwrap();
    let p = StoreRetentionPolicy {
        detached_ttl_ms: Some(1),
        max_retained_sessions: Some(1),
        max_retained_payload_bytes: Some(1),
    };
    let report = rt.sweep_retention(&p, u64::MAX).await.unwrap();
    assert_eq!(report.retired, 0);
    assert_eq!(report.protected_unknown, 1);
    let s = rt.inspect(&k).await.unwrap();
    assert_eq!(s.unknown_executions.len(), 1);
    let fresh = rt
        .attach(
            &k,
            s.revision,
            json!({}),
            "new-owner".into(),
            "new-live".into(),
        )
        .await
        .unwrap();
    let s = fresh.record().await.unwrap();
    fresh
        .acknowledge(
            s.revision,
            s.unknown_executions
                .iter()
                .map(|u| u.execution_id.clone())
                .collect(),
        )
        .await
        .unwrap();
    fresh.detach().await.unwrap();
    assert_eq!(rt.sweep_retention(&p, u64::MAX).await.unwrap().retired, 1);
}
struct Gated {
    inner: MemoryStore,
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
    calls: std::sync::atomic::AtomicUsize,
}
#[async_trait::async_trait]
impl SessionStore for Gated {
    fn durable(&self) -> bool {
        false
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
    async fn retire_detached(&self, id: &str, rev: u64, now: u64, reason: &str) -> Result<bool> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
        self.inner.retire_detached(id, rev, now, reason).await
    }
}
fn gated() -> Arc<Gated> {
    Arc::new(Gated {
        inner: MemoryStore::new(),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
        calls: Default::default(),
    })
}
#[tokio::test]
async fn attach_wins_scan_race_and_stale_retirement_cannot_delete_it() {
    let b = gated();
    let rt = StoreRuntime::open(b.clone()).await.unwrap();
    let (k, j) = create(&rt, json!({})).await;
    j.detach().await.unwrap();
    let r = rt.clone();
    let task = tokio::spawn(async move {
        r.sweep_retention(
            &StoreRetentionPolicy {
                detached_ttl_ms: Some(1),
                ..Default::default()
            },
            u64::MAX,
        )
        .await
    });
    b.entered.notified().await;
    let snapshot = rt.inspect(&k).await.unwrap();
    let _fresh = rt
        .attach(
            &k,
            snapshot.revision,
            json!({}),
            "new".into(),
            "fresh".into(),
        )
        .await
        .unwrap();
    b.release.add_permits(1);
    let report = task.await.unwrap().unwrap();
    assert_eq!(report.retired, 0);
    assert_eq!(report.protected_active, 1);
    assert!(rt.inspect(&k).await.unwrap().attached);
}
#[tokio::test]
async fn aborted_sweep_waiter_keeps_mutation_owned_and_sweeps_serial() {
    let b = gated();
    let rt = StoreRuntime::open(b.clone()).await.unwrap();
    let (k, j) = create(&rt, json!({})).await;
    j.detach().await.unwrap();
    let r = rt.clone();
    let p = StoreRetentionPolicy {
        detached_ttl_ms: Some(1),
        ..Default::default()
    };
    let copy = p.clone();
    let task = tokio::spawn(async move { r.sweep_retention(&copy, u64::MAX).await });
    b.entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let r = rt.clone();
    let second = tokio::spawn(async move { r.sweep_retention(&p, u64::MAX).await });
    tokio::task::yield_now().await;
    assert_eq!(b.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    b.release.add_permits(1);
    assert_eq!(second.await.unwrap().unwrap().remaining_sessions, 0);
    assert!(matches!(rt.inspect(&k).await, Err(StoreError::Forgotten)));
}
#[tokio::test]
async fn request_admission_failure_does_not_poison_settlement() {
    let b = Arc::new(MemoryStore::new());
    let rt = StoreRuntime::open(b).await.unwrap();
    let (_, j) = create(
        &rt,
        json!({"session":{"limits":{"max_model_request_bytes":2}}}),
    )
    .await;
    j.begin_run(params(vec![]), snap(), json!({}))
        .await
        .unwrap();
    assert!(matches!(
        j.commit(
            "turn",
            RunMutation::ModelInput {
                step_id: "step".into(),
                step_index: 0,
                request: json!({"large":"data"})
            }
        )
        .await,
        Err(StoreError::LimitExceeded(_))
    ));
    let record = j.record().await.unwrap();
    assert!(record.runs["turn"].model_inputs.is_empty());
    let mut s = snap();
    s.status = RunStatus::Failed;
    j.finalize("turn", s).await.unwrap();
    j.detach().await.unwrap();
}

#[tokio::test]
async fn count_budget_uses_detachment_then_id_order_across_metadata_pages() {
    let b = Arc::new(MemoryStore::new());
    let rt = StoreRuntime::open(b.clone()).await.unwrap();
    let mut keys = Vec::new();
    for _ in 0..132 {
        let (k, j) = create(&rt, json!({})).await;
        j.detach().await.unwrap();
        keys.push(k);
    }
    let mut metadata = b.metadata_page(None, 200).await.unwrap();
    metadata.sort_by(|a, b| {
        (a.detached_since_ms, &a.recovery_id).cmp(&(b.detached_since_ms, &b.recovery_id))
    });
    let keep = metadata.last().unwrap().recovery_id.clone();
    let report = rt
        .sweep_retention(
            &StoreRetentionPolicy {
                max_retained_sessions: Some(1),
                ..Default::default()
            },
            0,
        )
        .await
        .unwrap();
    assert_eq!(report.retired, 131);
    assert_eq!(report.remaining_sessions, 1);
    for key in keys {
        assert_eq!(rt.inspect(&key).await.is_ok(), key.recovery_id == keep);
    }
}

#[tokio::test]
async fn oversized_completed_outcome_is_kept_while_next_dispatch_is_refused() {
    let b = Arc::new(MemoryStore::new());
    let rt = StoreRuntime::open(b).await.unwrap();
    let (_, j) = create(
        &rt,
        json!({"session":{"limits":{"max_history_bytes":1000}}}),
    )
    .await;
    j.begin_run(params(vec![]), snap(), json!({}))
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
    for call in ["a", "b"] {
        j.commit(
            "turn",
            RunMutation::ModelItem {
                step_id: "step".into(),
                item: CanonicalItem::tool_call(call, None, "effect", Some(json!({})), "{}"),
            },
        )
        .await
        .unwrap();
    }
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
            call_id: "a".into(),
            execution: whale_protocol::contexts::ToolExecutionRecord {
                call_id: "a".into(),
                original_arguments: json!({}),
                arguments: json!({}),
            },
        },
    )
    .await
    .unwrap();
    let output = whale_protocol::CanonicalToolOutput::text("字".repeat(2000));
    j.commit(
        "turn",
        RunMutation::ToolOutcome {
            call_id: "a".into(),
            result: CanonicalItem::tool_result("a", output.clone(), false),
        },
    )
    .await
    .unwrap();
    j.commit(
        "turn",
        RunMutation::ToolOutcome {
            call_id: "b".into(),
            result: CanonicalItem::tool_result(
                "b",
                whale_protocol::CanonicalToolOutput::text("not dispatched"),
                true,
            ),
        },
    )
    .await
    .unwrap();
    j.commit(
        "turn",
        RunMutation::ToolBatch {
            call_ids: vec!["a".into(), "b".into()],
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        j.commit(
            "turn",
            RunMutation::ModelInput {
                step_id: "next".into(),
                step_index: 1,
                request: json!({})
            }
        )
        .await,
        Err(StoreError::LimitExceeded(_))
    ));
    let mut terminal = snap();
    terminal.status = RunStatus::Failed;
    let final_ = j.finalize("turn", terminal).await.unwrap();
    assert!(final_.history.iter().any(|item|matches!(item,CanonicalItem::ToolResult{call_id,output:actual,is_error:false,..} if call_id=="a"&&actual==&output)));
    j.detach().await.unwrap();
}

#[tokio::test]
async fn large_model_output_blocks_dispatch_intent_without_discarding_history() {
    let b = Arc::new(MemoryStore::new());
    let rt = StoreRuntime::open(b).await.unwrap();
    let (_, j) = create(&rt, json!({"session":{"limits":{"max_history_bytes":300}}})).await;
    j.begin_run(params(vec![]), snap(), json!({}))
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
    let text = CanonicalItem::assistant_text(
        "large".repeat(400),
        whale_protocol::MessagePhase::FinalAnswer,
    );
    j.commit(
        "turn",
        RunMutation::ModelItem {
            step_id: "step".into(),
            item: text.clone(),
        },
    )
    .await
    .unwrap();
    j.commit(
        "turn",
        RunMutation::ModelItem {
            step_id: "step".into(),
            item: CanonicalItem::tool_call("call", None, "effect", Some(json!({})), "{}"),
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
    assert!(matches!(
        j.commit(
            "turn",
            RunMutation::DispatchIntent {
                call_id: "call".into(),
                execution: whale_protocol::contexts::ToolExecutionRecord {
                    call_id: "call".into(),
                    original_arguments: json!({}),
                    arguments: json!({})
                }
            }
        )
        .await,
        Err(StoreError::LimitExceeded(_))
    ));
    let record = j.record().await.unwrap();
    assert!(record.runs["turn"].calls[0].intent.is_none());
    assert!(record.history.contains(&text));
    let mut terminal = snap();
    terminal.status = RunStatus::Failed;
    j.finalize("turn", terminal).await.unwrap();
    assert!(j.record().await.unwrap().unknown_executions.is_empty());
}
