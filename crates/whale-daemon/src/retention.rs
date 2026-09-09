//! Completed payloads expire independently of accepted identities.
use super::*;
use std::{
    collections::HashSet,
    ops::{Deref, DerefMut},
    sync::{Mutex as StdMutex, Weak},
};
use whale_protocol::retention::{RetentionPolicy, RunRetentionPolicy, RUN_EXPIRED};

#[derive(Default)]
pub(super) struct RunRegistry {
    live: HashMap<(String, String), Arc<RunRecord>>,
    expired: HashMap<(String, String), String>,
    delivered: HashMap<(String, String), tokio::time::Instant>,
}

#[derive(Clone)]
struct RetirementCandidate {
    key: (String, String),
    run: Arc<RunRecord>,
}
impl Deref for RunRegistry {
    type Target = HashMap<(String, String), Arc<RunRecord>>;
    fn deref(&self) -> &Self::Target {
        &self.live
    }
}
impl DerefMut for RunRegistry {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.live
    }
}
impl RunRegistry {
    pub(super) fn expired(&self, key: &(String, String), owner: &str) -> bool {
        self.expired.get(key).is_some_and(|saved| saved == owner)
    }
    pub(super) fn expired_error() -> JSONRPCError {
        JSONRPCError::new(RUN_EXPIRED, "RunExpired", None)
    }
    pub(super) fn delivered(&mut self, run: &Arc<RunRecord>, now: tokio::time::Instant) {
        let key = (run.params.thread_id.clone(), run.params.turn_id.clone());
        if self
            .live
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, run))
        {
            self.delivered.entry(key).or_insert(now);
        }
    }
    pub(super) fn remove_session(&mut self, thread: &str, owner: &str) {
        self.live
            .retain(|(session, _), run| session != thread || run.owner != owner);
        self.expired
            .retain(|(session, _), saved| session != thread || saved != owner);
        self.delivered.retain(|key, _| self.live.contains_key(key));
    }
    fn candidates(
        &self,
        policy: &RunRetentionPolicy,
        now: tokio::time::Instant,
    ) -> Vec<RetirementCandidate> {
        let mut retire = HashSet::new();
        let mut sessions: HashMap<String, Vec<((String, String), tokio::time::Instant)>> =
            HashMap::new();
        for (key, at) in &self.delivered {
            if !self.live.contains_key(key) {
                continue;
            }
            if policy.terminal_ttl_ms.is_some_and(|ttl| {
                now.saturating_duration_since(*at) >= std::time::Duration::from_millis(ttl)
            }) {
                retire.insert(key.clone());
            } else {
                sessions
                    .entry(key.0.clone())
                    .or_default()
                    .push((key.clone(), *at));
            }
        }
        if let Some(maximum) = policy.max_terminal_runs_per_session {
            for records in sessions.values_mut() {
                records.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
                let excess = (records.len() as u64).saturating_sub(maximum) as usize;
                retire.extend(records.iter().take(excess).map(|(key, _)| key.clone()));
            }
        }
        retire
            .into_iter()
            .filter_map(|key| {
                self.live
                    .get(&key)
                    .cloned()
                    .map(|run| RetirementCandidate { key, run })
            })
            .collect()
    }

    fn retire(&mut self, candidate: &RetirementCandidate) -> bool {
        if !self
            .live
            .get(&candidate.key)
            .is_some_and(|current| Arc::ptr_eq(current, &candidate.run))
        {
            return false;
        }
        self.live.remove(&candidate.key);
        self.expired
            .insert(candidate.key.clone(), candidate.run.owner.clone());
        self.delivered.remove(&candidate.key);
        true
    }
}

async fn retire_candidate(
    runs: &Arc<Mutex<RunRegistry>>,
    views: &super::session_views::SessionViewRegistry,
    management: &super::session_management::SessionManagementRegistry,
    interactions: &super::interactions::InteractionRegistry,
    candidate: &RetirementCandidate,
) -> bool {
    if !interactions.retire_run(
        &candidate.run.owner,
        &candidate.key.0,
        &candidate.key.1,
        Arc::as_ptr(&candidate.run) as usize,
    ) {
        return false;
    }
    if !management
        .retire_run(
            views,
            &candidate.run.owner,
            &candidate.key.0,
            &candidate.key.1,
            Arc::as_ptr(&candidate.run) as usize,
        )
        .await
    {
        return false;
    }
    runs.lock().await.retire(candidate)
}

pub(super) struct Retention {
    pub(super) policy: RetentionPolicy,
    worker: StdMutex<Option<tokio::task::JoinHandle<()>>>,
    stores: Arc<StdMutex<Vec<Weak<whale_store::StoreRuntime>>>>,
}
impl Default for Retention {
    fn default() -> Self {
        Self::new(RetentionPolicy::default())
    }
}
impl Retention {
    fn new(policy: RetentionPolicy) -> Self {
        Self {
            policy,
            worker: StdMutex::new(None),
            stores: Arc::new(StdMutex::new(Vec::new())),
        }
    }
    pub(super) fn start(
        &self,
        runs: &Arc<Mutex<RunRegistry>>,
        views: &super::session_views::SessionViewRegistry,
        management: &super::session_management::SessionManagementRegistry,
        interactions: &super::interactions::InteractionRegistry,
        store: Option<&Arc<whale_store::StoreRuntime>>,
    ) {
        if let Some(store) = store {
            let candidate = Arc::downgrade(store);
            let mut stores = self.stores.lock().unwrap();
            stores.retain(|existing| existing.strong_count() > 0);
            if !stores.iter().any(|existing| existing.ptr_eq(&candidate)) {
                stores.push(candidate);
            }
        }
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        let mut worker = self.worker.lock().unwrap();
        if worker.is_some() {
            return;
        }
        let policy = self.policy.clone();
        let runs = Arc::downgrade(runs);
        let views = views.clone();
        let management = management.clone();
        let interactions = interactions.clone();
        let stores = self.stores.clone();
        let period = std::time::Duration::from_millis(policy.sweep_interval_ms);
        let Some(first) = tokio::time::Instant::now().checked_add(period) else {
            return;
        };
        *worker = Some(tokio::spawn(async move {
            let mut timer = tokio::time::interval_at(first, period);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                timer.tick().await;
                let Some(registry) = runs.upgrade() else {
                    break;
                };
                let candidates = registry
                    .lock()
                    .await
                    .candidates(&policy.runs, tokio::time::Instant::now());
                for candidate in candidates {
                    retire_candidate(&registry, &views, &management, &interactions, &candidate)
                        .await;
                }
                management.prune_closed();
                drop(registry);
                if policy.store.is_enabled() {
                    let candidates = {
                        let mut registered = stores.lock().unwrap();
                        registered.retain(|store| store.strong_count() > 0);
                        registered.clone()
                    };
                    // Clones may select distinct stores. Keep each live binding, without
                    // retaining every backend while another one's I/O is pending.
                    for candidate in candidates {
                        if let Some(store) = candidate.upgrade() {
                            let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
                            if let Err(error) = store.sweep_retention(&policy.store, now).await {
                                warn!(error=%error,"Session retention sweep failed");
                            }
                        }
                    }
                }
            }
        }));
    }
}
impl Drop for Retention {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.get_mut().unwrap().take() {
            worker.abort();
        }
    }
}
impl DaemonServer {
    /// Configure before sharing the server. Cleanup runs even while connections are idle.
    pub fn with_retention_policy(mut self, policy: RetentionPolicy) -> Result<Self, String> {
        policy.validate()?;
        if Arc::strong_count(&self.retention) != 1
            || self.retention.worker.lock().unwrap().is_some()
        {
            return Err(
                "Retention policy must be configured before sharing or starting maintenance".into(),
            );
        }
        if tokio::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(policy.sweep_interval_ms))
            .is_none()
        {
            return Err("sweep_interval_ms exceeds the scheduler range".into());
        }
        self.retention = Arc::new(Retention::new(policy));
        self.retention.start(
            &self.runs,
            &self.session_views,
            &self.session_management,
            &self.interactions,
            self.store.as_ref(),
        );
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Sink;
    #[async_trait]
    impl OutgoingTransport for Sink {
        async fn send_line(&self, _: &str) -> std::io::Result<()> {
            Ok(())
        }
    }
    fn fixture() -> DaemonServer {
        let gate = Arc::new(ApprovalGate::new());
        let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
            Arc::new(ToolRegistry::new()),
            gate.clone(),
        )))
        .with_stream_provider(Arc::new(|_, _| {
            Ok(Box::pin(futures::stream::iter([Ok(
                AgentStreamEvent::TurnCompleted {
                    thread_id: "model".into(),
                    turn_id: "model".into(),
                    usage: Default::default(),
                },
            )])))
        }));
        DaemonServer::new(Arc::new(engine), gate)
            .with_retention_policy(
                serde_json::from_value(
                    json!({"sweep_interval_ms":2,"runs":{"terminal_ttl_ms":20}}),
                )
                .unwrap(),
            )
            .unwrap()
    }
    async fn completed(server: &DaemonServer) -> Weak<RunRecord> {
        let writer = AnyTransportWriter::new(Arc::new(Sink));
        for (method, params) in [
            (
                "protocol.initialize",
                json!({"client":{"name":"test","version":"1"},"protocol_versions":[1],"required_capabilities":[]}),
            ),
            (
                "session.start_thread",
                json!({"session_id":"session","model":"mock","provider_config":{"api":"openai_responses","auth":{"type":"none"}}}),
            ),
            (
                "thread.start_turn",
                json!({"thread_id":"session","turn_id":"run","input_items":[]}),
            ),
        ] {
            server
                .handle_message(
                    &json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}).to_string(),
                    &writer,
                )
                .await;
        }
        let run = server
            .runs
            .lock()
            .await
            .get(&("session".into(), "run".into()))
            .unwrap()
            .clone();
        let mut delivery = run.terminal_delivery.subscribe();
        while delivery.borrow_and_update().is_none() {
            delivery.changed().await.unwrap();
        }
        Arc::downgrade(&run)
    }
    #[tokio::test]
    async fn actual_idle_clock_releases_run_without_any_followup_rpc() {
        let server = fixture();
        let run = completed(&server).await;
        assert!(run.upgrade().is_some());
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        assert!(
            run.upgrade().is_none(),
            "the autonomous worker must release the full run payload"
        );
        assert_eq!(server.runs.lock().await.expired.len(), 1);
    }
    #[tokio::test(start_paused = true)]
    async fn maintenance_has_one_worker_and_does_not_keep_runtime_owner_alive() {
        let server = fixture();
        let _run = completed(&server).await;
        let weak_registry = Arc::downgrade(&server.runs);
        let task = server
            .retention
            .worker
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .id();
        server.retention.start(
            &server.runs,
            &server.session_views,
            &server.session_management,
            &server.interactions,
            server.store.as_ref(),
        );
        assert_eq!(
            server
                .retention
                .worker
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .id(),
            task
        );
        let monitor = server
            .retention
            .worker
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .abort_handle();
        drop(server);
        tokio::task::yield_now().await;
        assert!(weak_registry.upgrade().is_none());
        assert!(
            monitor.is_finished(),
            "last owner drop must stop the worker without another request"
        );
    }
    #[tokio::test(start_paused = true)]
    async fn sweep_boundary_releases_registry_payload_but_keeps_inflight_reference() {
        let server = fixture();
        let _weak = completed(&server).await;
        let registry = server.runs.lock().await;
        let key = ("session".into(), "run".into());
        let held = registry.get(&key).unwrap().clone();
        let at = registry.delivered[&key];
        assert!(registry
            .candidates(
                &server.retention.policy.runs,
                at + std::time::Duration::from_millis(19)
            )
            .is_empty());
        let candidates = registry.candidates(
            &server.retention.policy.runs,
            at + std::time::Duration::from_millis(20),
        );
        assert_eq!(candidates.len(), 1);
        drop(registry);
        server
            .session_views
            .retire_run(&key.0, &key.1, Arc::as_ptr(&candidates[0].run) as usize);
        let mut registry = server.runs.lock().await;
        assert!(registry.retire(&candidates[0]));
        assert!(registry.expired(&key, &held.owner));
        assert!(held.snapshot.lock().await.status.is_terminal());
    }
    #[tokio::test(start_paused = true)]
    async fn delayed_old_delivery_cannot_retire_a_reused_identity() {
        let old_server = fixture();
        let old = completed(&old_server).await.upgrade().unwrap();
        let new_server = fixture();
        let new = completed(&new_server).await.upgrade().unwrap();
        assert_ne!(old.owner, new.owner);
        new.snapshot.lock().await.status = RunStatus::Running;
        let key = ("session".into(), "run".into());
        let mut registry = RunRegistry::default();
        registry.insert(key.clone(), old.clone());
        // EOF removed the old owner before the delayed delivery registration.
        registry.remove_session("session", &old.owner);
        registry.insert(key.clone(), new.clone());
        let at = tokio::time::Instant::now();
        registry.delivered(&old, at);
        assert!(
            registry
                .candidates(
                    &old_server.retention.policy.runs,
                    at + std::time::Duration::from_millis(20)
                )
                .is_empty(),
            "the old invocation must not make the new active run eligible"
        );
        assert!(Arc::ptr_eq(registry.get(&key).unwrap(), &new));
    }

    #[tokio::test(start_paused = true)]
    async fn replay_is_scrubbed_before_exact_run_identity_becomes_expired() {
        let server = fixture();
        let run = completed(&server).await.upgrade().unwrap();
        let key = ("session".to_string(), "run".to_string());
        let snapshot = server
            .session_views
            .get(
                &run.owner,
                &whale_protocol::session_views::GetSessionParams {
                    thread_id: key.0.clone(),
                    history_limit: 8,
                },
            )
            .unwrap();
        let after = whale_protocol::session_views::SessionCursor {
            seq: 0,
            ..snapshot.cursor.clone()
        };
        let at = server.runs.lock().await.delivered[&key];
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let sweep = tokio::spawn({
            let server = server.clone();
            let entered = entered.clone();
            let release = release.clone();
            async move {
                let candidates = server.runs.lock().await.candidates(
                    &server.retention.policy.runs,
                    at + std::time::Duration::from_millis(20),
                );
                assert_eq!(candidates.len(), 1);
                let candidate = &candidates[0];
                let scrubbed = server.session_views.retire_run(
                    &candidate.key.0,
                    &candidate.key.1,
                    Arc::as_ptr(&candidate.run) as usize,
                );
                assert!(scrubbed);
                entered.add_permits(1);
                release.acquire().await.unwrap().forget();
                assert!(server.runs.lock().await.retire(candidate));
            }
        });
        entered.acquire().await.unwrap().forget();

        {
            let registry = server.runs.lock().await;
            assert!(registry.contains_key(&key));
            assert!(!registry.expired(&key, &run.owner));
        }
        let replay = server
            .session_views
            .subscribe(
                &run.owner,
                &whale_protocol::session_views::SubscribeSessionParams {
                    thread_id: key.0.clone(),
                    after,
                    through: None,
                    limit: 8,
                },
            )
            .unwrap();
        assert_eq!(
            replay.gap.unwrap().reason,
            whale_protocol::session_views::ReplayGapReason::Retention
        );

        release.add_permits(1);
        sweep.await.unwrap();
        assert!(server.runs.lock().await.expired(&key, &run.owner));
    }

    #[tokio::test(start_paused = true)]
    async fn missing_or_replaced_view_never_exposes_run_expired() {
        let server = fixture();
        let run = completed(&server).await.upgrade().unwrap();
        let key = ("session".to_string(), "run".to_string());
        let at = server.runs.lock().await.delivered[&key];
        let candidate = server
            .runs
            .lock()
            .await
            .candidates(
                &server.retention.policy.runs,
                at + std::time::Duration::from_millis(20),
            )
            .pop()
            .unwrap();
        server.session_views.remove(&key.0, &run.owner);
        assert!(
            !retire_candidate(
                &server.runs,
                &server.session_views,
                &server.session_management,
                &server.interactions,
                &candidate,
            )
            .await
        );
        let registry = server.runs.lock().await;
        assert!(registry.contains_key(&key));
        assert!(!registry.expired(&key, &run.owner));
    }

    #[tokio::test(start_paused = true)]
    async fn failed_terminal_projection_never_expires_an_exposed_active_run() {
        let server = fixture();
        let run = completed(&server).await.upgrade().unwrap();
        let key = ("session".to_string(), "run".to_string());
        server.session_views.remove(&key.0, &run.owner);
        server
            .session_views
            .insert(
                &run.owner,
                key.0.clone(),
                None,
                serde_json::Map::new(),
                Vec::new(),
                10,
                AnyTransportWriter::new(Arc::new(Sink)),
            )
            .unwrap();
        let mut active = run.snapshot.lock().await.clone();
        active.status = RunStatus::Running;
        active.items.clear();
        active.result = None;
        active.error = None;
        active.last_seq = 0;
        server
            .session_views
            .begin_run(&run.owner, active, Arc::as_ptr(&run) as usize, 10)
            .unwrap();
        let mut invalid_terminal = run.snapshot.lock().await.clone();
        invalid_terminal.last_seq = invalid_terminal.last_seq.saturating_add(1);
        assert!(server
            .session_views
            .apply_run_event(
                &run.owner,
                RunEvent {
                    thread_id: key.0.clone(),
                    turn_id: key.1.clone(),
                    seq: invalid_terminal.last_seq.saturating_add(1),
                    payload: RunEventPayload::Finished {
                        snapshot: invalid_terminal,
                    },
                },
                20,
            )
            .is_err());

        let at = server.runs.lock().await.delivered[&key];
        let candidate = server
            .runs
            .lock()
            .await
            .candidates(
                &server.retention.policy.runs,
                at + std::time::Duration::from_millis(20),
            )
            .pop()
            .unwrap();
        assert!(
            !retire_candidate(
                &server.runs,
                &server.session_views,
                &server.session_management,
                &server.interactions,
                &candidate,
            )
            .await
        );
        let view = server
            .session_views
            .get(
                &run.owner,
                &whale_protocol::session_views::GetSessionParams {
                    thread_id: key.0.clone(),
                    history_limit: 8,
                },
            )
            .unwrap();
        assert_eq!(
            view.active_run.unwrap().snapshot.turn_id,
            key.1,
            "failed terminal projection still exposes the active payload"
        );
        let registry = server.runs.lock().await;
        assert!(registry.contains_key(&key));
        assert!(!registry.expired(&key, &run.owner));
    }
}
