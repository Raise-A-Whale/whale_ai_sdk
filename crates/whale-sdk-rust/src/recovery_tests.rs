use crate::*;
use serde_json::json;
use whale_protocol::recovery::*;

async fn peer(recovery: bool) -> (WhaleClient, mpsc::Receiver<String>) {
    let (tx, mut rx) = mpsc::channel(32);
    let client = WhaleClient {
        inner: Arc::new(ClientInner {
            state: ClientState::new(),
            writer: ManagedWriter::channel(tx),
            compatibility_owner: None,
        }),
    };
    let c = client.clone();
    let init = tokio::spawn(async move { c.initialize().await });
    let request = initialization_tests::next(&mut rx).await;
    let mut reply = initialization_tests::valid_reply(&request);
    if recovery {
        reply["capabilities"]
            .as_array_mut()
            .unwrap()
            .push(json!(CAPABILITY_SESSION_RECOVERY));
    }
    respond(&client, &request, reply);
    init.await.unwrap().unwrap();
    (client, rx)
}
fn respond(client: &WhaleClient, request: &Value, result: Value) {
    client.inner.state.incoming(
        &json!({"jsonrpc":"2.0","id":request["id"],"result":result}).to_string(),
        &client.inner.writer,
    );
}
fn reject(client: &WhaleClient, request: &Value, code: i64) {
    client.inner.state.incoming(
        &json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":code,"message":"rejected"}})
            .to_string(),
        &client.inner.writer,
    );
}
struct Lookup(Arc<std::sync::atomic::AtomicUsize>);
#[async_trait]
impl HostTool for Lookup {
    fn name(&self) -> &str {
        "lookup"
    }
    fn description(&self) -> &str {
        "lookup"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object"})
    }
    async fn execute(&self, _: Value) -> Result<CanonicalToolOutput, String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(CanonicalToolOutput::text("done"))
    }
}
fn agent(client: &WhaleClient) -> (Agent, Arc<std::sync::atomic::AtomicUsize>) {
    let mut def = AgentDefinition::new("business", "model");
    def.tool_names = vec!["lookup".into()];
    def.max_steps = 7;
    def.timeout_ms = Some(5000);
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    (
        client
            .agent(def, vec![Arc::new(Lookup(count.clone()))])
            .unwrap(),
        count,
    )
}
fn accepted(request: &Value) -> Value {
    json!({"thread":{"thread_id":request["params"]["session"]["session_id"],"created_at":"2026-09-08T00:00:00Z"},"key":request["params"]["key"],"epoch":1})
}
#[tokio::test]
async fn recovery_requires_optional_capability_before_any_mutation() {
    let (client, mut rx) = peer(false).await;
    assert!(matches!(
        agent(&client)
            .0
            .create_persistent_session(&RecoveryKey::new())
            .await,
        Err(SdkError::ProtocolCompatibility(_))
    ));
    assert!(rx.try_recv().is_err());
    assert!(client.inner.state.tools.is_empty());
    assert!(!client.inner.state.closed.load(Ordering::SeqCst));
    client.close().await;
}
#[tokio::test]
async fn staged_callbacks_are_inactive_until_persistent_ack_and_defaults_survive() {
    let (client, mut rx) = peer(true).await;
    let (agent, count) = agent(&client);
    let key = RecoveryKey::new();
    let k = key.clone();
    let task = tokio::spawn(async move { agent.create_persistent_session(&k).await });
    let request = initialization_tests::next(&mut rx).await;
    let sid = request["params"]["session"]["session_id"].as_str().unwrap();
    assert!(uuid::Uuid::parse_str(sid).is_ok());
    assert_eq!(
        request["params"]["run_defaults"],
        json!({"max_steps":7,"timeout_ms":5000})
    );
    let reverse = json!({"jsonrpc":"2.0","id":"early","method":"tool.execute_host","params":{
        "thread_id":sid,"binding_id":request["params"]["session"]["tools"][0]["binding_id"],"call_id":"c","name":"lookup","arguments":{}}});
    client
        .inner
        .state
        .incoming(&reverse.to_string(), &client.inner.writer);
    assert!(initialization_tests::next(&mut rx)
        .await
        .get("error")
        .is_some());
    assert_eq!(count.load(Ordering::SeqCst), 0);
    respond(&client, &request, accepted(&request));
    let session = task.await.unwrap().unwrap();
    assert_eq!(session.recovery_key(), Some(&key));
    assert_eq!(session.max_steps, 7);
    assert_eq!(session.timeout_ms, Some(5000));
    assert!(client.inner.state.ensure_session_open(session.id()).is_ok());
    client.close().await;
}
#[tokio::test]
async fn abandoned_creation_waiter_settles_then_detaches_unclaimed_session() {
    let (client, mut rx) = peer(true).await;
    let agent = agent(&client).0;
    let key = RecoveryKey::new();
    let task = tokio::spawn(async move { agent.create_persistent_session(&key).await });
    let request = initialization_tests::next(&mut rx).await;
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    assert_eq!(client.inner.state.tool_bindings.len(), 1);
    respond(&client, &request, accepted(&request));
    let sid = request["params"]["session"]["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let request = initialization_tests::next(&mut rx).await;
    assert_eq!(
        request["method"],
        whale_protocol::sessions::METHOD_SESSION_CLOSE
    );
    respond(&client, &request, json!({"thread_id":sid,"closed":true}));
    tokio::time::timeout(Duration::from_secs(1), async {
        while !client.inner.state.tool_bindings.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(client.inner.state.tool_bindings.is_empty());
    client.close().await;
}
#[tokio::test]
async fn explicit_recovery_rejection_rolls_back_but_uncertain_reply_fails_closed() {
    for code in [
        -32601,
        -32602,
        RECOVERY_UNAVAILABLE,
        RECOVERY_REJECTED,
        STORE_FAILED,
        -32603,
    ] {
        let (client, mut rx) = peer(true).await;
        let agent = agent(&client).0;
        let task =
            tokio::spawn(async move { agent.create_persistent_session(&RecoveryKey::new()).await });
        let request = initialization_tests::next(&mut rx).await;
        reject(&client, &request, code);
        assert!(task.await.unwrap().is_err());
        assert!(client.inner.state.tools.is_empty());
        assert!(client.inner.state.tool_bindings.is_empty());
        assert_eq!(
            client.inner.state.closed.load(Ordering::SeqCst),
            matches!(code, STORE_FAILED | -32603)
        );
        client.close().await;
    }
}
#[tokio::test]
async fn wrong_persistent_identity_and_invalid_epoch_fail_closed() {
    for field in ["thread", "key", "epoch"] {
        let (client, mut rx) = peer(true).await;
        let agent = agent(&client).0;
        let task =
            tokio::spawn(async move { agent.create_persistent_session(&RecoveryKey::new()).await });
        let request = initialization_tests::next(&mut rx).await;
        let mut reply = accepted(&request);
        match field {
            "thread" => reply["thread"]["thread_id"] = json!("wrong"),
            "key" => reply["key"] = serde_json::to_value(RecoveryKey::new()).unwrap(),
            _ => reply["epoch"] = json!(0),
        }
        respond(&client, &request, reply);
        assert!(task.await.unwrap().is_err());
        assert!(client.inner.state.closed.load(Ordering::SeqCst));
        assert!(client.inner.state.tools.is_empty());
    }
}

#[tokio::test]
async fn abandoned_acknowledgment_still_observes_commit_failure_and_closes_connection() {
    let (client, mut rx) = peer(true).await;
    let agent = agent(&client).0;
    let creation =
        tokio::spawn(async move { agent.create_persistent_session(&RecoveryKey::new()).await });
    let request = initialization_tests::next(&mut rx).await;
    respond(&client, &request, accepted(&request));
    let session = creation.await.unwrap().unwrap();
    assert!(session
        .acknowledge_unknown(0, vec!["effect".into()])
        .await
        .is_err());
    assert!(rx.try_recv().is_err());
    let ack = tokio::spawn(async move {
        session
            .acknowledge_unknown(u64::MAX, vec!["effect".into()])
            .await
    });
    let request = initialization_tests::next(&mut rx).await;
    assert_eq!(request["method"], METHOD_RECOVERY_ACKNOWLEDGE);
    assert_eq!(
        request["params"]["expected_revision"].as_u64(),
        Some(u64::MAX)
    );
    ack.abort();
    assert!(matches!(ack.await, Err(error) if error.is_cancelled()));
    reject(&client, &request, STORE_FAILED);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !client.inner.state.closed.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(client.inner.state.tools.is_empty());
}

#[tokio::test]
async fn abandoned_forget_waiter_preserves_request_and_wrong_result_fails_closed() {
    let (client, mut rx) = peer(true).await;
    let c = client.clone();
    let key = RecoveryKey::new();
    let forget = tokio::spawn(async move { c.forget_session(&key, u64::MAX).await });
    let request = initialization_tests::next(&mut rx).await;
    forget.abort();
    assert!(matches!(forget.await, Err(error) if error.is_cancelled()));
    assert_eq!(client.inner.state.pending.len(), 1);
    respond(
        &client,
        &request,
        json!({"recovery_id":uuid::Uuid::new_v4().to_string(),"forgotten":true}),
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        while !client.inner.state.closed.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(client.inner.state.pending.is_empty());
}

#[tokio::test]
async fn rejected_close_of_unclaimed_attachment_fails_the_connection_closed() {
    for code in [-32601, -32602] {
        let (client, mut rx) = peer(true).await;
        let agent = agent(&client).0;
        let creation =
            tokio::spawn(async move { agent.create_persistent_session(&RecoveryKey::new()).await });
        let request = initialization_tests::next(&mut rx).await;
        creation.abort();
        assert!(matches!(creation.await, Err(error) if error.is_cancelled()));
        respond(&client, &request, accepted(&request));
        let close = initialization_tests::next(&mut rx).await;
        assert_eq!(
            close["method"],
            whale_protocol::sessions::METHOD_SESSION_CLOSE
        );
        reject(&client, &close, code);
        tokio::time::timeout(Duration::from_millis(100), async {
            while !client.inner.state.closed.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("failed cleanup must not leave an unclaimed attachment alive");
        assert!(client.inner.state.tools.is_empty());
        assert!(client.inner.state.tool_bindings.is_empty());
    }
}

mod tool_pack {
    use super::*;
    use crate::tool_pack_tests::{assert_build_state_removed, EventLog, TransactionProbePack};

    const PROVIDER_REFERENCE_SENTINEL: &str = "WHALE_TEST_PROVIDER_CREDENTIAL";

    fn pack_agent(client: &WhaleClient, pack: Arc<TransactionProbePack>) -> Agent {
        let mut definition = AgentDefinition::new("persistent-pack-agent", "fixture-model");
        definition.tool_names = vec!["a".into()];
        definition.provider_config = Some(ProviderConfig {
            api: ProviderApi::OpenaiChatCompletions,
            base_url: None,
            auth: Some(ProviderAuth::Env {
                variable: PROVIDER_REFERENCE_SENTINEL.into(),
            }),
        });
        client
            .agent_with_tool_packs(definition, Vec::new(), vec![pack])
            .unwrap()
    }

    fn snapshot(key: &RecoveryKey, revision: u64) -> Value {
        json!({
            "recovery_id": key.recovery_id,
            "revision": revision,
            "epoch": 1,
            "attached": false,
            "configuration": {},
            "history": [],
            "runs": [],
            "unknown_executions": []
        })
    }

    async fn start_attach(
        client: &WhaleClient,
        rx: &mut mpsc::Receiver<String>,
        agent: Agent,
        key: RecoveryKey,
    ) -> tokio::task::JoinHandle<Result<WhaleThread, SdkError>> {
        let expected = key.clone();
        let task = tokio::spawn(async move { agent.recover_session(&key).await });
        let inspect = initialization_tests::next(rx).await;
        assert_eq!(inspect["method"], METHOD_RECOVERY_INSPECT);
        assert_eq!(inspect["params"]["key"], json!(expected));
        respond(client, &inspect, snapshot(&expected, 2));
        task
    }

    async fn close_live(
        client: &WhaleClient,
        rx: &mut mpsc::Receiver<String>,
        thread: WhaleThread,
    ) {
        let sid = thread.id().to_owned();
        let task = tokio::spawn(async move { thread.close().await });
        let request = initialization_tests::next(rx).await;
        assert_eq!(
            request["method"],
            whale_protocol::sessions::METHOD_SESSION_CLOSE
        );
        respond(client, &request, json!({"thread_id": sid, "closed": true}));
        assert!(task.await.unwrap().unwrap());
    }

    #[tokio::test]
    async fn persistent_create_and_attach_bind_distinct_safe_contexts_and_close_normally() {
        let (client, mut rx) = peer(true).await;
        let log = Arc::new(EventLog::default());
        let pack = Arc::new(TransactionProbePack::new("A", "a", log.clone()));
        let agent = pack_agent(&client, pack.clone());
        let key = RecoveryKey::new();

        let creating = tokio::spawn({
            let agent = agent.clone();
            let key = key.clone();
            async move { agent.create_persistent_session(&key).await }
        });
        let create = initialization_tests::next(&mut rx).await;
        assert_eq!(create["method"], METHOD_SESSION_CREATE_PERSISTENT);
        assert_eq!(pack.bind_count(), 1);
        let first_sid = create["params"]["session"]["session_id"]
            .as_str()
            .unwrap()
            .to_owned();
        respond(&client, &create, accepted(&create));
        let first = creating.await.unwrap().unwrap();
        assert_eq!(first.id(), first_sid);
        close_live(&client, &mut rx, first).await;

        let attaching = start_attach(&client, &mut rx, agent, key.clone()).await;
        let attach = initialization_tests::next(&mut rx).await;
        assert_eq!(attach["method"], METHOD_RECOVERY_ATTACH);
        assert_eq!(pack.bind_count(), 2);
        let second_sid = attach["params"]["session"]["session_id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_ne!(first_sid, second_sid);
        respond(&client, &attach, accepted(&attach));
        let second = attaching.await.unwrap().unwrap();
        assert_eq!(second.id(), second_sid);
        close_live(&client, &mut rx, second).await;

        let contexts = pack.contexts();
        assert_eq!(contexts.len(), 2);
        assert_eq!(contexts[0].session_id(), first_sid);
        assert_eq!(contexts[1].session_id(), second_sid);
        assert_eq!(
            contexts[0].kind(),
            &SessionBindKind::PersistentCreate {
                recovery_id: key.recovery_id.clone()
            }
        );
        assert_eq!(
            contexts[1].kind(),
            &SessionBindKind::PersistentAttach {
                recovery_id: key.recovery_id.clone()
            }
        );
        let captured = contexts
            .iter()
            .map(|context| {
                format!(
                    "{}:{}:{:?}",
                    context.session_id(),
                    context.agent_name(),
                    context.kind()
                )
            })
            .chain(log.snapshot())
            .collect::<Vec<_>>()
            .join("|");
        assert!(!captured.contains(&key.secret));
        assert!(!captured.contains(PROVIDER_REFERENCE_SENTINEL));
        assert_eq!(pack.normal_close_count(), 2);
        assert_eq!(pack.emergency_close_count(), 0);
        client.close().await;
    }

    async fn assert_failure_cleanup(attach: bool, code: i64) {
        let (client, mut rx) = peer(true).await;
        let log = Arc::new(EventLog::default());
        let pack = Arc::new(TransactionProbePack::new("A", "a", log.clone()));
        let agent = pack_agent(&client, pack.clone());
        let key = RecoveryKey::new();
        let task = if attach {
            start_attach(&client, &mut rx, agent, key).await
        } else {
            tokio::spawn(async move { agent.create_persistent_session(&key).await })
        };
        let request = initialization_tests::next(&mut rx).await;
        assert_eq!(
            request["method"],
            if attach {
                METHOD_RECOVERY_ATTACH
            } else {
                METHOD_SESSION_CREATE_PERSISTENT
            }
        );
        assert_eq!(pack.bind_count(), 1);
        reject(&client, &request, code);
        assert!(task.await.unwrap().is_err());

        let definite = matches!(
            code,
            JSONRPCError::METHOD_NOT_FOUND
                | JSONRPCError::INVALID_PARAMS
                | RECOVERY_UNAVAILABLE
                | RECOVERY_REJECTED
        );
        assert_eq!(pack.normal_close_count(), usize::from(definite));
        assert_eq!(pack.emergency_close_count(), usize::from(!definite));
        assert_eq!(client.inner.state.closed.load(Ordering::SeqCst), !definite);
        assert_build_state_removed(&client);
        client.close().await;
    }

    #[tokio::test]
    async fn persistent_failure_classes_choose_normal_rollback_or_emergency_fence() {
        for attach in [false, true] {
            for code in [
                JSONRPCError::METHOD_NOT_FOUND,
                JSONRPCError::INVALID_PARAMS,
                RECOVERY_UNAVAILABLE,
                RECOVERY_REJECTED,
                STORE_FAILED,
                JSONRPCError::INTERNAL_ERROR,
            ] {
                assert_failure_cleanup(attach, code).await;
            }
        }
    }

    #[tokio::test]
    async fn attach_eof_emergency_closes_bound_packs_and_removes_routes() {
        let (client, mut rx) = peer(true).await;
        let log = Arc::new(EventLog::default());
        let pack = Arc::new(TransactionProbePack::new("A", "a", log));
        let agent = pack_agent(&client, pack.clone());
        let key = RecoveryKey::new();
        let task = start_attach(&client, &mut rx, agent, key).await;
        let request = initialization_tests::next(&mut rx).await;
        assert_eq!(request["method"], METHOD_RECOVERY_ATTACH);
        assert_eq!(pack.bind_count(), 1);

        client.inner.state.disconnect("fixture EOF");
        assert!(task.await.unwrap().is_err());
        assert_eq!(pack.normal_close_count(), 0);
        assert_eq!(pack.emergency_close_count(), 1);
        assert_build_state_removed(&client);
    }

    #[tokio::test]
    async fn abandoned_attach_success_quiesces_then_normal_closes_the_owned_pack() {
        let (client, mut rx) = peer(true).await;
        let log = Arc::new(EventLog::default());
        let pack = Arc::new(TransactionProbePack::new("A", "a", log.clone()));
        let agent = pack_agent(&client, pack.clone());
        let key = RecoveryKey::new();
        let task = start_attach(&client, &mut rx, agent, key).await;
        let attach = initialization_tests::next(&mut rx).await;
        assert_eq!(attach["method"], METHOD_RECOVERY_ATTACH);
        let sid = attach["params"]["session"]["session_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let binding_id = attach["params"]["session"]["tools"][0]["binding_id"]
            .as_str()
            .expect("persistent attach must stage the pack handler")
            .to_owned();
        let context = pack.contexts()[0].clone();
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        respond(&client, &attach, accepted(&attach));

        let close = initialization_tests::next(&mut rx).await;
        assert_eq!(
            close["method"],
            whale_protocol::sessions::METHOD_SESSION_CLOSE
        );
        client.inner.state.incoming(
            &json!({
                "jsonrpc": "2.0",
                "id": "late",
                "method": "tool.execute_host",
                "params": {
                    "thread_id": sid,
                    "binding_id": binding_id,
                    "call_id": "late-call",
                    "name": "a",
                    "arguments": {}
                }
            })
            .to_string(),
            &client.inner.writer,
        );
        let rejected = initialization_tests::next(&mut rx).await;
        assert_eq!(rejected["id"], "late");
        assert!(rejected.get("error").is_some());
        assert_eq!(pack.normal_close_count(), 0);
        respond(&client, &close, json!({"thread_id": sid, "closed": true}));
        log.wait_for("close:A").await;

        assert!(context.session_cancelled().is_cancelled());
        assert_eq!(pack.normal_close_count(), 1);
        assert_eq!(pack.emergency_close_count(), 0);
        assert!(client.inner.state.tools.is_empty());
        assert!(client.inner.state.tool_bindings.is_empty());
        assert_eq!(client.inner.state.attached_pack_count(&sid), 0);
        client.close().await;
    }

    #[tokio::test]
    async fn recovery_and_provider_preflight_fail_before_the_first_bind() {
        let (client, mut rx) = peer(false).await;
        let pack = Arc::new(TransactionProbePack::new(
            "A",
            "a",
            Arc::new(EventLog::default()),
        ));
        let agent = pack_agent(&client, pack.clone());
        assert!(agent
            .create_persistent_session(&RecoveryKey::new())
            .await
            .is_err());
        assert_eq!(pack.bind_count(), 0);
        assert!(rx.try_recv().is_err());
        client.close().await;

        let (client, mut rx) = peer(true).await;
        let pack = Arc::new(TransactionProbePack::new(
            "A",
            "a",
            Arc::new(EventLog::default()),
        ));
        let mut definition = AgentDefinition::new("provider-preflight", "fixture-model");
        definition.tool_names = vec!["a".into()];
        definition.provider_ref = Some("registered-provider".into());
        let agent = client
            .agent_with_tool_packs(definition, Vec::new(), vec![pack.clone()])
            .unwrap();
        let task =
            tokio::spawn(async move { agent.create_persistent_session(&RecoveryKey::new()).await });
        let inspect = initialization_tests::next(&mut rx).await;
        assert_eq!(
            inspect["method"],
            whale_protocol::models::METHOD_PROVIDER_INSPECT
        );
        reject(&client, &inspect, JSONRPCError::INVALID_PARAMS);
        assert!(task.await.unwrap().is_err());
        assert_eq!(pack.bind_count(), 0);
        assert!(rx.try_recv().is_err());
        client.close().await;
    }

    #[tokio::test]
    async fn invalid_attach_identity_is_secret_free_and_emergency_fails_closed() {
        for field in ["thread", "key", "epoch"] {
            let (client, mut rx) = peer(true).await;
            let pack = Arc::new(TransactionProbePack::new(
                "A",
                "a",
                Arc::new(EventLog::default()),
            ));
            let agent = pack_agent(&client, pack.clone());
            let key = RecoveryKey::new();
            let task = start_attach(&client, &mut rx, agent, key.clone()).await;
            let attach = initialization_tests::next(&mut rx).await;
            let mut result = accepted(&attach);
            let wrong_key = RecoveryKey::new();
            match field {
                "thread" => result["thread"]["thread_id"] = json!("wrong-session"),
                "key" => result["key"] = json!(wrong_key.clone()),
                _ => result["epoch"] = json!(0),
            }
            respond(&client, &attach, result);
            let error = match task.await.unwrap() {
                Err(error) => error,
                Ok(_) => panic!("invalid persistent identity unexpectedly succeeded"),
            };
            let rendered = error.to_string();
            assert!(!rendered.contains(&key.secret));
            assert!(!rendered.contains(&wrong_key.secret));
            assert!(client.inner.state.closed.load(Ordering::SeqCst));
            assert_eq!(pack.normal_close_count(), 0);
            assert_eq!(pack.emergency_close_count(), 1);
            assert_build_state_removed(&client);
        }
    }
}
