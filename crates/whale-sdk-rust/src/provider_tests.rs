use crate::*;
use serde_json::json;
use whale_protocol::models::InspectProviderParams;
async fn fixture() -> (WhaleClient, mpsc::Receiver<String>) {
    let (tx, mut rx) = mpsc::channel(8);
    let client = WhaleClient {
        inner: Arc::new(ClientInner {
            state: ClientState::new(),
            writer: ManagedWriter::channel(tx),
            compatibility_owner: None,
        }),
    };
    crate::initialization_tests::initialize_peer(&client, &mut rx).await;
    (client, rx)
}

struct Policy;
#[async_trait]
impl HostContextPolicy for Policy {
    async fn build(
        &self,
        r: ContextBuildRequest,
        _: CancellationSignal,
    ) -> Result<ModelContext, String> {
        Ok(ModelContext {
            system_prompt: r.system_prompt,
            items: r.history,
        })
    }
}
fn definition() -> AgentDefinition {
    let mut definition = AgentDefinition::new("native", "local-test");
    definition.provider_ref = Some("local".into());
    definition
}
fn descriptor() -> Value {
    json!({"model":"local-test","provider_ref":"local","capabilities":{
        "scope":"model","user_content":["text"],"assistant_content":["text"],"tool_result_content":["text"],
        "tool_calls":true,"reasoning_text":false,"reasoning_signatures":false,"encrypted_reasoning":false,"options":[]}})
}
async fn next(rx: &mut mpsc::Receiver<String>) -> Value {
    serde_json::from_str(
        &tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}
fn reply(client: &WhaleClient, id: &Value, result: Value) {
    client.inner.state.incoming(
        &json!({"jsonrpc":"2.0","id":id,"result":result}).to_string(),
        &client.inner.writer,
    );
}
#[tokio::test]
async fn reference_preflight_rejects_old_peer_without_publishing_bindings_or_starting() {
    let (client, mut rx) = fixture().await;
    let agent = client
        .agent(definition(), vec![])
        .unwrap()
        .with_context_policy(Arc::new(Policy));
    let creating = tokio::spawn(async move { agent.create_session().await });
    let request = next(&mut rx).await;
    assert_eq!(request["method"], "provider.inspect");
    assert!(client.inner.state.context_policies.is_empty());
    client.inner.state.incoming(&json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32601,"message":"Method not found"}}).to_string(),&client.inner.writer);
    assert!(creating.await.unwrap().is_err());
    assert!(rx.try_recv().is_err());
    assert!(client.inner.state.tools.is_empty());
    assert!(client.inner.state.tool_bindings.is_empty());
    assert!(client.inner.state.context_policies.is_empty());
    client.close().await;
}
#[tokio::test]
async fn malformed_or_wrong_inspection_never_starts_a_session() {
    for response in [
        {
            let mut r = descriptor();
            r["model"] = json!("wrong");
            r
        },
        {
            let mut r = descriptor();
            r["provider_ref"] = json!("wrong");
            r
        },
        {
            let mut r = descriptor();
            r["capabilities"]["scope"] = json!("unrecognized");
            r
        },
        json!({"model":"local-test","provider_ref":"local","capabilities":{}}),
    ] {
        let (client, mut rx) = fixture().await;
        let agent = client
            .agent(definition(), vec![])
            .unwrap()
            .with_context_policy(Arc::new(Policy));
        let creating = tokio::spawn(async move { agent.create_session().await });
        let request = next(&mut rx).await;
        assert_eq!(request["method"], "provider.inspect");
        reply(&client, &request["id"], response);
        assert!(creating.await.unwrap().is_err());
        assert!(rx.try_recv().is_err());
        assert!(client.inner.state.context_policies.is_empty());
        client.close().await;
    }
}
#[tokio::test]
async fn typed_inspection_and_agent_reference_round_trip() {
    let (client, mut rx) = fixture().await;
    let inspecting = client.clone();
    let pending = tokio::spawn(async move {
        inspecting
            .inspect_provider(InspectProviderParams {
                model: "local-test".into(),
                provider_ref: Some("local".into()),
                provider: None,
                provider_config: None,
            })
            .await
    });
    let req = next(&mut rx).await;
    reply(&client, &req["id"], descriptor());
    assert!(pending.await.unwrap().unwrap().capabilities.tool_calls);
    let agent = client.agent(definition(), vec![]).unwrap();
    let creating = tokio::spawn(async move { agent.create_session().await });
    let req = next(&mut rx).await;
    assert_eq!(req["method"], "provider.inspect");
    reply(&client, &req["id"], descriptor());
    let req = next(&mut rx).await;
    assert_eq!(req["method"], "session.start_thread");
    assert_eq!(req["params"]["provider_ref"], "local");
    reply(
        &client,
        &req["id"],
        json!({"thread_id":req["params"]["session_id"],"created_at":"now"}),
    );
    assert!(creating.await.unwrap().is_ok());
    client.close().await;
}

#[tokio::test]
async fn preflight_uses_the_effective_default_model() {
    let (client, mut rx) = fixture().await;
    let mut d = definition();
    d.default_options.model = Some("effective".into());
    let agent = client.agent(d, vec![]).unwrap();
    let creating = tokio::spawn(async move { agent.create_session().await });
    let request = next(&mut rx).await;
    assert_eq!(request["method"], "provider.inspect");
    assert_eq!(request["params"]["model"], "effective");
    let mut value = descriptor();
    value["model"] = json!("effective");
    reply(&client, &request["id"], value);
    let request = next(&mut rx).await;
    assert_eq!(request["params"]["options"]["model"], "effective");
    reply(
        &client,
        &request["id"],
        json!({"thread_id":request["params"]["session_id"],"created_at":"now"}),
    );
    assert!(creating.await.unwrap().is_ok());
    client.close().await;
}
