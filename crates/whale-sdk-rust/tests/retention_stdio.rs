//! Gated production-process consumers. Ignored by default; requires built daemon + HTTP fixture.
use async_trait::async_trait;
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use whale_protocol::{runs::RunEventPayload, CanonicalToolOutput, TurnStatus};
use whale_sdk_rust::{
    Agent, AgentDefinition, HostTool, ProviderApi, ProviderAuth, ProviderConfig, RecoveryKey,
    RunHandle, SdkError, SessionLimits, WhaleClient,
};

struct Lookup(Arc<Mutex<Vec<String>>>);
#[async_trait]
impl HostTool for Lookup {
    fn name(&self) -> &str {
        "lookup"
    }
    fn description(&self) -> &str {
        "Business lookup"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"]})
    }
    async fn execute(&self, args: Value) -> Result<CanonicalToolOutput, String> {
        self.0
            .lock()
            .unwrap()
            .push(args["query"].as_str().unwrap().into());
        Ok(CanonicalToolOutput::structured(args))
    }
}
async fn client(dir: &std::path::Path) -> WhaleClient {
    let config = dir.join("retention.json");
    let db = dir.join("sessions.sqlite");
    std::fs::write(
        &config,
        json!({"sweep_interval_ms":20,"runs":{"terminal_ttl_ms":100}}).to_string(),
    )
    .unwrap();
    WhaleClient::spawn_daemon_with_args(
        std::env::var("WHALE_RETENTION_DAEMON")
            .expect("set WHALE_RETENTION_DAEMON to built whale-daemon"),
        &[
            "--retention-config",
            config.to_str().unwrap(),
            "--session-store",
            db.to_str().unwrap(),
            "--log-level",
            "warn",
        ],
    )
    .await
    .unwrap()
}
fn agent(client: &WhaleClient, effects: &Arc<Mutex<Vec<String>>>, model: &str) -> Agent {
    let mut def = AgentDefinition::new("rust-retention-agent", model);
    def.provider_config = Some(ProviderConfig {
        api: ProviderApi::OpenaiResponses,
        base_url: Some(std::env::var("WHALE_RETENTION_BASE_URL").unwrap()),
        auth: Some(ProviderAuth::None),
    });
    def.tool_names = vec!["lookup".into()];
    def.limits = Some(SessionLimits {
        max_accepted_turns: Some(2),
        ..Default::default()
    });
    client
        .agent(def, vec![Arc::new(Lookup(effects.clone()))])
        .unwrap()
}
async fn expire(client: &WhaleClient, run: &RunHandle) {
    let saved = tokio::time::timeout(Duration::from_secs(10), run.result())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.status, TurnStatus::Completed);
    // No requests while idle: production maintenance must run independently.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(matches!(run.snapshot().await, Err(SdkError::RunExpired(_))));
    assert!(matches!(
        client.get_run(run.thread_id(), run.id()).await,
        Err(SdkError::RunExpired(_))
    ));
    assert!(matches!(run.cancel().await, Err(SdkError::RunExpired(_))));
    assert_eq!(saved, run.result().await.unwrap());
    let mut events = run.events().unwrap();
    let mut final_seen = false;
    while let Some(event) = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .unwrap()
        .unwrap()
    {
        final_seen |= matches!(event.payload, RunEventPayload::Finished { .. });
    }
    assert!(final_seen);
}

#[tokio::test]
#[ignore = "requires production daemon and controlled HTTP fixture"]
async fn idle_expiry_preserves_result_and_counts_expired_turns() {
    let dir = tempfile::tempdir().unwrap();
    let client = client(dir.path()).await;
    let effects = Arc::new(Mutex::new(Vec::new()));
    let session = agent(&client, &effects, "rust-retention-live")
        .create_session()
        .await
        .unwrap();
    let first = session.start_turn("first").await.unwrap();
    expire(&client, &first).await;
    let second = session.start_turn("second").await.unwrap();
    assert_ne!(first.id(), second.id());
    assert_eq!(second.result().await.unwrap().status, TurnStatus::Completed);
    assert!(matches!(
        session.start_turn("third").await,
        Err(SdkError::LimitExceeded(_))
    ));
    assert_eq!(*effects.lock().unwrap(), ["original", "original"]);
    client.close().await;
}

#[tokio::test]
#[ignore = "requires production daemon and controlled HTTP fixture"]
async fn durable_archive_and_admission_count_outlive_live_expiration_and_attach() {
    let dir = tempfile::tempdir().unwrap();
    let client = client(dir.path()).await;
    let effects = Arc::new(Mutex::new(Vec::new()));
    let key = RecoveryKey::new();
    let agent = agent(&client, &effects, "rust-retention-durable");
    let session = agent.create_persistent_session(&key).await.unwrap();
    let first = session.start_turn("first").await.unwrap();
    expire(&client, &first).await;
    let archive = client.inspect_recovery(&key).await.unwrap();
    assert_eq!(archive.runs.len(), 1);
    assert_eq!(archive.runs[0].snapshot.turn_id, first.id());
    assert_eq!(archive.runs[0].model_inputs.len(), 2);
    assert!(archive.runs[0]
        .model_inputs
        .iter()
        .all(|input| input.completed));
    session.close().await.unwrap();
    let attached = agent.recover_session(&key).await.unwrap();
    assert_ne!(session.id(), attached.id());
    assert_eq!(*effects.lock().unwrap(), ["original"]);
    assert_eq!(
        attached
            .start_turn("second")
            .await
            .unwrap()
            .result()
            .await
            .unwrap()
            .status,
        TurnStatus::Completed
    );
    assert!(matches!(
        attached.start_turn("third").await,
        Err(SdkError::LimitExceeded(_))
    ));
    assert_eq!(*effects.lock().unwrap(), ["original", "original"]);
    assert_eq!(client.inspect_recovery(&key).await.unwrap().runs.len(), 2);
    client.close().await;
}
