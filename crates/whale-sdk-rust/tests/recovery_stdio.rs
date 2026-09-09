//! Gated public-consumer recovery tests. Ignored by default; requires daemon + SQLite fixture.
use async_trait::async_trait;
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use whale_protocol::{CanonicalToolOutput, TurnStatus};
use whale_sdk_rust::{
    Agent, AgentDefinition, HostTool, ProviderApi, ProviderAuth, ProviderConfig, RecoveryKey,
    SdkError, WhaleClient,
};

struct Lookup(Arc<Mutex<Vec<String>>>);
#[async_trait]
impl HostTool for Lookup {
    fn name(&self) -> &str {
        "lookup"
    }
    fn description(&self) -> &str {
        "Durable lookup"
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
async fn client(db: &std::path::Path) -> WhaleClient {
    WhaleClient::spawn_daemon_with_args(
        std::env::var("WHALE_RECOVERY_DAEMON")
            .expect("set WHALE_RECOVERY_DAEMON to built whale-daemon"),
        &[
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
    let mut def = AgentDefinition::new("rust-durable-agent", model);
    def.provider_config = Some(ProviderConfig {
        api: ProviderApi::OpenaiResponses,
        base_url: Some(std::env::var("WHALE_RECOVERY_BASE_URL").unwrap()),
        auth: Some(ProviderAuth::None),
    });
    def.tool_names = vec!["lookup".into()];
    def.max_steps = 7;
    def.timeout_ms = Some(10000);
    client
        .agent(def, vec![Arc::new(Lookup(effects.clone()))])
        .unwrap()
}
#[tokio::test]
#[ignore = "requires production recovery daemon and HTTP fixture"]
async fn completed_history_and_actual_model_inputs_survive_owned_process_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("sessions.sqlite");
    let key = RecoveryKey::new();
    let effects = Arc::new(Mutex::new(Vec::new()));
    let first = client(&db).await;
    let session = agent(&first, &effects, "rust-recovery-restart")
        .create_persistent_session(&key)
        .await
        .unwrap();
    let old_sid = session.id().to_owned();
    let run = session.start_turn("first-user").await.unwrap();
    let old_turn = run.id().to_owned();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), run.result())
            .await
            .unwrap()
            .unwrap()
            .status,
        TurnStatus::Completed
    );
    assert_eq!(*effects.lock().unwrap(), ["original"]);
    first.close().await;
    let second = client(&db).await;
    let saved = second.inspect_recovery(&key).await.unwrap();
    assert!(!saved.attached);
    assert!(saved.unknown_executions.is_empty());
    let archive = saved
        .runs
        .iter()
        .find(|r| r.snapshot.turn_id == old_turn)
        .unwrap();
    assert_eq!(archive.snapshot.thread_id, old_sid);
    assert_eq!(archive.model_inputs.len(), 2);
    assert!(archive.model_inputs.iter().all(|i| i.completed));
    assert!(archive.model_inputs[0]
        .request
        .to_string()
        .contains("first-user"));
    assert!(!saved.history.is_empty());
    let session = agent(&second, &effects, "rust-recovery-restart")
        .recover_session(&key)
        .await
        .unwrap();
    assert_ne!(old_sid, session.id());
    assert_eq!(session.recovery_key(), Some(&key));
    assert_eq!(*effects.lock().unwrap(), ["original"]);
    assert!(second.get_run(&old_sid, &old_turn).await.is_err());
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(10),
            session.start_turn("second-user").await.unwrap().result()
        )
        .await
        .unwrap()
        .unwrap()
        .status,
        TurnStatus::Completed
    );
    assert_eq!(*effects.lock().unwrap(), ["original", "original"]);
    session.close().await.unwrap();
    let saved = second.inspect_recovery(&key).await.unwrap();
    assert!(!saved.attached);
    assert!(second.forget_session(&key, saved.revision).await.unwrap());
    second.close().await;
}
#[tokio::test]
#[ignore = "requires production recovery daemon and HTTP fixture"]
async fn wrong_key_and_changed_configuration_do_not_execute_tools() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("sessions.sqlite");
    let key = RecoveryKey::new();
    let effects = Arc::new(Mutex::new(Vec::new()));
    let first = client(&db).await;
    agent(&first, &effects, "rust-recovery-rejected")
        .create_persistent_session(&key)
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    first.close().await;
    let second = client(&db).await;
    let wrong = RecoveryKey {
        recovery_id: key.recovery_id.clone(),
        secret: "0".repeat(64),
    };
    assert!(matches!(
        second.inspect_recovery(&wrong).await,
        Err(SdkError::Rpc { code: -32021, .. })
    ));
    assert!(matches!(
        agent(&second, &effects, "rust-recovery-changed")
            .recover_session(&key)
            .await,
        Err(SdkError::Rpc { code: -32021, .. })
    ));
    assert!(effects.lock().unwrap().is_empty());
    assert!(!second.inspect_recovery(&key).await.unwrap().attached);
    agent(&second, &effects, "rust-recovery-rejected")
        .recover_session(&key)
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    assert!(second
        .forget_session(&key, second.inspect_recovery(&key).await.unwrap().revision)
        .await
        .unwrap());
    second.close().await;
}
