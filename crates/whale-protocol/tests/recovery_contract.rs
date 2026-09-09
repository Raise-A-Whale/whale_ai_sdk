use serde_json::json;
use whale_protocol::recovery::{AcknowledgeUnknownParams, RecoveryKey, SessionRunDefaults};

#[test]
fn keys_are_explicit_portable_and_redacted_in_diagnostics() {
    let key = RecoveryKey::new();
    key.validate().unwrap();
    let wire = serde_json::to_value(&key).unwrap();
    assert_eq!(wire["secret"], key.secret);
    assert!(!format!("{key:?}").contains(&key.secret));
    assert_ne!(key, RecoveryKey::new());
    assert_eq!(serde_json::from_value::<RecoveryKey>(wire).unwrap(), key);
}

#[test]
fn malformed_keys_and_defaults_are_rejected() {
    let mut key = RecoveryKey::new();
    key.secret = "short".into();
    assert!(key.validate().is_err());
    key.secret = "x".repeat(64);
    assert!(key.validate().is_err());
    assert!(SessionRunDefaults {
        max_steps: 0,
        timeout_ms: None
    }
    .validate()
    .is_err());
    assert!(SessionRunDefaults {
        max_steps: 1,
        timeout_ms: Some(0)
    }
    .validate()
    .is_err());
    assert!(serde_json::from_value::<SessionRunDefaults>(json!({"max_steps":true})).is_err());
}

#[test]
fn acknowledgements_are_revision_scoped_and_reject_duplicate_execution_ids() {
    let mut params = AcknowledgeUnknownParams {
        key: RecoveryKey::new(),
        expected_revision: 1,
        execution_ids: vec!["call-a".into()],
    };
    params.validate().unwrap();
    params.execution_ids.push("call-a".into());
    assert!(params.validate().is_err());
    params.execution_ids = vec![];
    assert!(params.validate().is_err());
    params.execution_ids = vec!["call-a".into()];
    params.expected_revision = 0;
    assert!(params.validate().is_err());
}
