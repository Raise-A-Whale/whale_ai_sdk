use serde_json::json;
use whale_protocol::{agents::AgentDefinition, retention::*, rpc::StartThreadParams};

#[test]
fn absent_policies_preserve_default_wire_and_disable_retirement() {
    let policy: RetentionPolicy = serde_json::from_value(json!({})).unwrap();
    assert!(!policy.is_enabled());
    assert_eq!(policy.sweep_interval_ms, 1000);
    assert!(policy.validate().is_ok());
    let agent = AgentDefinition::new("business", "model");
    assert!(agent.limits.is_none());
    assert!(serde_json::to_value(agent).unwrap().get("limits").is_none());
    let params: StartThreadParams = serde_json::from_value(json!({"model":"model"})).unwrap();
    assert!(params.limits.is_none());
    assert!(serde_json::to_value(params)
        .unwrap()
        .get("limits")
        .is_none());
}

#[test]
fn config_rejects_typos_invalid_numbers_and_empty_intervals() {
    for value in [
        json!({"sweep_interval_ms":0}),
        json!({"runs":{"terminal_ttl_ms":0}}),
        json!({"store":{"max_retained_sessions":0}}),
    ] {
        assert!(serde_json::from_value::<RetentionPolicy>(value)
            .unwrap()
            .validate()
            .is_err());
    }
    for value in [
        json!({"runs":{"terminal_ttl_ms":true}}),
        json!({"store":{"max_retained_payload_bytes":1.5}}),
        json!({"store":{"max_retained_sessions":-1}}),
        json!({"typo":1}),
    ] {
        assert!(serde_json::from_value::<RetentionPolicy>(value).is_err());
    }
    for value in [
        json!({"max_history_bytes":0}),
        json!({"max_model_request_bytes":0}),
        json!({"max_accepted_turns":0}),
    ] {
        assert!(serde_json::from_value::<SessionLimits>(value)
            .unwrap()
            .validate()
            .is_err());
    }
    for value in [
        json!({"max_accepted_turns":true}),
        json!({"max_history_bytes":1.0}),
        json!({"unknown":5}),
    ] {
        assert!(serde_json::from_value::<SessionLimits>(value).is_err());
    }
}

#[test]
fn policy_and_agent_limits_preserve_unsigned_max_and_validate_nested_values() {
    let policy:RetentionPolicy=serde_json::from_value(json!({"runs":{"max_terminal_runs_per_session":u64::MAX},"store":{"detached_ttl_ms":1,"max_retained_sessions":4,"max_retained_payload_bytes":1024}})).unwrap();
    assert!(policy.validate().is_ok());
    assert!(policy.is_enabled());
    assert_eq!(
        serde_json::to_value(policy).unwrap()["runs"]["max_terminal_runs_per_session"],
        json!(u64::MAX)
    );
    let mut agent = AgentDefinition::new("business", "model");
    agent.limits = Some(SessionLimits {
        max_accepted_turns: Some(2),
        max_history_bytes: Some(4096),
        max_model_request_bytes: Some(u64::MAX),
    });
    assert!(agent.validate().is_ok());
    let decoded: AgentDefinition =
        serde_json::from_value(serde_json::to_value(&agent).unwrap()).unwrap();
    assert_eq!(decoded, agent);
    agent.limits.as_mut().unwrap().max_accepted_turns = Some(0);
    assert!(agent.validate().is_err());
}
