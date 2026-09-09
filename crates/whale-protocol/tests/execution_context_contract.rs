use serde_json::json;
use whale_protocol::contexts::*;
use whale_protocol::rpc::{StartThreadParams, ToolExecuteHostParams};
use whale_protocol::runs::RunSnapshot;

#[test]
fn legacy_requests_and_snapshots_default_new_context_fields() {
    let thread: StartThreadParams = serde_json::from_value(json!({"model":"fixture"})).unwrap();
    assert!(thread.context_policy.is_none());
    assert!(thread.agent_name.is_none());
    let tool: ToolExecuteHostParams = serde_json::from_value(json!({
        "call_id":"reverse-1","name":"lookup","arguments":{}
    }))
    .unwrap();
    assert!(tool.context.is_none());
    assert!(tool.binding_id.is_none());
    let snapshot: RunSnapshot = serde_json::from_value(json!({
        "thread_id":"session","turn_id":"run","status":"running","items":[],
        "usage":{"input_tokens":0,"output_tokens":0},"pending_approvals":[],"last_seq":0
    }))
    .unwrap();
    assert!(snapshot.tool_executions.is_empty());
}

#[test]
fn host_correlation_and_canonical_identity_remain_distinct() {
    let request: ToolExecuteHostParams = serde_json::from_value(json!({
        "binding_id":"callback-version-1","call_id":"connection:reverse-id","name":"lookup","arguments":{"key":"x"},
        "context":{"agent_name":"analyst","thread_id":"s","turn_id":"r",
                   "call_id":"model-call-2","deadline_unix_ms":1234}
    })).unwrap();
    let context = request.context.as_ref().unwrap();
    assert_eq!(context.call_id, "model-call-2");
    assert_eq!(context.run.thread_id, "s");
    assert_ne!(request.call_id, context.call_id);
    assert_eq!(request.binding_id.as_deref(), Some("callback-version-1"));
    assert_eq!(serde_json::to_value(context).unwrap()["turn_id"], "r");
}

#[test]
fn policy_and_progress_are_validated_at_the_boundary() {
    assert!(ContextPolicyConfig::RecentTurns { max_turns: 0 }
        .validate()
        .is_err());
    assert!(ContextPolicyConfig::RecentTurns { max_turns: 2 }
        .validate()
        .is_ok());
    assert_eq!(
        serde_json::to_value(ContextPolicyConfig::Host).unwrap(),
        json!({"type":"host"})
    );
    for progress in [f64::NAN, f64::INFINITY, -0.1, 1.01] {
        assert!(ToolReportProgressParams {
            call_id: "rpc".into(),
            message: "working".into(),
            progress: Some(progress)
        }
        .validate()
        .is_err());
    }
    assert!(ToolReportProgressParams {
        call_id: "rpc".into(),
        message: "working".into(),
        progress: Some(0.5)
    }
    .validate()
    .is_ok());
}
