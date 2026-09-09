use serde_json::json;
use whale_protocol::runs::{
    RunApprovalParams, RunEvent, RunEventPayload, RunStatus, StartTurnParams,
};

#[test]
fn stream_envelope_preserves_run_identity_and_sequence() {
    let event: RunEvent = serde_json::from_value(json!({
        "thread_id":"session", "turn_id":"run", "seq":1, "type":"stream",
        "event":{"type":"text_delta","turn_id":"run","item_id":"item","delta":"hello"}
    }))
    .unwrap();
    assert_eq!(event.turn_id, "run");
    assert_eq!(event.seq, 1);
    assert!(matches!(event.payload, RunEventPayload::Stream { .. }));
}

#[test]
fn finished_envelope_retains_failure_and_final_sequence() {
    let value = json!({
        "thread_id":"s", "turn_id":"r", "seq":4, "type":"finished",
        "snapshot":{
            "thread_id":"s","turn_id":"r","status":"failed","items":[],
            "usage":{"input_tokens":2,"output_tokens":0},"pending_approvals":[],
            "last_seq":4,"error":{"code":"provider_error","message":"connection closed"}
        }
    });
    let event: RunEvent = serde_json::from_value(value).unwrap();
    match event.payload {
        RunEventPayload::Finished { snapshot } => {
            assert!(snapshot.status.is_terminal());
            assert_eq!(snapshot.last_seq, event.seq);
            assert_eq!(snapshot.error.unwrap().code, "provider_error");
        }
        _ => panic!("expected a terminal snapshot"),
    }
    assert!(!RunStatus::WaitingApproval.is_terminal());
    assert!(!RunStatus::Cancelling.is_terminal());
}

#[test]
fn start_defaults_and_modified_arguments_match_language_clients() {
    let params: StartTurnParams = serde_json::from_value(json!({
        "thread_id":"s","turn_id":"r","input_items":[]
    }))
    .unwrap();
    assert_eq!(params.max_steps, 10);
    assert_eq!(params.timeout_ms, None);
    let approval: RunApprovalParams = serde_json::from_value(json!({
        "thread_id":"s","turn_id":"r","request_id":"a",
        "decision":"modify_arguments","arguments":{"path":"safe"}
    }))
    .unwrap();
    let value = serde_json::to_value(approval).unwrap();
    assert_eq!(value["decision"], "modify_arguments");
    assert_eq!(value["arguments"]["path"], "safe");
}
