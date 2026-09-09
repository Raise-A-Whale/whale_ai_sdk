use serde_json::json;
use whale_protocol::sessions::{CloseSessionParams, CloseSessionResult, METHOD_SESSION_CLOSE};

#[test]
fn close_session_contract_distinguishes_first_and_repeated_close() {
    assert_eq!(METHOD_SESSION_CLOSE, "session.close");
    let request = CloseSessionParams {
        thread_id: "session-a".into(),
    };
    assert_eq!(
        serde_json::to_value(&request).unwrap(),
        json!({"thread_id":"session-a"})
    );
    assert!(request.validate().is_ok());
    for closed in [true, false] {
        let result = CloseSessionResult {
            thread_id: "session-a".into(),
            closed,
        };
        let wire = json!({"thread_id":"session-a","closed":closed});
        assert_eq!(serde_json::to_value(&result).unwrap(), wire);
        assert_eq!(
            serde_json::from_value::<CloseSessionResult>(wire).unwrap(),
            result
        );
    }
}

#[test]
fn close_session_requires_a_nonempty_string_identity() {
    for thread_id in ["", " ", "\n\t"] {
        assert!(CloseSessionParams {
            thread_id: thread_id.into()
        }
        .validate()
        .is_err());
    }
    assert!(serde_json::from_value::<CloseSessionParams>(json!({})).is_err());
    assert!(serde_json::from_value::<CloseSessionParams>(json!({"thread_id":42})).is_err());
}
