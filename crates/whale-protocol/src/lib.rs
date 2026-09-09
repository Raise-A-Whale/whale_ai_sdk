//! whale-protocol: Canonical protocol definitions, stream events, and JSON-RPC schemas
//! for the Whale AI SDK.

pub mod agents;
pub mod canonical;
pub mod contexts;
pub mod events;
pub mod initialization;
pub mod interactions;
pub mod models;
pub mod recovery;
pub mod retention;
pub mod rpc;
pub mod runs;
pub mod session_management;
pub mod session_views;
pub mod sessions;

pub use canonical::{
    new_item_id, CanonicalContent, CanonicalItem, CanonicalToolOutput, ItemId, MessagePhase,
};
pub use events::{AgentStreamEvent, UsageMetrics};
pub use rpc::*;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_canonical_user_message_serde() {
        let msg = CanonicalItem::user_text("Hello Whale!");
        let json_str = serde_json::to_string(&msg).expect("serialize");
        assert!(json_str.contains("\"type\":\"user_message\""));
        assert!(json_str.contains("\"text\":\"Hello Whale!\""));

        let deserialized: CanonicalItem = serde_json::from_str(&json_str).expect("deserialize");
        assert_eq!(msg, deserialized);
    }

    #[test]
    fn test_canonical_assistant_message_with_phases() {
        let commentary =
            CanonicalItem::assistant_text("Analyzing code structure...", MessagePhase::Commentary);
        let final_ans =
            CanonicalItem::assistant_text("Here is the answer.", MessagePhase::FinalAnswer);

        let c_json = serde_json::to_string(&commentary).expect("serialize commentary");
        assert!(c_json.contains("\"phase\":\"commentary\""));
        let de_c: CanonicalItem = serde_json::from_str(&c_json).expect("deserialize");
        assert_eq!(commentary, de_c);

        let f_json = serde_json::to_string(&final_ans).expect("serialize final");
        assert!(f_json.contains("\"phase\":\"final_answer\""));
        let de_f: CanonicalItem = serde_json::from_str(&f_json).expect("deserialize");
        assert_eq!(final_ans, de_f);
    }

    #[test]
    fn test_canonical_reasoning_serde() {
        let reasoning = CanonicalItem::reasoning(
            "Step 1: Check inputs. Step 2: Formulate solution.",
            Some("sig_12345abc".to_string()),
            Some("encrypted_blob".to_string()),
        );

        let json_str = serde_json::to_string(&reasoning).expect("serialize");
        assert!(json_str.contains("\"type\":\"reasoning\""));
        assert!(
            json_str.contains("\"thinking\":\"Step 1: Check inputs. Step 2: Formulate solution.\"")
        );
        assert!(json_str.contains("\"signature\":\"sig_12345abc\""));
        assert!(json_str.contains("\"encrypted_content\":\"encrypted_blob\""));

        let de: CanonicalItem = serde_json::from_str(&json_str).expect("deserialize");
        assert_eq!(reasoning, de);
    }

    #[test]
    fn test_canonical_tool_call_and_result_serde() {
        let args = json!({
            "command": "cargo test",
            "timeout": 30
        });
        let tool_call = CanonicalItem::tool_call(
            "call_abc123",
            Some("bash".to_string()),
            "execute_command",
            Some(args),
            "{\"command\":\"cargo test\",\"timeout\":30}",
        );

        let call_json = serde_json::to_string(&tool_call).expect("serialize tool call");
        assert!(call_json.contains("\"type\":\"tool_call\""));
        assert!(call_json.contains("\"namespace\":\"bash\""));
        assert!(call_json.contains("\"name\":\"execute_command\""));

        let de_call: CanonicalItem = serde_json::from_str(&call_json).expect("deserialize");
        assert_eq!(tool_call, de_call);

        // Tool result with structured output
        let tool_res = CanonicalItem::tool_result(
            "call_abc123",
            CanonicalToolOutput::structured(json!({"exit_code": 0, "stdout": "all tests passed"})),
            false,
        );
        let res_json = serde_json::to_string(&tool_res).expect("serialize tool result");
        assert!(res_json.contains("\"type\":\"tool_result\""));
        assert!(res_json.contains("\"is_error\":false"));
        assert!(res_json.contains("\"type\":\"structured\""));

        let de_res: CanonicalItem = serde_json::from_str(&res_json).expect("deserialize");
        assert_eq!(tool_res, de_res);

        // Tool result with multi-modal blocks
        let block_res = CanonicalItem::tool_result(
            "call_chart123",
            CanonicalToolOutput::blocks(vec![
                CanonicalContent::text("Chart generated successfully"),
                CanonicalContent::image_uri("image/png", "file:///tmp/chart.png"),
            ]),
            false,
        );
        let block_json = serde_json::to_string(&block_res).expect("serialize blocks");
        assert!(block_json.contains("\"type\":\"blocks\""));
        let de_block: CanonicalItem = serde_json::from_str(&block_json).expect("deserialize");
        assert_eq!(block_res, de_block);
    }

    #[test]
    fn test_agent_stream_events_serde() {
        let turn_id = "turn_001";
        let thread_id = "thread_001";

        let events = vec![
            AgentStreamEvent::TurnStarted {
                turn_id: turn_id.to_string(),
                thread_id: thread_id.to_string(),
            },
            AgentStreamEvent::ItemStarted {
                turn_id: turn_id.to_string(),
                item_id: "item_01".to_string(),
                item_type: "assistant_message".to_string(),
                phase: Some(MessagePhase::Commentary),
            },
            AgentStreamEvent::TextDelta {
                turn_id: turn_id.to_string(),
                item_id: "item_01".to_string(),
                delta: "Hello".to_string(),
            },
            AgentStreamEvent::ReasoningDelta {
                turn_id: turn_id.to_string(),
                item_id: "item_02".to_string(),
                delta: "Let me think".to_string(),
            },
            AgentStreamEvent::ReasoningSignature {
                turn_id: turn_id.to_string(),
                item_id: "item_02".to_string(),
                signature: "sig_abc".to_string(),
            },
            AgentStreamEvent::ToolCallDelta {
                turn_id: turn_id.to_string(),
                item_id: "item_03".to_string(),
                call_id: "call_01".to_string(),
                delta: "{\"query\":".to_string(),
            },
            AgentStreamEvent::ItemCompleted {
                turn_id: turn_id.to_string(),
                item: CanonicalItem::user_text("test input"),
            },
            AgentStreamEvent::ApprovalRequested {
                turn_id: turn_id.to_string(),
                request_id: "req_appr_1".to_string(),
                tool_call: CanonicalItem::tool_call(
                    "call_del",
                    None,
                    "delete_database",
                    None,
                    "{}",
                ),
                reason: Some("Destructive operation".to_string()),
            },
            AgentStreamEvent::TurnCompleted {
                turn_id: turn_id.to_string(),
                thread_id: thread_id.to_string(),
                usage: UsageMetrics {
                    input_tokens: 1500,
                    output_tokens: 300,
                    reasoning_tokens: 100,
                    cache_creation_input_tokens: 500,
                    cache_read_input_tokens: 1000,
                },
            },
            AgentStreamEvent::TurnFailed {
                turn_id: turn_id.to_string(),
                thread_id: thread_id.to_string(),
                error_code: "RATE_LIMIT".to_string(),
                error_message: "Rate limit exceeded".to_string(),
            },
        ];

        for ev in &events {
            let s = serde_json::to_string(ev).expect("serialize event");
            let de: AgentStreamEvent = serde_json::from_str(&s).expect("deserialize event");
            assert_eq!(ev, &de);
        }
    }

    #[test]
    fn test_jsonrpc_request_response_serde() {
        let req = JSONRPCRequest::new(
            1,
            METHOD_SESSION_START_THREAD,
            Some(StartThreadParams {
                limits: None,
                provider_ref: None,
                agent_name: None,
                context_policy: None,
                provider_config: None,
                options: None,
                session_id: Some("sess_123".to_string()),
                provider: None,
                model: "claude-3-7-sonnet".to_string(),
                system_prompt: Some("You are an expert engineer".to_string()),
                tools: vec![],
                metadata: serde_json::Map::new(),
            }),
        )
        .unwrap();

        let req_json = serde_json::to_string(&req).expect("serialize req");
        assert!(req_json.contains("\"jsonrpc\":\"2.0\""));
        assert!(req_json.contains("\"id\":1"));
        assert!(req_json.contains("\"method\":\"session.start_thread\""));

        let de_req: JSONRPCRequest = serde_json::from_str(&req_json).expect("deserialize req");
        assert_eq!(req, de_req);

        let res = JSONRPCResponse::success(
            1,
            StartThreadResult {
                thread_id: "th_abc".to_string(),
                created_at: "2026-09-07T12:00:00Z".to_string(),
            },
        )
        .unwrap();

        let res_json = serde_json::to_string(&res).expect("serialize res");
        assert!(res_json.contains("\"jsonrpc\":\"2.0\""));
        assert!(res_json.contains("\"thread_id\":\"th_abc\""));

        let de_res: JSONRPCResponse = serde_json::from_str(&res_json).expect("deserialize res");
        assert_eq!(res, de_res);

        // Error response
        let err_res = JSONRPCResponse::error(
            "req_2",
            JSONRPCError::new(
                JSONRPCError::INVALID_PARAMS,
                "Missing required field",
                Some(json!({"field": "model"})),
            ),
        );
        let err_json = serde_json::to_string(&err_res).expect("serialize err res");
        assert!(err_json.contains("\"code\":-32602"));
        let de_err: JSONRPCResponse = serde_json::from_str(&err_json).expect("deserialize err res");
        assert_eq!(err_res, de_err);
    }

    #[test]
    fn test_jsonrpc_notification_serde() {
        let notif = JSONRPCNotification::new(
            METHOD_TURN_STREAM_EVENTS,
            Some(StreamEventsParams {
                turn_id: "t1".to_string(),
                thread_id: "th1".to_string(),
                event: AgentStreamEvent::TextDelta {
                    turn_id: "t1".to_string(),
                    item_id: "i1".to_string(),
                    delta: "world".to_string(),
                },
            }),
        )
        .unwrap();

        let notif_json = serde_json::to_string(&notif).expect("serialize notif");
        assert!(notif_json.contains("\"jsonrpc\":\"2.0\""));
        assert!(notif_json.contains("\"method\":\"turn.stream_events\""));
        assert!(!notif_json.contains("\"id\"")); // Notifications must not have id

        let de_notif: JSONRPCNotification =
            serde_json::from_str(&notif_json).expect("deserialize notif");
        assert_eq!(notif, de_notif);
    }

    #[test]
    fn test_reverse_rpc_and_approval_serde() {
        // Reverse RPC tool execution
        let tool_exec_params = ToolExecuteHostParams {
            binding_id: None,
            context: None,
            thread_id: None,
            call_id: "call_host_1".to_string(),
            namespace: Some("python_host".to_string()),
            name: "fetch_local_db".to_string(),
            arguments: json!({"query": "SELECT * FROM users"}),
        };
        let s = serde_json::to_string(&tool_exec_params).unwrap();
        let de: ToolExecuteHostParams = serde_json::from_str(&s).unwrap();
        assert_eq!(tool_exec_params, de);

        let tool_exec_res = ToolExecuteHostResult {
            call_id: "call_host_1".to_string(),
            output: CanonicalToolOutput::structured(json!([{"id": 1, "name": "Alice"}])),
            is_error: false,
        };
        let s2 = serde_json::to_string(&tool_exec_res).unwrap();
        let de2: ToolExecuteHostResult = serde_json::from_str(&s2).unwrap();
        assert_eq!(tool_exec_res, de2);

        // Approval resolve
        let appr = ApprovalResolveParams {
            request_id: "req_approve_001".to_string(),
            decision: ApprovalDecision::Approve,
            feedback: Some("Approved by Admin".to_string()),
        };
        let s3 = serde_json::to_string(&appr).unwrap();
        assert!(s3.contains("\"decision\":\"approve\""));
        let de3: ApprovalResolveParams = serde_json::from_str(&s3).unwrap();
        assert_eq!(appr, de3);
    }
}
