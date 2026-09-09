mod common;

use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::mpsc;
use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
use whale_daemon::{AnyTransportWriter, DaemonServer, OutgoingTransport};
use whale_protocol::rpc::JSONRPCRequest;
use whale_store::{MemoryStore, StoreRuntime};

struct Capture(mpsc::UnboundedSender<Value>);

#[async_trait]
impl OutgoingTransport for Capture {
    async fn send_line(&self, line: &str) -> std::io::Result<()> {
        self.0
            .send(serde_json::from_str(line).expect("daemon emits JSON"))
            .map_err(|_| std::io::Error::other("capture closed"))
    }
}

fn fixture() -> (
    DaemonServer,
    AnyTransportWriter,
    mpsc::UnboundedReceiver<Value>,
    AnyTransportWriter,
    mpsc::UnboundedReceiver<Value>,
) {
    let gate = Arc::new(ApprovalGate::new());
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    )))
    .with_stream_provider(Arc::new(|_, _| Ok(Box::pin(futures::stream::pending()))));
    let server = DaemonServer::new(Arc::new(engine), gate);
    let (a_tx, a_rx) = mpsc::unbounded_channel();
    let (b_tx, b_rx) = mpsc::unbounded_channel();
    (
        server,
        AnyTransportWriter::new(Arc::new(Capture(a_tx))),
        a_rx,
        AnyTransportWriter::new(Arc::new(Capture(b_tx))),
        b_rx,
    )
}

async fn persistent_fixture() -> (
    DaemonServer,
    AnyTransportWriter,
    mpsc::UnboundedReceiver<Value>,
    AnyTransportWriter,
    mpsc::UnboundedReceiver<Value>,
) {
    let (server, owner_a, rx_a, owner_b, rx_b) = fixture();
    let store = Arc::new(
        StoreRuntime::open(Arc::new(MemoryStore::new()))
            .await
            .unwrap(),
    );
    (
        server.with_store_runtime(store),
        owner_a,
        rx_a,
        owner_b,
        rx_b,
    )
}

async fn send(
    server: &DaemonServer,
    writer: &AnyTransportWriter,
    id: u64,
    method: &str,
    params: Value,
) {
    server
        .handle_message(
            &json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}).to_string(),
            writer,
        )
        .await;
}

async fn response(rx: &mut mpsc::UnboundedReceiver<Value>, id: u64) -> Value {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let value = rx.recv().await.expect("response");
            if value["id"] == id {
                return value;
            }
        }
    })
    .await
    .expect("response deadline")
}

async fn call(
    server: &DaemonServer,
    writer: &AnyTransportWriter,
    rx: &mut mpsc::UnboundedReceiver<Value>,
    id: u64,
    method: &str,
    params: Value,
) -> Value {
    send(server, writer, id, method, params).await;
    response(rx, id).await
}

#[tokio::test]
async fn list_window_is_owner_scoped_and_membership_is_fixed() {
    let (server, owner_a, mut rx_a, owner_b, mut rx_b) = fixture();
    common::initialize(&server, &owner_a, Some(&mut rx_a)).await;
    common::initialize(&server, &owner_b, Some(&mut rx_b)).await;

    for (id, thread) in [(1, "a-one"), (2, "a-two")] {
        let response = call(
            &server,
            &owner_a,
            &mut rx_a,
            id,
            "session.start_thread",
            json!({"session_id":thread,"model":"test","metadata":{"version":0}}),
        )
        .await;
        assert!(response.get("error").is_none(), "{response}");
    }
    let response = call(
        &server,
        &owner_b,
        &mut rx_b,
        10,
        "session.start_thread",
        json!({"session_id":"b-one","model":"test"}),
    )
    .await;
    assert!(response.get("error").is_none(), "{response}");

    let first = call(
        &server,
        &owner_a,
        &mut rx_a,
        20,
        "session.list",
        json!({"limit":1}),
    )
    .await;
    assert_eq!(
        first["result"]["sessions"][0]["summary"]["thread_id"], "a-one",
        "{first}"
    );
    let cursor = first["result"]["next_cursor"].clone();
    assert!(cursor.is_string(), "{first}");

    let wrong_owner = call(
        &server,
        &owner_b,
        &mut rx_b,
        19,
        "session.list",
        json!({"cursor":cursor.clone(),"limit":8}),
    )
    .await;
    assert_eq!(wrong_owner["error"]["data"]["kind"], "list_cursor_invalid");

    let response = call(
        &server,
        &owner_a,
        &mut rx_a,
        21,
        "session.start_thread",
        json!({"session_id":"a-late","model":"test"}),
    )
    .await;
    assert!(response.get("error").is_none(), "{response}");
    let replaced = call(
        &server,
        &owner_a,
        &mut rx_a,
        22,
        "session.metadata.replace",
        json!({"thread_id":"a-two","expected_view_revision":0,"metadata":{"version":1}}),
    )
    .await;
    assert!(replaced.get("error").is_none(), "{replaced}");
    let closed = call(
        &server,
        &owner_a,
        &mut rx_a,
        23,
        "session.close",
        json!({"thread_id":"a-two"}),
    )
    .await;
    assert!(closed.get("error").is_none(), "{closed}");

    let second = call(
        &server,
        &owner_a,
        &mut rx_a,
        24,
        "session.list",
        json!({"cursor":cursor.clone(),"limit":8}),
    )
    .await;
    assert_eq!(
        second["result"]["sessions"].as_array().unwrap().len(),
        1,
        "{second}"
    );
    assert_eq!(
        second["result"]["sessions"][0]["summary"]["thread_id"],
        "a-two"
    );
    assert_eq!(
        second["result"]["sessions"][0]["summary"]["metadata"]["version"],
        1
    );
    assert_eq!(second["result"]["sessions"][0]["lifecycle"], "closed");
    assert!(second["result"].get("next_cursor").is_none());

    let b_page = call(
        &server,
        &owner_b,
        &mut rx_b,
        25,
        "session.list",
        json!({"limit":8}),
    )
    .await;
    let b_threads: Vec<_> = b_page["result"]["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["summary"]["thread_id"].as_str().unwrap())
        .collect();
    assert_eq!(b_threads, ["b-one"]);

    let leaked = serde_json::to_string(&second["result"]).unwrap();
    for forbidden in ["owner", "store_revision", "epoch", "recovery_secret"] {
        assert!(!leaked.contains(forbidden), "leaked {forbidden}: {leaked}");
    }

    let mut forged = cursor.as_str().unwrap().as_bytes().to_vec();
    forged[0] = if forged[0] == b'A' { b'B' } else { b'A' };
    let forged = String::from_utf8(forged).unwrap();
    let invalid = call(
        &server,
        &owner_a,
        &mut rx_a,
        26,
        "session.list",
        json!({"cursor":forged,"limit":8}),
    )
    .await;
    assert_eq!(invalid["error"]["data"]["kind"], "list_cursor_invalid");
}

async fn direct(
    server: &DaemonServer,
    writer: &AnyTransportWriter,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
    serde_json::to_value(
        server
            .dispatch_request(
                JSONRPCRequest::new(id, method, Some(params)).unwrap(),
                writer,
            )
            .await,
    )
    .unwrap()
}

#[tokio::test(start_paused = true)]
async fn list_cursor_ttl_and_closed_pressure_are_typed_and_never_evict_open() {
    let (server, owner, mut rx, _other, _other_rx) = fixture();
    common::initialize(&server, &owner, Some(&mut rx)).await;
    for thread in ["open", "ttl-a", "ttl-b"] {
        let result = direct(
            &server,
            &owner,
            1,
            "session.start_thread",
            json!({"session_id":thread,"model":"test"}),
        )
        .await;
        assert!(result.get("error").is_none(), "{result}");
    }
    let first = direct(&server, &owner, 2, "session.list", json!({"limit":1})).await;
    let expired_cursor = first["result"]["next_cursor"].clone();
    tokio::time::advance(std::time::Duration::from_millis(
        whale_protocol::session_management::SESSION_MANAGEMENT_CURSOR_TTL_MS,
    ))
    .await;
    let expired = direct(
        &server,
        &owner,
        3,
        "session.list",
        json!({"cursor":expired_cursor,"limit":8}),
    )
    .await;
    assert_eq!(expired["error"]["data"]["kind"], "list_cursor_expired");
    let fresh_window = direct(&server, &owner, 4, "session.list", json!({"limit":1})).await;
    let generation_cursor = fresh_window["result"]["next_cursor"].clone();

    for index in 0..=whale_protocol::session_management::MAX_CLOSED_SESSION_TOMBSTONES_PER_OWNER {
        let thread = format!("closed-{index:03}");
        let created = direct(
            &server,
            &owner,
            10,
            "session.start_thread",
            json!({"session_id":thread,"model":"test"}),
        )
        .await;
        assert!(created.get("error").is_none(), "{created}");
        let closed = direct(
            &server,
            &owner,
            11,
            "session.close",
            json!({"thread_id":thread}),
        )
        .await;
        assert!(closed.get("error").is_none(), "{closed}");
    }

    let page = direct(&server, &owner, 12, "session.list", json!({"limit":256})).await;
    let sessions = page["result"]["sessions"].as_array().unwrap();
    assert_eq!(sessions.len(), 131); // three Open records plus 128 Closed tombstones.
    assert!(sessions
        .iter()
        .any(|entry| entry["summary"]["thread_id"] == "open" && entry["lifecycle"] == "open"));
    assert!(!sessions
        .iter()
        .any(|entry| entry["summary"]["thread_id"] == "closed-000"));
    let invalidated = direct(
        &server,
        &owner,
        14,
        "session.list",
        json!({"cursor":generation_cursor,"limit":8}),
    )
    .await;
    assert_eq!(invalidated["error"]["data"]["kind"], "list_cursor_expired");
    let evicted = direct(
        &server,
        &owner,
        13,
        "session.get.v2",
        json!({"thread_id":"closed-000","history_limit":1}),
    )
    .await;
    assert_eq!(evicted["error"]["data"]["kind"], "tombstone_expired");
}

#[tokio::test(start_paused = true)]
async fn closed_tombstone_ttl_expires_without_touching_open_records() {
    let (server, owner, mut rx, _other, _other_rx) = fixture();
    common::initialize(&server, &owner, Some(&mut rx)).await;
    for thread in ["open", "closed"] {
        assert!(direct(
            &server,
            &owner,
            1,
            "session.start_thread",
            json!({"session_id":thread,"model":"test"}),
        )
        .await
        .get("error")
        .is_none());
    }
    assert!(direct(
        &server,
        &owner,
        2,
        "session.close",
        json!({"thread_id":"closed"}),
    )
    .await
    .get("error")
    .is_none());
    tokio::time::advance(std::time::Duration::from_millis(
        whale_protocol::session_management::CLOSED_SESSION_TOMBSTONE_TTL_MS,
    ))
    .await;
    tokio::task::yield_now().await;
    let expired = direct(
        &server,
        &owner,
        3,
        "session.get.v2",
        json!({"thread_id":"closed","history_limit":1}),
    )
    .await;
    assert_eq!(expired["error"]["data"]["kind"], "tombstone_expired");
    let open = direct(
        &server,
        &owner,
        4,
        "session.get.v2",
        json!({"thread_id":"open","history_limit":1}),
    )
    .await;
    assert_eq!(open["result"]["lifecycle"], "open", "{open}");
}

#[tokio::test]
async fn initialization_negotiates_catalog_and_history_without_protocol_baseline_changes() {
    let (server, owner, mut rx, _other, _other_rx) = fixture();
    server
        .handle_message(
            &json!({
                "jsonrpc":"2.0",
                "id":1,
                "method":"protocol.initialize",
                "params":{
                    "client":{"name":"catalog-test","version":"1"},
                    "protocol_versions":[1],
                    "required_capabilities":["session_catalog.v1","session_history.v1"]
                }
            })
            .to_string(),
            &owner,
        )
        .await;
    let initialized = response(&mut rx, 1).await;
    let capabilities = initialized["result"]["capabilities"].as_array().unwrap();
    assert!(capabilities
        .iter()
        .any(|value| value == "session_catalog.v1"));
    assert!(capabilities
        .iter()
        .any(|value| value == "session_history.v1"));
    assert!(!whale_protocol::initialization::PROTOCOL_CAPABILITIES.contains(&"session_catalog.v1"));
    assert!(!whale_protocol::initialization::PROTOCOL_CAPABILITIES.contains(&"session_history.v1"));
}

#[tokio::test]
async fn unattached_store_records_are_absent_until_published_to_the_current_owner() {
    let (server, owner_a, mut rx_a, owner_b, mut rx_b) = persistent_fixture().await;
    common::initialize(&server, &owner_a, Some(&mut rx_a)).await;
    common::initialize(&server, &owner_b, Some(&mut rx_b)).await;
    let key = json!({
        "recovery_id":uuid::Uuid::new_v4().to_string(),
        "secret":format!("{}{}",uuid::Uuid::new_v4().simple(),uuid::Uuid::new_v4().simple())
    });
    let config = |thread: &str| {
        json!({
            "session_id":thread,
            "model":"test",
            "provider_config":{"api":"openai_responses","auth":{"type":"none"}},
            "tools":[]
        })
    };
    let created = call(
        &server,
        &owner_a,
        &mut rx_a,
        1,
        "session.create_persistent",
        json!({
            "key":key,
            "session":config("11111111-1111-4111-8111-111111111111"),
            "run_defaults":{}
        }),
    )
    .await;
    assert!(created.get("error").is_none(), "{created}");
    assert!(call(
        &server,
        &owner_a,
        &mut rx_a,
        2,
        "session.close",
        json!({"thread_id":"11111111-1111-4111-8111-111111111111"}),
    )
    .await
    .get("error")
    .is_none());
    server.disconnect_connection(&owner_a).await;

    let detached = call(
        &server,
        &owner_b,
        &mut rx_b,
        3,
        "session.list",
        json!({"limit":8}),
    )
    .await;
    assert_eq!(detached["result"]["sessions"], json!([]), "{detached}");
    let inspected = call(
        &server,
        &owner_b,
        &mut rx_b,
        4,
        "session.recovery.inspect",
        json!({"key":key}),
    )
    .await;
    assert_eq!(inspected["result"]["attached"], false, "{inspected}");
    let attached = call(
        &server,
        &owner_b,
        &mut rx_b,
        5,
        "session.recovery.attach",
        json!({
            "key":key,
            "expected_revision":inspected["result"]["revision"],
            "session":config("22222222-2222-4222-8222-222222222222"),
            "run_defaults":{}
        }),
    )
    .await;
    assert!(attached.get("error").is_none(), "{attached}");
    let listed = call(
        &server,
        &owner_b,
        &mut rx_b,
        6,
        "session.list",
        json!({"limit":8}),
    )
    .await;
    assert_eq!(
        listed["result"]["sessions"][0]["summary"]["thread_id"],
        "22222222-2222-4222-8222-222222222222",
        "{listed}"
    );
    assert_eq!(
        listed["result"]["sessions"][0]["persistence"]["kind"],
        "persistent"
    );
    let encoded = serde_json::to_string(&listed["result"]).unwrap();
    assert!(!encoded.contains(key["secret"].as_str().unwrap()));
    assert!(!encoded.contains("epoch"));
}
