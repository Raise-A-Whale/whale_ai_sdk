mod common;

use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::mpsc;
use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
use whale_daemon::{AnyTransportWriter, DaemonServer, OutgoingTransport};
use whale_protocol::{CanonicalContent, CanonicalItem};
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
) {
    let gate = Arc::new(ApprovalGate::new());
    let engine = AgentEngine::new(Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    )))
    .with_stream_provider(Arc::new(|_, _| {
        Ok(Box::pin(futures::stream::iter([Ok(
            whale_protocol::AgentStreamEvent::TurnCompleted {
                thread_id: "provider".into(),
                turn_id: "provider".into(),
                usage: Default::default(),
            },
        )])))
    }));
    let (tx, rx) = mpsc::unbounded_channel();
    (
        DaemonServer::new(Arc::new(engine), gate),
        AnyTransportWriter::new(Arc::new(Capture(tx))),
        rx,
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

fn user_item(id: &str, text: impl Into<String>) -> CanonicalItem {
    CanonicalItem::UserMessage {
        id: id.into(),
        content: vec![CanonicalContent::Text { text: text.into() }],
    }
}

async fn run_item(
    server: &DaemonServer,
    writer: &AnyTransportWriter,
    rx: &mut mpsc::UnboundedReceiver<Value>,
    request_id: u64,
    thread_id: &str,
    item_id: &str,
    text: impl Into<String>,
) {
    let turn_id = format!("turn-{item_id}");
    let response = call(
        server,
        writer,
        rx,
        request_id,
        "thread.start_turn",
        json!({
            "thread_id":thread_id,
            "turn_id":turn_id,
            "input_items":[user_item(item_id, text)]
        }),
    )
    .await;
    assert!(response.get("error").is_none(), "{response}");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let frame = rx.recv().await.expect("terminal Run event");
            if frame["method"] == "turn.event"
                && frame["params"]["type"] == "finished"
                && frame["params"]["turn_id"] == turn_id
            {
                return;
            }
        }
    })
    .await
    .expect("terminal Run event deadline");
}

#[tokio::test]
async fn backward_history_freezes_through_while_new_items_append() {
    let (server, writer, mut rx) = fixture();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    send(
        &server,
        &writer,
        1,
        "session.start_thread",
        json!({"session_id":"history","model":"test"}),
    )
    .await;
    assert!(response(&mut rx, 1).await.get("error").is_none());

    for (offset, item) in ["item-0", "item-1", "item-2", "item-3", "item-4"]
        .into_iter()
        .enumerate()
    {
        run_item(
            &server,
            &writer,
            &mut rx,
            10 + offset as u64,
            "history",
            item,
            item,
        )
        .await;
    }

    let page = call(
        &server,
        &writer,
        &mut rx,
        20,
        "session.history",
        json!({"thread_id":"history","limit":2}),
    )
    .await;
    assert_eq!(page["result"]["start_index"], 3, "{page}");
    assert_eq!(page["result"]["end_index"], 5, "{page}");
    assert_eq!(page["result"]["through"]["index"], 5, "{page}");
    let page_ids: Vec<_> = page["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_str().unwrap())
        .collect();
    assert_eq!(page_ids, ["item-3", "item-4"]);
    let cursor = page["result"]["next_cursor"].clone();

    run_item(&server, &writer, &mut rx, 30, "history", "item-5", "item-5").await;
    let second = call(
        &server,
        &writer,
        &mut rx,
        31,
        "session.history",
        json!({"thread_id":"history","cursor":cursor,"limit":2}),
    )
    .await;
    assert_eq!(second["result"]["start_index"], 1, "{second}");
    assert_eq!(second["result"]["end_index"], 3, "{second}");
    assert_eq!(second["result"]["through"]["index"], 5, "{second}");
    assert_eq!(second["result"]["current_end"]["index"], 6, "{second}");
    let second_ids: Vec<_> = second["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_str().unwrap())
        .collect();
    assert_eq!(second_ids, ["item-1", "item-2"]);

    let snapshot = call(
        &server,
        &writer,
        &mut rx,
        32,
        "session.get.v2",
        json!({"thread_id":"history","history_limit":2}),
    )
    .await;
    let before = json!({
        "thread_id":"history",
        "stream_id":snapshot["result"]["cursor"]["stream_id"],
        "index":snapshot["result"]["history"]["start_index"]
    });
    let before_page = call(
        &server,
        &writer,
        &mut rx,
        33,
        "session.history",
        json!({"thread_id":"history","before":before,"limit":2}),
    )
    .await;
    assert_eq!(before_page["result"]["end_index"], 4, "{before_page}");
    let before_ids: Vec<_> = before_page["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_str().unwrap())
        .collect();
    assert_eq!(before_ids, ["item-2", "item-3"]);

    let closed = call(
        &server,
        &writer,
        &mut rx,
        34,
        "session.close",
        json!({"thread_id":"history"}),
    )
    .await;
    assert!(closed.get("error").is_none(), "{closed}");
    let tombstone_page = call(
        &server,
        &writer,
        &mut rx,
        35,
        "session.history",
        json!({"thread_id":"history","limit":1}),
    )
    .await;
    assert_eq!(tombstone_page["result"]["end_index"], 6, "{tombstone_page}");
}

#[tokio::test]
async fn history_rejects_stream_mismatch_forgery_and_oversized_item() {
    let (server, writer, mut rx) = fixture();
    common::initialize(&server, &writer, Some(&mut rx)).await;
    let created = call(
        &server,
        &writer,
        &mut rx,
        1,
        "session.start_thread",
        json!({"session_id":"history","model":"test"}),
    )
    .await;
    assert!(created.get("error").is_none(), "{created}");
    run_item(
        &server,
        &writer,
        &mut rx,
        2,
        "history",
        "huge",
        "x".repeat(whale_protocol::session_management::MAX_SESSION_MANAGEMENT_PAGE_BYTES + 32),
    )
    .await;

    let oversized = call(
        &server,
        &writer,
        &mut rx,
        3,
        "session.history",
        json!({"thread_id":"history","limit":1}),
    )
    .await;
    assert_eq!(
        oversized["error"]["data"]["kind"], "resource_limit",
        "{oversized}"
    );
    assert_eq!(oversized["error"]["data"]["item_index"], 0);

    let snapshot = call(
        &server,
        &writer,
        &mut rx,
        4,
        "session.get.v2",
        json!({"thread_id":"history","history_limit":1}),
    )
    .await;
    let reset = call(
        &server,
        &writer,
        &mut rx,
        5,
        "session.history",
        json!({
            "thread_id":"history",
            "before":{
                "thread_id":"history",
                "stream_id":"00000000-0000-4000-8000-000000000000",
                "index":1
            },
            "limit":1
        }),
    )
    .await;
    assert_eq!(
        reset["error"]["data"]["kind"], "history_stream_reset",
        "{reset}"
    );
    assert_ne!(
        reset["error"]["data"]["current"]["stream_id"],
        "00000000-0000-4000-8000-000000000000"
    );
    assert_eq!(
        reset["error"]["data"]["current"]["stream_id"],
        snapshot["result"]["cursor"]["stream_id"]
    );
}

#[tokio::test]
async fn persistent_reattach_uses_a_new_history_stream_and_old_anchor_resets() {
    let (server, owner_a, mut rx_a) = fixture();
    let runtime = Arc::new(
        StoreRuntime::open(Arc::new(MemoryStore::new()))
            .await
            .unwrap(),
    );
    let server = server.with_store_runtime(runtime);
    let (b_tx, mut rx_b) = mpsc::unbounded_channel();
    let owner_b = AnyTransportWriter::new(Arc::new(Capture(b_tx)));
    common::initialize(&server, &owner_a, Some(&mut rx_a)).await;
    common::initialize(&server, &owner_b, Some(&mut rx_b)).await;
    let thread = "11111111-1111-4111-8111-111111111111";
    let attached_thread = "22222222-2222-4222-8222-222222222222";
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
        json!({"key":key,"session":config(thread),"run_defaults":{}}),
    )
    .await;
    assert!(created.get("error").is_none(), "{created}");
    for (id, item) in [(2, "persistent-0"), (3, "persistent-1")] {
        run_item(&server, &owner_a, &mut rx_a, id, thread, item, item).await;
    }
    let old_snapshot = call(
        &server,
        &owner_a,
        &mut rx_a,
        4,
        "session.get.v2",
        json!({"thread_id":thread,"history_limit":1}),
    )
    .await;
    let old_stream = old_snapshot["result"]["cursor"]["stream_id"].clone();
    let old_anchor = json!({
        "thread_id":attached_thread,
        "stream_id":old_snapshot["result"]["cursor"]["stream_id"],
        "index":old_snapshot["result"]["history"]["total_items"]
    });
    let old_page = call(
        &server,
        &owner_a,
        &mut rx_a,
        5,
        "session.history",
        json!({"thread_id":thread,"limit":1}),
    )
    .await;
    let old_cursor = old_page["result"]["next_cursor"].clone();
    assert!(call(
        &server,
        &owner_a,
        &mut rx_a,
        6,
        "session.close",
        json!({"thread_id":thread}),
    )
    .await
    .get("error")
    .is_none());
    server.disconnect_connection(&owner_a).await;

    let inspected = call(
        &server,
        &owner_b,
        &mut rx_b,
        7,
        "session.recovery.inspect",
        json!({"key":key}),
    )
    .await;
    let attached = call(
        &server,
        &owner_b,
        &mut rx_b,
        8,
        "session.recovery.attach",
        json!({
            "key":key,
            "expected_revision":inspected["result"]["revision"],
            "session":config(attached_thread),
            "run_defaults":{}
        }),
    )
    .await;
    assert!(attached.get("error").is_none(), "{attached}");
    let reset = call(
        &server,
        &owner_b,
        &mut rx_b,
        9,
        "session.history",
        json!({"thread_id":attached_thread,"before":old_anchor,"limit":1}),
    )
    .await;
    assert_eq!(
        reset["error"]["data"]["kind"], "history_stream_reset",
        "{reset}"
    );
    assert_eq!(reset["error"]["data"]["requested"]["stream_id"], old_stream);
    let owner_bound = call(
        &server,
        &owner_b,
        &mut rx_b,
        10,
        "session.history",
        json!({"thread_id":attached_thread,"cursor":old_cursor,"limit":1}),
    )
    .await;
    assert_eq!(
        owner_bound["error"]["data"]["kind"], "history_cursor_invalid",
        "{owner_bound}"
    );
}
