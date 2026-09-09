use serde_json::{json, Value};
use tokio::sync::mpsc;
use whale_daemon::{AnyTransportWriter, DaemonServer};

pub fn initialization_request() -> String {
    json!({"jsonrpc":"2.0","id":"test-initialize","method":"protocol.initialize","params":{"client":{"name":"daemon-tests","version":"1"},"protocol_versions":[1],"required_capabilities":["runs.v1"]}}).to_string()
}
pub async fn initialize(
    server: &DaemonServer,
    writer: &AnyTransportWriter,
    receiver: Option<&mut mpsc::UnboundedReceiver<Value>>,
) {
    server
        .handle_message(&initialization_request(), writer)
        .await;
    if let Some(receiver) = receiver {
        let ack = receiver
            .recv()
            .await
            .expect("initialization acknowledgement");
        assert_eq!(ack["id"], "test-initialize");
        assert_eq!(ack["result"]["protocol_version"], 1, "{ack}");
    }
}
