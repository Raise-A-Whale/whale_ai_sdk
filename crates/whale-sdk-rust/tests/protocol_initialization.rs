//! Actual stdio bootstrap acceptance. The shared runner supplies a deliberately
//! incompatible peer and independently audits its process and request log.
use serde_json::Value;
use std::{path::PathBuf, time::Duration};
use whale_sdk_rust::{SdkError, WhaleClient};

async fn failure_reaps_owned_peer(explicit: bool) {
    let path = std::env::var("WHALE_PROTOCOL_PEER").expect("shared peer executable");
    let log = PathBuf::from(std::env::var("WHALE_PROTOCOL_PEER_LOG").expect("peer JSONL log"));
    let before = std::fs::read_to_string(&log)
        .unwrap_or_default()
        .lines()
        .count();
    let client = WhaleClient::spawn_daemon(path).await.unwrap();
    let failure = tokio::time::timeout(Duration::from_secs(15), async {
        if explicit {
            client.initialize().await.map(|_| ())
        } else {
            client.create_thread("unused", None).await.map(|_| ())
        }
    })
    .await
    .expect("initialization did not honor its timeout");
    assert!(
        matches!(failure, Err(SdkError::ProtocolCompatibility(_))),
        "{failure:?}"
    );
    assert!(client.initialize().await.is_err());
    assert!(client
        .create_thread("must-not-dispatch", None)
        .await
        .is_err());

    // Check the process before an explicit close, so teardown cannot hide a leak.
    let records: Vec<Value> = std::fs::read_to_string(&log)
        .unwrap()
        .lines()
        .skip(before)
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let pid = records
        .iter()
        .find(|record| record["event"] == "started")
        .expect("peer process was not actually started")["pid"]
        .as_u64()
        .unwrap();
    let status = std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(
        !status.success(),
        "failed initialization leaked peer PID {pid}"
    );
    let methods: Vec<_> = records
        .iter()
        .filter(|record| record["event"] == "request")
        .map(|record| record["method"].as_str().unwrap())
        .collect();
    if std::env::var("WHALE_PROTOCOL_PEER_MODE").as_deref() == Ok("early_eof") {
        assert!(methods.len() <= 1);
        assert!(methods
            .iter()
            .all(|method| *method == "protocol.initialize"));
    } else {
        assert_eq!(methods, vec!["protocol.initialize"]);
    }
    client.close().await;
}

#[tokio::test]
#[ignore = "requires scripts/protocol_peer_fixture.py and WHALE_PROTOCOL_PEER_* environment"]
async fn explicit_initialization_failure_reaps_owned_peer() {
    failure_reaps_owned_peer(true).await;
}

#[tokio::test]
#[ignore = "requires scripts/protocol_peer_fixture.py and WHALE_PROTOCOL_PEER_* environment"]
async fn automatic_initialization_failure_reaps_owned_peer() {
    failure_reaps_owned_peer(false).await;
}
