use crate::{ManagedWriter, WhaleClient};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use whale_daemon::DaemonServer;

#[tokio::test]
async fn legacy_in_process_close_waits_for_embedded_eof_cleanup() {
    let server = Arc::new(DaemonServer::default_server());
    let client = WhaleClient::in_process(server.clone());
    let session = client.create_thread("fixture", None).await.unwrap();
    assert!(server.sessions().contains_key(session.id()));

    tokio::time::timeout(Duration::from_secs(2), client.close())
        .await
        .expect("legacy close must be bounded");

    assert!(
        !server.sessions().contains_key(session.id()),
        "close returned before the embedded server observed EOF and removed its session"
    );
}

#[tokio::test]
async fn concurrent_and_later_close_callers_observe_the_completed_cleanup() {
    let server = Arc::new(DaemonServer::default_server());
    let client = WhaleClient::in_process(server.clone());
    let session = client.create_thread("fixture", None).await.unwrap();
    let session_id = session.id().to_owned();

    let first_client = client.clone();
    let second_client = client.clone();
    let first = tokio::spawn(async move { first_client.close().await });
    let second = tokio::spawn(async move { second_client.close().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        first.await.unwrap();
        second.await.unwrap();
    })
    .await
    .expect("concurrent close callers did not finish");
    assert!(
        !server.sessions().contains_key(&session_id),
        "a close caller returned before the shared cleanup completed"
    );

    tokio::time::timeout(Duration::from_millis(100), client.close())
        .await
        .expect("a later close caller must observe cached completion");
    assert!(!server.sessions().contains_key(&session_id));
}

#[tokio::test]
async fn dropping_a_nonfinal_client_clone_does_not_close_the_connection() {
    let server = Arc::new(DaemonServer::default_server());
    let client = WhaleClient::in_process(server.clone());
    let ordinary_clone = client.clone();
    drop(ordinary_clone);

    let session = client.create_thread("fixture", None).await.unwrap();
    assert!(server.sessions().contains_key(session.id()));
    client.close().await;
}

#[tokio::test]
async fn closing_a_channel_writer_drops_its_sender_and_publishes_eof() {
    let (sender, mut receiver) = mpsc::channel(1);
    let writer = ManagedWriter::channel(sender);

    writer.close().await;

    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), receiver.recv())
            .await
            .expect("receiver stayed open after writer close"),
        None
    );
}
