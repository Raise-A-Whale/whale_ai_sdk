use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use whale_daemon::{OutgoingTransport, UnixStreamWriter};

#[tokio::test]
async fn cancelling_a_partially_written_frame_keeps_next_frame_valid() {
    let (socket, peer) = tokio::net::UnixStream::pair().unwrap();
    let (_, write) = socket.into_split();
    let writer = Arc::new(UnixStreamWriter::new(write));
    let expected = serde_json::json!({"payload":"x".repeat(2*1024*1024)});
    let line = expected.to_string();
    let first_writer = writer.clone();
    let first = tokio::spawn(async move { first_writer.send_line(&line).await });
    let mut reader = BufReader::new(peer);
    let mut frame = vec![0; 1024];
    reader.read_exact(&mut frame).await.unwrap();
    first.abort();
    let _ = first.await;
    let second_writer = writer.clone();
    let second = tokio::spawn(async move { second_writer.send_line("{\"second\":true}").await });
    tokio::time::timeout(Duration::from_secs(2), reader.read_until(b'\n', &mut frame))
        .await
        .unwrap()
        .unwrap();
    let value: serde_json::Value =
        serde_json::from_slice(&frame).expect("cancelled write corrupted JSON framing");
    assert_eq!(value, expected);
    let mut next = String::new();
    reader.read_line(&mut next).await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&next).unwrap(),
        serde_json::json!({"second":true})
    );
    second.await.unwrap().unwrap();
}

#[tokio::test]
async fn close_interrupts_blocked_io_and_permanently_rejects_sends() {
    let (socket, _peer) = tokio::io::duplex(4);
    let writer = whale_daemon::transport::FrameWriter::new(socket);
    let clone = writer.clone();
    let blocked = tokio::spawn(async move { clone.send_line("{\"long\":\"payload\"}").await });
    tokio::task::yield_now().await;
    tokio::time::timeout(Duration::from_secs(1), writer.close())
        .await
        .unwrap();
    assert!(blocked.await.unwrap().is_err());
    assert!(writer.send_line("{}").await.is_err());
}
