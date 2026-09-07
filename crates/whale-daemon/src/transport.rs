//! Transport abstractions for whale-daemon JSON-RPC communication over Stdio and Unix Domain Sockets (UDS).

use std::sync::Arc;
use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

/// Abstraction for sending outgoing messages to a client.
#[async_trait]
pub trait OutgoingTransport: Send + Sync {
    /// Sends a raw JSON line string (without trailing newline).
    async fn send_line(&self, line: &str) -> Result<(), std::io::Error>;
}

/// Thread-safe writer for standard output.
pub struct StdioWriter {
    stdout: Arc<Mutex<tokio::io::Stdout>>,
}

impl StdioWriter {
    pub fn new(stdout: Arc<Mutex<tokio::io::Stdout>>) -> Self {
        Self { stdout }
    }
}

#[async_trait]
impl OutgoingTransport for StdioWriter {
    async fn send_line(&self, line: &str) -> Result<(), std::io::Error> {
        let mut out = self.stdout.lock().await;
        out.write_all(line.as_bytes()).await?;
        out.write_all(b"\n").await?;
        out.flush().await?;
        Ok(())
    }
}

/// Thread-safe writer for Unix Domain Sockets.
pub struct UnixStreamWriter {
    write_half: Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
}

impl UnixStreamWriter {
    pub fn new(write_half: tokio::net::unix::OwnedWriteHalf) -> Self {
        Self {
            write_half: Arc::new(Mutex::new(write_half)),
        }
    }
}

#[async_trait]
impl OutgoingTransport for UnixStreamWriter {
    async fn send_line(&self, line: &str) -> Result<(), std::io::Error> {
        let mut out = self.write_half.lock().await;
        out.write_all(line.as_bytes()).await?;
        out.write_all(b"\n").await?;
        out.flush().await?;
        Ok(())
    }
}

/// Generic transport writer wrapper.
#[derive(Clone)]
pub struct AnyTransportWriter {
    inner: Arc<dyn OutgoingTransport>,
}

impl AnyTransportWriter {
    pub fn new(writer: Arc<dyn OutgoingTransport>) -> Self {
        Self { inner: writer }
    }
}

#[async_trait]
impl OutgoingTransport for AnyTransportWriter {
    async fn send_line(&self, line: &str) -> Result<(), std::io::Error> {
        self.inner.send_line(line).await
    }
}
