//! Transport abstractions for whale-daemon JSON-RPC communication over Stdio and Unix Domain Sockets (UDS).

use async_trait::async_trait;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

/// Abstraction for sending outgoing messages to a client.
#[async_trait]
pub trait OutgoingTransport: Send + Sync {
    /// Sends a raw JSON line string (without trailing newline).
    async fn send_line(&self, line: &str) -> Result<(), std::io::Error>;
}

/// A bounded, connection-owned frame writer. Cancelling a send only drops its
/// acknowledgement; the actor still writes the complete JSON line. Explicit
/// close may interrupt I/O because that connection can never be reused.
#[derive(Clone)]
pub struct FrameWriter {
    inner: Arc<FrameWriterInner>,
}

struct FrameWriterInner {
    queue: tokio::sync::mpsc::Sender<(Vec<u8>, tokio::sync::oneshot::Sender<std::io::Result<()>>)>,
    shutdown: tokio::sync::watch::Sender<bool>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl FrameWriter {
    pub fn new<W: tokio::io::AsyncWrite + Unpin + Send + 'static>(writer: W) -> Self {
        Self::from_shared(Arc::new(Mutex::new(writer)))
    }

    fn from_shared<W: tokio::io::AsyncWrite + Unpin + Send + 'static>(
        writer: Arc<Mutex<W>>,
    ) -> Self {
        let (queue, mut frames) = tokio::sync::mpsc::channel::<(
            Vec<u8>,
            tokio::sync::oneshot::Sender<std::io::Result<()>>,
        )>(128);
        let (shutdown, mut closing) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(async move {
            loop {
                let frame = tokio::select! {
                    biased;
                    _ = closing.changed() => break,
                    frame = frames.recv() => frame,
                };
                let Some((frame, ack)) = frame else {
                    break;
                };
                let result = tokio::select! {
                    biased;
                    _ = closing.changed() => break,
                    result = async {
                        let mut out = writer.lock().await;
                        out.write_all(&frame).await?;
                        out.flush().await
                    } => result,
                };
                let failed = result.is_err();
                let _ = ack.send(result);
                if failed {
                    break;
                }
            }
        });
        Self {
            inner: Arc::new(FrameWriterInner {
                queue,
                shutdown,
                task: Mutex::new(Some(task)),
            }),
        }
    }

    /// Permanently closes the writer, interrupts blocked I/O, and joins its actor.
    pub async fn close(&self) {
        self.inner.shutdown.send_replace(true);
        let mut task = self.inner.task.lock().await;
        if let Some(task) = task.take() {
            let _ = task.await;
        }
    }
}

#[async_trait]
impl OutgoingTransport for FrameWriter {
    async fn send_line(&self, line: &str) -> std::io::Result<()> {
        let closed = || std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Frame writer closed");
        if *self.inner.shutdown.borrow() {
            return Err(closed());
        }
        let mut frame = line.as_bytes().to_vec();
        frame.push(b'\n');
        let (ack, result) = tokio::sync::oneshot::channel();
        self.inner
            .queue
            .send((frame, ack))
            .await
            .map_err(|_| closed())?;
        result.await.map_err(|_| closed())?
    }
}

/// Thread-safe writer for standard output.
pub struct StdioWriter {
    writer: FrameWriter,
}
impl StdioWriter {
    pub fn new(stdout: Arc<Mutex<tokio::io::Stdout>>) -> Self {
        Self {
            writer: FrameWriter::from_shared(stdout),
        }
    }
}
#[async_trait]
impl OutgoingTransport for StdioWriter {
    async fn send_line(&self, line: &str) -> std::io::Result<()> {
        self.writer.send_line(line).await
    }
}

/// Thread-safe writer for Unix Domain Sockets.
pub struct UnixStreamWriter {
    writer: FrameWriter,
}
impl UnixStreamWriter {
    pub fn new(write_half: tokio::net::unix::OwnedWriteHalf) -> Self {
        Self {
            writer: FrameWriter::new(write_half),
        }
    }
}
#[async_trait]
impl OutgoingTransport for UnixStreamWriter {
    async fn send_line(&self, line: &str) -> std::io::Result<()> {
        self.writer.send_line(line).await
    }
}

/// Generic transport writer wrapper.
#[derive(Clone)]
pub struct AnyTransportWriter {
    inner: Arc<dyn OutgoingTransport>,
    connection_id: Arc<str>,
}

impl AnyTransportWriter {
    /// Stable connection identity shared by clones.
    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }

    pub fn new(writer: Arc<dyn OutgoingTransport>) -> Self {
        Self {
            inner: writer,
            connection_id: uuid::Uuid::new_v4().to_string().into(),
        }
    }
}

#[async_trait]
impl OutgoingTransport for AnyTransportWriter {
    async fn send_line(&self, line: &str) -> Result<(), std::io::Error> {
        self.inner.send_line(line).await
    }
}
