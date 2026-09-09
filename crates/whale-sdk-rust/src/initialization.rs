//! Shared connection-owned bootstrap; cancellation only abandons a caller's wait.

use crate::*;
use whale_protocol::initialization::{InitializeParams, InitializeResult, METHOD_INITIALIZE};

const LEGACY_INITIALIZATION_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum InitializationFailure {
    Failed(String),
    TimedOut { timeout: Duration },
}

impl std::fmt::Display for InitializationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed(message) => formatter.write_str(message),
            Self::TimedOut { timeout } => write!(
                formatter,
                "protocol.initialize timed out after {} seconds",
                timeout.as_secs_f64()
            ),
        }
    }
}

pub(crate) type Initialization =
    std::sync::OnceLock<watch::Sender<Option<Result<InitializeResult, InitializationFailure>>>>;

impl ClientState {
    /// Reader-side gate: never await the handshake on the task that receives its ACK.
    pub(crate) fn is_initialized(&self) -> bool {
        !self.closed.load(Ordering::SeqCst)
            && self
                .initialization
                .get()
                .is_some_and(|completion| matches!(completion.borrow().as_ref(), Some(Ok(_))))
    }

    pub(crate) fn require_initialized(&self) -> Result<(), SdkError> {
        match self
            .initialization
            .get()
            .and_then(|state| state.borrow().clone())
        {
            Some(Ok(_)) => Ok(()),
            Some(Err(error)) => Err(SdkError::ProtocolCompatibility(error.to_string())),
            None => Err(SdkError::ProtocolCompatibility(
                "Connection initialization is not complete".into(),
            )),
        }
    }
}

impl WhaleClient {
    /// Validates protocol version and required daemon features once per connection.
    /// Ordinary SDK operations call this automatically. Dropping a waiter does not
    /// restart or cancel the shared handshake; explicit client close still stops it.
    /// Failure permanently closes the transport and any owned daemon process.
    pub async fn initialize(&self) -> Result<InitializeResult, SdkError> {
        let deadline = tokio::time::Instant::now()
            .checked_add(LEGACY_INITIALIZATION_TIMEOUT)
            .expect("the legacy initialization timeout fits in Tokio Instant");
        self.initialize_until(deadline, LEGACY_INITIALIZATION_TIMEOUT)
            .await
            .map_err(|failure| SdkError::ProtocolCompatibility(failure.to_string()))
    }

    /// Connection-owned initialization using the caller's one startup deadline.
    /// The first caller fixes that deadline for the shared handshake; abandoning any
    /// waiter leaves the worker and its failure cleanup running.
    pub(crate) async fn initialize_until(
        &self,
        deadline: tokio::time::Instant,
        timeout: Duration,
    ) -> Result<InitializeResult, InitializationFailure> {
        // Even a reader that observed EOF before the first call must enter the
        // shared cleanup transaction: a peer may close stdout but stay alive.
        let completion = self.inner.state.initialization.get_or_init(|| {
            let (sender, _) = watch::channel(None);
            let completed = sender.clone();
            let client = self.clone();
            tokio::spawn(async move {
                let params = InitializeParams::sdk("whale-rust", env!("CARGO_PKG_VERSION"));
                let response = tokio::time::timeout_at(
                    deadline,
                    client.inner.state.request_inner::<_, InitializeResult>(
                        &client.inner.writer,
                        METHOD_INITIALIZE,
                        Some(params.clone()),
                        None,
                    ),
                )
                .await;
                let outcome = match response {
                    Ok(Ok(result)) => result
                        .validate_for(&params)
                        .map(|_| result)
                        .map_err(InitializationFailure::Failed),
                    Ok(Err(error)) => Err(InitializationFailure::Failed(error.to_string())),
                    Err(_) => Err(InitializationFailure::TimedOut { timeout }),
                };
                if outcome.is_err() {
                    client.close().await;
                }
                completed.send_replace(Some(outcome));
            });
            sender
        });
        let mut receiver = completion.subscribe();
        loop {
            let outcome = receiver.borrow_and_update().clone();
            if let Some(outcome) = outcome {
                return match outcome {
                    Ok(_) if self.inner.state.closed.load(Ordering::SeqCst) => Err(
                        InitializationFailure::Failed("Client closed after initialization".into()),
                    ),
                    outcome => outcome,
                };
            }
            receiver.changed().await.map_err(|_| {
                InitializationFailure::Failed("Initialization completion channel closed".into())
            })?;
        }
    }
}
