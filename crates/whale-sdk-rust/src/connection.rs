//! Internal connection ownership and shutdown coordination.

use crate::{ClientInner, ClientState, ManagedWriter, WhaleClient};
use futures::StreamExt;
use std::process::ExitStatus;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::Child;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use whale_daemon::{AnyTransportWriter, DaemonServer};

pub(crate) const LEGACY_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug)]
pub(crate) struct ConnectionShutdown {
    pub(crate) forced: bool,
    pub(crate) error: Option<String>,
    pub(crate) failure: Option<ConnectionFailure>,
    process_status: Option<ExitStatus>,
    background_reap: Option<BackgroundReap>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConnectionShutdownPhase {
    Deadline,
    OwnerTask,
    Writer,
    Reader,
    EmbeddedServer,
    ManagedChild,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConnectionFailureKind {
    Failed,
    TimedOut,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ConnectionFailure {
    pub(crate) phase: ConnectionShutdownPhase,
    pub(crate) kind: ConnectionFailureKind,
    pub(crate) message: String,
}

impl ConnectionShutdown {
    pub(crate) fn clean(process_status: Option<ExitStatus>) -> Self {
        Self {
            forced: false,
            error: None,
            failure: None,
            process_status,
            background_reap: None,
        }
    }

    #[cfg(test)]
    fn observed_process_status(&self) -> Option<ExitStatus> {
        self.process_status.clone().or_else(|| {
            self.background_reap
                .as_ref()
                .and_then(|reap| match reap.completion.borrow().as_ref() {
                    Some(Ok(status)) => Some(status.clone()),
                    _ => None,
                })
        })
    }

    #[cfg(test)]
    fn has_background_reap(&self) -> bool {
        self.background_reap.is_some()
    }

    #[cfg(test)]
    async fn wait_for_reap(&self) -> Result<ExitStatus, String> {
        if let Some(status) = &self.process_status {
            return Ok(status.clone());
        }
        let Some(reap) = &self.background_reap else {
            return Err("shutdown has no managed child reap".into());
        };
        let mut completion = reap.completion.clone();
        loop {
            if let Some(result) = completion.borrow_and_update().clone() {
                return result;
            }
            if completion.changed().await.is_err() {
                return Err("background child reaper stopped without a result".into());
            }
        }
    }
}

#[derive(Clone, Debug)]
struct BackgroundReap {
    // Kept in the cached internal result so Task 3 can observe the final reap
    // separately from the bounded shutdown result.
    #[allow(dead_code)]
    completion: watch::Receiver<Option<Result<ExitStatus, String>>>,
}

enum StopReason {
    Requested(&'static str),
    ReaderFinished,
}

/// A cloneable, non-owning way for client capabilities to stop their connection.
#[derive(Clone)]
pub(crate) struct ConnectionStop {
    command: mpsc::WeakSender<StopReason>,
    completion: watch::Receiver<Option<ConnectionShutdown>>,
}

impl ConnectionStop {
    pub(crate) async fn shutdown(&self, message: &'static str) -> ConnectionShutdown {
        if let Some(result) = self.completion.borrow().clone() {
            return result;
        }
        if let Some(command) = self.command.upgrade() {
            let _ = command.try_send(StopReason::Requested(message));
        }
        wait_for_shutdown(self.completion.clone()).await
    }
}

/// The unique strong ownership token for a connection's reader and endpoint.
/// Dropping it closes the command channel and starts emergency cleanup in the
/// already-running owner task.
pub(crate) struct ConnectionOwner {
    command: mpsc::Sender<StopReason>,
    completion: watch::Receiver<Option<ConnectionShutdown>>,
}

impl ConnectionOwner {
    /// Starts the same cached cleanup transaction used by async shutdown without
    /// requiring an entered Tokio context. This is the Runtime Drop stop fence.
    pub(crate) fn request_shutdown(&self, message: &'static str) {
        let _ = self.command.try_send(StopReason::Requested(message));
    }

    pub(crate) async fn shutdown(&self, message: &'static str) -> ConnectionShutdown {
        if let Some(result) = self.completion.borrow().clone() {
            return result;
        }
        self.request_shutdown(message);
        wait_for_shutdown(self.completion.clone()).await
    }

    // Kept for the existing process-cleanup assertion while process ownership
    // moves behind the connection owner. Task 2 replaces this private probe with
    // public Runtime shutdown diagnostics.
    #[cfg(test)]
    pub(crate) async fn lock(&self) -> &Self {
        self
    }

    #[cfg(test)]
    pub(crate) fn try_wait(&self) -> std::io::Result<Option<ExitStatus>> {
        Ok(self
            .completion
            .borrow()
            .as_ref()
            .and_then(ConnectionShutdown::observed_process_status))
    }
}

async fn wait_for_shutdown(
    mut completion: watch::Receiver<Option<ConnectionShutdown>>,
) -> ConnectionShutdown {
    loop {
        if let Some(result) = completion.borrow_and_update().clone() {
            return result;
        }
        if completion.changed().await.is_err() {
            return ConnectionShutdown {
                forced: true,
                error: Some("connection owner stopped without publishing cleanup".into()),
                failure: Some(ConnectionFailure {
                    phase: ConnectionShutdownPhase::OwnerTask,
                    kind: ConnectionFailureKind::Failed,
                    message: "connection owner stopped without publishing cleanup".into(),
                }),
                process_status: None,
                background_reap: None,
            };
        }
    }
}

pub(crate) struct OpenConnection {
    pub(crate) client: WhaleClient,
    pub(crate) owner: ConnectionOwner,
    pub(crate) process_id: Option<u32>,
}

impl OpenConnection {
    pub(crate) fn into_legacy(mut self) -> WhaleClient {
        let _legacy_process_id = self.process_id;
        let inner = Arc::get_mut(&mut self.client.inner)
            .expect("new connection must not expose client clones before assigning its owner");
        inner.compatibility_owner = Some(self.owner);
        self.client
    }
}

enum OwnedEndpoint {
    Embedded(JoinHandle<Result<(), std::io::Error>>),
    Managed(Child),
    External,
}

pub(crate) fn open_embedded(
    server: Arc<DaemonServer>,
    shutdown_timeout: Duration,
) -> OpenConnection {
    let state = ClientState::new();
    let (server_tx, server_rx) = mpsc::channel::<String>(128);
    let (client_tx, client_rx) = mpsc::channel::<String>(128);
    let writer = ManagedWriter::channel(server_tx);
    let daemon_writer = ManagedWriter::channel(client_tx);
    let server_task = tokio::spawn(async move {
        let lines = ReceiverStream::new(server_rx).map(Ok);
        server
            .run(lines, AnyTransportWriter::new(daemon_writer))
            .await
    });

    open_with_reader_task(
        state,
        writer,
        move |stop| spawn_channel_reader(client_rx, stop),
        OwnedEndpoint::Embedded(server_task),
        shutdown_timeout,
        None,
    )
}

pub(crate) fn open_io<R>(
    read: R,
    writer: Arc<ManagedWriter>,
    child: Option<Child>,
    shutdown_timeout: Duration,
) -> OpenConnection
where
    R: AsyncRead + Send + Unpin + 'static,
{
    let state = ClientState::new();
    let process_id = child.as_ref().and_then(Child::id);
    let endpoint = child
        .map(OwnedEndpoint::Managed)
        .unwrap_or(OwnedEndpoint::External);
    open_with_reader_task(
        state,
        writer,
        move |stop| spawn_io_reader(read, stop),
        endpoint,
        shutdown_timeout,
        process_id,
    )
}

/// Test seam for making a duplex peer part of the owner's joined endpoint.
/// Production embedded construction uses `open_embedded`; keeping this private
/// lets Runtime rollback tests prove completion ordering without a public hook.
#[cfg(test)]
pub(crate) fn open_io_with_owned_task<R>(
    read: R,
    writer: Arc<ManagedWriter>,
    owned_task: JoinHandle<Result<(), std::io::Error>>,
    shutdown_timeout: Duration,
) -> OpenConnection
where
    R: AsyncRead + Send + Unpin + 'static,
{
    let state = ClientState::new();
    open_with_reader_task(
        state,
        writer,
        move |stop| spawn_io_reader(read, stop),
        OwnedEndpoint::Embedded(owned_task),
        shutdown_timeout,
        None,
    )
}

fn open_with_reader_task(
    state: Arc<ClientState>,
    writer: Arc<ManagedWriter>,
    spawn_reader: impl FnOnce(ReaderStop) -> JoinHandle<()>,
    endpoint: OwnedEndpoint,
    shutdown_timeout: Duration,
    process_id: Option<u32>,
) -> OpenConnection {
    let (command, commands) = mpsc::channel(1);
    let (completed, completion) = watch::channel(None);
    let reader = spawn_reader(ReaderStop {
        state: state.clone(),
        writer: writer.clone(),
        command: command.downgrade(),
    });
    let owner_completion = completion.clone();
    tokio::spawn(run_owner(
        commands,
        completed,
        state.clone(),
        writer.clone(),
        reader,
        endpoint,
        shutdown_timeout,
    ));

    let stop = ConnectionStop {
        command: command.downgrade(),
        completion: completion.clone(),
    };
    state
        .connection
        .set(stop)
        .unwrap_or_else(|_| unreachable!("new ClientState has no connection stop"));
    OpenConnection {
        client: WhaleClient {
            inner: Arc::new(ClientInner {
                state,
                writer,
                compatibility_owner: None,
            }),
        },
        owner: ConnectionOwner {
            command,
            completion: owner_completion,
        },
        process_id,
    }
}

struct ReaderStop {
    state: Arc<ClientState>,
    writer: Arc<ManagedWriter>,
    command: mpsc::WeakSender<StopReason>,
}

impl ReaderStop {
    fn finished(self) {
        if let Some(command) = self.command.upgrade() {
            let _ = command.try_send(StopReason::ReaderFinished);
        }
    }
}

fn spawn_channel_reader(mut receiver: mpsc::Receiver<String>, stop: ReaderStop) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut shutdown = stop.state.shutdown.subscribe();
        loop {
            tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                line = receiver.recv() => match line {
                    Some(line) => stop.state.incoming(&line, &stop.writer),
                    None => break,
                }
            }
        }
        stop.finished();
    })
}

fn spawn_io_reader<R>(read: R, stop: ReaderStop) -> JoinHandle<()>
where
    R: AsyncRead + Send + Unpin + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(read).lines();
        let mut shutdown = stop.state.shutdown.subscribe();
        loop {
            tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                line = lines.next_line() => match line {
                    Ok(Some(line)) if !line.trim().is_empty() => stop.state.incoming(&line, &stop.writer),
                    Ok(Some(_)) => {},
                    _ => break,
                }
            }
        }
        stop.finished();
    })
}

async fn run_owner(
    mut commands: mpsc::Receiver<StopReason>,
    completed: watch::Sender<Option<ConnectionShutdown>>,
    state: Arc<ClientState>,
    writer: Arc<ManagedWriter>,
    reader: JoinHandle<()>,
    endpoint: OwnedEndpoint,
    shutdown_timeout: Duration,
) {
    let reason = match commands.recv().await {
        Some(StopReason::Requested(message)) => message,
        Some(StopReason::ReaderFinished) => "Daemon disconnected",
        None => "Connection owner dropped",
    };
    let outcome =
        cleanup_connection(state, writer, reader, endpoint, shutdown_timeout, reason).await;
    completed.send_replace(Some(outcome));
}

async fn cleanup_connection(
    state: Arc<ClientState>,
    writer: Arc<ManagedWriter>,
    mut reader: JoinHandle<()>,
    endpoint: OwnedEndpoint,
    shutdown_timeout: Duration,
    reason: &'static str,
) -> ConnectionShutdown {
    let started = Instant::now();
    let mut outcome = ConnectionShutdown::clean(None);
    let deadline = match started.checked_add(shutdown_timeout) {
        Some(deadline) => deadline,
        None => {
            record_failure(
                &mut outcome,
                ConnectionShutdownPhase::Deadline,
                ConnectionFailureKind::Failed,
                "shutdown timeout exceeds the platform Instant range",
            );
            started
                .checked_add(LEGACY_SHUTDOWN_TIMEOUT)
                .expect("the legacy shutdown duration fits in Instant")
        }
    };
    let graceful_deadline = deadline
        .checked_sub(force_reserve(shutdown_timeout))
        .unwrap_or(started);
    state.disconnect(reason);

    let mut writer_close = tokio::spawn(async move {
        writer.close().await;
    });
    match join_task_until(graceful_deadline, &mut writer_close).await {
        Some(Ok(())) => {}
        Some(Err(error)) => record_failure(
            &mut outcome,
            ConnectionShutdownPhase::Writer,
            ConnectionFailureKind::Failed,
            format!("local writer close task failed: {error}"),
        ),
        None => {
            outcome.forced = true;
            record_warning(&mut outcome, "local writer close exceeded graceful window");
            match join_task_until(deadline, &mut writer_close).await {
                Some(Ok(())) => {}
                Some(Err(error)) => record_failure(
                    &mut outcome,
                    ConnectionShutdownPhase::Writer,
                    ConnectionFailureKind::Failed,
                    format!("local writer close task failed after grace: {error}"),
                ),
                None => {
                    record_failure(
                        &mut outcome,
                        ConnectionShutdownPhase::Writer,
                        ConnectionFailureKind::TimedOut,
                        "local writer close timed out; close continues in background",
                    );
                    // Dropping a Tokio JoinHandle detaches the task. It keeps the
                    // writer alive and completes the close after contention ends.
                    drop(writer_close);
                }
            }
        }
    }

    match join_task_until(graceful_deadline, &mut reader).await {
        Some(Ok(())) => {}
        Some(Err(error)) => {
            record_failure(
                &mut outcome,
                ConnectionShutdownPhase::Reader,
                ConnectionFailureKind::Failed,
                format!("SDK reader task failed: {error}"),
            );
        }
        None => {
            outcome.forced = true;
            record_warning(&mut outcome, "SDK reader join timed out");
            reader.abort();
            match join_task_until(deadline, &mut reader).await {
                Some(Ok(())) => {}
                Some(Err(error)) if error.is_cancelled() => {}
                Some(Err(error)) => record_failure(
                    &mut outcome,
                    ConnectionShutdownPhase::Reader,
                    ConnectionFailureKind::Failed,
                    format!("SDK reader task failed after abort: {error}"),
                ),
                None => record_failure(
                    &mut outcome,
                    ConnectionShutdownPhase::Reader,
                    ConnectionFailureKind::TimedOut,
                    "SDK reader abort join timed out",
                ),
            }
        }
    }

    match endpoint {
        OwnedEndpoint::Embedded(mut server) => {
            match join_task_until(graceful_deadline, &mut server).await {
                Some(Ok(Ok(()))) => {}
                Some(Ok(Err(error))) => record_failure(
                    &mut outcome,
                    ConnectionShutdownPhase::EmbeddedServer,
                    ConnectionFailureKind::Failed,
                    format!("embedded server loop failed: {error}"),
                ),
                Some(Err(error)) => record_failure(
                    &mut outcome,
                    ConnectionShutdownPhase::EmbeddedServer,
                    ConnectionFailureKind::Failed,
                    format!("embedded server task failed: {error}"),
                ),
                None => {
                    outcome.forced = true;
                    record_warning(
                        &mut outcome,
                        "embedded server connection loop join timed out",
                    );
                    server.abort();
                    match join_task_until(deadline, &mut server).await {
                        Some(Ok(Ok(()))) => {}
                        Some(Ok(Err(error))) => record_failure(
                            &mut outcome,
                            ConnectionShutdownPhase::EmbeddedServer,
                            ConnectionFailureKind::Failed,
                            format!("embedded server loop failed after abort: {error}"),
                        ),
                        Some(Err(error)) if error.is_cancelled() => {}
                        Some(Err(error)) => record_failure(
                            &mut outcome,
                            ConnectionShutdownPhase::EmbeddedServer,
                            ConnectionFailureKind::Failed,
                            format!("embedded server task failed after abort: {error}"),
                        ),
                        None => record_failure(
                            &mut outcome,
                            ConnectionShutdownPhase::EmbeddedServer,
                            ConnectionFailureKind::TimedOut,
                            "embedded server abort join timed out",
                        ),
                    }
                }
            }
        }
        OwnedEndpoint::Managed(mut child) => {
            match timeout_remaining(graceful_deadline, child.wait()).await {
                Some(Ok(status)) => outcome.process_status = Some(status),
                Some(Err(error)) => {
                    outcome.forced = true;
                    record_failure(
                        &mut outcome,
                        ConnectionShutdownPhase::ManagedChild,
                        ConnectionFailureKind::Failed,
                        format!("managed child wait failed: {error}"),
                    );
                    force_child(child, deadline, &mut outcome).await;
                }
                None => {
                    outcome.forced = true;
                    record_warning(&mut outcome, "managed child graceful wait timed out");
                    force_child(child, deadline, &mut outcome).await;
                }
            }
        }
        OwnedEndpoint::External => {}
    }
    outcome
}

fn force_reserve(shutdown_timeout: Duration) -> Duration {
    Duration::from_secs(1).min(shutdown_timeout / 2)
}

async fn join_task_until<T>(
    deadline: Instant,
    task: &mut JoinHandle<T>,
) -> Option<Result<T, tokio::task::JoinError>> {
    if task.is_finished() {
        return Some(task.await);
    }
    timeout_remaining(deadline, task).await
}

async fn force_child(mut child: Child, deadline: Instant, outcome: &mut ConnectionShutdown) {
    let kill_error = child.start_kill().err();
    match timeout_remaining(deadline, child.wait()).await {
        Some(reap_result) => {
            let reaped = reap_result.is_ok();
            record_forced_reap(kill_error, reap_result, outcome);
            if !reaped {
                outcome.background_reap = Some(continue_child_reap(child));
            }
        }
        None => {
            if let Some(error) = kill_error {
                record_failure(
                    outcome,
                    ConnectionShutdownPhase::ManagedChild,
                    ConnectionFailureKind::Failed,
                    format!("managed child kill failed: {error}"),
                );
            }
            record_failure(
                outcome,
                ConnectionShutdownPhase::ManagedChild,
                ConnectionFailureKind::TimedOut,
                "managed child reap timed out; reap continues in background",
            );
            outcome.background_reap = Some(continue_child_reap(child));
        }
    }
}

fn record_forced_reap(
    kill_error: Option<std::io::Error>,
    reap_result: std::io::Result<ExitStatus>,
    outcome: &mut ConnectionShutdown,
) {
    match reap_result {
        Ok(status) => {
            if let Some(error) = kill_error {
                record_warning(
                    outcome,
                    format!("managed child kill raced with successful reap: {error}"),
                );
            }
            outcome.process_status = Some(status);
        }
        Err(error) => {
            if let Some(kill_error) = kill_error {
                record_failure(
                    outcome,
                    ConnectionShutdownPhase::ManagedChild,
                    ConnectionFailureKind::Failed,
                    format!("managed child kill failed: {kill_error}"),
                );
            }
            record_failure(
                outcome,
                ConnectionShutdownPhase::ManagedChild,
                ConnectionFailureKind::Failed,
                format!("managed child reap failed: {error}"),
            );
        }
    }
}

fn continue_child_reap(mut child: Child) -> BackgroundReap {
    let (completed, completion) = watch::channel(None);
    tokio::spawn(async move {
        let result = child
            .wait()
            .await
            .map_err(|error| format!("managed child background reap failed: {error}"));
        completed.send_replace(Some(result));
    });
    BackgroundReap { completion }
}

async fn timeout_remaining<F: std::future::Future>(
    deadline: Instant,
    future: F,
) -> Option<F::Output> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return None;
    }
    tokio::time::timeout(remaining, future).await.ok()
}

fn append_error(outcome: &mut ConnectionShutdown, error: &str) {
    match &mut outcome.error {
        Some(current) => {
            current.push_str("; ");
            current.push_str(error);
        }
        None => outcome.error = Some(error.to_owned()),
    }
}

fn record_warning(outcome: &mut ConnectionShutdown, warning: impl Into<String>) {
    let warning = warning.into();
    append_error(outcome, &warning);
}

fn record_failure(
    outcome: &mut ConnectionShutdown,
    phase: ConnectionShutdownPhase,
    kind: ConnectionFailureKind,
    error: impl Into<String>,
) {
    let error = error.into();
    append_error(outcome, &error);
    if outcome.failure.is_none() {
        outcome.failure = Some(ConnectionFailure {
            phase,
            kind,
            message: error,
        });
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::oneshot;

    fn state_and_writer() -> (Arc<ClientState>, Arc<ManagedWriter>) {
        let (sender, _receiver) = mpsc::channel(1);
        (ClientState::new(), ManagedWriter::channel(sender))
    }

    struct DropCount(Arc<AtomicUsize>);

    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn pending_counted_endpoint(count: Arc<AtomicUsize>) -> OwnedEndpoint {
        let count = DropCount(count);
        OwnedEndpoint::Embedded(tokio::spawn(async move {
            let _count = count;
            futures::future::pending::<Result<(), std::io::Error>>().await
        }))
    }

    fn immediate_counted_endpoint(count: Arc<AtomicUsize>) -> OwnedEndpoint {
        let count = DropCount(count);
        OwnedEndpoint::Embedded(tokio::spawn(async move {
            let _count = count;
            Ok(())
        }))
    }

    fn owner_fixture(
        reader: JoinHandle<()>,
        endpoint: OwnedEndpoint,
        shutdown_timeout: Duration,
    ) -> (ConnectionOwner, ConnectionStop, Arc<ClientState>) {
        let (state, writer) = state_and_writer();
        let (command, commands) = mpsc::channel(1);
        let (completed, completion) = watch::channel(None);
        tokio::spawn(run_owner(
            commands,
            completed,
            state.clone(),
            writer,
            reader,
            endpoint,
            shutdown_timeout,
        ));
        let stop = ConnectionStop {
            command: command.downgrade(),
            completion: completion.clone(),
        };
        (
            ConnectionOwner {
                command,
                completion,
            },
            stop,
            state,
        )
    }

    async fn wait_until(predicate: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !predicate() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("condition was not observed");
    }

    #[tokio::test(start_paused = true)]
    async fn writer_close_after_grace_before_total_deadline_is_forced_success() {
        let (sender, mut receiver) = mpsc::channel(1);
        let writer = ManagedWriter::channel(sender);
        let mut writer_close_started = writer.shutdown.subscribe();
        let retained_writer = writer.clone();
        let target_guard = retained_writer.target.lock().await;
        let state = ClientState::new();
        let reader = tokio::spawn(async {});
        let cleanup = tokio::spawn(cleanup_connection(
            state,
            writer,
            reader,
            OwnedEndpoint::External,
            Duration::from_millis(200),
            "test",
        ));
        while !*writer_close_started.borrow_and_update() {
            writer_close_started.changed().await.unwrap();
        }

        tokio::time::advance(Duration::from_millis(110)).await;
        tokio::task::yield_now().await;
        drop(target_guard);
        let outcome = cleanup.await.unwrap();

        assert!(outcome.forced, "missed grace was not forced: {outcome:?}");
        assert!(
            outcome.failure.is_none(),
            "writer close inside the total deadline was fatal: {outcome:?}"
        );
        assert_eq!(receiver.recv().await, None);
        drop(retained_writer);
    }

    #[tokio::test(start_paused = true)]
    async fn writer_close_after_total_deadline_reports_timeout_but_still_finishes() {
        let (sender, mut receiver) = mpsc::channel(1);
        let writer = ManagedWriter::channel(sender);
        let mut writer_close_started = writer.shutdown.subscribe();
        let retained_writer = writer.clone();
        let target_guard = retained_writer.target.lock().await;
        let state = ClientState::new();
        let reader = tokio::spawn(async {});
        let cleanup = tokio::spawn(cleanup_connection(
            state,
            writer,
            reader,
            OwnedEndpoint::External,
            Duration::from_millis(200),
            "test",
        ));
        while !*writer_close_started.borrow_and_update() {
            writer_close_started.changed().await.unwrap();
        }

        tokio::time::advance(Duration::from_millis(110)).await;
        tokio::task::yield_now().await;
        assert!(
            !cleanup.is_finished(),
            "writer close returned at the graceful rather than total deadline"
        );
        tokio::time::advance(Duration::from_millis(100)).await;
        let outcome = cleanup.await.unwrap();
        assert!(outcome.forced, "writer timeout was not forced: {outcome:?}");
        assert!(matches!(
            outcome.failure,
            Some(ConnectionFailure {
                phase: ConnectionShutdownPhase::Writer,
                kind: ConnectionFailureKind::TimedOut,
                ..
            })
        ));

        drop(target_guard);
        tokio::time::timeout(Duration::from_secs(1), async {
            assert_eq!(receiver.recv().await, None);
        })
        .await
        .expect("writer close was abandoned when the public cleanup budget expired");
        drop(retained_writer);
    }

    #[cfg(unix)]
    #[test]
    fn successful_reap_suppresses_a_raced_start_kill_error() {
        use std::os::unix::process::ExitStatusExt;

        let mut outcome = ConnectionShutdown::clean(None);
        record_forced_reap(
            Some(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "process already exited",
            )),
            Ok(ExitStatus::from_raw(0)),
            &mut outcome,
        );

        assert!(
            outcome.failure.is_none(),
            "kill/reap race became fatal: {outcome:?}"
        );
        assert!(outcome.observed_process_status().unwrap().success());
    }

    #[tokio::test]
    async fn reader_join_error_is_not_reported_as_clean_shutdown() {
        let (state, writer) = state_and_writer();
        let reader = tokio::spawn(async { panic!("reader fixture panic") });

        let outcome = cleanup_connection(
            state,
            writer,
            reader,
            OwnedEndpoint::External,
            Duration::from_secs(1),
            "test",
        )
        .await;

        assert!(
            outcome
                .error
                .as_deref()
                .is_some_and(|error| error.contains("SDK reader task failed")),
            "reader JoinError was published as clean: {outcome:?}"
        );
    }

    #[tokio::test]
    async fn embedded_join_error_is_not_reported_as_clean_shutdown() {
        let (state, writer) = state_and_writer();
        let reader = tokio::spawn(async {});
        let server = tokio::spawn(async {
            panic!("embedded fixture panic");
            #[allow(unreachable_code)]
            Ok(())
        });

        let outcome = cleanup_connection(
            state,
            writer,
            reader,
            OwnedEndpoint::Embedded(server),
            Duration::from_secs(1),
            "test",
        )
        .await;

        assert!(
            outcome
                .error
                .as_deref()
                .is_some_and(|error| error.contains("embedded server task failed")),
            "embedded JoinError was published as clean: {outcome:?}"
        );
    }

    #[tokio::test]
    async fn embedded_io_error_is_not_reported_as_clean_shutdown() {
        let (state, writer) = state_and_writer();
        let reader = tokio::spawn(async {});
        let server = tokio::spawn(async {
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "embedded fixture I/O",
            ))
        });

        let outcome = cleanup_connection(
            state,
            writer,
            reader,
            OwnedEndpoint::Embedded(server),
            Duration::from_secs(1),
            "test",
        )
        .await;

        assert!(
            outcome
                .error
                .as_deref()
                .is_some_and(|error| error.contains("embedded server loop failed")),
            "embedded io::Error was published as clean: {outcome:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reader_abort_join_cannot_exceed_the_shutdown_bound() {
        let (entered_tx, entered_rx) = oneshot::channel();
        let reader = tokio::spawn(async move {
            let _ = entered_tx.send(());
            std::thread::sleep(Duration::from_millis(300));
        });
        entered_rx.await.unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let (owner, stop, _state) = owner_fixture(
            reader,
            immediate_counted_endpoint(count.clone()),
            Duration::from_millis(20),
        );

        let outcome = tokio::time::timeout(
            Duration::from_millis(120),
            stop.shutdown("reader abort test"),
        )
        .await
        .expect("reader abort join exceeded the bounded shutdown return");
        let later = owner.shutdown("later observer").await;

        assert!(outcome.forced);
        assert!(outcome
            .error
            .as_deref()
            .is_some_and(|error| error.contains("SDK reader")));
        assert_eq!(outcome.error, later.error);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn embedded_abort_join_cannot_exceed_the_shutdown_bound() {
        let reader = tokio::spawn(async {});
        let (entered_tx, entered_rx) = oneshot::channel();
        let count = Arc::new(AtomicUsize::new(0));
        let endpoint_count = count.clone();
        let server = tokio::spawn(async move {
            let _count = DropCount(endpoint_count);
            let _ = entered_tx.send(());
            std::thread::sleep(Duration::from_millis(300));
            Ok(())
        });
        entered_rx.await.unwrap();
        let (owner, stop, _state) = owner_fixture(
            reader,
            OwnedEndpoint::Embedded(server),
            Duration::from_millis(20),
        );

        let outcome = tokio::time::timeout(
            Duration::from_millis(120),
            stop.shutdown("embedded abort test"),
        )
        .await
        .expect("embedded abort join exceeded the bounded shutdown return");
        let later = owner.shutdown("later observer").await;

        assert!(outcome.forced);
        assert!(outcome
            .error
            .as_deref()
            .is_some_and(|error| error.contains("embedded server")));
        assert_eq!(outcome.error, later.error);
        wait_until(|| count.load(Ordering::SeqCst) == 1).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn short_shutdown_timeout_still_has_a_graceful_child_window() {
        let (state, writer) = state_and_writer();
        let reader = tokio::spawn(async {});
        let mut command = tokio::process::Command::new("/bin/sleep");
        command.arg("5").kill_on_drop(true);
        let child = command.spawn().unwrap();
        let started = Instant::now();

        let outcome = cleanup_connection(
            state,
            writer,
            reader,
            OwnedEndpoint::Managed(child),
            Duration::from_millis(120),
            "test",
        )
        .await;

        assert!(outcome.forced);
        assert!(
            started.elapsed() >= Duration::from_millis(30),
            "the force reserve consumed the entire short timeout: {:?}",
            started.elapsed()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn child_reap_continues_after_the_public_shutdown_deadline() {
        let (state, writer) = state_and_writer();
        let reader = tokio::spawn(async {});
        let mut command = tokio::process::Command::new("/bin/sleep");
        command.arg("5").kill_on_drop(true);
        let child = command.spawn().unwrap();

        let outcome = cleanup_connection(
            state,
            writer,
            reader,
            OwnedEndpoint::Managed(child),
            Duration::from_nanos(1),
            "test",
        )
        .await;

        assert!(outcome.forced);
        assert!(outcome.process_status.is_none());
        assert!(
            outcome.has_background_reap(),
            "the timed-out shutdown dropped its only observable reap ownership: {outcome:?}"
        );
        let status = tokio::time::timeout(Duration::from_secs(2), outcome.wait_for_reap())
            .await
            .expect("background reaper did not finish")
            .expect("background reaper failed");
        assert!(!status.success(), "fixture should have been force-killed");
    }

    #[tokio::test]
    async fn concurrent_and_later_stops_close_the_endpoint_exactly_once() {
        let count = Arc::new(AtomicUsize::new(0));
        let reader = tokio::spawn(futures::future::pending::<()>());
        let (owner, stop, _state) = owner_fixture(
            reader,
            pending_counted_endpoint(count.clone()),
            Duration::from_millis(40),
        );
        assert_eq!(owner.command.max_capacity(), 1);

        let first = tokio::spawn({
            let stop = stop.clone();
            async move { stop.shutdown("first").await }
        });
        let second = tokio::spawn({
            let stop = stop.clone();
            async move { stop.shutdown("second").await }
        });
        let (first, second) = tokio::join!(first, second);
        let first = first.unwrap();
        let second = second.unwrap();
        let later = owner.shutdown("later").await;

        wait_until(|| count.load(Ordering::SeqCst) == 1).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(first.error, second.error);
        assert_eq!(first.error, later.error);
        assert_eq!(first.forced, later.forced);
    }

    #[tokio::test]
    async fn dropping_owner_starts_the_shared_cleanup_transaction() {
        let count = Arc::new(AtomicUsize::new(0));
        let reader = tokio::spawn(async {});
        let (owner, stop, state) = owner_fixture(
            reader,
            immediate_counted_endpoint(count.clone()),
            Duration::from_secs(1),
        );

        drop(owner);
        let outcome = stop.shutdown("observer").await;

        assert!(outcome.error.is_none(), "{outcome:?}");
        assert!(state.closed.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn synchronous_owner_stop_fence_starts_cleanup_while_owner_is_retained() {
        let count = Arc::new(AtomicUsize::new(0));
        let reader = tokio::spawn(async {});
        let (owner, _stop, state) = owner_fixture(
            reader,
            immediate_counted_endpoint(count.clone()),
            Duration::from_secs(1),
        );
        let mut completion = owner.completion.clone();

        owner.request_shutdown("synchronous stop fence");
        let outcome = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(outcome) = completion.borrow_and_update().clone() {
                    return outcome;
                }
                completion.changed().await.unwrap();
            }
        })
        .await
        .expect("synchronous owner request did not start cleanup");

        assert!(outcome.error.is_none(), "{outcome:?}");
        assert!(state.closed.load(Ordering::SeqCst));
        assert_eq!(count.load(Ordering::SeqCst), 1);
        drop(owner);
    }

    #[tokio::test]
    async fn reader_eof_starts_the_shared_cleanup_transaction() {
        let count = Arc::new(AtomicUsize::new(0));
        let (state, writer) = state_and_writer();
        let (command, commands) = mpsc::channel(1);
        let (completed, completion) = watch::channel(None);
        let reader_stop = ReaderStop {
            state: state.clone(),
            writer: writer.clone(),
            command: command.downgrade(),
        };
        let reader = tokio::spawn(async move { reader_stop.finished() });
        tokio::spawn(run_owner(
            commands,
            completed,
            state.clone(),
            writer,
            reader,
            immediate_counted_endpoint(count.clone()),
            Duration::from_secs(1),
        ));
        let owner = ConnectionOwner {
            command,
            completion,
        };

        wait_until(|| state.closed.load(std::sync::atomic::Ordering::SeqCst)).await;
        let outcome = owner.shutdown("later observer").await;

        assert!(outcome.error.is_none(), "{outcome:?}");
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancelling_a_close_waiter_does_not_cancel_shared_cleanup() {
        let count = Arc::new(AtomicUsize::new(0));
        let reader = tokio::spawn(futures::future::pending::<()>());
        let (owner, stop, state) = owner_fixture(
            reader,
            pending_counted_endpoint(count.clone()),
            Duration::from_millis(40),
        );
        let waiter = tokio::spawn({
            let stop = stop.clone();
            async move { stop.shutdown("cancelled waiter").await }
        });
        wait_until(|| state.closed.load(std::sync::atomic::Ordering::SeqCst)).await;
        waiter.abort();
        let _ = waiter.await;

        let later = stop.shutdown("later observer").await;

        assert!(later.forced);
        wait_until(|| count.load(Ordering::SeqCst) == 1).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        drop(owner);
    }

    #[tokio::test]
    async fn oversized_timeout_does_not_panic_during_emergency_cleanup() {
        let (state, writer) = state_and_writer();
        let reader = tokio::spawn(async {});

        let outcome = cleanup_connection(
            state,
            writer,
            reader,
            OwnedEndpoint::External,
            Duration::MAX,
            "test",
        )
        .await;

        assert!(outcome
            .error
            .as_deref()
            .is_some_and(|error| error.contains("Instant range")));
    }
}
