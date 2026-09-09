//! Application-facing ownership for a Whale Agent runtime.

use crate::connection::{self, ConnectionFailureKind, ConnectionShutdown, ConnectionShutdownPhase};
use crate::{
    Agent, AgentDefinition, HostTool, InitializeResult, ManagedWriter, SdkError, ToolPack,
    WhaleClient,
};
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use whale_daemon::DaemonServer;

const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeMode {
    Embedded,
    ManagedProcess,
    ExternalUds,
}

pub enum RuntimeSource {
    Embedded {
        server: DaemonServer,
    },
    ManagedProcess {
        executable: PathBuf,
        args: Vec<OsString>,
        environment: Vec<(OsString, OsString)>,
    },
    ExternalUds {
        path: PathBuf,
    },
}

impl RuntimeSource {
    fn mode(&self) -> RuntimeMode {
        match self {
            Self::Embedded { .. } => RuntimeMode::Embedded,
            Self::ManagedProcess { .. } => RuntimeMode::ManagedProcess,
            Self::ExternalUds { .. } => RuntimeMode::ExternalUds,
        }
    }
}

impl fmt::Debug for RuntimeSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Embedded { .. } => formatter
                .debug_struct("Embedded")
                .field("server", &"<embedded>")
                .finish(),
            Self::ManagedProcess {
                executable,
                args,
                environment,
            } => {
                let environment_names: Vec<_> = environment.iter().map(|(name, _)| name).collect();
                formatter
                    .debug_struct("ManagedProcess")
                    .field("executable", executable)
                    .field("args", args)
                    .field("environment", &environment_names)
                    .finish()
            }
            Self::ExternalUds { path } => formatter
                .debug_struct("ExternalUds")
                .field("path", path)
                .finish(),
        }
    }
}

pub struct RuntimeOptions {
    pub source: RuntimeSource,
    pub startup_timeout: Duration,
    pub shutdown_timeout: Duration,
}

impl RuntimeOptions {
    pub fn embedded() -> Self {
        Self {
            source: RuntimeSource::Embedded {
                server: DaemonServer::default_server(),
            },
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }

    pub fn managed(executable: impl Into<PathBuf>) -> Self {
        Self {
            source: RuntimeSource::ManagedProcess {
                executable: executable.into(),
                args: Vec::new(),
                environment: Vec::new(),
            },
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }

    pub fn external_uds(
        path: impl Into<PathBuf>,
        startup_timeout: Duration,
        shutdown_timeout: Duration,
    ) -> Self {
        Self {
            source: RuntimeSource::ExternalUds { path: path.into() },
            startup_timeout,
            shutdown_timeout,
        }
    }

    pub fn with_arg(mut self, argument: impl Into<OsString>) -> Self {
        if let RuntimeSource::ManagedProcess { args, .. } = &mut self.source {
            args.push(argument.into());
        }
        self
    }

    pub fn with_environment(
        mut self,
        name: impl Into<OsString>,
        value: impl Into<OsString>,
    ) -> Self {
        if let RuntimeSource::ManagedProcess { environment, .. } = &mut self.source {
            environment.push((name.into(), value.into()));
        }
        self
    }
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self::embedded()
    }
}

impl fmt::Debug for RuntimeOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeOptions")
            .field("source", &self.source)
            .field("startup_timeout", &self.startup_timeout)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeInfo {
    pub mode: RuntimeMode,
    pub peer: InitializeResult,
    pub process_id: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShutdownDisposition {
    Graceful,
    Forced,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeShutdown {
    pub mode: RuntimeMode,
    pub disposition: ShutdownDisposition,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeShutdownPhase {
    Deadline,
    OwnerTask,
    Writer,
    Reader,
    EmbeddedServer,
    ManagedChild,
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum RuntimeError {
    #[error("invalid runtime configuration for {field}: {message}")]
    InvalidConfiguration {
        field: &'static str,
        message: String,
    },
    #[error("failed to open {mode:?} runtime source: {message}")]
    SourceFailure { mode: RuntimeMode, message: String },
    #[error("{mode:?} runtime initialization failed: {message}")]
    InitializationFailure { mode: RuntimeMode, message: String },
    #[error("{mode:?} runtime startup timed out after {timeout:?}")]
    StartupTimeout {
        mode: RuntimeMode,
        timeout: Duration,
    },
    #[error("{mode:?} runtime shutdown failed in {phase:?}: {message}")]
    ShutdownFailure {
        mode: RuntimeMode,
        phase: RuntimeShutdownPhase,
        message: String,
    },
    #[error("{mode:?} runtime shutdown timed out in {phase:?} after {timeout:?}")]
    ShutdownTimeout {
        mode: RuntimeMode,
        phase: RuntimeShutdownPhase,
        timeout: Duration,
    },
}

/// Unique application owner. `WhaleClient` clones expose capability only and do
/// not retain this value's connection owner.
pub struct WhaleRuntime {
    client: WhaleClient,
    owner: connection::ConnectionOwner,
    info: RuntimeInfo,
    shutdown_timeout: Duration,
}

#[derive(Clone, Copy)]
struct SourceOpenContext {
    startup_deadline: tokio::time::Instant,
    startup_timeout: Duration,
    shutdown_timeout: Duration,
}

impl fmt::Debug for WhaleRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WhaleRuntime")
            .field("info", &self.info)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .finish_non_exhaustive()
    }
}

impl WhaleRuntime {
    pub async fn open(options: RuntimeOptions) -> Result<Self, RuntimeError> {
        Self::open_with_source_factory(options, open_source).await
    }

    async fn open_with_source_factory<F, FactoryFuture>(
        options: RuntimeOptions,
        source_factory: F,
    ) -> Result<Self, RuntimeError>
    where
        F: FnOnce(RuntimeSource, SourceOpenContext) -> FactoryFuture,
        FactoryFuture: Future<Output = Result<connection::OpenConnection, RuntimeError>>,
    {
        validate_options(&options)?;
        let RuntimeOptions {
            source,
            startup_timeout,
            shutdown_timeout,
        } = options;
        let mode = source.mode();
        let startup_deadline = tokio::time::Instant::now()
            .checked_add(startup_timeout)
            .ok_or_else(|| RuntimeError::InvalidConfiguration {
                field: "startup_timeout",
                message: "duration exceeds the platform Instant range".into(),
            })?;
        // A source future may already own a spawned resource while rolling an
        // acquisition failure back. Let that rollback finish instead of dropping
        // it at the deadline; the original deadline still reduces (or exhausts)
        // the initialization window once source creation succeeds.
        let source_context = SourceOpenContext {
            startup_deadline,
            startup_timeout,
            shutdown_timeout,
        };
        let connection = source_factory(source, source_context).await?;
        let peer = connection
            .client
            .initialize_until(startup_deadline, startup_timeout)
            .await
            .map_err(|failure| match failure {
                crate::initialization::InitializationFailure::TimedOut { timeout } => {
                    RuntimeError::StartupTimeout { mode, timeout }
                }
                crate::initialization::InitializationFailure::Failed(message) => {
                    RuntimeError::InitializationFailure { mode, message }
                }
            })?;
        let info = RuntimeInfo {
            mode,
            peer,
            process_id: connection.process_id,
        };
        Ok(Self {
            client: connection.client,
            owner: connection.owner,
            info,
            shutdown_timeout,
        })
    }

    pub fn info(&self) -> &RuntimeInfo {
        &self.info
    }

    pub fn client(&self) -> &WhaleClient {
        &self.client
    }

    pub fn agent(
        &self,
        definition: AgentDefinition,
        tools: Vec<Arc<dyn HostTool>>,
    ) -> Result<Agent, SdkError> {
        self.client.agent(definition, tools)
    }

    /// Captures an Agent with reusable ToolPack factories through this runtime's client.
    pub fn agent_with_tool_packs(
        &self,
        definition: AgentDefinition,
        tools: Vec<Arc<dyn HostTool>>,
        packs: Vec<Arc<dyn ToolPack>>,
    ) -> Result<Agent, SdkError> {
        self.client.agent_with_tool_packs(definition, tools, packs)
    }

    pub async fn shutdown(&self) -> Result<RuntimeShutdown, RuntimeError> {
        self.client.inner.state.disconnect("Runtime shutdown");
        map_shutdown(
            self.info.mode,
            self.shutdown_timeout,
            self.owner.shutdown("Runtime shutdown").await,
        )
    }
}

async fn open_source(
    source: RuntimeSource,
    context: SourceOpenContext,
) -> Result<connection::OpenConnection, RuntimeError> {
    match source {
        RuntimeSource::Embedded { server } => Ok(connection::open_embedded(
            Arc::new(server),
            context.shutdown_timeout,
        )),
        RuntimeSource::ManagedProcess {
            executable,
            args,
            environment,
        } => open_managed(executable, args, environment, context.shutdown_timeout).await,
        RuntimeSource::ExternalUds { path } => open_external(path, context).await,
    }
}

#[cfg(unix)]
async fn open_external(
    path: PathBuf,
    context: SourceOpenContext,
) -> Result<connection::OpenConnection, RuntimeError> {
    let connector = UnixStream::connect(&path);
    open_external_with_connector(
        path.clone(),
        context.startup_deadline,
        context.startup_timeout,
        context.shutdown_timeout,
        connector,
    )
    .await
}

#[cfg(unix)]
async fn open_external_with_connector<F>(
    path: PathBuf,
    startup_deadline: tokio::time::Instant,
    startup_timeout: Duration,
    shutdown_timeout: Duration,
    connector: F,
) -> Result<connection::OpenConnection, RuntimeError>
where
    F: Future<Output = std::io::Result<UnixStream>>,
{
    let stream = match tokio::time::timeout_at(startup_deadline, connector).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(error)) => {
            return Err(RuntimeError::SourceFailure {
                mode: RuntimeMode::ExternalUds,
                message: format!("external UDS connect to {} failed: {error}", path.display()),
            });
        }
        Err(_) => {
            return Err(RuntimeError::StartupTimeout {
                mode: RuntimeMode::ExternalUds,
                timeout: startup_timeout,
            });
        }
    };
    let (read, write) = stream.into_split();
    Ok(connection::open_io(
        read,
        ManagedWriter::io(write),
        None,
        shutdown_timeout,
    ))
}

#[cfg(not(unix))]
async fn open_external(
    path: PathBuf,
    _context: SourceOpenContext,
) -> Result<connection::OpenConnection, RuntimeError> {
    Err(RuntimeError::SourceFailure {
        mode: RuntimeMode::ExternalUds,
        message: format!(
            "external UDS transport is unavailable on this platform: {}",
            path.display()
        ),
    })
}

async fn open_managed(
    executable: PathBuf,
    args: Vec<OsString>,
    environment: Vec<(OsString, OsString)>,
    shutdown_timeout: Duration,
) -> Result<connection::OpenConnection, RuntimeError> {
    let mut command = Command::new(executable);
    command
        .args(["--listen", "stdio"])
        .args(args)
        .envs(environment)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    let child = command
        .spawn()
        .map_err(|error| RuntimeError::SourceFailure {
            mode: RuntimeMode::ManagedProcess,
            message: format!("managed process spawn failed: {error}"),
        })?;
    let (stdout, stdin, child) = claim_managed_stdio(child).await?;
    Ok(connection::open_io(
        stdout,
        ManagedWriter::io(stdin),
        Some(child),
        shutdown_timeout,
    ))
}

async fn claim_managed_stdio(
    mut child: Child,
) -> Result<(ChildStdout, ChildStdin, Child), RuntimeError> {
    let stdin = child.stdin.take();
    let stdout = child.stdout.take();
    match (stdout, stdin) {
        (Some(stdout), Some(stdin)) => Ok((stdout, stdin, child)),
        _ => {
            let cleanup = tokio::spawn(async move {
                let kill_error = child.start_kill().err();
                let wait_result = child.wait().await;
                (kill_error, wait_result)
            });
            let cleanup_message = match cleanup.await {
                Ok((_, Ok(_))) => String::new(),
                Ok((kill_error, Err(wait_error))) => match kill_error {
                    Some(kill_error) => {
                        format!("; kill failed: {kill_error}; reap failed: {wait_error}")
                    }
                    None => format!("; reap failed: {wait_error}"),
                },
                Err(error) => format!("; cleanup task failed: {error}"),
            };
            Err(RuntimeError::SourceFailure {
                mode: RuntimeMode::ManagedProcess,
                message: format!(
                    "managed process did not expose piped stdin/stdout{cleanup_message}"
                ),
            })
        }
    }
}

impl Drop for WhaleRuntime {
    fn drop(&mut self) {
        self.client.inner.state.disconnect("Runtime dropped");
        self.owner.request_shutdown("Runtime dropped");
    }
}

fn validate_options(options: &RuntimeOptions) -> Result<(), RuntimeError> {
    for (field, duration) in [
        ("startup_timeout", options.startup_timeout),
        ("shutdown_timeout", options.shutdown_timeout),
    ] {
        if duration.is_zero() {
            return Err(RuntimeError::InvalidConfiguration {
                field,
                message: "duration must be positive".into(),
            });
        }
        if tokio::time::Instant::now().checked_add(duration).is_none() {
            return Err(RuntimeError::InvalidConfiguration {
                field,
                message: "duration exceeds the platform Instant range".into(),
            });
        }
    }
    if options.shutdown_timeout < Duration::from_nanos(2) {
        return Err(RuntimeError::InvalidConfiguration {
            field: "shutdown_timeout",
            message: "duration must leave time for both graceful and forced cleanup".into(),
        });
    }
    if let RuntimeSource::ManagedProcess {
        executable,
        args,
        environment,
    } = &options.source
    {
        if os_contains_nul(executable.as_os_str()) {
            return Err(RuntimeError::InvalidConfiguration {
                field: "executable",
                message: "managed executable contains NUL".into(),
            });
        }
        if args.iter().any(|argument| {
            os_contains_nul(argument.as_os_str()) || is_owned_listen_argument(argument.as_os_str())
        }) {
            return Err(RuntimeError::InvalidConfiguration {
                field: "args",
                message: "managed arguments contain NUL or the SDK-owned --listen option".into(),
            });
        }
        if environment.iter().any(|(name, value)| {
            name.is_empty()
                || os_contains_nul(name.as_os_str())
                || os_contains_equals(name.as_os_str())
                || os_contains_nul(value.as_os_str())
        }) {
            return Err(RuntimeError::InvalidConfiguration {
                field: "environment",
                message: "managed environment names must be nonempty without '=' or NUL, and values must not contain NUL".into(),
            });
        }
    }
    Ok(())
}

#[cfg(unix)]
fn os_contains_nul(value: &OsStr) -> bool {
    use std::os::unix::ffi::OsStrExt;
    value.as_bytes().contains(&0)
}

#[cfg(unix)]
fn os_contains_equals(value: &OsStr) -> bool {
    use std::os::unix::ffi::OsStrExt;
    value.as_bytes().contains(&b'=')
}

#[cfg(unix)]
fn is_owned_listen_argument(value: &OsStr) -> bool {
    use std::os::unix::ffi::OsStrExt;
    let bytes = value.as_bytes();
    bytes == b"--listen" || bytes.starts_with(b"--listen=")
}

#[cfg(windows)]
fn os_contains_nul(value: &OsStr) -> bool {
    use std::os::windows::ffi::OsStrExt;
    value.encode_wide().any(|unit| unit == 0)
}

#[cfg(windows)]
fn os_contains_equals(value: &OsStr) -> bool {
    use std::os::windows::ffi::OsStrExt;
    value.encode_wide().any(|unit| unit == b'=' as u16)
}

#[cfg(windows)]
fn is_owned_listen_argument(value: &OsStr) -> bool {
    use std::os::windows::ffi::OsStrExt;
    let units: Vec<_> = value.encode_wide().collect();
    let exact: Vec<_> = "--listen".encode_utf16().collect();
    let prefix: Vec<_> = "--listen=".encode_utf16().collect();
    units == exact || units.starts_with(&prefix)
}

fn map_shutdown(
    mode: RuntimeMode,
    timeout: Duration,
    outcome: ConnectionShutdown,
) -> Result<RuntimeShutdown, RuntimeError> {
    if let Some(failure) = outcome.failure {
        let phase = map_shutdown_phase(failure.phase);
        return match failure.kind {
            ConnectionFailureKind::Failed => Err(RuntimeError::ShutdownFailure {
                mode,
                phase,
                message: failure.message,
            }),
            ConnectionFailureKind::TimedOut => Err(RuntimeError::ShutdownTimeout {
                mode,
                phase,
                timeout,
            }),
        };
    }
    Ok(RuntimeShutdown {
        mode,
        disposition: if outcome.forced {
            ShutdownDisposition::Forced
        } else {
            ShutdownDisposition::Graceful
        },
    })
}

fn map_shutdown_phase(phase: ConnectionShutdownPhase) -> RuntimeShutdownPhase {
    match phase {
        ConnectionShutdownPhase::Deadline => RuntimeShutdownPhase::Deadline,
        ConnectionShutdownPhase::OwnerTask => RuntimeShutdownPhase::OwnerTask,
        ConnectionShutdownPhase::Writer => RuntimeShutdownPhase::Writer,
        ConnectionShutdownPhase::Reader => RuntimeShutdownPhase::Reader,
        ConnectionShutdownPhase::EmbeddedServer => RuntimeShutdownPhase::EmbeddedServer,
        ConnectionShutdownPhase::ManagedChild => RuntimeShutdownPhase::ManagedChild,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::{ConnectionFailure, ConnectionFailureKind};
    use crate::ManagedWriter;
    use serde_json::{json, Value};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, ReadHalf};
    use tokio::sync::oneshot;
    use whale_protocol::initialization::{InitializeParams, PeerInfo, METHOD_INITIALIZE};

    #[cfg(unix)]
    unsafe extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
    }

    fn fake_transport() -> (ReadHalf<DuplexStream>, Arc<ManagedWriter>, DuplexStream) {
        let (client_io, peer_io) = tokio::io::duplex(8 * 1024);
        let (read, write) = tokio::io::split(client_io);
        (read, ManagedWriter::io(write), peer_io)
    }

    fn test_options(startup_timeout: Duration, shutdown_timeout: Duration) -> RuntimeOptions {
        RuntimeOptions {
            source: RuntimeSource::Embedded {
                server: DaemonServer::default_server(),
            },
            startup_timeout,
            shutdown_timeout,
        }
    }

    #[cfg(unix)]
    #[tokio::test(start_paused = true)]
    async fn external_connect_timeout_maps_to_the_typed_startup_timeout() {
        let startup_timeout = Duration::from_millis(40);
        let startup_deadline = tokio::time::Instant::now()
            .checked_add(startup_timeout)
            .unwrap();
        let opening = tokio::spawn(open_external_with_connector(
            PathBuf::from("/fixture/runtime.sock"),
            startup_deadline,
            startup_timeout,
            Duration::from_secs(1),
            std::future::pending::<std::io::Result<tokio::net::UnixStream>>(),
        ));
        tokio::task::yield_now().await;

        tokio::time::advance(startup_timeout).await;
        let failure = match opening.await.unwrap() {
            Ok(_) => panic!("pending external connector unexpectedly opened"),
            Err(failure) => failure,
        };

        assert_eq!(
            failure,
            RuntimeError::StartupTimeout {
                mode: RuntimeMode::ExternalUds,
                timeout: startup_timeout,
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn runtime_source_and_initialization_share_one_absolute_startup_deadline() {
        let startup_timeout = Duration::from_millis(100);
        let source_time = Duration::from_millis(60);
        let ack_time = Duration::from_millis(110);
        let started_at = tokio::time::Instant::now();
        let startup_deadline = started_at.checked_add(startup_timeout).unwrap();
        let (read, writer, peer) = fake_transport();
        let connection = connection::open_io(read, writer, None, Duration::from_secs(1));
        let (request_seen, request_received) = oneshot::channel();
        let (ack_attempted, ack_observed) = oneshot::channel();
        let peer_task = tokio::spawn(async move {
            let (read, mut write) = tokio::io::split(peer);
            let mut lines = BufReader::new(read).lines();
            let request: Value = serde_json::from_str(
                &lines
                    .next_line()
                    .await
                    .unwrap()
                    .expect("initialize request after source acquisition"),
            )
            .unwrap();
            assert_eq!(request["method"], METHOD_INITIALIZE);
            let params: InitializeParams =
                serde_json::from_value(request["params"].clone()).unwrap();
            let result = InitializeResult::negotiate(
                &params,
                PeerInfo {
                    name: "cumulative-deadline-peer".into(),
                    version: "fixture".into(),
                },
            )
            .unwrap();
            request_seen.send(()).unwrap();

            tokio::time::sleep_until(started_at.checked_add(ack_time).unwrap()).await;
            ack_attempted.send(()).unwrap();
            let response = json!({
                "jsonrpc": "2.0",
                "id": request["id"],
                "result": result,
            });
            let _ = write.write_all(response.to_string().as_bytes()).await;
            let _ = write.write_all(b"\n").await;
            let _ = write.flush().await;
        });

        let options = RuntimeOptions::external_uds(
            "/fixture/runtime.sock",
            startup_timeout,
            Duration::from_secs(1),
        );
        let opening = tokio::spawn(WhaleRuntime::open_with_source_factory(
            options,
            move |source, context| async move {
                assert_eq!(source.mode(), RuntimeMode::ExternalUds);
                assert_eq!(context.startup_deadline, startup_deadline);
                tokio::time::sleep(source_time).await;
                Ok(connection)
            },
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(source_time).await;
        request_received.await.unwrap();

        tokio::time::advance(Duration::from_millis(39)).await;
        tokio::task::yield_now().await;
        assert!(!opening.is_finished(), "open timed out before its deadline");

        tokio::time::advance(Duration::from_millis(2)).await;
        for _ in 0..100 {
            if opening.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            opening.is_finished(),
            "initialization reset the startup budget after source acquisition"
        );
        let failure = opening.await.unwrap().unwrap_err();
        assert_eq!(
            failure,
            RuntimeError::StartupTimeout {
                mode: RuntimeMode::ExternalUds,
                timeout: startup_timeout,
            }
        );

        tokio::time::advance(Duration::from_millis(9)).await;
        ack_observed.await.unwrap();
        peer_task.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn managed_pipe_acquisition_failure_kills_and_reaps_before_returning() {
        use std::process::Stdio;

        let mut command = tokio::process::Command::new("/bin/sleep");
        command
            .arg("5")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .kill_on_drop(true);
        let child = command.spawn().unwrap();
        let pid = child.id().unwrap();

        let failure = match claim_managed_stdio(child).await {
            Ok(_) => panic!("missing stdout was accepted"),
            Err(failure) => failure,
        };
        assert!(matches!(
            failure,
            RuntimeError::SourceFailure {
                mode: RuntimeMode::ManagedProcess,
                ..
            }
        ));
        assert_eq!(unsafe { kill(pid as i32, 0) }, -1);
        assert_eq!(std::io::Error::last_os_error().raw_os_error(), Some(3));
    }

    #[cfg(unix)]
    #[tokio::test(start_paused = true)]
    async fn startup_deadline_does_not_abandon_a_started_pre_owner_child_reap() {
        use std::process::Stdio;

        let (pid_sent, pid_received) = oneshot::channel();
        let (release_reap, reap_released) = oneshot::channel();
        let options = test_options(Duration::from_millis(20), Duration::from_secs(1));
        let opening = tokio::spawn(WhaleRuntime::open_with_source_factory(
            options,
            move |_, _| async move {
                let mut command = tokio::process::Command::new("/bin/sleep");
                command.arg("5").stdout(Stdio::null()).kill_on_drop(true);
                let mut child = command.spawn().unwrap();
                let pid = child.id().unwrap();
                child.start_kill().unwrap();
                pid_sent.send(pid).unwrap();
                let _ = reap_released.await;
                child.wait().await.unwrap();
                Err(RuntimeError::SourceFailure {
                    mode: RuntimeMode::ManagedProcess,
                    message: "fixture source failure".into(),
                })
            },
        ));
        let pid = pid_received.await.unwrap();

        tokio::time::advance(Duration::from_millis(21)).await;
        tokio::task::yield_now().await;
        assert!(
            !opening.is_finished(),
            "startup deadline abandoned a started child reap"
        );

        release_reap.send(()).unwrap();
        let failure = opening.await.unwrap().unwrap_err();
        assert!(matches!(failure, RuntimeError::SourceFailure { .. }));
        assert_eq!(unsafe { kill(pid as i32, 0) }, -1);
        assert_eq!(std::io::Error::last_os_error().raw_os_error(), Some(3));
    }

    #[tokio::test]
    async fn invalid_deadlines_never_invoke_the_source_factory() {
        for (startup_timeout, shutdown_timeout, field) in [
            (Duration::ZERO, Duration::from_secs(1), "startup_timeout"),
            (Duration::from_secs(1), Duration::ZERO, "shutdown_timeout"),
            (
                Duration::from_secs(1),
                Duration::from_nanos(1),
                "shutdown_timeout",
            ),
        ] {
            let calls = Arc::new(AtomicUsize::new(0));
            let factory_calls = calls.clone();
            let result = WhaleRuntime::open_with_source_factory(
                test_options(startup_timeout, shutdown_timeout),
                move |_, _| async move {
                    factory_calls.fetch_add(1, Ordering::SeqCst);
                    Err(RuntimeError::SourceFailure {
                        mode: RuntimeMode::Embedded,
                        message: "invalid options reached the source factory".into(),
                    })
                },
            )
            .await;

            assert!(matches!(
                result,
                Err(RuntimeError::InvalidConfiguration {
                    field: actual,
                    ..
                }) if actual == field
            ));
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn invalid_managed_values_never_invoke_the_async_source_factory() {
        use std::os::unix::ffi::OsStringExt;

        let valid = || RuntimeOptions::managed("/fixture/peer");
        let cases = vec![
            (valid().with_environment("", "value"), "environment"),
            (valid().with_environment("BAD=NAME", "value"), "environment"),
            (
                valid().with_environment(OsString::from_vec(b"BAD\0NAME".to_vec()), "value"),
                "environment",
            ),
            (
                valid().with_environment("NAME", OsString::from_vec(b"BAD\0VALUE".to_vec())),
                "environment",
            ),
            (valid().with_arg("--listen"), "args"),
            (
                valid().with_arg(OsString::from_vec({
                    let mut argument = b"--listen=".to_vec();
                    argument.push(0x80);
                    argument
                })),
                "args",
            ),
            (
                valid().with_arg(OsString::from_vec(b"BAD\0ARG".to_vec())),
                "args",
            ),
            (
                RuntimeOptions::managed(PathBuf::from(OsString::from_vec(
                    b"/bad\0executable".to_vec(),
                ))),
                "executable",
            ),
        ];

        for (options, field) in cases {
            let calls = Arc::new(AtomicUsize::new(0));
            let factory_calls = calls.clone();
            let result = WhaleRuntime::open_with_source_factory(options, move |_, _| async move {
                factory_calls.fetch_add(1, Ordering::SeqCst);
                Err(RuntimeError::SourceFailure {
                    mode: RuntimeMode::ManagedProcess,
                    message: "invalid managed options reached source factory".into(),
                })
            })
            .await;

            assert!(matches!(
                result,
                Err(RuntimeError::InvalidConfiguration {
                    field: actual,
                    ..
                }) if actual == field
            ));
            assert_eq!(calls.load(Ordering::SeqCst), 0, "invalid {field}");
        }
    }

    #[tokio::test]
    async fn aborting_open_after_initialize_ingress_starts_owner_cleanup() {
        let shutdown_timeout = Duration::from_secs(1);
        let (read, writer, peer) = fake_transport();
        let (request_seen, request_received) = oneshot::channel();
        let (eof_seen, eof_received) = oneshot::channel();
        let peer_task = tokio::spawn(async move {
            let (read, _write) = tokio::io::split(peer);
            let mut lines = BufReader::new(read).lines();
            let request: Value = serde_json::from_str(
                &lines
                    .next_line()
                    .await
                    .unwrap()
                    .expect("initialize request"),
            )
            .unwrap();
            assert_eq!(request["method"], METHOD_INITIALIZE);
            request_seen.send(()).unwrap();
            assert!(lines.next_line().await.unwrap().is_none());
            eof_seen.send(()).unwrap();
            Ok(())
        });
        let connection =
            connection::open_io_with_owned_task(read, writer, peer_task, shutdown_timeout);
        let cleanup_observer = connection.client.clone();
        let opening = tokio::spawn(WhaleRuntime::open_with_source_factory(
            test_options(Duration::from_secs(5), shutdown_timeout),
            move |_, _| std::future::ready(Ok(connection)),
        ));
        request_received.await.unwrap();

        opening.abort();
        assert!(opening.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(2), eof_received)
            .await
            .expect("owner cleanup did not close the fake peer")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), cleanup_observer.close())
            .await
            .expect("owner cleanup did not publish completion");
    }

    #[tokio::test]
    async fn incompatible_initialize_rolls_back_before_open_returns() {
        let shutdown_timeout = Duration::from_secs(1);
        let (read, writer, peer) = fake_transport();
        let eof_seen = Arc::new(AtomicBool::new(false));
        let peer_eof_seen = eof_seen.clone();
        let peer_task = tokio::spawn(async move {
            let (read, mut write) = tokio::io::split(peer);
            let mut lines = BufReader::new(read).lines();
            let request: Value = serde_json::from_str(
                &lines
                    .next_line()
                    .await
                    .unwrap()
                    .expect("initialize request"),
            )
            .unwrap();
            assert_eq!(request["method"], METHOD_INITIALIZE);
            let response = json!({
                "jsonrpc": "2.0",
                "id": request["id"],
                "result": {
                    "server": { "name": "fake-peer", "version": "test" },
                    "protocol_version": 2,
                    "capabilities": []
                }
            });
            write
                .write_all(response.to_string().as_bytes())
                .await
                .unwrap();
            write.write_all(b"\n").await.unwrap();
            write.flush().await.unwrap();
            assert!(lines.next_line().await.unwrap().is_none());
            peer_eof_seen.store(true, Ordering::SeqCst);
            Ok(())
        });
        let connection =
            connection::open_io_with_owned_task(read, writer, peer_task, shutdown_timeout);

        let failure = WhaleRuntime::open_with_source_factory(
            test_options(Duration::from_secs(2), shutdown_timeout),
            move |_, _| std::future::ready(Ok(connection)),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            failure,
            RuntimeError::InitializationFailure {
                mode: RuntimeMode::Embedded,
                ..
            }
        ));
        assert!(
            eof_seen.load(Ordering::SeqCst),
            "open returned before failed-initialize cleanup reached peer EOF"
        );
    }

    #[test]
    fn successful_forced_cleanup_is_not_promoted_from_warning_text_to_an_error() {
        let mut outcome = ConnectionShutdown::clean(None);
        outcome.forced = true;
        outcome.error = Some("graceful embedded join timed out before successful abort".into());

        assert_eq!(
            map_shutdown(RuntimeMode::Embedded, Duration::from_secs(1), outcome),
            Ok(RuntimeShutdown {
                mode: RuntimeMode::Embedded,
                disposition: ShutdownDisposition::Forced,
            })
        );
    }

    #[test]
    fn typed_timeout_controls_the_public_variant_without_parsing_its_message() {
        let mut outcome = ConnectionShutdown::clean(None);
        outcome.forced = true;
        outcome.error = Some("opaque diagnostic".into());
        outcome.failure = Some(ConnectionFailure {
            phase: ConnectionShutdownPhase::Reader,
            kind: ConnectionFailureKind::TimedOut,
            message: "opaque diagnostic".into(),
        });

        assert_eq!(
            map_shutdown(RuntimeMode::Embedded, Duration::from_millis(250), outcome,),
            Err(RuntimeError::ShutdownTimeout {
                mode: RuntimeMode::Embedded,
                phase: RuntimeShutdownPhase::Reader,
                timeout: Duration::from_millis(250),
            })
        );
    }
}
