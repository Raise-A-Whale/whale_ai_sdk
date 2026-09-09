use std::{ffi::OsString, path::PathBuf, sync::Arc, time::Duration};
use whale_sdk_rust::{
    AgentDefinition, DaemonServer, RuntimeError, RuntimeMode, RuntimeOptions, RuntimeSource,
    ShutdownDisposition, WhaleRuntime,
};

#[cfg(unix)]
mod support;

#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
#[cfg(unix)]
use support::process::{incompatible_initialize_response, managed_options, os_bytes, RuntimePeer};
#[cfg(unix)]
use support::uds::{ConnectionPlan, UdsEvent, UdsFixture};

#[tokio::test]
async fn embedded_default_open_is_ready_before_return_and_reports_its_peer() {
    let runtime = WhaleRuntime::open(RuntimeOptions::embedded())
        .await
        .unwrap();

    assert_eq!(runtime.info().mode, RuntimeMode::Embedded);
    assert_eq!(runtime.info().process_id, None);
    assert_eq!(
        runtime.info().peer,
        runtime.client().initialize().await.unwrap()
    );

    let stopped = runtime.shutdown().await.unwrap();
    assert_eq!(stopped.mode, RuntimeMode::Embedded);
    assert_eq!(stopped.disposition, ShutdownDisposition::Graceful);
}

#[tokio::test]
async fn embedded_runtime_composes_an_agent_and_session_without_transport_objects() {
    let server = DaemonServer::default_server();
    let retained = server.clone();
    let runtime = WhaleRuntime::open(RuntimeOptions {
        source: RuntimeSource::Embedded { server },
        startup_timeout: Duration::from_secs(2),
        shutdown_timeout: Duration::from_secs(2),
    })
    .await
    .unwrap();
    let agent = runtime
        .agent(
            AgentDefinition::new("application-agent", "fixture"),
            Vec::new(),
        )
        .unwrap();
    let session = agent.create_session().await.unwrap();
    assert!(retained.sessions().contains_key(session.id()));

    let first = runtime.shutdown().await.unwrap();
    let later = runtime.shutdown().await.unwrap();
    assert_eq!(first, later);
    assert_eq!(first.disposition, ShutdownDisposition::Graceful);
    assert!(!retained.sessions().contains_key(session.id()));
}

#[tokio::test]
async fn embedded_zero_deadlines_fail_before_the_server_is_used() {
    let startup_server = DaemonServer::default_server();
    let retained_startup = startup_server.clone();
    let startup = WhaleRuntime::open(RuntimeOptions {
        source: RuntimeSource::Embedded {
            server: startup_server,
        },
        startup_timeout: Duration::ZERO,
        shutdown_timeout: Duration::from_secs(1),
    })
    .await
    .unwrap_err();
    assert!(matches!(
        startup,
        RuntimeError::InvalidConfiguration {
            field: "startup_timeout",
            ..
        }
    ));
    assert!(retained_startup.sessions().is_empty());

    let shutdown_server = DaemonServer::default_server();
    let retained_shutdown = shutdown_server.clone();
    let shutdown = WhaleRuntime::open(RuntimeOptions {
        source: RuntimeSource::Embedded {
            server: shutdown_server,
        },
        startup_timeout: Duration::from_secs(1),
        shutdown_timeout: Duration::ZERO,
    })
    .await
    .unwrap_err();
    assert!(matches!(
        shutdown,
        RuntimeError::InvalidConfiguration {
            field: "shutdown_timeout",
            ..
        }
    ));
    assert!(retained_shutdown.sessions().is_empty());
}

#[tokio::test]
async fn embedded_shutdown_budget_must_have_graceful_and_forced_windows() {
    let failure = WhaleRuntime::open(RuntimeOptions {
        source: RuntimeSource::Embedded {
            server: DaemonServer::default_server(),
        },
        startup_timeout: Duration::from_secs(1),
        shutdown_timeout: Duration::from_nanos(1),
    })
    .await
    .unwrap_err();

    assert!(matches!(
        failure,
        RuntimeError::InvalidConfiguration {
            field: "shutdown_timeout",
            ..
        }
    ));
}

#[tokio::test]
async fn embedded_runtime_drop_stops_its_owner_despite_a_retained_client_clone() {
    let server = DaemonServer::default_server();
    let retained = server.clone();
    let runtime = WhaleRuntime::open(RuntimeOptions {
        source: RuntimeSource::Embedded { server },
        startup_timeout: Duration::from_secs(2),
        shutdown_timeout: Duration::from_secs(2),
    })
    .await
    .unwrap();
    let client = runtime.client().clone();
    let session = client.create_thread("fixture", None).await.unwrap();
    let session_id = session.id().to_owned();

    drop(runtime);

    tokio::time::timeout(Duration::from_secs(2), async {
        while retained.sessions().contains_key(&session_id) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping the Runtime did not finish its connection cleanup");
    assert!(client.create_thread("closed", None).await.is_err());
}

#[tokio::test]
async fn embedded_cloned_runtimes_keep_sessions_isolated_and_cleanup_before_return() {
    let server = DaemonServer::default_server();
    let retained = server.clone();
    let options = |server| RuntimeOptions {
        source: RuntimeSource::Embedded { server },
        startup_timeout: Duration::from_secs(2),
        shutdown_timeout: Duration::from_secs(2),
    };
    let runtime_a = WhaleRuntime::open(options(server.clone())).await.unwrap();
    let runtime_b = WhaleRuntime::open(options(server)).await.unwrap();
    let session_a = runtime_a
        .client()
        .create_thread("embedded-a", None)
        .await
        .unwrap();
    let session_b = runtime_b
        .client()
        .create_thread("embedded-b", None)
        .await
        .unwrap();
    let session_a_id = session_a.id().to_owned();
    let session_b_id = session_b.id().to_owned();

    runtime_a.shutdown().await.unwrap();
    assert!(!retained.sessions().contains_key(&session_a_id));
    assert!(retained.sessions().contains_key(&session_b_id));

    let session_b_after_a = runtime_b
        .client()
        .create_thread("embedded-b-after-a", None)
        .await
        .unwrap();
    let session_b_after_a_id = session_b_after_a.id().to_owned();
    assert!(retained.sessions().contains_key(&session_b_after_a_id));

    runtime_b.shutdown().await.unwrap();
    assert!(!retained.sessions().contains_key(&session_b_id));
    assert!(!retained.sessions().contains_key(&session_b_after_a_id));
}

#[test]
fn runtime_error_is_cloneable_and_comparable_and_debug_redacts_environment_values() {
    fn assert_clone_eq<T: Clone + Eq>() {}
    assert_clone_eq::<RuntimeError>();

    let source = RuntimeSource::ManagedProcess {
        executable: PathBuf::from("/example/daemon"),
        args: vec![OsString::from("--safe-argument")],
        environment: vec![(
            OsString::from("WHALE_TOKEN"),
            OsString::from("secret-value"),
        )],
    };
    let debug = format!("{source:?}");
    assert!(debug.contains("WHALE_TOKEN"));
    assert!(!debug.contains("secret-value"));
    assert!(!format!("{:?}", RuntimeOptions::embedded()).contains("DaemonServer"));

    let external = RuntimeSource::ExternalUds {
        path: PathBuf::from("/tmp/whale.sock"),
    };
    let external_debug = format!("{external:?}");
    assert!(external_debug.contains("ExternalUds"));
    assert!(external_debug.contains("/tmp/whale.sock"));
}

#[cfg(unix)]
#[tokio::test]
async fn external_open_waits_for_real_initialize_ack_and_reports_peer() {
    let fixture = UdsFixture::new(vec![ConnectionPlan::current().hold_initialize()]);
    let opening = tokio::spawn(WhaleRuntime::open(
        fixture.options(Duration::from_secs(2), Duration::from_secs(1)),
    ));

    fixture
        .wait_for_event(0, UdsEvent::InitializeReceived)
        .await;
    assert!(
        !opening.is_finished(),
        "open returned before initialize ACK"
    );
    fixture.release_initialize(0);

    let runtime = opening.await.unwrap().unwrap();
    assert_eq!(runtime.info().mode, RuntimeMode::ExternalUds);
    assert_eq!(runtime.info().process_id, None);
    assert_eq!(runtime.info().peer.server.name, "uds-peer-0");
    runtime.shutdown().await.unwrap();
    fixture.wait_for_event(0, UdsEvent::ClientEof).await;
    fixture.wait_for_event(0, UdsEvent::ConnectionEnded).await;
    assert_eq!(fixture.event_count(0, UdsEvent::InitializeAcked), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn external_immediate_connect_failure_is_source_failure() {
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("missing.sock");
    let failure = WhaleRuntime::open(RuntimeOptions::external_uds(
        missing,
        Duration::from_secs(1),
        Duration::from_secs(1),
    ))
    .await
    .unwrap_err();

    assert!(matches!(
        failure,
        RuntimeError::SourceFailure {
            mode: RuntimeMode::ExternalUds,
            ..
        }
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn external_failed_timed_out_and_aborted_open_close_only_their_socket() {
    let fixture = UdsFixture::new(vec![
        ConnectionPlan::incompatible(),
        ConnectionPlan::no_reply(),
        ConnectionPlan::current().hold_initialize(),
        ConnectionPlan::current(),
    ]);

    let incompatible =
        WhaleRuntime::open(fixture.options(Duration::from_secs(1), Duration::from_secs(1)))
            .await
            .unwrap_err();
    assert!(matches!(
        incompatible,
        RuntimeError::InitializationFailure {
            mode: RuntimeMode::ExternalUds,
            ..
        }
    ));
    fixture.wait_for_event(0, UdsEvent::ClientEof).await;
    fixture.wait_for_event(0, UdsEvent::ConnectionEnded).await;

    let timed_out =
        WhaleRuntime::open(fixture.options(Duration::from_millis(100), Duration::from_secs(1)))
            .await
            .unwrap_err();
    assert!(matches!(
        timed_out,
        RuntimeError::StartupTimeout {
            mode: RuntimeMode::ExternalUds,
            timeout,
        } if timeout == Duration::from_millis(100)
    ));
    fixture.wait_for_event(1, UdsEvent::ClientEof).await;
    fixture.wait_for_event(1, UdsEvent::ConnectionEnded).await;

    let aborted = tokio::spawn(WhaleRuntime::open(
        fixture.options(Duration::from_secs(2), Duration::from_secs(1)),
    ));
    fixture
        .wait_for_event(2, UdsEvent::InitializeReceived)
        .await;
    assert!(!aborted.is_finished());
    aborted.abort();
    assert!(aborted.await.unwrap_err().is_cancelled());
    fixture.wait_for_event(2, UdsEvent::ClientEof).await;
    fixture.wait_for_event(2, UdsEvent::ConnectionEnded).await;

    let runtime =
        WhaleRuntime::open(fixture.options(Duration::from_secs(1), Duration::from_secs(1)))
            .await
            .unwrap();
    let thread = runtime
        .client()
        .create_thread("after-three-failed-opens", None)
        .await
        .unwrap();
    assert_eq!(thread.id(), "uds-3-session-1");
    fixture
        .wait_for_event(
            3,
            UdsEvent::BusinessRequest {
                model: "after-three-failed-opens".into(),
            },
        )
        .await;
    runtime.shutdown().await.unwrap();
    fixture.wait_for_event(3, UdsEvent::ClientEof).await;
    fixture.wait_for_event(3, UdsEvent::ConnectionEnded).await;
}

#[cfg(unix)]
#[tokio::test]
async fn external_connections_are_isolated_and_listener_remains_usable() {
    let fixture = UdsFixture::new(vec![
        ConnectionPlan::current(),
        ConnectionPlan::current(),
        ConnectionPlan::current(),
    ]);
    let runtime_a =
        WhaleRuntime::open(fixture.options(Duration::from_secs(1), Duration::from_secs(1)))
            .await
            .unwrap();
    let runtime_b =
        WhaleRuntime::open(fixture.options(Duration::from_secs(1), Duration::from_secs(1)))
            .await
            .unwrap();

    runtime_a.shutdown().await.unwrap();
    fixture.wait_for_event(0, UdsEvent::ClientEof).await;
    fixture.wait_for_event(0, UdsEvent::ConnectionEnded).await;
    assert_eq!(fixture.event_count(1, UdsEvent::ClientEof), 0);

    let thread_b = runtime_b
        .client()
        .create_thread("external-b-after-a", None)
        .await
        .unwrap();
    assert_eq!(thread_b.id(), "uds-1-session-1");
    fixture
        .wait_for_event(
            1,
            UdsEvent::BusinessRequest {
                model: "external-b-after-a".into(),
            },
        )
        .await;

    let runtime_c =
        WhaleRuntime::open(fixture.options(Duration::from_secs(1), Duration::from_secs(1)))
            .await
            .unwrap();
    let thread_c = runtime_c
        .client()
        .create_thread("external-c", None)
        .await
        .unwrap();
    assert_eq!(thread_c.id(), "uds-2-session-1");
    fixture
        .wait_for_event(
            2,
            UdsEvent::BusinessRequest {
                model: "external-c".into(),
            },
        )
        .await;

    runtime_b.shutdown().await.unwrap();
    runtime_c.shutdown().await.unwrap();
    fixture.wait_for_event(1, UdsEvent::ClientEof).await;
    fixture.wait_for_event(2, UdsEvent::ClientEof).await;
    fixture.wait_for_event(1, UdsEvent::ConnectionEnded).await;
    fixture.wait_for_event(2, UdsEvent::ConnectionEnded).await;
    assert_eq!(fixture.event_count(0, UdsEvent::ClientEof), 1);
    assert_eq!(fixture.event_count(1, UdsEvent::ClientEof), 1);
    assert_eq!(fixture.event_count(2, UdsEvent::ClientEof), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn external_shutdown_is_single_flight_for_concurrent_and_later_callers() {
    let fixture = UdsFixture::new(vec![ConnectionPlan::current()]);
    let runtime =
        WhaleRuntime::open(fixture.options(Duration::from_secs(1), Duration::from_secs(1)))
            .await
            .unwrap();

    let (first, second) = tokio::join!(runtime.shutdown(), runtime.shutdown());
    let first = first.unwrap();
    let second = second.unwrap();
    let later = runtime.shutdown().await.unwrap();
    assert_eq!(first, second);
    assert_eq!(first, later);
    assert_eq!(first.disposition, ShutdownDisposition::Graceful);
    fixture.wait_for_event(0, UdsEvent::ClientEof).await;
    fixture.wait_for_event(0, UdsEvent::ConnectionEnded).await;
    assert_eq!(fixture.event_count(0, UdsEvent::ClientEof), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn external_cancelled_shutdown_waiter_does_not_cancel_cleanup() {
    let fixture = UdsFixture::new(vec![ConnectionPlan::current()]);
    let runtime = Arc::new(
        WhaleRuntime::open(fixture.options(Duration::from_secs(1), Duration::from_secs(1)))
            .await
            .unwrap(),
    );
    let first_runtime = runtime.clone();
    let (first_polled_pending, started) = tokio::sync::oneshot::channel();
    let first = tokio::spawn(async move {
        let mut shutdown = Box::pin(async move { first_runtime.shutdown().await });
        let mut first_polled_pending = Some(first_polled_pending);
        let mut stay_pending = false;
        std::future::poll_fn(move |context| {
            if stay_pending {
                return std::task::Poll::Pending;
            }
            match std::future::Future::poll(shutdown.as_mut(), context) {
                std::task::Poll::Ready(result) => std::task::Poll::Ready(result),
                std::task::Poll::Pending => {
                    stay_pending = true;
                    first_polled_pending
                        .take()
                        .expect("first-poll signal is sent once")
                        .send(())
                        .expect("shutdown test still observes first poll");
                    std::task::Poll::Pending
                }
            }
        })
        .await
    });
    started.await.unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());

    let second = runtime.shutdown().await.unwrap();
    let later = runtime.shutdown().await.unwrap();
    assert_eq!(second, later);
    fixture.wait_for_event(0, UdsEvent::ClientEof).await;
    fixture.wait_for_event(0, UdsEvent::ConnectionEnded).await;
    assert_eq!(fixture.event_count(0, UdsEvent::ClientEof), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn external_shutdown_immediately_invalidates_retained_client_clones() {
    let fixture = UdsFixture::new(vec![ConnectionPlan::current()]);
    let runtime =
        WhaleRuntime::open(fixture.options(Duration::from_secs(1), Duration::from_secs(1)))
            .await
            .unwrap();
    let retained = runtime.client().clone();
    let mut shutdown = Box::pin(runtime.shutdown());
    let first_poll = std::future::poll_fn(|context| {
        std::task::Poll::Ready(std::future::Future::poll(shutdown.as_mut(), context))
    })
    .await;
    assert!(first_poll.is_pending());

    assert!(retained
        .create_thread("must-not-reach-peer", None)
        .await
        .is_err());
    assert_eq!(
        fixture.event_count(
            0,
            UdsEvent::BusinessRequest {
                model: "must-not-reach-peer".into(),
            }
        ),
        0
    );
    fixture.wait_for_event(0, UdsEvent::ClientEof).await;
    shutdown.await.unwrap();
    fixture.wait_for_event(0, UdsEvent::ConnectionEnded).await;
    assert_eq!(
        fixture.event_count(
            0,
            UdsEvent::BusinessRequest {
                model: "must-not-reach-peer".into(),
            }
        ),
        0,
        "retained client emitted a late frame after shutdown completion"
    );
}

#[cfg(unix)]
#[test]
fn external_runtime_drop_outside_entered_tokio_context_is_safe() {
    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (fixture, runtime, retained) = tokio_runtime.block_on(async {
        let fixture = UdsFixture::new(vec![ConnectionPlan::current()]);
        let runtime =
            WhaleRuntime::open(fixture.options(Duration::from_secs(1), Duration::from_secs(1)))
                .await
                .unwrap();
        let retained = runtime.client().clone();
        (fixture, runtime, retained)
    });

    drop(runtime);

    tokio_runtime.block_on(async {
        fixture.wait_for_event(0, UdsEvent::ClientEof).await;
        fixture.wait_for_event(0, UdsEvent::ConnectionEnded).await;
        assert!(retained
            .create_thread("closed-after-drop", None)
            .await
            .is_err());
    });
    drop(fixture);
    drop(tokio_runtime);
}

#[cfg(unix)]
#[tokio::test]
async fn managed_open_waits_for_initialize_ack_and_reports_the_live_child_pid() {
    let peer = RuntimePeer::new();
    let options = peer.options(
        "ready_after_release",
        Duration::from_secs(2),
        Duration::from_secs(1),
    );
    let opening = tokio::spawn(WhaleRuntime::open(options));

    peer.wait_for_event("initialize_received").await;
    let pid = peer.pid();
    peer.assert_alive(pid);
    assert!(
        !opening.is_finished(),
        "open returned before initialize ACK"
    );

    peer.release_initialize().await.unwrap();
    let runtime = opening.await.unwrap().unwrap();
    assert_eq!(runtime.info().mode, RuntimeMode::ManagedProcess);
    assert_eq!(runtime.info().process_id, Some(pid));
    peer.assert_alive(pid);

    runtime.shutdown().await.unwrap();
    peer.assert_reaped_now(pid);
}

#[cfg(unix)]
#[test]
fn managed_constructor_uses_positive_default_deadlines() {
    let options = RuntimeOptions::managed(support::process::fixture_program());
    assert!(options.startup_timeout > Duration::ZERO);
    assert!(options.shutdown_timeout > Duration::ZERO);
}

#[cfg(unix)]
#[tokio::test]
async fn managed_arguments_preserve_spaces_metacharacters_and_non_utf8_bytes() {
    let peer = RuntimePeer::new();
    let spaced = OsString::from("one value;$(printf shell-must-not-run)");
    let non_utf8 = OsString::from_vec(vec![b'f', 0x80, b'o']);
    let options = peer
        .options("current", Duration::from_secs(2), Duration::from_secs(1))
        .with_arg("--")
        .with_arg(spaced.clone())
        .with_arg(non_utf8.clone());

    let runtime = WhaleRuntime::open(options).await.unwrap();
    let arguments = peer.arguments();
    assert!(arguments.contains(&os_bytes(&spaced)));
    assert!(arguments.contains(&os_bytes(&non_utf8)));
    let listen_positions: Vec<_> = arguments
        .windows(2)
        .enumerate()
        .filter_map(|(index, pair)| {
            (pair[0] == b"--listen" && pair[1] == b"stdio").then_some(index)
        })
        .collect();
    assert_eq!(listen_positions.len(), 1, "argv: {arguments:?}");
    assert_eq!(&arguments[..2], [b"--listen".to_vec(), b"stdio".to_vec()]);
    assert!(!peer.path().join("shell-must-not-run").exists());

    let pid = peer.pid();
    runtime.shutdown().await.unwrap();
    peer.assert_reaped_now(pid);
}

#[cfg(unix)]
#[tokio::test]
async fn managed_environment_is_child_only_and_debug_redacts_values() {
    const NAME: &str = "WHALE_RUNTIME_TEST_VALUE";
    const SECRET: &str = "managed-secret-value";
    let parent_before = std::env::var_os(NAME);
    let peer = RuntimePeer::new();
    let options = peer
        .options("current", Duration::from_secs(2), Duration::from_secs(1))
        .with_environment(NAME, SECRET);
    let debug = format!("{options:?}");
    assert!(debug.contains(NAME));
    assert!(!debug.contains(SECRET));

    let runtime = WhaleRuntime::open(options).await.unwrap();
    assert_eq!(peer.read("environment-present"), "present\n");
    assert_eq!(peer.read("environment-value"), SECRET);
    assert!(!peer.read("inherited-path").is_empty());
    assert_eq!(std::env::var_os(NAME), parent_before);
    assert!(!format!("{:?}", runtime.info()).contains(SECRET));
    assert!(!format!("{runtime:?}").contains(SECRET));

    let pid = peer.pid();
    runtime.shutdown().await.unwrap();
    peer.assert_reaped_now(pid);
}

#[cfg(unix)]
#[tokio::test]
async fn managed_source_and_validation_errors_redact_environment_values() {
    const SECRET: &str = "source-error-secret-value";
    let source_failure = WhaleRuntime::open(
        RuntimeOptions::managed("/a/missing/whale-runtime-peer")
            .with_environment("WHALE_RUNTIME_SECRET", SECRET),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        &source_failure,
        RuntimeError::SourceFailure { .. }
    ));
    assert!(!source_failure.to_string().contains(SECRET));
    assert!(!format!("{source_failure:?}").contains(SECRET));

    let invalid_value = OsString::from_vec(b"source-error-secret-value\0suffix".to_vec());
    let validation_failure = WhaleRuntime::open(
        RuntimeOptions::managed(support::process::fixture_program())
            .with_environment("WHALE_RUNTIME_SECRET", invalid_value),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        &validation_failure,
        RuntimeError::InvalidConfiguration {
            field: "environment",
            ..
        }
    ));
    assert!(!validation_failure.to_string().contains(SECRET));
    assert!(!format!("{validation_failure:?}").contains(SECRET));
}

#[cfg(unix)]
#[tokio::test]
async fn managed_invalid_configuration_never_spawns_a_child() {
    let cases = vec![
        (
            "startup zero",
            managed_options(Duration::ZERO, Duration::from_secs(1)),
            "startup_timeout",
        ),
        (
            "startup unrepresentable",
            managed_options(Duration::MAX, Duration::from_secs(1)),
            "startup_timeout",
        ),
        (
            "shutdown zero",
            managed_options(Duration::from_secs(1), Duration::ZERO),
            "shutdown_timeout",
        ),
        (
            "shutdown lacks force window",
            managed_options(Duration::from_secs(1), Duration::from_nanos(1)),
            "shutdown_timeout",
        ),
        (
            "shutdown unrepresentable",
            managed_options(Duration::from_secs(1), Duration::MAX),
            "shutdown_timeout",
        ),
        (
            "empty environment name",
            managed_options(Duration::from_secs(1), Duration::from_secs(1))
                .with_environment("", "value"),
            "environment",
        ),
        (
            "equals environment name",
            managed_options(Duration::from_secs(1), Duration::from_secs(1))
                .with_environment("BAD=NAME", "value"),
            "environment",
        ),
        (
            "NUL environment name",
            managed_options(Duration::from_secs(1), Duration::from_secs(1))
                .with_environment(OsString::from_vec(b"BAD\0NAME".to_vec()), "value"),
            "environment",
        ),
        (
            "NUL environment value",
            managed_options(Duration::from_secs(1), Duration::from_secs(1))
                .with_environment("NAME", OsString::from_vec(b"BAD\0VALUE".to_vec())),
            "environment",
        ),
        (
            "owned listen",
            managed_options(Duration::from_secs(1), Duration::from_secs(1)).with_arg("--listen"),
            "args",
        ),
        (
            "owned listen equals",
            managed_options(Duration::from_secs(1), Duration::from_secs(1))
                .with_arg("--listen=unix:///tmp/not-owned"),
            "args",
        ),
        (
            "owned listen non-UTF8 suffix",
            managed_options(Duration::from_secs(1), Duration::from_secs(1)).with_arg(
                OsString::from_vec({
                    let mut bytes = b"--listen=".to_vec();
                    bytes.push(0x80);
                    bytes
                }),
            ),
            "args",
        ),
        (
            "NUL argument",
            managed_options(Duration::from_secs(1), Duration::from_secs(1))
                .with_arg(OsString::from_vec(b"BAD\0ARG".to_vec())),
            "args",
        ),
        (
            "NUL executable",
            RuntimeOptions::managed(PathBuf::from(OsString::from_vec(
                b"/tmp/bad\0executable".to_vec(),
            ))),
            "executable",
        ),
    ];

    for (label, options, field) in cases {
        let peer = RuntimePeer::new();
        let options = options
            .with_arg("--fixture-dir")
            .with_arg(peer.path().as_os_str())
            .with_arg("--fixture-mode")
            .with_arg("current")
            .with_arg("--fixture-response")
            .with_arg(support::process::current_initialize_response());
        let failure = WhaleRuntime::open(options).await.unwrap_err();
        assert!(
            matches!(failure, RuntimeError::InvalidConfiguration { field: actual, .. } if actual == field),
            "{label}: {failure:?}"
        );
        assert!(!peer.exists("pid"), "{label} spawned a child");
        assert!(!peer.exists("events"), "{label} reached fixture code");
    }
}

#[cfg(unix)]
#[tokio::test]
async fn managed_startup_timeout_reaps_before_returning() {
    let peer = RuntimePeer::new();
    let failure = WhaleRuntime::open(peer.options(
        "no_reply",
        Duration::from_millis(120),
        Duration::from_secs(1),
    ))
    .await
    .unwrap_err();
    assert!(matches!(failure, RuntimeError::StartupTimeout { .. }));
    let pid = peer.pid();
    peer.assert_reaped_now(pid);
    assert_eq!(peer.event_count("initialize_received"), 1);
    assert_eq!(peer.event_count("business_input"), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn managed_incompatible_handshake_reaps_before_returning() {
    let peer = RuntimePeer::new();
    let failure = WhaleRuntime::open(peer.options_with_response(
        "incompatible",
        Duration::from_secs(1),
        Duration::from_secs(1),
        incompatible_initialize_response(),
    ))
    .await
    .unwrap_err();
    assert!(matches!(
        failure,
        RuntimeError::InitializationFailure { .. }
    ));
    let pid = peer.pid();
    peer.assert_reaped_now(pid);
    assert_eq!(peer.event_count("stdin_eof"), 1);
    assert_eq!(peer.event_count("business_input"), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn managed_early_eof_reaps_before_returning() {
    let peer = RuntimePeer::new();
    let failure =
        WhaleRuntime::open(peer.options("eof", Duration::from_secs(1), Duration::from_secs(1)))
            .await
            .unwrap_err();
    assert!(matches!(
        failure,
        RuntimeError::InitializationFailure { .. }
    ));
    let pid = peer.pid();
    peer.assert_reaped_now(pid);
    assert_eq!(peer.event_count("business_input"), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn managed_aborted_open_reaps_its_child() {
    let peer = RuntimePeer::new();
    let opening = tokio::spawn(WhaleRuntime::open(peer.options(
        "ready_after_release",
        Duration::from_secs(5),
        Duration::from_millis(300),
    )));
    peer.wait_for_event("initialize_received").await;
    let pid = peer.pid();
    peer.assert_alive(pid);

    opening.abort();
    assert!(opening.await.unwrap_err().is_cancelled());
    peer.assert_reaped(pid).await;
}

#[cfg(unix)]
#[tokio::test]
async fn managed_release_helper_returns_if_the_peer_exits_before_release() {
    let peer = RuntimePeer::new();
    let opening = tokio::spawn(WhaleRuntime::open(peer.options(
        "exit_before_release",
        Duration::from_secs(1),
        Duration::from_secs(1),
    )));
    peer.wait_for_event("normal_exit").await;
    let pid = peer.pid();

    let release = tokio::time::timeout(Duration::from_millis(100), peer.release_initialize())
        .await
        .expect("release helper blocked after the peer exited");
    assert!(
        release.is_err(),
        "release unexpectedly succeeded after exit"
    );

    assert!(matches!(
        opening.await.unwrap().unwrap_err(),
        RuntimeError::InitializationFailure { .. }
    ));
    peer.assert_reaped_now(pid);
}

#[cfg(unix)]
#[tokio::test]
async fn managed_graceful_shutdown_closes_stdin_once_and_reaps() {
    let peer = RuntimePeer::new();
    let runtime =
        WhaleRuntime::open(peer.options("current", Duration::from_secs(1), Duration::from_secs(1)))
            .await
            .unwrap();
    let pid = peer.pid();
    peer.assert_alive(pid);

    let stopped = runtime.shutdown().await.unwrap();
    assert_eq!(stopped.disposition, ShutdownDisposition::Graceful);
    assert_eq!(peer.event_count("stdin_eof"), 1);
    assert_eq!(peer.event_count("normal_exit"), 1);
    peer.assert_reaped_now(pid);
}

#[cfg(unix)]
#[tokio::test]
async fn managed_forced_shutdown_kills_and_reaps_within_the_total_budget() {
    let peer = RuntimePeer::new();
    let shutdown_timeout = Duration::from_millis(300);
    let runtime = WhaleRuntime::open(peer.options(
        "linger_after_eof",
        Duration::from_secs(1),
        shutdown_timeout,
    ))
    .await
    .unwrap();
    let pid = peer.pid();
    let started = tokio::time::Instant::now();

    let shutdown_bound = shutdown_timeout + Duration::from_millis(50);
    let stopped = tokio::time::timeout(shutdown_bound, runtime.shutdown())
        .await
        .expect("shutdown exceeded its public bound")
        .unwrap();
    assert_eq!(stopped.disposition, ShutdownDisposition::Forced);
    assert!(started.elapsed() <= shutdown_bound);
    assert_eq!(peer.event_count("stdin_eof"), 1);
    assert_eq!(peer.event_count("linger_after_eof"), 1);
    peer.assert_reaped_now(pid);
}

#[cfg(unix)]
#[tokio::test]
async fn managed_shutdown_is_single_flight_for_concurrent_and_later_callers() {
    let peer = RuntimePeer::new();
    let runtime = WhaleRuntime::open(peer.options(
        "linger_after_eof",
        Duration::from_secs(1),
        Duration::from_millis(300),
    ))
    .await
    .unwrap();
    let pid = peer.pid();

    let (first, second) = tokio::join!(runtime.shutdown(), runtime.shutdown());
    let first = first.unwrap();
    let second = second.unwrap();
    let later = runtime.shutdown().await.unwrap();
    assert_eq!(first, second);
    assert_eq!(first, later);
    assert_eq!(first.disposition, ShutdownDisposition::Forced);
    assert_eq!(peer.event_count("stdin_eof"), 1);
    assert_eq!(peer.event_count("linger_after_eof"), 1);
    peer.assert_reaped_now(pid);
}
