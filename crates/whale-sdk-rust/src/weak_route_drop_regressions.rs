use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub(crate) struct AfterUpgrade {
    upgraded: Barrier,
    resume: Barrier,
}

impl AfterUpgrade {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            upgraded: Barrier::new(2),
            resume: Barrier::new(2),
        })
    }

    pub(crate) fn pause_route(&self) {
        self.upgraded.wait();
        self.resume.wait();
    }

    pub(crate) fn drop_last_external_arc(&self, drop_arc: impl FnOnce()) {
        self.upgraded.wait();
        drop_arc();
        self.resume.wait();
    }
}

pub(crate) fn run_isolated(
    child_environment: &str,
    exact_test_name: &str,
    child_case: impl FnOnce(),
) {
    const COMPLETION_ENVIRONMENT: &str = "WHALE_SDK_WEAK_ROUTE_COMPLETION_FILE";

    if std::env::var_os(child_environment).is_some() {
        child_case();
        let completion = std::env::var_os(COMPLETION_ENVIRONMENT)
            .expect("isolated regression child has no completion path");
        std::fs::write(completion, b"completed")
            .expect("isolated regression child could not record completion");
        return;
    }

    let completion = unique_completion_path(child_environment);
    let mut child = Command::new(std::env::current_exe().expect("test executable is unavailable"))
        .args([
            "--exact",
            exact_test_name,
            "--nocapture",
            "--test-threads=1",
        ])
        .env(child_environment, "1")
        .env(COMPLETION_ENVIRONMENT, &completion)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("could not spawn isolated regression child");

    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(10)),
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = std::fs::remove_file(&completion);
                panic!(
                    "isolated regression child timed out; the route likely dropped its last Hub Arc while retaining the DashMap shard guard"
                );
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = std::fs::remove_file(&completion);
                panic!("could not wait for isolated regression child: {error}");
            }
        }
    };

    let completion_result = std::fs::read(&completion);
    let _ = std::fs::remove_file(&completion);
    assert!(
        status.success(),
        "isolated regression child failed: {status}"
    );
    assert_eq!(
        completion_result.ok().as_deref(),
        Some(b"completed".as_slice()),
        "isolated regression child did not execute the requested exact test"
    );
}

fn unique_completion_path(child_environment: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "whale-sdk-weak-route-{}-{nonce}-{child_environment}",
        std::process::id()
    ))
}
