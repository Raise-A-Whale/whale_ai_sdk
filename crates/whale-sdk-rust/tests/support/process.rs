#![cfg(unix)]

use serde_json::json;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::TempDir;
use whale_sdk_rust::{InitializeParams, InitializeResult, PeerInfo, RuntimeOptions};

const WAIT_BOUND: Duration = Duration::from_secs(3);
const ESRCH: i32 = 3;

unsafe extern "C" {
    fn kill(pid: i32, signal: i32) -> i32;
}

pub struct RuntimePeer {
    directory: TempDir,
}

impl RuntimePeer {
    pub fn new() -> Self {
        Self {
            directory: tempfile::tempdir().expect("runtime fixture directory"),
        }
    }

    pub fn path(&self) -> &Path {
        self.directory.path()
    }

    pub fn options(
        &self,
        mode: &str,
        startup_timeout: Duration,
        shutdown_timeout: Duration,
    ) -> RuntimeOptions {
        self.options_with_response(
            mode,
            startup_timeout,
            shutdown_timeout,
            current_initialize_response(),
        )
    }

    pub fn options_with_response(
        &self,
        mode: &str,
        startup_timeout: Duration,
        shutdown_timeout: Duration,
        response: OsString,
    ) -> RuntimeOptions {
        managed_options(startup_timeout, shutdown_timeout)
            .with_arg("--fixture-dir")
            .with_arg(self.path().as_os_str())
            .with_arg("--fixture-mode")
            .with_arg(mode)
            .with_arg("--fixture-response")
            .with_arg(response)
    }

    pub fn exists(&self, name: &str) -> bool {
        self.path().join(name).exists()
    }

    pub fn read(&self, name: &str) -> String {
        fs::read_to_string(self.path().join(name)).expect("fixture evidence")
    }

    pub fn pid(&self) -> u32 {
        self.read("pid").trim().parse().expect("fixture PID")
    }

    pub fn event_count(&self, event: &str) -> usize {
        fs::read_to_string(self.path().join("events"))
            .unwrap_or_default()
            .lines()
            .filter(|observed| *observed == event)
            .count()
    }

    pub fn arguments(&self) -> Vec<Vec<u8>> {
        let count: usize = self.read("argc").trim().parse().expect("fixture argc");
        (1..=count)
            .map(|index| decode_hex(self.read(&format!("argv.{index}.hex")).trim()))
            .collect()
    }

    pub async fn wait_for_event(&self, event: &str) {
        self.wait_until(|| self.event_count(event) > 0, event).await;
    }

    pub async fn release_initialize(&self) -> Result<(), String> {
        self.wait_for_event("release_ready").await;
        if self.event_count("normal_exit") > 0 {
            return Err("runtime peer exited before initialize release".into());
        }
        let path = self.path().join("release");
        fs::write(path, b"continue\n")
            .map_err(|error| format!("failed to release initialize gate: {error}"))?;
        if self.event_count("normal_exit") > 0 && self.event_count("released") == 0 {
            return Err("runtime peer exited before consuming initialize release".into());
        }
        Ok(())
    }

    pub fn assert_alive(&self, pid: u32) {
        assert_eq!(
            probe_process(pid),
            Ok(()),
            "managed peer {pid} is not alive"
        );
    }

    pub fn assert_reaped_now(&self, pid: u32) {
        assert_eq!(
            probe_process(pid),
            Err(ESRCH),
            "managed peer {pid} still exists or is a zombie at return"
        );
    }

    pub async fn assert_reaped(&self, pid: u32) {
        self.wait_until(|| probe_process(pid) == Err(ESRCH), "child reap")
            .await;
        assert_eq!(
            probe_process(pid),
            Err(ESRCH),
            "managed peer {pid} still exists or is a zombie"
        );
    }

    async fn wait_until(&self, predicate: impl Fn() -> bool, label: &str) {
        tokio::time::timeout(WAIT_BOUND, async {
            while !predicate() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for fixture evidence: {label}"));
    }
}

pub fn fixture_program() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/runtime_peer.sh")
}

pub fn managed_options(startup_timeout: Duration, shutdown_timeout: Duration) -> RuntimeOptions {
    let mut options = RuntimeOptions::managed(fixture_program());
    options.startup_timeout = startup_timeout;
    options.shutdown_timeout = shutdown_timeout;
    options
}

pub fn current_initialize_response() -> OsString {
    let params = InitializeParams::sdk("whale-rust", env!("CARGO_PKG_VERSION"));
    let result = InitializeResult::negotiate(
        &params,
        PeerInfo {
            name: "runtime-peer".into(),
            version: "fixture".into(),
        },
    )
    .expect("current fixture initialize result");
    OsString::from(
        json!({
            "jsonrpc": "2.0",
            "id": "__WHALE_REQUEST_ID__",
            "result": result,
        })
        .to_string(),
    )
}

pub fn incompatible_initialize_response() -> OsString {
    let mut response: serde_json::Value =
        serde_json::from_str(current_initialize_response().to_str().unwrap()).unwrap();
    response["result"]["protocol_version"] = json!(u32::MAX);
    OsString::from(response.to_string())
}

pub fn os_bytes(value: &OsStr) -> Vec<u8> {
    value.as_bytes().to_vec()
}

fn probe_process(pid: u32) -> Result<(), i32> {
    let result = unsafe { kill(pid as i32, 0) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().raw_os_error().unwrap_or(-1))
    }
}

fn decode_hex(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0, "odd fixture hex length");
    (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).expect("fixture hex byte"))
        .collect()
}
