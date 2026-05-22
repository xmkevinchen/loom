//! Integration tests for `ClaudeCodeAdapter` per plan F-001 AC3.
//!
//! Test cases:
//! 1. `/bin/echo hello` → captures stdout, verdict Pass, exit 0.
//! 2. `/bin/sh -c "exec /bin/sleep 60"` with 2s timeout → timeout fires AND
//!    the `sleep` grandchild is reaped (not orphaned to PID 1). This is the
//!    plan's MF1 concern; the test must verify the actual grandchild PID is
//!    gone, not just the immediate `sh` child.
//! 3. Cancellation: spawn `sleep 30`, then trigger `CancellationToken`; expect
//!    a quick return with verdict Cancelled.

use loom_rt::artifact::{FeatureSpec, WorkerVerdict};
use loom_rt::worker::Worker;
use loom_rt::worker_claude_code::ClaudeCodeAdapter;
use std::ffi::OsString;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn echo_hello_captures_stdout() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let adapter = ClaudeCodeAdapter::new(
        PathBuf::from("/bin/echo"),
        vec![OsString::from("hello")],
        Duration::from_secs(5),
    );
    let spec = FeatureSpec {
        feature_dir: tmp.path().to_path_buf(),
        worker_identity: "echo-test".into(),
        dispatch_metadata: serde_yaml::Value::Null,
    };

    let artifact = adapter
        .run(spec, CancellationToken::new())
        .await
        .expect("run should succeed");

    assert_eq!(artifact.verdict, WorkerVerdict::Pass, "echo should pass");
    assert_eq!(artifact.exit_code, 0);
    assert!(
        !artifact.drain_truncated,
        "expected full drain on small output"
    );
    assert!(
        artifact.stdout_path.exists(),
        "stdout file should exist at {:?}",
        artifact.stdout_path
    );
    let captured = std::fs::read_to_string(&artifact.stdout_path).expect("read stdout file");
    assert_eq!(
        captured.trim_end(),
        "hello",
        "stdout content should be 'hello'"
    );
}

#[tokio::test]
async fn sleep_60_with_2s_timeout_reaps_grandchild() {
    // Use a unique sentinel embedded into the SLEEP argv (not the wrapper-shell
    // argv) so pgrep -f finds the actual grandchild process — not just the
    // parent shell. `sleep` doesn't accept arbitrary trailing tokens, so we use
    // `exec -a <sentinel> /bin/sleep 60` to make the shell EXEC-replace itself
    // with sleep while renaming the exec'd process. After exec, the process IS
    // the sleep, with `<sentinel>` as its argv[0].
    //
    // CI surfaced that `/bin/sh` is NOT portable for this: on macOS `/bin/sh`
    // is bash (which supports `exec -a`), but on Ubuntu it's `dash`, which
    // does NOT support `exec -a`. Dash would error immediately and the wrapper
    // would exit non-zero before the timeout fires — making the test see
    // `WorkerVerdict::Fail` instead of `WorkerVerdict::Timeout`. Use
    // `/bin/bash` explicitly so the `exec -a` semantics work on both
    // macos-latest and ubuntu-latest runners (both ship /bin/bash by default).
    let sentinel = format!("loom-test-sleep-{}", std::process::id());
    let bash_script = format!("exec -a {} /bin/sleep 60", sentinel);

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let adapter = ClaudeCodeAdapter::new(
        PathBuf::from("/bin/bash"),
        vec![OsString::from("-c"), OsString::from(&bash_script)],
        Duration::from_secs(2),
    );
    let spec = FeatureSpec {
        feature_dir: tmp.path().to_path_buf(),
        worker_identity: "sleep-timeout-test".into(),
        dispatch_metadata: serde_yaml::Value::Null,
    };

    let started = Instant::now();
    let artifact = adapter
        .run(spec, CancellationToken::new())
        .await
        .expect("run should succeed");
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(7),
        "should have timed out, elapsed={:?} (expected < 7s)",
        elapsed
    );
    assert_eq!(artifact.verdict, WorkerVerdict::Timeout);

    // Grandchild-orphan check: after process-group SIGKILL + wait, NEITHER
    // the sh wrapper NOR the sleep grandchild should remain. Allow 200ms for
    // the kernel to tear down the group.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let pgrep = std::process::Command::new("pgrep")
        .args(["-f", &sentinel])
        .output()
        .expect("pgrep should be available");
    let leaked_pids = String::from_utf8_lossy(&pgrep.stdout);
    assert!(
        leaked_pids.trim().is_empty(),
        "expected no orphaned process matching {:?}; pgrep found: {:?}",
        sentinel,
        leaked_pids
    );
}

#[tokio::test]
async fn cancellation_returns_quickly() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let adapter = ClaudeCodeAdapter::new(
        PathBuf::from("/bin/sleep"),
        vec![OsString::from("30")],
        Duration::from_secs(60), // would normally take 30s; cancellation should win
    );
    let spec = FeatureSpec {
        feature_dir: tmp.path().to_path_buf(),
        worker_identity: "cancel-test".into(),
        dispatch_metadata: serde_yaml::Value::Null,
    };

    let cancel = CancellationToken::new();
    let cancel_handle = cancel.clone();

    // Trigger cancellation after 500ms.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        cancel_handle.cancel();
    });

    let started = Instant::now();
    let artifact = adapter.run(spec, cancel).await.expect("run should succeed");
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(5),
        "cancellation should return well before 30s sleep + timeout; elapsed={:?}",
        elapsed
    );
    assert_eq!(artifact.verdict, WorkerVerdict::Cancelled);
}
