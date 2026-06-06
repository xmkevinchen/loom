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

/// F-015 — fork-grandchild group-kill transitivity. The existing
/// `sleep_60_with_2s_timeout_reaps_grandchild` test uses `exec`-replacement
/// (a ONE-level tree: the "grandchild" IS the exec'd child, same PID). This
/// test builds a genuine TWO-level tree: `sh` backgrounds a `sleep` whose PID
/// (`$!`) is independent of the shell, then verifies that cancelling the worker
/// delivers SIGKILL all the way to that fork-grandchild via the process-group
/// kill. It pins the loom-side composition: `process_group(0)` at spawn
/// (worker_claude_code.rs:122) + nothing letting a fork-child escape the group
/// + `kill_process_group` targeting the right group (worker_claude_code.rs:197).
#[cfg(unix)]
#[tokio::test]
async fn cancel_kills_fork_grandchild_in_group() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let pid_file = tmp.path().join("grandchild.pid");

    // `&` + `$!` are POSIX (dash-safe — no bash needed). The backgrounded
    // `sleep` is a fork-child of the shell with its OWN pid ($!), inheriting the
    // shell's process group (job control is OFF in non-interactive `sh -c`, so
    // it stays in-group). The trailing foreground `sleep 600` keeps the shell
    // (group leader) alive until we cancel.
    let script = format!("sleep 600 & echo $! > {}; sleep 600", pid_file.display());
    let adapter = ClaudeCodeAdapter::new(
        PathBuf::from("/bin/sh"),
        vec![OsString::from("-c"), OsString::from(&script)],
        Duration::from_secs(60), // long; cancellation wins well before this
    );
    let spec = FeatureSpec {
        feature_dir: tmp.path().to_path_buf(),
        worker_identity: "grandchild-group-kill-test".into(),
        dispatch_metadata: serde_yaml::Value::Null,
    };

    let cancel = CancellationToken::new();
    let cancel_handle = cancel.clone();

    // Ready-gate: poll the pid-file until the grandchild's pid is readable AND
    // the grandchild is alive RIGHT NOW (kill(pid,0)==0). Asserting liveness
    // before cancel is what makes the later ESRCH non-vacuous: it proves the pid
    // was a LIVE process at cancel time, so its death is attributable to the
    // group-kill, not to a never-existed / already-dead pid. Bounded at 5s; on
    // timeout we still cancel (so run() returns) but yield None → loud failure.
    let pid_file_gate = pid_file.clone();
    let gate = async move {
        let gate_start = Instant::now();
        let grandchild_pid = loop {
            if let Ok(contents) = std::fs::read_to_string(&pid_file_gate) {
                if let Ok(pid) = contents.trim().parse::<i32>() {
                    if pid > 0 && unsafe { libc::kill(pid as libc::pid_t, 0) } == 0 {
                        break Some(pid);
                    }
                }
            }
            if gate_start.elapsed() > Duration::from_secs(5) {
                break None;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        cancel_handle.cancel();
        grandchild_pid
    };

    let (artifact, grandchild_pid) = tokio::join!(adapter.run(spec, cancel), gate);
    let artifact = artifact.expect("run should succeed");
    let grandchild_pid =
        grandchild_pid.expect("ready-gate never observed a live grandchild pid within 5s");

    assert_eq!(artifact.verdict, WorkerVerdict::Cancelled);

    // Transitivity assertion: the fork-grandchild must reach ESRCH. A zombie
    // answers kill(pid,0) with 0 until init reaps it (the grandchild reparents
    // to init once the group-kill takes down its `sh` parent), hence the bounded
    // poll rather than a single check. EPERM / any non-ESRCH errno counts as
    // still-alive (F-006 errno discipline). Timeout here == the group-kill did
    // NOT reach the grandchild (transitivity broken).
    let poll_start = Instant::now();
    loop {
        let rc = unsafe { libc::kill(grandchild_pid as libc::pid_t, 0) };
        if rc == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
            break;
        }
        assert!(
            poll_start.elapsed() < Duration::from_secs(2),
            "fork-grandchild pid {grandchild_pid} still alive 2s after cancel — \
             group-kill did not reach it (transitivity broken)"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
