//! AC6 — PATH-only env scrub reference case.
//!
//! Verifies the structural invariant: after `apply_scrubbed_path` removes
//! the directory containing the `loom` binary from `PATH`, the spawned
//! subprocess (a shell) cannot reach `loom` via PATH lookup, but it CAN
//! still invoke the binary by absolute path. HOME/USER/SHELL must remain
//! observable in the child (NOT stripped by env_clear — we don't call it).
//!
//! Per Codex MF3: both checks required (PATH unreachable AND absolute path
//! works) — proves the scrub is selective, not destructive.

use loom_rt::artifact::{FeatureSpec, WorkerVerdict};
use loom_rt::spawn_env::apply_scrubbed_path;
use loom_rt::worker::Worker;
use loom_rt::worker_claude_code::ClaudeCodeAdapter;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

fn loom_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_loom"))
}

#[tokio::test]
async fn which_loom_fails_after_scrub() {
    let bin = loom_binary();
    let bin_dir = bin
        .parent()
        .expect("binary should have parent dir")
        .to_path_buf();

    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c").arg("command -v loom; echo exit=$?");
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    apply_scrubbed_path(&mut cmd, &bin_dir);

    let out = cmd.output().await.expect("spawn /bin/sh");
    let stdout = String::from_utf8_lossy(&out.stdout);
    // `command -v loom` exits non-zero when not found; we echo $? so we can
    // assert on a deterministic textual marker rather than parse exit codes.
    assert!(
        stdout.contains("exit=1") || stdout.contains("exit=127"),
        "expected `command -v loom` to fail under scrubbed PATH; got stdout={stdout:?}"
    );
}

#[tokio::test]
async fn absolute_path_loom_version_still_works() {
    let bin = loom_binary();
    let bin_dir = bin.parent().unwrap().to_path_buf();

    let script = format!("{} --version", bin.display());
    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c").arg(&script);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    apply_scrubbed_path(&mut cmd, &bin_dir);

    let out = cmd.output().await.expect("spawn /bin/sh");
    assert!(
        out.status.success(),
        "absolute-path loom invocation should succeed; status={:?}, stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("loom"),
        "version output should mention `loom`; got {stdout:?}"
    );
}

#[tokio::test]
async fn home_user_shell_preserved() {
    let bin = loom_binary();
    let bin_dir = bin.parent().unwrap().to_path_buf();

    // Only assert preservation for vars actually present in the parent env
    // (CI environments may not set USER, e.g.).
    let parent_has = |k: &str| std::env::var(k).is_ok();

    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c")
        .arg(r#"echo HOME=$HOME; echo USER=$USER; echo SHELL=$SHELL; echo TMPDIR=$TMPDIR"#);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    apply_scrubbed_path(&mut cmd, &bin_dir);

    let out = cmd.output().await.expect("spawn /bin/sh");
    let stdout = String::from_utf8_lossy(&out.stdout);

    if parent_has("HOME") {
        let parent = std::env::var("HOME").unwrap();
        assert!(
            stdout.contains(&format!("HOME={parent}")),
            "child HOME should mirror parent; got: {stdout:?}"
        );
    }
    if parent_has("SHELL") {
        let parent = std::env::var("SHELL").unwrap();
        assert!(
            stdout.contains(&format!("SHELL={parent}")),
            "child SHELL should mirror parent; got: {stdout:?}"
        );
    }
    if parent_has("USER") {
        let parent = std::env::var("USER").unwrap();
        assert!(
            stdout.contains(&format!("USER={parent}")),
            "child USER should mirror parent; got: {stdout:?}"
        );
    }
}

/// End-to-end AC6: prove the scrub is applied via the **actual dispatch
/// path** (`ClaudeCodeAdapter::with_scrubbed_path` → `Worker::run` → spawn),
/// not just by calling `apply_scrubbed_path` in the test harness directly.
///
/// We dispatch `/bin/sh -c 'command -v loom > stdout; echo exit=$?'` through
/// the adapter and inspect its captured stdout file. The captured stdout
/// must include `exit=1` or `exit=127` (PATH lookup failed).
#[tokio::test]
async fn dispatch_path_scrubs_path_end_to_end() {
    let bin = loom_binary();
    let bin_dir = bin.parent().unwrap().to_path_buf();

    let adapter = ClaudeCodeAdapter::with_scrubbed_path(
        PathBuf::from("/bin/sh"),
        vec![
            OsString::from("-c"),
            OsString::from("command -v loom; echo exit=$?"),
        ],
        Duration::from_secs(10),
        bin_dir,
    );

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let spec = FeatureSpec {
        feature_dir: tmp.path().to_path_buf(),
        worker_identity: "scrub-e2e".into(),
        dispatch_metadata: serde_yaml::Value::Null,
    };

    let artifact = adapter
        .run(spec, CancellationToken::new())
        .await
        .expect("adapter dispatch should succeed");

    // sh exits 0 even when `command -v` fails (we echo $? after).
    assert_eq!(artifact.verdict, WorkerVerdict::Pass);
    let captured = std::fs::read_to_string(&artifact.stdout_path).expect("read stdout");
    assert!(
        captured.contains("exit=1") || captured.contains("exit=127"),
        "expected dispatch-path scrub to render `loom` unreachable via PATH; \
         worker stdout was: {captured:?}"
    );
}
