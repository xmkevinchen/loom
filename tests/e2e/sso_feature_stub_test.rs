//! Stub-based e2e test for the SSO login feature dispatch flow.
//!
//! This test uses `/bin/echo` as the worker command (instead of real
//! `claude --headless`) to verify the dispatch → artifact → verdict
//! pipeline shape without requiring AE-BL #1 to have shipped.
//!
//! Runs unconditionally (no `#[ignore]`); CI always passes this.
//! See `sso_feature_integration_test.rs` for the real-AE variant.

use loom_rt::artifact::{FeatureSpec, WorkerVerdict};
use loom_rt::worker::Worker;
use loom_rt::worker_claude_code::ClaudeCodeAdapter;
use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Simulates dispatching one "add SSO login" feature to a stub worker.
/// Verifies: artifact produced, verdict captured, stdout file present.
#[tokio::test]
async fn sso_feature_stub_dispatches_and_captures_artifact() {
    let tmp = tempfile::TempDir::new().expect("tempdir");

    // Stub: echo a fake AE completion marker instead of running real AE.
    let adapter = ClaudeCodeAdapter::new(
        PathBuf::from("/bin/echo"),
        vec![OsString::from("ae:work complete — SSO feature stub")],
        Duration::from_secs(10),
    );

    let spec = FeatureSpec {
        feature_dir: tmp.path().to_path_buf(),
        worker_identity: "claude-code-0".into(),
        dispatch_metadata: serde_yaml::Value::Null,
    };

    let artifact = adapter
        .run(spec, CancellationToken::new())
        .await
        .expect("stub dispatch should succeed");

    // (a) Feature dispatched — adapter returned without error.
    assert_eq!(
        artifact.verdict,
        WorkerVerdict::Pass,
        "stub worker should pass"
    );

    // (b) Artifact produced — stdout file exists and is non-empty.
    assert!(
        artifact.stdout_path.exists(),
        "stdout artifact should exist at {:?}",
        artifact.stdout_path
    );
    let captured = std::fs::read_to_string(&artifact.stdout_path).expect("read stdout");
    assert!(
        captured.contains("SSO"),
        "stdout should contain the stub output"
    );

    // (c) Worker identity tag present in artifact.
    assert_eq!(artifact.worker_identity, "claude-code-0");

    // (d) No drain truncation on small output.
    assert!(!artifact.drain_truncated);
}
