//! Real-AE integration test for the SSO login feature dispatch flow.
//!
//! BLOCKED on AE-plugin-BL #1 (headless invocation protocol). Without
//! AE-BL #1, `ClaudeCodeAdapter` spawns CC sessions at blank prompts and
//! the test cannot produce a meaningful feature DAG.
//!
//! All tests in this file carry `#[ignore]`. Run with:
//!   cargo test --test sso_feature_integration_test -- --ignored
//!
//! When AE-BL #1 ships:
//! 1. Replace the `todo!()` bodies below with real invocations.
//! 2. Adjust `env_vars` in the adapter if AE-BL #1 requires specific env
//!    vars beyond HOME/USER/SHELL (e.g. ANTHROPIC_API_KEY, CLAUDE_* vars)
//!    per Codex Consider #5 in plan F-001 Step 8.
//! 3. Remove `#[ignore]` from the passing tests; keep it only on flaky ones.

use loom_rt::artifact::{FeatureSpec, WorkerVerdict};
use loom_rt::worker::Worker;
use loom_rt::worker_claude_code::ClaudeCodeAdapter;
use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Full 6-phase loop: given goal "add SSO login", verify:
///   (a) feature DAG produced under `.ae/features/active/`
///   (b) ≥1 feature dispatched to a worker
///   (c) verdict captured (pass or fail — not pending)
///   (d) dispatch log contains worker-identity tags
///
/// GATED on AE-plugin-BL #1 (headless invocation protocol).
/// Track in: /Users/ckai/Workspace/Projects/agentic-engineering
#[tokio::test]
#[ignore = "BLOCKED: AE-plugin-BL #1 (headless invocation) not yet shipped"]
async fn sso_feature_real_ae_full_loop() {
    todo!(
        "implement after AE-BL #1 ships: \
         (1) invoke `claude --headless ae:backlog 'add SSO login'` via ClaudeCodeAdapter, \
         (2) read feature DAG from .ae/features/active/, \
         (3) dispatch ≥1 feature, \
         (4) assert verdict captured"
    );
}

/// Verify that the real `claude --headless` binary is reachable and returns
/// a zero exit code for `--version` (smoke-tests the headless invocation path
/// before running the full loop).
///
/// GATED on AE-plugin-BL #1 — without it we don't know the correct
/// `--headless` flag shape.
#[tokio::test]
#[ignore = "BLOCKED: AE-plugin-BL #1 (headless invocation) not yet shipped"]
async fn claude_headless_version_smoke() {
    let tmp = tempfile::TempDir::new().expect("tempdir");

    // Placeholder: replace with real claude binary path + headless flag per BL #1 spec.
    let adapter = ClaudeCodeAdapter::new(
        PathBuf::from("claude"),
        vec![OsString::from("--version")],
        Duration::from_secs(30),
    );

    let spec = FeatureSpec {
        feature_dir: tmp.path().to_path_buf(),
        worker_identity: "claude-headless-smoke".into(),
        dispatch_metadata: serde_yaml::Value::Null,
    };

    let artifact = adapter
        .run(spec, CancellationToken::new())
        .await
        .expect("claude --version should not error");

    assert_eq!(
        artifact.verdict,
        WorkerVerdict::Pass,
        "claude --version should exit 0"
    );
}
