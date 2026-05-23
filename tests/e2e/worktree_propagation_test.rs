//! F-004 e2e: worker commit propagation to refs/heads/loom-features/F-NNN.
//!
//! Verifies that after a worker exits, `run_dispatch_loop` writes (or
//! correctly declines to write) the rescue ref according to verdict +
//! HEAD-advance state. The four test cases mirror the four outcomes the
//! Step-2 `propagate_worktree_commits` function distinguishes:
//!
//!  1. Pass + HEAD advanced → rescue ref written, points at worker's commit.
//!  2. Pass + zero commits → no ref (zero-commit guard skips propagation).
//!  3. Fail + commits → no ref (call-site verdict gate blocks).
//!  4. Re-dispatch + Pass → ref overwrites with prior SHA recoverable from
//!     `git reflog show refs/heads/loom-features/...`.
//!
//! The test harness must use a real `git init` workspace + initial commit;
//! `tempfile::TempDir` alone is insufficient because `git worktree add
//! --detach HEAD` silently fails on an unborn HEAD, and `maybe_create_worktree`
//! would then return None — workers would run in feature_dir fallback and
//! propagation never fires.

use async_trait::async_trait;
use loom_rt::artifact::{Artifact, FeatureSpec, WorkerVerdict};
use loom_rt::discovery::DiscoveredFeature;
use loom_rt::dispatch::run_dispatch_loop;
use loom_rt::worker::Worker;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

/// Test worker that:
///   1. writes a file inside `spec.feature_dir` (= the worktree, when one
///      was created),
///   2. optionally runs `git add` + `git commit` from inside that dir,
///   3. returns an Artifact with the test-controlled verdict.
///
/// `received_feature_dir` is a side-channel so the test can assert post
/// dispatch that the worker really did run inside a worktree (per plan
/// dep-analyst MF-C). Without this the negative-path tests (zero-commit /
/// fail) could pass for the wrong reason: if `maybe_create_worktree`
/// silently returned None and the worker ran in feature_dir fallback,
/// `propagate_worktree_commits` never fires and the ref correctly doesn't
/// exist — but we'd never have exercised the gate logic we claim to test.
struct StubCommitWorker {
    verdict: WorkerVerdict,
    do_commit: bool,
    file_content: String,
    received_feature_dir: Arc<Mutex<Option<PathBuf>>>,
    /// SHA of the commit the stub actually produced inside the worktree
    /// (captured via `git rev-parse HEAD` after the stub's own `git commit`).
    /// Tests use this to assert the rescue ref equals the worker's exact
    /// commit — AC2 says "outputs the worker's commit SHA (not the initial
    /// commit)"; `assert_ne!(sha, initial)` alone is necessary-not-sufficient.
    /// `None` when `do_commit = false` (zero-commit / fail tests).
    committed_sha: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl Worker for StubCommitWorker {
    async fn run(&self, spec: FeatureSpec, _cancel: CancellationToken) -> anyhow::Result<Artifact> {
        // 0. Record the feature_dir we were handed so the test can assert
        //    we actually ran inside a worktree (not the feature_dir
        //    fallback). Without this the negative-path tests below could
        //    pass for the wrong reason.
        *self.received_feature_dir.lock().unwrap() = Some(spec.feature_dir.clone());

        // 1. Write a marker file in spec.feature_dir.
        let file = spec.feature_dir.join("worker_output.txt");
        std::fs::write(&file, &self.file_content)
            .map_err(|e| anyhow::anyhow!("write worker_output: {e}"))?;

        // 2. Optionally commit. When do_commit = false we exercise the
        //    zero-commit guard (HEAD never advances).
        if self.do_commit {
            let add = Command::new("git")
                .args([
                    "-C",
                    spec.feature_dir.to_str().expect("feature_dir to_str"),
                    "add",
                    "worker_output.txt",
                ])
                .status()
                .await?;
            anyhow::ensure!(add.success(), "git add failed: {:?}", add);

            let commit = Command::new("git")
                .args([
                    "-C",
                    spec.feature_dir.to_str().expect("feature_dir to_str"),
                    "commit",
                    "-q",
                    "-m",
                    "stub commit",
                ])
                .status()
                .await?;
            anyhow::ensure!(commit.success(), "git commit failed: {:?}", commit);

            // Capture the SHA we just produced so the test can match the
            // rescue ref against it byte-for-byte (AC2 strict equality).
            let head = Command::new("git")
                .args([
                    "-C",
                    spec.feature_dir.to_str().expect("feature_dir to_str"),
                    "rev-parse",
                    "HEAD",
                ])
                .output()
                .await?;
            anyhow::ensure!(
                head.status.success(),
                "rev-parse HEAD failed: {:?}",
                head.status
            );
            let sha = String::from_utf8(head.stdout)
                .map_err(|e| anyhow::anyhow!("rev-parse stdout not UTF-8: {e}"))?
                .trim()
                .to_string();
            *self.committed_sha.lock().unwrap() = Some(sha);
        }

        // 3. Build a minimal Artifact. The stdout file just needs to exist.
        let stdout_path = spec.feature_dir.join("stub_stdout.log");
        let _ = std::fs::write(&stdout_path, "stub\n");

        Ok(Artifact {
            verdict: self.verdict,
            stdout_path,
            reasoning_trace: None,
            duration: Duration::from_millis(1),
            worker_identity: spec.worker_identity,
            exit_code: if self.verdict == WorkerVerdict::Pass {
                0
            } else {
                1
            },
            drain_truncated: false,
        })
    }
}

/// Initialise `workspace` as a real git repo:
///   - `git init`
///   - user.email / user.name (otherwise `git commit` refuses)
///   - one initial empty commit (so `git worktree add --detach HEAD` has
///     something to detach from; without it `maybe_create_worktree` would
///     return None and the propagation code path would never run)
///   - an `.ae/features/active/F-001-stub/index.md` so the test's
///     hand-built `DiscoveredFeature` corresponds to a real on-disk shape.
///
/// Returns the path to the feature dir.
async fn setup_workspace(workspace: &Path) -> PathBuf {
    let ws_str = workspace.to_str().expect("workspace to_str");

    run_git(ws_str, &["init", "-q"]).await;
    run_git(ws_str, &["config", "user.email", "test@loom"]).await;
    run_git(ws_str, &["config", "user.name", "test"]).await;
    run_git(ws_str, &["commit", "--allow-empty", "-q", "-m", "initial"]).await;

    let feat_dir = workspace.join(".ae/features/active/F-001-stub");
    std::fs::create_dir_all(&feat_dir).expect("create feature dir");
    std::fs::write(
        feat_dir.join("index.md"),
        "---\nid: F-001-stub\n---\n\nF-004 propagation stub.\n",
    )
    .expect("write index.md");

    feat_dir
}

/// Run `git -C workspace <args>` and assert success. Fails the test on
/// non-zero exit / spawn error.
async fn run_git(workspace: &str, args: &[&str]) {
    let mut full = vec!["-C", workspace];
    full.extend_from_slice(args);
    let status = Command::new("git")
        .args(&full)
        .status()
        .await
        .unwrap_or_else(|e| panic!("git {:?} spawn failed: {}", args, e));
    assert!(
        status.success(),
        "git {:?} non-zero ({:?}) in workspace {}",
        args,
        status,
        workspace
    );
}

/// Workspace HEAD SHA. Panics on failure (tests can rely on the workspace
/// being valid at this point).
async fn workspace_head_sha(workspace: &Path) -> String {
    let out = Command::new("git")
        .args([
            "-C",
            workspace.to_str().expect("workspace to_str"),
            "rev-parse",
            "HEAD",
        ])
        .output()
        .await
        .expect("git rev-parse HEAD");
    assert!(out.status.success(), "rev-parse HEAD non-zero");
    String::from_utf8(out.stdout)
        .expect("rev-parse stdout utf-8")
        .trim()
        .to_string()
}

/// Returns `Some(<sha>)` if the ref exists, `None` otherwise. Never panics
/// on a missing ref — that's the distinguishing assertion for tests 2 and 3.
async fn ref_sha(workspace: &Path, ref_name: &str) -> Option<String> {
    let out = Command::new("git")
        .args([
            "-C",
            workspace.to_str().expect("workspace to_str"),
            "rev-parse",
            "--verify",
            "--quiet",
            ref_name,
        ])
        .output()
        .await
        .expect("git rev-parse --verify");
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8(out.stdout).ok()?;
    let trimmed = s.trim().to_string();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

const REF_NAME: &str = "refs/heads/loom-features/F-001-stub";

/// Worktree-create sanity check (per plan dep-analyst MF-C).
///
/// Asserts the worker received a path under `<workspace>/.loom/worktrees/`
/// — i.e. that `maybe_create_worktree` actually produced a worktree and
/// dispatch passed it down via `FeatureSpec::feature_dir`. If
/// `maybe_create_worktree` had silently returned None, the worker would
/// have received the AE feature dir and the propagation code path would
/// never have run; without this check, the negative-path tests below
/// (zero-commit, verdict-fail) would silently pass for the wrong reason.
fn assert_worker_ran_in_worktree(
    captured: &Arc<Mutex<Option<PathBuf>>>,
    workspace: &Path,
    ae_feature_dir: &Path,
) {
    let received = captured
        .lock()
        .expect("captured lock poisoned")
        .clone()
        .expect("StubCommitWorker should have recorded its received feature_dir");
    assert_ne!(
        received, ae_feature_dir,
        "worker ran in feature_dir fallback (worktree was never created); negative-path test would have false-passed"
    );
    let wt_root = workspace.join(".loom").join("worktrees");
    assert!(
        received.starts_with(&wt_root),
        "worker's feature_dir {} should sit under {} (.loom/worktrees)",
        received.display(),
        wt_root.display()
    );
}

/// (1) Pass + HEAD-advance → rescue ref points at worker's commit.
#[tokio::test]
async fn test_pass_head_advance_creates_ref() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let ws = tmp.path().to_path_buf();
    let feat_dir = setup_workspace(&ws).await;
    let initial = workspace_head_sha(&ws).await;

    let captured = Arc::new(Mutex::new(None));
    let committed_sha = Arc::new(Mutex::new(None));
    let ae_feature_dir = feat_dir.clone();
    let features = vec![DiscoveredFeature {
        id: "F-001-stub".into(),
        feature_dir: feat_dir,
        depends_on: vec![],
        work_state: None,
    }];
    let stub = Arc::new(StubCommitWorker {
        verdict: WorkerVerdict::Pass,
        do_commit: true,
        file_content: "pass+advance\n".into(),
        received_feature_dir: captured.clone(),
        committed_sha: committed_sha.clone(),
    });

    let report = run_dispatch_loop(
        features,
        vec![stub],
        1,
        ws.clone(),
        CancellationToken::new(),
    )
    .await
    .expect("dispatch");
    assert_eq!(report.dispatched_count, 1);

    // Worktree-create sanity FIRST so a regression to feature_dir
    // fallback fails the test on its actual cause, not on a downstream
    // ref-content assertion.
    assert_worker_ran_in_worktree(&captured, &ws, &ae_feature_dir);

    let sha = ref_sha(&ws, REF_NAME)
        .await
        .expect("rescue ref should exist after Pass+commit dispatch");
    assert_ne!(
        sha, initial,
        "rescue ref should not point at the initial commit; got {} (initial {})",
        sha, initial
    );

    // AC2 strict equality: rescue ref must equal the worker's actual
    // commit SHA, not merely "something other than initial". The plan's
    // AC2 spec calls out "The captured SHA equals what the
    // StubCommitWorker actually committed (read from the worktree
    // pre-cleanup)" — `assert_ne!(sha, initial)` is necessary-not-
    // sufficient. Without this, a propagation bug that wrote any
    // non-initial SHA would pass the inequality check silently.
    let worker_sha = committed_sha
        .lock()
        .expect("committed_sha lock poisoned")
        .clone()
        .expect("StubCommitWorker should have captured its committed SHA");
    assert_eq!(
        sha, worker_sha,
        "rescue ref should equal the worker's actual commit SHA (AC2); got ref={} worker_commit={}",
        sha, worker_sha
    );
}

/// (2) Pass + zero-commit → zero-commit guard kicks in, no ref written.
#[tokio::test]
async fn test_pass_zero_commit_no_ref() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let ws = tmp.path().to_path_buf();
    let feat_dir = setup_workspace(&ws).await;

    let captured = Arc::new(Mutex::new(None));
    let ae_feature_dir = feat_dir.clone();
    let features = vec![DiscoveredFeature {
        id: "F-001-stub".into(),
        feature_dir: feat_dir,
        depends_on: vec![],
        work_state: None,
    }];
    let stub = Arc::new(StubCommitWorker {
        verdict: WorkerVerdict::Pass,
        do_commit: false, // writes file but never commits
        file_content: "no-commit\n".into(),
        received_feature_dir: captured.clone(),
        committed_sha: Arc::new(Mutex::new(None)),
    });

    let _ = run_dispatch_loop(
        features,
        vec![stub],
        1,
        ws.clone(),
        CancellationToken::new(),
    )
    .await
    .expect("dispatch");

    // Sanity FIRST: prove we actually exercised the zero-commit guard on
    // a worktree, not the feature_dir fallback.
    assert_worker_ran_in_worktree(&captured, &ws, &ae_feature_dir);

    assert!(
        ref_sha(&ws, REF_NAME).await.is_none(),
        "rescue ref must NOT exist when worker made zero commits"
    );
}

/// (3) Fail (even with commits) → call-site verdict gate blocks propagation.
#[tokio::test]
async fn test_fail_no_ref() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let ws = tmp.path().to_path_buf();
    let feat_dir = setup_workspace(&ws).await;

    let captured = Arc::new(Mutex::new(None));
    let ae_feature_dir = feat_dir.clone();
    let features = vec![DiscoveredFeature {
        id: "F-001-stub".into(),
        feature_dir: feat_dir,
        depends_on: vec![],
        work_state: None,
    }];
    let stub = Arc::new(StubCommitWorker {
        verdict: WorkerVerdict::Fail,
        do_commit: true, // worker commits — but the verdict gate at the
        // call site (`WorkerVerdict::Pass` match) blocks propagation.
        file_content: "fail+commit\n".into(),
        received_feature_dir: captured.clone(),
        committed_sha: Arc::new(Mutex::new(None)),
    });

    let _ = run_dispatch_loop(
        features,
        vec![stub],
        1,
        ws.clone(),
        CancellationToken::new(),
    )
    .await
    .expect("dispatch");

    // Sanity FIRST: prove we exercised the verdict gate on a worktree,
    // not the feature_dir fallback.
    assert_worker_ran_in_worktree(&captured, &ws, &ae_feature_dir);

    assert!(
        ref_sha(&ws, REF_NAME).await.is_none(),
        "rescue ref must NOT exist when verdict=Fail (commits stay dangling, recoverable via gc.pruneExpire window)"
    );
}

/// (4) Re-dispatch with Pass+different-content → ref overwrites silently
/// and reflog retains both SHAs (Topic 2 silent-overwrite + reflog audit).
#[tokio::test]
async fn test_redispatch_silent_overwrite_with_reflog() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let ws = tmp.path().to_path_buf();
    let feat_dir = setup_workspace(&ws).await;

    // First dispatch.
    let first_captured = Arc::new(Mutex::new(None));
    let ae_feature_dir = feat_dir.clone();
    let first_features = vec![DiscoveredFeature {
        id: "F-001-stub".into(),
        feature_dir: feat_dir.clone(),
        depends_on: vec![],
        work_state: None,
    }];
    let first_stub = Arc::new(StubCommitWorker {
        verdict: WorkerVerdict::Pass,
        do_commit: true,
        file_content: "first dispatch\n".into(),
        received_feature_dir: first_captured.clone(),
        committed_sha: Arc::new(Mutex::new(None)),
    });
    let _ = run_dispatch_loop(
        first_features,
        vec![first_stub],
        1,
        ws.clone(),
        CancellationToken::new(),
    )
    .await
    .expect("first dispatch");
    assert_worker_ran_in_worktree(&first_captured, &ws, &ae_feature_dir);
    let first_sha = ref_sha(&ws, REF_NAME)
        .await
        .expect("first dispatch should write rescue ref");

    // Defensive `worktree prune` between dispatches (per plan C-5): same
    // process → same `std::process::id()` → same wt_path. `cleanup()`
    // already runs `git worktree remove --force` which both deletes the
    // dir AND deregisters from `.git/worktrees/`, so the second
    // `git worktree add` should reuse the path. `prune` is the documented
    // belt-and-braces fix if the deregister step ever flakes.
    let _ = Command::new("git")
        .args([
            "-C",
            ws.to_str().expect("workspace to_str"),
            "worktree",
            "prune",
        ])
        .status()
        .await;

    // Second dispatch with different content → different commit SHA.
    let second_captured = Arc::new(Mutex::new(None));
    let second_features = vec![DiscoveredFeature {
        id: "F-001-stub".into(),
        feature_dir: feat_dir.clone(),
        depends_on: vec![],
        work_state: None,
    }];
    let second_stub = Arc::new(StubCommitWorker {
        verdict: WorkerVerdict::Pass,
        do_commit: true,
        file_content: "second dispatch\n".into(),
        received_feature_dir: second_captured.clone(),
        committed_sha: Arc::new(Mutex::new(None)),
    });
    let _ = run_dispatch_loop(
        second_features,
        vec![second_stub],
        1,
        ws.clone(),
        CancellationToken::new(),
    )
    .await
    .expect("second dispatch");
    assert_worker_ran_in_worktree(&second_captured, &ws, &ae_feature_dir);
    let second_sha = ref_sha(&ws, REF_NAME)
        .await
        .expect("rescue ref should still exist after second dispatch (overwritten)");

    assert_ne!(
        first_sha, second_sha,
        "second dispatch should produce a different SHA (different file content); got first={} second={}",
        first_sha, second_sha
    );

    // Reflog audit: both SHAs must appear in the reflog so an operator
    // can recover the first dispatch's commit via `git reflog show ...`.
    // Use `--format=%H` to force full SHAs in the output — git's default
    // abbreviation honours `core.abbrev` and grows with repo size, so a
    // hardcoded prefix length would be brittle across environments
    // (per Doodlestein regret Track 4).
    let reflog = Command::new("git")
        .args([
            "-C",
            ws.to_str().expect("workspace to_str"),
            "reflog",
            "show",
            "--format=%H",
            REF_NAME,
        ])
        .output()
        .await
        .expect("reflog show");
    assert!(
        reflog.status.success(),
        "reflog show should succeed; got {:?}\nstderr: {}",
        reflog.status,
        String::from_utf8_lossy(&reflog.stderr)
    );
    let log_text = String::from_utf8_lossy(&reflog.stdout).into_owned();
    let line_count = log_text.lines().count();
    assert!(
        line_count >= 2,
        "reflog should list both dispatches (≥ 2 entries); got {} lines:\n{}",
        line_count,
        log_text
    );

    // Both full SHAs must appear in the reflog output.
    assert!(
        log_text.contains(&first_sha),
        "reflog should contain full first SHA {}\nfull reflog:\n{}",
        first_sha,
        log_text
    );
    assert!(
        log_text.contains(&second_sha),
        "reflog should contain full second SHA {}\nfull reflog:\n{}",
        second_sha,
        log_text
    );
}
