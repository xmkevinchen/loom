//! F-006 Step 2 — stale-worktree startup cleanup (BL-005).
//!
//! Drives `prune_stale_worktrees_with` with an injected liveness closure so
//! the dead/alive decision is deterministic (no dependence on real OS PID
//! state). Asserts via `git worktree list --porcelain` rather than a substring
//! match on `git worktree list`, which is fragile under macOS `/tmp` →
//! `/private/tmp` symlink resolution.

use loom_rt::dispatch::prune_stale_worktrees_with;
use std::path::Path;
use tokio::process::Command;

/// `git -C <workspace> <args>`, asserting success.
async fn run_git(workspace: &str, args: &[&str]) {
    let mut full = vec!["-C", workspace];
    full.extend_from_slice(args);
    let status = Command::new("git")
        .args(&full)
        .status()
        .await
        .unwrap_or_else(|e| panic!("git {args:?} spawn failed: {e}"));
    assert!(status.success(), "git {args:?} non-zero ({status:?})");
}

/// `git worktree add --detach .loom/worktrees/<name> HEAD`.
async fn add_worktree(workspace: &Path, name: &str) {
    let wt = workspace.join(".loom/worktrees").join(name);
    run_git(
        workspace.to_str().unwrap(),
        &["worktree", "add", "--detach", wt.to_str().unwrap(), "HEAD"],
    )
    .await;
}

async fn porcelain_listing(workspace: &str) -> String {
    let out = Command::new("git")
        .args(["-C", workspace, "worktree", "list", "--porcelain"])
        .output()
        .await
        .expect("git worktree list --porcelain");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[tokio::test]
async fn prunes_dead_preserves_live_and_ignores_unparseable() {
    let tmp = tempfile::tempdir().unwrap();
    let ws = tmp.path();
    let ws_s = ws.to_str().unwrap();

    // Real git repo. user.email/user.name are REQUIRED or `git commit` fails
    // "Author identity unknown" and the assertions below pass vacuously on CI.
    run_git(ws_s, &["init", "-q"]).await;
    run_git(ws_s, &["config", "user.email", "test@loom"]).await;
    run_git(ws_s, &["config", "user.name", "test"]).await;
    run_git(ws_s, &["commit", "--allow-empty", "-q", "-m", "initial"]).await;
    std::fs::create_dir_all(ws.join(".loom/worktrees")).unwrap();

    let live = std::process::id();
    // Two dead orphans (pids 1 and 2 — the injected closure marks them dead).
    // Two of them so we also verify the loop processes every entry, not just
    // the first.
    add_worktree(ws, "F-901-1").await;
    add_worktree(ws, "F-904-2").await;
    // One live worktree tagged with the current pid.
    let live_name = format!("F-902-{live}");
    add_worktree(ws, &live_name).await;
    // A non-worktree dir whose name does not parse — must be left untouched.
    std::fs::create_dir_all(ws.join(".loom/worktrees/garbage")).unwrap();

    let dead_a = ws.join(".loom/worktrees/F-901-1");
    let dead_b = ws.join(".loom/worktrees/F-904-2");
    let live_p = ws.join(".loom/worktrees").join(&live_name);
    let garbage = ws.join(".loom/worktrees/garbage");
    assert!(dead_a.exists() && dead_b.exists() && live_p.exists() && garbage.exists());

    // Injected liveness: only the current pid is alive.
    prune_stale_worktrees_with(ws, |pid| pid == live).await;

    // Both dead orphans reclaimed (loop did not stop after the first).
    assert!(!dead_a.exists(), "dead F-901-1 should be removed");
    assert!(!dead_b.exists(), "dead F-904-2 should be removed");
    // Live worktree preserved.
    assert!(live_p.exists(), "live worktree must be preserved");
    // Unparseable-name dir untouched.
    assert!(garbage.exists(), "unparseable-name dir must be left untouched");

    // git admin entries: dead absent, live present.
    let listing = porcelain_listing(ws_s).await;
    assert!(
        !listing.contains("F-901-1"),
        "dead admin entry should be pruned:\n{listing}"
    );
    assert!(
        !listing.contains("F-904-2"),
        "dead admin entry should be pruned:\n{listing}"
    );
    assert!(
        listing.contains(&live_name),
        "live admin entry should remain:\n{listing}"
    );
}
