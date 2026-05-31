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
    // A pid the injected closure marks ALIVE but that is NOT our own pid — so
    // the corresponding worktree is preserved via the `is_alive` true-branch,
    // independent of the `pid == self_pid` short-circuit.
    const ALIVE_FAKE: u32 = 424242;
    // Two dead orphans (pids 1 and 2 — the injected closure marks them dead).
    // Two of them so we also verify the loop processes every entry, not just
    // the first.
    add_worktree(ws, "F-901-1").await;
    add_worktree(ws, "F-904-2").await;
    // One live worktree tagged with the current pid (preserved via pid == self).
    let live_name = format!("F-902-{live}");
    add_worktree(ws, &live_name).await;
    // One live worktree tagged with a NON-self pid the closure reports alive
    // (preserved via the is_alive true-branch — distinguishes it from self).
    let alive_name = format!("F-906-{ALIVE_FAKE}");
    add_worktree(ws, &alive_name).await;
    // A non-worktree dir whose name does not parse — must be left untouched.
    std::fs::create_dir_all(ws.join(".loom/worktrees/garbage")).unwrap();
    // A parseable, dead-pid dir that is NOT a registered git worktree, so
    // `git worktree remove --force` returns non-zero. Exercises the
    // warn-and-continue error arm and proves the loop does not abort on a
    // single remove failure (AC3 "best-effort continues on error").
    std::fs::create_dir_all(ws.join(".loom/worktrees/F-905-3")).unwrap();

    let dead_a = ws.join(".loom/worktrees/F-901-1");
    let dead_b = ws.join(".loom/worktrees/F-904-2");
    let live_p = ws.join(".loom/worktrees").join(&live_name);
    let alive_p = ws.join(".loom/worktrees").join(&alive_name);
    let garbage = ws.join(".loom/worktrees/garbage");
    let remove_fails = ws.join(".loom/worktrees/F-905-3");
    assert!(dead_a.exists() && dead_b.exists() && live_p.exists() && alive_p.exists());

    // Injected liveness: our own pid and ALIVE_FAKE are alive; everything else dead.
    prune_stale_worktrees_with(ws, |pid| pid == live || pid == ALIVE_FAKE).await;

    // Both real dead orphans reclaimed even though F-905-3's remove failed
    // (loop continued past the error and processed every entry).
    assert!(!dead_a.exists(), "dead F-901-1 should be removed");
    assert!(!dead_b.exists(), "dead F-904-2 should be removed");
    // Live worktree preserved via pid == self.
    assert!(live_p.exists(), "self-pid live worktree must be preserved");
    // Live worktree preserved via is_alive(non-self) == true.
    assert!(
        alive_p.exists(),
        "is_alive-true (non-self) worktree must be preserved"
    );
    // Unparseable-name dir untouched (never reached the remove call).
    assert!(
        garbage.exists(),
        "unparseable-name dir must be left untouched"
    );
    // The non-worktree dead-pid dir: remove returned non-zero, so it remains —
    // and crucially the loop still reclaimed the real orphans above.
    assert!(
        remove_fails.exists(),
        "non-worktree dir survives a failed `git worktree remove`, but must not abort the loop"
    );

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
        "self-pid live admin entry should remain:\n{listing}"
    );
    assert!(
        listing.contains(&alive_name),
        "is_alive-true admin entry should remain:\n{listing}"
    );
}
