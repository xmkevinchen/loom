//! Phase 2 + 3 — DAG ready-set scheduling + parallel execution.
//!
//! v0.1 contract:
//! - Ready set = features whose `pipeline.work != "done"` AND whose
//!   `depends_on` deps are all `done`.
//! - Parallelism bounded by a tokio `Semaphore` (default 4, capped by
//!   `max_parallel` from policy).
//! - Each ready feature is run inside a per-feature `git worktree` so worker
//!   subprocesses see an isolated working tree. The worktree is torn down on
//!   completion (best-effort cleanup; failures are logged, not propagated).

use crate::artifact::Artifact;
use crate::discovery::DiscoveredFeature;
use crate::verdict::{parse_review_once, AeVerdict};
use crate::worker::Worker;
use anyhow::{Context, Result};
use serde::Serialize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Per-feature outcome captured after a Worker invocation.
///
/// F-010 splits the two signals that used to be conflated in one `verdict`:
/// - `verdict` — the operator-facing **AE review** judgment read from
///   `<feature_dir>/review.md` after the worker exits: `pass` | `fail` |
///   `unknown` (no readable review.md). This is the dispatch.log headline.
/// - `worker_exit_status` — the worker **process** signal: `pass` | `fail` |
///   `timeout` | `cancelled` | `error` | `panic`. This is what
///   [`crate::iteration`]'s `any_fail` and `main::exit_code_for_report`
///   classify (unchanged values, just a renamed field).
///
/// Both serialize into `dispatch.log`. The schema gained `worker_exit_status`
/// and `verdict` changed meaning — intentional; dispatch.log is a local,
/// single-consumer artifact (no external reader of the old `verdict` key).
#[derive(Debug, Serialize)]
pub struct FeatureOutcome {
    pub feature_id: String,
    pub worker_identity: String,
    pub verdict: String,
    pub worker_exit_status: String,
    pub exit_code: i32,
    pub duration_ms: u128,
    pub stdout_path: PathBuf,
    pub drain_truncated: bool,
    pub error: Option<String>,
}

/// Aggregate report from a single dispatch cycle.
#[derive(Debug, Serialize)]
pub struct DispatchReport {
    pub started_at_ms: u64,
    pub elapsed_ms: u128,
    pub dispatched_count: usize,
    pub outcomes: Vec<FeatureOutcome>,
}

/// Compute the ready set: features that are not yet done AND whose deps are done.
///
/// `features` may carry features in any state; this function does not mutate.
pub fn ready_set(features: &[DiscoveredFeature]) -> Vec<DiscoveredFeature> {
    use std::collections::HashSet;
    let done_ids: HashSet<&str> = features
        .iter()
        .filter(|f| f.is_done())
        .map(|f| f.id.as_str())
        .collect();

    features
        .iter()
        .filter(|f| !f.is_done())
        .filter(|f| f.depends_on.iter().all(|d| done_ids.contains(d.as_str())))
        .cloned()
        .collect()
}

/// Run one dispatch cycle: pick the ready set, run each on a worker (parallel,
/// bounded by `max_parallel`), return a report.
///
/// `workers` is round-robin assigned across the ready set. v0.1 default has
/// one `ClaudeCodeAdapter` in the pool; passing several lets v0.2+ wire
/// heterogeneous adapters without breaking this signature.
pub async fn run_dispatch_loop(
    features: Vec<DiscoveredFeature>,
    workers: Vec<Arc<dyn Worker>>,
    max_parallel: usize,
    workspace: PathBuf,
    cancel: CancellationToken,
) -> Result<DispatchReport> {
    let ready = ready_set(&features);
    info!(
        ready_count = ready.len(),
        total = features.len(),
        max_parallel,
        "dispatch: cycle start"
    );
    let started = Instant::now();
    let started_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    if ready.is_empty() || workers.is_empty() {
        return Ok(DispatchReport {
            started_at_ms,
            elapsed_ms: started.elapsed().as_millis(),
            dispatched_count: 0,
            outcomes: Vec::new(),
        });
    }

    let permits = Arc::new(Semaphore::new(max_parallel.max(1)));
    let mut handles = Vec::with_capacity(ready.len());

    for (i, feature) in ready.into_iter().enumerate() {
        let worker = workers[i % workers.len()].clone();
        let permits = permits.clone();
        let cancel = cancel.clone();
        let workspace = workspace.clone();
        let worker_identity = format!("{}-w{}", feature.id, i);

        let handle = tokio::task::spawn(async move {
            let _permit = permits
                .acquire_owned()
                .await
                .context("acquire dispatch permit")?;
            run_one_feature(feature, worker, worker_identity, &workspace, cancel).await
        });
        handles.push(handle);
    }

    let mut outcomes = Vec::with_capacity(handles.len());
    for h in handles {
        match h.await {
            Ok(Ok(outcome)) => outcomes.push(outcome),
            Ok(Err(e)) => {
                warn!(error = %e, "dispatch: per-feature task returned Err");
                outcomes.push(FeatureOutcome {
                    feature_id: "<unknown>".into(),
                    worker_identity: "<unknown>".into(),
                    verdict: "unknown".into(),
                    worker_exit_status: "error".into(),
                    exit_code: -1,
                    duration_ms: 0,
                    stdout_path: PathBuf::new(),
                    drain_truncated: false,
                    error: Some(format!("{e:#}")),
                });
            }
            Err(join_err) => {
                warn!(error = %join_err, "dispatch: per-feature task join error");
                outcomes.push(FeatureOutcome {
                    feature_id: "<unknown>".into(),
                    worker_identity: "<unknown>".into(),
                    verdict: "unknown".into(),
                    worker_exit_status: "panic".into(),
                    exit_code: -1,
                    duration_ms: 0,
                    stdout_path: PathBuf::new(),
                    drain_truncated: false,
                    error: Some(format!("{join_err}")),
                });
            }
        }
    }

    Ok(DispatchReport {
        started_at_ms,
        elapsed_ms: started.elapsed().as_millis(),
        dispatched_count: outcomes.len(),
        outcomes,
    })
}

async fn run_one_feature(
    feature: DiscoveredFeature,
    worker: Arc<dyn Worker>,
    worker_identity: String,
    workspace: &std::path::Path,
    cancel: CancellationToken,
) -> Result<FeatureOutcome> {
    let feature_id = feature.id.clone();
    let started = Instant::now();

    // Per-feature worktree isolation. Best-effort: if `git worktree add`
    // fails (e.g. workspace is not a git repo, or the feature dir is in use)
    // we fall back to running directly inside the feature_dir. Either way
    // the spec field we pass to Worker is the actual on-disk path.
    let worktree = maybe_create_worktree(workspace, &feature_id, &feature.feature_dir).await;
    let effective_feature_dir = worktree
        .as_ref()
        .map(|w| w.path.clone())
        .unwrap_or_else(|| feature.feature_dir.clone());

    let spec = crate::artifact::FeatureSpec {
        feature_dir: effective_feature_dir,
        worker_identity: worker_identity.clone(),
        dispatch_metadata: serde_yaml::Value::Null,
    };

    let result = worker.run(spec, cancel).await;

    if let Some(w) = worktree {
        // F-004: propagate worker commits to a named ref BEFORE cleanup
        // destroys the worktree. Gated on (a) worker returned Ok, (b) verdict
        // was Pass. `propagate_worktree_commits` itself handles the
        // HEAD-change / semantic-verify / shallow-clone skip-guards.
        //
        // Ordering constraint: this call MUST happen INSIDE the
        // `if let Some(w)` block (so `w.path` is still valid) and BEFORE
        // `w.cleanup().await` (which move-consumes the Worktree). It MUST
        // also stay BEFORE the `match result` block below (line 193+) which
        // move-consumes `feature_id` into FeatureOutcome.
        if let Ok(artifact) = result.as_ref() {
            if matches!(artifact.verdict, crate::artifact::WorkerVerdict::Pass) {
                propagate_worktree_commits(&w, &feature_id).await;
            }
        }
        w.cleanup().await;
    }

    let outcome = match result {
        Ok(artifact) => {
            // F-010: the operator-facing verdict is the AE review judgment, read
            // from the MAIN-TREE review.md (the `feature.feature_dir` captured
            // pre-worktree). F-008's feature-scoped symlink guarantees the
            // worker's in-worktree write landed at this inode and survives the
            // `w.cleanup()` above, so reading post-cleanup is safe.
            let review_path = feature.feature_dir.join("review.md");
            let verdict = match parse_review_once(&review_path) {
                Some(AeVerdict::Pass) => "pass".to_string(),
                Some(AeVerdict::Fail) => "fail".to_string(),
                None => {
                    warn!(
                        feature_id = %feature_id,
                        "no readable review.md verdict; dispatch.log verdict=unknown"
                    );
                    "unknown".to_string()
                }
            };
            FeatureOutcome {
                feature_id,
                worker_identity,
                verdict,
                worker_exit_status: artifact_verdict_str(&artifact),
                exit_code: artifact.exit_code,
                duration_ms: artifact.duration.as_millis(),
                stdout_path: artifact.stdout_path,
                drain_truncated: artifact.drain_truncated,
                error: None,
            }
        }
        Err(e) => FeatureOutcome {
            feature_id,
            worker_identity,
            verdict: "unknown".into(),
            worker_exit_status: "error".into(),
            exit_code: -1,
            duration_ms: started.elapsed().as_millis(),
            stdout_path: PathBuf::new(),
            drain_truncated: false,
            error: Some(format!("{e:#}")),
        },
    };
    Ok(outcome)
}

fn artifact_verdict_str(a: &Artifact) -> String {
    use crate::artifact::WorkerVerdict::*;
    match a.verdict {
        Pass => "pass".into(),
        Fail => "fail".into(),
        Timeout => "timeout".into(),
        Cancelled => "cancelled".into(),
    }
}

/// Per-feature git worktree. `cleanup` removes the worktree via
/// `git worktree remove --force`.
struct Worktree {
    path: PathBuf,
    workspace: PathBuf,
    /// F-004: HEAD SHA captured at `git worktree add` time. The propagation
    /// step (Strategy A: post-hoc `git update-ref`) compares this against
    /// the worktree's final HEAD to skip the no-commit case — without it
    /// we'd write a rescue ref that points at the initial commit and
    /// pretends a no-op worker produced output.
    initial_sha: String,
}

impl Worktree {
    async fn cleanup(self) {
        let status = tokio::process::Command::new("git")
            .args([
                "worktree",
                "remove",
                "--force",
                &self.path.to_string_lossy(),
            ])
            .current_dir(&self.workspace)
            .status()
            .await;
        match status {
            Ok(s) if s.success() => {}
            Ok(s) => warn!(status = ?s, path = %self.path.display(), "worktree cleanup non-zero"),
            Err(e) => {
                warn!(error = %e, path = %self.path.display(), "worktree cleanup spawn failed")
            }
        }
    }
}

/// Parse a `.loom/worktrees/` directory basename of the shape
/// `<feature_id>-<pid>` back into its parts.
///
/// Returns `None` (⇒ caller leaves the dir untouched) unless the basename
/// splits, on its LAST `-`, into a valid feature id (reusing the Step-1
/// `validate_feature_id` allowlist) and a non-empty all-digit non-zero pid.
/// `rsplit_once` isolates the pid correctly even for hyphen-heavy ids
/// (`F-006-some-slug-12345` → `("F-006-some-slug", 12345)`); the prefix
/// validation is what prevents force-removing a stray dir like
/// `not-a-feature-12345`.
fn parse_worktree_dir_name(name: &str) -> Option<(&str, u32)> {
    let (feature_id, pid_str) = name.rsplit_once('-')?;
    crate::feature_id::validate_feature_id(feature_id).ok()?;
    if pid_str.is_empty() || !pid_str.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let pid: u32 = pid_str.parse().ok()?;
    if pid == 0 {
        return None;
    }
    Some((feature_id, pid))
}

/// Liveness probe used to decide whether a PID-tagged worktree is reclaimable.
///
/// Gates the irreversible `git worktree remove --force`, so it MUST fail toward
/// "alive": only `ESRCH` (no such process) counts as dead. In particular
/// `EPERM` (the pid exists but is owned by another uid) is treated as ALIVE —
/// removing a live process's worktree is unrecoverable, whereas keeping an
/// orphan merely lets it accumulate (harmless under the single-orchestrator
/// model the `LOOM_PARENT_PID` recursion guard enforces; multi-process
/// coordination is deferred to v0.2). PID reuse can likewise yield a false
/// "alive" — same harmless-accumulation outcome.
#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    // SAFETY: `kill` with signal 0 performs the permission/existence checks
    // without delivering a signal and touches no memory. Pure liveness probe.
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if rc == 0 {
        return true;
    }
    // rc == -1: dead only when errno is ESRCH; EPERM and anything else ⇒ alive.
    !matches!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    )
}

/// Non-unix fallback: never classify a process as dead, so startup cleanup
/// never reclaims anything (conservative; the worktree flow is unix-only for
/// v0.1 — Windows tracked by BL-008 / BL-012).
#[cfg(not(unix))]
fn process_is_alive(_pid: u32) -> bool {
    true
}

/// Reclaim orphan worktrees left under `<workspace>/.loom/worktrees/` by a
/// prior `loom` process that died (SIGKILL / OOM / panic) before
/// `Worktree::cleanup` could run. Best-effort, warn-and-continue: any single
/// failure logs and the loop proceeds to the remaining entries.
///
/// `is_alive` is injected so tests can drive the dead/alive decision
/// deterministically instead of depending on real OS PID state.
pub async fn prune_stale_worktrees_with<F: Fn(u32) -> bool>(
    workspace: &std::path::Path,
    is_alive: F,
) {
    let wt_root = workspace.join(".loom").join("worktrees");
    let entries = match std::fs::read_dir(&wt_root) {
        Ok(e) => e,
        Err(_) => return, // no worktrees dir yet → nothing to reclaim
    };
    // Canonical root for the escape check below — resolves the macOS
    // `/tmp` → `/private/tmp` symlink (and any other) so the per-entry
    // comparison is apples-to-apples. If the root itself can't be
    // canonicalized we cannot prove containment for anything, so bail.
    let wt_root_canon = match std::fs::canonicalize(&wt_root) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, path = %wt_root.display(), "prune: cannot canonicalize worktrees root; skipping");
            return;
        }
    };
    let self_pid = std::process::id();
    let mut removed_any = false;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue; // non-UTF-8 name → leave untouched
        };
        let Some((_feature_id, pid)) = parse_worktree_dir_name(name) else {
            continue; // not a <feature_id>-<pid> worktree dir → leave untouched
        };
        if pid == self_pid || is_alive(pid) {
            continue; // our own (defensive) or a live process → preserve
        }
        // Defense-in-depth: never run remove on a path that escapes the
        // worktrees root. Compare CANONICAL paths (not lexical `starts_with`,
        // which a symlink'd entry would defeat) and fail closed if the entry
        // cannot be canonicalized (Codex accumulated-checkpoint finding).
        match std::fs::canonicalize(&path) {
            Ok(real) if real.starts_with(&wt_root_canon) => {}
            Ok(_) => {
                warn!(path = %path.display(), "prune: refusing entry that escapes .loom/worktrees (symlink?)");
                continue;
            }
            Err(e) => {
                warn!(error = %e, path = %path.display(), "prune: cannot canonicalize entry; skipping");
                continue;
            }
        }
        let status = tokio::process::Command::new("git")
            .args(["worktree", "remove", "--force", &path.to_string_lossy()])
            .current_dir(workspace)
            .status()
            .await;
        match status {
            Ok(s) if s.success() => {
                removed_any = true;
                info!(path = %path.display(), pid, "prune: reclaimed stale worktree (dead pid)");
            }
            Ok(s) => {
                warn!(status = ?s, path = %path.display(), "prune: worktree remove non-zero (continuing)")
            }
            Err(e) => {
                warn!(error = %e, path = %path.display(), "prune: worktree remove spawn failed (continuing)")
            }
        }
    }
    if removed_any {
        // Clear any now-dangling `.git/worktrees/` admin entries left behind.
        let _ = tokio::process::Command::new("git")
            .args(["worktree", "prune"])
            .current_dir(workspace)
            .status()
            .await;
    }
}

/// Startup entry point: prune stale worktrees using the real OS liveness probe.
pub async fn prune_stale_worktrees(workspace: &std::path::Path) {
    prune_stale_worktrees_with(workspace, process_is_alive).await;
}

/// F-008 (BL-021): symlink the dispatched feature's main-tree dir into the
/// worktree at `<wt>/.ae/features/active/<slug>` so the worker — whose cwd is
/// the worktree root — resolves exactly its one plan via `/ae:work`'s
/// `.ae/features/active/F-*/plan.md` glob, and so its `review.md`/`index.md`
/// writes land at the main-tree inode the verdict watcher + scans see (and
/// survive `git worktree remove --force`, verified empirically). Feature-scoped
/// (a single dir, never the whole `.ae/`) so sibling features don't leak into
/// the glob. Best-effort: any failure logs and the worktree is still usable for
/// source isolation. F-004 is untouched — source commits hit the worktree HEAD;
/// this gitignored symlink under `.loom/` is invisible to
/// `propagate_worktree_commits`. (Stdout re-home → BL-026.)
#[cfg(unix)]
fn link_feature_dir_into_worktree(wt_path: &std::path::Path, feature_dir: &std::path::Path) {
    // Target MUST be absolute — a relative symlink from `<wt>/.ae/features/active/`
    // would resolve to the wrong place. `canonicalize` doubles as the
    // target-exists guard: if the main-tree feature dir is gone, skip rather
    // than create a dangling link.
    let target = match std::fs::canonicalize(feature_dir) {
        Ok(t) => t,
        Err(e) => {
            warn!(error = %e, feature_dir = %feature_dir.display(), "worktree symlink: target dir missing/uncanonicalizable; skipping (worker runs without .ae visibility)");
            return;
        }
    };
    // Link basename = the SLUGGED main-tree dir name (`F-NNN-<slug>`), NOT the
    // bare feature_id — the glob matches `F-*` and the slugged dir holds the plan.
    let Some(name) = feature_dir.file_name() else {
        warn!(feature_dir = %feature_dir.display(), "worktree symlink: feature_dir has no basename; skipping");
        return;
    };
    let active = wt_path.join(".ae").join("features").join("active");
    if let Err(e) = std::fs::create_dir_all(&active) {
        warn!(error = %e, "worktree symlink: cannot create .ae/features/active in worktree; skipping");
        return;
    }
    let link = active.join(name);
    // Idempotent create: replace a stale link from a prior run. Tolerate
    // NotFound; any other remove error (e.g. `link` is a directory from a stray
    // checkout) → skip rather than blow up.
    match std::fs::remove_file(&link) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            warn!(error = %e, path = %link.display(), "worktree symlink: cannot clear existing path (not a regular file?); skipping");
            return;
        }
    }
    if let Err(e) = std::os::unix::fs::symlink(&target, &link) {
        warn!(error = %e, link = %link.display(), target = %target.display(), "worktree symlink: create failed; worker runs without .ae visibility");
    }
}

/// Non-unix: artifact bridging deferred (copy-back) — see BL-026 follow-up.
/// The worktree is still created; the worker just lacks `.ae/` visibility on
/// platforms without POSIX symlinks (Windows tracked by BL-008 / BL-012).
#[cfg(not(unix))]
fn link_feature_dir_into_worktree(_wt_path: &std::path::Path, _feature_dir: &std::path::Path) {}

async fn maybe_create_worktree(
    workspace: &std::path::Path,
    feature_id: &str,
    feature_dir: &std::path::Path,
) -> Option<Worktree> {
    // v0.1: derive a sibling worktree path under `<workspace>/.loom/worktrees/`.
    // We always check out HEAD detached so we don't conflict with the main
    // branch. Real branching belongs to v0.2+ once we wire git ops properly.
    //
    // Known v0.0.x deviation from plan: the plan checklist says
    // "git worktree add <path> <branch>", but v0.0.x uses `--detach HEAD`.
    // Workers DO commit (verified F-SMOKE 2026-05-22, commit landed on the
    // detached HEAD) — but those commits become dangling after
    // `worktree remove --force` runs in cleanup, because no ref reaches them.
    // BL-014 (worktree → main commit propagation) tracks the per-feature
    // branch fix; until that lands, treat dangling commits as expected for
    // best-effort v0.0.x scope (suitable for stub features; not yet for real
    // multi-commit feature work).
    let wt_root = workspace.join(".loom").join("worktrees");
    if let Err(e) = std::fs::create_dir_all(&wt_root) {
        warn!(error = %e, "worktree: skipping (cannot create .loom/worktrees)");
        return None;
    }
    let wt_path = wt_root.join(format!("{}-{}", feature_id, std::process::id()));

    let status = tokio::process::Command::new("git")
        .args([
            "worktree",
            "add",
            "--detach",
            &wt_path.to_string_lossy(),
            "HEAD",
        ])
        .current_dir(workspace)
        .status()
        .await;
    match status {
        Ok(s) if s.success() => {
            // F-004: capture HEAD SHA right after `git worktree add` succeeds.
            // The propagation step (Strategy A) compares this against the
            // worktree's final HEAD to detect "did the worker make any
            // commits?" before writing a refs/heads/loom-features/F-NNN
            // rescue ref. Fail closed: if rev-parse can't give us a SHA we
            // tear down the half-built worktree and fall back to the
            // feature_dir path, so the propagation precondition (we have an
            // initial SHA to compare against) always holds for live Worktrees.
            let rev_parse = tokio::process::Command::new("git")
                .args(["-C", &wt_path.to_string_lossy(), "rev-parse", "HEAD"])
                .output()
                .await;
            let initial_sha: Option<String> = match rev_parse {
                Ok(o) if o.status.success() => match String::from_utf8(o.stdout) {
                    Ok(s) => Some(s.trim().to_string()),
                    Err(e) => {
                        warn!(error = %e, "worktree: rev-parse stdout not UTF-8");
                        None
                    }
                },
                Ok(o) => {
                    warn!(status = ?o.status, "worktree: rev-parse HEAD non-zero");
                    None
                }
                Err(e) => {
                    warn!(error = %e, "worktree: rev-parse spawn failed");
                    None
                }
            };
            match initial_sha {
                Some(initial_sha) => {
                    // F-008: symlink the dispatched feature's gitignored .ae/ dir
                    // into the worktree so the worker (cwd = worktree root)
                    // resolves its plan and its verdict reaches the main-tree
                    // watcher. Feature-scoped (one dir) so /ae:work's
                    // `.ae/features/active/F-*` glob resolves exactly one plan.
                    // Best-effort; F-004 untouched (source commits hit the
                    // worktree HEAD; this gitignored symlink under .loom/ is
                    // invisible to propagate_worktree_commits).
                    link_feature_dir_into_worktree(&wt_path, feature_dir);
                    Some(Worktree {
                        path: wt_path,
                        workspace: workspace.to_path_buf(),
                        initial_sha,
                    })
                }
                None => {
                    // Roll back the half-constructed worktree so we don't
                    // leak `.loom/worktrees/<id>-<pid>` directories on
                    // rev-parse failure. Mirrors `Worktree::cleanup`'s
                    // warn-and-continue pattern — surface rollback failure
                    // in the log so an orphaned worktree doesn't disappear
                    // silently into `.git/worktrees/` admin metadata.
                    let rollback = tokio::process::Command::new("git")
                        .args(["worktree", "remove", "--force", &wt_path.to_string_lossy()])
                        .current_dir(workspace)
                        .status()
                        .await;
                    match rollback {
                        Ok(s) if s.success() => {}
                        Ok(s) => {
                            warn!(status = ?s, path = %wt_path.display(), "worktree: rollback non-zero — orphaned worktree may leak")
                        }
                        Err(e) => {
                            warn!(error = %e, path = %wt_path.display(), "worktree: rollback spawn failed — orphaned worktree may leak")
                        }
                    }
                    None
                }
            }
        }
        Ok(s) => {
            warn!(status = ?s, "worktree: git worktree add non-zero — running in feature_dir");
            None
        }
        Err(e) => {
            warn!(error = %e, "worktree: git spawn failed — running in feature_dir");
            None
        }
    }
}

/// F-004: write a `refs/heads/loom-features/<feature_id>` ref pointing at
/// the worktree's final HEAD before `Worktree::cleanup` destroys it.
///
/// Best-effort, warn-and-continue: every failure path logs + returns
/// without bubbling up to the dispatch outcome. The caller already gates
/// on `WorkerVerdict::Pass`; this function adds the orthogonal hygiene
/// guards (HEAD changed from `initial_sha` regardless of ancestry,
/// captured SHA semantically names a commit, workspace is not a shallow
/// clone) before writing.
///
/// Re-dispatches silently overwrite by design (Topic 2 in conclusion.md);
/// `--create-reflog` keeps the previous SHA recoverable for the window
/// configured by `gc.reflogExpire` (default 90 days). The overwrite event
/// is surfaced in the log so operators reading `.loom/run-*.log` see when
/// a prior SHA was replaced.
async fn propagate_worktree_commits(worktree: &Worktree, feature_id: &str) {
    let wt_path = worktree.path.to_string_lossy();
    let workspace = worktree.workspace.to_string_lossy();
    let ref_name = format!("refs/heads/loom-features/{}", feature_id);

    // 1. Capture worktree's final HEAD SHA.
    let final_out = tokio::process::Command::new("git")
        .args(["-C", &wt_path, "rev-parse", "HEAD"])
        .output()
        .await;
    let final_sha = match final_out {
        Ok(o) if o.status.success() => match String::from_utf8(o.stdout) {
            Ok(s) => s.trim().to_string(),
            Err(e) => {
                warn!(error = %e, feature_id, path = %worktree.path.display(), "propagation: final rev-parse stdout not UTF-8");
                return;
            }
        },
        Ok(o) => {
            warn!(status = ?o.status, feature_id, path = %worktree.path.display(), "propagation: final rev-parse HEAD non-zero");
            return;
        }
        Err(e) => {
            warn!(error = %e, feature_id, path = %worktree.path.display(), "propagation: final rev-parse spawn failed");
            return;
        }
    };

    // 2. Zero-commit guard. Worker spawned but never advanced HEAD → no
    //    rescue ref needed (and we don't want to point at the initial
    //    commit and pretend a no-op worker produced output).
    if final_sha == worktree.initial_sha {
        tracing::debug!(feature_id, sha = %final_sha, "propagation: no commits made, skipping");
        return;
    }

    // 3. Semantic SHA guard. Defense against a `rev-parse HEAD` that
    //    succeeds with garbage stdout (unlikely on git ≥ 1.8 but cheap to
    //    verify). `<sha>^{commit}` resolves the SHA as a commit object.
    let verify = tokio::process::Command::new("git")
        .args([
            "-C",
            &wt_path,
            "rev-parse",
            "--verify",
            "--quiet",
            &format!("{}^{{commit}}", final_sha),
        ])
        .status()
        .await;
    match verify {
        Ok(s) if s.success() => {}
        Ok(s) => {
            warn!(status = ?s, feature_id, sha = %final_sha, "propagation: final SHA failed semantic verify; skipping");
            return;
        }
        Err(e) => {
            warn!(error = %e, feature_id, sha = %final_sha, "propagation: semantic verify spawn failed; skipping");
            return;
        }
    }

    // 4. Shallow-clone guard. On a shallow workspace, the worker's commit
    //    may sit at the shallow boundary and the ref would later break
    //    `git merge loom-features/F-NNN`. Best-effort: if the check itself
    //    fails (no git, bad shell) we proceed — the rescue ref is still
    //    better than losing the commit to GC.
    let shallow = tokio::process::Command::new("git")
        .args(["-C", &workspace, "rev-parse", "--is-shallow-repository"])
        .output()
        .await;
    if let Ok(o) = &shallow {
        if o.status.success() {
            if let Ok(s) = std::str::from_utf8(&o.stdout) {
                if s.trim() == "true" {
                    warn!(feature_id, workspace = %worktree.workspace.display(), "propagation: workspace is a shallow clone; skipping rescue ref to avoid pointing into shallow boundary");
                    return;
                }
            }
        }
    }

    // 5. Overwrite detection. If a prior dispatch wrote this same ref to a
    //    DIFFERENT SHA, surface that fact in the log so operators reading
    //    `.loom/run-*.log` can recover the prior SHA via reflog. We do
    //    not refuse the overwrite — Topic 2 explicitly chose silent
    //    overwrite as the v0.0.x policy.
    let prior = tokio::process::Command::new("git")
        .args([
            "-C",
            &workspace,
            "rev-parse",
            "--verify",
            "--quiet",
            &ref_name,
        ])
        .output()
        .await;
    if let Ok(o) = &prior {
        if o.status.success() {
            if let Ok(prior_str) = std::str::from_utf8(&o.stdout) {
                let prior_sha = prior_str.trim();
                if !prior_sha.is_empty() && prior_sha != final_sha {
                    info!(
                        feature_id,
                        prior_sha,
                        final_sha = %final_sha,
                        ref_name = %ref_name,
                        "propagation: ref overwriting prior dispatch's SHA — recover via: git reflog show {}",
                        ref_name
                    );
                }
            }
        }
    }

    // 6. Write the rescue ref. `--create-reflog` enables operator recovery
    //    of overwritten SHAs even when `core.logAllRefUpdates = false`.
    let write = tokio::process::Command::new("git")
        .args([
            "-C",
            &workspace,
            "update-ref",
            "--create-reflog",
            &ref_name,
            &final_sha,
        ])
        .status()
        .await;
    match write {
        Ok(s) if s.success() => {
            info!(feature_id, sha = %final_sha, ref_name = %ref_name, "propagation: rescue ref written");
        }
        Ok(s) => {
            warn!(status = ?s, feature_id, sha = %final_sha, ref_name = %ref_name, "propagation: update-ref non-zero");
        }
        Err(e) => {
            warn!(error = %e, feature_id, ref_name = %ref_name, "propagation: update-ref spawn failed");
        }
    }
}

// Suppress unused-Duration import warning on cfg paths.
#[allow(dead_code)]
fn _force_duration_use() -> Duration {
    Duration::from_secs(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::DiscoveredFeature;

    fn feat(id: &str, deps: &[&str], done: bool) -> DiscoveredFeature {
        DiscoveredFeature {
            id: id.into(),
            feature_dir: PathBuf::from(format!(".ae/features/active/{id}")),
            depends_on: deps.iter().map(|s| s.to_string()).collect(),
            work_state: if done { Some("done".into()) } else { None },
        }
    }

    #[test]
    fn parse_worktree_dir_name_round_trips_real_dispatch_names() {
        // Dispatch builds `format!("{}-{}", feature_id, pid)`. Confirm the
        // reverse parse isolates the pid even for slugged ids, and that a
        // tightened-grammar-valid slug (F-007 BL-024) still round-trips —
        // the path e2e worktree tests don't exercise (all use slugless ids).
        assert_eq!(
            parse_worktree_dir_name("F-006-12345"),
            Some(("F-006", 12345))
        );
        assert_eq!(
            parse_worktree_dir_name("F-006-some-slug-12345"),
            Some(("F-006-some-slug", 12345))
        );
        assert_eq!(
            parse_worktree_dir_name("F-006-a-b-c-99"),
            Some(("F-006-a-b-c", 99))
        );
        // Unparseable / non-feature names → None (left untouched by prune).
        assert_eq!(parse_worktree_dir_name("garbage"), None);
        assert_eq!(parse_worktree_dir_name("not-a-feature-1"), None);
        // Zero / non-numeric pid → None.
        assert_eq!(parse_worktree_dir_name("F-006-0"), None);
        assert_eq!(parse_worktree_dir_name("F-006-abc"), None);
    }

    #[test]
    fn ready_set_filters_done_and_pending_deps() {
        let f = vec![
            feat("F-001", &[], true),         // done
            feat("F-002", &["F-001"], false), // ready (dep done)
            feat("F-003", &["F-002"], false), // not ready (dep not done)
            feat("F-004", &[], false),        // ready (no deps)
        ];
        let r = ready_set(&f);
        let ids: Vec<&str> = r.iter().map(|d| d.id.as_str()).collect();
        assert!(ids.contains(&"F-002"));
        assert!(ids.contains(&"F-004"));
        assert!(!ids.contains(&"F-001"));
        assert!(!ids.contains(&"F-003"));
    }

    // ---- F-008: feature-scoped .ae symlink into the worktree ----
    // Unix-gated to match the `link_feature_dir_into_worktree` cfg-split — these
    // exercise POSIX symlink APIs and would break a non-unix `cargo test` build.

    #[cfg(unix)]
    fn git(ws: &std::path::Path, args: &[&str]) {
        let st = std::process::Command::new("git")
            .args(args)
            .current_dir(ws)
            .status()
            .unwrap_or_else(|e| panic!("git {args:?}: {e}"));
        assert!(st.success(), "git {args:?} non-zero");
    }

    /// AC1: feature-scoped symlink uses the SLUGGED dir name (not the bare id),
    /// resolves to the main-tree inode, write-through reaches main and survives
    /// `git worktree remove --force`. Mismatched id (`F-999`) vs dir
    /// (`F-999-test-slugged-dir`) guards the slug-not-id rule from drift.
    #[cfg(unix)]
    #[tokio::test]
    async fn maybe_create_worktree_symlinks_feature_dir_feature_scoped() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        git(ws, &["init", "-q"]);
        git(ws, &["config", "user.email", "t@loom"]);
        git(ws, &["config", "user.name", "t"]);
        std::fs::write(ws.join(".gitignore"), ".ae/\n.loom/\n").unwrap();
        git(ws, &["add", ".gitignore"]);
        git(ws, &["commit", "-q", "-m", "init"]);

        // Main-tree feature dir: id F-999 but SLUGGED dir name (untracked, gitignored).
        let feat = ws.join(".ae/features/active/F-999-test-slugged-dir");
        std::fs::create_dir_all(&feat).unwrap();
        std::fs::write(feat.join("index.md"), "---\nid: F-999\n---\n").unwrap();
        std::fs::write(feat.join("plan.md"), "plan").unwrap();

        let wt = maybe_create_worktree(ws, "F-999", &feat)
            .await
            .expect("worktree created");

        let active = wt.path.join(".ae/features/active");
        let link = active.join("F-999-test-slugged-dir");
        // (a) link exists as a symlink under the SLUGGED name
        assert!(
            std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink(),
            "slugged-name symlink must exist"
        );
        // (b) the BARE-id name must NOT exist (proves basename came from feature_dir.file_name())
        assert!(
            !active.join("F-999").exists(),
            "bare-id link must not exist (slug-not-id rule)"
        );
        // (c) canonical target == canonical main-tree feature dir (both canonicalized — macOS /tmp)
        assert_eq!(
            std::fs::canonicalize(&link).unwrap(),
            std::fs::canonicalize(&feat).unwrap()
        );
        // single child → /ae:work glob resolves exactly one plan
        assert_eq!(std::fs::read_dir(&active).unwrap().count(), 1);

        // write-through lands in the main tree and survives cleanup
        std::fs::write(link.join("review.md"), "verdict: pass\n").unwrap();
        assert!(
            feat.join("review.md").exists(),
            "write-through reaches main tree"
        );
        wt.cleanup().await;
        assert!(
            feat.join("review.md").exists(),
            "main-tree artifact must survive git worktree remove --force"
        );
    }

    /// AC2: idempotent create (stale link replaced) + target-missing fallback
    /// (no dangling link) + directory-in-path is skipped. Exercises the helper
    /// directly — no git needed, and avoids the pid-derived worktree-path reuse
    /// that would block calling maybe_create_worktree twice in one process.
    #[cfg(unix)]
    #[test]
    fn link_feature_dir_into_worktree_idempotent_and_guards() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let wt = root.join("wt");
        let main_feat = root.join(".ae/features/active/F-100-demo");
        std::fs::create_dir_all(&main_feat).unwrap();
        std::fs::create_dir_all(wt.join(".ae/features/active")).unwrap();
        let link = wt.join(".ae/features/active/F-100-demo");

        // Idempotency: pre-place a STALE symlink pointing elsewhere.
        let bogus = root.join("bogus-old-target");
        std::fs::create_dir_all(&bogus).unwrap();
        std::os::unix::fs::symlink(&bogus, &link).unwrap();
        link_feature_dir_into_worktree(&wt, &main_feat);
        assert_eq!(
            std::fs::canonicalize(&link).unwrap(),
            std::fs::canonicalize(&main_feat).unwrap(),
            "stale link must be replaced to point at the correct main-tree dir"
        );

        // Fallback: missing target → NO symlink created (no dangling link).
        let wt2 = root.join("wt2");
        std::fs::create_dir_all(wt2.join(".ae/features/active")).unwrap();
        let missing = root.join(".ae/features/active/F-404-gone");
        link_feature_dir_into_worktree(&wt2, &missing);
        assert!(
            std::fs::symlink_metadata(wt2.join(".ae/features/active/F-404-gone")).is_err(),
            "no link should be created when the target is missing"
        );

        // Directory-in-path guard: a real directory (not a symlink) already at
        // the link path is left intact — `remove_file` can't unlink a dir, so
        // the helper warns and skips rather than clobbering it. (Closes the
        // coverage the docstring claims; F-008 review.)
        let wt3 = root.join("wt3");
        let occupied = wt3.join(".ae/features/active/F-100-demo");
        std::fs::create_dir_all(&occupied).unwrap();
        std::fs::write(occupied.join("sentinel"), "keep").unwrap();
        link_feature_dir_into_worktree(&wt3, &main_feat);
        assert!(
            !std::fs::symlink_metadata(&occupied)
                .unwrap()
                .file_type()
                .is_symlink()
                && occupied.is_dir(),
            "a real directory at the link path must be left intact (skip, not clobber)"
        );
        assert!(
            occupied.join("sentinel").exists(),
            "directory contents must be preserved on skip"
        );
    }
}
