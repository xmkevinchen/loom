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
#[derive(Debug, Serialize)]
pub struct FeatureOutcome {
    pub feature_id: String,
    pub worker_identity: String,
    pub verdict: String,
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
                    verdict: "error".into(),
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
                    verdict: "panic".into(),
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
    let worktree = maybe_create_worktree(workspace, &feature_id).await;
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
        Ok(artifact) => FeatureOutcome {
            feature_id,
            worker_identity,
            verdict: artifact_verdict_str(&artifact),
            exit_code: artifact.exit_code,
            duration_ms: artifact.duration.as_millis(),
            stdout_path: artifact.stdout_path,
            drain_truncated: artifact.drain_truncated,
            error: None,
        },
        Err(e) => FeatureOutcome {
            feature_id,
            worker_identity,
            verdict: "error".into(),
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

async fn maybe_create_worktree(workspace: &std::path::Path, feature_id: &str) -> Option<Worktree> {
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
                Some(initial_sha) => Some(Worktree {
                    path: wt_path,
                    workspace: workspace.to_path_buf(),
                    initial_sha,
                }),
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
}
