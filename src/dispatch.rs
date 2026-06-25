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
use crate::discovery::{read_done_features, DiscoveredFeature};
use crate::verdict::{parse_review_once, AeVerdict};
use crate::worker::Worker;
use anyhow::{Context, Result};
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
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
    /// F-018: the ref this run wrote — `refs/heads/loom-features/<id>` for a
    /// clean pass (merge candidate) or `refs/heads/loom-rescue/<id>-<status>`
    /// for a non-pass worker whose worktree HEAD advanced (survival-only).
    /// `None` when no ref was written (no worktree, no commits, or a guard
    /// skip). Surfaced in the dispatch log so an operator sees where a
    /// timed-out/failed worker's committed work was preserved.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rescue_ref: Option<String>,
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

/// Build the scheduling view that credits archived-done dependencies (F-017 /
/// BL-022). Returns `active` extended with each `done/` feature that should
/// count toward `ready_set`'s `done_ids` — i.e. every archived feature EXCEPT:
///   - one whose `id` is also present as an INCOMPLETE (`!is_done`) feature in
///     `active` — the in-flight active copy wins (active-presence suppression,
///     mirrors `/ae:roadmap`'s active-PREEMPTS-done); and
///   - one whose `done/<dir>/review.md` is readable AND says `verdict: fail`
///     (fail-guard against AE mis-archiving a non-pass; a missing/unreadable/
///     `pass` review still credits, so legit-done features without a review.md
///     are not fail-closed).
///
/// Done features carry `work_state == Some("done")` (forced by
/// [`read_done_features`]) so `ready_set`'s `!is_done()` filter keeps them out of
/// the dispatch set — they only contribute to `done_ids`.
///
/// Best-effort: a `read_done_features` error logs a `warn!` and yields
/// active-only credit for this cycle (never aborts the dispatch).
pub fn done_credited_view(
    active: Vec<DiscoveredFeature>,
    workspace: &std::path::Path,
) -> Vec<DiscoveredFeature> {
    use std::collections::HashSet;
    let active_incomplete: HashSet<&str> = active
        .iter()
        .filter(|f| !f.is_done())
        .map(|f| f.id.as_str())
        .collect();

    let done = match read_done_features(workspace) {
        Ok(d) => d,
        Err(e) => {
            warn!(error = %e, "done-credit: read_done_features failed; crediting active features only this cycle");
            Vec::new()
        }
    };

    // Fail-guard is PER-ID, not per-directory (review F-017: a duplicate-id
    // anomaly — e.g. done/F-001-v1 fail + done/F-001-v2 missing-review — must
    // not let the review-less copy mask the failed one). Collect every id that
    // has ANY archived copy with a `verdict: fail` review, then suppress credit
    // for that id entirely. Owned `String` set so `done` can be consumed below.
    let fail_ids: HashSet<String> = done
        .iter()
        .filter(|d| {
            matches!(
                parse_review_once(&d.feature_dir.join("review.md")),
                Some(AeVerdict::Fail)
            )
        })
        .map(|d| d.id.clone())
        .collect();

    let mut credited: Vec<DiscoveredFeature> = Vec::new();
    for d in done {
        if active_incomplete.contains(d.id.as_str()) {
            warn!(feature_id = %d.id, "done-credit: id is live (incomplete) in active/; suppressing the archived copy's credit");
            continue;
        }
        if fail_ids.contains(&d.id) {
            warn!(feature_id = %d.id, "done-credit: an archived copy of this id has review verdict fail; not crediting (per-id fail-guard)");
            continue;
        }
        credited.push(d);
    }

    let mut view = active;
    view.extend(credited);
    view
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
                    rescue_ref: None,
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
                    rescue_ref: None,
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

/// F-016: read the AE review verdict from wherever the `/ae:review` archive `mv`
/// actually landed it — a 4-site, first-hit-wins probe across the worktree
/// cleanup boundary (conclusion D2). The archive is LLM-executed and lands
/// non-deterministically (D1):
///   A  main-tree active/   (relative mv ENOENT-failed → review.md never moved)
///   B  worktree-local done/ (cleanup-destroyed → MUST be read before w.cleanup())
///   C  main-tree done/      (absolute-path landing; F-015's case)
///
/// Probe order (first hit wins):
///   1. `main_feature_dir/review.md`                           — A (and the
///      F-008 symlink/main-active write-through; survives cleanup)
///   2. `<wt>/.ae/features/active/<basename>/review.md`        — B, wt-active
///      `<wt>/.ae/features/done/<basename>/review.md`          — B, wt-local done/
///   3. `<workspace>/.ae/features/done/<basename>/review.md`   — C, GUARDED
///
/// The worktree arms (2) are unguarded: single-writer, this-run, structurally
/// immune to a stale leftover. Probe C (3) is the ONE shared, persistent path —
/// a `done/` review.md lives forever and the dominant workflow re-activates a
/// feature for fixup — so it carries an mtime freshness guard
/// `mtime >= dispatch_started`. That guard plus exact-basename scoping is the
/// SOLE defense against certifying a stale prior-cycle pass as a fresh one
/// (identity-scoping alone cannot distinguish a same-feature prior pass). Probe
/// C is exact-basename, never a `read_dir` scan. `basename` =
/// `main_feature_dir`'s final component (the slugged dir name `F-NNN-<slug>`).
///
/// Transitional until the AE-determinism BL collapses the probe set → 1
/// (companion AE-repo BL; conclusion D5). v0.2 coarse-FS / true-concurrent
/// residual on probe C → BL-040.
fn read_ae_verdict(
    main_feature_dir: &std::path::Path,
    wt_path: Option<&std::path::Path>,
    workspace: &std::path::Path,
    dispatch_started: std::time::SystemTime,
) -> Option<AeVerdict> {
    let basename = main_feature_dir.file_name();

    // Probe A — main-tree active/ (the F-008 symlink write-through inode;
    // survives `git worktree remove --force`). The common case: archive did not
    // run, or its relative mv ENOENT-failed and review.md never left active/.
    if let Some(v) = parse_review_once(&main_feature_dir.join("review.md")) {
        return Some(v);
    }

    if let Some(name) = basename {
        // Probe B — worktree-local. MUST be read before `w.cleanup()` destroys
        // the worktree. wt-active is the same inode as probe A via the F-008
        // symlink (belt-and-suspenders for the symlink-failed path); wt-local
        // done/ is where a trailing-slash-deref archive (landing B) lands.
        if let Some(wt) = wt_path {
            if let Some(v) =
                parse_review_once(&wt.join(".ae/features/active").join(name).join("review.md"))
            {
                return Some(v);
            }
            let wt_done = wt.join(".ae/features/done").join(name).join("review.md");
            if let Some(v) = parse_review_once(&wt_done) {
                // Heal-log (B2): verdict recovered from the worktree-local done/
                // landing — the archive moved it; we read it pre-cleanup.
                warn!(
                    feature_dir = %main_feature_dir.display(),
                    "F-016: verdict healed from worktree-local done/ (archive landing B)"
                );
                return Some(v);
            }
        }

        // Probe C — shared main-tree done/ (absolute-path landing C / F-015).
        // The ONLY persistent, shared probe → freshness-guarded. Path built from
        // `workspace` + literal `.ae/features/done` + basename (P3: unambiguous
        // from workspace, NOT a relative `../../` from feature_dir). A stale
        // prior-cycle review (mtime < dispatch_started) is SKIPPED — never
        // healed. Any metadata/clock error ⇒ stale/skip, never propagates as a
        // feature error (codex-C3). `>=` accepts a write landing exactly at
        // dispatch_started. (`is_some_and` is the clippy-clean equivalent of the
        // plan's `map_or(false, …)` guard expression — identical semantics.)
        // CAVEAT (BL-040a): on a coarse-granularity FS (HFS+ 1s mtime) the
        // inclusive `>=` admits a stale leftover written in the SAME wall-clock
        // second as dispatch_started — a bounded false-heal window. APFS/ext4
        // (ns resolution) are immune; HFS+ residual tracked in BL-040a.
        let p = workspace
            .join(".ae/features/done")
            .join(name)
            .join("review.md");
        let fresh = std::fs::metadata(&p)
            .ok()
            .and_then(|m| m.modified().ok())
            .is_some_and(|mt| mt >= dispatch_started);
        if fresh {
            if let Some(v) = parse_review_once(&p) {
                // Heal-log (B2, probe C): freshness-guarded heal from the shared
                // main-tree done/.
                warn!(
                    feature_dir = %main_feature_dir.display(),
                    "F-016: verdict healed from main-tree done/ (archive landing C; freshness-guarded)"
                );
                return Some(v);
            }
            // B2 (probe C): a FRESH done/ review.md is present (passed the
            // guard) but its verdict is not parseable (malformed/unclosed
            // frontmatter, `verdict: pending`). Distinguish this from
            // genuinely-missing — without this line the caller's None-arm logs
            // the generic "no readable review.md verdict", indistinguishable
            // from "no file anywhere", reintroducing the undiagnosable exit-8
            // this feature exists to kill. Falls through to None (no behavior
            // change); the verdict is intentionally NOT healed.
            warn!(
                feature_dir = %main_feature_dir.display(),
                path = %p.display(),
                "F-016: fresh main-tree done/ review.md present but verdict not parseable; NOT healing (falls through to missing)"
            );
        }
    }

    None
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
    // F-016 P1 (dispatch_started): freshness floor for the probe-C stale-leftover
    // guard. Captured LOCALLY at run_one_feature ENTRY, BEFORE `worker.run()`
    // below — any review THIS run writes has mtime ≥ it, while a stale
    // prior-cycle review (a full dispatch ago) has mtime < it.
    //
    // NOTE: this is a per-feature-invocation capture. It is equivalent to a
    // dispatch-time capture ONLY because the current model makes exactly one
    // sequential worker call per feature per cycle. If `run_one_feature` ever
    // becomes a retry-loop body, this MUST move to an outer-passed timestamp
    // (conclusion P1: thread `dispatch_started` from `run_dispatch_loop`) or the
    // guard window silently widens across retries. Consumed ONLY by probe C in
    // `read_ae_verdict`; the worktree arms are unguarded by design.
    let dispatch_started = std::time::SystemTime::now();

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

    // F-016 P2 (borrow/move order): `ae_verdict` is declared before the worktree
    // block so both arms (worktree present / absent) converge on it. In the
    // worktree arm the verdict is read AFTER `propagate_worktree_commits` and
    // BEFORE `w.cleanup()`: both borrow `&w`/`&w.path`, and cleanup
    // move-consumes `w`, so the read MUST precede it — probe B's worktree-local
    // done/ is destroyed by cleanup. Compile-enforced ordering.
    let ae_verdict: Option<AeVerdict>;
    let mut rescue_ref: Option<String> = None;
    if let Some(w) = worktree {
        // F-018 (was F-004 Pass-gated): propagate worker commits to a named ref
        // BEFORE cleanup destroys the worktree — UNCONDITIONALLY whenever a
        // worktree exists, regardless of Ok/Err AND regardless of verdict. The
        // verdict gates main-line MERGE, never commit survival (BL-041; F-005 Q1
        // established the rescue mechanism is permissive). The status string
        // selects the ref namespace: `pass` → merge-candidate
        // `loom-features/<id>`; any non-pass (`timeout`/`fail`/`cancelled`/
        // `error`) → survival-only `loom-rescue/<id>-<status>`.
        // `propagate_worktree_commits` handles the zero-commit / semantic-verify
        // / shallow-clone guards and returns the written ref name (or None).
        let status = match result.as_ref() {
            Ok(a) => match a.verdict {
                crate::artifact::WorkerVerdict::Pass => "pass",
                crate::artifact::WorkerVerdict::Timeout => "timeout",
                crate::artifact::WorkerVerdict::Fail => "fail",
                crate::artifact::WorkerVerdict::Cancelled => "cancelled",
            },
            Err(_) => "error",
        };
        rescue_ref = propagate_worktree_commits(&w, &feature_id, status).await;
        ae_verdict = read_ae_verdict(
            &feature.feature_dir,
            Some(&w.path),
            workspace,
            dispatch_started,
        );
        w.cleanup().await;
    } else {
        ae_verdict = read_ae_verdict(&feature.feature_dir, None, workspace, dispatch_started);
    }

    let outcome = match result {
        Ok(artifact) => {
            // F-010: the operator-facing verdict is the AE review judgment. F-016
            // reads it via `read_ae_verdict` (4-site probe) ABOVE, BEFORE
            // `w.cleanup()` — so the cleanup-destroyed worktree-local done/
            // landing (B) is still reachable and the non-deterministic archive mv
            // (landings A/B/C) is healed. The stored `ae_verdict` is consumed here.
            //
            // F-014 (REVERSES F-010's AC3 neutral path — refined Option B per the
            // ae:consensus verdict, .ae/analyses/001-consensus-f014-ac3-reversal-
            // scope.md): a CLEAN worker landing on the `None` branch gets
            // "missing" on Unix (→ EXIT_REVIEW_MISSING) — unconditionally, incl.
            // the best-effort-symlink failure path and no-worktree mode. Non-clean
            // workers and non-Unix keep "unknown" (crash already exits 4 via
            // worker_exit_status; non-Unix folds into BL-008).
            let verdict = match ae_verdict {
                Some(AeVerdict::Pass) => "pass".to_string(),
                Some(AeVerdict::Fail) => "fail".to_string(),
                None => {
                    let v = no_review_verdict(matches!(
                        artifact.verdict,
                        crate::artifact::WorkerVerdict::Pass
                    ));
                    warn!(
                        feature_id = %feature_id,
                        verdict = v,
                        "no readable review.md verdict; dispatch.log records it"
                    );
                    v.to_string()
                }
            };
            FeatureOutcome {
                feature_id,
                worker_identity,
                verdict,
                worker_exit_status: artifact_worker_status_str(&artifact),
                exit_code: artifact.exit_code,
                duration_ms: artifact.duration.as_millis(),
                stdout_path: artifact.stdout_path,
                drain_truncated: artifact.drain_truncated,
                error: None,
                rescue_ref,
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
            rescue_ref,
        },
    };
    Ok(outcome)
}

/// Map the worker's process-level `WorkerVerdict` to the `worker_exit_status`
/// string (F-010: this feeds the PROCESS field, not the AE `verdict`).
/// F-014: verdict string for the no-readable-TERMINAL-verdict case ("missing" =
/// absent file, unreadable, malformed, or `pending` — per BL-031's "no readable
/// review.md verdict"). `clean` = the worker process signal (WorkerVerdict::Pass),
/// NEVER an AE field. On Unix a clean worker with no terminal verdict is an
/// incomplete run → "missing" (→ EXIT_REVIEW_MISSING, refined Option B per the
/// ae:consensus verdict — this DELIBERATELY REVERSES F-010's AC3 neutral path,
/// unconditionally: symlink success/failure and no-worktree mode alike). Crash /
/// timeout / cancelled keep "unknown" on all platforms; non-Unix keeps "unknown"
/// for everything (exit 0; BL-008 platform umbrella).
fn no_review_verdict(clean: bool) -> &'static str {
    match (cfg!(unix), clean) {
        (true, true) => "missing",
        _ => "unknown",
    }
}

fn artifact_worker_status_str(a: &Artifact) -> String {
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
/// Best-effort, warn-and-continue: every failure path logs + returns `None`
/// without bubbling up to the dispatch outcome. F-018: called UNCONDITIONALLY
/// whenever a worktree exists (any verdict / Ok or Err) — `status` selects the
/// ref namespace (`pass` = merge-candidate `loom-features/<id>`; non-pass =
/// survival-only `loom-rescue/<id>-<status>`). The orthogonal hygiene guards
/// (HEAD changed from `initial_sha`, captured SHA semantically names a commit,
/// workspace is not a shallow clone) still apply. Returns the written ref name,
/// or `None` on any guard-skip / write failure.
///
/// Re-dispatches silently overwrite by design (Topic 2 in conclusion.md);
/// `--create-reflog` keeps the previous SHA recoverable for the window
/// configured by `gc.reflogExpire` (default 90 days). The overwrite event
/// is surfaced in the log so operators reading `.loom/run-*.log` see when
/// a prior SHA was replaced.
async fn propagate_worktree_commits(
    worktree: &Worktree,
    feature_id: &str,
    status: &str,
) -> Option<String> {
    let wt_path = worktree.path.to_string_lossy();
    let workspace = worktree.workspace.to_string_lossy();
    // F-018: `pass` → merge-candidate ref; any non-pass status (timeout/fail/
    // cancelled/error) → survival-only `loom-rescue/<id>-<status>` so a non-pass
    // ref is never mistaken for reviewed, merge-ready work. `feature_id` already
    // passed validate_feature_id; `status` is a fixed lowercase literal set.
    let ref_name = if status == "pass" {
        format!("refs/heads/loom-features/{}", feature_id)
    } else {
        format!("refs/heads/loom-rescue/{}-{}", feature_id, status)
    };

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
                return None;
            }
        },
        Ok(o) => {
            warn!(status = ?o.status, feature_id, path = %worktree.path.display(), "propagation: final rev-parse HEAD non-zero");
            return None;
        }
        Err(e) => {
            warn!(error = %e, feature_id, path = %worktree.path.display(), "propagation: final rev-parse spawn failed");
            return None;
        }
    };

    // 2. Zero-commit guard. Worker spawned but never advanced HEAD → no
    //    rescue ref needed (and we don't want to point at the initial
    //    commit and pretend a no-op worker produced output).
    if final_sha == worktree.initial_sha {
        tracing::debug!(feature_id, sha = %final_sha, "propagation: no commits made, skipping");
        return None;
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
            return None;
        }
        Err(e) => {
            warn!(error = %e, feature_id, sha = %final_sha, "propagation: semantic verify spawn failed; skipping");
            return None;
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
                    // F-018: action branches on ref kind. A merge-candidate
                    // (`loom-features`, status=pass) ref at a shallow boundary
                    // would later break `git merge` → skip. A `loom-rescue` ref
                    // is never merged, so the boundary risk is irrelevant —
                    // write it anyway (a recoverable ref beats GC loss).
                    if status == "pass" {
                        warn!(feature_id, workspace = %worktree.workspace.display(), "propagation: shallow clone; skipping merge-candidate loom-features ref to avoid a shallow-boundary merge break");
                        return None;
                    }
                    warn!(feature_id, ref_name = %ref_name, "propagation: shallow clone; writing rescue ref anyway (recoverable beats GC loss; not merge-eligible)");
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
            if status != "pass" {
                info!(feature_id, ref_name = %ref_name, "propagation: commit preserved at {} — not eligible for auto-merge", ref_name);
            }
            Some(ref_name)
        }
        Ok(s) => {
            warn!(status = ?s, feature_id, sha = %final_sha, ref_name = %ref_name, "propagation: update-ref non-zero");
            None
        }
        Err(e) => {
            warn!(error = %e, feature_id, ref_name = %ref_name, "propagation: update-ref spawn failed");
            None
        }
    }
}

// Suppress unused-Duration import warning on cfg paths.
#[allow(dead_code)]
fn _force_duration_use() -> Duration {
    Duration::from_secs(0)
}

/// F-021: the terminal worker-status segment of a `loom-rescue/<id>-<status>`
/// ref name. Mirrors the set `propagate_worktree_commits` writes; a ref whose
/// terminal segment is outside this set is not a Loom rescue ref and must never
/// be deleted by `gc-refs`.
// F-021 N1 primitives, consumed by `prune_rescue_refs` (N2).
const RESCUE_STATUSES: [&str; 5] = ["pass", "timeout", "fail", "cancelled", "error"];

/// F-021: parse a `refs/heads/loom-rescue/<feature_id>-<status>` ref into its
/// `(feature_id, status)` parts, or `None` if it is not a well-formed Loom
/// rescue ref.
///
/// Strictly scoped to the `loom-rescue/` namespace: `loom-features/` merge
/// candidates and any other ref return `None`, so they are never eligible for
/// `gc-refs` deletion. Both halves are validated as defense-in-depth before a
/// ref is ever handed to `git update-ref -d` — `feature_id` via
/// [`crate::feature_id::validate_feature_id`] (the BL-006 path/ref-injection
/// gate), `status` against [`RESCUE_STATUSES`]. The status is the terminal
/// `-`-segment; since no status string contains a hyphen, `rsplit_once('-')`
/// splits a slugged id unambiguously.
fn parse_rescue_ref_name(ref_name: &str) -> Option<(String, String)> {
    let basename = ref_name.strip_prefix("refs/heads/loom-rescue/")?;
    let (feature_id, status) = basename.rsplit_once('-')?;
    if crate::feature_id::validate_feature_id(feature_id).is_err() {
        return None;
    }
    if !RESCUE_STATUSES.contains(&status) {
        return None;
    }
    Some((feature_id.to_string(), status.to_string()))
}

/// F-021: pure age filter — return the names of refs whose age (`now -
/// write_time`) STRICTLY exceeds `max_age`. A ref exactly at the boundary
/// survives (deletion is strictly-older, never equal); a `write_time` in the
/// future (clock skew) is treated as fresh. `now` is injected so the retention
/// policy is testable without wall-clock sleeps.
fn select_stale_refs(
    refs: &[(String, SystemTime)],
    max_age: Duration,
    now: SystemTime,
) -> Vec<String> {
    refs.iter()
        .filter(|(_, write_time)| {
            now.duration_since(*write_time)
                .map(|age| age > max_age)
                .unwrap_or(false)
        })
        .map(|(name, _)| name.clone())
        .collect()
}

/// F-021: high-watermark — when a single `gc-refs` sweep stales more than this
/// many refs, the caller surfaces a terminal-visible warning (a low
/// `--max-age-days` mass-wipe guard).
const GC_REFS_WATERMARK: u64 = 20;

/// F-021: outcome of a `gc-refs` sweep over `refs/heads/loom-rescue/*`.
///
/// `deleted` + `skipped` + `errors` (live mode) or `would_delete` + `skipped`
/// (dry-run) account for every dateable ref; un-dateable refs (no reflog) are
/// warned and excluded from all counters.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct GcRefsSummary {
    /// Refs deleted this run (always 0 in dry-run).
    pub deleted: u64,
    /// Stale refs that WOULD be deleted (dry-run only; 0 in live mode).
    pub would_delete: u64,
    /// Within-window (fresh) refs left untouched.
    pub skipped: u64,
    /// Per-ref delete failures (CAS mismatch / vanished); non-fatal.
    pub errors: u64,
    /// Set when the stale count exceeded [`GC_REFS_WATERMARK`]; the caller (N3)
    /// turns this into a terminal-visible (stderr) warning — the `warn!` below
    /// only reaches `.loom/run-*.log`.
    pub watermark_triggered: bool,
    /// The stale ref names TARGETED this sweep — in live mode this is the union
    /// of successfully-deleted and CAS-errored refs (not just `deleted`); in
    /// dry-run it is the would-be-deleted candidates. The caller prints these in
    /// dry-run for forensic review before a destructive run.
    pub names: Vec<String>,
}

/// F-021: the unix write-time of a ref's newest reflog entry, or `None` when
/// the ref has no reflog (expired by `gc.reflogExpire`, or never written under
/// `core.logAllRefUpdates=false`). Parses the `@{<unix>}` token from
/// `git reflog show -1 --format=%gD --date=unix <ref>` — using the reflog WRITE
/// time, never `%(creatordate)` (which is the pointed-to commit's date and
/// would prune a fresh ref pointing at an old commit).
fn rescue_ref_write_time(workspace: &Path, refname: &str) -> Result<Option<SystemTime>> {
    let ws = workspace.to_str().context("workspace path not UTF-8")?;
    let out = std::process::Command::new("git")
        .args([
            "-C",
            ws,
            "reflog",
            "show",
            "-1",
            "--format=%gD",
            "--date=unix",
            refname,
        ])
        .output()
        .context("spawn git reflog show")?;
    if !out.status.success() {
        // Conscious tradeoff (data-safety over signal): a non-zero exit means
        // EITHER "ref has no reflog" (expected — gc.reflogExpire / never logged)
        // OR a git/fs failure (corrupt pack, perms). We can't reliably tell them
        // apart from the exit code, so we treat both as un-dateable and SKIP
        // (never delete on uncertainty). Cost: a real fs failure shows up as
        // "no reflog; skipping" rather than a hard error.
        return Ok(None);
    }
    let stdout = String::from_utf8(out.stdout).context("reflog stdout not UTF-8")?;
    let line = stdout.lines().next().unwrap_or("").trim();
    if line.is_empty() {
        return Ok(None);
    }
    // `<refname>@{<unix>}` — rsplit on `@{` (refnames can't contain it).
    let secs = line
        .rsplit_once("@{")
        .and_then(|(_, rest)| rest.strip_suffix('}'))
        .and_then(|digits| digits.parse::<u64>().ok());
    Ok(secs.map(|s| SystemTime::UNIX_EPOCH + Duration::from_secs(s)))
}

/// F-021: CAS delete of a single rescue ref via `git update-ref -d <ref> <oid>`
/// — git deletes only if the ref STILL points at `expected_oid`, so a
/// concurrent `loom run` that rewrote the ref in the enumerate→delete window
/// loses the CAS and its new value survives. Extracted as a seam so tests can
/// stage the mismatch deterministically.
pub(crate) fn delete_rescue_ref_cas(
    workspace: &Path,
    refname: &str,
    expected_oid: &str,
) -> Result<()> {
    let ws = workspace.to_str().context("workspace path not UTF-8")?;
    let st = std::process::Command::new("git")
        .args(["-C", ws, "update-ref", "-d", refname, expected_oid])
        .status()
        .context("spawn git update-ref -d")?;
    if st.success() {
        Ok(())
    } else {
        anyhow::bail!("git update-ref -d {refname} {expected_oid} failed (status {st:?})");
    }
}

/// F-021: age-delete stale `refs/heads/loom-rescue/*` refs. Enumerates each ref
/// WITH its OID (for CAS), dates it from the reflog, and — in live mode —
/// CAS-deletes those strictly older than `max_age`. Dry-run counts candidates
/// without touching anything. Per-ref delete failures are non-fatal (the sweep
/// never aborts); un-dateable refs are warned and skipped. `now` is injected
/// for deterministic tests.
pub fn prune_rescue_refs(
    workspace: &Path,
    max_age: Duration,
    now: SystemTime,
    dry_run: bool,
) -> Result<GcRefsSummary> {
    prune_rescue_refs_with(workspace, max_age, now, dry_run, delete_rescue_ref_cas)
}

/// F-021: testable core of [`prune_rescue_refs`] — the CAS delete is INJECTED
/// (mirrors the `is_alive` seam in `prune_stale_worktrees_with`) so the
/// error-accumulation + non-abort guarantee is exercisable without staging a
/// real concurrent rewrite. Production callers pass [`delete_rescue_ref_cas`].
fn prune_rescue_refs_with<F>(
    workspace: &Path,
    max_age: Duration,
    now: SystemTime,
    dry_run: bool,
    mut delete: F,
) -> Result<GcRefsSummary>
where
    F: FnMut(&Path, &str, &str) -> Result<()>,
{
    let ws = workspace.to_str().context("workspace path not UTF-8")?;
    // 1. Enumerate loom-rescue refs WITH their OID (null-delimited → CAS).
    let out = std::process::Command::new("git")
        .args([
            "-C",
            ws,
            "for-each-ref",
            "--format=%(refname)%00%(objectname)",
            "refs/heads/loom-rescue/",
        ])
        .output()
        .context("spawn git for-each-ref")?;
    if !out.status.success() {
        anyhow::bail!("git for-each-ref failed (status {:?})", out.status);
    }
    let listing = String::from_utf8(out.stdout).context("for-each-ref stdout not UTF-8")?;

    // 2. Resolve each ref's write-time; drop un-dateable ones with a warn.
    let mut dateable: Vec<(String, String, SystemTime)> = Vec::new();
    for line in listing.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some((refname, oid)) = line.split_once('\0') else {
            continue;
        };
        // Defense-in-depth (BL-006): even though for-each-ref is scoped to
        // `loom-rescue/`, re-validate the name (feature_id + known status)
        // before this ref can reach `update-ref -d`. A non-conforming ref under
        // the namespace (hand-created, malformed) is left untouched.
        if parse_rescue_ref_name(refname).is_none() {
            tracing::debug!(
                ref_name = refname,
                "gc-refs: ref under loom-rescue/ failed name validation; leaving untouched"
            );
            continue;
        }
        match rescue_ref_write_time(workspace, refname)? {
            Some(write_time) => dateable.push((refname.to_string(), oid.to_string(), write_time)),
            None => warn!(
                ref_name = refname,
                "gc-refs: rescue ref has no reflog entry; skipping (un-dateable)"
            ),
        }
    }

    // 3. Age-filter, then CAS-delete (or count, in dry-run).
    let pairs: Vec<(String, SystemTime)> =
        dateable.iter().map(|(r, _, t)| (r.clone(), *t)).collect();
    let stale: std::collections::HashSet<String> = select_stale_refs(&pairs, max_age, now)
        .into_iter()
        .collect();

    let mut summary = GcRefsSummary::default();
    for (refname, oid, _) in &dateable {
        if !stale.contains(refname) {
            summary.skipped += 1;
            continue;
        }
        summary.names.push(refname.clone());
        if dry_run {
            summary.would_delete += 1;
            continue;
        }
        match delete(workspace, refname, oid) {
            Ok(()) => summary.deleted += 1,
            Err(e) => {
                warn!(ref_name = %refname, error = %e, "gc-refs: ref delete failed (CAS mismatch or vanished); continuing");
                summary.errors += 1;
            }
        }
    }
    // Watermark counts every TARGETED ref (`names` = stale set, regardless of
    // CAS outcome), not just successful deletes — a wave of CAS failures under a
    // concurrent writer must not muffle the mass-action alert.
    summary.watermark_triggered = summary.names.len() as u64 > GC_REFS_WATERMARK;
    Ok(summary)
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

    // F-017 Step 2: write a done/<dir>/index.md (+ optional review.md) under a
    // tempdir workspace so `done_credited_view` (via `read_done_features`) sees it.
    fn write_done_feature(tmp: &std::path::Path, dir: &str, id: &str, review: Option<&str>) {
        let d = tmp.join(".ae/features/done").join(dir);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join("index.md"), format!("---\nid: {id}\n---\n")).unwrap();
        if let Some(verdict) = review {
            std::fs::write(
                d.join("review.md"),
                format!("---\nverdict: {verdict}\n---\nbody\n"),
            )
            .unwrap();
        }
    }

    #[test]
    fn done_credited_view_credits_archived_dep() {
        // AC2: F-002 depends on F-001; F-001 lives only in done/ → F-002 ready.
        let tmp = tempfile::tempdir().unwrap();
        let active = vec![feat("F-002", &["F-001"], false)];
        write_done_feature(tmp.path(), "F-001-slug", "F-001", None);
        let view = done_credited_view(active, tmp.path());
        assert!(
            ready_set(&view).iter().any(|f| f.id == "F-002"),
            "archived-done F-001 must credit done_ids and unblock F-002"
        );
    }

    #[test]
    fn done_credited_view_no_done_leaves_dep_blocked_and_is_inert() {
        // AC2 control + AC3(e): no done/ → F-002 stays blocked; view == active.
        let tmp = tempfile::tempdir().unwrap();
        let active = vec![feat("F-002", &["F-001"], false)];
        let view = done_credited_view(active, tmp.path());
        assert_eq!(
            view.len(),
            1,
            "empty/absent done/ → view identical to active"
        );
        assert!(
            !ready_set(&view).iter().any(|f| f.id == "F-002"),
            "without credit F-002 stays blocked (proves credit, not unconditional readiness)"
        );
    }

    #[test]
    fn done_credited_view_skips_invalid_done_id() {
        // AC3(a): a done/ dir with an unparseable/invalid id must not credit.
        let tmp = tempfile::tempdir().unwrap();
        let active = vec![feat("F-002", &["F-001"], false)];
        let d = tmp.path().join(".ae/features/done/F-001-slug");
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join("index.md"), "---\nid: \"../etc\"\n---\n").unwrap();
        let view = done_credited_view(active, tmp.path());
        assert!(
            !ready_set(&view).iter().any(|f| f.id == "F-002"),
            "invalid-id done/ entry must never enter done_ids"
        );
    }

    #[test]
    fn done_credited_view_suppresses_dup_active_incomplete() {
        // AC3(b): F-001 live+incomplete in active AND archived in done → the
        // in-flight active copy suppresses the done copy's credit.
        let tmp = tempfile::tempdir().unwrap();
        let active = vec![feat("F-001", &[], false), feat("F-002", &["F-001"], false)];
        write_done_feature(tmp.path(), "F-001-other", "F-001", None);
        let view = done_credited_view(active, tmp.path());
        assert!(
            !ready_set(&view).iter().any(|f| f.id == "F-002"),
            "in-flight active F-001 must suppress the done copy → F-002 stays blocked"
        );
    }

    #[test]
    fn done_credited_view_fail_guard_blocks_credit() {
        // AC3(c): a done/ feature whose review.md says fail must not credit.
        let tmp = tempfile::tempdir().unwrap();
        let active = vec![feat("F-002", &["F-001"], false)];
        write_done_feature(tmp.path(), "F-001-slug", "F-001", Some("fail"));
        let view = done_credited_view(active, tmp.path());
        assert!(
            !ready_set(&view).iter().any(|f| f.id == "F-002"),
            "done/ review verdict:fail must not credit (fail-guard)"
        );
    }

    #[test]
    fn done_credited_view_fail_guard_is_per_id() {
        // Review F-017 P2-B: duplicate-id done dirs — one fail, one missing
        // review. The fail copy must block credit for the id (per-id), not let
        // the review-less sibling mask it (the old per-directory bug).
        let tmp = tempfile::tempdir().unwrap();
        let active = vec![feat("F-002", &["F-001"], false)];
        write_done_feature(tmp.path(), "F-001-v1", "F-001", Some("fail"));
        write_done_feature(tmp.path(), "F-001-v2", "F-001", None);
        let view = done_credited_view(active, tmp.path());
        assert!(
            !ready_set(&view).iter().any(|f| f.id == "F-002"),
            "a fail archive for an id must block credit even when a sibling copy lacks a review"
        );
    }

    #[test]
    fn done_credited_view_pass_review_credits() {
        // AC3(d): a readable verdict:pass review credits (and missing review,
        // covered by done_credited_view_credits_archived_dep, also credits —
        // no fail-closed regression).
        let tmp = tempfile::tempdir().unwrap();
        let active = vec![feat("F-002", &["F-001"], false)];
        write_done_feature(tmp.path(), "F-001-slug", "F-001", Some("pass"));
        let view = done_credited_view(active, tmp.path());
        assert!(
            ready_set(&view).iter().any(|f| f.id == "F-002"),
            "verdict:pass done/ feature credits normally"
        );
    }

    #[test]
    fn deps_stuck_false_when_archived_dep_credits() {
        // AC4: the exact deps_stuck expression dispatch_command computes
        // (`any_incomplete && ready_set(&view).is_empty()`) on the credited view.
        let deps_stuck = |active: Vec<DiscoveredFeature>, ws: &std::path::Path| {
            let any_incomplete = active.iter().any(|f| !f.is_done());
            let view = done_credited_view(active, ws);
            any_incomplete && ready_set(&view).is_empty()
        };
        // archived-done F-001 present → F-002 ready → NOT deps_stuck.
        let tmp = tempfile::tempdir().unwrap();
        write_done_feature(tmp.path(), "F-001-slug", "F-001", None);
        assert!(
            !deps_stuck(vec![feat("F-002", &["F-001"], false)], tmp.path()),
            "a dependency credited from done/ must not derive deps_stuck"
        );
        // control: F-001 absent everywhere → genuinely blocked → deps_stuck.
        let tmp2 = tempfile::tempdir().unwrap();
        assert!(
            deps_stuck(vec![feat("F-002", &["F-001"], false)], tmp2.path()),
            "a genuinely missing dependency still derives deps_stuck"
        );
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

    // F-018: a Worker that COMMITS in the worktree (spec.feature_dir = worktree
    // root) before returning a configurable verdict (or Err). The existing
    // StubVerdictWorker never touches git, so it can't exercise the rescue-ref
    // propagation path.
    struct StubCommitWorker {
        verdict: crate::artifact::WorkerVerdict,
        err_after_commit: bool,
    }

    #[async_trait::async_trait]
    impl crate::worker::Worker for StubCommitWorker {
        async fn run(
            &self,
            spec: crate::artifact::FeatureSpec,
            _cancel: CancellationToken,
        ) -> anyhow::Result<crate::artifact::Artifact> {
            // Advance the worktree HEAD with a non-gitignored file.
            std::fs::write(spec.feature_dir.join("work.txt"), "done").unwrap();
            for a in [
                ["add", "work.txt"].as_slice(),
                ["commit", "-q", "-m", "work"].as_slice(),
            ] {
                std::process::Command::new("git")
                    .args(a)
                    .current_dir(&spec.feature_dir)
                    .status()
                    .unwrap();
            }
            if self.err_after_commit {
                anyhow::bail!("worker errored AFTER committing real work");
            }
            Ok(crate::artifact::Artifact {
                verdict: self.verdict,
                stdout_path: spec.feature_dir.join("stub.out"),
                reasoning_trace: None,
                duration: Duration::from_millis(1),
                worker_identity: spec.worker_identity,
                exit_code: 0,
                drain_truncated: false,
            })
        }
    }

    fn git_ws_with_feature(
        id: &str,
        slug: &str,
    ) -> (tempfile::TempDir, std::path::PathBuf, DiscoveredFeature) {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        git(&ws, &["init", "-q"]);
        git(&ws, &["config", "user.email", "t@loom"]);
        git(&ws, &["config", "user.name", "t"]);
        std::fs::write(ws.join(".gitignore"), ".ae/\n.loom/\n").unwrap();
        git(&ws, &["add", ".gitignore"]);
        git(&ws, &["commit", "-q", "-m", "init"]);
        let feat = ws.join(format!(".ae/features/active/{slug}"));
        std::fs::create_dir_all(&feat).unwrap();
        std::fs::write(feat.join("index.md"), format!("---\nid: {id}\n---\n")).unwrap();
        let feature = DiscoveredFeature {
            id: id.into(),
            feature_dir: feat,
            depends_on: vec![],
            work_state: None,
        };
        (tmp, ws, feature)
    }

    fn ref_exists(ws: &std::path::Path, ref_name: &str) -> bool {
        std::process::Command::new("git")
            .args([
                "-C",
                ws.to_str().unwrap(),
                "rev-parse",
                "--verify",
                "--quiet",
                ref_name,
            ])
            .status()
            .unwrap()
            .success()
    }

    /// F-021 AC1: `parse_rescue_ref_name` accepts well-formed `loom-rescue`
    /// refs and rejects everything out of scope (wrong namespace, unknown
    /// status, bad feature id, no status segment).
    #[test]
    fn parse_rescue_ref_name_accepts_valid_rejects_out_of_scope() {
        assert_eq!(
            parse_rescue_ref_name("refs/heads/loom-rescue/F-018-timeout"),
            Some(("F-018".to_string(), "timeout".to_string()))
        );
        // Slugged id: status is the terminal segment; statuses contain no hyphen.
        assert_eq!(
            parse_rescue_ref_name("refs/heads/loom-rescue/F-021-rescue-ref-retention-cleanup-fail"),
            Some((
                "F-021-rescue-ref-retention-cleanup".to_string(),
                "fail".to_string()
            ))
        );
        // Wrong namespace — merge candidate, never deletable.
        assert_eq!(
            parse_rescue_ref_name("refs/heads/loom-features/F-018"),
            None
        );
        // Unknown status, not in the 5-set.
        assert_eq!(
            parse_rescue_ref_name("refs/heads/loom-rescue/F-018-bogus"),
            None
        );
        // feature_id fails validate_feature_id.
        assert_eq!(
            parse_rescue_ref_name("refs/heads/loom-rescue/INVALID-timeout"),
            None
        );
        // No status segment: rsplit_once → ("F","018"), validate_feature_id("F") fails.
        assert_eq!(parse_rescue_ref_name("refs/heads/loom-rescue/F-018"), None);
    }

    /// F-021 AC2: `select_stale_refs` selects refs strictly older than max_age;
    /// the boundary survives.
    #[test]
    fn select_stale_refs_age_boundary() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000_000);
        let max_age = Duration::from_secs(90 * 86_400);
        let day = Duration::from_secs(86_400);
        let refs = vec![
            ("old".to_string(), now - 100 * day),     // stale
            ("fresh".to_string(), now - 10 * day),    // fresh
            ("boundary".to_string(), now - 90 * day), // exactly max_age → survives
        ];
        assert_eq!(
            select_stale_refs(&refs, max_age, now),
            vec!["old".to_string()]
        );
    }

    /// F-021: a bare git workspace with one commit (no feature dir).
    fn git_ws() -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        git(ws, &["init", "-q"]);
        git(ws, &["config", "user.email", "t@loom"]);
        git(ws, &["config", "user.name", "t"]);
        git(ws, &["commit", "-q", "--allow-empty", "-m", "init"]);
        tmp
    }

    fn rev_parse(ws: &std::path::Path, rev: &str) -> String {
        String::from_utf8(
            std::process::Command::new("git")
                .args(["-C", ws.to_str().unwrap(), "rev-parse", rev])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_string()
    }

    /// F-021: write a `loom-rescue/*` ref at HEAD, optionally back-dating its
    /// reflog entry via `GIT_COMMITTER_DATE` so age tests are deterministic.
    fn write_rescue_ref(ws: &std::path::Path, refname: &str, committer_date: Option<&str>) {
        let sha = rev_parse(ws, "HEAD");
        let mut cmd = std::process::Command::new("git");
        cmd.args([
            "-C",
            ws.to_str().unwrap(),
            "update-ref",
            "--create-reflog",
            refname,
            &sha,
        ]);
        if let Some(d) = committer_date {
            cmd.env("GIT_COMMITTER_DATE", d);
        }
        assert!(
            cmd.status().unwrap().success(),
            "write_rescue_ref {refname}"
        );
    }

    /// F-021 AC3: a back-dated rescue ref is deleted; a fresh one and an
    /// un-dateable one (reflog removed) both survive the sweep.
    #[cfg(unix)]
    #[test]
    fn prune_rescue_refs_deletes_stale_preserves_fresh_and_undateable() {
        let tmp = git_ws();
        let ws = tmp.path();
        write_rescue_ref(
            ws,
            "refs/heads/loom-rescue/F-001-timeout",
            Some("2020-01-01T00:00:00 +0000"),
        );
        write_rescue_ref(ws, "refs/heads/loom-rescue/F-002-fail", None);
        write_rescue_ref(ws, "refs/heads/loom-rescue/F-003-error", None);
        std::fs::remove_file(ws.join(".git/logs/refs/heads/loom-rescue/F-003-error")).unwrap();
        // Defense-in-depth (BL-006): a malformed ref under loom-rescue/, even
        // back-dated well past max_age, must NOT be deleted (fails name validation).
        write_rescue_ref(
            ws,
            "refs/heads/loom-rescue/garbage",
            Some("2020-01-01T00:00:00 +0000"),
        );

        let summary = prune_rescue_refs(
            ws,
            Duration::from_secs(90 * 86_400),
            SystemTime::now(),
            false,
        )
        .unwrap();

        assert_eq!(
            summary.deleted, 1,
            "only the well-formed back-dated ref is stale"
        );
        assert_eq!(summary.skipped, 1, "the fresh ref is within window");
        assert!(
            ref_exists(ws, "refs/heads/loom-rescue/garbage"),
            "a malformed ref is never deleted even when old (defense-in-depth)"
        );
        assert!(
            !summary.watermark_triggered,
            "1 deletion is below watermark"
        );
        assert!(
            !ref_exists(ws, "refs/heads/loom-rescue/F-001-timeout"),
            "stale ref deleted"
        );
        assert!(
            ref_exists(ws, "refs/heads/loom-rescue/F-002-fail"),
            "fresh ref survives"
        );
        assert!(
            ref_exists(ws, "refs/heads/loom-rescue/F-003-error"),
            "un-dateable ref survives (never auto-deleted)"
        );
    }

    /// F-021 AC3 (CAS seam): a concurrent rewrite makes the CAS delete fail and
    /// the ref survives; deleting with the current OID succeeds.
    #[cfg(unix)]
    #[test]
    fn delete_rescue_ref_cas_fails_on_oid_mismatch_preserves_ref() {
        let tmp = git_ws();
        let ws = tmp.path();
        let refname = "refs/heads/loom-rescue/F-004-fail";
        write_rescue_ref(ws, refname, None);
        let oid_a = rev_parse(ws, refname);
        // Concurrent-rewrite simulation: advance HEAD, repoint the ref to OID_B.
        git(ws, &["commit", "-q", "--allow-empty", "-m", "second"]);
        let oid_b = rev_parse(ws, "HEAD");
        git(ws, &["update-ref", refname, &oid_b]);

        assert!(
            delete_rescue_ref_cas(ws, refname, &oid_a).is_err(),
            "CAS delete must fail when the ref moved"
        );
        assert!(ref_exists(ws, refname), "ref survives a failed CAS delete");
        delete_rescue_ref_cas(ws, refname, &oid_b).unwrap();
        assert!(
            !ref_exists(ws, refname),
            "CAS delete with current OID succeeds"
        );
    }

    /// F-021 AC5: `--dry-run` reports the stale candidate but deletes nothing.
    #[cfg(unix)]
    #[test]
    fn prune_rescue_refs_dry_run_lists_without_deleting() {
        let tmp = git_ws();
        let ws = tmp.path();
        write_rescue_ref(
            ws,
            "refs/heads/loom-rescue/F-001-timeout",
            Some("2020-01-01T00:00:00 +0000"),
        );
        let summary = prune_rescue_refs(
            ws,
            Duration::from_secs(90 * 86_400),
            SystemTime::now(),
            true,
        )
        .unwrap();
        assert_eq!(summary.deleted, 0, "dry-run deletes nothing");
        assert!(
            summary.would_delete >= 1,
            "dry-run reports the stale candidate"
        );
        assert!(
            ref_exists(ws, "refs/heads/loom-rescue/F-001-timeout"),
            "dry-run leaves the ref intact"
        );
    }

    /// F-021 AC3 (CAS error branch, E2E): a delete failure is counted in
    /// `errors` and the sweep CONTINUES to the next ref (non-abort) — exercised
    /// via the injected delete seam, since a real CAS mismatch can't be staged
    /// single-threaded through `prune_rescue_refs`.
    #[cfg(unix)]
    #[test]
    fn prune_rescue_refs_counts_delete_errors_without_aborting() {
        let tmp = git_ws();
        let ws = tmp.path();
        write_rescue_ref(
            ws,
            "refs/heads/loom-rescue/F-001-timeout",
            Some("2020-01-01T00:00:00 +0000"),
        );
        write_rescue_ref(
            ws,
            "refs/heads/loom-rescue/F-002-fail",
            Some("2020-01-01T00:00:00 +0000"),
        );
        let mut deleted = Vec::new();
        let summary = prune_rescue_refs_with(
            ws,
            Duration::from_secs(90 * 86_400),
            SystemTime::now(),
            false,
            |_ws: &std::path::Path, refname: &str, _oid: &str| {
                if refname.ends_with("F-001-timeout") {
                    anyhow::bail!("simulated CAS mismatch")
                } else {
                    deleted.push(refname.to_string());
                    Ok(())
                }
            },
        )
        .unwrap();
        assert_eq!(
            summary.errors, 1,
            "the failing delete is counted, not propagated"
        );
        assert_eq!(summary.deleted, 1, "the sweep continued past the failure");
        assert_eq!(
            deleted,
            vec!["refs/heads/loom-rescue/F-002-fail".to_string()],
            "the second ref was still attempted after the first failed"
        );
    }

    /// F-021 P3 (watermark trigger): more than `GC_REFS_WATERMARK` targeted refs
    /// sets `watermark_triggered` (the mass-action alert the caller surfaces to
    /// stderr). Counts targets regardless of CAS outcome.
    #[cfg(unix)]
    #[test]
    fn prune_rescue_refs_watermark_trips_above_threshold() {
        let tmp = git_ws();
        let ws = tmp.path();
        for i in 0..=GC_REFS_WATERMARK {
            write_rescue_ref(
                ws,
                &format!("refs/heads/loom-rescue/F-{i:03}-timeout"),
                Some("2020-01-01T00:00:00 +0000"),
            );
        }
        let summary = prune_rescue_refs(
            ws,
            Duration::from_secs(90 * 86_400),
            SystemTime::now(),
            true,
        )
        .unwrap();
        assert!(
            summary.would_delete > GC_REFS_WATERMARK,
            "all {} back-dated refs are candidates",
            GC_REFS_WATERMARK + 1
        );
        assert!(
            summary.watermark_triggered,
            "{}+ candidates must trip the >{} watermark",
            GC_REFS_WATERMARK + 1,
            GC_REFS_WATERMARK
        );
    }

    /// AC1: a non-Pass worker (Timeout / Cancelled / Err-after-commit) that
    /// advanced the worktree HEAD gets a namespaced `loom-rescue/<id>-<status>`
    /// rescue ref written before cleanup, surfaced on the outcome's rescue_ref.
    #[cfg(unix)]
    #[tokio::test]
    async fn run_one_feature_writes_namespaced_rescue_ref_for_nonpass() {
        use crate::artifact::WorkerVerdict;
        let cases: &[(WorkerVerdict, bool, &str)] = &[
            (WorkerVerdict::Timeout, false, "timeout"),
            (WorkerVerdict::Cancelled, false, "cancelled"),
            (WorkerVerdict::Fail, false, "fail"),
            // Err AFTER committing → status "error"; the verdict value is unused.
            (WorkerVerdict::Pass, true, "error"),
        ];
        for (verdict, err_after, status) in cases {
            let (_tmp, ws, feature) = git_ws_with_feature("F-018", "F-018-rescue");
            let worker: Arc<dyn crate::worker::Worker> = Arc::new(StubCommitWorker {
                verdict: *verdict,
                err_after_commit: *err_after,
            });
            let outcome = run_one_feature(
                feature,
                worker,
                "F-018-w0".into(),
                &ws,
                CancellationToken::new(),
            )
            .await
            .unwrap();
            let expected = format!("refs/heads/loom-rescue/F-018-{status}");
            assert_eq!(
                outcome.rescue_ref.as_deref(),
                Some(expected.as_str()),
                "{status}: rescue_ref must name the namespaced ref"
            );
            assert!(
                ref_exists(&ws, &expected),
                "{status}: {expected} must exist in the main workspace after cleanup"
            );
            assert!(
                !ref_exists(&ws, "refs/heads/loom-features/F-018"),
                "{status}: a non-pass worker must NOT write the merge-candidate ref"
            );
        }
    }

    /// AC2: Pass → `loom-features/<id>` (unchanged merge-candidate name); a
    /// worker that makes NO commit → no ref of either kind (zero-commit guard).
    #[cfg(unix)]
    #[tokio::test]
    async fn run_one_feature_pass_uses_loom_features_and_zero_commit_writes_nothing() {
        use crate::artifact::WorkerVerdict;
        // Pass + commits → loom-features/<id>.
        let (_tmp, ws, feature) = git_ws_with_feature("F-018", "F-018-pass");
        let worker: Arc<dyn crate::worker::Worker> = Arc::new(StubCommitWorker {
            verdict: WorkerVerdict::Pass,
            err_after_commit: false,
        });
        let outcome = run_one_feature(
            feature,
            worker,
            "F-018-w0".into(),
            &ws,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            outcome.rescue_ref.as_deref(),
            Some("refs/heads/loom-features/F-018"),
            "Pass must write the merge-candidate ref under loom-features/"
        );
        assert!(!ref_exists(&ws, "refs/heads/loom-rescue/F-018-pass"));

        // Zero-commit worker (StubVerdictWorker never touches git) → no ref.
        let (_tmp2, ws2, feature2) = git_ws_with_feature("F-020", "F-020-noop");
        let noop: Arc<dyn crate::worker::Worker> = Arc::new(StubVerdictWorker {
            verdict: WorkerVerdict::Timeout,
            exit_code: 1,
        });
        let outcome2 = run_one_feature(
            feature2,
            noop,
            "F-020-w0".into(),
            &ws2,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            outcome2.rescue_ref, None,
            "a worker that advanced no commits must write no rescue ref"
        );
    }

    /// AC3: on a SHALLOW workspace, a non-pass `loom-rescue` ref is written
    /// anyway (recoverable beats GC loss), while a `pass` `loom-features`
    /// merge-candidate ref is still skipped (a shallow-boundary ref would break
    /// a later `git merge`).
    #[cfg(unix)]
    #[tokio::test]
    async fn propagate_shallow_writes_rescue_but_skips_merge_candidate() {
        let src = tempfile::tempdir().unwrap();
        let src_ws = src.path();
        git(src_ws, &["init", "-q"]);
        git(src_ws, &["config", "user.email", "t@loom"]);
        git(src_ws, &["config", "user.name", "t"]);
        std::fs::write(src_ws.join("a.txt"), "0").unwrap();
        git(src_ws, &["add", "a.txt"]);
        git(src_ws, &["commit", "-q", "-m", "c0"]);

        // Depth-1 clone → shallow workspace.
        let dst = tempfile::tempdir().unwrap();
        let shallow = dst.path().join("shallow");
        let st = std::process::Command::new("git")
            .args([
                "clone",
                "--depth",
                "1",
                "-q",
                &format!("file://{}", src_ws.display()),
                shallow.to_str().unwrap(),
            ])
            .status()
            .unwrap();
        assert!(st.success(), "shallow clone must succeed");
        git(&shallow, &["config", "user.email", "t@loom"]);
        git(&shallow, &["config", "user.name", "t"]);
        let is_shallow = std::process::Command::new("git")
            .args([
                "-C",
                shallow.to_str().unwrap(),
                "rev-parse",
                "--is-shallow-repository",
            ])
            .output()
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&is_shallow.stdout).trim(),
            "true",
            "fixture must be a shallow clone"
        );

        // Make a committed worktree off the shallow clone; return its Worktree.
        let mk_wt = |name: &str| -> Worktree {
            let wt_path = dst.path().join(name);
            let initial = String::from_utf8(
                std::process::Command::new("git")
                    .args(["-C", shallow.to_str().unwrap(), "rev-parse", "HEAD"])
                    .output()
                    .unwrap()
                    .stdout,
            )
            .unwrap()
            .trim()
            .to_string();
            git(
                &shallow,
                &[
                    "worktree",
                    "add",
                    "-q",
                    "--detach",
                    wt_path.to_str().unwrap(),
                ],
            );
            std::fs::write(wt_path.join("w.txt"), name).unwrap();
            git(&wt_path, &["add", "w.txt"]);
            git(&wt_path, &["commit", "-q", "-m", "work"]);
            Worktree {
                path: wt_path,
                workspace: shallow.clone(),
                initial_sha: initial,
            }
        };

        // non-pass → rescue ref WRITTEN despite shallow.
        let wt1 = mk_wt("wt1");
        let r1 = propagate_worktree_commits(&wt1, "F-018", "timeout").await;
        assert_eq!(
            r1.as_deref(),
            Some("refs/heads/loom-rescue/F-018-timeout"),
            "rescue ref must be written even on a shallow clone"
        );
        assert!(ref_exists(&shallow, "refs/heads/loom-rescue/F-018-timeout"));

        // pass → merge-candidate SKIPPED on shallow.
        let wt2 = mk_wt("wt2");
        let r2 = propagate_worktree_commits(&wt2, "F-019", "pass").await;
        assert_eq!(
            r2, None,
            "merge-candidate ref must be skipped on a shallow clone"
        );
        assert!(!ref_exists(&shallow, "refs/heads/loom-features/F-019"));
    }

    /// F-019 AC1 (loop half): a Timeout worker driven through run_dispatch_loop
    /// yields a DispatchReport whose outcome carries `worker_exit_status ==
    /// "timeout"` — the input that the delivery+exit half
    /// (main.rs `timeout_worker_yields_dispatch_log_and_nonzero_exit`) then
    /// turns into a dispatch log + non-zero exit.
    #[cfg(unix)]
    #[tokio::test]
    async fn run_dispatch_loop_timeout_yields_timeout_outcome() {
        let (_tmp, ws, feature) = git_ws_with_feature("F-019", "F-019-timeout");
        let worker: Arc<dyn crate::worker::Worker> = Arc::new(StubVerdictWorker {
            verdict: crate::artifact::WorkerVerdict::Timeout,
            exit_code: 1,
        });
        let report =
            run_dispatch_loop(vec![feature], vec![worker], 1, ws, CancellationToken::new())
                .await
                .unwrap();
        assert_eq!(report.dispatched_count, 1);
        assert_eq!(
            report.outcomes[0].worker_exit_status, "timeout",
            "a TimedOut worker must surface as a timeout outcome for delivery"
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

    // --- F-010: run_one_feature splits the AE verdict (review.md) from the
    // worker process signal. Workspace is a non-git tempdir so
    // maybe_create_worktree fails gracefully and the worker runs directly in
    // feature_dir; run_one_feature then reads feature_dir/review.md. ---

    use crate::artifact::{Artifact, FeatureSpec, WorkerVerdict};
    use async_trait::async_trait;

    struct StubVerdictWorker {
        verdict: WorkerVerdict,
        exit_code: i32,
    }

    #[async_trait]
    impl Worker for StubVerdictWorker {
        async fn run(&self, spec: FeatureSpec, _cancel: CancellationToken) -> Result<Artifact> {
            Ok(Artifact {
                verdict: self.verdict,
                stdout_path: spec.feature_dir.join("stub.out"),
                reasoning_trace: None,
                duration: Duration::from_millis(1),
                worker_identity: spec.worker_identity,
                exit_code: self.exit_code,
                drain_truncated: false,
            })
        }
    }

    async fn run_stub(
        workspace: &std::path::Path,
        feature_dir: &std::path::Path,
        verdict: WorkerVerdict,
        exit_code: i32,
    ) -> FeatureOutcome {
        let feature = DiscoveredFeature {
            id: "F-200".into(),
            feature_dir: feature_dir.to_path_buf(),
            depends_on: vec![],
            work_state: None,
        };
        let worker: Arc<dyn Worker> = Arc::new(StubVerdictWorker { verdict, exit_code });
        run_one_feature(
            feature,
            worker,
            "F-200-w0".into(),
            workspace,
            CancellationToken::new(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn run_one_feature_clean_worker_review_fail_yields_ae_fail() {
        // AC1: worker exits clean but review.md says fail → verdict=fail (AE),
        // worker_exit_status=pass (process).
        let tmp = tempfile::tempdir().unwrap();
        let fd = tmp.path().join(".ae/features/active/F-200-x");
        std::fs::create_dir_all(&fd).unwrap();
        std::fs::write(fd.join("review.md"), "---\nverdict: fail\n---\n").unwrap();
        let o = run_stub(tmp.path(), &fd, WorkerVerdict::Pass, 0).await;
        assert_eq!(o.verdict, "fail", "AE verdict comes from review.md");
        assert_eq!(
            o.worker_exit_status, "pass",
            "process signal is the clean exit"
        );
    }

    #[tokio::test]
    async fn run_one_feature_crash_no_review_is_worker_fail_verdict_unknown() {
        // AC2: worker crash + no review.md → worker_exit_status=fail, verdict=unknown.
        let tmp = tempfile::tempdir().unwrap();
        let fd = tmp.path().join(".ae/features/active/F-200-x");
        std::fs::create_dir_all(&fd).unwrap();
        let o = run_stub(tmp.path(), &fd, WorkerVerdict::Fail, 1).await;
        assert_eq!(o.worker_exit_status, "fail", "process crash surfaced");
        assert_eq!(o.verdict, "unknown", "no review.md → no AE judgment");
    }

    /// F-014: DELIBERATE REVERSAL of F-010's AC3 neutral path. The old contract
    /// ("clean worker + no review.md → verdict=unknown → exit 0") is replaced by
    /// refined Option B (ae:consensus verdict,
    /// .ae/analyses/001-consensus-f014-ac3-reversal-scope.md): on Unix a clean
    /// worker with no readable TERMINAL review verdict emits "missing"
    /// (→ EXIT_REVIEW_MISSING), unconditionally — symlink success, symlink
    /// failure, and no-worktree mode alike.
    #[cfg(unix)]
    #[tokio::test]
    async fn run_one_feature_clean_no_review_is_verdict_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let fd = tmp.path().join(".ae/features/active/F-200-x");
        std::fs::create_dir_all(&fd).unwrap();
        let o = run_stub(tmp.path(), &fd, WorkerVerdict::Pass, 0).await;
        assert_eq!(o.verdict, "missing");
        assert_eq!(o.worker_exit_status, "pass");
    }

    /// F-014 (Doodlestein Cliff 3): the non-Unix contract — clean worker + no
    /// review stays "unknown" (exit 0; BL-008 platform umbrella). Source-level
    /// coverage on a Unix host; runs when a non-Unix target is tested.
    #[cfg(not(unix))]
    #[tokio::test]
    async fn run_one_feature_clean_no_review_stays_unknown_on_non_unix() {
        let tmp = tempfile::tempdir().unwrap();
        let fd = tmp.path().join(".ae/features/active/F-200-x");
        std::fs::create_dir_all(&fd).unwrap();
        let o = run_stub(tmp.path(), &fd, WorkerVerdict::Pass, 0).await;
        assert_eq!(o.verdict, "unknown");
        assert_eq!(o.worker_exit_status, "pass");
    }

    /// F-014: a CANCELLED worker is not clean — no-review verdict stays
    /// "unknown" on all platforms (conclusion row 3; codex matrix row).
    #[tokio::test]
    async fn run_one_feature_cancelled_no_review_is_verdict_unknown() {
        let tmp = tempfile::tempdir().unwrap();
        let fd = tmp.path().join(".ae/features/active/F-200-x");
        std::fs::create_dir_all(&fd).unwrap();
        let o = run_stub(tmp.path(), &fd, WorkerVerdict::Cancelled, 130).await;
        assert_eq!(o.verdict, "unknown");
        assert_eq!(o.worker_exit_status, "cancelled");
    }

    /// F-014: no_review_verdict helper cells — both bool arms (the cfg!(unix)
    /// dimension is compile-time; on a Unix host these pin the Unix column).
    #[test]
    fn no_review_verdict_clean_is_missing_on_unix() {
        #[cfg(unix)]
        assert_eq!(no_review_verdict(true), "missing");
        #[cfg(not(unix))]
        assert_eq!(no_review_verdict(true), "unknown");
    }

    #[test]
    fn no_review_verdict_not_clean_is_unknown_everywhere() {
        assert_eq!(no_review_verdict(false), "unknown");
    }

    /// Worker that writes review.md to a path UNDER its (worktree-root) cwd,
    /// modelling `/ae:review` writing through the F-008 symlink.
    struct StubWriteReviewWorker {
        review_rel: PathBuf,
        verdict: &'static str,
    }

    #[async_trait]
    impl Worker for StubWriteReviewWorker {
        async fn run(&self, spec: FeatureSpec, _cancel: CancellationToken) -> Result<Artifact> {
            let review = spec.feature_dir.join(&self.review_rel);
            if let Some(p) = review.parent() {
                let _ = std::fs::create_dir_all(p);
            }
            std::fs::write(&review, format!("---\nverdict: {}\n---\n", self.verdict))?;
            Ok(Artifact {
                verdict: WorkerVerdict::Pass,
                stdout_path: spec.feature_dir.join("stub.out"),
                reasoning_trace: None,
                duration: Duration::from_millis(1),
                worker_identity: spec.worker_identity,
                exit_code: 0,
                drain_truncated: false,
            })
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_one_feature_reads_review_through_worktree_symlink() {
        // PRODUCTION path (F-010 review, challenger): a REAL git worktree is
        // created, F-008 symlinks the feature dir in, the worker writes review.md
        // through the symlink, and run_one_feature reads it back from the
        // MAIN-TREE feature_dir AFTER cleanup. The other run_one_feature tests
        // only cover the no-worktree fallback (non-git tempdir).
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        git(ws, &["init", "-q"]);
        git(ws, &["config", "user.email", "t@loom"]);
        git(ws, &["config", "user.name", "t"]);
        std::fs::write(ws.join(".gitignore"), ".ae/\n.loom/\n").unwrap();
        git(ws, &["add", ".gitignore"]);
        git(ws, &["commit", "-q", "-m", "init"]);

        let basename = "F-201-x";
        let fd = ws.join(".ae/features/active").join(basename);
        std::fs::create_dir_all(&fd).unwrap();
        std::fs::write(fd.join("plan.md"), "plan").unwrap();

        // Worker (cwd = worktree root) writes to the symlinked feature dir:
        // <wt>/.ae/features/active/<basename>/review.md → (symlink) → fd.
        let review_rel = PathBuf::from(".ae/features/active")
            .join(basename)
            .join("review.md");
        let worker: Arc<dyn Worker> = Arc::new(StubWriteReviewWorker {
            review_rel,
            verdict: "fail",
        });
        let feature = DiscoveredFeature {
            id: "F-201".into(),
            feature_dir: fd.clone(),
            depends_on: vec![],
            work_state: None,
        };

        let outcome = run_one_feature(
            feature,
            worker,
            "F-201-w0".into(),
            ws,
            CancellationToken::new(),
        )
        .await
        .unwrap();

        // AC1 production path: the AE verdict was read back from the main-tree
        // feature_dir (via the F-008 symlink) after worktree cleanup.
        assert_eq!(
            outcome.verdict, "fail",
            "AE verdict must be read back through the worktree symlink"
        );
        assert_eq!(outcome.worker_exit_status, "pass");

        // Challenger #2: the dispatch ae_review_failed derivation fires on a REAL
        // run_one_feature output (not just a hand-built report). `decide_exit`
        // itself is a bin-crate fn, unit-tested in main.rs.
        let report = DispatchReport {
            started_at_ms: 0,
            elapsed_ms: 0,
            dispatched_count: 1,
            outcomes: vec![outcome],
        };
        assert!(
            report.outcomes.iter().any(|o| o.verdict == "fail"),
            "dispatch ae_review_failed derivation fires on a real outcome"
        );
    }

    // --- F-016: verdict read survives the AE `/ae:review` archive `mv`, which
    // lands non-deterministically in one of three sites (D1):
    //   A  main-tree active/ survives (relative mv ENOENT-fails)
    //   B  worktree-local done/  (cleanup-destroyed → MUST be read pre-cleanup)
    //   C  main-tree done/       (F-015's case → guarded probe C)
    // The stub runs the REAL write-through-symlink + mkdir + mv from
    // spec.feature_dir (= worktree ROOT, dispatch.rs:193-199, NOT the feature
    // subdir; P5). It reaches the main tree via `workspace`/`basename`
    // constructor fields set by the fixture — never by resolving the symlink
    // in run(). ---

    #[cfg(unix)]
    #[derive(Clone, Copy)]
    enum Scenario {
        A,
        B,
        C,
    }

    #[cfg(unix)]
    struct StubArchiveWorker {
        landing: Scenario,
        write_review: bool,
        workspace: PathBuf,
        basename: String,
    }

    #[cfg(unix)]
    #[async_trait]
    impl Worker for StubArchiveWorker {
        async fn run(&self, spec: FeatureSpec, _cancel: CancellationToken) -> Result<Artifact> {
            // The active review path resolves through the F-008 symlink at
            // <wt>/.ae/features/active/<basename>/ → main-tree active/.
            let active_review = spec
                .feature_dir
                .join(".ae/features/active")
                .join(&self.basename)
                .join("review.md");
            if self.write_review {
                if let Some(p) = active_review.parent() {
                    let _ = std::fs::create_dir_all(p);
                }
                std::fs::write(&active_review, "---\nverdict: pass\n---\n")?;
                match self.landing {
                    // A: faithful repro of the AE relative-path mv that
                    // ENOENT-fails — the target parent done/<basename>/ is NOT
                    // created, so rename returns NotFound and review.md stays at
                    // active/ (probe A's location). IGNORE NotFound; never a
                    // silent no-op skip (codex-MF2). (The plan's illustrative
                    // literal targeted active/<basename>/done_review.md whose
                    // parent exists and would SUCCEED, moving review.md away from
                    // probe A and breaking AC4's control — the documented
                    // behaviour "ENOENT-fail, review.md stays at active/" governs.)
                    Scenario::A => {
                        let target = spec
                            .feature_dir
                            .join(".ae/features/done")
                            .join(&self.basename)
                            .join("review.md");
                        match std::fs::rename(&active_review, &target) {
                            Ok(()) => {}
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                            Err(e) => return Err(e.into()),
                        }
                    }
                    // B: worktree-local done/ — EXACTLY where probe B reads
                    // (`wt.join(".ae/features/done/<basename>/review.md")`).
                    Scenario::B => {
                        let done = spec
                            .feature_dir
                            .join(".ae/features/done")
                            .join(&self.basename);
                        std::fs::create_dir_all(&done)?;
                        std::fs::rename(&active_review, done.join("review.md"))?;
                    }
                    // C: MAIN-tree done/ — EXACTLY where probe C reads
                    // (`workspace.join(".ae/features/done/<basename>/review.md")`).
                    // NOT the main-tree active/ dir (that is probe A's location; P5).
                    Scenario::C => {
                        let done = self
                            .workspace
                            .join(".ae/features/done")
                            .join(&self.basename);
                        std::fs::create_dir_all(&done)?;
                        std::fs::rename(&active_review, done.join("review.md"))?;
                    }
                }
            }
            Ok(Artifact {
                verdict: WorkerVerdict::Pass,
                stdout_path: spec.feature_dir.join("stub.out"),
                reasoning_trace: None,
                duration: Duration::from_millis(1),
                worker_identity: spec.worker_identity,
                exit_code: 0,
                drain_truncated: false,
            })
        }
    }

    /// F-016 AC4/F-010 gotcha: a git-init real-worktree fixture so
    /// `maybe_create_worktree` actually creates a worktree + F-008 symlink. A
    /// non-git tempdir silently SKIPS the worktree path — exactly the gap that
    /// passed f02b0b7's broken fix through review. Returns the TempDir (keep
    /// alive) and the slugged basename; feature dir =
    /// `<tmp>/.ae/features/active/<basename>`.
    #[cfg(unix)]
    fn git_init_feature_fixture() -> (tempfile::TempDir, String) {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        git(ws, &["init", "-q"]);
        git(ws, &["config", "user.email", "t@loom"]);
        git(ws, &["config", "user.name", "t"]);
        std::fs::write(ws.join(".gitignore"), ".ae/\n.loom/\n").unwrap();
        git(ws, &["add", ".gitignore"]);
        git(ws, &["commit", "-q", "-m", "init"]);
        let basename = "F-016-x".to_string();
        let fd = ws.join(".ae/features/active").join(&basename);
        std::fs::create_dir_all(&fd).unwrap();
        std::fs::write(fd.join("plan.md"), "plan").unwrap();
        (tmp, basename)
    }

    /// F-016: drive the REAL `run_one_feature` with a `StubArchiveWorker` over
    /// the git-init fixture (not a shortcut).
    #[cfg(unix)]
    async fn run_archive(
        tmp: &tempfile::TempDir,
        basename: &str,
        landing: Scenario,
        write_review: bool,
    ) -> FeatureOutcome {
        let ws = tmp.path();
        let fd = ws.join(".ae/features/active").join(basename);
        let worker: Arc<dyn Worker> = Arc::new(StubArchiveWorker {
            landing,
            write_review,
            workspace: ws.to_path_buf(),
            basename: basename.to_string(),
        });
        let feature = DiscoveredFeature {
            id: "F-016".into(),
            feature_dir: fd,
            depends_on: vec![],
            work_state: None,
        };
        run_one_feature(
            feature,
            worker,
            "F-016-w0".into(),
            ws,
            CancellationToken::new(),
        )
        .await
        .unwrap()
    }

    /// AC1/AC4: scenario B — the archive lands in the WORKTREE-LOCAL done/
    /// (cleanup-destroyed). The verdict must be read pre-cleanup from probe B.
    /// RED before Step 2 (the single main-active/ read returns "missing").
    #[cfg(unix)]
    #[tokio::test]
    #[allow(non_snake_case)] // A/B/C name the conclusion's landing sites (AC1)
    async fn archived_pass_B_worktree_local_heals() {
        let (tmp, basename) = git_init_feature_fixture();
        let o = run_archive(&tmp, &basename, Scenario::B, true).await;
        assert_eq!(
            o.verdict, "pass",
            "worktree-local done/ archive must heal (probe B, read pre-cleanup)"
        );
    }

    /// AC1/AC4: scenario C — the archive lands in the MAIN-tree done/ (F-015's
    /// case). Verdict read from the guarded probe C (mtime ≥ dispatch_started,
    /// satisfied by this fresh write). RED before Step 2.
    #[cfg(unix)]
    #[tokio::test]
    #[allow(non_snake_case)] // A/B/C name the conclusion's landing sites (AC1)
    async fn archived_pass_C_main_done_heals() {
        let (tmp, basename) = git_init_feature_fixture();
        let o = run_archive(&tmp, &basename, Scenario::C, true).await;
        assert_eq!(
            o.verdict, "pass",
            "main-tree done/ archive must heal (probe C, fresh mtime)"
        );
    }

    /// AC1/AC4: scenario A — the archive mv ENOENT-fails, review.md stays at
    /// main active/ (probe A). No-regression control: already GREEN today.
    #[cfg(unix)]
    #[tokio::test]
    #[allow(non_snake_case)] // A/B/C name the conclusion's landing sites (AC1/AC4)
    async fn scenario_A_unarchived_still_reads() {
        let (tmp, basename) = git_init_feature_fixture();
        let o = run_archive(&tmp, &basename, Scenario::A, true).await;
        assert_eq!(
            o.verdict, "pass",
            "unarchived review at main active/ still reads (probe A)"
        );
    }

    /// AC3 (gate / single discriminator): a STALE prior-cycle done/ pass + a
    /// worker that writes NO review this run yields "missing" — the probe-C
    /// freshness guard refuses to certify a stale leftover. Pre-seed
    /// `workspace/.ae/features/done/<basename>/review.md` (pass) and back-date its
    /// mtime by 1h (no sleep — deterministic + NTP-rewind-immune), then run a
    /// no-review worker. Bare-C (no guard) = RED ("pass"); guarded-C = GREEN.
    #[cfg(unix)]
    #[tokio::test]
    async fn probe_c_does_not_heal_stale_prior_cycle_review() {
        let (tmp, basename) = git_init_feature_fixture();
        let done = tmp.path().join(".ae/features/done").join(&basename);
        std::fs::create_dir_all(&done).unwrap();
        let stale = done.join("review.md");
        std::fs::write(&stale, "---\nverdict: pass\n---\n").unwrap();
        // Back-date the stale review well before any plausible dispatch_started
        // (captured at run_one_feature entry, ≈ now). No sleep needed.
        let f = std::fs::File::options().write(true).open(&stale).unwrap();
        f.set_modified(std::time::SystemTime::now() - Duration::from_secs(3600))
            .unwrap();

        // Worker writes NO review this run (write_review: false) → genuinely
        // missing; only the stale prior-cycle done/ file exists, and it must NOT
        // heal.
        let o = run_archive(&tmp, &basename, Scenario::A, false).await;
        assert_eq!(
            o.verdict, "missing",
            "a stale prior-cycle done/ pass must never certify as a fresh pass"
        );
    }

    /// AC3 (positive control, paired with the stale gate above): a FRESH
    /// main-tree done/ archive (mtime ≥ dispatch_started by construction) DOES
    /// heal — proves the guard rejects stale WITHOUT also rejecting the F-015
    /// case it exists to fix.
    #[cfg(unix)]
    #[tokio::test]
    #[allow(non_snake_case)] // C names the conclusion's landing site (AC3)
    async fn fresh_archive_C_heals_f015() {
        let (tmp, basename) = git_init_feature_fixture();
        let o = run_archive(&tmp, &basename, Scenario::C, true).await;
        assert_eq!(
            o.verdict, "pass",
            "a fresh main-tree done/ archive heals (guard accepts mtime ≥ dispatch_started)"
        );
    }
}
