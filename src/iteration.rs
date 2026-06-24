//! Phases 4-5 — Aggregate+decide + Iteration controller.
//!
//! Loop: drain verdict watcher → per-cycle review.md scan → collect ready
//! features → dispatch → write `.loom/status.json` → check exit condition.
//! Repeat until DAG exhausted OR cancel token fired (SIGINT) OR an AE
//! review wrote `verdict: fail`. All Loom on-disk writes go through
//! `atomic_write`.
//!
//! **Two-tier verdict correctness model**: the per-cycle `parse_review_once`
//! disk scan is the AUTHORITATIVE source of terminal state; the
//! [`crate::verdict::watch_verdicts`] notify channel is a latency
//! optimization. The scan recovers from channel saturation drops, CI
//! notify backend flakiness, and any other reason the watcher missed an
//! event. See F-002 plan for the design rationale.

use crate::discovery::{read_active_features, DiscoveredFeature};
use crate::dispatch::{done_credited_view, run_dispatch_loop, DispatchReport, FeatureOutcome};
use crate::state::StatusSnapshot;
use crate::verdict::{self, AeVerdict};
use crate::worker::Worker;
use anyhow::Result;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Per-run context held across iteration cycles.
pub struct LoomContext {
    pub workspace: PathBuf,
    pub loom_dir: PathBuf,
    pub workers: Vec<Arc<dyn Worker>>,
    pub max_parallel: usize,
}

/// Outcome of one [`run_iteration_loop`] run. Named fields (vs a tuple) keep
/// the loop's signals unambiguous.
///
/// Note: cancellation is intentionally NOT a field here. Both entry points
/// (`run_command`, `dispatch_command`) detect cancel via a single post-loop
/// `cancel.is_cancelled()` read (the loop takes `&CancellationToken` so the
/// caller still owns the token), so a late SIGINT — including one landing
/// during a clean DAG-exhausted exit — is signalled identically on both paths.
/// See `main::decide_exit` and F-009 Step 5.
pub struct IterationOutcome {
    /// One [`DispatchReport`] per dispatched cycle, in order.
    pub reports: Vec<DispatchReport>,
    /// `true` iff at least one feature's `review.md` transitioned to
    /// `verdict: fail` during this run (observed via watcher or per-cycle
    /// scan). The caller maps `true` to [`crate::cli::EXIT_AE_REVIEW_REJECTED`]
    /// — distinct from [`crate::cli::EXIT_DISPATCH_HAD_FAILURE`] which signals
    /// worker-execution failure.
    pub ae_review_failed: bool,
    /// `true` iff the loop exited via the deps-stuck break: work remained but
    /// nothing was dispatchable (the dependency graph gated every pending
    /// feature). The caller maps `true` to [`crate::cli::EXIT_DEPS_STUCK`] —
    /// the weakest non-zero signal, below cancel (F-013; same threading
    /// pattern as `ae_review_failed`).
    pub deps_stuck: bool,
    /// `true` iff ≥1 feature's LAST dispatch outcome carried the F-014
    /// `"missing"` verdict (clean worker, no readable terminal review) AND the
    /// feature never reached a terminal disk verdict (the authoritative
    /// watcher/scan tiers — a review landing after the worker's point-in-time
    /// read heals the feature via `terminal_pass` and must NOT count). The
    /// caller maps `true` to [`crate::cli::EXIT_REVIEW_MISSING`]. Computed once
    /// at loop exit (immune to the loop's multiple break paths by
    /// construction — F-014 plan, Doodlestein cliff 1).
    pub review_missing: bool,
}

/// F-014: exit-time reconciliation for the run path. Walk all outcomes in
/// cycle order keeping the LAST verdict per `feature_id`; a feature counts iff
/// that final verdict is `"missing"` AND its directory basename (bridged via
/// `id_to_basename`, captured at each cycle's feature read — ids and basenames
/// DIFFER) is absent from both terminal sets. Outcomes alone are not canonical:
/// the disk tiers are authoritative, so terminal classification overrides a
/// stale point-in-time "missing". Unknown ids (no basename mapping) count as
/// unresolved — defensive: a feature we cannot bridge cannot be proven healed.
///
/// Documented edge (F-014 plan; codex accumulated review): discovery does NOT
/// enforce `feature_id` uniqueness across dirs. Duplicate ids collapse in both
/// the last-verdict map and `id_to_basename`, so one duplicate's pass could
/// mask the other's missing. Discovery-level dedup is out of scope here —
/// re-file as a BL if observed in practice.
fn unresolved_missing(
    reports: &[DispatchReport],
    terminal_pass: &HashSet<String>,
    terminal_fail: &HashSet<String>,
    id_to_basename: &HashMap<String, String>,
) -> bool {
    let mut last: HashMap<&str, &str> = HashMap::new();
    for r in reports {
        for o in &r.outcomes {
            last.insert(o.feature_id.as_str(), o.verdict.as_str());
        }
    }
    last.iter().any(|(id, verdict)| {
        *verdict == "missing"
            && match id_to_basename.get(*id) {
                Some(basename) => {
                    !terminal_pass.contains(basename) && !terminal_fail.contains(basename)
                }
                None => true,
            }
    })
}

/// Run the iteration controller until the DAG is exhausted or cancelled.
///
/// Takes `cancel` by reference so the caller retains the token to make the
/// authoritative post-loop `cancel.is_cancelled()` exit decision. Returns an
/// [`IterationOutcome`]; see its field docs for the per-signal meaning.
pub async fn run_iteration_loop(
    ctx: &LoomContext,
    cancel: &CancellationToken,
) -> Result<IterationOutcome> {
    let mut reports: Vec<DispatchReport> = Vec::new();
    let mut cycle: u64 = 0;
    let mut terminal_pass: HashSet<String> = HashSet::new();
    let mut terminal_fail: HashSet<String> = HashSet::new();
    let mut ae_review_failed = false;
    let mut deps_stuck = false;
    // F-014: id→basename bridge for the exit-time review_missing reconciliation.
    // Captured at each cycle's feature read while the DiscoveredFeature list is
    // in hand (ids and dir basenames differ — bridge once at capture).
    let mut id_to_basename: HashMap<String, String> = HashMap::new();

    // Phase 4 — spawn the verdict watcher. The guard lives on this function's
    // stack (NOT on LoomContext); when the loop exits the guard drops, the
    // notify watcher tears down, and the std::thread exits cleanly.
    let features_dir = ctx.workspace.join(".ae").join("features").join("active");
    if !features_dir.exists() {
        // Production path (main::run_command) filters via discover_features
        // before reaching us, but run_iteration_loop is a pub library entry
        // point — direct callers (tests, future v0.2 dispatch upgrade) get a
        // friendly error instead of a confusing notify::watch failure.
        anyhow::bail!(
            "features_dir does not exist: {} (workspace not initialized?)",
            features_dir.display()
        );
    }
    let (_watcher_guard, mut rx) = verdict::watch_verdicts(&features_dir)?;

    // Restart idempotency: pre-populate the terminal sets from any review.md
    // files already on disk before the watcher started. Without this, a
    // restarted loom would re-dispatch features whose verdict was written
    // before the watcher came up.
    pre_populate_terminal_sets(&ctx.workspace, &mut terminal_pass, &mut terminal_fail)?;

    loop {
        if cancel.is_cancelled() {
            info!("iteration: cancelled before next cycle");
            break;
        }
        cycle += 1;
        info!(cycle, "phase: iteration — cycle start");

        // Tier 1 (fast path): drain the watcher channel. Order is load-bearing
        // — drain BEFORE read_active_features so events emitted during the
        // previous cycle's dispatch are visible before we compute this cycle's
        // ready set.
        info!(cycle, "phase: aggregate_decide — draining watcher channel");
        while let Ok(evt) = rx.try_recv() {
            let key = evt.feature_id.clone();
            info!(
                feature_id = %key,
                verdict = ?evt.verdict,
                "phase: aggregate_decide — verdict received via watcher"
            );
            apply_verdict(&mut terminal_pass, &mut terminal_fail, key, evt.verdict);
        }

        info!(cycle, "phase: scheduling — reading active feature DAG");
        let features = read_active_features(&ctx.workspace)?;

        // F-014: insert-time id→basename capture for the exit-time
        // review_missing reconciliation (Doodlestein strategic — no lookup
        // layer in the removal path; the map only accumulates).
        for f in &features {
            if let Some(name) = f.feature_dir.file_name().and_then(|n| n.to_str()) {
                // Last capture wins: a mid-run dir rename refreshes the
                // mapping so the exit-time reconciliation compares against the
                // CURRENT basename (codex review P2 — or_insert would pin the
                // stale pre-rename basename and false-positive exit 8).
                id_to_basename.insert(f.id.clone(), name.to_string());
            }
        }

        // Tier 2 (authoritative): per-cycle disk scan of any active feature
        // not already classified. Recovers from notify channel saturation,
        // CI notify backend flakiness, and any other reason the watcher
        // missed an event. O(active_features) disk reads per cycle —
        // negligible at v0.0.2 scale.
        for f in &features {
            let Some(name) = f.feature_dir.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if terminal_pass.contains(name) || terminal_fail.contains(name) {
                continue;
            }
            let review_path = f.feature_dir.join("review.md");
            if let Some(v) = verdict::parse_review_once(&review_path) {
                let key = name.to_owned();
                info!(
                    feature_id = %key,
                    verdict = ?v,
                    "phase: aggregate_decide — verdict observed via scan"
                );
                apply_verdict(&mut terminal_pass, &mut terminal_fail, key, v);
            }
        }

        // Pause-and-notify on AE review fail. Distinct from worker-fail below;
        // returned to the caller via the tuple so it can pick exit code 5
        // instead of 4.
        if !terminal_fail.is_empty() {
            warn!(
                failed_features = ?terminal_fail,
                "iteration: AE review verdict: fail — pause-and-notify"
            );
            write_status(ctx, cycle, "paused_on_ae_fail", &features)?;
            ae_review_failed = true;
            break;
        }

        write_status(ctx, cycle, "dispatch", &features)?;

        // Apply terminal_pass overrides to the feature list so dispatch.rs's
        // ready_set() (which only checks is_done()) naturally filters these
        // out. dispatch.rs stays untouched. Consumes `features` (the
        // post-dispatch scan below re-reads from disk so it sees any verdict
        // files written during dispatch).
        let effective_features = mark_terminally_done(features, &terminal_pass);

        let ready_count = effective_features.iter().filter(|f| !f.is_done()).count();
        info!(cycle, ready_count, "phase: execution — dispatch decision");
        if ready_count == 0 {
            info!(cycle, "iteration: DAG exhausted (no incomplete features)");
            write_status(ctx, cycle, "done", &effective_features)?;
            break;
        }

        let report = run_dispatch_loop(
            // F-017: credit archived-done deps so a dependency AE moved
            // active→done still satisfies downstream `depends_on`. The active-only
            // `ready_count` / deps-stuck checks above are unchanged; only the slice
            // handed to run_dispatch_loop's ready_set gains the done credit.
            done_credited_view(effective_features.clone(), &ctx.workspace),
            ctx.workers.clone(),
            ctx.max_parallel,
            ctx.workspace.clone(),
            cancel.clone(),
        )
        .await?;

        info!(
            cycle,
            dispatched = report.dispatched_count,
            elapsed_ms = report.elapsed_ms as u64,
            "iteration: cycle complete"
        );

        if report.dispatched_count == 0 {
            // No ready set even though incomplete features exist — deps
            // gate everything. Pause-and-notify; exit to avoid busy-looping.
            //
            // Documented edge: an empty `ctx.workers` vec also yields
            // `dispatched_count == 0` and would be labelled deps-stuck here.
            // Unreachable from the loom binary (main.rs always builds
            // default_worker); for library callers the run is genuinely
            // incomplete either way, so the weakest-non-zero label is still
            // honest (F-013).
            let pending: Vec<&str> = effective_features
                .iter()
                .filter(|f| !f.is_done())
                .map(|f| f.id.as_str())
                .collect();
            warn!(
                cycle,
                pending_features = ?pending,
                "iteration: no features ready though work remains — deps stuck. Exiting."
            );
            write_status(ctx, cycle, "blocked", &effective_features)?;
            reports.push(report);
            deps_stuck = true;
            break;
        }

        // Post-dispatch verdict observation. Watcher events from review.md
        // files written DURING this cycle's dispatch land in the channel
        // while run_dispatch_loop is awaiting workers; drain + per-cycle
        // scan once more so the simultaneous worker-fail + verdict-fail
        // case lets verdict-fail win per AC4 ("Both: review-fail wins").
        // Without this, the loop would break on any_fail with
        // ae_review_failed=false → exit 4 instead of 5.
        let post_features = read_active_features(&ctx.workspace)?;
        while let Ok(evt) = rx.try_recv() {
            let key = evt.feature_id.clone();
            info!(
                feature_id = %key,
                verdict = ?evt.verdict,
                "phase: aggregate_decide — verdict received via watcher (post-dispatch)"
            );
            apply_verdict(&mut terminal_pass, &mut terminal_fail, key, evt.verdict);
        }
        for f in &post_features {
            let Some(name) = f.feature_dir.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if terminal_pass.contains(name) || terminal_fail.contains(name) {
                continue;
            }
            let review_path = f.feature_dir.join("review.md");
            if let Some(v) = verdict::parse_review_once(&review_path) {
                let key = name.to_owned();
                info!(
                    feature_id = %key,
                    verdict = ?v,
                    "phase: aggregate_decide — verdict observed via scan (post-dispatch)"
                );
                apply_verdict(&mut terminal_pass, &mut terminal_fail, key, v);
            }
        }

        // Classifies the WORKER PROCESS signal (`worker_exit_status`) for the
        // mid-loop pause — NOT the AE `verdict` (which the per-cycle review.md
        // scan above handles via `terminal_fail`). F-010 split the two fields.
        let any_fail = report
            .outcomes
            .iter()
            .any(|o| matches!(o.worker_exit_status.as_str(), "fail" | "error" | "timeout"));
        reports.push(report);

        // AC4 precedence: verdict-fail wins over worker-fail when both fire
        // in the same cycle. terminal_fail MUST be checked BEFORE any_fail.
        if !terminal_fail.is_empty() {
            warn!(
                failed_features = ?terminal_fail,
                "iteration: AE review verdict: fail observed post-dispatch — pause-and-notify"
            );
            write_status(ctx, cycle, "paused_on_ae_fail", &post_features)?;
            ae_review_failed = true;
            break;
        }

        if any_fail {
            warn!(
                cycle,
                "iteration: at least one feature failed — pause-and-notify"
            );
            write_status(ctx, cycle, "paused_on_fail", &post_features)?;
            break;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // F-014: single exit-time computation — every break path above funnels
    // through here, so no break can bypass the reconciliation (plan cliff 1).
    // Hardening (codex review): consume any still-queued watcher events and
    // run one final authoritative disk scan before reconciling, so a terminal
    // verdict landing in the narrow window after the last per-cycle scan
    // (external writer, or any future loop restructuring) cannot yield a
    // false exit 8. In-model this window is already empty — worker writes
    // land before dispatch returns and the per-cycle Tier-2 scan precedes
    // every break — but the re-scan is O(active features) and removes the
    // entire argument class.
    let terminal_fail_before_final_scan = terminal_fail.len();
    while let Ok(evt) = rx.try_recv() {
        apply_verdict(
            &mut terminal_pass,
            &mut terminal_fail,
            evt.feature_id.clone(),
            evt.verdict,
        );
    }
    pre_populate_terminal_sets(&ctx.workspace, &mut terminal_pass, &mut terminal_fail)?;
    // F-014 review (challenger C1): a fail verdict discovered ONLY by this
    // final window must raise ae_review_failed too — otherwise it would heal
    // "missing" (suppressing exit 8) while the run still reports 4/0 instead
    // of 5. Delta-scoped on purpose: entries already known before the final
    // scan have either fired the in-loop ae-fail break (flag already true) or
    // belong to a prefired-cancel run whose F-009 semantics must not change.
    if terminal_fail.len() > terminal_fail_before_final_scan {
        ae_review_failed = true;
    }
    let review_missing =
        unresolved_missing(&reports, &terminal_pass, &terminal_fail, &id_to_basename);

    Ok(IterationOutcome {
        reports,
        ae_review_failed,
        deps_stuck,
        review_missing,
    })
}

/// Insert `key` into the set matching `verdict`, removing it from the opposite
/// set so a feature whose review.md is rewritten (pass→fail or fail→pass)
/// is correctly reclassified.
fn apply_verdict(
    pass: &mut HashSet<String>,
    fail: &mut HashSet<String>,
    key: String,
    verdict: AeVerdict,
) {
    match verdict {
        AeVerdict::Pass => {
            fail.remove(&key);
            pass.insert(key);
        }
        AeVerdict::Fail => {
            pass.remove(&key);
            fail.insert(key);
        }
    }
}

/// Clone `features` and overwrite `work_state = Some("done")` for any feature
/// whose `feature_dir` basename appears in `pass`. The basename match is
/// intentional — [`crate::verdict::VerdictEvent::feature_id`] is the directory
/// basename (full slug), NOT the frontmatter `id:` (bare `F-NNN`).
fn mark_terminally_done(
    features: Vec<DiscoveredFeature>,
    pass: &HashSet<String>,
) -> Vec<DiscoveredFeature> {
    features
        .into_iter()
        .map(|mut f| {
            let is_terminal = f
                .feature_dir
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|name| pass.contains(name));
            if is_terminal {
                f.work_state = Some("done".into());
            }
            f
        })
        .collect()
}

/// Scan every active feature's `review.md` on disk once and populate the
/// terminal sets accordingly. Called at `run_iteration_loop` entry, before
/// the main loop, so a restarted loom does not re-dispatch features whose
/// verdict was already written.
///
/// Per-feature errors (missing review.md, unreadable file, parse error) are
/// silently dropped — `parse_review_once` returns `None` for all of those.
fn pre_populate_terminal_sets(
    workspace: &std::path::Path,
    pass: &mut HashSet<String>,
    fail: &mut HashSet<String>,
) -> Result<()> {
    let features = read_active_features(workspace)?;
    for f in &features {
        let Some(name) = f.feature_dir.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let review_path = f.feature_dir.join("review.md");
        if let Some(v) = verdict::parse_review_once(&review_path) {
            let key = name.to_owned();
            info!(
                feature_id = %key,
                verdict = ?v,
                "phase: aggregate_decide — pre-populated terminal verdict from disk scan"
            );
            apply_verdict(pass, fail, key, v);
        }
    }
    Ok(())
}

fn write_status(
    ctx: &LoomContext,
    cycle: u64,
    phase: &str,
    features: &[DiscoveredFeature],
) -> Result<()> {
    let mut by_id: BTreeMap<String, String> = BTreeMap::new();
    for f in features {
        by_id.insert(
            f.id.clone(),
            f.work_state.clone().unwrap_or_else(|| "unknown".into()),
        );
    }
    let snap = StatusSnapshot {
        phase: phase.into(),
        cycle,
        features: by_id,
    };
    snap.write_to(&ctx.loom_dir)
}

/// Combine multiple cycle reports into a single `DispatchReport` for
/// delivery. Sums per-cycle elapsed; concatenates outcomes; preserves the
/// earliest start time.
pub fn aggregate_reports(reports: Vec<DispatchReport>) -> DispatchReport {
    if reports.is_empty() {
        return DispatchReport {
            started_at_ms: 0,
            elapsed_ms: 0,
            dispatched_count: 0,
            outcomes: Vec::new(),
        };
    }
    let started_at_ms = reports.iter().map(|r| r.started_at_ms).min().unwrap_or(0);
    let elapsed_ms: u128 = reports.iter().map(|r| r.elapsed_ms).sum();
    let mut outcomes: Vec<FeatureOutcome> = Vec::new();
    for r in reports {
        outcomes.extend(r.outcomes);
    }
    DispatchReport {
        started_at_ms,
        elapsed_ms,
        dispatched_count: outcomes.len(),
        outcomes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feat(id: &str, basename: &str, deps: &[&str], done: bool) -> DiscoveredFeature {
        DiscoveredFeature {
            id: id.into(),
            feature_dir: PathBuf::from(format!(".ae/features/active/{basename}")),
            depends_on: deps.iter().map(|s| s.to_string()).collect(),
            work_state: if done { Some("done".into()) } else { None },
        }
    }

    #[test]
    fn apply_verdict_pass_removes_from_fail() {
        let mut pass: HashSet<String> = HashSet::new();
        let mut fail: HashSet<String> = HashSet::new();
        fail.insert("F-X".into());
        apply_verdict(&mut pass, &mut fail, "F-X".into(), AeVerdict::Pass);
        assert!(pass.contains("F-X"));
        assert!(!fail.contains("F-X"));
    }

    #[test]
    fn apply_verdict_fail_removes_from_pass() {
        let mut pass: HashSet<String> = HashSet::new();
        let mut fail: HashSet<String> = HashSet::new();
        pass.insert("F-Y".into());
        apply_verdict(&mut pass, &mut fail, "F-Y".into(), AeVerdict::Fail);
        assert!(fail.contains("F-Y"));
        assert!(!pass.contains("F-Y"));
    }

    #[test]
    fn mark_terminally_done_matches_basename_not_id() {
        // pass set contains the FULL directory basename (with slug).
        let mut pass: HashSet<String> = HashSet::new();
        pass.insert("F-002-wire-verdict-listener-into-iteration-loop".into());

        let features = vec![feat(
            "F-002",
            "F-002-wire-verdict-listener-into-iteration-loop",
            &[],
            false,
        )];
        let marked = mark_terminally_done(features, &pass);
        assert_eq!(marked.len(), 1);
        assert!(marked[0].is_done());
    }

    #[test]
    fn mark_terminally_done_rejects_bare_id_match() {
        // Bare ID in the pass set must NOT match a feature whose dir basename
        // has a slug — the contract is full-basename only.
        let mut pass: HashSet<String> = HashSet::new();
        pass.insert("F-002".into());

        let features = vec![feat(
            "F-002",
            "F-002-wire-verdict-listener-into-iteration-loop",
            &[],
            false,
        )];
        let marked = mark_terminally_done(features, &pass);
        assert!(!marked[0].is_done());
    }

    #[test]
    fn mark_terminally_done_handles_empty_pass() {
        let pass: HashSet<String> = HashSet::new();
        let features = vec![feat("F-A", "F-A-slug", &[], false)];
        let marked = mark_terminally_done(features, &pass);
        assert!(!marked[0].is_done());
    }

    #[test]
    fn mark_terminally_done_orphaned_verdict_no_op() {
        // pass set references a feature no longer in the active list
        // (deleted between cycles). mark_terminally_done returns the input
        // unchanged — no panic, no spurious mutations.
        let mut pass: HashSet<String> = HashSet::new();
        pass.insert("F-DELETED".into());

        let features = vec![feat("F-A", "F-A-slug", &[], false)];
        let marked = mark_terminally_done(features, &pass);
        assert_eq!(marked.len(), 1);
        assert!(!marked[0].is_done());
    }

    #[test]
    fn pre_populate_terminal_sets_recovers_existing_review() {
        let tmp = tempfile::tempdir().unwrap();
        let feature_dir = tmp.path().join(".ae/features/active/F-901-test");
        std::fs::create_dir_all(&feature_dir).unwrap();
        std::fs::write(
            feature_dir.join("index.md"),
            "---\nid: F-901\npipeline:\n  work: in_progress\n---\n",
        )
        .unwrap();
        std::fs::write(
            feature_dir.join("review.md"),
            "---\nverdict: pass\n---\nbody\n",
        )
        .unwrap();

        let mut pass: HashSet<String> = HashSet::new();
        let mut fail: HashSet<String> = HashSet::new();
        pre_populate_terminal_sets(tmp.path(), &mut pass, &mut fail).unwrap();

        // Basename includes the slug `-test`, NOT the bare `F-901` id.
        assert!(pass.contains("F-901-test"));
        assert!(fail.is_empty());
    }

    #[test]
    fn pre_populate_terminal_sets_skips_missing_review() {
        let tmp = tempfile::tempdir().unwrap();
        let feature_dir = tmp.path().join(".ae/features/active/F-902-slug");
        std::fs::create_dir_all(&feature_dir).unwrap();
        std::fs::write(
            feature_dir.join("index.md"),
            "---\nid: F-902\npipeline:\n  work: in_progress\n---\n",
        )
        .unwrap();
        // No review.md at all.

        let mut pass: HashSet<String> = HashSet::new();
        let mut fail: HashSet<String> = HashSet::new();
        pre_populate_terminal_sets(tmp.path(), &mut pass, &mut fail).unwrap();

        // Missing review.md → silent skip, no entries.
        assert!(pass.is_empty());
        assert!(fail.is_empty());
    }

    #[tokio::test]
    async fn run_iteration_loop_breaks_on_prefired_cancel() {
        // A pre-fired token makes the very first top-of-loop cancel check break
        // before any dispatch (empty reports). The downstream half of the
        // contract — that this fired token drives the full chain to
        // EXIT_CANCELLED — is exercised end-to-end by the `main.rs` test
        // `cancelled_loop_outcome_drives_decide_exit_to_cancelled` (the loop is
        // a library entry point; `decide_exit` lives in the binary crate).
        let tmp = tempfile::tempdir().unwrap();
        let workspace = tmp.path().to_path_buf();
        std::fs::create_dir_all(workspace.join(".ae/features/active")).unwrap();
        let loom_dir = workspace.join(".loom");
        std::fs::create_dir_all(&loom_dir).unwrap();

        let ctx = LoomContext {
            workspace,
            loom_dir,
            workers: Vec::new(),
            max_parallel: 1,
        };

        let cancel = CancellationToken::new();
        cancel.cancel(); // pre-fire: loop breaks at the first top-of-loop check

        let outcome = run_iteration_loop(&ctx, &cancel).await.unwrap();
        assert!(
            outcome.reports.is_empty(),
            "a pre-fired cancel token must break the loop before any dispatch"
        );
        assert!(!outcome.ae_review_failed);
    }

    /// F-013: never actually runs in the deps-stuck fixtures — it exists so the
    /// workers vec is NON-EMPTY, forcing `dispatched_count == 0` to come from
    /// the `ready.is_empty()` branch (dispatch.rs), not `workers.is_empty()`.
    /// An empty-workers fixture would pass the same assertions without
    /// exercising dep-gating at all.
    struct StubNeverRunWorker;

    #[async_trait::async_trait]
    impl crate::worker::Worker for StubNeverRunWorker {
        async fn run(
            &self,
            spec: crate::artifact::FeatureSpec,
            _cancel: CancellationToken,
        ) -> anyhow::Result<crate::artifact::Artifact> {
            Ok(crate::artifact::Artifact {
                verdict: crate::artifact::WorkerVerdict::Pass,
                stdout_path: spec.feature_dir.join("stub.out"),
                reasoning_trace: None,
                duration: Duration::from_millis(1),
                worker_identity: spec.worker_identity,
                exit_code: 0,
                drain_truncated: false,
            })
        }
    }

    /// F-013 AC2: an unsatisfiable dependency gates the only pending feature →
    /// the deps-stuck break fires and the outcome carries `deps_stuck: true`.
    #[tokio::test]
    async fn run_iteration_loop_deps_stuck_sets_flag() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace = tmp.path().to_path_buf();
        let feature_dir = workspace.join(".ae/features/active/F-902-stuck");
        std::fs::create_dir_all(&feature_dir).unwrap();
        // F-900 does not exist anywhere → the dependency can never be done.
        std::fs::write(
            feature_dir.join("index.md"),
            "---\nid: F-902\ndepends_on:\n  - F-900\npipeline:\n  work: in_progress\n---\n",
        )
        .unwrap();
        let loom_dir = workspace.join(".loom");
        std::fs::create_dir_all(&loom_dir).unwrap();

        let ctx = LoomContext {
            workspace,
            loom_dir,
            workers: vec![std::sync::Arc::new(StubNeverRunWorker)],
            max_parallel: 1,
        };
        let cancel = CancellationToken::new();

        let outcome = run_iteration_loop(&ctx, &cancel).await.unwrap();
        assert!(
            outcome.deps_stuck,
            "an unsatisfiable dep must surface as deps_stuck on the outcome"
        );
        assert!(!outcome.ae_review_failed);
    }

    /// F-013 review fixup (challenger probe b): worker that writes a passing
    /// review.md, so cycle 1 makes REAL progress before cycle 2 hits the
    /// deps-stuck arm — covers the mid-run path the first-cycle-stuck fixtures
    /// cannot reach.
    struct StubPassReviewWorker;

    #[async_trait::async_trait]
    impl crate::worker::Worker for StubPassReviewWorker {
        async fn run(
            &self,
            spec: crate::artifact::FeatureSpec,
            _cancel: CancellationToken,
        ) -> anyhow::Result<crate::artifact::Artifact> {
            std::fs::write(
                spec.feature_dir.join("review.md"),
                "---\nverdict: pass\n---\n",
            )?;
            Ok(crate::artifact::Artifact {
                verdict: crate::artifact::WorkerVerdict::Pass,
                stdout_path: spec.feature_dir.join("stub.out"),
                reasoning_trace: None,
                duration: Duration::from_millis(1),
                worker_identity: spec.worker_identity,
                exit_code: 0,
                drain_truncated: false,
            })
        }
    }

    /// F-013 review fixup (challenger probe b): deps-stuck reached MID-RUN,
    /// after a cycle of real progress. Cycle 1 dispatches F-906 (ready, no
    /// deps; worker writes a passing review.md → terminal_pass). Cycle 2:
    /// F-906 is terminally done, F-907 remains (dep F-999 exists nowhere) →
    /// ready_count > 0 but ready_set empty → deps-stuck arm fires with
    /// partial progress already recorded.
    #[tokio::test]
    async fn run_iteration_loop_mid_run_deps_stuck_after_partial_progress() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace = tmp.path().to_path_buf();
        let done_first = workspace.join(".ae/features/active/F-906-progress");
        std::fs::create_dir_all(&done_first).unwrap();
        std::fs::write(
            done_first.join("index.md"),
            "---\nid: F-906\npipeline:\n  work: in_progress\n---\n",
        )
        .unwrap();
        let stuck = workspace.join(".ae/features/active/F-907-stuck");
        std::fs::create_dir_all(&stuck).unwrap();
        std::fs::write(
            stuck.join("index.md"),
            "---\nid: F-907\ndepends_on:\n  - F-999\npipeline:\n  work: in_progress\n---\n",
        )
        .unwrap();
        let loom_dir = workspace.join(".loom");
        std::fs::create_dir_all(&loom_dir).unwrap();

        let ctx = LoomContext {
            workspace,
            loom_dir,
            workers: vec![std::sync::Arc::new(StubPassReviewWorker)],
            max_parallel: 1,
        };
        let cancel = CancellationToken::new();

        let outcome = run_iteration_loop(&ctx, &cancel).await.unwrap();
        assert!(
            outcome.deps_stuck,
            "cycle-2 dep gate must surface as deps_stuck after cycle-1 progress"
        );
        assert!(!outcome.ae_review_failed);
        // Partial progress is real: cycle 1 dispatched F-906; the final
        // (deps-stuck) cycle dispatched nothing.
        assert!(
            outcome.reports.first().map(|r| r.dispatched_count) == Some(1),
            "cycle 1 must have dispatched the ready feature"
        );
        assert_eq!(
            outcome.reports.last().map(|r| r.dispatched_count),
            Some(0),
            "final cycle must be the zero-dispatch deps-stuck cycle"
        );
    }

    /// F-017 AC4(b): end-to-end iteration-path proof that an archived-done
    /// dependency is credited. `active/F-002` depends on `F-001`, which lives
    /// ONLY in `done/` (absent from active/). The done-credited view handed to
    /// run_dispatch_loop must unblock F-002 → it dispatches in cycle 1, and the
    /// run is NOT deps-stuck. (Also empirically refutes the review's
    /// "iteration deps-stuck ignores credit" concern: deps-stuck keys on
    /// post-dispatch `dispatched_count`, which the credit drives to 1.)
    #[tokio::test]
    async fn run_iteration_loop_credits_archived_done_dep() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace = tmp.path().to_path_buf();
        // Dependent in active/, blocked only by an archived-done dependency.
        let dependent = workspace.join(".ae/features/active/F-002-dependent");
        std::fs::create_dir_all(&dependent).unwrap();
        std::fs::write(
            dependent.join("index.md"),
            "---\nid: F-002\ndepends_on:\n  - F-001\npipeline:\n  work: in_progress\n---\n",
        )
        .unwrap();
        // Dependency archived to done/ — NOT present in active/.
        let archived = workspace.join(".ae/features/done/F-001-archived");
        std::fs::create_dir_all(&archived).unwrap();
        std::fs::write(
            archived.join("index.md"),
            "---\nid: F-001\npipeline:\n  work: done\n---\n",
        )
        .unwrap();
        let loom_dir = workspace.join(".loom");
        std::fs::create_dir_all(&loom_dir).unwrap();

        let ctx = LoomContext {
            workspace,
            loom_dir,
            workers: vec![std::sync::Arc::new(StubPassReviewWorker)],
            max_parallel: 1,
        };
        let cancel = CancellationToken::new();

        let outcome = run_iteration_loop(&ctx, &cancel).await.unwrap();
        assert!(
            !outcome.deps_stuck,
            "an archived-done dependency must be credited → F-002 dispatches, not deps-stuck"
        );
        assert_eq!(
            outcome.reports.first().map(|r| r.dispatched_count),
            Some(1),
            "cycle 1 must dispatch the dependent unblocked by the archived-done dep"
        );
        assert_eq!(
            outcome
                .reports
                .first()
                .and_then(|r| r.outcomes.first())
                .map(|o| o.feature_id.as_str()),
            Some("F-002"),
            "the dispatched feature is the dependent F-002"
        );
    }

    /// F-014: outcome fixture for unresolved_missing helper cells.
    fn out(feature_id: &str, verdict: &str) -> FeatureOutcome {
        FeatureOutcome {
            feature_id: feature_id.into(),
            worker_identity: "t".into(),
            verdict: verdict.into(),
            worker_exit_status: "pass".into(),
            exit_code: 0,
            duration_ms: 0,
            stdout_path: PathBuf::from("t.out"),
            drain_truncated: false,
            error: None,
            rescue_ref: None,
        }
    }

    fn rep(outcomes: Vec<FeatureOutcome>) -> DispatchReport {
        DispatchReport {
            started_at_ms: 0,
            elapsed_ms: 0,
            dispatched_count: outcomes.len(),
            outcomes,
        }
    }

    fn b(s: &str) -> String {
        s.to_string()
    }

    /// F-014 helper cells — deterministic unit coverage of the heal race the
    /// loop cannot test via timing (single-thread tokio has no interleave point
    /// between dispatch return and the post-dispatch scan).
    #[test]
    fn unresolved_missing_final_missing_no_terminal_is_true() {
        let reports = vec![rep(vec![out("F-1", "missing")])];
        let map = HashMap::from([(b("F-1"), b("F-1-slug"))]);
        assert!(unresolved_missing(
            &reports,
            &HashSet::new(),
            &HashSet::new(),
            &map
        ));
    }

    #[test]
    fn unresolved_missing_healed_by_disk_scan_is_false() {
        // The codex heal race: only outcome says "missing", but the
        // authoritative scan classified the feature terminal_pass.
        let reports = vec![rep(vec![out("F-1", "missing")])];
        let map = HashMap::from([(b("F-1"), b("F-1-slug"))]);
        let pass = HashSet::from([b("F-1-slug")]);
        assert!(!unresolved_missing(&reports, &pass, &HashSet::new(), &map));
    }

    #[test]
    fn unresolved_missing_redispatch_heal_is_false() {
        // Later outcome for the same feature wins: missing then pass.
        let reports = vec![
            rep(vec![out("F-1", "missing")]),
            rep(vec![out("F-1", "pass")]),
        ];
        let map = HashMap::from([(b("F-1"), b("F-1-slug"))]);
        assert!(!unresolved_missing(
            &reports,
            &HashSet::new(),
            &HashSet::new(),
            &map
        ));
    }

    #[test]
    fn unresolved_missing_empty_and_no_missing_is_false() {
        assert!(!unresolved_missing(
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &HashMap::new()
        ));
        let reports = vec![rep(vec![out("F-1", "pass"), out("F-2", "unknown")])];
        let map = HashMap::from([(b("F-1"), b("F-1-s")), (b("F-2"), b("F-2-s"))]);
        assert!(!unresolved_missing(
            &reports,
            &HashSet::new(),
            &HashSet::new(),
            &map
        ));
    }

    #[test]
    fn unresolved_missing_mixed_one_healed_one_unresolved_is_true() {
        let reports = vec![rep(vec![out("F-1", "missing"), out("F-2", "missing")])];
        let map = HashMap::from([(b("F-1"), b("F-1-s")), (b("F-2"), b("F-2-s"))]);
        let pass = HashSet::from([b("F-1-s")]); // F-1 healed; F-2 unresolved
        assert!(unresolved_missing(&reports, &pass, &HashSet::new(), &map));
    }

    /// F-014: worker that marks its feature `pipeline.work: done` but writes NO
    /// readable terminal review — the realistic run-path route to exit 8 (the
    /// next cycle sees work_state=done → DAG exhausted → clean loop exit with
    /// review_missing=true; every other exit is outranked by 130/4).
    struct StubDoneNoReviewWorker;

    #[async_trait::async_trait]
    impl crate::worker::Worker for StubDoneNoReviewWorker {
        async fn run(
            &self,
            spec: crate::artifact::FeatureSpec,
            _cancel: CancellationToken,
        ) -> anyhow::Result<crate::artifact::Artifact> {
            std::fs::write(
                spec.feature_dir.join("index.md"),
                "---\nid: F-910\npipeline:\n  work: done\n---\n",
            )?;
            Ok(crate::artifact::Artifact {
                verdict: crate::artifact::WorkerVerdict::Pass,
                stdout_path: spec.feature_dir.join("stub.out"),
                reasoning_trace: None,
                duration: Duration::from_millis(1),
                worker_identity: spec.worker_identity,
                exit_code: 0,
                drain_truncated: false,
            })
        }
    }

    /// F-014 loop integration (a): persistent missing — worker completes (marks
    /// work done) but never writes a review → review_missing = true on the
    /// exhausted-DAG exit.
    #[tokio::test]
    async fn run_iteration_loop_done_without_review_sets_review_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace = tmp.path().to_path_buf();
        let feature_dir = workspace.join(".ae/features/active/F-910-noreview");
        std::fs::create_dir_all(&feature_dir).unwrap();
        std::fs::write(
            feature_dir.join("index.md"),
            "---\nid: F-910\npipeline:\n  work: in_progress\n---\n",
        )
        .unwrap();
        let loom_dir = workspace.join(".loom");
        std::fs::create_dir_all(&loom_dir).unwrap();

        let ctx = LoomContext {
            workspace,
            loom_dir,
            workers: vec![std::sync::Arc::new(StubDoneNoReviewWorker)],
            max_parallel: 1,
        };
        let cancel = CancellationToken::new();

        let outcome = run_iteration_loop(&ctx, &cancel).await.unwrap();
        assert!(
            outcome.review_missing,
            "work-done-but-no-review must surface as review_missing"
        );
        assert!(!outcome.ae_review_failed);
        assert!(!outcome.deps_stuck);
    }

    /// F-014: stateful worker — cycle 1 leaves only a `pending` review (no
    /// terminal verdict → "missing"); on re-dispatch it writes a passing review.
    struct StubHealOnSecondRunWorker(std::sync::atomic::AtomicUsize);

    #[async_trait::async_trait]
    impl crate::worker::Worker for StubHealOnSecondRunWorker {
        async fn run(
            &self,
            spec: crate::artifact::FeatureSpec,
            _cancel: CancellationToken,
        ) -> anyhow::Result<crate::artifact::Artifact> {
            let n = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n == 0 {
                std::fs::write(
                    spec.feature_dir.join("review.md"),
                    "---\nverdict: pending\n---\n",
                )?;
            } else {
                std::fs::write(
                    spec.feature_dir.join("review.md"),
                    "---\nverdict: pass\n---\n",
                )?;
            }
            Ok(crate::artifact::Artifact {
                verdict: crate::artifact::WorkerVerdict::Pass,
                stdout_path: spec.feature_dir.join("stub.out"),
                reasoning_trace: None,
                duration: Duration::from_millis(1),
                worker_identity: spec.worker_identity,
                exit_code: 0,
                drain_truncated: false,
            })
        }
    }

    /// F-014 loop integration (b): re-dispatch heal — pending (missing) on
    /// cycle 1, pass on cycle 2 → review_missing = false.
    #[tokio::test]
    async fn run_iteration_loop_redispatch_heal_clears_review_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace = tmp.path().to_path_buf();
        let feature_dir = workspace.join(".ae/features/active/F-911-heals");
        std::fs::create_dir_all(&feature_dir).unwrap();
        std::fs::write(
            feature_dir.join("index.md"),
            "---\nid: F-911\npipeline:\n  work: in_progress\n---\n",
        )
        .unwrap();
        let loom_dir = workspace.join(".loom");
        std::fs::create_dir_all(&loom_dir).unwrap();

        let ctx = LoomContext {
            workspace,
            loom_dir,
            workers: vec![std::sync::Arc::new(StubHealOnSecondRunWorker(
                std::sync::atomic::AtomicUsize::new(0),
            ))],
            max_parallel: 1,
        };
        let cancel = CancellationToken::new();

        let outcome = run_iteration_loop(&ctx, &cancel).await.unwrap();
        assert!(
            !outcome.review_missing,
            "a re-dispatch that produces a passing review must clear the flag"
        );
        assert!(!outcome.ae_review_failed);
    }

    /// F-014 review fixup (architect P2): the exhausted-DAG happy path must
    /// leave review_missing false — already-done features were never
    /// dispatched, so no outcome can carry "missing".
    #[tokio::test]
    async fn run_iteration_loop_exhausted_dag_clears_review_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace = tmp.path().to_path_buf();
        let feature_dir = workspace.join(".ae/features/active/F-913-done");
        std::fs::create_dir_all(&feature_dir).unwrap();
        std::fs::write(
            feature_dir.join("index.md"),
            "---\nid: F-913\npipeline:\n  work: done\n---\n",
        )
        .unwrap();
        let loom_dir = workspace.join(".loom");
        std::fs::create_dir_all(&loom_dir).unwrap();

        let ctx = LoomContext {
            workspace,
            loom_dir,
            workers: vec![std::sync::Arc::new(StubNeverRunWorker)],
            max_parallel: 1,
        };
        let cancel = CancellationToken::new();

        let outcome = run_iteration_loop(&ctx, &cancel).await.unwrap();
        assert!(!outcome.review_missing);
        assert!(!outcome.deps_stuck);
        assert!(!outcome.ae_review_failed);
    }

    /// F-014 review fixup (challenger C3): worker that writes a PASSING review
    /// for one feature and marks the other done-without-review — routes by
    /// feature dir basename.
    struct StubMixedWorker;

    #[async_trait::async_trait]
    impl crate::worker::Worker for StubMixedWorker {
        async fn run(
            &self,
            spec: crate::artifact::FeatureSpec,
            _cancel: CancellationToken,
        ) -> anyhow::Result<crate::artifact::Artifact> {
            let name = spec
                .feature_dir
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();
            if name.contains("healthy") {
                std::fs::write(
                    spec.feature_dir.join("review.md"),
                    "---\nverdict: pass\n---\n",
                )?;
            } else {
                std::fs::write(
                    spec.feature_dir.join("index.md"),
                    "---\nid: F-915\npipeline:\n  work: done\n---\n",
                )?;
            }
            Ok(crate::artifact::Artifact {
                verdict: crate::artifact::WorkerVerdict::Pass,
                stdout_path: spec.feature_dir.join("stub.out"),
                reasoning_trace: None,
                duration: Duration::from_millis(1),
                worker_identity: spec.worker_identity,
                exit_code: 0,
                drain_truncated: false,
            })
        }
    }

    /// F-014 review fixup (challenger C3): mixed two-feature run — one feature
    /// completes with a passing review (healed/terminal), the other completes
    /// without one → review_missing must still be true at the loop level
    /// (exercises the id↔basename bridge + terminal-set comparison together,
    /// not just at the unresolved_missing unit layer).
    #[tokio::test]
    async fn run_iteration_loop_mixed_features_one_missing_sets_flag() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace = tmp.path().to_path_buf();
        let healthy = workspace.join(".ae/features/active/F-914-healthy");
        std::fs::create_dir_all(&healthy).unwrap();
        std::fs::write(
            healthy.join("index.md"),
            "---\nid: F-914\npipeline:\n  work: in_progress\n---\n",
        )
        .unwrap();
        let silent = workspace.join(".ae/features/active/F-915-silent");
        std::fs::create_dir_all(&silent).unwrap();
        std::fs::write(
            silent.join("index.md"),
            "---\nid: F-915\npipeline:\n  work: in_progress\n---\n",
        )
        .unwrap();
        let loom_dir = workspace.join(".loom");
        std::fs::create_dir_all(&loom_dir).unwrap();

        let ctx = LoomContext {
            workspace,
            loom_dir,
            workers: vec![std::sync::Arc::new(StubMixedWorker)],
            max_parallel: 2,
        };
        let cancel = CancellationToken::new();

        let outcome = run_iteration_loop(&ctx, &cancel).await.unwrap();
        assert!(
            outcome.review_missing,
            "one healed feature must not mask the other's missing review"
        );
        assert!(!outcome.ae_review_failed);
    }

    /// F-013 AC2 (negative): a cleanly exhausted DAG (all features done) takes
    /// the "DAG exhausted" break — deps_stuck must stay false.
    #[tokio::test]
    async fn run_iteration_loop_exhausted_dag_is_not_deps_stuck() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace = tmp.path().to_path_buf();
        let feature_dir = workspace.join(".ae/features/active/F-903-done");
        std::fs::create_dir_all(&feature_dir).unwrap();
        std::fs::write(
            feature_dir.join("index.md"),
            "---\nid: F-903\npipeline:\n  work: done\n---\n",
        )
        .unwrap();
        let loom_dir = workspace.join(".loom");
        std::fs::create_dir_all(&loom_dir).unwrap();

        let ctx = LoomContext {
            workspace,
            loom_dir,
            workers: vec![std::sync::Arc::new(StubNeverRunWorker)],
            max_parallel: 1,
        };
        let cancel = CancellationToken::new();

        let outcome = run_iteration_loop(&ctx, &cancel).await.unwrap();
        assert!(
            !outcome.deps_stuck,
            "a cleanly exhausted DAG must not be labelled deps-stuck"
        );
        assert!(!outcome.ae_review_failed);
    }

    #[test]
    fn pre_populate_terminal_sets_classifies_fail_verdict() {
        let tmp = tempfile::tempdir().unwrap();
        let feature_dir = tmp.path().join(".ae/features/active/F-903-fail");
        std::fs::create_dir_all(&feature_dir).unwrap();
        std::fs::write(
            feature_dir.join("index.md"),
            "---\nid: F-903\npipeline:\n  work: in_progress\n---\n",
        )
        .unwrap();
        std::fs::write(feature_dir.join("review.md"), "---\nverdict: fail\n---\n").unwrap();

        let mut pass: HashSet<String> = HashSet::new();
        let mut fail: HashSet<String> = HashSet::new();
        pre_populate_terminal_sets(tmp.path(), &mut pass, &mut fail).unwrap();

        assert!(fail.contains("F-903-fail"));
        assert!(pass.is_empty());
    }
}
