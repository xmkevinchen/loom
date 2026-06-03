//! `loom` binary entry point.
//!
//! Thin dispatch shell over `loom_rt::cli`. Each subcommand is handled by a
//! function below; the CLI shape itself lives in `src/cli.rs`. Exit codes
//! are documented on `loom_rt::cli` and applied here via
//! [`std::process::exit`].

use anyhow::{Context, Result};
use clap::Parser;
use loom_rt::cli::{
    Cli, Command, EXIT_AE_REVIEW_REJECTED, EXIT_CANCELLED, EXIT_DEPS_STUCK,
    EXIT_DISPATCH_HAD_FAILURE, EXIT_GENERIC_ERROR, EXIT_RECURSION_DETECTED, EXIT_REVIEW_MISSING,
    EXIT_WORKSPACE_NOT_INITIALIZED,
};
use loom_rt::delivery::write_dispatch_log;
use loom_rt::discovery::{discover_features, read_active_features, DiscoveredFeature};
use loom_rt::dispatch::{prune_stale_worktrees, run_dispatch_loop, DispatchReport};
use loom_rt::iteration::{aggregate_reports, run_iteration_loop, IterationOutcome, LoomContext};
use loom_rt::worker::Worker;
use loom_rt::worker_claude_code::ClaudeCodeAdapter;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let exit_code = match dispatch(cli).await {
        Ok(code) => code,
        Err(e) => {
            // Tracing may not be initialized if init_tracing itself failed.
            eprintln!("loom: error: {e:#}");
            EXIT_GENERIC_ERROR
        }
    };
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

async fn dispatch(cli: Cli) -> Result<i32> {
    let log_path = init_tracing()?;
    tracing::info!(
        name = "loom-rt",
        version = env!("CARGO_PKG_VERSION"),
        log_file = %log_path.display(),
        "loom runtime starting",
    );

    match cli.command {
        None => {
            println!(
                "loom-rt v{} — tracing initialized → {}",
                env!("CARGO_PKG_VERSION"),
                log_path.display()
            );
            Ok(0)
        }
        Some(Command::Run { goal }) => {
            if is_loom_spawned_subprocess() {
                eprintln!(
                    "loom: refusing to run; LOOM_PARENT_PID is set — \
                     this process was spawned by Loom and cannot recursively \
                     spawn workers"
                );
                return Ok(EXIT_RECURSION_DETECTED);
            }
            run_command(&goal).await
        }
        Some(Command::Dispatch { ids }) => {
            if is_loom_spawned_subprocess() {
                eprintln!(
                    "loom: refusing to dispatch; LOOM_PARENT_PID is set — \
                     this process was spawned by Loom and cannot recursively \
                     spawn workers"
                );
                return Ok(EXIT_RECURSION_DETECTED);
            }
            dispatch_command(&ids).await
        }
        Some(Command::Status) => status_command(),
        Some(Command::Version) => {
            println!(
                "loom-rt v{} ({})",
                env!("CARGO_PKG_VERSION"),
                build_profile()
            );
            Ok(0)
        }
    }
}

async fn run_command(goal: &str) -> Result<i32> {
    let workspace = std::env::current_dir().context("get cwd")?;
    let loom_dir = workspace.join(".loom");
    std::fs::create_dir_all(&loom_dir).with_context(|| format!("create {:?}", loom_dir))?;

    // Reclaim orphan worktrees from a prior crashed run before we create any
    // new ones this cycle (BL-005).
    prune_stale_worktrees(&workspace).await;

    tracing::info!(goal, workspace = %workspace.display(), "run: starting 6-phase loop");

    // Phase 1: Discovery.
    tracing::info!("phase: discovery — invoking ae:backlog + ae:analyze");
    let features = discover_features(goal, &workspace).await?;
    tracing::info!(features = features.len(), "run: discovery complete");
    if features.is_empty() {
        println!(
            "no features found under .ae/features/active/ — nothing to dispatch. \
             Stage features manually, or ensure `claude` is on PATH so Discovery \
             can invoke /ae:backlog + /ae:analyze to populate features/active/."
        );
        return Ok(0);
    }

    // Phases 2-5: dispatch + iteration loop.
    let workers: Vec<Arc<dyn Worker>> = vec![Arc::new(default_worker())];
    let ctx = LoomContext {
        workspace: workspace.clone(),
        loom_dir: loom_dir.clone(),
        workers,
        max_parallel: 4,
    };

    let cancel = CancellationToken::new();
    install_sigint_handler(cancel.clone());

    let IterationOutcome {
        reports,
        ae_review_failed,
    } = run_iteration_loop(&ctx, &cancel).await?;

    // Phase 6: Delivery.
    tracing::info!("phase: delivery — writing dispatch log");
    let aggregated = aggregate_reports(reports);
    let log_path = write_dispatch_log(&aggregated, &loom_dir)?;
    println!("dispatch log → {}", log_path.display());
    println!("status → {}", loom_dir.join("status.json").display());

    // Authoritative post-loop cancel read — the SAME mechanism dispatch_command
    // uses (main.rs dispatch arm), so a late SIGINT (incl. one landing during a
    // clean DAG-exhausted exit) signals 130 on both entry points.
    Ok(decide_exit(
        ae_review_failed,
        cancel.is_cancelled(),
        &aggregated,
        false, // deps_stuck — wired in F-013 Step 2 (IterationOutcome threading)
        false, // review_missing — detection wired by F-014
    ))
}

/// Decide the process exit code from the iteration loop's failure + cancel
/// signals. The single exit-decision point for BOTH `loom run` and
/// `loom dispatch`, so the two entry points agree on cancellation handling.
///
/// Precedence (highest first): AE-review failure (5) → worker-execution failure
/// (4) → operator cancel (130) → deps-stuck (7) → review-missing (8) →
/// success (0). A substantive failure outranks a cancel so a real worker/review
/// failure is never hidden behind a 130; the incomplete-run signals (7/8) are
/// the weakest non-zero outcomes, appended strictly below cancel so no shipped
/// meaning shifts (F-013; cli.rs append-only contract). Deps-stuck wins the
/// 7-vs-8 combined case — a stuck DAG is the root cause that explains absent
/// reviews downstream. See cli.rs exit-code table. Pure function, fully
/// testable.
fn decide_exit(
    ae_review_failed: bool,
    cancelled: bool,
    report: &DispatchReport,
    deps_stuck: bool,
    review_missing: bool,
) -> i32 {
    if ae_review_failed {
        return EXIT_AE_REVIEW_REJECTED;
    }
    let failure_code = exit_code_for_report(report);
    if failure_code != 0 {
        failure_code
    } else if cancelled {
        EXIT_CANCELLED
    } else if deps_stuck {
        EXIT_DEPS_STUCK
    } else if review_missing {
        EXIT_REVIEW_MISSING
    } else {
        0
    }
}

async fn dispatch_command(ids: &[String]) -> Result<i32> {
    let workspace = std::env::current_dir().context("get cwd")?;
    let active_dir = workspace.join(".ae").join("features").join("active");
    if !active_dir.exists() {
        eprintln!(
            "loom: workspace not initialized — {} does not exist",
            active_dir.display()
        );
        return Ok(EXIT_WORKSPACE_NOT_INITIALIZED);
    }
    let loom_dir = workspace.join(".loom");
    std::fs::create_dir_all(&loom_dir).with_context(|| format!("create {:?}", loom_dir))?;

    // Reclaim orphan worktrees from a prior crashed run before dispatch (BL-005).
    prune_stale_worktrees(&workspace).await;

    let all = read_active_features(&workspace)?;
    let wanted: std::collections::HashSet<&str> = ids.iter().map(String::as_str).collect();
    let selected: Vec<DiscoveredFeature> = all
        .into_iter()
        .filter(|f| wanted.contains(f.id.as_str()))
        .collect();

    let found_ids: std::collections::HashSet<&str> =
        selected.iter().map(|f| f.id.as_str()).collect();
    let missing: Vec<&str> = ids
        .iter()
        .map(String::as_str)
        .filter(|id| !found_ids.contains(id))
        .collect();
    if !missing.is_empty() {
        eprintln!(
            "loom: dispatch: feature(s) not found under .ae/features/active/: {}",
            missing.join(", ")
        );
    }
    if selected.is_empty() {
        return Ok(EXIT_GENERIC_ERROR);
    }

    tracing::info!(
        requested = ids.len(),
        matched = selected.len(),
        "dispatch: invoking single-cycle dispatch (Discovery skipped)"
    );

    let workers: Vec<Arc<dyn Worker>> = vec![Arc::new(default_worker())];
    // All clones of a CancellationToken share one cancellation state, so the
    // handler's clone, the dispatch-loop's clone, and the post-loop read below
    // all observe the same SIGINT.
    let cancel = CancellationToken::new();
    install_sigint_handler(cancel.clone());

    let report = run_dispatch_loop(selected, workers, 4, workspace.clone(), cancel.clone()).await?;
    let log_path = write_dispatch_log(&report, &loom_dir)?;
    println!("dispatch log → {}", log_path.display());
    // F-010: surface the AE review verdict on the single-cycle dispatch path.
    // `verdict == "fail"` is the AE judgment (set in run_one_feature from
    // review.md), distinct from `worker_exit_status` — so a process crash
    // (verdict="unknown") stays exit 4 and never a false 5. `loom run` sources
    // its ae_review_failed from the iteration loop's authoritative scan instead.
    //
    // The verdict here is POINT-IN-TIME: run_one_feature read review.md once, just
    // after the worker exited (the worker writes review.md before exiting, so it is
    // present). Unlike `loom run`, dispatch does NOT re-poll for a review written
    // AFTER the worker process exits — a late/async review write is invisible here.
    // Harmless under the v0.1 worker model (review is written in-process before
    // exit); tightening tracked by BL-031.
    let ae_review_failed = report.outcomes.iter().any(|o| o.verdict == "fail");
    // Same decide_exit as `loom run` so both entry points agree. Single dispatch
    // has no between-cycle gap, so the post-loop `is_cancelled()` is sufficient.
    Ok(decide_exit(
        ae_review_failed,
        cancel.is_cancelled(),
        &report,
        false, // deps_stuck — dispatch-path derivation wired in F-013 Step 3
        false, // review_missing — detection wired by F-014
    ))
}

fn status_command() -> Result<i32> {
    let workspace = std::env::current_dir().context("get cwd")?;
    let loom_dir = workspace.join(".loom");
    let status_path = loom_dir.join("status.json");

    if !status_path.exists() {
        println!("no loom run state found (.loom/status.json missing)");
        return Ok(0);
    }

    let raw =
        std::fs::read_to_string(&status_path).with_context(|| format!("read {:?}", status_path))?;
    println!("status file: {}", status_path.display());
    println!("{}", raw.trim_end());

    let (log_count, most_recent) = recent_run_logs(&loom_dir)?;
    if log_count == 0 {
        println!("\nrun logs: none under {}", loom_dir.display());
    } else {
        println!(
            "\nrun logs: {} file(s) under {}",
            log_count,
            loom_dir.display()
        );
        if let Some(p) = most_recent {
            println!("most recent: {}", p.display());
        }
    }
    Ok(0)
}

/// Count `.loom/run-*.log` files and return the lexicographically latest one
/// (timestamp filenames sort chronologically).
fn recent_run_logs(loom_dir: &Path) -> Result<(usize, Option<PathBuf>)> {
    if !loom_dir.exists() {
        return Ok((0, None));
    }
    let mut count = 0usize;
    let mut latest: Option<PathBuf> = None;
    for entry in std::fs::read_dir(loom_dir).with_context(|| format!("read_dir {:?}", loom_dir))? {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !(name.starts_with("run-") && name.ends_with(".log")) {
            continue;
        }
        count += 1;
        latest = match latest {
            None => Some(path),
            Some(prev) => {
                if path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|s| s > prev.file_name().and_then(|n| n.to_str()).unwrap_or(""))
                    .unwrap_or(false)
                {
                    Some(path)
                } else {
                    Some(prev)
                }
            }
        };
    }
    Ok((count, latest))
}

fn exit_code_for_report(report: &DispatchReport) -> i32 {
    // Classifies the WORKER PROCESS signal (`worker_exit_status`), NOT the AE
    // `verdict` field (F-010 split them). An AE review-fail routes to exit 5 via
    // `decide_exit`'s `ae_review_failed`, never through here.
    // NOTE: `"cancelled"` is deliberately absent from this match. Cancellation is
    // never signalled through `worker_exit_status` for exit purposes — it is
    // decided centrally by `decide_exit`'s post-loop `cancel.is_cancelled()`
    // branch (→ EXIT_CANCELLED). Adding `"cancelled"` here would be unreachable
    // dead code under the single-shared-token model (F-009 "Decisions not implemented").
    let any_fail = report.outcomes.iter().any(|o| {
        matches!(
            o.worker_exit_status.as_str(),
            "fail" | "error" | "timeout" | "panic"
        )
    });
    if any_fail {
        EXIT_DISPATCH_HAD_FAILURE
    } else {
        0
    }
}

/// Default v0.1 worker: spawns `claude` (or `/bin/echo` as harmless fallback
/// when claude isn't reachable, so a smoke `loom run "test"` doesn't crash).
///
/// Per AC6 + F-003 Step 1: derives the running binary path from
/// `std::env::current_exe()` and hands it to `with_scrubbed_path`, which uses
/// the per-segment canonical-probe algorithm so the worker subprocess cannot
/// recursively reach Loom via `PATH`.
///
/// Claude Code CLI invocation: `claude -p "<prompt>" --permission-mode
/// bypassPermissions`. The prompt instructs Claude to run `/ae:work` then
/// `/ae:review` so the spawned session both implements the plan and writes
/// the terminal verdict that F-002's watcher reacts to. `bypassPermissions`
/// is required for headless execution — without it Claude would block at
/// the first Bash/Edit permission prompt with no operator to approve.
/// Worker spawns in the feature dir (set by `worker_claude_code::run`) so
/// AE skills like `ae:work` resolve plans via the local `.ae/features/`
/// rather than Loom's own workspace.
fn default_worker() -> ClaudeCodeAdapter {
    let (cmd, args) = if which("claude").is_some() {
        (
            PathBuf::from("claude"),
            vec![
                OsString::from("-p"),
                OsString::from(
                    "Execute /ae:work to complete the plan in this feature directory, \
                     then execute /ae:review to verify it and write the terminal verdict.",
                ),
                OsString::from("--permission-mode"),
                OsString::from("bypassPermissions"),
            ],
        )
    } else {
        (
            PathBuf::from("/bin/echo"),
            vec![OsString::from(
                "[loom stub] claude not on PATH — skipping work",
            )],
        )
    };
    let timeout = Duration::from_secs(60 * 30);
    match loom_binary_path() {
        Some(bin) => ClaudeCodeAdapter::with_scrubbed_path(cmd, args, timeout, bin),
        None => {
            tracing::warn!(
                "default_worker: could not resolve loom binary path; PATH scrub disabled"
            );
            ClaudeCodeAdapter::new(cmd, args, timeout)
        }
    }
}

/// Resolve the running `loom` binary's path so spawn_env can canonicalize
/// it and probe each PATH segment for a `loom` resolving to the same target.
/// Returns `None` only on the rare platform where `current_exe()` is
/// unsupported.
fn loom_binary_path() -> Option<PathBuf> {
    std::env::current_exe().ok()
}

/// Boolean-presence check for the worker-side recursion guard (F-003 Step 2).
///
/// Returns `true` when the current process has `LOOM_PARENT_PID` set in its
/// environment, indicating it was spawned by another `loom` process (see
/// `worker_claude_code::run` for the parent-side injection). The dispatch
/// match arms for `Run` and `Dispatch` consult this and short-circuit to
/// `EXIT_RECURSION_DETECTED = 6` before any dispatch work, while `Status` /
/// `Version` / no-subcommand remain available so diagnostics survive inside
/// worker subprocesses (D-A finding from discussion 001).
fn is_loom_spawned_subprocess() -> bool {
    std::env::var("LOOM_PARENT_PID").is_ok()
}

fn which(name: &str) -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path_var) {
        let p = dir.join(name);
        if p.is_file() {
            return Some(p);
        }
    }
    None
}

fn install_sigint_handler(cancel: CancellationToken) {
    tokio::spawn(async move {
        if let Ok(()) = tokio::signal::ctrl_c().await {
            tracing::warn!("SIGINT received — cancelling iteration loop");
            cancel.cancel();
        }
    });
}

fn build_profile() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

/// Initialize the global tracing subscriber with a stdout fmt layer and a
/// JSON-line file layer at `.loom/run-<UTC-timestamp>.log`. Returns the log
/// file path so the caller can echo it to the user.
fn init_tracing() -> Result<PathBuf> {
    let log_dir = Path::new(".loom");
    std::fs::create_dir_all(log_dir).with_context(|| format!("create log dir {:?}", log_dir))?;

    let timestamp = utc_timestamp(SystemTime::now());
    let log_path = log_dir.join(format!("run-{timestamp}.log"));

    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .with_context(|| format!("open log file {:?}", log_path))?;

    let stdout_layer = fmt::layer().with_target(false);
    let file_layer = fmt::layer().json().with_target(false).with_writer(file);

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(stdout_layer)
        .with(file_layer)
        .init();

    Ok(log_path)
}

/// Format `now` as a filesystem-safe UTC timestamp: `YYYYMMDDTHHMMSSZ`.
/// Pure-std implementation (no `chrono` / `time` crate) — civil-from-days
/// algorithm by Howard Hinnant (http://howardhinnant.github.io/date_algorithms.html).
fn utc_timestamp(now: SystemTime) -> String {
    let secs = now
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let time_of_day = secs.rem_euclid(86_400);
    let hour = (time_of_day / 3600) as u32;
    let minute = ((time_of_day % 3600) / 60) as u32;
    let second = (time_of_day % 60) as u32;

    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y_offset = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = y_offset + if month <= 2 { 1 } else { 0 };

    format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z")
}

#[cfg(test)]
mod tests {
    use super::{decide_exit, utc_timestamp};
    use loom_rt::cli::{
        EXIT_AE_REVIEW_REJECTED, EXIT_CANCELLED, EXIT_DEPS_STUCK, EXIT_DISPATCH_HAD_FAILURE,
        EXIT_REVIEW_MISSING,
    };
    use loom_rt::dispatch::{DispatchReport, FeatureOutcome};
    use std::path::PathBuf;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn utc_timestamp_epoch() {
        assert_eq!(utc_timestamp(UNIX_EPOCH), "19700101T000000Z");
    }

    #[test]
    fn utc_timestamp_known_moment() {
        let t = UNIX_EPOCH + Duration::from_secs(1_705_322_096);
        assert_eq!(utc_timestamp(t), "20240115T123456Z");
    }

    #[test]
    fn utc_timestamp_leap_year_feb29() {
        let t = UNIX_EPOCH + Duration::from_secs(1_709_164_800);
        assert_eq!(utc_timestamp(t), "20240229T000000Z");
    }

    fn report_with(verdicts: &[&str]) -> DispatchReport {
        let outcomes: Vec<FeatureOutcome> = verdicts
            .iter()
            .enumerate()
            .map(|(i, v)| FeatureOutcome {
                feature_id: format!("F-{i}"),
                worker_identity: "test".into(),
                // F-009 cells assert PROCESS-failure exit codes → the `v` string
                // is the worker_exit_status; verdict (AE) is "unknown" (no review).
                verdict: "unknown".into(),
                worker_exit_status: (*v).to_string(),
                exit_code: if matches!(*v, "pass") { 0 } else { 1 },
                duration_ms: 0,
                stdout_path: PathBuf::new(),
                drain_truncated: false,
                error: None,
            })
            .collect();
        DispatchReport {
            started_at_ms: 0,
            elapsed_ms: 0,
            dispatched_count: outcomes.len(),
            outcomes,
        }
    }

    /// F-010: build a one-outcome report with explicit (AE verdict, process
    /// worker_exit_status) so the `loom dispatch` exit-5 derivation can be tested.
    fn report_with_ae(verdict: &str, worker_exit_status: &str) -> DispatchReport {
        DispatchReport {
            started_at_ms: 0,
            elapsed_ms: 0,
            dispatched_count: 1,
            outcomes: vec![FeatureOutcome {
                feature_id: "F-0".into(),
                worker_identity: "test".into(),
                verdict: verdict.into(),
                worker_exit_status: worker_exit_status.into(),
                exit_code: 0,
                duration_ms: 0,
                stdout_path: PathBuf::new(),
                drain_truncated: false,
                error: None,
            }],
        }
    }

    /// F-010 (Step 2): `dispatch_command` derives ae_review_failed from the
    /// report's AE verdict, so a review-fail → exit 5 on the single-cycle path.
    #[test]
    fn dispatch_ae_verdict_fail_drives_exit_five() {
        let report = report_with_ae("fail", "pass");
        let ae_review_failed = report.outcomes.iter().any(|o| o.verdict == "fail");
        assert!(ae_review_failed);
        assert_eq!(
            decide_exit(ae_review_failed, false, &report, false, false),
            EXIT_AE_REVIEW_REJECTED
        );
    }

    /// F-010 (AC2): a crash leaves verdict=unknown (not "fail"), so the dispatch
    /// derivation does NOT fire exit 5 — the worker failure stays exit 4.
    #[test]
    fn dispatch_crash_unknown_verdict_stays_four_not_five() {
        let report = report_with_ae("unknown", "fail");
        let ae_review_failed = report.outcomes.iter().any(|o| o.verdict == "fail");
        assert!(
            !ae_review_failed,
            "crash leaves verdict=unknown, not a review-fail"
        );
        assert_eq!(
            decide_exit(ae_review_failed, false, &report, false, false),
            EXIT_DISPATCH_HAD_FAILURE
        );
    }

    /// AC4 truth table: all four cells of (worker_pass/fail × ae_pass/fail).
    /// Verdict-fail wins the dual-condition case — see cli.rs precedence rule.
    #[test]
    fn decide_exit_worker_pass_ae_pass_returns_zero() {
        assert_eq!(
            decide_exit(false, false, &report_with(&["pass"]), false, false),
            0
        );
    }

    #[test]
    fn decide_exit_worker_fail_ae_pass_returns_four() {
        assert_eq!(
            decide_exit(false, false, &report_with(&["fail"]), false, false),
            EXIT_DISPATCH_HAD_FAILURE
        );
    }

    #[test]
    fn decide_exit_worker_pass_ae_fail_returns_five() {
        assert_eq!(
            decide_exit(true, false, &report_with(&["pass"]), false, false),
            EXIT_AE_REVIEW_REJECTED
        );
    }

    #[test]
    fn decide_exit_worker_fail_ae_fail_review_wins_returns_five() {
        assert_eq!(
            decide_exit(true, false, &report_with(&["fail"]), false, false),
            EXIT_AE_REVIEW_REJECTED
        );
    }

    /// F-009 cancel-precedence cells (AC1): cancel signals 130 only when nothing
    /// more actionable failed; a worker- or review-fail outranks it.
    #[test]
    fn decide_exit_cancel_no_failure_returns_cancelled() {
        // Core regression: cancelled run with an all-"pass" report → 130, not 0.
        assert_eq!(
            decide_exit(false, true, &report_with(&["pass"]), false, false),
            EXIT_CANCELLED
        );
    }

    #[test]
    fn decide_exit_worker_fail_outranks_cancel_returns_four() {
        assert_eq!(
            decide_exit(false, true, &report_with(&["fail"]), false, false),
            EXIT_DISPATCH_HAD_FAILURE
        );
    }

    #[test]
    fn decide_exit_review_fail_outranks_cancel_returns_five() {
        assert_eq!(
            decide_exit(true, true, &report_with(&["pass"]), false, false),
            EXIT_AE_REVIEW_REJECTED
        );
    }

    /// F-013 incomplete-run cells — targeted precedence coverage, not exhaustive.
    /// Full chain under test: 5 > 4 > 130 > 7 (deps-stuck) > 8 (review-missing) > 0,
    /// appended strictly below cancel per the cli.rs append-only contract
    /// ("existing meanings will not shift").
    #[test]
    fn decide_exit_deps_stuck_alone_returns_seven() {
        assert_eq!(
            decide_exit(false, false, &report_with(&["pass"]), true, false),
            EXIT_DEPS_STUCK
        );
    }

    #[test]
    fn decide_exit_review_missing_alone_returns_eight() {
        assert_eq!(
            decide_exit(false, false, &report_with(&["pass"]), false, true),
            EXIT_REVIEW_MISSING
        );
    }

    #[test]
    fn decide_exit_deps_stuck_wins_over_review_missing() {
        // Combined case: deps-stuck is the root cause (a stuck DAG explains
        // absent reviews downstream; the reverse doesn't hold).
        assert_eq!(
            decide_exit(false, false, &report_with(&["pass"]), true, true),
            EXIT_DEPS_STUCK
        );
    }

    #[test]
    fn decide_exit_cancel_outranks_deps_stuck() {
        assert_eq!(
            decide_exit(false, true, &report_with(&["pass"]), true, false),
            EXIT_CANCELLED
        );
    }

    #[test]
    fn decide_exit_cancel_outranks_review_missing() {
        assert_eq!(
            decide_exit(false, true, &report_with(&["pass"]), false, true),
            EXIT_CANCELLED
        );
    }

    #[test]
    fn decide_exit_review_fail_outranks_incomplete_signals() {
        assert_eq!(
            decide_exit(true, false, &report_with(&["pass"]), true, true),
            EXIT_AE_REVIEW_REJECTED
        );
    }

    #[test]
    fn decide_exit_worker_fail_outranks_both_incomplete_signals() {
        assert_eq!(
            decide_exit(false, false, &report_with(&["fail"]), true, true),
            EXIT_DISPATCH_HAD_FAILURE
        );
    }

    #[test]
    fn decide_exit_worker_fail_outranks_deps_stuck() {
        // Independent 4-outranks-7 assertion (plan review M2).
        assert_eq!(
            decide_exit(false, false, &report_with(&["fail"]), true, false),
            EXIT_DISPATCH_HAD_FAILURE
        );
    }

    #[test]
    fn decide_exit_worker_fail_outranks_review_missing() {
        // Independent 4-outranks-8 assertion (plan review M2).
        assert_eq!(
            decide_exit(false, false, &report_with(&["fail"]), false, true),
            EXIT_DISPATCH_HAD_FAILURE
        );
    }

    /// AC1 end-to-end chain (F-009 review — closes the gap between "loop bails on
    /// cancel" and "process exits 130"): a fired token drives the real
    /// `run_command` exit pipeline — `run_iteration_loop` → post-loop
    /// `cancel.is_cancelled()` → `decide_exit` — to `EXIT_CANCELLED`. Covers the
    /// linkage the unit truth-table and the iteration-level loop-break test each
    /// only touch in isolation. (The pre-existing SIGINT handler + the `main()`
    /// i32→process-exit mapping are unchanged by F-009 and out of scope here.)
    #[tokio::test]
    async fn cancelled_loop_outcome_drives_decide_exit_to_cancelled() {
        use loom_rt::iteration::{aggregate_reports, run_iteration_loop, LoomContext};
        use tokio_util::sync::CancellationToken;

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
        cancel.cancel(); // operator SIGINT, modelled by a fired token

        let outcome = run_iteration_loop(&ctx, &cancel).await.unwrap();
        let aggregated = aggregate_reports(outcome.reports);
        assert_eq!(
            decide_exit(
                outcome.ae_review_failed,
                cancel.is_cancelled(),
                &aggregated,
                false,
                false
            ),
            EXIT_CANCELLED,
            "a fired token must drive the loop→decide_exit chain to EXIT_CANCELLED"
        );
    }
}
