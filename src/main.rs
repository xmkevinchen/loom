//! `loom` binary entry point.
//!
//! Thin dispatch shell over `loom_rt::cli`. Each subcommand is handled by a
//! function below; the CLI shape itself lives in `src/cli.rs`. Exit codes
//! are documented on `loom_rt::cli` and applied here via
//! [`std::process::exit`].

use anyhow::{Context, Result};
use clap::Parser;
use loom_rt::cli::{
    Cli, Command, EXIT_AE_REVIEW_REJECTED, EXIT_DISPATCH_HAD_FAILURE, EXIT_GENERIC_ERROR,
    EXIT_RECURSION_DETECTED, EXIT_WORKSPACE_NOT_INITIALIZED,
};
use loom_rt::delivery::write_dispatch_log;
use loom_rt::discovery::{discover_features, read_active_features, DiscoveredFeature};
use loom_rt::dispatch::{run_dispatch_loop, DispatchReport};
use loom_rt::iteration::{aggregate_reports, run_iteration_loop, LoomContext};
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

    tracing::info!(goal, workspace = %workspace.display(), "run: starting 6-phase loop");

    // Phase 1: Discovery.
    tracing::info!("phase: discovery — invoking ae:backlog + ae:analyze");
    let features = discover_features(goal, &workspace).await?;
    tracing::info!(features = features.len(), "run: discovery complete");
    if features.is_empty() {
        println!(
            "no features found under .ae/features/active/ — nothing to dispatch. \
             (Stage features manually or wait for AE-BL #1 to enable headless ae:backlog.)"
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

    let (reports, ae_review_failed) = run_iteration_loop(&ctx, cancel).await?;

    // Phase 6: Delivery.
    tracing::info!("phase: delivery — writing dispatch log");
    let aggregated = aggregate_reports(reports);
    let log_path = write_dispatch_log(&aggregated, &loom_dir)?;
    println!("dispatch log → {}", log_path.display());
    println!("status → {}", loom_dir.join("status.json").display());

    Ok(decide_exit(ae_review_failed, &aggregated))
}

/// Decide the process exit code from the iteration loop's two failure signals.
///
/// AE-review failure takes precedence over worker-execution failure — operator
/// must address the verdict before retry (see cli.rs exit code table for the
/// precedence rule). Pure function, fully testable.
fn decide_exit(ae_review_failed: bool, report: &DispatchReport) -> i32 {
    if ae_review_failed {
        EXIT_AE_REVIEW_REJECTED
    } else {
        exit_code_for_report(report)
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
    let cancel = CancellationToken::new();
    install_sigint_handler(cancel.clone());

    let report = run_dispatch_loop(selected, workers, 4, workspace.clone(), cancel).await?;
    let log_path = write_dispatch_log(&report, &loom_dir)?;
    println!("dispatch log → {}", log_path.display());
    Ok(exit_code_for_report(&report))
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
    let any_fail = report
        .outcomes
        .iter()
        .any(|o| matches!(o.verdict.as_str(), "fail" | "error" | "timeout" | "panic"));
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
fn default_worker() -> ClaudeCodeAdapter {
    let (cmd, args) = if which("claude").is_some() {
        (
            PathBuf::from("claude"),
            vec![OsString::from("--headless"), OsString::from("ae:work")],
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
    use loom_rt::cli::{EXIT_AE_REVIEW_REJECTED, EXIT_DISPATCH_HAD_FAILURE};
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
                verdict: (*v).to_string(),
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

    /// AC4 truth table: all four cells of (worker_pass/fail × ae_pass/fail).
    /// Verdict-fail wins the dual-condition case — see cli.rs precedence rule.
    #[test]
    fn decide_exit_worker_pass_ae_pass_returns_zero() {
        assert_eq!(decide_exit(false, &report_with(&["pass"])), 0);
    }

    #[test]
    fn decide_exit_worker_fail_ae_pass_returns_four() {
        assert_eq!(
            decide_exit(false, &report_with(&["fail"])),
            EXIT_DISPATCH_HAD_FAILURE
        );
    }

    #[test]
    fn decide_exit_worker_pass_ae_fail_returns_five() {
        assert_eq!(
            decide_exit(true, &report_with(&["pass"])),
            EXIT_AE_REVIEW_REJECTED
        );
    }

    #[test]
    fn decide_exit_worker_fail_ae_fail_review_wins_returns_five() {
        assert_eq!(
            decide_exit(true, &report_with(&["fail"])),
            EXIT_AE_REVIEW_REJECTED
        );
    }
}
