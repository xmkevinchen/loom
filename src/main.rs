//! `loom` binary entry point.
//!
//! Step 5 wired structured logging. Step 6 adds the `loom run "<goal>"`
//! subcommand: discover → iterate → deliver. Other subcommands (status,
//! dispatch, etc.) are Step 7.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use loom_rt::delivery::write_dispatch_log;
use loom_rt::discovery::discover_features;
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

#[derive(Parser, Debug)]
#[command(name = "loom", version, about = "AE meta-harness orchestrator")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the 6-phase loop against the given high-level goal.
    Run {
        /// The natural-language goal handed to `ae:backlog` + `ae:analyze`.
        goal: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
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
            Ok(())
        }
        Some(Command::Run { goal }) => run_command(&goal).await,
    }
}

async fn run_command(goal: &str) -> Result<()> {
    let workspace = std::env::current_dir().context("get cwd")?;
    let loom_dir = workspace.join(".loom");
    std::fs::create_dir_all(&loom_dir)
        .with_context(|| format!("create {:?}", loom_dir))?;

    tracing::info!(goal, workspace = %workspace.display(), "run: starting 6-phase loop");

    // Phase 1: Discovery.
    let features = discover_features(goal, &workspace).await?;
    tracing::info!(features = features.len(), "run: discovery complete");
    if features.is_empty() {
        println!(
            "no features found under .ae/features/active/ — nothing to dispatch. \
             (Stage features manually or wait for AE-BL #1 to enable headless ae:backlog.)"
        );
        return Ok(());
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

    let reports = run_iteration_loop(&ctx, cancel).await?;

    // Phase 6: Delivery.
    let aggregated = aggregate_reports(reports);
    let log_path = write_dispatch_log(&aggregated, &loom_dir)?;
    println!("dispatch log → {}", log_path.display());
    println!("status → {}", loom_dir.join("status.json").display());
    Ok(())
}

/// Default v0.1 worker: spawns `claude` (or `/bin/echo` as harmless fallback
/// when claude isn't reachable, so a smoke `loom run "test"` doesn't crash).
///
/// Per AC6: derives the running binary's parent dir from
/// `std::env::current_exe()` and hands it to `with_scrubbed_path`, so the
/// worker subprocess cannot recursively reach Loom via `PATH`.
fn default_worker() -> ClaudeCodeAdapter {
    let (cmd, args) = if which("claude").is_some() {
        (
            PathBuf::from("claude"),
            vec![OsString::from("--headless"), OsString::from("ae:work")],
        )
    } else {
        (
            PathBuf::from("/bin/echo"),
            vec![OsString::from("[loom stub] claude not on PATH — skipping work")],
        )
    };
    let timeout = Duration::from_secs(60 * 30);
    match loom_bin_dir() {
        Some(dir) => ClaudeCodeAdapter::with_scrubbed_path(cmd, args, timeout, dir),
        None => {
            tracing::warn!(
                "default_worker: could not resolve loom binary dir; PATH scrub disabled"
            );
            ClaudeCodeAdapter::new(cmd, args, timeout)
        }
    }
}

/// Resolve the directory containing the running `loom` binary so we can
/// strip it from spawned workers' PATH. Returns `None` only on the rare
/// platform where `current_exe()` is unsupported.
fn loom_bin_dir() -> Option<PathBuf> {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()))
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

/// Initialize the global tracing subscriber with a stdout fmt layer and a
/// JSON-line file layer at `.loom/run-<UTC-timestamp>.log`. Returns the log
/// file path so the caller can echo it to the user.
fn init_tracing() -> Result<PathBuf> {
    let log_dir = Path::new(".loom");
    std::fs::create_dir_all(log_dir)
        .with_context(|| format!("create log dir {:?}", log_dir))?;

    let timestamp = utc_timestamp(SystemTime::now());
    let log_path = log_dir.join(format!("run-{timestamp}.log"));

    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .with_context(|| format!("open log file {:?}", log_path))?;

    let stdout_layer = fmt::layer().with_target(false);
    let file_layer = fmt::layer()
        .json()
        .with_target(false)
        .with_writer(file);

    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

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

    format!(
        "{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z"
    )
}

#[cfg(test)]
mod tests {
    use super::utc_timestamp;
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
}
