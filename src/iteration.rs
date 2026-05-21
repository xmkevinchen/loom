//! Phase 5 — Iteration controller.
//!
//! Loop: collect ready features → dispatch → wait for verdicts → update
//! `.loom/status.json` atomically → check exit condition. Repeat until DAG
//! exhausted OR cancel token fired (SIGINT). All Loom on-disk writes go
//! through `atomic_write`.

use crate::discovery::{read_active_features, DiscoveredFeature};
use crate::dispatch::{run_dispatch_loop, DispatchReport, FeatureOutcome};
use crate::state::StatusSnapshot;
use crate::worker::Worker;
use anyhow::Result;
use std::collections::BTreeMap;
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

/// Run the iteration controller until the DAG is exhausted or cancelled.
///
/// Returns an aggregated list of dispatch reports (one per cycle) for the
/// final delivery phase.
pub async fn run_iteration_loop(
    ctx: &LoomContext,
    cancel: CancellationToken,
) -> Result<Vec<DispatchReport>> {
    let mut reports: Vec<DispatchReport> = Vec::new();
    let mut cycle: u64 = 0;

    loop {
        if cancel.is_cancelled() {
            info!("iteration: cancelled before next cycle");
            break;
        }
        cycle += 1;

        let features = read_active_features(&ctx.workspace)?;
        write_status(ctx, cycle, "dispatch", &features)?;

        let ready_count = features.iter().filter(|f| !f.is_done()).count();
        if ready_count == 0 {
            info!(cycle, "iteration: DAG exhausted (no incomplete features)");
            write_status(ctx, cycle, "done", &features)?;
            break;
        }

        let report = run_dispatch_loop(
            features.clone(),
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
            // gate everything. Pause-and-notify path (Step 6 Phase 4 policy
            // default for `fail`). We don't implement a verdict-driven
            // pump in v0.1; just exit to avoid busy-looping.
            warn!(
                cycle,
                "iteration: no features ready though work remains — deps stuck. Exiting."
            );
            write_status(ctx, cycle, "blocked", &features)?;
            reports.push(report);
            break;
        }

        // TODO v0.2: wire `verdict::watch_verdicts` as the trigger source.
        // v0.1 derives the fail signal from worker exit codes directly and
        // relies on workers having written their reviews before we re-read.
        // The notify-driven listener is fully implemented in src/verdict.rs
        // (unit-tested with terminal-state filter + 3× retry) but not yet
        // connected to LoomContext — mid-execution `ae:review` state
        // transitions are therefore not observed in v0.1. Acceptable v0.1
        // scope per QA review; revisit when multi-producer telemetry lands.
        let any_fail = report
            .outcomes
            .iter()
            .any(|o| matches!(o.verdict.as_str(), "fail" | "error" | "timeout"));
        reports.push(report);

        if any_fail {
            warn!(
                cycle,
                "iteration: at least one feature failed — pause-and-notify"
            );
            let final_features = read_active_features(&ctx.workspace)?;
            write_status(ctx, cycle, "paused_on_fail", &final_features)?;
            break;
        }

        // Yield briefly so notify events from this cycle's writes settle
        // before we re-read the DAG.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    Ok(reports)
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
