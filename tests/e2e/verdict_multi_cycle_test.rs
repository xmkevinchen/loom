//! e2e tests for F-002 — verdict watcher wired into the iteration loop.
//!
//! Two distinct tests:
//!
//! - [`multi_cycle_dag_converges_with_stub_worker`] (Step 5a, AC1): direct
//!   library call to [`loom_rt::iteration::run_iteration_loop`]. A custom
//!   in-test [`StubWriteVerdictWorker`] writes `review.md` + updates
//!   `index.md` per dispatched feature. Asserts the 3-feature linear DAG
//!   converges in ≤ 6 cycles and the returned tuple shows no AE review fail.
//!
//! - [`phase_markers_appear_in_run_log`] (Step 5b, AC3): subprocess `loom run`
//!   against a pre-staged 1-feature workspace whose `review.md` is already
//!   `verdict: pass`. The subprocess uses the real global tracing init and
//!   writes a JSON log file; the test reads `.loom/run-*.log` and asserts
//!   all 6 phase strings appear. Subprocess sidesteps the
//!   `tracing_subscriber::set_default` limitation across spawned threads.

use anyhow::Result;
use async_trait::async_trait;
use loom_rt::artifact::{Artifact, FeatureSpec, WorkerVerdict};
use loom_rt::atomic_write::atomic_write;
use loom_rt::discovery::read_active_features;
use loom_rt::iteration::{run_iteration_loop, IterationOutcome, LoomContext};
use loom_rt::worker::Worker;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Stub worker that simulates "ae:work followed by ae:review verdict: pass":
/// writes `review.md` with `verdict: pass` AND updates `index.md` to
/// `pipeline.work: done`, then returns a Pass artifact. Matches the v0.0.2
/// "stub worker double-write" production form documented in the F-002 plan.
struct StubWriteVerdictWorker;

#[async_trait]
impl Worker for StubWriteVerdictWorker {
    async fn run(&self, spec: FeatureSpec, _cancel: CancellationToken) -> Result<Artifact> {
        let review = spec.feature_dir.join("review.md");
        atomic_write(&review, b"---\nverdict: pass\n---\nstub review\n")?;

        // Rewrite index.md flipping pipeline.work to done. Read existing,
        // patch the work line (or append a pipeline block if missing).
        let index = spec.feature_dir.join("index.md");
        let existing = std::fs::read_to_string(&index)?;
        let patched = patch_pipeline_work_done(&existing);
        atomic_write(&index, patched.as_bytes())?;

        let stdout = spec.feature_dir.join(".stub-stdout");
        atomic_write(&stdout, b"stub-ok\n")?;

        Ok(Artifact {
            verdict: WorkerVerdict::Pass,
            stdout_path: stdout,
            reasoning_trace: None,
            duration: Duration::from_millis(1),
            worker_identity: spec.worker_identity,
            exit_code: 0,
            drain_truncated: false,
        })
    }
}

/// Replace `work: <anything>` under `pipeline:` with `work: done`. If no
/// `pipeline:` block exists, append one. Simple line-based patcher — the test
/// fixtures are stable so this doesn't need YAML round-tripping.
fn patch_pipeline_work_done(input: &str) -> String {
    if input.contains("  work:") {
        input
            .lines()
            .map(|line| {
                if line.trim_start().starts_with("work:") {
                    "  work: done".to_string()
                } else {
                    line.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n"
    } else {
        // Find the closing `---` of frontmatter, insert pipeline block before it.
        let mut lines: Vec<&str> = input.lines().collect();
        if lines.len() >= 2 && lines[0] == "---" {
            if let Some(end_idx) = lines.iter().skip(1).position(|l| *l == "---") {
                let insert_at = end_idx + 1;
                lines.insert(insert_at, "pipeline:");
                lines.insert(insert_at + 1, "  work: done");
            }
        }
        lines.join("\n") + "\n"
    }
}

fn write_feature(features_root: &Path, id_slug: &str, frontmatter_id: &str, deps: &[&str]) {
    let dir = features_root.join(id_slug);
    std::fs::create_dir_all(&dir).unwrap();
    let mut fm = format!("---\nid: {frontmatter_id}\n");
    if !deps.is_empty() {
        fm.push_str("depends_on:\n");
        for d in deps {
            fm.push_str(&format!("  - {d}\n"));
        }
    }
    fm.push_str("pipeline:\n  work: in_progress\n---\n\nbody\n");
    std::fs::write(dir.join("index.md"), fm).unwrap();
}

#[tokio::test]
async fn multi_cycle_dag_converges_with_stub_worker() {
    let tmp = tempfile::tempdir().unwrap();
    let workspace = tmp.path().to_path_buf();
    let features_root = workspace.join(".ae/features/active");

    // Linear DAG: F-101 → F-102 → F-103. Frontmatter id matches the directory
    // basename so a future schema change couldn't pass on a mismatch.
    write_feature(&features_root, "F-101", "F-101", &[]);
    write_feature(&features_root, "F-102", "F-102", &["F-101"]);
    write_feature(&features_root, "F-103", "F-103", &["F-102"]);

    let loom_dir = workspace.join(".loom");
    std::fs::create_dir_all(&loom_dir).unwrap();

    let workers: Vec<Arc<dyn Worker>> = vec![Arc::new(StubWriteVerdictWorker)];
    let ctx = LoomContext {
        workspace: workspace.clone(),
        loom_dir,
        workers,
        max_parallel: 4,
    };

    let cancel = CancellationToken::new();
    let IterationOutcome {
        reports,
        ae_review_failed,
    } = run_iteration_loop(&ctx, &cancel).await.unwrap();

    // (a) No AE review fail — all stubs wrote verdict: pass.
    assert!(
        !ae_review_failed,
        "ae_review_failed should be false; all stubs wrote verdict: pass"
    );

    // (b) Convergence: cycles bounded by ≤ 6 (linear 3-feature DAG, ~1
    //     dispatch + 1 drain-or-scan-to-unblock per feature). reports.len()
    //     == number of cycles that produced a dispatch (the DAG-exhausted
    //     and pause-on-fail exit paths do not append a final report).
    assert!(
        reports.len() <= 6,
        "loop should converge in ≤ 6 cycles, got {}",
        reports.len()
    );

    // (c) Total dispatched outcomes = 3 (one per feature), all Pass.
    let total_dispatched: usize = reports.iter().map(|r| r.dispatched_count).sum();
    assert_eq!(
        total_dispatched, 3,
        "expected 3 features dispatched across cycles, got {total_dispatched}"
    );
    for report in &reports {
        for outcome in &report.outcomes {
            // F-010: verdict is now the AE review judgment (stub wrote review.md
            // verdict: pass); worker_exit_status is the process signal.
            assert_eq!(
                outcome.verdict, "pass",
                "feature {} should have AE verdict pass, got {}",
                outcome.feature_id, outcome.verdict
            );
            assert_eq!(
                outcome.worker_exit_status, "pass",
                "feature {} worker process should be pass, got {}",
                outcome.feature_id, outcome.worker_exit_status
            );
        }
    }

    // (d) End state on disk: all three features have pipeline.work: done.
    let final_features = read_active_features(&workspace).unwrap();
    assert_eq!(final_features.len(), 3);
    for f in &final_features {
        assert!(
            f.is_done(),
            "feature {} should be is_done() at end, work_state = {:?}",
            f.id,
            f.work_state
        );
    }
}

#[test]
fn phase_markers_appear_in_run_log() {
    let tmp = tempfile::tempdir().unwrap();
    let workspace = tmp.path().to_path_buf();
    let features_root = workspace.join(".ae/features/active");

    // 1-feature workspace; review.md pre-staged with verdict: pass so the
    // per-cycle scan classifies it terminal on cycle 1 without any worker
    // run. The dispatch will still see one feature in the read_active set
    // but it'll be marked done by terminal_pass before reaching the worker.
    write_feature(&features_root, "F-110", "F-110", &[]);
    std::fs::write(
        features_root.join("F-110/review.md"),
        "---\nverdict: pass\n---\n",
    )
    .unwrap();

    // Ensure the binary is built. CARGO_BIN_EXE_loom is set by cargo for
    // tests in the loom-rt crate. Run with current_dir = tempdir so the
    // .loom/run-*.log lands in our tempdir, not the project dir.
    let bin = env!("CARGO_BIN_EXE_loom");
    let output = StdCommand::new(bin)
        .arg("run")
        .arg("phase-marker-smoke")
        .current_dir(&workspace)
        .output()
        .expect("subprocess loom run should spawn");

    // The run may exit 0 (DAG exhausted via terminal_pass) or non-zero if
    // discovery fails (no `claude` on PATH and stub fallback). What we care
    // about is the log file existing with phase markers.
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        workspace.join(".loom").exists(),
        "subprocess should create .loom dir; stderr: {stderr}; stdout: {stdout}"
    );

    let log_path = find_latest_log(&workspace.join(".loom"))
        .expect("at least one .loom/run-*.log should exist after subprocess run");
    let log_contents =
        std::fs::read_to_string(&log_path).unwrap_or_else(|e| panic!("read log {log_path:?}: {e}"));

    for marker in &[
        "phase: discovery",
        "phase: scheduling",
        "phase: execution",
        "phase: aggregate_decide",
        "phase: iteration",
        "phase: delivery",
    ] {
        assert!(
            log_contents.contains(marker),
            "log {log_path:?} missing marker {marker:?}\n\
             ----- log content -----\n{log_contents}\n----- end log -----"
        );
    }
}

/// Stub worker that simulates "ae:work followed by ae:review verdict: fail":
/// writes `review.md` with `verdict: fail` AND returns Artifact{verdict:Fail}.
/// Used to exercise the AC4 dual-failure path (worker fail AND review fail in
/// the same cycle → verdict-fail must win the exit-code precedence).
struct StubFailDualWriteWorker;

#[async_trait]
impl Worker for StubFailDualWriteWorker {
    async fn run(&self, spec: FeatureSpec, _cancel: CancellationToken) -> Result<Artifact> {
        let review = spec.feature_dir.join("review.md");
        atomic_write(&review, b"---\nverdict: fail\n---\nstub fail review\n")?;
        let stdout = spec.feature_dir.join(".stub-stdout");
        atomic_write(&stdout, b"stub-fail\n")?;
        Ok(Artifact {
            verdict: WorkerVerdict::Fail,
            stdout_path: stdout,
            reasoning_trace: None,
            duration: Duration::from_millis(1),
            worker_identity: spec.worker_identity,
            exit_code: 1,
            drain_truncated: false,
        })
    }
}

/// AC4 dual-failure regression: when a worker exits Fail AND writes
/// `review.md` with `verdict: fail` in the same dispatch, `run_iteration_loop`
/// MUST set `ae_review_failed = true` so the caller picks
/// `EXIT_AE_REVIEW_REJECTED = 5` over `EXIT_DISPATCH_HAD_FAILURE = 4`.
/// Regression for the pre-fixup bug where the loop broke on `any_fail`
/// before the post-dispatch drain+scan could observe the verdict.
#[tokio::test]
async fn dual_failure_review_verdict_wins_over_worker_fail() {
    let tmp = tempfile::tempdir().unwrap();
    let workspace = tmp.path().to_path_buf();
    let features_root = workspace.join(".ae/features/active");

    write_feature(&features_root, "F-120", "F-120", &[]);

    let loom_dir = workspace.join(".loom");
    std::fs::create_dir_all(&loom_dir).unwrap();

    let workers: Vec<Arc<dyn Worker>> = vec![Arc::new(StubFailDualWriteWorker)];
    let ctx = LoomContext {
        workspace: workspace.clone(),
        loom_dir,
        workers,
        max_parallel: 4,
    };

    let cancel = CancellationToken::new();
    let IterationOutcome {
        reports,
        ae_review_failed,
    } = run_iteration_loop(&ctx, &cancel).await.unwrap();

    // F-010: the "worker produced a fail" intent is the PROCESS signal, now in
    // worker_exit_status (what iteration::any_fail classifies for the mid-loop
    // pause). The AE review-fail is asserted separately via ae_review_failed below.
    let fail_outcomes: usize = reports
        .iter()
        .flat_map(|r| r.outcomes.iter())
        .filter(|o| o.worker_exit_status == "fail")
        .count();
    assert!(
        fail_outcomes >= 1,
        "stub worker should have produced >=1 worker_exit_status fail, got {fail_outcomes}"
    );

    assert!(
        ae_review_failed,
        "ae_review_failed MUST be true when review.md was written verdict:fail \
         in the same cycle as worker fail (AC4 precedence)"
    );
}

fn find_latest_log(loom_dir: &Path) -> Option<PathBuf> {
    let entries = std::fs::read_dir(loom_dir).ok()?;
    let mut candidates: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("run-") && n.ends_with(".log"))
                .unwrap_or(false)
        })
        .collect();
    candidates.sort();
    candidates.pop()
}
