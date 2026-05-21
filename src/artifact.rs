//! Worker I/O types: `FeatureSpec` (input) + `Artifact` (output) + `WorkerVerdict`.
//!
//! Filled in at Step 4 of plan F-001. `verdict.rs` (separate module) is where
//! Step 6 Phase 4 implements the AE-review-verdict file listener; the
//! `WorkerVerdict` enum here describes the worker subprocess's own outcome
//! (exit status / timed out), distinct from the AE review verdict.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// What Loom hands to a `Worker` to identify the unit of work.
///
/// `feature_dir` points at an AE feature directory under `.ae/features/active/`.
/// `worker_identity` is opaque but stable across retries (used for tracing +
/// per-feature artifact path naming).
///
/// `dispatch_metadata` is a flexible per-adapter config bag. **v0.1 contract**:
/// `ClaudeCodeAdapter` ignores this field and treats `Null` as the expected
/// value; any other shape is silently accepted but unused. v0.2+ adapters
/// (Codex / Gemini / oMLX) will define typed sub-schemas — the shape lock is
/// a v0.2 prerequisite before adding the second adapter (Track 1 architecture
/// review, Step 4).
#[derive(Debug, Clone)]
pub struct FeatureSpec {
    pub feature_dir: PathBuf,
    pub worker_identity: String,
    pub dispatch_metadata: serde_yaml::Value,
}

/// Outcome of a single worker subprocess invocation.
///
/// Named `WorkerVerdict` (not `Verdict`) to avoid collision with the future
/// `verdict.rs` content that will describe AE-review verdicts read from
/// `review.md` frontmatter (Step 6 Phase 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkerVerdict {
    /// Subprocess exited with status 0.
    Pass,
    /// Subprocess exited with a non-zero status.
    Fail,
    /// Subprocess was killed by Loom because it exceeded its timeout.
    Timeout,
    /// Subprocess was cancelled by the dispatcher via `CancellationToken`.
    Cancelled,
}

/// What a `Worker` returns after running a `FeatureSpec`.
#[derive(Debug)]
pub struct Artifact {
    pub verdict: WorkerVerdict,
    /// Path to the structured stdout file Loom wrote on behalf of the worker.
    pub stdout_path: PathBuf,
    /// Optional reasoning trace (stderr or worker-emitted JSON), if any.
    pub reasoning_trace: Option<String>,
    pub duration: Duration,
    pub worker_identity: String,
    /// Raw exit code (-1 if killed by signal / cancelled).
    pub exit_code: i32,
    /// True when at least one drain task failed to deliver its full output
    /// (timeout, IO error, or join panic). The stdout / stderr files are
    /// best-effort partial on `true`. Distinguishes "worker produced no
    /// output" from "worker output was lost during drain" — load-bearing for
    /// Step 6's verdict aggregation (Track 1 P1 + Track 4 strategic, Step 4).
    pub drain_truncated: bool,
}
