//! F-023 run journal — crash-durable NDJSON record of per-run worker lifecycle.
//!
//! A non-graceful process death (SIGKILL / OOM-kill / power-loss) kills the
//! orchestrator before Phase-6 dispatch-log delivery runs, leaving no durable
//! record of the run. The run journal is that durability layer: an append-only
//! `.loom/journal-<run_id>.ndjson`, written per-event with `sync_all`, scanned
//! on the next startup to reconstruct the lost run's outcome.
//!
//! This module owns the journal handle + `run_id` mint. Event emission lands in
//! Step 3 (`RunJournal::append`); startup recovery in Step 4.

use crate::atomic_write::fsync_parent_dir;
use anyhow::{Context, Result};
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Per-run journal handle.
///
/// `writer` is `Arc<Mutex<File>>` (not a bare `Mutex<File>`) because Step 3's
/// `append` must run the lock-acquire + `write_all` + `sync_all` sequence
/// entirely inside a `tokio::task::spawn_blocking` closure — a `MutexGuard` is
/// `!Send` and cannot cross the blocking boundary, so the closure takes its own
/// clone of the `Arc`.
pub struct RunJournal {
    pub run_id: String,
    pub path: PathBuf,
    pub writer: Arc<Mutex<File>>,
}

impl RunJournal {
    /// Mint a fresh `run_id`, create `<loom_dir>/journal-<run_id>.ndjson` in
    /// append mode, and fsync the parent dir so the new file's directory entry
    /// is itself durable (matching the power-loss guarantee the journal exists
    /// to provide).
    ///
    /// Call AFTER `init_tracing` AND AFTER startup recovery (Step 4): recovery
    /// must not observe this run's own freshly-created empty journal, or it
    /// would misclassify it as an orphan on every invocation.
    pub fn create(loom_dir: &Path) -> Result<Self> {
        // codex Step-2 P1: persist `.loom`'s OWN directory entry (in the
        // workspace dir) BEFORE creating any file inside it. `create_dir_all`
        // fsyncs nothing, and the per-file parent fsync below only persists the
        // journal's entry WITHIN `.loom` — a power-loss could still lose the
        // whole `.loom` subtree, journal included, if `.loom`'s own entry never
        // reached disk. Idempotent + cheap on subsequent runs (`.loom` already
        // durable). Requires `loom_dir` to already exist (the entry points
        // `create_dir_all` it before calling here).
        fsync_parent_dir(loom_dir)?;
        let run_id = mint_run_id();
        let path = loom_dir.join(format!("journal-{run_id}.ndjson"));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("create run journal {:?}", path))?;
        fsync_parent_dir(&path)?;
        Ok(Self {
            run_id,
            path,
            writer: Arc::new(Mutex::new(file)),
        })
    }

    /// Append a `worker-start` event (the worker is about to run for `feature_id`).
    pub async fn worker_start(&self, feature_id: &str) -> Result<()> {
        self.append(JournalRecord {
            event: "worker-start",
            run_id: &self.run_id,
            feature_id,
            ts_ms: now_ms(),
            worker_exit_status: None,
            verdict: None,
            ref_name: None,
        })
        .await
    }

    /// Append a `worker-finish` event. `worker_exit_status` is recorded as an
    /// OPAQUE STRING (whatever the dispatch arms already produce — no
    /// panic-specific value); `verdict` is the AE review judgment.
    pub async fn worker_finish(
        &self,
        feature_id: &str,
        worker_exit_status: &str,
        verdict: &str,
    ) -> Result<()> {
        self.append(JournalRecord {
            event: "worker-finish",
            run_id: &self.run_id,
            feature_id,
            ts_ms: now_ms(),
            worker_exit_status: Some(worker_exit_status),
            verdict: Some(verdict),
            ref_name: None,
        })
        .await
    }

    /// Append a `rescue-ref-written` event (a worker's commits were preserved
    /// under `ref_name` before worktree cleanup).
    pub async fn rescue_ref_written(&self, feature_id: &str, ref_name: &str) -> Result<()> {
        self.append(JournalRecord {
            event: "rescue-ref-written",
            run_id: &self.run_id,
            feature_id,
            ts_ms: now_ms(),
            worker_exit_status: None,
            verdict: None,
            ref_name: Some(ref_name),
        })
        .await
    }

    /// Serialize ONE record to a single NDJSON line OUTSIDE the lock, then run
    /// the entire lock-acquire + `write_all` + `sync_all` sequence inside one
    /// `spawn_blocking` closure. A `std::sync::MutexGuard` is `!Send`, so the
    /// guard must never cross an `.await`; cloning the `Arc<Mutex<File>>` into
    /// the blocking closure keeps the lock's whole lifetime on the blocking
    /// thread. `sync_all` per event is the crash-durability guarantee (AC8).
    async fn append(&self, record: JournalRecord<'_>) -> Result<()> {
        let line = format!("{}\n", serde_json::to_string(&record)?);
        let writer = Arc::clone(&self.writer);
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let mut guard = writer.lock().expect("run journal mutex poisoned");
            guard.write_all(line.as_bytes())?;
            guard.sync_all()
        })
        .await
        .context("join journal append task")?
        .context("write/sync journal record")
    }
}

/// One NDJSON journal line. A flat, `event`-tagged record (not an enum) keeps
/// Step 4's recovery parse simple: every line has `event` + `run_id` +
/// `feature_id` + `ts_ms`; event-specific fields are omitted when absent.
#[derive(Serialize)]
struct JournalRecord<'a> {
    event: &'a str,
    run_id: &'a str,
    feature_id: &'a str,
    ts_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    worker_exit_status: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    verdict: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ref_name: Option<&'a str>,
}

/// Milliseconds since the Unix epoch (event timestamp). `0` on a clock error —
/// the journal records best-effort timing, never fails on a clock read.
fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// Mint a `run_id` with enough entropy to avoid same-second filename
/// collisions: `<unix-millis>-<pid>`. Pure std (no new crate) — millis from
/// `SystemTime`, pid from `std::process`. The millis prefix keeps ids
/// lexically sortable; the pid disambiguates two runs minted in the same
/// millisecond from different processes.
fn mint_run_id() -> String {
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("{ms}-{}", std::process::id())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_mints_run_id_and_makes_journal_file() {
        let dir = tempfile::tempdir().unwrap();
        let j = RunJournal::create(dir.path()).unwrap();
        assert!(j.path.exists(), "journal file must exist after create");
        let name = j.path.file_name().unwrap().to_str().unwrap();
        assert!(
            name.contains(&j.run_id),
            "journal filename {name:?} must embed run_id {:?}",
            j.run_id
        );
        assert!(name.starts_with("journal-") && name.ends_with(".ndjson"));
    }

    #[test]
    fn create_fsyncs_freshly_made_loom_dir_entry() {
        // codex Step-2 P1: a freshly-created `.loom` under a workspace — create
        // must persist `.loom`'s own entry (fsync the workspace dir) before
        // writing the journal inside it. True power-loss isn't unit-testable;
        // this proves the loom-parent fsync runs without error on a brand-new
        // `.loom` and the journal still lands.
        let workspace = tempfile::tempdir().unwrap();
        let loom_dir = workspace.path().join(".loom");
        std::fs::create_dir_all(&loom_dir).unwrap();
        let j = RunJournal::create(&loom_dir).unwrap();
        assert!(j.path.exists());
        assert_eq!(j.path.parent().unwrap(), loom_dir);
    }

    #[test]
    fn mint_run_id_has_millis_pid_shape() {
        let id = mint_run_id();
        let (ms, pid) = id.split_once('-').expect("run_id is <millis>-<pid>");
        assert!(ms.parse::<u128>().is_ok(), "millis part is numeric");
        assert!(pid.parse::<u32>().is_ok(), "pid part is numeric");
    }

    #[tokio::test]
    async fn append_writes_exactly_n_well_formed_ndjson_records() {
        // AC8: N appends → exactly N newline-terminated, well-formed JSON lines,
        // no truncation. Every line carries event + run_id + feature_id + ts_ms;
        // event-specific fields appear only on their variant.
        let dir = tempfile::tempdir().unwrap();
        let j = RunJournal::create(dir.path()).unwrap();
        j.worker_start("F-001").await.unwrap();
        j.rescue_ref_written("F-001", "refs/heads/loom-rescue/F-001-timeout")
            .await
            .unwrap();
        j.worker_finish("F-001", "timeout", "unknown")
            .await
            .unwrap();

        let body = std::fs::read_to_string(&j.path).unwrap();
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 3, "exactly 3 records, no truncation");

        for line in &lines {
            let v: serde_json::Value = serde_json::from_str(line).expect("each line is valid JSON");
            assert_eq!(v["run_id"], j.run_id);
            assert_eq!(v["feature_id"], "F-001");
            assert!(v["ts_ms"].is_number());
        }

        let v0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(v0["event"], "worker-start");
        assert!(
            v0.get("worker_exit_status").is_none(),
            "worker-start omits worker_exit_status"
        );

        let v1: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(v1["event"], "rescue-ref-written");
        assert_eq!(v1["ref_name"], "refs/heads/loom-rescue/F-001-timeout");

        let v2: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(v2["event"], "worker-finish");
        // Opaque status string — recorded verbatim, no panic-specific branch.
        assert_eq!(v2["worker_exit_status"], "timeout");
        assert_eq!(v2["verdict"], "unknown");
    }
}
