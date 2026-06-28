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

/// BL-054: proof-of-recovery witness token. `RunJournal::create` consumes one by
/// value, so a journal cannot be minted without first calling `recover_orphan_runs`
/// (the token's sole source) — the recover-before-mint ordering invariant becomes a
/// compile-time requirement instead of a comment. Its `pub(crate)` field makes the
/// token unforgeable outside the crate; it is deliberately NOT `Clone`/`Copy` (one
/// recovery authorizes one create). Homed in `journal.rs` (not the recovery module)
/// on purpose: `create` takes it as a parameter type, so homing it elsewhere would
/// make `journal` import the recovery module and revive the dependency cycle BL-053
/// dissolves. Field is `pub(crate)` so the recovery module (its sole intended
/// minter) can construct it; the binary + external crates still cannot forge it.
pub struct RecoveryDone(pub(crate) ());

impl RunJournal {
    /// Mint a fresh `run_id`, create `<loom_dir>/journal-<run_id>.ndjson` in
    /// append mode, and fsync the parent dir so the new file's directory entry
    /// is itself durable (matching the power-loss guarantee the journal exists
    /// to provide).
    ///
    /// Call AFTER `init_tracing` AND AFTER startup recovery (Step 4): recovery
    /// must not observe this run's own freshly-created empty journal, or it
    /// would misclassify it as an orphan on every invocation.
    pub fn create(loom_dir: &Path, _recovery: RecoveryDone) -> Result<Self> {
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
        let j = RunJournal::create(dir.path(), RecoveryDone(())).unwrap();
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
        let j = RunJournal::create(&loom_dir, RecoveryDone(())).unwrap();
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
        let j = RunJournal::create(dir.path(), RecoveryDone(())).unwrap();
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

    #[tokio::test]
    async fn journal_append_lossless_over_fifty_records() {
        // AC1: N sequential appends → exactly N newline-delimited, well-formed
        // JSON records, none truncated or interleaved. Drives past the ≥50-append
        // floor the AC mandates to exercise the `Arc<Mutex<File>>` serialization
        // (O_APPEND alone is not atomic for regular files — the lock is the guarantee).
        let dir = tempfile::tempdir().unwrap();
        let j = RunJournal::create(dir.path(), RecoveryDone(())).unwrap();
        const N: usize = 60;
        for i in 0..N {
            j.worker_start(&format!("F-{i:03}")).await.unwrap();
        }
        let body = std::fs::read_to_string(&j.path).unwrap();
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), N, "exactly N records, none lost or truncated");
        for (i, line) in lines.iter().enumerate() {
            let v: serde_json::Value =
                serde_json::from_str(line).expect("each of N lines is valid JSON");
            assert_eq!(v["event"], "worker-start");
            assert_eq!(
                v["feature_id"],
                format!("F-{i:03}"),
                "no record interleaved"
            );
            assert_eq!(v["run_id"], j.run_id);
        }
    }

    #[test]
    fn run_id_shared_across_filenames() {
        // AC2: one minted run_id correlates the TWO recovery-load-bearing
        // filenames — `journal-<id>.ndjson` and `dispatch-<id>.log` — so startup
        // recovery can pair a journal with its dispatch log. `run-<id>.log` is
        // independent by design (conclusion 01: the run-log↔journal link is
        // operator-convenience, NOT load-bearing for recovery), so it is
        // intentionally NOT asserted here. See `WAIVED_AC AC2` in milestones/notes.md.
        let dir = tempfile::tempdir().unwrap();
        let j = RunJournal::create(dir.path(), RecoveryDone(())).unwrap();
        let journal_name = j.path.file_name().unwrap().to_str().unwrap();
        assert!(
            journal_name.contains(&j.run_id),
            "journal filename embeds the minted run_id"
        );
        // A dispatch log written under this run adopts the same minted run_id —
        // this is the journal↔log pairing the recovery scan relies on.
        let report = crate::dispatch::DispatchReport {
            started_at_ms: 0,
            elapsed_ms: 0,
            dispatched_count: 0,
            outcomes: vec![],
        };
        let dlog = crate::delivery::write_dispatch_log(&report, dir.path(), &j.run_id).unwrap();
        let dlog_name = dlog.file_name().unwrap().to_str().unwrap();
        assert!(
            dlog_name.contains(&j.run_id),
            "dispatch log filename embeds the SAME minted run_id ({}): {dlog_name}",
            j.run_id
        );
        // Entropy: the <millis>-<pid> shape distinguishes two same-second runs.
        assert!(j.run_id.contains('-'), "run_id carries millis-pid entropy");
    }

    #[tokio::test]
    async fn worker_finish_opaque_status() {
        // AC8: worker_finish records its status string VERBATIM with no
        // status-specific branch in the journal code. Each worker_exit_status
        // outcome round-trips unchanged — including F-020's "panic" — without any
        // journal change. (The dispatch layer that FEEDS these strings into
        // worker_finish is covered by dispatch.rs's run_one_feature outcome tests;
        // this proves the journal's opaqueness.)
        for status in ["pass", "fail", "timeout", "cancelled", "error", "panic"] {
            let dir = tempfile::tempdir().unwrap();
            let j = RunJournal::create(dir.path(), RecoveryDone(())).unwrap();
            j.worker_finish("F-001", status, "unknown").await.unwrap();
            let body = std::fs::read_to_string(&j.path).unwrap();
            let v: serde_json::Value = serde_json::from_str(body.lines().last().unwrap()).unwrap();
            assert_eq!(v["event"], "worker-finish");
            assert_eq!(
                v["worker_exit_status"], status,
                "status {status:?} recorded verbatim (opaque passthrough)"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn journal_append_serializes_concurrent_writers() {
        // AC1 (concurrency intent — codex/challenger review): the Arc<Mutex<File>>
        // must SERIALIZE concurrent appends. The sequential test above satisfies
        // the AC's literal "N sequential appends" but would also pass with no lock
        // at all (O_APPEND suffices single-threaded). This drives N tasks appending
        // AT ONCE: every line must still be intact (not interleaved) and all N
        // distinct feature_ids present — the property only the lock guarantees.
        let dir = tempfile::tempdir().unwrap();
        let j = Arc::new(RunJournal::create(dir.path(), RecoveryDone(())).unwrap());
        const N: usize = 50;
        let mut handles = Vec::with_capacity(N);
        for i in 0..N {
            let j = Arc::clone(&j);
            handles.push(tokio::spawn(async move {
                j.worker_start(&format!("F-{i:03}")).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let body = std::fs::read_to_string(&j.path).unwrap();
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), N, "every concurrent append produced one line");
        let mut ids: Vec<String> = lines
            .iter()
            .map(|l| {
                let v: serde_json::Value =
                    serde_json::from_str(l).expect("each line is intact JSON, not interleaved");
                v["feature_id"].as_str().unwrap().to_string()
            })
            .collect();
        ids.sort();
        ids.dedup();
        assert_eq!(
            ids.len(),
            N,
            "all N distinct feature_ids present, none lost or merged"
        );
    }
}
