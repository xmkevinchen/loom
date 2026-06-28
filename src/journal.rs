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
use crate::delivery::deliver;
use crate::dispatch::{DispatchReport, FeatureOutcome};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
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
/// dissolves.
pub struct RecoveryDone(());

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

// ===========================================================================
// Startup recovery (Step 4)
// ===========================================================================

/// One journal line as read back during recovery (owned, tolerant). `run_id`
/// and `ts_ms` are intentionally not parsed: `run_id` comes from the filename,
/// and aggregation is by file order, not timestamp.
#[derive(Deserialize)]
struct RecoveredRecord {
    event: String,
    feature_id: String,
    #[serde(default)]
    worker_exit_status: Option<String>,
    #[serde(default)]
    verdict: Option<String>,
    #[serde(default)]
    ref_name: Option<String>,
}

/// Per-`feature_id` aggregation state. The three signals are tracked
/// INDEPENDENTLY so a `worker-finish` ALWAYS wins over a start-only fallback
/// regardless of record order (a finish physically preceding its start still
/// yields the finished outcome).
#[derive(Default)]
struct Agg {
    seen_start: bool,
    /// Latest `worker-finish` (file order): `(worker_exit_status, verdict)`.
    finish: Option<(String, String)>,
    /// Latest `rescue-ref-written` ref name.
    rescue_ref: Option<String>,
}

/// Startup recovery: scan `loom_dir` for orphan run journals and reconcile each.
///
/// MUST be called BEFORE this run mints its own journal (so the current run's
/// journal is never in scope) and ONLY from `run` / `dispatch` (never
/// `status` — recovery writes dispatch logs). Returns the number of journals
/// reconciled.
///
/// For each `journal-<run_id>.ndjson` (`.done` files and dispatch logs are
/// skipped by the filename filter):
/// - a NON-EMPTY, valid-JSON `dispatch-<run_id>.log` already exists → the log is
///   authoritative; rename the journal `.done` only, NEVER re-synthesize
///   (verdict-precedence rule). Makes a crash DURING recovery idempotent: a
///   re-run sees the just-written valid log and only completes the rename.
/// - otherwise → synthesize a dispatch log from the journal, then rename `.done`.
pub fn recover_orphan_runs(loom_dir: &Path) -> (RecoveryDone, Result<usize>) {
    // BL-054: mint the RecoveryDone token UNCONDITIONALLY — this fn is its sole
    // source, so `RunJournal::create` cannot run without proof recovery was
    // ATTEMPTED first. The recovery outcome rides in the `Result`; a failure is
    // non-fatal (entry points warn + continue with the token in hand). The proven
    // scan body is left untouched in `recover_orphan_runs_inner`.
    (RecoveryDone(()), recover_orphan_runs_inner(loom_dir))
}

fn recover_orphan_runs_inner(loom_dir: &Path) -> Result<usize> {
    let read_dir = match std::fs::read_dir(loom_dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => {
            return Err(e).with_context(|| format!("scan {:?} for orphan journals", loom_dir))
        }
    };
    let mut reconciled = 0;
    for entry in read_dir {
        // codex Step-4 P3: surface a per-entry scan error rather than silently
        // dropping it — an unreadable entry could hide an orphan journal, and a
        // durability layer must not lose runs without a trace.
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(error = %e, dir = ?loom_dir, "skipping unreadable entry during journal recovery scan");
                continue;
            }
        };
        let path = entry.path();
        let run_id = match journal_run_id(&path) {
            Some(id) => id,
            None => continue,
        };
        let dispatch_log = loom_dir.join(format!("dispatch-{run_id}.log"));
        if !dispatch_log_is_valid(&dispatch_log) {
            // Orphan: reconstruct the lost run's outcome before finalizing.
            let report = synthesize_report(&path)?;
            // Integration review (codex/arch P3): a run that minted a journal but
            // exited BEFORE dispatching any worker (no features found / no
            // matching ids / a discovery error) leaves a ZERO-event journal →
            // zero outcomes. Writing a phantom empty dispatch log for a run that
            // did nothing only confuses `loom status`; finalize `.done` WITHOUT a
            // log. A worker-start-only journal still yields one error/unknown
            // outcome and IS recorded.
            if !report.outcomes.is_empty() {
                // Integration review (arch P2): go through `deliver` (not the raw
                // `write_dispatch_log`) so a recovery write failure surfaces to
                // the operator on stderr — the same operator-visibility contract
                // F-024 established for the normal delivery path.
                deliver(&report, loom_dir, &run_id)?;
            }
        }
        finalize_done(&path)?;
        reconciled += 1;
    }
    Ok(reconciled)
}

/// `Some(run_id)` iff `path`'s filename is `journal-<run_id>.ndjson`. Returns
/// `None` for `.done` files (they end `.ndjson.done`, not `.ndjson`), dispatch
/// logs, and anything else.
fn journal_run_id(path: &Path) -> Option<String> {
    let name = path.file_name()?.to_str()?;
    let mid = name.strip_prefix("journal-")?.strip_suffix(".ndjson")?;
    (!mid.is_empty()).then(|| mid.to_string())
}

/// A dispatch log counts as authoritative iff it exists, is non-empty, and
/// parses as a JSON object carrying BOTH an `outcomes` array AND a numeric
/// `dispatched_count` — the [`DispatchReport`] shape `write_dispatch_log`
/// (delivery.rs) always emits. codex review P2-A: an `outcomes`-only check
/// accepts a bare `{"outcomes":[]}` stub as authoritative, which would finalize
/// an un-synthesized journal over a non-Loom file; requiring a second mandatory
/// field a stub is unlikely to carry tightens the shape gate. A REAL empty-run
/// log still passes (it carries `dispatched_count: 0`). Under `atomic_write` a
/// Loom-written log is complete-or-absent, so this is defense-in-depth against
/// external interference, not the load-bearing crash-recovery check.
fn dispatch_log_is_valid(path: &Path) -> bool {
    match std::fs::read_to_string(path) {
        Ok(s) if !s.trim().is_empty() => serde_json::from_str::<serde_json::Value>(&s)
            .ok()
            .map(|v| {
                v.get("outcomes").is_some_and(serde_json::Value::is_array)
                    && v.get("dispatched_count")
                        .is_some_and(serde_json::Value::is_number)
            })
            .unwrap_or(false),
        _ => false,
    }
}

/// Parse an orphan journal tolerantly (skip torn / invalid / unknown lines;
/// dup / out-of-order records are absorbed by the aggregation) and synthesize a
/// [`DispatchReport`]. A `feature_id` with a `worker-finish` uses that outcome;
/// a `seen_start` with no finish → `worker_exit_status:"error"` +
/// `verdict:"unknown"`; neither → no outcome.
fn synthesize_report(journal_path: &Path) -> Result<DispatchReport> {
    let content = std::fs::read_to_string(journal_path)
        .with_context(|| format!("read orphan journal {:?}", journal_path))?;
    let mut aggs: BTreeMap<String, Agg> = BTreeMap::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let rec: RecoveredRecord = match serde_json::from_str(line) {
            Ok(r) => r,
            Err(_) => continue, // torn / invalid line — tolerate
        };
        let agg = aggs.entry(rec.feature_id).or_default();
        match rec.event.as_str() {
            "worker-start" => agg.seen_start = true,
            "worker-finish" => {
                // finish ALWAYS wins; latest finish (file order) overwrites.
                let status = rec.worker_exit_status.unwrap_or_else(|| "error".into());
                let verdict = rec.verdict.unwrap_or_else(|| "unknown".into());
                agg.finish = Some((status, verdict));
            }
            "rescue-ref-written" if rec.ref_name.is_some() => agg.rescue_ref = rec.ref_name,
            _ => {} // unknown event (or rescue-ref with no ref_name) — skip
        }
    }

    let mut outcomes = Vec::new();
    for (feature_id, agg) in aggs {
        let (worker_exit_status, verdict) = match agg.finish {
            Some((status, verdict)) => (status, verdict),
            None if agg.seen_start => ("error".into(), "unknown".into()),
            None => continue, // neither start nor finish — nothing to record
        };
        outcomes.push(FeatureOutcome {
            feature_id,
            worker_identity: "<recovered>".into(),
            verdict,
            worker_exit_status,
            exit_code: -1,
            duration_ms: 0,
            stdout_path: PathBuf::new(),
            drain_truncated: false,
            error: None,
            rescue_ref: agg.rescue_ref,
        });
    }
    Ok(DispatchReport {
        started_at_ms: 0,
        elapsed_ms: 0,
        dispatched_count: outcomes.len(),
        outcomes,
    })
}

/// Rename `journal-<id>.ndjson` → `journal-<id>.ndjson.done`, THEN fsync the
/// parent dir. The parent-dir fsync MUST follow the rename so the `.done`
/// directory entry is itself durable across power-loss (fsyncing before the
/// rename would leave the rename non-durable).
fn finalize_done(journal_path: &Path) -> Result<()> {
    let mut done = journal_path.as_os_str().to_owned();
    done.push(".done");
    let done = PathBuf::from(done);
    std::fs::rename(journal_path, &done)
        .with_context(|| format!("rename {:?} -> {:?}", journal_path, done))?;
    fsync_parent_dir(&done)?;
    Ok(())
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

    #[test]
    fn dispatch_log_validity_requires_dispatchreport_shape() {
        // codex review P2-A: an `outcomes`-only stub must NOT count as an
        // authoritative dispatch log (else recovery finalizes .done over an
        // un-synthesized journal). Require the DispatchReport shape: an `outcomes`
        // array AND a numeric `dispatched_count`.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("d.log");
        let write = |s: &str| std::fs::write(&p, s).unwrap();

        // A real (even empty-run) DispatchReport → authoritative.
        write(r#"{"started_at_ms":0,"elapsed_ms":0,"dispatched_count":0,"outcomes":[]}"#);
        assert!(
            dispatch_log_is_valid(&p),
            "a real empty-run DispatchReport is authoritative"
        );
        // A bare outcomes-only stub (no dispatched_count) → NOT authoritative.
        write(r#"{"outcomes":[]}"#);
        assert!(
            !dispatch_log_is_valid(&p),
            "an outcomes-only stub must not pass as authoritative"
        );
        // Empty object, non-array outcomes, empty file → NOT authoritative.
        write("{}");
        assert!(!dispatch_log_is_valid(&p));
        write(r#"{"outcomes":"x","dispatched_count":1}"#);
        assert!(!dispatch_log_is_valid(&p), "outcomes must be an array");
        write("   ");
        assert!(!dispatch_log_is_valid(&p), "whitespace-only is not valid");
    }

    // ---- Step 4: startup recovery ----

    #[test]
    fn recovery_synthesizes_log_for_orphan_without_dispatch_log() {
        let dir = tempfile::tempdir().unwrap();
        let loom = dir.path();
        let jpath = loom.join("journal-111-1.ndjson");
        std::fs::write(
            &jpath,
            concat!(
                r#"{"event":"worker-start","run_id":"111-1","feature_id":"F-001","ts_ms":1}"#,
                "\n",
                r#"{"event":"worker-finish","run_id":"111-1","feature_id":"F-001","ts_ms":2,"worker_exit_status":"timeout","verdict":"unknown"}"#,
                "\n",
            ),
        )
        .unwrap();

        let n = recover_orphan_runs(loom).1.unwrap();
        assert_eq!(n, 1);

        let dlog = loom.join("dispatch-111-1.log");
        assert!(
            dlog.exists(),
            "orphan without log → synthesized dispatch log"
        );
        let body = std::fs::read_to_string(&dlog).unwrap();
        assert!(body.contains("F-001"));
        assert!(body.contains("timeout"), "finish status preserved");
        // Journal renamed to .done.
        assert!(!jpath.exists());
        assert!(loom.join("journal-111-1.ndjson.done").exists());
    }

    #[test]
    fn recovery_start_only_yields_error_unknown_and_tolerates_torn_final_line() {
        let dir = tempfile::tempdir().unwrap();
        let loom = dir.path();
        let jpath = loom.join("journal-222-1.ndjson");
        // A valid start line + a TORN final line (truncated mid-JSON, no newline).
        std::fs::write(
            &jpath,
            concat!(
                r#"{"event":"worker-start","run_id":"222-1","feature_id":"F-002","ts_ms":1}"#,
                "\n",
                r#"{"event":"worker-fini"#,
            ),
        )
        .unwrap();

        recover_orphan_runs(loom).1.unwrap();
        let body = std::fs::read_to_string(loom.join("dispatch-222-1.log")).unwrap();
        assert!(body.contains("F-002"));
        // start-without-finish → error / unknown (torn finish line ignored).
        assert!(
            body.contains("\"worker_exit_status\": \"error\""),
            "start-only → worker_exit_status error"
        );
        assert!(
            body.contains("\"verdict\": \"unknown\""),
            "start-only → verdict unknown"
        );
    }

    #[test]
    fn recovery_with_valid_dispatch_log_renames_done_without_resynthesis() {
        let dir = tempfile::tempdir().unwrap();
        let loom = dir.path();
        let jpath = loom.join("journal-333-1.ndjson");
        std::fs::write(
            &jpath,
            "{\"event\":\"worker-start\",\"run_id\":\"333-1\",\"feature_id\":\"F-003\",\"ts_ms\":1}\n",
        )
        .unwrap();
        // A pre-existing VALID dispatch log is authoritative.
        let dlog = loom.join("dispatch-333-1.log");
        // A real authoritative log carries the full DispatchReport shape
        // (dispatched_count + outcomes) — codex review P2-A tightened
        // dispatch_log_is_valid to require it, so an `{"outcomes":[]}` stub no
        // longer counts as authoritative.
        std::fs::write(
            &dlog,
            r#"{"started_at_ms":0,"elapsed_ms":0,"dispatched_count":0,"outcomes":[]}"#,
        )
        .unwrap();
        let before = std::fs::read_to_string(&dlog).unwrap();

        recover_orphan_runs(loom).1.unwrap();
        // .done rename happened; the valid log is UNCHANGED (no re-synthesis).
        assert!(loom.join("journal-333-1.ndjson.done").exists());
        assert!(!jpath.exists());
        assert_eq!(
            std::fs::read_to_string(&dlog).unwrap(),
            before,
            "an authoritative dispatch log must never be overwritten by recovery"
        );
    }

    #[test]
    fn recovery_finish_before_start_still_synthesizes_finished_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let loom = dir.path();
        let jpath = loom.join("journal-444-1.ndjson");
        // worker-finish PHYSICALLY BEFORE worker-start (out of order).
        std::fs::write(
            &jpath,
            concat!(
                r#"{"event":"worker-finish","run_id":"444-1","feature_id":"F-004","ts_ms":9,"worker_exit_status":"pass","verdict":"pass"}"#,
                "\n",
                r#"{"event":"worker-start","run_id":"444-1","feature_id":"F-004","ts_ms":1}"#,
                "\n",
            ),
        )
        .unwrap();

        recover_orphan_runs(loom).1.unwrap();
        let body = std::fs::read_to_string(loom.join("dispatch-444-1.log")).unwrap();
        // finish wins regardless of order → status "pass", NOT start-only "error".
        // (The dispatch log always carries an `"error": null` KEY, so assert on
        // the specific worker_exit_status value, not the bare substring "error".)
        assert!(
            body.contains("\"worker_exit_status\": \"pass\""),
            "finish wins over start-only fallback"
        );
        assert!(
            !body.contains("\"worker_exit_status\": \"error\""),
            "a finished feature must not be recorded as error"
        );
    }

    #[test]
    fn recovery_treats_non_dispatchreport_json_log_as_not_authoritative() {
        // codex Step-4 P2: a `dispatch-<id>.log` that is valid JSON but NOT a
        // DispatchReport (no `outcomes` array) must NOT be treated as
        // authoritative — the orphan journal is synthesized + overwrites it,
        // rather than being finalized over a meaningless file.
        let dir = tempfile::tempdir().unwrap();
        let loom = dir.path();
        let jpath = loom.join("journal-555-1.ndjson");
        std::fs::write(
            &jpath,
            concat!(
                r#"{"event":"worker-start","run_id":"555-1","feature_id":"F-005","ts_ms":1}"#,
                "\n",
                r#"{"event":"worker-finish","run_id":"555-1","feature_id":"F-005","ts_ms":2,"worker_exit_status":"fail","verdict":"fail"}"#,
                "\n",
            ),
        )
        .unwrap();
        // A bogus-but-valid-JSON dispatch log (no `outcomes` array).
        let dlog = loom.join("dispatch-555-1.log");
        std::fs::write(&dlog, "{}").unwrap();

        recover_orphan_runs(loom).1.unwrap();
        // The journal WAS treated as an orphan → log re-synthesized with real
        // content (now contains the feature + an outcomes array), journal .done.
        let body = std::fs::read_to_string(&dlog).unwrap();
        assert!(
            body.contains("F-005"),
            "bogus log must be overwritten by synthesis"
        );
        assert!(body.contains("outcomes"));
        assert!(loom.join("journal-555-1.ndjson.done").exists());
    }

    #[test]
    fn recovery_is_inert_when_no_journals_present() {
        let dir = tempfile::tempdir().unwrap();
        // Only a dispatch log + a .done journal — neither is a live orphan.
        std::fs::write(dir.path().join("dispatch-x.log"), "{}").unwrap();
        std::fs::write(dir.path().join("journal-old.ndjson.done"), "x").unwrap();
        let n = recover_orphan_runs(dir.path()).1.unwrap();
        assert_eq!(
            n, 0,
            "no live journal-*.ndjson → nothing reconciled, nothing written"
        );
        // No new dispatch log was synthesized.
        assert!(!dir.path().join("dispatch-old.log").exists());
    }

    #[test]
    fn recovery_zero_event_journal_finalizes_done_without_phantom_log() {
        // Integration review (codex/arch P3): a journal minted by a run that
        // exited before dispatching any worker has NO events → zero outcomes.
        // Recovery must rename it .done WITHOUT writing a phantom empty dispatch
        // log (which would show a no-op run as a dispatched run in `loom status`).
        let dir = tempfile::tempdir().unwrap();
        let loom = dir.path();
        let jpath = loom.join("journal-999-1.ndjson");
        std::fs::write(&jpath, "").unwrap(); // zero events

        let n = recover_orphan_runs(loom).1.unwrap();
        assert_eq!(
            n, 1,
            "the eventless journal is still reconciled (finalized)"
        );
        assert!(
            loom.join("journal-999-1.ndjson.done").exists(),
            "eventless journal is renamed .done"
        );
        assert!(!jpath.exists());
        assert!(
            !loom.join("dispatch-999-1.log").exists(),
            "no phantom dispatch log for a run that dispatched nothing"
        );
    }
}
