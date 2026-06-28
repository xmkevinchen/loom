//! F-023 startup recovery (Step 4) — reconcile orphan run journals.
//!
//! Scans `.loom` for `journal-<run_id>.ndjson` files left by a non-graceful
//! process death and reconstructs each lost run's outcome (synthesizing a
//! dispatch log when none survives) before finalizing the journal `.done`.
//! Homed in its own module so `journal` (which owns the write path) need not
//! import `dispatch`/`delivery`, dissolving the dependency cycle BL-053.

use crate::atomic_write::fsync_parent_dir;
use crate::delivery::deliver;
use crate::dispatch::{DispatchReport, FeatureOutcome};
use crate::journal::RecoveryDone;
use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

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

#[cfg(test)]
mod tests {
    use super::*;

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
