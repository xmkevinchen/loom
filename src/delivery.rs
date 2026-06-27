//! Phase 6 — Delivery.
//!
//! Emits the final structured dispatch log to
//! `<loom_dir>/dispatch-<run_id>.log` via [`atomic_write`]. Contents:
//! per-feature outcomes + cross-feature timing + worker identity +
//! decision trace.

use crate::atomic_write::atomic_write;
use crate::dispatch::{DispatchReport, FeatureOutcome};
use crate::state::Json;
use anyhow::Result;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Write the dispatch report under `loom_dir` and return the path.
///
/// Filename: `dispatch-<run_id>.log` — F-023 names it with the run's `run_id`
/// (not a UTC timestamp) so startup recovery can correlate it with the run's
/// `journal-<run_id>.ndjson`. The `.log` extension matches other Loom on-disk
/// artifacts even though the body is JSON (operator can `cat` it; tools can
/// `jq` it).
pub fn write_dispatch_log(
    report: &DispatchReport,
    loom_dir: &Path,
    run_id: &str,
) -> Result<PathBuf> {
    let path = loom_dir.join(format!("dispatch-{run_id}.log"));
    let json = report_to_json(report).to_string_pretty();
    atomic_write(&path, json.as_bytes())?;
    Ok(path)
}

/// F-019: write the dispatch log unconditionally — Phase 6 delivery must never
/// be swallowed by a `?` upstream (BL-042: a loop error left zero durable
/// record). On a `write_dispatch_log` failure (disk full, permission, missing
/// `.loom`), print the error AND the intended `.loom` directory to stderr so the
/// operator can diagnose even when durability can't be guaranteed. Lives here
/// (not dispatch.rs) because delivery.rs already owns `write_dispatch_log` + the
/// `DispatchReport` import; a `deliver` in dispatch.rs would form a
/// dispatch↔delivery `use` cycle.
///
/// F-024 Item 2: returns `Result<PathBuf>` — `Ok(path)` when the log landed,
/// `Err` when it did not. Previously a write failure still returned a bare
/// `PathBuf` (the intended dir), so callers printed a misleading
/// `dispatch log → <dir>` success line for a log that was never written. The
/// stderr diagnostic is still emitted here; the `Err` lets the caller suppress
/// the false success line. Delivery stays best-effort — callers log and
/// continue, they do not abort on a delivery failure.
pub fn deliver(report: &DispatchReport, loom_dir: &Path, run_id: &str) -> Result<PathBuf> {
    write_dispatch_log(report, loom_dir, run_id).map_err(|e| {
        eprintln!(
            "loom: FAILED to write dispatch log under {} — {e:#}",
            loom_dir.display()
        );
        e
    })
}

/// F-019: build a degraded one-outcome report recording a loop-level error, so
/// [`deliver`] has something durable to write when the dispatch/iteration loop
/// returned `Err` before producing any per-feature outcome. The synthetic
/// outcome uses the `"<loom>"` sentinel feature_id + `worker_exit_status:
/// "error"` so existing dispatch-log readers parse it with no schema change.
pub fn degraded_report(err: &anyhow::Error) -> DispatchReport {
    DispatchReport {
        started_at_ms: 0,
        elapsed_ms: 0,
        dispatched_count: 0,
        outcomes: vec![FeatureOutcome {
            feature_id: "<loom>".into(),
            worker_identity: "<loom>".into(),
            verdict: "unknown".into(),
            worker_exit_status: "error".into(),
            exit_code: -1,
            duration_ms: 0,
            stdout_path: PathBuf::new(),
            drain_truncated: false,
            error: Some(format!("{err:#}")),
            rescue_ref: None,
        }],
    }
}

fn report_to_json(report: &DispatchReport) -> Json {
    let mut top = BTreeMap::new();
    top.insert("schema".into(), Json::U64(1));
    top.insert("started_at_ms".into(), Json::U64(report.started_at_ms));
    top.insert("elapsed_ms".into(), Json::U64(report.elapsed_ms as u64));
    top.insert(
        "dispatched_count".into(),
        Json::U64(report.dispatched_count as u64),
    );
    let outcomes: Vec<Json> = report.outcomes.iter().map(outcome_to_json).collect();
    top.insert("outcomes".into(), Json::Array(outcomes));
    Json::Object(top)
}

fn outcome_to_json(o: &FeatureOutcome) -> Json {
    let mut m = BTreeMap::new();
    m.insert("feature_id".into(), Json::Str(o.feature_id.clone()));
    m.insert(
        "worker_identity".into(),
        Json::Str(o.worker_identity.clone()),
    );
    m.insert("verdict".into(), Json::Str(o.verdict.clone()));
    m.insert(
        "worker_exit_status".into(),
        Json::Str(o.worker_exit_status.clone()),
    );
    m.insert("exit_code".into(), Json::I64(o.exit_code as i64));
    m.insert("duration_ms".into(), Json::U64(o.duration_ms as u64));
    m.insert(
        "stdout_path".into(),
        Json::Str(o.stdout_path.to_string_lossy().to_string()),
    );
    m.insert("drain_truncated".into(), Json::Bool(o.drain_truncated));
    m.insert(
        "error".into(),
        match &o.error {
            Some(e) => Json::Str(e.clone()),
            None => Json::Null,
        },
    );
    // F-018/F-019 review fixup: surface rescue_ref in the dispatch log so an
    // operator sees where a non-pass worker's committed work was preserved.
    // outcome_to_json is a MANUAL serializer — F-018's `#[serde(skip_serializing_if)]`
    // on the struct never reached it, so the field was silently dropped.
    m.insert(
        "rescue_ref".into(),
        match &o.rescue_ref {
            Some(r) => Json::Str(r.clone()),
            None => Json::Null,
        },
    );
    Json::Object(m)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deliver_writes_degraded_log_on_loop_error() {
        // F-019 AC2 (residual bug): a loop-level Err still yields a durable
        // dispatch log via degraded_report → deliver — never swallowed by an
        // upstream `?`. The synthetic "<loom>" / "error" outcome records the
        // failure even when no per-feature outcome exists.
        let dir = tempfile::tempdir().unwrap();
        let err = anyhow::anyhow!("simulated disk error");
        let degraded = degraded_report(&err);
        assert_eq!(degraded.outcomes.len(), 1);
        assert_eq!(degraded.outcomes[0].feature_id, "<loom>");
        assert_eq!(degraded.outcomes[0].worker_exit_status, "error");
        let log_path = deliver(&degraded, dir.path(), "1700000000000-42").unwrap();
        let logged = std::fs::read_to_string(&log_path).unwrap();
        assert!(
            logged.contains("<loom>"),
            "degraded log must record the synthetic loom outcome"
        );
        assert!(
            logged.contains("simulated disk error"),
            "degraded log must record the loop error message"
        );
    }

    #[test]
    fn dispatch_log_records_rescue_ref() {
        // F-018/F-019 review fixup: rescue_ref must appear in the dispatch log
        // (outcome_to_json is a manual serializer that previously dropped it).
        let dir = tempfile::tempdir().unwrap();
        let report = DispatchReport {
            started_at_ms: 0,
            elapsed_ms: 0,
            dispatched_count: 1,
            outcomes: vec![FeatureOutcome {
                feature_id: "F-018".into(),
                worker_identity: "F-018-w0".into(),
                verdict: "unknown".into(),
                worker_exit_status: "timeout".into(),
                exit_code: 1,
                duration_ms: 0,
                stdout_path: PathBuf::new(),
                drain_truncated: false,
                error: None,
                rescue_ref: Some("refs/heads/loom-rescue/F-018-timeout".into()),
            }],
        };
        let log_path = write_dispatch_log(&report, dir.path(), "1700000000000-7").unwrap();
        let logged = std::fs::read_to_string(&log_path).unwrap();
        assert!(
            logged.contains("rescue_ref"),
            "dispatch log must include the rescue_ref key"
        );
        assert!(
            logged.contains("refs/heads/loom-rescue/F-018-timeout"),
            "dispatch log must record the rescue ref value"
        );
    }

    #[test]
    fn deliver_returns_err_and_does_not_panic_on_write_failure() {
        // F-024 Item 2: a write_dispatch_log failure (loom_dir's parent is a
        // FILE → ENOTDIR) must NOT panic and must return Err — so the caller can
        // suppress the misleading `dispatch log → <dir>` success line. The stderr
        // diagnostic is emitted as a side-effect (not asserted here).
        let tmp = tempfile::tempdir().unwrap();
        let blocker = tmp.path().join("blocker");
        std::fs::write(&blocker, "x").unwrap(); // a file, not a dir
        let bad = blocker.join("loom"); // parent is a file → write fails
        let report = DispatchReport {
            started_at_ms: 0,
            elapsed_ms: 0,
            dispatched_count: 0,
            outcomes: vec![],
        };
        let r = deliver(&report, &bad, "1700000000000-1");
        assert!(
            r.is_err(),
            "deliver returns Err on write failure (not a bare best-effort path)"
        );
    }

    #[test]
    fn deliver_writes_normal_log_on_ok() {
        // Control: deliver with a real report writes the normal log.
        let dir = tempfile::tempdir().unwrap();
        let report = DispatchReport {
            started_at_ms: 0,
            elapsed_ms: 0,
            dispatched_count: 0,
            outcomes: vec![],
        };
        let log_path = deliver(&report, dir.path(), "1700000000000-2").unwrap();
        assert!(
            log_path.exists(),
            "deliver must write a log for a normal report"
        );
    }

    #[test]
    fn writes_dispatch_log() {
        let dir = tempfile::tempdir().unwrap();
        let report = DispatchReport {
            started_at_ms: 1_000_000,
            elapsed_ms: 250,
            dispatched_count: 1,
            outcomes: vec![FeatureOutcome {
                feature_id: "F-001".into(),
                // F-010: distinct values prove the log carries BOTH the AE
                // verdict and the process signal as separate keys.
                verdict: "unknown".into(),
                worker_exit_status: "fail".into(),
                worker_identity: "F-001-w0".into(),
                exit_code: 1,
                duration_ms: 200,
                stdout_path: PathBuf::from("/tmp/out"),
                drain_truncated: false,
                error: None,
                rescue_ref: None,
            }],
        };
        let path = write_dispatch_log(&report, dir.path(), "1700000000000-3").unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("\"F-001\""));
        assert!(content.contains("\"verdict\": \"unknown\""));
        assert!(content.contains("\"worker_exit_status\": \"fail\""));
        assert!(content.contains("\"dispatched_count\": 1"));
    }

    #[test]
    fn dispatch_log_and_journal_share_one_run_id() {
        // F-023 AC2: the two recovery-correlated filenames derive from ONE minted
        // run_id. Mint a journal, build the dispatch log from the SAME run_id, and
        // assert both filenames embed it — so startup recovery can pair them.
        let dir = tempfile::tempdir().unwrap();
        let journal = crate::journal::RunJournal::create(dir.path()).unwrap();
        let report = DispatchReport {
            started_at_ms: 0,
            elapsed_ms: 0,
            dispatched_count: 0,
            outcomes: vec![],
        };
        let dispatch_path = write_dispatch_log(&report, dir.path(), &journal.run_id).unwrap();
        let jname = journal.path.file_name().unwrap().to_str().unwrap();
        let dname = dispatch_path.file_name().unwrap().to_str().unwrap();
        assert!(jname.contains(&journal.run_id), "journal embeds run_id");
        assert!(
            dname.contains(&journal.run_id),
            "dispatch log embeds run_id"
        );
        assert_eq!(jname, format!("journal-{}.ndjson", journal.run_id));
        assert_eq!(dname, format!("dispatch-{}.log", journal.run_id));
    }
}
