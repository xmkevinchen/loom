//! Phase 6 — Delivery.
//!
//! Emits the final structured dispatch log to
//! `<loom_dir>/dispatch-<UTC-timestamp>.log` via [`atomic_write`]. Contents:
//! per-feature outcomes + cross-feature timing + worker identity +
//! decision trace.

use crate::atomic_write::atomic_write;
use crate::dispatch::{DispatchReport, FeatureOutcome};
use crate::state::Json;
use anyhow::Result;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Write the dispatch report under `loom_dir` and return the path.
///
/// Filename: `dispatch-<UTC-timestamp>.log` (the `.log` extension matches
/// other Loom on-disk artifacts even though the body is JSON — operator
/// can `cat` it; tools can `jq` it).
pub fn write_dispatch_log(report: &DispatchReport, loom_dir: &Path) -> Result<PathBuf> {
    let ts = utc_timestamp(SystemTime::now());
    let path = loom_dir.join(format!("dispatch-{ts}.log"));
    let json = report_to_json(report).to_string_pretty();
    atomic_write(&path, json.as_bytes())?;
    Ok(path)
}

/// F-019: write the dispatch log unconditionally — Phase 6 delivery must never
/// be swallowed by a `?` upstream (BL-042: a loop error left zero durable
/// record). On a `write_dispatch_log` failure (disk full, permission, missing
/// `.loom`), print the error AND the intended `.loom` directory to stderr so the
/// operator can diagnose even when durability can't be guaranteed, and return
/// the best-effort intended directory. Lives here (not dispatch.rs) because
/// delivery.rs already owns `write_dispatch_log` + the `DispatchReport` import;
/// a `deliver` in dispatch.rs would form a dispatch↔delivery `use` cycle.
pub fn deliver(report: &DispatchReport, loom_dir: &Path) -> PathBuf {
    match write_dispatch_log(report, loom_dir) {
        Ok(path) => path,
        Err(e) => {
            eprintln!(
                "loom: FAILED to write dispatch log under {} — {e:#}",
                loom_dir.display()
            );
            loom_dir.to_path_buf()
        }
    }
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

/// Format `now` as `YYYYMMDDTHHMMSSZ` UTC. Duplicate of the helper in
/// `main.rs` — kept inline (small + zero churn) per Step 6 strict-scope
/// guidance.
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
        let log_path = deliver(&degraded, dir.path());
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
        let log_path = write_dispatch_log(&report, dir.path()).unwrap();
        let logged = std::fs::read_to_string(&log_path).unwrap();
        assert!(logged.contains("rescue_ref"), "dispatch log must include the rescue_ref key");
        assert!(
            logged.contains("refs/heads/loom-rescue/F-018-timeout"),
            "dispatch log must record the rescue ref value"
        );
    }

    #[test]
    fn deliver_returns_best_effort_path_and_does_not_panic_on_write_failure() {
        // AC2 "never swallowed": a write_dispatch_log failure (loom_dir's parent
        // is a FILE → ENOTDIR) must NOT panic — deliver prints to stderr and
        // returns the best-effort intended dir.
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
        let p = deliver(&report, &bad);
        assert_eq!(p, bad, "deliver returns the best-effort intended dir on write failure");
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
        let log_path = deliver(&report, dir.path());
        assert!(log_path.exists(), "deliver must write a log for a normal report");
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
        let path = write_dispatch_log(&report, dir.path()).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("\"F-001\""));
        assert!(content.contains("\"verdict\": \"unknown\""));
        assert!(content.contains("\"worker_exit_status\": \"fail\""));
        assert!(content.contains("\"dispatched_count\": 1"));
    }
}
