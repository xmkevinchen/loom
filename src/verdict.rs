//! Phase 4 — Verdict listener (notify-watched review.md).
//!
//! Watches `<workspace>/.ae/features/active/*/review.md` for modify events.
//! On each event, parses the YAML frontmatter and emits a `VerdictEvent`
//! ONLY when `verdict` is `pass` or `fail`. `verdict: pending`, missing,
//! or empty frontmatter are NORMAL intermediate states during a running
//! `ae:review` skill — silently dropped so we don't misreport
//! `pause-and-notify` mid-execution (verdict state filter).
//!
//! Per Step 6 plan: 3× retry with 200ms gap on YAML parse error within a
//! single event (handles concurrent-write race during a single review.md
//! write). The state filter is separate from the retry path.

use anyhow::{Context, Result};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;
use tokio::sync::mpsc as tokio_mpsc;
use tracing::{debug, info, warn};

/// Verdict as understood by Loom's policy engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AeVerdict {
    Pass,
    Fail,
}

/// One emitted event: a feature's review.md transitioned to a terminal verdict.
#[derive(Debug, Clone)]
pub struct VerdictEvent {
    pub feature_id: String,
    pub verdict: AeVerdict,
    pub review_path: PathBuf,
}

#[derive(Debug, Deserialize)]
struct ReviewFrontmatter {
    #[serde(default)]
    verdict: Option<String>,
}

/// Spawn a notify watcher on `features_dir` and return a tokio channel that
/// yields `VerdictEvent`s when feature review.md files transition to a
/// terminal verdict (`pass` or `fail`).
///
/// The returned `WatcherGuard` keeps the OS watcher alive; drop it to stop
/// watching. The receiver is closed (returning `None`) when the watcher
/// thread exits.
pub fn watch_verdicts(
    features_dir: &Path,
) -> Result<(WatcherGuard, tokio_mpsc::Receiver<VerdictEvent>)> {
    let (tx_evt, rx_evt) = mpsc::channel::<notify::Result<Event>>();
    let mut watcher: RecommendedWatcher = notify::recommended_watcher(move |res| {
        let _ = tx_evt.send(res);
    })
    .context("create notify watcher")?;
    watcher
        .watch(features_dir, RecursiveMode::Recursive)
        .with_context(|| format!("watch {:?}", features_dir))?;

    // 256 absorbs the startup burst when notify replays events on watcher
    // registration (observed on macOS FSEvents). Steady-state throughput
    // needs far less; this is purely headroom for the registration spike.
    let (tx_out, rx_out) = tokio_mpsc::channel::<VerdictEvent>(256);

    info!(dir = %features_dir.display(), "verdict: notify watcher started");

    std::thread::spawn(move || {
        while let Ok(res) = rx_evt.recv() {
            match res {
                Ok(event) => process_event(event, &tx_out),
                Err(e) => warn!(error = %e, "verdict: notify error"),
            }
        }
        debug!("verdict: notify watcher thread exiting");
    });

    Ok((WatcherGuard { _watcher: watcher }, rx_out))
}

/// Lives as long as the OS watcher; drop to stop watching.
///
/// Teardown is async-by-construction: dropping the guard closes the
/// `notify::RecommendedWatcher`, which closes the std::mpsc sender, which
/// makes `rx_evt.recv()` in the spawned event-pump thread return `Err` and
/// exit cleanly. The thread's exit is NOT joined here — it is observable
/// for a brief window after `Drop::drop` returns. Harmless at v0.0.2 scale
/// (no resource accumulation, no test-leak symptoms). If a future caller
/// needs deterministic post-drop quiescence (e.g., a tight spawn/drop loop
/// in a test harness), promote the spawn's `JoinHandle` into this struct
/// and join in `Drop`.
pub struct WatcherGuard {
    _watcher: RecommendedWatcher,
}

fn process_event(event: Event, tx: &tokio_mpsc::Sender<VerdictEvent>) {
    if !matches!(event.kind, EventKind::Modify(_) | EventKind::Create(_)) {
        return;
    }
    for path in event.paths {
        if path.file_name().and_then(|n| n.to_str()) != Some("review.md") {
            continue;
        }
        let Some(feature_dir) = path.parent() else {
            continue;
        };
        let Some(feature_id) = feature_dir.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        match parse_with_retry(&path) {
            Some(verdict) => {
                info!(
                    feature_id = feature_id,
                    verdict = ?verdict,
                    path = %path.display(),
                    "verdict: emitting"
                );
                let evt = VerdictEvent {
                    feature_id: feature_id.to_string(),
                    verdict,
                    review_path: path.clone(),
                };
                // try_send (NOT blocking_send) so saturation is observable as
                // a warn log instead of stalling the watcher std::thread.
                // blocking_send would not deadlock the tokio runtime (it's safe
                // from a non-tokio thread), but the resulting latency would
                // accumulate on subsequent file events. Dropped events are
                // recovered by the per-cycle review.md scan in
                // iteration::run_iteration_loop.
                match tx.try_send(evt) {
                    Ok(()) => {}
                    Err(tokio_mpsc::error::TrySendError::Full(dropped)) => {
                        warn!(
                            feature_id = %dropped.feature_id,
                            "verdict: channel saturated — dropping event; per-cycle review.md scan will recover"
                        );
                    }
                    Err(tokio_mpsc::error::TrySendError::Closed(_)) => {
                        // Receiver shut down; normal shutdown signal — drop silently.
                    }
                }
            }
            None => {
                debug!(path = %path.display(), "verdict: intermediate state (no terminal verdict) — dropping");
            }
        }
    }
}

/// Parse review.md frontmatter into a terminal verdict.
///
/// Returns `Some(Pass | Fail)` when a terminal verdict is set, `None` when
/// the verdict is pending / missing / empty (state filter drops these
/// silently). Retries the YAML parse 3× with 200ms gap on parse-error path
/// to handle concurrent-write race within a single review.md write.
fn parse_with_retry(path: &Path) -> Option<AeVerdict> {
    for attempt in 0..3 {
        match parse_once(path) {
            Ok(Some(v)) => return Some(v),
            Ok(None) => return None, // terminal "this is intermediate" — no retry needed
            Err(e) => {
                debug!(
                    attempt,
                    error = %e,
                    path = %path.display(),
                    "verdict: parse error, retrying"
                );
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
    None
}

/// Best-effort one-shot parse of a `review.md` file's terminal verdict.
///
/// Swallows ALL errors (missing file, read failure, YAML parse failure)
/// as `None`. Intended for the per-cycle scan in
/// [`crate::iteration::run_iteration_loop`] and the restart pre-populate
/// path — both need "skip if missing or unreadable" semantics.
///
/// The watcher path uses [`parse_with_retry`] instead, which retries
/// transient parse errors 3× to absorb concurrent-write races.
pub fn parse_review_once(path: &Path) -> Option<AeVerdict> {
    parse_once(path).ok().flatten()
}

fn parse_once(path: &Path) -> Result<Option<AeVerdict>> {
    let content =
        std::fs::read_to_string(path).with_context(|| format!("read review {:?}", path))?;
    let rest = match content.strip_prefix("---\n") {
        Some(r) => r,
        None => return Ok(None), // no frontmatter yet — intermediate
    };
    let end = match rest.find("\n---") {
        Some(i) => i,
        None => return Ok(None), // not yet closed — intermediate
    };
    let yaml = &rest[..end];
    let fm: ReviewFrontmatter = serde_yaml::from_str(yaml)
        .with_context(|| format!("parse review frontmatter {:?}", path))?;
    match fm.verdict.as_deref() {
        Some("pass") => Ok(Some(AeVerdict::Pass)),
        Some("fail") => Ok(Some(AeVerdict::Fail)),
        Some("pending") | Some("") | None => Ok(None),
        Some(other) => {
            debug!(
                verdict = other,
                path = %path.display(),
                "verdict: unrecognized verdict value — treating as intermediate"
            );
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, content: &str) {
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p).unwrap();
        }
        std::fs::write(path, content).unwrap();
    }

    #[test]
    fn parse_pass() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("review.md");
        write(&p, "---\nverdict: pass\n---\nbody\n");
        assert_eq!(parse_with_retry(&p), Some(AeVerdict::Pass));
    }

    #[test]
    fn parse_fail() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("review.md");
        write(&p, "---\nverdict: fail\n---\n");
        assert_eq!(parse_with_retry(&p), Some(AeVerdict::Fail));
    }

    #[test]
    fn parse_pending_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("review.md");
        write(&p, "---\nverdict: pending\n---\n");
        assert_eq!(parse_with_retry(&p), None);
    }

    #[test]
    fn parse_missing_frontmatter_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("review.md");
        write(&p, "just a body, no fm\n");
        assert_eq!(parse_with_retry(&p), None);
    }

    #[test]
    fn parse_empty_verdict_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("review.md");
        write(&p, "---\nverdict: \"\"\n---\n");
        assert_eq!(parse_with_retry(&p), None);
    }

    fn evt(id: &str) -> VerdictEvent {
        VerdictEvent {
            feature_id: id.into(),
            verdict: AeVerdict::Pass,
            review_path: PathBuf::new(),
        }
    }

    #[tokio::test]
    async fn try_send_full_when_channel_saturated() {
        // Channel capacity 2; receiver kept alive but not polling → 3rd send
        // returns TrySendError::Full and must not panic or block.
        let (tx, _rx_kept) = tokio_mpsc::channel::<VerdictEvent>(2);
        assert!(tx.try_send(evt("a")).is_ok());
        assert!(tx.try_send(evt("b")).is_ok());
        match tx.try_send(evt("c")) {
            Err(tokio_mpsc::error::TrySendError::Full(dropped)) => {
                assert_eq!(dropped.feature_id, "c");
            }
            other => panic!("expected Full, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn try_send_closed_when_receiver_dropped() {
        // Receiver dropped → try_send returns Closed (not Full), distinct
        // arm matches process_event's silent-drop path for shutdown.
        let (tx, rx) = tokio_mpsc::channel::<VerdictEvent>(8);
        drop(rx);
        match tx.try_send(evt("x")) {
            Err(tokio_mpsc::error::TrySendError::Closed(dropped)) => {
                assert_eq!(dropped.feature_id, "x");
            }
            other => panic!("expected Closed, got {other:?}"),
        }
    }
}
