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
use std::fs::{File, OpenOptions};
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
}
