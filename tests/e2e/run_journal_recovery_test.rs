//! e2e SIGKILL → recovery (F-023 Step 5 / AC6). Spawns the real `loom` binary
//! with a blocking PATH-stub worker, waits until the worker is in-flight (so a
//! `worker-start` event is durably in the run journal), SIGKILLs loom (no
//! graceful Phase-6 delivery → no dispatch log), then restarts loom and asserts
//! startup recovery synthesized a dispatch log for the orphan run AND renamed
//! the orphan journal `.done`. A third restart asserts idempotence (the
//! recovered orphan is not re-processed).
//!
//! Harness mirrors `sigint_e2e_test.rs` (local copy — cross-target dedup is
//! BL-032). The blocking stub `exec sleep 600`s after writing markers, so once
//! the markers appear the worker is provably running and `worker-start` (emitted
//! before `worker.run()` and `sync_all`'d) is already on disk.
//!
//! Crash-DURING-recovery idempotence (kill between the dispatch-log write and
//! the `.done` rename) is covered deterministically by the journal unit tests
//! (`recovery_with_valid_dispatch_log_renames_done_without_resynthesis`): a
//! precise mid-recovery SIGKILL of a live process is not achievable without
//! production fault-injection hooks, which would be out of scope. The
//! third-restart idempotence check here covers the binary-level no-op case.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const POLL_CADENCE: Duration = Duration::from_millis(25);
const EXIT_DEADLINE: Duration = Duration::from_secs(15);
const READY_DEADLINE: Duration = Duration::from_secs(10);

fn loom_bin() -> &'static str {
    env!("CARGO_BIN_EXE_loom")
}

fn write_feature(features_root: &Path, id: &str) {
    let dir = features_root.join(id);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("index.md"),
        format!("---\nid: {id}\npipeline:\n  work: in_progress\n---\n\nbody\n"),
    )
    .unwrap();
}

/// PATH-stub `claude`. Worker calls (LOOM_PARENT_PID set): in blocking mode
/// write ready+working markers then `exec sleep 600`; in exit mode return 0
/// immediately. Discovery calls (no LOOM_PARENT_PID) exit 0.
fn write_stub(stub_dir: &Path) {
    let script = "#!/bin/sh\n\
        if [ -n \"$LOOM_PARENT_PID\" ]; then\n\
        \x20   if [ \"$LOOM_STUB_MODE\" = exit ]; then\n\
        \x20       exit 0\n\
        \x20   fi\n\
        \x20   echo \"$$\" > \"$LOOM_STUB_MARKER_DIR/ready\"\n\
        \x20   echo \"$$\" > \"$LOOM_STUB_MARKER_DIR/working\"\n\
        \x20   exec sleep 600\n\
        fi\n\
        exit 0\n";
    let path = stub_dir.join("claude");
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

enum StubMode {
    Blocking,
    QuickExit,
}

fn spawn_loom(
    workspace: &Path,
    args: &[&str],
    stub_dir: &Path,
    marker_dir: &Path,
    mode: StubMode,
) -> Child {
    let path_var = format!(
        "{}:{}",
        stub_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let mut cmd = Command::new(loom_bin());
    // codex Step-5 P2-1: this test asserts on FILES (synthesized dispatch log +
    // `.done` rename), never on loom's output, and `wait_for_exit` does not
    // drain pipes — so discard stdio rather than risk a full-pipe `write(2)`
    // stall that would masquerade as a hang.
    cmd.args(args)
        .current_dir(workspace)
        .env("PATH", path_var)
        .env("LOOM_STUB_MARKER_DIR", marker_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let StubMode::QuickExit = mode {
        cmd.env("LOOM_STUB_MODE", "exit");
    }
    cmd.process_group(0);
    cmd.spawn().expect("spawn loom binary")
}

fn read_stub_pid(marker_dir: &Path) -> Option<u32> {
    ["working", "ready"].iter().find_map(|m| {
        std::fs::read_to_string(marker_dir.join(m))
            .ok()?
            .trim()
            .parse()
            .ok()
    })
}

fn kill_recorded_stub(marker_dir: &Path) {
    if let Some(pid) = read_stub_pid(marker_dir) {
        unsafe {
            libc::kill(pid as libc::pid_t, libc::SIGKILL);
        }
    }
}

/// Poll until both markers exist (worker provably in-flight). Kills loom + the
/// stub on deadline before panicking so no Child/worker leaks.
fn poll_markers(child: &mut Child, marker_dir: &Path, deadline: Duration) {
    let start = Instant::now();
    loop {
        if marker_dir.join("ready").exists() && marker_dir.join("working").exists() {
            return;
        }
        if start.elapsed() >= deadline {
            child.kill().ok();
            child.wait().ok();
            kill_recorded_stub(marker_dir);
            panic!("readiness gate: worker never reached in-flight state within {deadline:?}");
        }
        std::thread::sleep(POLL_CADENCE);
    }
}

/// Wait for loom to exit on its own (or kill it on deadline). Returns the exit
/// status. stdio is `Stdio::null()` (spawn_loom), so there is no pipe to drain.
fn wait_for_exit(
    child: &mut Child,
    deadline: Duration,
    marker_dir: &Path,
) -> std::process::ExitStatus {
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("try_wait loom") {
            return status;
        }
        if start.elapsed() >= deadline {
            child.kill().ok();
            let s = child.wait().ok();
            kill_recorded_stub(marker_dir);
            panic!("loom did not exit within {deadline:?} (status {s:?})");
        }
        std::thread::sleep(POLL_CADENCE);
    }
}

/// `(filename, run_id)` of a live `journal-<run_id>.ndjson` under `.loom`
/// (`.done` files are skipped — they end `.ndjson.done`).
fn find_orphan_journal(workspace: &Path) -> Option<(String, String)> {
    let loom = workspace.join(".loom");
    for entry in std::fs::read_dir(&loom).ok()?.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(mid) = name
            .strip_prefix("journal-")
            .and_then(|s| s.strip_suffix(".ndjson"))
        {
            return Some((name.clone(), mid.to_string()));
        }
    }
    None
}

struct Fixture {
    _tmp: tempfile::TempDir,
    workspace: PathBuf,
    stub_dir: PathBuf,
    marker_dir: PathBuf,
}

fn fixture(feature_id: &str) -> Fixture {
    let tmp = tempfile::tempdir().expect("tempdir");
    let workspace = tmp.path().join("workspace");
    write_feature(&workspace.join(".ae/features/active"), feature_id);
    let stub_dir = tmp.path().join("stub");
    std::fs::create_dir_all(&stub_dir).unwrap();
    write_stub(&stub_dir);
    let marker_dir = tmp.path().join("markers");
    std::fs::create_dir_all(&marker_dir).unwrap();
    Fixture {
        _tmp: tmp,
        workspace,
        stub_dir,
        marker_dir,
    }
}

fn loom(workspace: &Path) -> PathBuf {
    workspace.join(".loom")
}

/// AC6 — SIGKILL mid-run leaves an orphan journal; the next startup recovers it
/// into a synthesized dispatch log and renames the journal `.done`.
#[test]
fn sigkill_mid_run_recovers_orphan_journal_on_restart() {
    let fx = fixture("F-200");

    // Run 1: blocking worker → worker-start synced → SIGKILL loom (no delivery).
    let mut child = spawn_loom(
        &fx.workspace,
        &["dispatch", "F-200"],
        &fx.stub_dir,
        &fx.marker_dir,
        StubMode::Blocking,
    );
    poll_markers(&mut child, &fx.marker_dir, READY_DEADLINE);
    let (orphan_journal, orphan_run_id) = find_orphan_journal(&fx.workspace)
        .expect("a journal-*.ndjson exists once the worker is in-flight");
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGKILL);
    }
    child.wait().ok();
    kill_recorded_stub(&fx.marker_dir); // the sleep worker reparented to init

    // Pre-restart: orphan journal present, NO dispatch log for it.
    assert!(
        loom(&fx.workspace).join(&orphan_journal).exists(),
        "orphan journal must survive the kill"
    );
    let synth_log = loom(&fx.workspace).join(format!("dispatch-{orphan_run_id}.log"));
    assert!(!synth_log.exists(), "the killed run wrote no dispatch log");

    // Clear markers so run 2's quick-exit worker can't be mistaken for run 1's.
    std::fs::remove_file(fx.marker_dir.join("ready")).ok();
    std::fs::remove_file(fx.marker_dir.join("working")).ok();

    // Run 2 (restart): startup recovery runs BEFORE this run's work.
    let mut child2 = spawn_loom(
        &fx.workspace,
        &["dispatch", "F-200"],
        &fx.stub_dir,
        &fx.marker_dir,
        StubMode::QuickExit,
    );
    wait_for_exit(&mut child2, EXIT_DEADLINE, &fx.marker_dir);

    // Recovery: orphan journal renamed .done + a dispatch log synthesized for it.
    assert!(
        loom(&fx.workspace)
            .join(format!("{orphan_journal}.done"))
            .exists(),
        "orphan journal must be renamed .done after recovery"
    );
    assert!(!loom(&fx.workspace).join(&orphan_journal).exists());
    assert!(
        synth_log.exists(),
        "recovery must synthesize a dispatch log for the orphan run"
    );
    let body = std::fs::read_to_string(&synth_log).unwrap();
    assert!(
        body.contains("F-200"),
        "synthesized log records the in-flight feature: {body}"
    );
    assert!(
        body.contains("\"worker_exit_status\": \"error\""),
        "a start-without-finish feature is recorded error/unknown: {body}"
    );

    // Idempotence: a third restart must NOT re-process the recovered orphan.
    let before = std::fs::read_to_string(&synth_log).unwrap();
    let mut child3 = spawn_loom(
        &fx.workspace,
        &["dispatch", "F-200"],
        &fx.stub_dir,
        &fx.marker_dir,
        StubMode::QuickExit,
    );
    wait_for_exit(&mut child3, EXIT_DEADLINE, &fx.marker_dir);
    assert_eq!(
        std::fs::read_to_string(&synth_log).unwrap(),
        before,
        "the recovered orphan's synthesized log must be untouched by a later run"
    );
    assert!(
        loom(&fx.workspace)
            .join(format!("{orphan_journal}.done"))
            .exists(),
        ".done journal stays .done (not re-scanned, not reverted)"
    );
}
