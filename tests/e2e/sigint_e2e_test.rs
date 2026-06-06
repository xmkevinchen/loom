//! e2e SIGINT harness (F-012): spawns the real `loom` binary, delivers a real
//! mid-flight SIGINT, and asserts the causal cancellation chain — exit code
//! 130 PLUS a parsed dispatch-log outcome with `worker_exit_status ==
//! "cancelled"` (the only record reachable solely via the worker's `select!`
//! cancel arm, dispatch.rs). Zero production changes; all machinery lives in
//! this test target.
//!
//! Design source: `.ae/features/active/F-012-*/discussions/001-sigint-e2e-scope/`
//! (conclusion.md Decisions 1-5 + constraint ledger C1-C16).
//!
//! Fallback note (designated, do not improvise): if readiness polling proves
//! flaky on slow CI, swap to the pipe handshake — test creates `pipe()`,
//! passes the write-fd number via a `LOOM_TEST_HANDSHAKE_FD` env var
//! (`Command::env`), the stub writes 1 byte to that fd instead of marker
//! files, and the test blocks on `read()`.
#![cfg(unix)]

use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Poll cadence for child-exit and marker polling (10-50ms band per
/// discussion topic 05; 25ms balances latency vs spin).
const POLL_CADENCE: Duration = Duration::from_millis(25);

/// Loom-exit deadline after SIGINT (and overall run budget for the negative
/// control). Budget arithmetic refined in Step 5 (C11).
const EXIT_DEADLINE: Duration = Duration::from_secs(15);

/// Readiness gate: both stub markers must appear before the test signals
/// (conclusion Decision 2 / C3 — proves "interrupted running work", not
/// "killed at spawn").
const READY_DEADLINE: Duration = Duration::from_secs(10);

fn loom_bin() -> &'static str {
    env!("CARGO_BIN_EXE_loom")
}

/// Minimal feature fixture — mirrors verdict_multi_cycle_test::write_feature
/// (local copy; cross-target dedup is BL-032). `pipeline.work: in_progress`
/// is load-bearing: `work: done` features are filtered by ready_set, so no
/// worker would run and no dispatch log would be written (plan review MF-d).
fn write_feature(features_root: &Path, id: &str) {
    let dir = features_root.join(id);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("index.md"),
        format!("---\nid: {id}\npipeline:\n  work: in_progress\n---\n\nbody\n"),
    )
    .unwrap();
}

/// Pre-staged passing review — without it a clean worker classifies
/// `verdict: "missing"` and loom exits 8 (EXIT_REVIEW_MISSING, F-014), not 0
/// (Doodlestein-adversarial first-cliff).
fn write_review_pass(features_root: &Path, id: &str) {
    std::fs::write(
        features_root.join(id).join("review.md"),
        "---\nverdict: pass\n---\n",
    )
    .unwrap();
}

/// PATH-stub `claude` (strict POSIX sh — C6). Discriminates worker calls from
/// discovery calls via LOOM_PARENT_PID (C5: injected on every worker spawn at
/// worker_claude_code.rs:141, never for discovery at discovery.rs:63-90 —
/// prompt sniffing is forbidden, it hangs on goals containing skill names).
///
/// Worker branch (blocking, default): writes the ready marker, then PID +
/// working marker (C3 double-marker; `$$` survives `exec` — the PID is
/// unchanged), then `exec sleep 600` (no grandchild — C6).
/// Worker branch (LOOM_STUB_MODE=exit): exits 0 immediately — used by the
/// negative control, where a blocking worker would hang loom to the 30min
/// worker timeout (C4).
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

/// Stub invocation mode, passed per-child via `Command::env` (C9 — never
/// `std::env::set_var`, which would corrupt parallel sibling tests).
enum StubMode {
    Blocking,
    QuickExit,
}

/// Spawn the real loom binary against `workspace`.
///
/// NOTE (C16/C8): this is `std::process::Command`, which needs the
/// `CommandExt` trait import for `.process_group(0)` — do NOT copy the
/// production pattern (worker_claude_code.rs:123 calls process_group on a
/// TOKIO Command, which has it natively). Forgetting the group would let the
/// timeout cleanup's kill hit the cargo-test runner's process group.
fn spawn_loom(
    workspace: &Path,
    args: &[&str],
    stub_dir: &Path,
    marker_dir: &Path,
    mode: StubMode,
) -> Child {
    // Absolute stub dir prepended (C7): a relative entry would resolve
    // against the WORKER's cwd (the feature dir), not the test cwd.
    let path_var = format!(
        "{}:{}",
        stub_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let mut cmd = Command::new(loom_bin());
    cmd.args(args)
        .current_dir(workspace)
        .env("PATH", path_var)
        .env("LOOM_STUB_MARKER_DIR", marker_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let StubMode::QuickExit = mode {
        cmd.env("LOOM_STUB_MODE", "exit");
    }
    cmd.process_group(0);
    cmd.spawn().expect("spawn loom binary")
}

enum WaitOutcome {
    /// Loom exited on its own. `output` carries labeled stdout+stderr so
    /// assertion failures on unexpected statuses keep their root cause
    /// (gemini review P2: crash-before-deadline must not lose logs).
    Exited {
        status: std::process::ExitStatus,
        output: String,
    },
    /// Deadline overrun. Diagnostics were printed to stderr and are returned
    /// for assertion (Step 5 automated diagnostics test).
    TimedOut { diagnostics: String },
}

/// Drain a pipe on a background thread from spawn time — draining only after
/// exit would let a chatty child fill the pipe buffer, block in write(2), and
/// masquerade as a hang / false timeout (codex review P1).
fn spawn_reader<R: Read + Send + 'static>(pipe: Option<R>) -> std::thread::JoinHandle<String> {
    std::thread::spawn(move || {
        let mut buf = String::new();
        if let Some(mut p) = pipe {
            p.read_to_string(&mut buf).ok();
        }
        buf
    })
}

/// Poll `child` until exit or `deadline`.
///
/// Both paths end with a last-resort `kill_recorded_stub` (C12 extended per
/// review: loom crashing or exiting without tearing down its worker must not
/// leak an `exec sleep 600` into parallel CI). On deadline: diagnostics first
/// (C11 ordering — they must survive a slow kill), then `child.kill()`, then
/// the stub kill.
fn wait_with_deadline(
    child: &mut Child,
    deadline: Duration,
    workspace: &Path,
    marker_dir: &Path,
) -> WaitOutcome {
    let stdout_reader = spawn_reader(child.stdout.take());
    let stderr_reader = spawn_reader(child.stderr.take());
    let collect = |out: std::thread::JoinHandle<String>, err: std::thread::JoinHandle<String>| {
        format!(
            "--- loom stdout ---\n{}\n--- loom stderr ---\n{}\n",
            out.join().unwrap_or_default(),
            err.join().unwrap_or_default()
        )
    };

    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("try_wait loom") {
            let output = collect(stdout_reader, stderr_reader);
            kill_recorded_stub(marker_dir);
            return WaitOutcome::Exited { status, output };
        }
        if start.elapsed() >= deadline {
            let mut diagnostics = format!(
                "loom did not exit within {deadline:?}.\n\
                 hint: under a CI wrapper that sets SIGINT to SIG_IGN (e.g. nohup-style\n\
                 supervisors) the signal is discarded before loom's handler — a timeout\n\
                 here (rather than signal-death) is the documented signature of that\n\
                 wrapper environment (C13).\n\
                 .loom artifacts: {:?}\n",
                list_loom_artifacts(workspace)
            );
            eprintln!("{diagnostics}");
            child.kill().ok();
            child.wait().ok();
            kill_recorded_stub(marker_dir);
            let tail = collect(stdout_reader, stderr_reader);
            eprintln!("{tail}");
            diagnostics.push_str(&tail);
            return WaitOutcome::TimedOut { diagnostics };
        }
        std::thread::sleep(POLL_CADENCE);
    }
}

fn list_loom_artifacts(workspace: &Path) -> Vec<String> {
    std::fs::read_dir(workspace.join(".loom"))
        .map(|entries| {
            entries
                .flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default()
}

/// Read the stub PID from the markers and SIGKILL it — last-resort cleanup on
/// every wait resolution. On the healthy exit-130 path the stub is already
/// dead (loom's group-kill); SIGKILL on the dead PID is a harmless ESRCH.
/// Falls back to the ready marker for the window where the stub was killed
/// between its two marker writes (codex review P2).
fn kill_recorded_stub(marker_dir: &Path) {
    if let Some(pid) = read_stub_pid(marker_dir) {
        unsafe {
            libc::kill(pid as libc::pid_t, libc::SIGKILL);
        }
    }
}

fn read_stub_pid(marker_dir: &Path) -> Option<u32> {
    // Parse PER marker — an existing-but-unparseable `working` (created but
    // not yet flushed) must fall through to `ready`, not abort the lookup
    // (codex review P2: find_map over read-only stopped at the first
    // readable file).
    ["working", "ready"].iter().find_map(|m| {
        std::fs::read_to_string(marker_dir.join(m))
            .ok()?
            .trim()
            .parse()
            .ok()
    })
}

/// Poll until BOTH markers exist (C3 double-marker gate), returning the stub
/// PID. The working marker is written second, so its presence implies ready —
/// both are still checked explicitly. Panics loudly on deadline: a worker
/// that never reached in-flight state means the chain under test never armed.
fn poll_markers(marker_dir: &Path, deadline: Duration) -> u32 {
    let start = Instant::now();
    loop {
        if marker_dir.join("ready").exists() && marker_dir.join("working").exists() {
            if let Some(pid) = read_stub_pid(marker_dir) {
                return pid;
            }
        }
        if start.elapsed() >= deadline {
            panic!(
                "readiness gate: ready/working markers not observed within {deadline:?} — \
                 worker never reached in-flight state (stub not invoked, or dispatch died early)"
            );
        }
        std::thread::sleep(POLL_CADENCE);
    }
}

/// Assert the graceful-cancel exit shape via an explicit three-branch match
/// (plan review codex-MF: the diagnostic intent must be loud — a bare
/// assert_eq! would print `None != Some(130)` with no hint that signal-death
/// means the readiness heuristic lost the race).
fn assert_exit_130(status: std::process::ExitStatus, output: &str) {
    match status.code() {
        Some(130) => {}
        Some(n) => panic!("expected exit 130, got {n} — loom exited without graceful cancel\n{output}"),
        None => panic!(
            "loom died by signal {:?} — SIGINT handler race lost (readiness gate fired too early)\n{output}",
            status.signal()
        ),
    }
}

#[derive(serde::Deserialize)]
struct DispatchLog {
    outcomes: Vec<Outcome>,
}

/// Test-local deserialization target (plan review MF-a) — serde ignores the
/// log's other fields by default; a malformed log must PANIC, never silently
/// yield zero outcomes.
#[derive(serde::Deserialize)]
struct Outcome {
    worker_exit_status: String,
}

/// Parse every `.loom/dispatch-*.log` (C15 — NOT `run-*.log`, that is the
/// tracing log) and concatenate their outcomes.
fn read_dispatch_outcomes(workspace: &Path) -> Vec<Outcome> {
    let mut all = Vec::new();
    let Ok(entries) = std::fs::read_dir(workspace.join(".loom")) else {
        return all;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with("dispatch-") && name.ends_with(".log") {
            let body = std::fs::read_to_string(entry.path()).expect("read dispatch log");
            let log: DispatchLog = serde_json::from_str(&body)
                .unwrap_or_else(|e| panic!("malformed dispatch log {name}: {e}"));
            all.extend(log.outcomes);
        }
    }
    all
}

/// Per-test fixture bundle: workspace + stub dir + marker dir, all inside one
/// per-test tempdir (C10 — stale sentinels from prior runs / parallel
/// cross-test reads are structurally impossible).
struct Fixture {
    _tmp: tempfile::TempDir,
    workspace: PathBuf,
    stub_dir: PathBuf,
    marker_dir: PathBuf,
}

fn fixture(feature_id: &str) -> Fixture {
    let tmp = tempfile::tempdir().expect("tempdir");
    let workspace = tmp.path().join("workspace");
    let features_root = workspace.join(".ae/features/active");
    write_feature(&features_root, feature_id);
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

/// AC1 — dispatch entry point: real loom, real mid-flight SIGINT, graceful
/// exit 130 + the causal dispatch-log record. No pre-staged review.md here —
/// the cancelled (non-clean) worker classifies `verdict: "unknown"`, so the
/// F-014 review-missing path (exit 8) cannot fire (adversarial trace).
#[test]
fn dispatch_mid_flight_sigint_exits_130_with_cancelled_outcome() {
    let fx = fixture("F-100");
    let mut child = spawn_loom(
        &fx.workspace,
        &["dispatch", "F-100"],
        &fx.stub_dir,
        &fx.marker_dir,
        StubMode::Blocking,
    );
    poll_markers(&fx.marker_dir, READY_DEADLINE);

    // pid-targeted SIGINT on loom's OWN handler path (conclusion Decision 4 —
    // killpg would also signal pgid-inheriting children and mask the chain).
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGINT);
    }

    match wait_with_deadline(&mut child, EXIT_DEADLINE, &fx.workspace, &fx.marker_dir) {
        WaitOutcome::Exited { status, output } => assert_exit_130(status, &output),
        WaitOutcome::TimedOut { diagnostics } => {
            panic!("loom hung after SIGINT — cancel chain never completed\n{diagnostics}")
        }
    }

    // Positive-existence on the parsed field (C1): the only record reachable
    // solely via the select! cancel arm — unfakeable by the SIGKILLed stub.
    let outcomes = read_dispatch_outcomes(&fx.workspace);
    assert!(
        outcomes.iter().any(|o| o.worker_exit_status == "cancelled"),
        "dispatch log must record a cancelled worker outcome (got {:?})",
        outcomes
            .iter()
            .map(|o| o.worker_exit_status.as_str())
            .collect::<Vec<_>>()
    );
}

/// AC3 — negative control: clean run, no signal, zero "cancelled" outcomes.
/// The ≥1-outcome clause forces the parser onto a REAL log, closing the
/// vacuous absent-file pass; this is what makes the positive tests'
/// "cancelled" assertion non-hardcodable.
#[test]
fn no_signal_clean_run_has_zero_cancelled() {
    let fx = fixture("F-100");
    write_review_pass(&fx.workspace.join(".ae/features/active"), "F-100");

    let mut child = spawn_loom(
        &fx.workspace,
        &["dispatch", "F-100"],
        &fx.stub_dir,
        &fx.marker_dir,
        StubMode::QuickExit,
    );
    let (status, output) =
        match wait_with_deadline(&mut child, EXIT_DEADLINE, &fx.workspace, &fx.marker_dir) {
            WaitOutcome::Exited { status, output } => (status, output),
            WaitOutcome::TimedOut { diagnostics } => {
                panic!("negative control hung — quick-exit stub not honored?\n{diagnostics}")
            }
        };
    assert_eq!(
        status.code(),
        Some(0),
        "clean dispatch with pre-staged passing review must exit 0 \
         (8 here = review.md pre-stage missing, see write_review_pass)\n{output}"
    );

    let outcomes = read_dispatch_outcomes(&fx.workspace);
    assert!(
        !outcomes.is_empty(),
        "negative control must exercise the parser against a real dispatch log"
    );
    assert!(
        outcomes.iter().all(|o| o.worker_exit_status != "cancelled"),
        "no-signal run must record zero cancelled outcomes"
    );
}
