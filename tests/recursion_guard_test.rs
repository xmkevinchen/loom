//! F-003 AC3 + AC4 — `LOOM_PARENT_PID` worker-side recursion guard tests.
//!
//! Three subprocess scenarios, all driven against `$CARGO_BIN_EXE_loom`:
//!
//! - **Test 3a (AC3 — `loom run` refusal)**: env-var set + `loom run "smoke"`
//!   → exit 6 (`EXIT_RECURSION_DETECTED`) with "LOOM_PARENT_PID" in stderr.
//! - **Test 3b (AC3 — `loom dispatch` refusal)**: env-var set + `loom dispatch`
//!   → same exit 6 + stderr text. Both Run and Dispatch arms must explicitly
//!   refuse per architect C2 plan-review finding.
//! - **Test 4 (AC4 — `loom status` survival)**: env-var set + `loom status` →
//!   exit 0 with normal output, NOT a refusal. Verifies the D-A discussion
//!   finding that read-only diagnostics remain available inside workers
//!   regardless of the env var (status / version / no-subcommand are
//!   intentionally unguarded).
//!
//! Each test spawns its own subprocess via `std::process::Command`, so the
//! `LOOM_PARENT_PID` env-var manipulation is per-child and safe to run in
//! parallel — no shared mutable parent state.

use std::path::PathBuf;
use std::process::Command;

fn loom_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_loom"))
}

#[test]
fn test_3a_run_refusal() {
    // Run in a tempdir so the child loom's `init_tracing()` (which fires
    // before the recursion guard in dispatch()) creates `.loom/run-*.log`
    // in an isolated dir, not the project root. Otherwise repeated test
    // runs pile up log files in the working tree.
    let tmp = tempfile::tempdir().expect("tempdir");
    let out = Command::new(loom_binary())
        .arg("run")
        .arg("smoke")
        .env("LOOM_PARENT_PID", "12345")
        .current_dir(tmp.path())
        .output()
        .expect("spawn loom run");

    assert_eq!(
        out.status.code(),
        Some(6),
        "loom run with LOOM_PARENT_PID must exit 6 (EXIT_RECURSION_DETECTED); \
         got status={:?} stderr={:?}",
        out.status,
        String::from_utf8_lossy(&out.stderr),
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("LOOM_PARENT_PID"),
        "stderr should diagnose the refusal via the env-var name; got {stderr:?}"
    );
}

#[test]
fn test_3b_dispatch_refusal() {
    // Same tempdir-cwd rationale as test_3a — keep init_tracing artifacts
    // isolated to a per-test directory.
    let tmp = tempfile::tempdir().expect("tempdir");
    let out = Command::new(loom_binary())
        .arg("dispatch")
        .arg("F-X-stub")
        .env("LOOM_PARENT_PID", "12345")
        .current_dir(tmp.path())
        .output()
        .expect("spawn loom dispatch");

    assert_eq!(
        out.status.code(),
        Some(6),
        "loom dispatch with LOOM_PARENT_PID must exit 6; \
         got status={:?} stderr={:?}",
        out.status,
        String::from_utf8_lossy(&out.stderr),
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("LOOM_PARENT_PID"),
        "stderr should diagnose the refusal via the env-var name; got {stderr:?}"
    );
}

#[test]
fn test_4_status_survival() {
    // `loom status` is read-only and must remain reachable inside workers
    // even when LOOM_PARENT_PID is set — D-A invariance constraint.
    //
    // Run in a tempdir so an existing `.loom/status.json` in the project
    // root doesn't influence the child's view of state (status reads CWD).
    let tmp = tempfile::tempdir().expect("tempdir");
    let out = Command::new(loom_binary())
        .arg("status")
        .env("LOOM_PARENT_PID", "12345")
        .current_dir(tmp.path())
        .output()
        .expect("spawn loom status");

    assert_eq!(
        out.status.code(),
        Some(0),
        "loom status with LOOM_PARENT_PID must still succeed (D-A invariance); \
         got status={:?} stderr={:?}",
        out.status,
        String::from_utf8_lossy(&out.stderr),
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        !stdout.contains("refusing"),
        "stdout must not contain a refusal message; got {stdout:?}"
    );
    // In an empty tempdir the status surface emits the no-state-found
    // message. Either that exact wording or a normal "status file:" line
    // would prove the command ran past the guard.
    assert!(
        stdout.contains("no loom run state found") || stdout.contains("status file:"),
        "stdout should look like normal status output, not a refusal; got {stdout:?}"
    );
}
