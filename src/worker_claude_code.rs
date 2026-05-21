//! `ClaudeCodeAdapter` — concrete `Worker` that spawns a subprocess and
//! streams its stdout / stderr.
//!
//! v0.1 uses this adapter directly. v0.2+ may add Codex / Gemini / oMLX
//! variants behind the same trait.
//!
//! Subprocess management pattern is the Codex CLI `consume_output` idiom
//! (codex-rs/core/src/exec.rs:1322-1425 @ 0b4f86095c8005d8f74e9c62b971d72c1670aa88):
//! concurrent stdout + stderr drain tasks, `tokio::select!` between
//! `child.wait()` and `tokio::time::timeout`, **process-group kill** on
//! timeout / cancellation (so `sh`-wrapped or `sudo`-wrapped grandchildren are
//! reaped, not orphaned to PID 1), bounded-timeout drain join that always
//! runs even if the child future is dropped.
//!
//! Step 6 will plug a real PATH-scrubbed env via `src/spawn_env.rs`; the v0.1
//! adapter exposes `env_vars` as the placeholder injection point.

use crate::artifact::{Artifact, FeatureSpec, WorkerVerdict};
use crate::worker::Worker;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, BufReader};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Spawns a child process (`claude --headless` in production; `/bin/echo` /
/// `/bin/sh` in tests) and captures its output as an `Artifact`.
pub struct ClaudeCodeAdapter {
    pub command: PathBuf,
    pub args: Vec<OsString>,
    pub timeout: Duration,
    pub io_drain_timeout: Duration,
    /// When `Some`, env is reset to this map before spawn (clears host env).
    /// Step 6's `spawn_env.rs` will populate this with the PATH-only-scrubbed
    /// env (HOME/USER/SHELL/TMPDIR preserved, Loom binary dir filtered out).
    /// v0.1 default: `None` ⇒ inherit host env for stub testing.
    pub env_vars: Option<HashMap<OsString, OsString>>,
}

impl ClaudeCodeAdapter {
    /// Construct with sensible defaults: 2s `io_drain_timeout`, no env override.
    pub fn new(command: PathBuf, args: Vec<OsString>, timeout: Duration) -> Self {
        Self {
            command,
            args,
            timeout,
            io_drain_timeout: Duration::from_secs(2),
            env_vars: None,
        }
    }
}

#[async_trait]
impl Worker for ClaudeCodeAdapter {
    async fn run(&self, spec: FeatureSpec, cancel: CancellationToken) -> Result<Artifact> {
        let started = Instant::now();

        let mut cmd = Command::new(&self.command);
        cmd.args(&self.args);
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        // On Unix: put the child in its own process group so we can deliver
        // SIGKILL to the group on timeout / cancellation. Without this, a
        // child like `sh -c "sleep 60"` would have its `sh` killed but the
        // `sleep` grandchild would orphan to init (Track 3 + 4 P1 / plan MF1).
        #[cfg(unix)]
        cmd.process_group(0);

        if let Some(env) = &self.env_vars {
            cmd.env_clear();
            cmd.envs(env);
        }

        let mut child = cmd
            .spawn()
            .with_context(|| format!("spawn {:?}", self.command))?;

        // Capture child PID for the group-kill path; tokio::process::Child::id
        // returns Option (None after wait completes) — grab eagerly.
        let child_pid: Option<u32> = child.id();

        // Take BEFORE awaiting wait — otherwise the borrow checker rejects
        // partial-move of `child` (the E0382 footgun plan F-001 anticipated).
        let stdout_reader = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("stdout pipe unexpectedly unavailable"))?;
        let stderr_reader = child
            .stderr
            .take()
            .ok_or_else(|| anyhow!("stderr pipe unexpectedly unavailable"))?;

        // Concurrent drain tasks. Holding the JoinHandles in this future means
        // any future-drop also drops the handles; we always join them
        // explicitly below before `run` returns so they don't leak to
        // background — Track 3 P1.
        let stdout_drain = tokio::spawn(async move {
            let mut reader = BufReader::new(stdout_reader);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await?;
            Ok::<Vec<u8>, std::io::Error>(buf)
        });
        let stderr_drain = tokio::spawn(async move {
            let mut reader = BufReader::new(stderr_reader);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await?;
            Ok::<Vec<u8>, std::io::Error>(buf)
        });

        // Wait with timeout OR cancellation, whichever comes first.
        let (exit_status_opt, outcome) = tokio::select! {
            wait_result = tokio::time::timeout(self.timeout, child.wait()) => {
                match wait_result {
                    Ok(Ok(status)) => (Some(status), RunOutcome::Exited),
                    Ok(Err(e)) => return Err(anyhow!("waiting for child: {e}")),
                    Err(_elapsed) => (None, RunOutcome::TimedOut),
                }
            }
            _ = cancel.cancelled() => {
                (None, RunOutcome::Cancelled)
            }
        };

        // Group-kill + reap path (timeout or cancellation).
        let exit_status = if let Some(status) = exit_status_opt {
            status
        } else {
            kill_process_group(child_pid);
            // Race: process may have already exited by group-kill; start_kill
            // is harmless then. Always reap via wait to avoid zombies.
            let _ = child.start_kill();
            child
                .wait()
                .await
                .context("reap child after group-kill")?
        };

        // Drain join with bounded timeout — ALWAYS runs (Track 3 P1).
        // Even if the parent future was about to be dropped, we got here so
        // we can guarantee the drain tasks are awaited or aborted.
        let (stdout_bytes, stdout_trunc) = drain_with_timeout(
            stdout_drain,
            self.io_drain_timeout,
            "stdout",
            &spec.worker_identity,
        )
        .await;
        let (stderr_bytes, stderr_trunc) = drain_with_timeout(
            stderr_drain,
            self.io_drain_timeout,
            "stderr",
            &spec.worker_identity,
        )
        .await;
        let drain_truncated = stdout_trunc || stderr_trunc;

        // Persist stdout to `<feature_dir>/.loom/workers/<identity>.stdout`.
        let stdout_path = spec
            .feature_dir
            .join(".loom")
            .join("workers")
            .join(format!("{}.stdout", spec.worker_identity));
        if let Some(parent) = stdout_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create dir {:?}", parent))?;
        }
        tokio::fs::write(&stdout_path, &stdout_bytes)
            .await
            .with_context(|| format!("write stdout file {:?}", stdout_path))?;

        let verdict = match outcome {
            RunOutcome::Cancelled => WorkerVerdict::Cancelled,
            RunOutcome::TimedOut => WorkerVerdict::Timeout,
            RunOutcome::Exited if exit_status.success() => WorkerVerdict::Pass,
            RunOutcome::Exited => WorkerVerdict::Fail,
        };

        let reasoning_trace = if stderr_bytes.is_empty() {
            None
        } else {
            Some(String::from_utf8_lossy(&stderr_bytes).into_owned())
        };

        Ok(Artifact {
            verdict,
            stdout_path,
            reasoning_trace,
            duration: started.elapsed(),
            worker_identity: spec.worker_identity,
            exit_code: exit_status.code().unwrap_or(-1),
            drain_truncated,
        })
    }
}

enum RunOutcome {
    Exited,
    TimedOut,
    Cancelled,
}

/// Join a drain task with a bounded timeout. Returns the bytes + a
/// `truncated` flag (true if any failure path). Failures are logged via
/// `tracing::warn!` so the operator sees them once Step 5 wires up a
/// subscriber (Track 1 P1: don't silently swallow data-loss errors).
async fn drain_with_timeout(
    handle: tokio::task::JoinHandle<std::io::Result<Vec<u8>>>,
    timeout: Duration,
    stream: &str,
    worker_identity: &str,
) -> (Vec<u8>, bool) {
    match tokio::time::timeout(timeout, handle).await {
        Ok(Ok(Ok(bytes))) => (bytes, false),
        Ok(Ok(Err(io_err))) => {
            warn!(
                stream = stream,
                worker = worker_identity,
                error = %io_err,
                "drain task hit IO error; output truncated",
            );
            (Vec::new(), true)
        }
        Ok(Err(join_err)) => {
            warn!(
                stream = stream,
                worker = worker_identity,
                error = %join_err,
                "drain task panicked or was cancelled; output truncated",
            );
            (Vec::new(), true)
        }
        Err(_elapsed) => {
            warn!(
                stream = stream,
                worker = worker_identity,
                timeout_ms = timeout.as_millis() as u64,
                "drain task exceeded io_drain_timeout; output truncated",
            );
            (Vec::new(), true)
        }
    }
}

/// Send `SIGKILL` to the child's process group on Unix. Best-effort: if
/// `pid` is `None` (child already reaped) or the syscall fails, we move on
/// — the subsequent `child.wait()` is the authoritative reap.
#[cfg(unix)]
fn kill_process_group(pid: Option<u32>) {
    if let Some(pid) = pid {
        // SAFETY: libc::kill with negative pid signals the process group. We
        // own the PID (the child we spawned) + the process group id matches
        // the child's PID because of `process_group(0)` at spawn.
        let pgid_signed = -(pid as i32);
        unsafe {
            libc::kill(pgid_signed, libc::SIGKILL);
        }
    }
}

#[cfg(not(unix))]
fn kill_process_group(_pid: Option<u32>) {
    // No-op on non-Unix (Windows path uses Job Objects, out of v0.1 scope).
}
