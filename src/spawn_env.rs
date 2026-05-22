//! PATH-only environment scrub for spawned workers.
//!
//! Plan F-001 Step 6 / AC6: workers must NOT be able to reach the Loom binary
//! via `PATH`, so a worker that types `loom` can't recursively invoke its
//! orchestrator. HOME / USER / SHELL / TMPDIR / CLAUDE_* etc. are preserved
//! by NOT calling `env_clear()` — only `PATH` is rewritten.
//!
//! Per Codex MF3 + Architect MF4: prior design called for `env_clear()` which
//! also stripped CLAUDE_* and surprised users; the PATH-only approach is the
//! minimum-viable structural enforcement.
//!
//! F-003 Step 1 replaced the original substring-match scrub with a per-segment
//! canonical probe. Substring matched ANY PATH segment that textually contained
//! the binary's parent dir — over-strips shared dirs like `~/bin/` that hold
//! both `loom` and unrelated tools. Per-segment compares
//! `segment.join("loom").canonicalize()` against the canonical loom binary,
//! stripping ONLY segments that genuinely resolve to OUR binary. Fail-open
//! per segment: a non-canonicalizable segment is kept (defense-in-depth via
//! the LOOM_PARENT_PID env-var guard covers the residual recursion path).

use std::path::{Path, PathBuf};
use tokio::process::Command;
use tracing::{debug, warn};

/// Read `PATH`, filter out any segment that resolves to the same canonical
/// path as `loom_binary` (i.e., segments where `segment/loom` canonicalizes
/// to the same target), and apply the filtered value to `cmd`. Returns the
/// filtered PATH string so the caller can log / assert on it.
///
/// Algorithm (F-003): per-segment canonical probe.
///   1. Canonicalize `loom_binary`. On error, leave PATH unchanged and emit
///      a loud warning — the LOOM_PARENT_PID env-var guard (M3 second layer)
///      still prevents recursion. Fail-open is safe by design.
///   2. For each PATH segment from `std::env::split_paths`, compute
///      `segment.join("loom").canonicalize()`. If `Ok` and equal to the
///      canonical binary → strip. If `Err` → keep (segment cannot host our
///      binary either; over-stripping would re-introduce the BL-007 problem).
///   3. Rebuild via `std::env::join_paths` (cross-platform PATH semantics).
pub fn apply_scrubbed_path(cmd: &mut Command, loom_binary: &Path) -> String {
    let original = std::env::var_os("PATH").unwrap_or_default();

    let canonical_loom = match loom_binary.canonicalize() {
        Ok(p) => p,
        Err(err) => {
            // Loud visibility per Qwen Q5 disposition; no hard-exit because
            // defense-in-depth via LOOM_PARENT_PID env-var guard (F-003 Step 2)
            // still protects against recursion.
            eprintln!(
                "loom: warn: cannot canonicalize own binary path ({err}); \
                 PATH-scrub disabled — recursion prevention falls back to \
                 LOOM_PARENT_PID env-var guard alone"
            );
            warn!(
                error = %err,
                binary = %loom_binary.display(),
                "spawn_env.apply_scrubbed_path: canonicalize failed, leaving PATH unchanged"
            );
            let s = original.to_string_lossy().into_owned();
            cmd.env("PATH", &s);
            return s;
        }
    };

    let segments: Vec<PathBuf> = std::env::split_paths(&original).collect();
    let kept: Vec<PathBuf> = segments
        .iter()
        .filter(|seg| {
            // Skip empty segments (POSIX treats `::` as cwd; not relevant for us).
            if seg.as_os_str().is_empty() {
                return false;
            }
            // Per-segment probe: if `<seg>/loom` resolves to OUR canonical
            // binary, drop it; otherwise keep. Errors (no loom there, or
            // transient FS issue) → keep (fail-open per segment).
            match seg.join("loom").canonicalize() {
                Ok(candidate) => candidate != canonical_loom,
                Err(_) => true,
            }
        })
        .cloned()
        .collect();

    let filtered_path = std::env::join_paths(&kept)
        .map(|os| os.to_string_lossy().into_owned())
        .unwrap_or_default();

    debug!(
        loom_binary = %loom_binary.display(),
        canonical = %canonical_loom.display(),
        original_segments = segments.len(),
        filtered_segments = kept.len(),
        "spawn_env.apply_scrubbed_path"
    );

    cmd.env("PATH", &filtered_path);
    filtered_path
}

#[cfg(test)]
mod tests {
    use super::apply_scrubbed_path;
    use std::path::PathBuf;
    use tokio::process::Command;

    /// Helper: create a fake `loom` executable at `<dir>/loom` so canonicalize
    /// succeeds in unit tests. On Unix we set the exec bit; the test only
    /// cares that the path exists and resolves.
    fn write_fake_loom(dir: &std::path::Path) -> PathBuf {
        let p = dir.join("loom");
        std::fs::write(&p, b"#!/bin/sh\nexit 0\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        p
    }

    // Unit tests mutate process-wide env, which races against parallel test
    // runners (cargo test threads share a process). Wrap all scenarios in
    // ONE test so the env-mutation sequence is deterministic. AC6 end-to-end
    // coverage (real-subprocess PATH scrub via the dispatch path) lives in
    // `tests/spawn_env_test.rs`.
    #[test]
    fn per_segment_canonical_scrub_cases() {
        let saved = std::env::var("PATH").ok();

        let tmp_loom = tempfile::tempdir().unwrap();
        let loom_bin = write_fake_loom(tmp_loom.path());

        let tmp_other = tempfile::tempdir().unwrap();
        write_fake_loom(tmp_other.path()); // unrelated `loom`-named file

        let canonical_loom_dir = loom_bin.parent().unwrap().canonicalize().unwrap();
        let canonical_other_dir = tmp_other.path().canonicalize().unwrap();

        // Case 1: PATH containing the binary's parent dir → that segment
        // dropped, other dirs (even those holding a same-named file that
        // canonicalizes to a DIFFERENT path) preserved.
        let composed = format!(
            "/usr/bin:{}:{}:/usr/local/bin",
            canonical_loom_dir.display(),
            canonical_other_dir.display(),
        );
        // SAFETY: this is the only env mutator in this single-threaded test.
        unsafe {
            std::env::set_var("PATH", &composed);
        }
        let mut cmd = Command::new("/bin/true");
        let filtered = apply_scrubbed_path(&mut cmd, &loom_bin);
        assert!(
            !filtered.contains(canonical_loom_dir.to_str().unwrap()),
            "filtered PATH must not contain loom binary's parent dir; got {filtered}"
        );
        assert!(
            filtered.contains(canonical_other_dir.to_str().unwrap()),
            "unrelated dir with different-canonical loom must be preserved; got {filtered}"
        );
        assert!(filtered.contains("/usr/bin"));
        assert!(filtered.contains("/usr/local/bin"));

        // Case 2: empty PATH stays empty.
        unsafe {
            std::env::remove_var("PATH");
        }
        let mut cmd = Command::new("/bin/true");
        let filtered = apply_scrubbed_path(&mut cmd, &loom_bin);
        assert_eq!(filtered, "");

        // Case 3: canonicalize failure on the binary itself → fail-open
        // (PATH unchanged). Use a non-existent path.
        let bogus = PathBuf::from("/nonexistent/path/to/loom-binary-xyz");
        unsafe {
            std::env::set_var("PATH", "/usr/bin:/usr/local/bin");
        }
        let mut cmd = Command::new("/bin/true");
        let filtered = apply_scrubbed_path(&mut cmd, &bogus);
        assert_eq!(
            filtered, "/usr/bin:/usr/local/bin",
            "canonicalize failure must leave PATH unchanged (fail-open)"
        );

        unsafe {
            if let Some(v) = saved {
                std::env::set_var("PATH", v);
            } else {
                std::env::remove_var("PATH");
            }
        }
    }
}
