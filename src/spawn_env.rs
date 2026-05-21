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

use std::path::Path;
use tokio::process::Command;
use tracing::debug;

/// Read `PATH`, filter out any segment whose string contains `exclude_dir`'s
/// path, and apply the filtered value to `cmd`. Returns the filtered PATH
/// string so the caller can log / assert on it.
///
/// Comparison is by substring on the colon-separated PATH segments. v0.1 picks
/// substring over exact directory equality because on macOS the Loom binary
/// can sit under `target/debug/`, `target/release/`, or a `cargo install`
/// destination — substring catches all of them without canonicalization.
pub fn apply_scrubbed_path(cmd: &mut Command, exclude_dir: &Path) -> String {
    let exclude_str = exclude_dir.to_string_lossy();
    let original = std::env::var("PATH").unwrap_or_default();

    let filtered: Vec<&str> = original
        .split(':')
        .filter(|seg| !seg.is_empty() && !seg.contains(exclude_str.as_ref()))
        .collect();
    let filtered_path = filtered.join(":");

    debug!(
        exclude_dir = %exclude_str,
        original_segments = original.split(':').count(),
        filtered_segments = filtered.len(),
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

    // Unit tests mutate process-wide env, which races against parallel test
    // runners (cargo test threads share a process). Wrap both scenarios in
    // ONE test so the env-mutation sequence is deterministic. AC6 coverage
    // (real-subprocess PATH scrub) lives in `tests/spawn_env_test.rs`.
    #[test]
    fn filters_and_empty_path_cases() {
        let saved = std::env::var("PATH").ok();

        // Case 1: filter out matching segments.
        // SAFETY: this is the only env mutator in this single-threaded test.
        unsafe {
            std::env::set_var(
                "PATH",
                "/usr/bin:/Users/me/loom/target/release:/usr/local/bin",
            );
        }
        let mut cmd = Command::new("/bin/true");
        let filtered = apply_scrubbed_path(&mut cmd, &PathBuf::from("target/release"));
        assert!(!filtered.contains("loom/target/release"));
        assert!(filtered.contains("/usr/bin"));
        assert!(filtered.contains("/usr/local/bin"));

        // Case 2: empty PATH stays empty.
        unsafe {
            std::env::remove_var("PATH");
        }
        let mut cmd = Command::new("/bin/true");
        let filtered = apply_scrubbed_path(&mut cmd, &PathBuf::from("target/release"));
        assert_eq!(filtered, "");

        unsafe {
            if let Some(v) = saved {
                std::env::set_var("PATH", v);
            } else {
                std::env::remove_var("PATH");
            }
        }
    }
}
