//! Minimal best-effort reader for `.claude/pipeline.yml` (F-022).
//!
//! Loom's only project-config knob today is the per-worker timeout. This reader
//! mirrors `discovery.rs`'s best-effort pattern: a missing or malformed
//! `pipeline.yml` degrades to the default and warns — it never aborts the run.
//! The single timeout key is deliberately the whole surface (F-019 split the
//! override out as exactly this); the struct composes cleanly with more keys later.

use serde::Deserialize;
use std::path::Path;
use std::time::Duration;
use tracing::{debug, warn};

/// Default per-worker timeout in minutes — the single source of truth (moved here
/// from `main.rs` in F-022). 90 min, raised from 30 in F-019.
pub const DEFAULT_WORKER_TIMEOUT_MINUTES: u64 = 90;

/// Project config read from `.claude/pipeline.yml`.
///
/// `#[serde(default)]` is **struct-level only** (paired with `impl Default`): an
/// absent `worker_timeout_minutes` key — or any of pipeline.yml's other keys
/// (`output`, `cross_family`, `test`, …) which serde ignores by default — yields
/// the 90-min default cleanly. A field-level `#[serde(default)]` would default the
/// `u64` to 0 and spuriously trip the zero-guard on an absent key, so it is omitted.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct LoomConfig {
    pub worker_timeout_minutes: u64,
}

impl Default for LoomConfig {
    fn default() -> Self {
        Self {
            worker_timeout_minutes: DEFAULT_WORKER_TIMEOUT_MINUTES,
        }
    }
}

impl LoomConfig {
    /// The worker timeout as a `Duration`. Overflow-safe: a pathological
    /// `worker_timeout_minutes` whose `× 60` overflows `u64` falls back to the
    /// 90-min default rather than panicking.
    pub fn worker_timeout(&self) -> Duration {
        self.worker_timeout_minutes
            .checked_mul(60)
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(60 * DEFAULT_WORKER_TIMEOUT_MINUTES))
    }
}

/// Read `<workspace>/.claude/pipeline.yml` best-effort.
///
/// - missing file → `debug!` + default (absence is normal for many workspaces),
/// - unreadable / malformed YAML → `warn!` + default,
/// - parsed `worker_timeout_minutes == 0` → `warn!` + substitute the default
///   (a zero `Duration` would time out every worker instantly).
///
/// Never returns `Err` / never aborts — the run proceeds on the default.
pub fn load_config(workspace: &Path) -> LoomConfig {
    let path = workspace.join(".claude").join("pipeline.yml");
    let raw = match std::fs::read_to_string(&path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            debug!(path = %path.display(), "config: no pipeline.yml — using defaults");
            return LoomConfig::default();
        }
        Err(e) => {
            warn!(path = %path.display(), error = %e,
                  "config: pipeline.yml unreadable — using defaults");
            return LoomConfig::default();
        }
    };
    let mut cfg = match serde_yaml::from_str::<LoomConfig>(&raw) {
        Ok(c) => c,
        Err(e) => {
            warn!(path = %path.display(), error = %e,
                  "config: pipeline.yml malformed — using defaults");
            return LoomConfig::default();
        }
    };
    if cfg.worker_timeout_minutes == 0 {
        warn!(
            "config: worker_timeout_minutes is 0 — substituting default {}",
            DEFAULT_WORKER_TIMEOUT_MINUTES
        );
        cfg.worker_timeout_minutes = DEFAULT_WORKER_TIMEOUT_MINUTES;
    }
    cfg
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// A tempdir workspace whose `.claude/pipeline.yml` holds `body`.
    fn ws_with_pipeline(body: &str) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let claude = dir.path().join(".claude");
        std::fs::create_dir_all(&claude).unwrap();
        std::fs::File::create(claude.join("pipeline.yml"))
            .unwrap()
            .write_all(body.as_bytes())
            .unwrap();
        dir
    }

    #[test]
    fn explicit_value_parsed() {
        let ws = ws_with_pipeline("worker_timeout_minutes: 45\n");
        let cfg = load_config(ws.path());
        assert_eq!(cfg.worker_timeout_minutes, 45);
        assert_eq!(cfg.worker_timeout(), Duration::from_secs(60 * 45));
    }

    #[test]
    fn absent_key_defaults_to_90() {
        // pipeline.yml present, but no worker_timeout_minutes key — struct-level
        // #[serde(default)] + impl Default yields 90 (NOT 0 → no spurious zero-guard).
        let ws = ws_with_pipeline("output:\n  plans: .ae/plans\ntest:\n  command: cargo test\n");
        assert_eq!(load_config(ws.path()).worker_timeout_minutes, 90);
    }

    #[test]
    fn malformed_yaml_defaults_to_90() {
        let ws = ws_with_pipeline("{{{ not yaml at all\n");
        assert_eq!(load_config(ws.path()).worker_timeout_minutes, 90); // no panic, no Err
    }

    #[test]
    fn missing_file_defaults_to_90() {
        let dir = tempfile::tempdir().unwrap(); // no .claude/pipeline.yml at all
        assert_eq!(load_config(dir.path()).worker_timeout_minutes, 90);
    }

    #[test]
    fn zero_value_substituted_with_default() {
        let ws = ws_with_pipeline("worker_timeout_minutes: 0\n");
        assert_eq!(load_config(ws.path()).worker_timeout_minutes, 90); // zero-guard
    }

    #[test]
    fn worker_timeout_is_overflow_safe() {
        // u64::MAX * 60 overflows → checked_mul None → 90-min fallback, no panic.
        let cfg = LoomConfig {
            worker_timeout_minutes: u64::MAX,
        };
        assert_eq!(cfg.worker_timeout(), Duration::from_secs(60 * 90));
    }

    #[test]
    fn absent_key_deserializes_to_90_via_struct_default() {
        // Strengthens absent_key_defaults_to_90 (review P2): that test goes through
        // load_config, whose zero-guard would mask a FIELD-level #[serde(default)]
        // (u64 default 0 → guard → 90). Deserialize a partial YAML DIRECTLY (no
        // load_config, no zero-guard) — the struct-level default must yield 90 here;
        // a field-level default would yield 0 and fail this, catching the regression.
        let cfg: LoomConfig = serde_yaml::from_str("output:\n  plans: x\n").unwrap();
        assert_eq!(cfg.worker_timeout_minutes, 90);
    }

    #[test]
    fn wrong_type_value_defaults_to_90() {
        // A present-but-non-integer worker_timeout_minutes fails u64 deserialization →
        // the same warn + default path as a wholly-malformed file (review P2: a
        // distinct scenario from "entire file is garbage").
        let ws = ws_with_pipeline("worker_timeout_minutes: not_a_number\n");
        assert_eq!(load_config(ws.path()).worker_timeout_minutes, 90);
    }
}
