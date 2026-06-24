//! Phase 1 — Discovery.
//!
//! `discover_features` spawns `claude -p "<prompt>" --permission-mode
//! bypassPermissions` twice (once for `/ae:backlog`, once for `/ae:analyze`,
//! hardcoded sequence per disc 002 AE-BL #9 SOFT) and then reads the
//! resulting feature DAG out of `<workspace>/.ae/features/active/`.
//!
//! When `claude` is not on PATH we skip the spawn and just read whatever
//! features the user pre-staged — Loom still works as a pure-dispatch
//! orchestrator over manually-staged features in that mode.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;
use tracing::{info, warn};

/// Parsed `pipeline:` sub-block of an AE feature `index.md`.
#[derive(Debug, Default, Deserialize)]
struct PipelineFrontmatter {
    #[serde(default)]
    work: Option<String>,
}

/// Subset of fields we read from a feature's `index.md` frontmatter.
#[derive(Debug, Deserialize)]
struct FeatureFrontmatter {
    id: String,
    #[serde(default)]
    depends_on: Vec<String>,
    #[serde(default)]
    pipeline: PipelineFrontmatter,
}

/// A feature node as understood by the dispatcher.
#[derive(Debug, Clone)]
pub struct DiscoveredFeature {
    pub id: String,
    pub feature_dir: std::path::PathBuf,
    pub depends_on: Vec<String>,
    /// `pipeline.work` value: `"done"`, `"in_progress"`, missing/None, etc.
    pub work_state: Option<String>,
}

impl DiscoveredFeature {
    pub fn is_done(&self) -> bool {
        matches!(self.work_state.as_deref(), Some("done"))
    }
}

/// Phase 1 entry point. Best-effort: invokes AE when `claude` is reachable,
/// then always reads the on-disk DAG. The return value reflects the on-disk
/// truth regardless of whether the spawn succeeded.
pub async fn discover_features(goal: &str, workspace: &Path) -> Result<Vec<DiscoveredFeature>> {
    if let Err(e) = maybe_invoke_ae(goal, workspace).await {
        warn!(
            error = %e,
            "discovery: AE invocation skipped — falling back to on-disk feature read",
        );
    }
    read_active_features(workspace)
}

async fn maybe_invoke_ae(goal: &str, workspace: &Path) -> Result<()> {
    // Only invoke if `claude` is on PATH. We do NOT scrub PATH here because
    // discovery runs in the orchestrator's own env, not a worker's; the
    // PATH-scrub guarantee applies to worker spawns only.
    if which("claude").is_none() {
        warn!("discovery: `claude` not on PATH — AE invocation skipped, falling back to on-disk feature read");
        return Ok(());
    }
    info!(goal = %goal, workspace = %workspace.display(), "discovery: invoking claude -p for ae:backlog + ae:analyze");

    // Hardcoded sequence per disc 002 Doodlestein strategic (AE-BL #9 SOFT).
    // Best-effort: we surface non-zero exit as a warning but proceed.
    //
    // Spawn shape: `claude -p "<prompt>" --permission-mode bypassPermissions`.
    // bypassPermissions is required for headless execution (no operator to
    // approve Bash/Edit prompts); slash command `/ae:backlog <goal>` triggers
    // the skill inside the spawned session. Mirrors default_worker pattern.
    let backlog_prompt = format!("/ae:backlog {}", goal);
    let backlog = tokio::process::Command::new("claude")
        .args([
            "-p",
            &backlog_prompt,
            "--permission-mode",
            "bypassPermissions",
        ])
        .current_dir(workspace)
        .status()
        .await
        .context("spawn claude -p /ae:backlog")?;
    if !backlog.success() {
        warn!(status = ?backlog, "discovery: ae:backlog returned non-zero");
    }

    let analyze = tokio::process::Command::new("claude")
        .args([
            "-p",
            "/ae:analyze",
            "--permission-mode",
            "bypassPermissions",
        ])
        .current_dir(workspace)
        .status()
        .await
        .context("spawn claude -p /ae:analyze")?;
    if !analyze.success() {
        warn!(status = ?analyze, "discovery: ae:analyze returned non-zero");
    }
    Ok(())
}

/// Walk `<workspace>/.ae/features/active/*/index.md` and parse each frontmatter.
pub fn read_active_features(workspace: &Path) -> Result<Vec<DiscoveredFeature>> {
    let dir = workspace.join(".ae").join("features").join("active");
    read_features_in(&dir, false)
}

/// Walk `<workspace>/.ae/features/done/*/index.md`, forcing `work_state =
/// Some("done")` on every returned feature regardless of the archived
/// `index.md`'s `pipeline.work` value. A feature AE's `/ae:review` archived
/// `active/ → done/` is terminally complete; surfacing it here lets the DAG
/// scheduler credit it as a satisfied dependency so downstream features become
/// ready (F-017 / BL-022 — the active-only scan dropped this credit).
pub fn read_done_features(workspace: &Path) -> Result<Vec<DiscoveredFeature>> {
    let dir = workspace.join(".ae").join("features").join("done");
    read_features_in(&dir, true)
}

/// Shared dir-walk for [`read_active_features`] / [`read_done_features`].
///
/// Missing `dir` → `Ok(Vec::new())` with NO warning (the absence of an
/// `active/` or `done/` dir is normal, not an error). A real `read_dir` failure
/// propagates as `Err`. When `force_done` is set, `work_state` is overwritten to
/// `Some("done")` (the archived-features case); otherwise it reflects the parsed
/// `pipeline.work` field verbatim.
fn read_features_in(dir: &Path, force_done: bool) -> Result<Vec<DiscoveredFeature>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    // Collect first, then sort by feature_dir basename — `read_dir` order is
    // filesystem-dependent (ext4/HFS+ differ), which would flake the ready-set
    // ordering + worker_identity-by-index assignment in dispatch.rs.
    // Architect P2-3 + Challenger C7 @ /ae:review 2026-05-21.
    let mut entries: Vec<std::path::PathBuf> = std::fs::read_dir(dir)
        .with_context(|| format!("read_dir {:?}", dir))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.is_dir())
        .collect();
    entries.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

    let mut out = Vec::new();
    for path in entries {
        let index = path.join("index.md");
        if !index.exists() {
            continue;
        }
        match parse_frontmatter(&index) {
            Ok(fm) => out.push(DiscoveredFeature {
                id: fm.id,
                feature_dir: path,
                depends_on: fm.depends_on,
                work_state: if force_done {
                    Some("done".to_string())
                } else {
                    fm.pipeline.work
                },
            }),
            Err(e) => warn!(
                feature_dir = %path.display(),
                error = %e,
                "discovery: skipping feature with unparseable frontmatter"
            ),
        }
    }
    Ok(out)
}

fn parse_frontmatter(path: &Path) -> Result<FeatureFrontmatter> {
    let content = std::fs::read_to_string(path).with_context(|| format!("read {:?}", path))?;
    // Strip leading `---\n ... \n---\n` block.
    let rest = content
        .strip_prefix("---\n")
        .ok_or_else(|| anyhow::anyhow!("missing frontmatter delimiter in {:?}", path))?;
    let end = rest
        .find("\n---")
        .ok_or_else(|| anyhow::anyhow!("missing closing frontmatter delimiter in {:?}", path))?;
    let yaml = &rest[..end];
    let fm: FeatureFrontmatter =
        serde_yaml::from_str(yaml).with_context(|| format!("parse frontmatter in {:?}", path))?;
    crate::feature_id::validate_feature_id(&fm.id)
        .with_context(|| format!("invalid feature id in {:?}", path))?;
    Ok(fm)
}

/// Minimal `which` — returns the first PATH segment containing `name`.
fn which(name: &str) -> Option<std::path::PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_active_features_handles_empty_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let features = read_active_features(tmp.path()).unwrap();
        assert!(features.is_empty());
    }

    #[test]
    fn read_active_features_parses_frontmatter() {
        let tmp = tempfile::tempdir().unwrap();
        let feature_dir = tmp.path().join(".ae/features/active/F-100-demo");
        std::fs::create_dir_all(&feature_dir).unwrap();
        std::fs::write(
            feature_dir.join("index.md"),
            "---\nid: F-100\ndepends_on:\n  - F-099\npipeline:\n  work: done\n---\n\nbody\n",
        )
        .unwrap();
        let features = read_active_features(tmp.path()).unwrap();
        assert_eq!(features.len(), 1);
        assert_eq!(features[0].id, "F-100");
        assert_eq!(features[0].depends_on, vec!["F-099"]);
        assert!(features[0].is_done());
    }

    #[test]
    fn read_active_features_missing_depends_on_is_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let feature_dir = tmp.path().join(".ae/features/active/F-200-demo");
        std::fs::create_dir_all(&feature_dir).unwrap();
        std::fs::write(
            feature_dir.join("index.md"),
            "---\nid: F-200\npipeline:\n  work: in_progress\n---\n",
        )
        .unwrap();
        let features = read_active_features(tmp.path()).unwrap();
        assert_eq!(features.len(), 1);
        assert!(features[0].depends_on.is_empty());
        assert!(!features[0].is_done());
    }

    #[test]
    fn read_active_features_skips_invalid_id() {
        let tmp = tempfile::tempdir().unwrap();
        // Valid feature dir.
        let good = tmp.path().join(".ae/features/active/F-006-demo");
        std::fs::create_dir_all(&good).unwrap();
        std::fs::write(good.join("index.md"), "---\nid: F-006\n---\n").unwrap();
        // Invalid id — path-traversal attempt; must be skipped, not abort.
        let bad = tmp.path().join(".ae/features/active/bad");
        std::fs::create_dir_all(&bad).unwrap();
        std::fs::write(bad.join("index.md"), "---\nid: \"../etc\"\n---\n").unwrap();
        // Invalid id — 4-digit long-id bypass; also skipped, distinct class.
        let bad2 = tmp.path().join(".ae/features/active/F-1234-demo");
        std::fs::create_dir_all(&bad2).unwrap();
        std::fs::write(bad2.join("index.md"), "---\nid: F-1234\n---\n").unwrap();

        let features = read_active_features(tmp.path()).unwrap();
        assert_eq!(features.len(), 1, "invalid-id features must be skipped");
        assert_eq!(features[0].id, "F-006");
    }

    #[test]
    fn read_done_features_forces_done_and_skips_invalid() {
        let tmp = tempfile::tempdir().unwrap();
        // Valid done feature whose archived index.md still says in_progress —
        // force_done MUST override it to "done" (F-017: archived = terminally complete).
        let good = tmp.path().join(".ae/features/done/F-003-slug");
        std::fs::create_dir_all(&good).unwrap();
        std::fs::write(
            good.join("index.md"),
            "---\nid: F-003\npipeline:\n  work: in_progress\n---\n",
        )
        .unwrap();
        // Invalid id — path-traversal attempt; must be skipped.
        let bad = tmp.path().join(".ae/features/done/bad");
        std::fs::create_dir_all(&bad).unwrap();
        std::fs::write(bad.join("index.md"), "---\nid: \"../etc\"\n---\n").unwrap();
        // Invalid id — 4-digit long-id bypass; also skipped.
        let bad2 = tmp.path().join(".ae/features/done/F-1234-x");
        std::fs::create_dir_all(&bad2).unwrap();
        std::fs::write(bad2.join("index.md"), "---\nid: F-1234\n---\n").unwrap();

        let features = read_done_features(tmp.path()).unwrap();
        assert_eq!(features.len(), 1, "invalid-id done features must be skipped");
        assert_eq!(features[0].id, "F-003");
        assert!(
            features[0].is_done(),
            "force_done must override the archived in_progress state"
        );
    }

    #[test]
    fn read_done_features_missing_dir_is_empty() {
        let tmp = tempfile::tempdir().unwrap();
        // No done/ dir → Ok(empty), no warn (mirrors read_active on missing active/).
        let features = read_done_features(tmp.path()).unwrap();
        assert!(features.is_empty());
    }
}
