//! Feature-id validation — the strict `^F-\d{3}(-slug)?$` grammar gate.
//!
//! Extracted from `discovery.rs` (F-007 / BL-025) so neither `discovery` (the
//! parse-time gate) nor `dispatch` (the worktree dir-name parser) owns the
//! id-grammar concern; both call into here.

use anyhow::Result;

/// Validate a feature id against the strict Loom convention `^F-\d{3}(-[a-z0-9-]+)?$`.
///
/// Byte-positional rather than char-class so the digit run is fixed at exactly
/// three and non-ASCII digits / stray UTF-8 are rejected. This is the single
/// gate before `id` flows into worktree paths (`dispatch.rs` worktree dir name)
/// and git ref names (`refs/heads/loom-features/<id>`), closing the
/// path-traversal + ref-injection surface (BL-006). Reused by `dispatch.rs`
/// when parsing `.loom/worktrees/<id>-<pid>` dir names back into ids.
pub(crate) fn validate_feature_id(id: &str) -> Result<()> {
    let b = id.as_bytes();
    let ok = b.len() >= 5
        && b[0] == b'F'
        && b[1] == b'-'
        && b[2..5].iter().all(u8::is_ascii_digit)
        && match b.get(5) {
            // `F-006`
            None => true,
            // `F-006-<slug>` — slug must be non-empty, ASCII [a-z0-9-]
            Some(b'-') => {
                b.len() > 6
                    && b[6..]
                        .iter()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
            }
            // anything else at index 5 (e.g. `F-0067`, `F-006x`)
            Some(_) => false,
        };
    if ok {
        Ok(())
    } else {
        anyhow::bail!("feature id {id:?} doesn't match ^F-\\d{{3}}(-slug)?$");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_feature_id_accepts_valid() {
        for id in ["F-006", "F-006-some-slug", "F-006-a-b-c", "F-001-stub", "F-100"] {
            assert!(validate_feature_id(id).is_ok(), "should accept {id:?}");
        }
    }

    #[test]
    fn validate_feature_id_rejects_invalid() {
        for id in [
            "../etc",         // path traversal
            "F-1/etc",        // slash (ref injection)
            "F-1\0",          // NUL
            "F-12",           // 2 digits
            "F-1234",         // 4 digits — long-id bypass
            "F-006-",         // empty slug
            "F-006-Bad_Slug", // uppercase + underscore
            "F-006-é",        // non-ASCII
            "F-0067",         // no dash after 3 digits
            "",               // empty
        ] {
            assert!(validate_feature_id(id).is_err(), "should reject {id:?}");
        }
    }
}
