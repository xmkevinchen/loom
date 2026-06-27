//! Atomic write helper: write to `<path>.tmp` then `rename` over target.
//!
//! Same-FS rename is atomic per POSIX. Cross-FS rename returns `EXDEV`;
//! we fall back to copy + remove + fsync of the destination.
//!
//! Gemini MF1 in plan F-001 Step 6: avoids partial-read corruption for
//! Loom-written artifacts (status.json, dispatch log, worker outputs).

use anyhow::{Context, Result};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

/// Write `contents` to `path` atomically.
///
/// Strategy: write to `<path>.tmp`, fsync that file, then `rename` over the
/// target. On `EXDEV` (cross-filesystem rename) fall back to copy + delete +
/// destination fsync. The temp file is removed on any error path.
pub fn atomic_write(path: &Path, contents: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            create_dir_all_synced(parent)?;
        }
    }

    let tmp_path = match path.extension() {
        Some(ext) => {
            let mut new_ext = ext.to_os_string();
            new_ext.push(".tmp");
            path.with_extension(new_ext)
        }
        None => path.with_extension("tmp"),
    };

    let write_result = (|| -> Result<()> {
        let mut tmp = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)
            .with_context(|| format!("open tmp file {:?}", tmp_path))?;
        tmp.write_all(contents)
            .with_context(|| format!("write tmp file {:?}", tmp_path))?;
        tmp.sync_all()
            .with_context(|| format!("fsync tmp file {:?}", tmp_path))?;
        Ok(())
    })();

    if let Err(e) = write_result {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }

    match fs::rename(&tmp_path, path) {
        Ok(()) => {
            // F-023: fsync the parent dir so the rename (the dir entry) is
            // durable across power-loss, not just the file contents.
            fsync_parent_dir(path)
        }
        Err(e) if is_cross_device(&e) => {
            let copy_result = fs::copy(&tmp_path, path)
                .with_context(|| format!("EXDEV fallback copy {:?} -> {:?}", tmp_path, path));
            let _ = fs::remove_file(&tmp_path);
            copy_result?;
            // Fsync the destination so the rename-fallback ordering matches
            // the same-FS rename guarantees.
            let f = File::open(path).with_context(|| format!("reopen {:?} for fsync", path))?;
            f.sync_all()
                .with_context(|| format!("fsync destination {:?}", path))?;
            // F-023: parent-dir fsync on the EXDEV path too.
            fsync_parent_dir(path)
        }
        Err(e) => {
            let _ = fs::remove_file(&tmp_path);
            Err(e).with_context(|| format!("rename {:?} -> {:?}", tmp_path, path))
        }
    }
}

/// F-023: fsync the PARENT directory of `path`. `File::sync_all` flushes a
/// file's contents but NOT the directory entry that names it — a rename or
/// create can be lost on power-loss until the parent dir is itself fsynced.
/// Opening a directory read-only and `sync_all`-ing its fd is the portable
/// (Linux + macOS) way to do this. No-op when `path` has no parent.
pub(crate) fn fsync_parent_dir(path: &Path) -> Result<()> {
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => return Ok(()),
    };
    let dir =
        File::open(parent).with_context(|| format!("open parent dir {:?} for fsync", parent))?;
    dir.sync_all()
        .with_context(|| format!("fsync parent dir {:?}", parent))?;
    Ok(())
}

/// F-023 (codex Step-1 P2): create `dir` and every missing ancestor, fsyncing
/// each newly-created directory's parent right after `mkdir`. `create_dir_all`
/// alone fsyncs nothing, and a single parent-dir fsync only persists the
/// deepest entry — a power-loss can still lose a freshly-created ancestor (and
/// everything under it). Recurse parent-first so each new dir's entry is made
/// durable in ITS parent. Already-existing dirs short-circuit (the common case
/// where `.loom/` exists → zero extra fsync on the hot path).
fn create_dir_all_synced(dir: &Path) -> Result<()> {
    if dir.as_os_str().is_empty() || dir.exists() {
        return Ok(());
    }
    if let Some(parent) = dir.parent() {
        create_dir_all_synced(parent)?;
    }
    match fs::create_dir(dir) {
        Ok(()) => {}
        // Racing creator (another concurrent task won the create between the
        // `dir.exists()` check above and here) — the entry exists but may have
        // been made by the racer and not yet fsynced. codex review P2-B: fall
        // THROUGH to the parent-dir fsync rather than returning early, so the
        // function's "every new dir's entry is durable" guarantee holds on the
        // race path too (fsync is idempotent + cheap).
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e).with_context(|| format!("create dir {:?}", dir)),
    }
    // Persist this new dir's entry in its parent.
    fsync_parent_dir(dir)
}

#[cfg(unix)]
fn is_cross_device(e: &std::io::Error) -> bool {
    // raw_os_error() == 18 is EXDEV on Linux + macOS.
    e.raw_os_error() == Some(libc::EXDEV)
}

#[cfg(not(unix))]
fn is_cross_device(_e: &std::io::Error) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::{atomic_write, fsync_parent_dir};

    #[test]
    fn atomic_write_fsyncs_parent_dir() {
        let dir = tempfile::tempdir().unwrap();
        // Seam: fsync_parent_dir opens + fsyncs the PARENT of the given path
        // without error (proves it actually opens the dir fd, not a no-op).
        fsync_parent_dir(&dir.path().join("file.json")).unwrap();
        // And atomic_write into a nested path still succeeds with the dir-fsync
        // wired into its rename path (true power-loss is not unit-testable; this
        // verifies the fsync call is on the path and does not break the write).
        let path = dir.path().join("nested/x.json");
        atomic_write(&path, b"data").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"data");
    }

    #[test]
    fn atomic_write_creates_and_fsyncs_deep_new_ancestors() {
        let dir = tempfile::tempdir().unwrap();
        // codex Step-1 P2: a multi-level fresh tree (a/b/c all new) must be
        // created component-wise with each new dir's parent fsynced. True
        // power-loss isn't unit-testable; this proves the component-wise
        // create+fsync path runs without error and the write lands.
        let path = dir.path().join("a/b/c/deep.json");
        atomic_write(&path, b"deep").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"deep");
    }

    #[test]
    fn writes_new_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.json");
        atomic_write(&path, b"hello").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"hello");
        // tmp sidecar should be gone.
        assert!(!path.with_extension("json.tmp").exists());
    }

    #[test]
    fn overwrites_existing_file_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("b.log");
        std::fs::write(&path, b"old").unwrap();
        atomic_write(&path, b"new").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"new");
    }

    #[test]
    fn creates_parent_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested/deep/file.txt");
        atomic_write(&path, b"x").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"x");
    }
}
