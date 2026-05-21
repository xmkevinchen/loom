//! Atomic write helper: write to `<path>.tmp` then `rename` over target.
//!
//! Same-FS rename is atomic per POSIX. Cross-FS rename returns `EXDEV`;
//! we fall back to copy + remove + fsync of the destination.
//!
//! Gemini MF1 in plan F-001 Step 6: avoids partial-read corruption for
//! Loom-written artifacts (status.json, dispatch log, worker outputs).

use anyhow::{Context, Result};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;

/// Write `contents` to `path` atomically.
///
/// Strategy: write to `<path>.tmp`, fsync that file, then `rename` over the
/// target. On `EXDEV` (cross-filesystem rename) fall back to copy + delete +
/// destination fsync. The temp file is removed on any error path.
pub fn atomic_write(path: &Path, contents: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create parent dir {:?}", parent))?;
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
        Ok(()) => Ok(()),
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
            Ok(())
        }
        Err(e) => {
            let _ = fs::remove_file(&tmp_path);
            Err(e).with_context(|| format!("rename {:?} -> {:?}", tmp_path, path))
        }
    }
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
    use super::atomic_write;

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
