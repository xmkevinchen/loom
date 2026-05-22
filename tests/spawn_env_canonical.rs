//! F-003 AC1 + AC2 — per-segment canonical PATH-scrub behavior tests.
//!
//! These tests exercise `apply_scrubbed_path` against two fixtures that the
//! pre-F-003 substring-match algorithm could not get right:
//!
//! - **Test 1 (AC1 — cargo-install shape)**: `loom` co-located with another
//!   tool in one dir, plus a separate unrelated dir on PATH. Per-segment
//!   probe strips the loom dir while preserving the unrelated dir. The
//!   in-dir co-located tool becoming unreachable is documented behavior
//!   (the "mathematical inherence" tradeoff from discussion 001) — the
//!   README directs users to `cargo install --root ~/.loom/` to dodge the
//!   UX cost in production. Test asserts the behavior, not a workaround.
//! - **Test 2 (AC2 — symlink aliasing)**: a symlink dir aliases the
//!   canonical loom dir. The substring match treated alias and canonical
//!   as different strings; the per-segment probe canonicalizes through the
//!   symlink and strips both. A third unrelated dir survives.
//!
//! ## Env serialization
//!
//! Both tests mutate process-wide `PATH` because `apply_scrubbed_path`
//! reads from the current env. Cargo runs integration tests in parallel
//! threads within a single binary, so an unguarded mutation would race
//! against the sibling test. A static `Mutex` serializes the
//! "set PATH → scrub → restore PATH" critical section. The A3 spawn step
//! in Test 1 runs AFTER the lock is released — clippy's
//! `await-holding-lock` lint plus general deadlock-safety arguments both
//! favor releasing the env lock before any subprocess wait. We use
//! `std::process::Command` for that spawn so the tests remain `#[test]`
//! (sync) and don't drag the tokio runtime in for a one-shot wait.

use loom_rt::spawn_env::apply_scrubbed_path;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Mutex;
use tokio::process::Command;

static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Write a minimal executable shell script at `<dir>/<name>`. Marks +x on
/// Unix so `command -v` accepts it. Returns the file path.
fn write_stub(dir: &Path, name: &str) -> PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, b"#!/bin/sh\nexit 0\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    p
}

/// Hold `ENV_LOCK`, swap `PATH` to `path_value`, run `work`, restore the
/// original `PATH`, release the lock. The closure runs inside the locked
/// section; its return value is propagated out for use AFTER the lock
/// drops — keeping any subprocess wait (Test 1's A3) outside the critical
/// section.
fn with_path_env<R>(path_value: &str, work: impl FnOnce() -> R) -> R {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let saved = std::env::var("PATH").ok();
    // SAFETY: ENV_LOCK serializes the sibling integration tests against
    // this PATH mutation; no concurrent writer races us in this binary.
    unsafe {
        std::env::set_var("PATH", path_value);
    }
    let result = work();
    unsafe {
        if let Some(v) = saved {
            std::env::set_var("PATH", v);
        } else {
            std::env::remove_var("PATH");
        }
    }
    result
}

#[test]
fn test_1_cargo_install_shape() {
    // <tmp>/cargo-bin-like/{loom, other-tool}: simulates `cargo install loom-rt`
    // landing in `~/.cargo/bin/` alongside other cargo-installed tools.
    let cargo_bin_like = tempfile::tempdir().unwrap();
    let loom_bin = write_stub(cargo_bin_like.path(), "loom");
    write_stub(cargo_bin_like.path(), "other-tool");

    // <tmp>/other-tools/{git-stub}: simulates an unrelated PATH segment
    // that must survive the scrub (the differentiator vs substring match).
    let other_tools = tempfile::tempdir().unwrap();
    write_stub(other_tools.path(), "git-stub");

    let canonical_cargo_bin = cargo_bin_like.path().canonicalize().unwrap();
    let canonical_other_tools = other_tools.path().canonicalize().unwrap();

    let composed = format!(
        "{}:{}",
        canonical_cargo_bin.display(),
        canonical_other_tools.display(),
    );

    let filtered = with_path_env(&composed, || {
        let mut dummy = Command::new("/bin/true");
        apply_scrubbed_path(&mut dummy, &loom_bin)
    });

    // A1: cargo-bin-like dir stripped (the dir holding our loom).
    assert!(
        !filtered.contains(canonical_cargo_bin.to_str().unwrap()),
        "filtered PATH must not contain loom's cargo-install-like dir; got {filtered:?}"
    );

    // A2: unrelated dir survives — the real point of the per-segment probe.
    // (Pre-F-003 substring match would have over-stripped any segment whose
    // textual prefix matched the loom dir. The probe compares canonical
    // targets, not strings, so unrelated dirs are preserved by construction.)
    assert!(
        filtered.contains(canonical_other_tools.to_str().unwrap()),
        "unrelated dir must survive the per-segment scrub; got {filtered:?}"
    );

    // A3: in-dir co-located tool stripping is documented behavior, not an
    // error. Spawning `/bin/sh -c "command -v other-tool"` does NOT find
    // `other-tool` because its directory `<tmp>/cargo-bin-like` is stripped.
    // This is the accepted "mathematical inherence" tradeoff from the
    // discussion conclusion — not a bug. The README guidance directs users
    // to `cargo install --root ~/.loom/` to avoid this UX cost in
    // production; the test is verifying the documented behavior, not a
    // regression.
    let out = std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg("command -v other-tool; echo exit=$?")
        .env("PATH", &filtered)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn /bin/sh for A3");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("exit=1") || stdout.contains("exit=127"),
        "`command -v other-tool` should fail under scrubbed PATH \
         (co-located tool documented as unreachable); got stdout={stdout:?}"
    );
}

#[test]
fn test_2_symlink_aliasing() {
    // <tmp>/canonical/loom (real); <tmp>/alias -> <tmp>/canonical (dir symlink).
    // PATH carries both segments so the substring match (pre-F-003) would
    // strip canonical but miss alias. The per-segment probe canonicalizes
    // through the symlink and strips both.
    let canonical = tempfile::tempdir().unwrap();
    let loom_bin = write_stub(canonical.path(), "loom");

    let alias_parent = tempfile::tempdir().unwrap();
    let alias_dir = alias_parent.path().join("alias");
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(canonical.path(), &alias_dir)
            .expect("create dir symlink alias -> canonical");
    }
    #[cfg(not(unix))]
    {
        // Symlink semantics differ on Windows (BL-008 territory); skip the
        // body — the assertion below relies on Unix symlink resolution.
        eprintln!("test_2_symlink_aliasing: non-Unix platform — skipping");
        return;
    }

    let safe = tempfile::tempdir().unwrap();
    write_stub(safe.path(), "safe-tool");

    // PATH segments as RAW (uncanonicalized) paths in this order:
    // canonical, alias, safe. `apply_scrubbed_path` retains the raw
    // PathBuf for kept segments (it only canonicalizes during the probe,
    // never overwrites the stored path), so assertions below check
    // against the raw input strings, not their canonical form.
    let raw_canonical = canonical.path().to_path_buf();
    let raw_alias = alias_dir.clone();
    let raw_safe = safe.path().to_path_buf();
    let composed = format!(
        "{}:{}:{}",
        raw_canonical.display(),
        raw_alias.display(),
        raw_safe.display(),
    );

    let filtered = with_path_env(&composed, || {
        let mut dummy = Command::new("/bin/true");
        apply_scrubbed_path(&mut dummy, &loom_bin)
    });

    assert!(
        !filtered.contains(raw_canonical.to_str().unwrap()),
        "canonical dir must be stripped; got {filtered:?}"
    );
    assert!(
        !filtered.contains(raw_alias.to_str().unwrap()),
        "alias dir must be stripped via canonicalize-through-symlink; got {filtered:?}"
    );
    assert!(
        filtered.contains(raw_safe.to_str().unwrap()),
        "safe dir must survive; got {filtered:?}"
    );
}
