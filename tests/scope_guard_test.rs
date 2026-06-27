//! F-023 AC9 scope guard (machine-enforced). The (B)-SEQUENCE decision ships
//! the run-journal spine ONLY: it records `worker_exit_status` as an opaque
//! string and MUST NOT contain F-020's panic-recovery cluster. This test greps
//! `src/` and FAILS if any forbidden F-020 symbol appears, so the scope guard
//! is a red CI test rather than a human-only promise.
//!
//! Allowed (pre-existing, NOT F-020 rescue work): the outer `JoinError` arm in
//! dispatch.rs already maps a tokio task panic to `worker_exit_status:"panic"`,
//! and main.rs matches that string. The guard targets the SPECIFIC additions
//! F-020 would make — a `WorkerRunEnd` enum, a `"panic"` element inside
//! `RESCUE_STATUSES`, and an inner `tokio::spawn` of `worker.run()` — not the
//! word "panic" wholesale.

use std::path::{Path, PathBuf};

fn src_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src")
}

/// codex Step-5 P2-3: walk `src/` RECURSIVELY so the guard still covers symbols
/// if `src/` ever gains subdirectories (e.g. `src/journal/`).
fn read_all_src() -> String {
    let mut combined = String::new();
    collect_rs(&src_dir(), &mut combined);
    combined
}

fn collect_rs(dir: &Path, out: &mut String) {
    for entry in std::fs::read_dir(dir).expect("read src dir").flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push_str(&std::fs::read_to_string(&path).expect("read src file"));
            out.push('\n');
        }
    }
}

#[test]
fn scope_guard_no_f020_panic_cluster() {
    let all_src = read_all_src();
    let dispatch =
        std::fs::read_to_string(src_dir().join("dispatch.rs")).expect("read dispatch.rs");

    // 1. F-020's per-run status enum must not exist.
    assert!(
        !all_src.contains("WorkerRunEnd"),
        "F-023 scope guard (AC9): `WorkerRunEnd` is F-020 work and must not appear"
    );

    // 2. RESCUE_STATUSES must not gain a "panic" element. Check the const
    //    definition's array literal specifically — the pre-existing outer-arm
    //    `worker_exit_status:\"panic\"` is allowed and is NOT in this array.
    let rescue_literal = rescue_statuses_literal(&dispatch);
    assert!(
        !rescue_literal.contains("\"panic\""),
        "F-023 scope guard (AC9): RESCUE_STATUSES must not include \"panic\" (F-020 work). Got: {rescue_literal}"
    );

    // 3. `worker.run()` must not be wrapped in an inner tokio spawn (F-020's
    //    panic-isolation mechanism). codex Step-5 P2-2: instead of a brittle
    //    char-proximity heuristic, scope the check to `run_one_feature`'s own
    //    body — the worker call lives there and is directly `.await`ed; the
    //    legitimate dispatch spawn lives in `run_dispatch_loop` (a different
    //    function, excluded). A new inner spawn-of-worker.run would necessarily
    //    add a `spawn(` INSIDE run_one_feature.
    let body = run_one_feature_body(&dispatch);
    assert!(
        body.contains("worker.run"),
        "scope guard self-check: worker.run must be inside run_one_feature"
    );
    assert!(
        !body.contains("spawn("),
        "F-023 scope guard (AC9): run_one_feature must not contain a `spawn(` — \
         `worker.run()` is called directly, never wrapped in an inner tokio::spawn (F-020)"
    );
}

/// Extract the `RESCUE_STATUSES` const's array literal (from `=` to the closing
/// `]`). Panics if the const is missing — its absence is itself a scope/refactor
/// signal worth a red test.
fn rescue_statuses_literal(dispatch: &str) -> &str {
    let start = dispatch
        .find("RESCUE_STATUSES")
        .expect("RESCUE_STATUSES const must exist");
    let eq = dispatch[start..]
        .find('=')
        .map(|i| start + i)
        .expect("RESCUE_STATUSES `=`");
    let close = dispatch[eq..]
        .find(']')
        .map(|i| eq + i + 1)
        .expect("RESCUE_STATUSES closing `]`");
    &dispatch[eq..close]
}

/// The text of `run_one_feature`'s definition: from `async fn run_one_feature`
/// up to the next top-level `fn` / `async fn` (its trailing region may include
/// the following item's doc comment — harmless for the `spawn(` / `worker.run`
/// checks). Panics if the function is missing — its disappearance is itself a
/// refactor signal worth a red test.
fn run_one_feature_body(dispatch: &str) -> &str {
    let start = dispatch
        .find("async fn run_one_feature")
        .expect("run_one_feature must exist");
    let rest = &dispatch[start + 1..];
    let next_fn = ["\nfn ", "\nasync fn ", "\npub fn ", "\npub async fn "]
        .iter()
        .filter_map(|m| rest.find(m))
        .min()
        .map(|i| start + 1 + i)
        .unwrap_or(dispatch.len());
    &dispatch[start..next_fn]
}
