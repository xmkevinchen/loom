//! e2e SIGINT harness (F-012): spawns the real `loom` binary, delivers a real
//! mid-flight SIGINT, and asserts the causal cancellation chain — exit code
//! 130 PLUS a parsed dispatch-log outcome with `worker_exit_status ==
//! "cancelled"` (the only record reachable solely via the worker's `select!`
//! cancel arm, dispatch.rs). Zero production changes; all machinery lives in
//! this test target.
//!
//! Design source: `.ae/features/active/F-012-*/discussions/001-sigint-e2e-scope/`
//! (conclusion.md Decisions 1-5 + constraint ledger C1-C16).
//!
//! Fallback note (designated, do not improvise): if readiness polling proves
//! flaky on slow CI, swap to the pipe handshake — test creates `pipe()`,
//! passes the write-fd number via a `LOOM_TEST_HANDSHAKE_FD` env var
//! (`Command::env`), the stub writes 1 byte to that fd instead of marker
//! files, and the test blocks on `read()`.
#![cfg(unix)]
