//! `loom` command-line surface.
//!
//! Centralizes clap derive definitions so `main.rs` stays a thin dispatch
//! shell. Every subcommand documented here has a paired handler in
//! `main.rs`.
//!
//! # Exit codes
//!
//! | Code | Meaning                                                        |
//! |------|----------------------------------------------------------------|
//! | 0    | Success.                                                       |
//! | 1    | Generic error (anyhow propagation default).                    |
//! | 2    | Invalid CLI arguments (clap default; not raised by us).        |
//! | 3    | Workspace not initialized (`.ae/features/active/` missing).    |
//! | 4    | Dispatch completed but at least one feature failed.            |
//! | 5    | AE review (review.md) returned `verdict: fail`.                |
//! | 6    | Worker subprocess detected `LOOM_PARENT_PID` env var and refused to recurse. |
//! | 7    | Deps-stuck: work remains but nothing dispatchable (F-013).     |
//! | 8    | Review missing: clean worker, no readable review.md verdict (F-014 wires detection). |
//! | 130  | Cancelled by SIGINT (operator interrupt); ranks below 4/5.     |
//!
//! Full precedence (highest first): 5 > 4 > 130 > 7 > 8 > 0. Codes 7 and 8
//! are the weakest non-zero signals — an "incomplete run" never masks a
//! cancel or a substantive failure; they exist so an incomplete run is never
//! reported as success (exit 0) to CI.
//!
//! Code 130 (operator cancel) ranks *below* codes 4 and 5: a substantive
//! dispatch failure or AE-review rejection outranks an operator Ctrl-C, so a
//! real failure is never masked by a cancel signal. Both `loom run` and
//! `loom dispatch` decide their exit through `main.rs::decide_exit`.
//!
//! Code 5 takes precedence over code 4 when both occur in the same run: an
//! AE review verdict is the more specific operator-facing signal, and the
//! operator must address the verdict before retrying. Code 6 is raised
//! before any dispatch work happens — only the `Run` and `Dispatch`
//! subcommand arms apply the guard, so `status` / `version` / `--help`
//! remain available inside worker subprocesses for diagnostics. Codes
//! above 0 are stable for shell-script consumption; new codes may be
//! appended in later minor versions but existing meanings will not shift.

use clap::{Parser, Subcommand};

/// Generic anyhow-style failure.
pub const EXIT_GENERIC_ERROR: i32 = 1;
/// `.ae/features/active/` not found in the current workspace.
pub const EXIT_WORKSPACE_NOT_INITIALIZED: i32 = 3;
/// Dispatch completed but at least one feature reported `fail`/`error`/`timeout`.
pub const EXIT_DISPATCH_HAD_FAILURE: i32 = 4;
/// AE review wrote `verdict: fail` to a feature's `review.md` (Phase 4).
///
/// Distinct from `EXIT_DISPATCH_HAD_FAILURE` (= 4) which signals a
/// worker-execution failure. Takes precedence over code 4 when both occur
/// in the same run — see the exit-code table in this module's doc comment.
pub const EXIT_AE_REVIEW_REJECTED: i32 = 5;
/// Worker subprocess detected `LOOM_PARENT_PID` and refused to recurse.
///
/// F-003 Step 2 (M3 defense-in-depth alongside the PATH-scrub in Step 1):
/// the parent injects `LOOM_PARENT_PID=<pid>` before spawning a worker;
/// any child that re-enters `loom run` / `loom dispatch` sees the env var
/// set and exits with this code rather than recursively spawning workers.
/// `status` / `version` / `--help` are explicitly excluded — they remain
/// available inside workers so diagnostic flows aren't blocked.
pub const EXIT_RECURSION_DETECTED: i32 = 6;
/// A run was cancelled by SIGINT (operator Ctrl-C) and nothing more
/// actionable failed.
///
/// `128 + SIGINT(2)` per the POSIX signal-exit convention. Ranks *below*
/// `EXIT_DISPATCH_HAD_FAILURE` (4) and `EXIT_AE_REVIEW_REJECTED` (5): a
/// substantive worker/review failure outranks an operator cancel, so a real
/// failure is never hidden behind a 130. Decided centrally in
/// `main.rs::decide_exit`, which both `loom run` and `loom dispatch` route
/// through so cancellation is signalled regardless of any per-worker verdict.
pub const EXIT_CANCELLED: i32 = 130;
/// A run ended with work remaining but nothing dispatchable — the dependency
/// graph gates every pending feature (F-013).
///
/// Weakest non-zero signal: ranks below `EXIT_CANCELLED` (130) — and therefore
/// below 4 and 5 — because an operator cancel or a substantive failure is
/// always the more actionable outcome. Appended per this module's append-only
/// contract; no existing code's meaning shifts. Wins over
/// `EXIT_REVIEW_MISSING` (8) when both occur in one run: a stuck DAG is the
/// root cause that explains absent reviews downstream, not vice versa.
pub const EXIT_DEPS_STUCK: i32 = 7;
/// A worker exited cleanly but produced no readable `review.md` verdict —
/// the expected AE-review artifact is absent (F-013 reserves the constant
/// and the `decide_exit` branch; F-014 wires the detection).
///
/// Ranks below `EXIT_DEPS_STUCK` (7) — see that constant's combined-case
/// rationale — and above only success (0).
pub const EXIT_REVIEW_MISSING: i32 = 8;

#[derive(Parser, Debug)]
#[command(
    name = "loom",
    version,
    about = "AE meta-harness orchestrator",
    long_about = "Loom is the ecosystem-agnostic meta-harness for Agentic Engineering \
                  (AE) workflows. It spawns headless agent workers across an AE \
                  feature DAG and drives the 6-phase orchestration loop \
                  (discover → dispatch → execute → verdict → iterate → deliver)."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the 6-phase loop against a goal.
    #[command(long_about = "Run the 6-phase orchestration loop on a goal. Phase 1 \
                      discovers features via ae:backlog + ae:analyze, then \
                      Phases 2-5 dispatch the resulting DAG through the worker \
                      pool, and Phase 6 writes the dispatch log.")]
    Run {
        /// The natural-language goal handed to `ae:backlog` + `ae:analyze`.
        goal: String,
    },

    /// Dispatch specific feature IDs (skip Discovery).
    #[command(
        long_about = "Dispatch one or more specific features through the worker \
                      pool, skipping Phase 1 Discovery. Reads existing features \
                      from .ae/features/active/ and filters to the named IDs. \
                      Useful for ad-hoc re-dispatch without re-running \
                      ae:backlog / ae:analyze."
    )]
    Dispatch {
        /// Feature IDs to dispatch (e.g. `F-001 F-002`).
        #[arg(required = true, value_name = "FEATURE_ID")]
        ids: Vec<String>,
    },

    /// Print Loom run state and recent log files.
    #[command(
        long_about = "Print Loom run state from .loom/status.json and a summary \
                      of recent .loom/run-*.log files. Read-only: never mutates \
                      on-disk state. Exits 0 with an empty-state message when \
                      .loom/status.json is missing."
    )]
    Status,

    /// Print loom-rt version.
    #[command(
        long_about = "Print the loom-rt version string. Equivalent to `--version` \
                      but exposed as an explicit subcommand for scripts that \
                      prefer subcommand-shaped invocations."
    )]
    Version,

    /// Delete stale `loom-rescue/*` refs older than a cutoff.
    #[command(
        name = "gc-refs",
        long_about = "Age-delete survival-only rescue refs (refs/heads/loom-rescue/*) \
                      whose newest reflog entry is older than --max-age-days. Scoped \
                      strictly to the loom-rescue/ namespace; merge candidates \
                      (loom-features/*) are never touched. Deletes are CAS-guarded \
                      against a concurrent writer. Use --dry-run to list candidates \
                      without deleting. Does NOT run automatically on `loom run`."
    )]
    GcRefs {
        /// Delete refs whose last write is older than this many days (minimum 1).
        #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..))]
        max_age_days: u64,
        /// List the refs that would be deleted without deleting anything.
        #[arg(long)]
        dry_run: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn parses_run_subcommand() {
        let cli = Cli::try_parse_from(["loom", "run", "ship login"]).unwrap();
        match cli.command {
            Some(Command::Run { goal }) => assert_eq!(goal, "ship login"),
            other => panic!("expected Run, got {other:?}"),
        }
    }

    #[test]
    fn parses_dispatch_subcommand_multi() {
        let cli = Cli::try_parse_from(["loom", "dispatch", "F-001", "F-002"]).unwrap();
        match cli.command {
            Some(Command::Dispatch { ids }) => assert_eq!(ids, vec!["F-001", "F-002"]),
            other => panic!("expected Dispatch, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_requires_at_least_one_id() {
        let err = Cli::try_parse_from(["loom", "dispatch"]).unwrap_err();
        // clap returns kind=MissingRequiredArgument when `required = true`
        // is violated.
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn parses_status_and_version() {
        let s = Cli::try_parse_from(["loom", "status"]).unwrap();
        assert!(matches!(s.command, Some(Command::Status)));
        let v = Cli::try_parse_from(["loom", "version"]).unwrap();
        assert!(matches!(v.command, Some(Command::Version)));
    }

    #[test]
    fn no_subcommand_is_allowed() {
        let cli = Cli::try_parse_from(["loom"]).unwrap();
        assert!(cli.command.is_none());
    }

    // F-021 AC4: `loom gc-refs` parsing — defaults, flags, zero-age rejection.
    #[test]
    fn parses_gc_refs_defaults() {
        let cli = Cli::try_parse_from(["loom", "gc-refs"]).unwrap();
        match cli.command {
            Some(Command::GcRefs {
                max_age_days,
                dry_run,
            }) => {
                assert_eq!(max_age_days, 30, "default max-age-days is 30");
                assert!(!dry_run, "default is a live run");
            }
            other => panic!("expected GcRefs, got {other:?}"),
        }
    }

    #[test]
    fn parses_gc_refs_with_args() {
        let cli = Cli::try_parse_from(["loom", "gc-refs", "--max-age-days", "30"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::GcRefs {
                max_age_days: 30,
                dry_run: false
            })
        ));
        let cli = Cli::try_parse_from(["loom", "gc-refs", "--dry-run"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::GcRefs { dry_run: true, .. })
        ));
    }

    #[test]
    fn gc_refs_rejects_zero_max_age() {
        let err = Cli::try_parse_from(["loom", "gc-refs", "--max-age-days", "0"]).unwrap_err();
        assert_eq!(
            err.kind(),
            clap::error::ErrorKind::ValueValidation,
            "max-age-days 0 must fail the range(1..) validator"
        );
    }

    #[test]
    fn exit_code_constants_are_stable() {
        assert_eq!(EXIT_GENERIC_ERROR, 1);
        assert_eq!(EXIT_WORKSPACE_NOT_INITIALIZED, 3);
        assert_eq!(EXIT_DISPATCH_HAD_FAILURE, 4);
        assert_eq!(EXIT_AE_REVIEW_REJECTED, 5);
        assert_eq!(EXIT_RECURSION_DETECTED, 6);
        assert_eq!(EXIT_CANCELLED, 130);
    }

    /// F-013: the incomplete-run codes are append-only additions; their values
    /// are part of the documented shell-script contract.
    #[test]
    fn incomplete_run_exit_codes_are_stable() {
        assert_eq!(EXIT_DEPS_STUCK, 7);
        assert_eq!(EXIT_REVIEW_MISSING, 8);
    }

    #[test]
    fn help_renders() {
        // Smoke: clap can render long help for every subcommand without panicking.
        Cli::command().debug_assert();
    }
}
