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
//!
//! Code 5 takes precedence over code 4 when both occur in the same run: an
//! AE review verdict is the more specific operator-facing signal, and the
//! operator must address the verdict before retrying. Codes above 0 are
//! stable for shell-script consumption; new codes may be appended in later
//! minor versions but existing meanings will not shift.

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

    #[test]
    fn exit_code_constants_are_stable() {
        assert_eq!(EXIT_GENERIC_ERROR, 1);
        assert_eq!(EXIT_WORKSPACE_NOT_INITIALIZED, 3);
        assert_eq!(EXIT_DISPATCH_HAD_FAILURE, 4);
        assert_eq!(EXIT_AE_REVIEW_REJECTED, 5);
    }

    #[test]
    fn help_renders() {
        // Smoke: clap can render long help for every subcommand without panicking.
        Cli::command().debug_assert();
    }
}
