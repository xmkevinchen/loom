# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — 2026-05-21

First release. Single-machine, single-goal, single worker adapter
(ClaudeCodeAdapter). Designed for the solo developer dogfooding their own Loom
build; multi-user / multi-host scenarios are v0.2+.

### Added

- **Worker trait + `ClaudeCodeAdapter`** (`src/worker.rs`, `src/worker_claude_code.rs`)
  modeled on the `consume_output` idiom from OpenAI Codex CLI
  (`codex-rs/core/src/exec.rs` at commit `0b4f86095c8005d8f74e9c62b971d72c1670aa88`):
  concurrent stdout / stderr drain in independent `tokio::spawn` tasks; bounded
  drain timeout with `handle.abort()`; two-step kill on timeout (process group
  kill + `child.start_kill`) so forked grandchildren do not orphan. See
  `docs/inspiration.md` § OpenAI Codex CLI.
- **6-phase orchestration loop** (`src/iteration.rs`, `src/dispatch.rs`,
  `src/discovery.rs`, `src/delivery.rs`): Discovery → Scheduling → Execution →
  Aggregate+decide → Iteration → Delivery. Single goal, single cycle by
  default; `pause-and-notify` on any feature failure (exit code 4).
- **Four CLI subcommands** (`src/cli.rs`): `run <goal>`, `dispatch <ids>...`,
  `status`, `version`. Exit codes documented as `EXIT_DISPATCH_HAD_FAILURE = 4`,
  `EXIT_WORKSPACE_NOT_INITIALIZED = 5`, `EXIT_GENERIC_ERROR = 1`.
- **Structured tracing** (`src/main.rs::init_tracing`): JSON-line file layer at
  `.loom/run-<UTC-timestamp>.log` plus a human-readable stdout layer. Each run
  log is `jq -c .`-parseable.
- **Atomic file writes** (`src/atomic_write.rs`): tmp-file + `rename(2)`
  pattern; auto-creates parent dirs. Used for `.loom/status.json` and dispatch
  log.
- **PATH-only scrub on spawn** (`src/spawn_env.rs`): the running Loom binary's
  directory is removed from the spawned worker's `PATH` so a worker cannot
  recursively discover `loom` and spawn another Claude Code. `HOME`, `USER`,
  `SHELL` are preserved. Verified end-to-end (AC6).
- **Per-feature git worktree isolation** (`src/dispatch.rs`): each dispatched
  feature runs in its own ephemeral `.loom/worktrees/<feature-id>-<pid>/`
  worktree, cleaned up after dispatch.
- **AE artifact reader** (`src/discovery.rs`, `src/verdict.rs`,
  `src/artifact.rs`): parses `.ae/features/active/<id>/index.md`
  frontmatter (`depends_on:`, status) and `review.md` verdict
  (`pass | fail | pending`). Loom is read-only with respect to AE artifacts.
- **Status snapshot** (`src/state.rs`): writes `.loom/status.json` (schema = 1)
  with cycle / phase / per-feature verdicts, suitable for v0.2+ TUI to read.
- **`docs/inspiration.md`**: 4 substantive reference-project sections
  (ccswarm, cosmix/loom, project-orchestrator, OpenAI Codex CLI), plus naming
  rationale (`loom-rt` on crates.io, public name `Loom`).
- **`docs/v02-growth-path.md`**: explicit out-of-v0.1 list and reversibility
  hooks (PARALLEL-to-CC reopen, language reversibility).
- **MIT license** (`LICENSE`). Picked as the most permissive Rust-tooling
  default. Cargo metadata sets `publish = false` so v0.1 is not pushed to
  crates.io.

### Known limitations

- **End-to-end real-AE smoke gated on AE-plugin-BL #1**: `claude --headless`
  invocation is the missing piece. Two integration tests under
  `tests/e2e/sso_feature_integration_test.rs` carry `#[ignore]` with the
  reason `BLOCKED: AE-plugin-BL #1 (headless invocation) not yet shipped`. The
  stub-AE end-to-end path (`tests/e2e/sso_feature_stub_test.rs`) passes.
- **Single machine, single goal**. Multi-goal concurrency, TUI / Web
  dashboard, multi-host scheduling, cost / quota tracking, additional worker
  adapters (Codex / Gemini / local), and cross-feature F1 / F2 / F3
  aggregation analysis are all v0.2+. See `docs/v02-growth-path.md`.
- **No `Co-Authored-By: Claude` commits**. Loom self-hosts via the AE plugin
  but ships under sole authorship.

### Build artifacts

- Binary: `target/release/loom`, ≈ 3.1 MiB (3,230,512 bytes on arm64 macOS).
- Test suite: **34 passed, 2 ignored** (28 lib + 3 main + 4 spawn_env + 1 e2e
  stub + 3 worker_claude_code; 2 e2e real-AE ignored as documented above).
- Toolchain: Rust stable (1.78+), floating channel pin per
  `rust-toolchain.toml`.

### Methodology

Shipped via 9-step plan F-001 with 8 feature commits (Step 0 init + Steps 1–8;
Step 9 = this entry). Methodology references:

- `.ae/features/active/F-001-build-loom-v0-1-ai-agent-orchestrator-em/discussions/001-loom-design-note/conclusion.md`
- `.ae/features/active/F-001-build-loom-v0-1-ai-agent-orchestrator-em/discussions/002-parallel-vs-augment/conclusion.md`
- `.ae/features/active/F-001-build-loom-v0-1-ai-agent-orchestrator-em/discussions/003-implementation-language/conclusion.md`

[0.1.0]: #010--2026-05-21
