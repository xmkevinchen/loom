# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Worker commit propagation** (F-004, BL-014). After a worker exits
  with `WorkerVerdict::Pass` AND its worktree's HEAD changed from the
  initial SHA captured at `git worktree add` time (regardless of
  ancestry relationship — rebase, cherry-pick, and cross-branch
  rewrites are first-class supported propagation paths), Loom writes a
  `refs/heads/loom-features/F-NNN` ref pointing at the worker's HEAD
  before `git worktree remove --force` runs in cleanup. Closes the
  F-SMOKE-observed dangling-commit failure mode (commits previously
  unreachable after worktree cleanup, eventually reaped by `git gc`).
  Re-dispatches silently overwrite the ref by design; `--create-reflog`
  keeps the prior SHA recoverable via `git reflog show
  loom-features/F-NNN` for the window set by `gc.reflogExpire` (default
  90 days). The propagation function is best-effort and warn-and-continue
  — six guards (HEAD-capture, zero-commit, semantic-verify,
  shallow-clone, overwrite-detect, ref-write) skip or log on their
  applicable failure modes (HEAD-capture / semantic-verify / ref-write
  warn on failure; zero-commit logs at debug for the expected-skip case;
  shallow-clone proceeds best-effort when the check itself errors;
  overwrite-detect is silent unless a prior SHA differs) and never fail
  the feature outcome. Scope: single Loom binary's working
  tree; cross-host synchronization, auto-merge into the default branch,
  and `loom merge F-NNN`/`--on-collision` UX are explicitly deferred.
  (`src/dispatch.rs`, `tests/e2e/worktree_propagation_test.rs`.)

## [0.0.3] — 2026-05-22

Patch release: fixes the broken `claude` spawn shape that shipped in
v0.0.1 + v0.0.2 plus pre-publish housekeeping. End-to-end self-host
dispatch is now verified (F-SMOKE smoke); Discovery phase spawn is
fixed but not yet smoke-tested end-to-end.

### Fixed

- **Worker + Discovery spawn shape**: F-001 wrote both `default_worker` and
  `discovery::maybe_invoke_ae` to spawn `claude` with a `--headless` flag
  that does not exist in the actual Claude Code CLI. Spawn would fail
  immediately; F-001's stub-AE tests didn't exercise the real-spawn paths,
  so the bug shipped unobserved into v0.0.1 + v0.0.2. Verified against
  `claude --help` on 2026-05-22: real non-interactive shape is
  `claude -p "<prompt>" --permission-mode bypassPermissions`, with skills
  triggered by `/skill-name` inside the prompt. Both spawn sites updated.
  (`src/main.rs`, `src/worker_claude_code.rs`, `src/discovery.rs`.)
- **Worker cwd**: `ClaudeCodeAdapter::run` never called
  `Command::current_dir`, so the spawned child inherited Loom's own cwd —
  an `ae:work` skill in that child would scan Loom's own
  `.ae/features/active/` instead of the dispatched feature's worktree, a
  self-recursion bug. Added `cmd.current_dir(&spec.feature_dir)`.
  (`src/worker_claude_code.rs`.)
- **AE-plugin-BL #1 references removed**: README + Discovery comments +
  `loom run` println previously claimed real-AE dispatch was "gated on
  AE-plugin-BL #1 (headless invocation protocol)". On 2026-05-22 we
  verified that BL was never filed upstream in the AE plugin repo (zero
  hits for `headless` / `Loom` across `agentic-engineering/.ae/backlog/`)
  and that Claude Code CLI already supports the `-p` shape; the gate was
  fictional. References cleaned up.
- **`tests/e2e/sso_feature_integration_test.rs` removed**: two
  `#[ignore]`'d placeholder tests gating on the same fictional BL #1.
  End-to-end self-host coverage is subsumed by F-SMOKE smoke
  (manual, 2026-05-22, see commit dc8ed2f).

### Verified

- **End-to-end self-host dispatch** (F-SMOKE smoke, 2026-05-22): a stub
  feature dispatched through Loom → `claude -p` → `/ae:work` →
  `/ae:review` → `review.md` written with `verdict: pass` → feature
  archived to `features/done/`. Roundtrip took 5m25s; no operator
  interaction. Discovery phase (`/ae:backlog` + `/ae:analyze` spawn) is
  not yet smoke-tested end-to-end.

### Housekeeping

- `.fastembed_cache/` (~87MB Mengdie embedding cache) added to `.gitignore`.
- `Cargo.toml` `repository` field added pointing at
  `https://github.com/xmkevinchen/loom`.
- One absolute home-dir path leak in
  `tests/e2e/sso_feature_integration_test.rs` removed before the file
  itself was deleted as obsolete.

### Testing & CI

Added 2026-05-22 post-tag — the v0.0.3 tag was moved from the original
release commit forward to the CI-green commit so `git checkout v0.0.3`
ships a CI-verified tree. The release commit history is preserved in
git; only the tag pointer moved.

- **GitHub Actions CI workflow** (`.github/workflows/ci.yml`) running
  `cargo build` / `cargo test` / `cargo clippy -- -D warnings` /
  `cargo fmt --check` on every push to `main` and PR. Matrix:
  `macos-latest` (Apple Silicon, arm64) + `ubuntu-latest` (x86_64).
  Windows intentionally excluded — Loom v0.0.x is Unix-only by design;
  tracked under BL-008 / BL-012. Concurrency group cancels in-progress
  runs on fast-follow pushes.
- **F-002 saturation test fix**: pre-create `F-SAT-POST/` dir to avoid
  Linux inotify recursive-add_watch race. The notify crate emulates
  recursive watching on Linux by add_watch'ing newly mkdir'd sub-dirs
  asynchronously — a mkdir-then-immediate-write sequence could win the
  race and drop the write event. The original F-002 fixture modeled
  that race; production never does (feature dirs already exist when
  review.md is written). Pre-creating `F-SAT-POST/` aligns the test
  with production semantics. macOS FSEvents is OS-level recursive and
  was not subject to this race.
- **Worker timeout test fix**: `/bin/sh` → `/bin/bash` for `exec -a`
  portability. Ubuntu's `/bin/sh` is dash (which doesn't support
  `exec -a` — a bashism); macOS `/bin/sh` is bash. The earlier
  fixture's comment misdiagnosed this as "macOS bash". Both runners
  ship `/bin/bash` by default.

Both test fixes are test-fixture-only — production code
(`verdict::watch_verdicts` and `ClaudeCodeAdapter`) is unchanged. CI was
the first surface to exercise Linux behavior; both bugs shipped silently
through F-001 / F-002 because no CI ran before this release.

## [0.0.2] — 2026-05-22

Distribution-readiness milestone. F-002 (verdict listener wired into the
iteration loop — the 6-phase loop now iterates past one cycle) + F-003
(per-segment canonical PATH-scrub + LOOM_PARENT_PID env-var recursion
guard — `cargo install loom-rt` stops being a footgun) ship together.

> **Note (post-tag clarification)**: this section originally claimed
> "Still gated on AE-plugin-BL #1 (`claude --headless` protocol)". After
> tagging we discovered that BL was never filed upstream and the
> `--headless` flag does not exist (real shape is `claude -p "<prompt>"`).
> Both Worker and Discovery spawn sites have been corrected on `main`;
> v0.0.2 binaries built from the d0db02e tag carry the broken spawn shape
> and will fail at the first real-claude invocation. Use v0.0.3 instead —
> see [0.0.3] § Fixed for details.

### Added

- **Per-segment canonical PATH-scrub** replacing the v0.0.1 substring match
  (F-003, BL-007). Each `PATH` segment is now compared to the running loom
  binary via `segment/loom.canonicalize()`; only segments whose `loom`
  resolves to OUR canonical target are stripped. Closes the symlink-aliasing
  recursion hole (the substring match treated `~/bin/loom -> ~/.cargo/bin/loom`
  as a different dir) and makes `cargo install loom-rt` safe to use in a
  shared `~/.cargo/bin/` because unrelated tools in the same dir survive
  beyond the per-segment match itself — see README "Install isolation" for
  the dedicated-root recommendation. (`src/spawn_env.rs`,
  `src/worker_claude_code.rs`, `src/main.rs`, `tests/spawn_env_test.rs`,
  `tests/spawn_env_canonical.rs`.)
- **`LOOM_PARENT_PID` worker-side recursion guard** (F-003). Parent injects
  `LOOM_PARENT_PID=<pid>` on every worker spawn (`src/worker_claude_code.rs`);
  child processes that re-enter `loom run` or `loom dispatch` see the env
  var set and exit `EXIT_RECURSION_DETECTED = 6` before doing any dispatch
  work. `loom status` / `loom version` / no-subcommand `--help` remain
  available inside workers so diagnostic flows are not blocked. Defense-in-depth
  partner to the PATH-scrub above. (`src/cli.rs`, `src/main.rs`,
  `tests/recursion_guard_test.rs`.)
- **PATH iteration via `std::env::split_paths` / `std::env::join_paths`**
  (F-003, partial BL-008). Cross-platform PATH semantics — the v0.0.1
  hand-split-on-`:` form is gone. Windows full support is still tracked in
  BL-008 (depends on rendering symlink-equivalent semantics for `.exe` +
  `;`-separated PATH).
- **`verdict::watch_verdicts` wired into the iteration loop** (F-002, BL-002).
  Closes Loom's 6-phase loop — the iteration controller now reacts to
  `review.md` terminal verdicts via a two-tier path: a notify watcher
  catches events with millisecond latency, and a per-cycle disk scan acts
  as the authoritative source so dropped events (channel saturation, CI
  notify backend flakiness) are recovered on the next cycle. Restart
  idempotency comes from a pre-populate scan at loop entry. (`src/iteration.rs`,
  `src/verdict.rs`, `src/main.rs`.)
- **`EXIT_AE_REVIEW_REJECTED = 5`** distinct exit code returned when any
  feature's `review.md` carries `verdict: fail`. Takes precedence over
  `EXIT_DISPATCH_HAD_FAILURE = 4` (worker-execution failure) when both
  occur — the AE review verdict is the more specific operator-facing
  signal. (`src/cli.rs`.)
- **Verdict channel widened 64 → 256** to absorb startup burst from notify
  replaying events on watcher registration (observed on macOS FSEvents),
  with explicit `try_send` saturation handling that warn-logs the dropped
  feature_id instead of stalling the watcher std::thread. Saturation
  drops are recovered by the per-cycle scan on the next cycle, making
  channel-loss semantics best-effort rather than data-loss. (`src/verdict.rs`.)
- **e2e multi-cycle test** at `tests/e2e/verdict_multi_cycle_test.rs` —
  direct library call validates 3-feature DAG convergence, subprocess
  `loom run` validates all 6 phase markers land in `.loom/run-*.log`.

### Changed

- `run_iteration_loop` signature returns `Result<(Vec<DispatchReport>, bool)>`
  where the trailing `bool` is `ae_review_failed`. The lone caller
  (`src/main.rs::run_command`) maps `true` → `EXIT_AE_REVIEW_REJECTED`.

[0.0.2]: #002--2026-05-22

## [0.0.1] — 2026-05-21

Scaffolding milestone. Not a "feature-complete v0.1" — the plan was named
"Build Loom v0.1" at design time, but the shipped capability is closer to
pre-alpha v0.0.x: harness shape + Codex CLI subprocess idiom moved over,
CLI surface scaffolded, but end-to-end orchestration is gated on
AE-plugin-BL #1 (real `claude --headless` protocol) AND on BL-002 (wiring
the verdict listener so the loop actually iterates past one cycle). What
ships at v0.0.1 is the **harness foundation**, not a usable multi-feature
orchestrator. v0.0.x will continue until BL-002 + BL-007 land + AE-BL #1
ships; v0.1.0 is the first release where the 6-phase loop actually
iterates against real AE.

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
  > **✓ Resolved 2026-05-22**: the "BL #1" reference was based on incorrect
  > F-001 assumptions; the actual Claude Code CLI already supports `-p` and
  > end-to-end dispatch works (verified via F-SMOKE smoke). The
  > `sso_feature_integration_test.rs` file has been removed on `main` as of
  > the cleanup commit following v0.0.2. See [Unreleased] § Fixed for details.
- **"6-phase loop" is effectively 5-phase at v0.1**: `src/verdict.rs::watch_verdicts`
  is implemented + unit-tested but not yet instantiated in the iteration loop
  (`src/iteration.rs:86-94` TODO). A `loom run` invocation completes one
  dispatch cycle and exits — no verdict-driven re-iterate flow at v0.1. v0.2
  wires this in. See `docs/v02-growth-path.md`.
- **PATH-scrub safe only in dedicated bin dir**: `src/spawn_env.rs` substring
  match is over-broad if loom is installed into a shared bin dir
  (`~/.cargo/bin/`, `/usr/local/bin/`). Safe for dev (`target/{debug,release}/`)
  and Loom-only install dirs. v0.2 switches to exact-canonical-segment match.
  See `docs/v02-growth-path.md`.
- **Single machine, single goal**. Multi-goal concurrency, TUI / Web
  dashboard, multi-host scheduling, cost / quota tracking, additional worker
  adapters (Codex / Gemini / local), and cross-feature F1 / F2 / F3
  aggregation analysis are all v0.2+. See `docs/v02-growth-path.md`.
- **No `Co-Authored-By: Claude` commits**. Loom self-hosts via the AE plugin
  but ships under sole authorship.

### Build artifacts

- Binary: `target/release/loom` (`loom-rt v0.0.1`), ≈ 3.1 MiB on arm64 macOS.
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

[0.0.3]: #003--2026-05-22
[0.0.1]: #001--2026-05-21
