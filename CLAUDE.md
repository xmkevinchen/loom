# Loom Project Instructions

## Status

v0.1 in development. Implementation language locked: **Rust** (per disc 003 conclusion). Runtime source under `src/`; agent-assisted development workflow per plan F-001.

## Language convention

Mirrors the AE plugin project:

- **Chat** — 中文（与 user 的对话）
- **Git-tracked docs** — English（README, CHANGELOG, design decisions, source code comments）
- **Non-archive docs** — 中文 OK（gitignored process artifacts under `.ae/`）

## Rust conventions

- Toolchain pinned via `rust-toolchain.toml` (added in Step 2 per plan F-001).
- Async runtime: `tokio` (full or process/fs/time/signal features).
- CLI: `clap`.
- Serialization: `serde` + `serde_yaml`.
- Logging: `tracing` + `tracing-subscriber` (JSON-line file subscriber to `.loom/run-*.log`).
- Errors: `anyhow` at boundaries; concrete error types within crates if needed.
- Format / lint: `cargo fmt` + `cargo clippy` clean before commit.
- Test layout: `cargo test` (`tests/` for integration, `#[cfg(test)]` mods for unit).

## Reader contract for agents

When reading this codebase via Claude Code:

- **Workflow** is agent-assisted: agents write Rust; user reviews and steers. Read-level Rust fluency is the user-target — do not assume the user will hand-write the borrow-checker fight.
- **Reference projects** in `docs/inspiration.md` are the first place to look for "how do other Rust orchestrators do this?" before inventing a pattern.
- **Recursion guard** (`src/spawn_env.rs`, shipped F-003): any code spawning Claude Code (or other backends) MUST scrub the host-binary directory from the spawned `PATH`. HOME / USER / SHELL / TMPDIR are preserved; only the Loom binary's dir is filtered out. `spawn_env.rs` is the single source of truth for the per-segment canonical-probe algorithm; it is wired into the default worker at `main.rs::default_worker` via `with_scrubbed_path`.
- **Artifact writes** (`src/atomic_write.rs`, shipped F-003): go via write-to-`.tmp` then `std::fs::rename` (atomic). EXDEV fallback for cross-filesystem renames.
- **Feature id validation** (`src/discovery.rs::validate_feature_id`, shipped F-006): every feature `id` read from an AE `index.md` frontmatter MUST match `^F-\d{3}(-slug)?$` (slug is ASCII `[a-z0-9-]`). Validation fires in `parse_frontmatter`; a non-conforming id makes `read_active_features` skip that feature with a warn (discovery does not abort). This is the single gate before `id` reaches worktree paths and `refs/heads/loom-features/<id>` ref names (defense-in-depth against path traversal + ref injection, BL-006).
- **Worktree startup cleanup** (`src/dispatch.rs::prune_stale_worktrees`, shipped F-006): worktrees live at `.loom/worktrees/<feature_id>-<pid>`. On startup (both `loom run` and `loom dispatch`) Loom scans that dir and reclaims orphans whose `<pid>` is no longer alive, via `git worktree remove --force`. Liveness uses `libc::kill(pid, 0)`: only `ESRCH` (no such process) counts as dead — `EPERM` (pid owned by another uid) and any other errno are treated as ALIVE, because the removal is irreversible and keeping an orphan is harmless. PID reuse can yield a false "alive" (orphan never reclaimed) — harmless under the single-orchestrator model the recursion guard enforces. Multi-process coordination (a `.lock-<pid>` sentinel, BL-005) is deferred to v0.2.

## Self-hosting (dogfooding)

Loom uses [AE plugin](../agentic-engineering) for its own development. This is intentional:

- Loom is AE's first real external user.
- Friction discovered while developing Loom is signal for AE plugin improvements.
- New Loom features go through the full AE pipeline: backlog → roadmap → analyze → discuss → plan → work → review.

To set up: install the AE plugin in Claude Code, then operate in this repo. AE artifacts accumulate under `.ae/` (gitignored).

## Relationship to AE plugin

- Loom is an **independent project**, not a fork or extension of the AE plugin.
- Loom **depends on** AE plugin's artifact schema (one-way: Loom reads AE artifacts).
- AE plugin does **not** depend on Loom — AE must continue to work standalone.
- If Loom needs a new AE artifact field, open a backlog item in the AE plugin repo, not a custom extension here.
- Layering: **L1 = Loom (execution), L2 = AE (methodology) inside each spawned worker, L3 = agent backends**. See `README.md`.

## Git

- Feature branches, PR to main (when remote is set up).
- Never push to remote unless explicitly approved by user.
- No `Co-Authored-By: Claude` trailer in commits.

## Path conventions

- `src/` — Rust runtime source (Step 2+).
- `tests/` — integration tests.
- `docs/` — tracked design documents and decisions (`inspiration.md`, `v02-growth-path.md`).
- `.ae/` — AE methodology workspace, gitignored.
- `.loom/` — runtime state + logs at runtime (gitignored).
