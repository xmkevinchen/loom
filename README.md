# Loom

> Ecosystem-agnostic meta-harness for [Agentic Engineering](https://github.com/xmkevinchen/agentic-engineering) (AE) workflows. Spawns headless agent workers, schedules feature work across an AE feature DAG, and aggregates per-feature verdicts.
>
> **v0.1 ships one worker adapter — Claude Code.** Codex / Gemini / local-model adapters are v0.2+ roadmap (see `docs/v02-growth-path.md`).

## Status

**Pre-alpha. v0.0.2 distribution-readiness milestone (2026-05-22).** Harness foundation — `Worker` trait + `ClaudeCodeAdapter` subprocess management (Codex CLI `consume_output` idiom moved over), `tracing` init, atomic writes, per-segment canonical PATH-scrub + LOOM_PARENT_PID env-var recursion guard, 4 CLI subcommands, 59 passing tests. The 6-phase loop iterates past one cycle (F-002 verdict listener wired). End-to-end self-host dispatch verified via F-SMOKE smoke test 2026-05-22: `loom dispatch <F-NNN>` → `claude -p` → `/ae:work` + `/ae:review` → review.md verdict round-trip works. Discovery phase (`/ae:backlog` + `/ae:analyze` spawn from Loom) is not yet smoke-tested end-to-end. Implementation language: Rust (per `.ae/features/done/F-001-build-loom-v0-1-ai-agent-orchestrator-em/discussions/003-implementation-language/conclusion.md`). See `CHANGELOG.md` § "Known limitations" and `docs/v02-growth-path.md` for the gap list.

## Target user

v0.1 is shaped for a single archetype: **a solo developer dogfooding their own Loom build**. Single-machine, single-goal, no auth or remote scheduling. Multi-user / multi-host scenarios are v0.2+ (see `docs/v02-growth-path.md`).

## User journey

```
$ loom run "add SSO login"
```

A typical session, end-to-end:

1. **Discovery** — Loom spawns a Claude Code headless session that invokes `ae:backlog` + `ae:analyze`, producing one or more AE features under `.ae/features/active/`.
2. **Scheduling** — Loom reads the resulting feature DAG (`index.md` `depends_on:`) and computes a ready set.
3. **Execution** — Loom dispatches the ready set in parallel (up to N=4 by default), each worker isolated in a per-feature git worktree. Workers run the full AE pipeline internally (`discuss → plan → work → review`).
4. **Aggregate + decide** — Loom observes each feature's `review.md` via a two-tier path: a notify-based watcher catches verdicts within milliseconds, and a per-cycle disk scan acts as the authoritative source if any notify event was missed. Passing verdicts unblock downstream features; a failing AE verdict exits `loom run` with code `5` (`EXIT_AE_REVIEW_REJECTED`) — distinct from worker-execution failure (code `4`).
5. **Iteration** — Loop Phases 2–4 until the DAG is exhausted or you `Ctrl-C`. An incomplete run is never reported as success: when work remains but the dependency graph gates every pending feature, both `loom run` and `loom dispatch` exit with code `7` (`EXIT_DEPS_STUCK`); code `8` (`EXIT_REVIEW_MISSING` — a clean worker that produced no readable `review.md` verdict) is reserved, with detection wired by F-014. Both rank below an operator cancel (`130`) and below codes `4`/`5` — full precedence `5 > 4 > 130 > 7 > 8 > 0` (see `src/cli.rs`).
6. **Delivery** — Loom emits a structured dispatch log (`.loom/dispatch-<timestamp>.log`) — per-feature outcomes, cross-feature timing, worker identity, decision trace.

### Phase 4 trigger source (dogfood form)

Loom's Phase 4 reacts to `review.md` writes — but in the current v0.0.x line, **`/ae:work` does NOT auto-invoke `/ae:review` on exit**. So a real dogfood `loom run "<goal>"` will see Phase 4 fire only when an external process writes the terminal verdict (the operator running `/ae:review` manually in a split session, or a stub worker that double-writes `review.md` + `index.md.pipeline.work=done`). This is intentional v0.0.x scope; v0.3+ closes the gap by making `ae:work` write a terminal verdict on exit. See `tests/e2e/verdict_multi_cycle_test.rs::StubWriteVerdictWorker` for the canonical stub-worker pattern.

## PARALLEL to Claude Code, not AUGMENT

Loom is a **separate face** alongside Claude Code, not a plugin inside it.

```
L1: Loom (execution layer)   — DAG scheduler, worker dispatch, persistence
        ↓ spawns headless
L2: AE plugin (methodology)  — runs INSIDE each spawned worker session
        ↓ orchestrates skills
L3: agent backends           — Claude Code / Codex / Gemini / local models
```

**UX expectation for v0.1.** Run `loom run "<goal>"` from a terminal **outside** of any Claude Code session. Loom spawns Claude Code headlessly as a worker. You do **not** keep a parent Claude Code session open while Loom is running.

Mechanism: Loom deliberately removes its own binary directory from the spawned worker's `PATH`. If you ran Loom from inside a Claude Code session that itself had `loom` on `PATH`, the spawned worker could in turn discover `loom`, spawn another Claude Code, and recurse indefinitely — a process-spawn loop that exhausts the host. PATH-absence is the structural guard against that recursion.

If this UX friction proves too high during the v0.1 ship gate (Step 4–6 end-to-end testing), PARALLEL positioning is reopenable — see `docs/v02-growth-path.md`.

## Recursion guard

Loom uses two complementary mechanisms (defense-in-depth) to prevent a worker subprocess from recursively spawning another Loom:

1. **PATH-scrub via per-segment canonical probe.** Before spawn, Loom canonicalizes its own binary and walks each `PATH` segment computing `segment/loom.canonicalize()`. Segments whose `loom` resolves to OUR canonical binary are stripped; everything else survives. Symlink aliases that point at the same target are caught because canonicalize collapses them. `HOME` / `USER` / `SHELL` / `TMPDIR` are preserved — only `PATH` is rewritten.
2. **`LOOM_PARENT_PID` env-var guard.** The parent injects `LOOM_PARENT_PID=<pid>` on every worker spawn. Any child Loom process that runs `loom run` or `loom dispatch` checks for that env var and refuses with exit code `6` (`EXIT_RECURSION_DETECTED`). `loom status` / `loom version` / `loom --help` are intentionally not guarded — read-only diagnostics must remain available inside worker subprocesses.

The two layers are **complementary** (not strictly independent — see corner case below): PATH-scrub catches workers that try to invoke `loom` as a bare command; the env-var guard catches workers that somehow obtain an absolute path (e.g. they were given one by a tool, or the operator installed Loom into a shared bin dir where PATH-scrub deliberately preserves the other tools in that dir — see "Install isolation" below). One narrow case where the env-var guard is load-bearing: per-segment fail-open in PATH-scrub means a segment whose `<seg>/loom` does not currently canonicalize is kept on PATH; if an attacker creates `<seg>/loom -> /path/to/our/loom` between scrub time and spawn time, the worker reaches Loom via PATH and only the `LOOM_PARENT_PID` guard prevents recursion. This is acceptable under Loom's single-user threat model (attacker with PATH-dir write access already wins) but means the two layers are coupled at this corner.

Do not run `cargo install --force loom-rt` (or `cargo install loom-rt --force`) while a `loom run` is in flight. The newly installed binary's canonical path may differ from the running binary's path; PATH-scrub would then silently miss the running instance because the per-segment probe compares against `current_exe()` of the in-flight process. The `LOOM_PARENT_PID` guard still blocks recursion, but operator visibility is reduced — finish or stop the running session first.

## What Loom is not

- **Not a Claude Code plugin** — AE plugin owns that surface. Loom is the outer harness.
- **Not a re-implementation of AE methodology** — AE plugin is the source of truth. Loom reads AE artifacts; it does not generate plans or reviews.
- **Not a general-purpose multi-agent runtime** — Loom is shaped for AE artifacts (feature DAG, `review.md` verdicts).

## Dependency direction

One-way: **Loom reads AE artifacts; AE plugin does not depend on Loom.** AE must continue to work standalone in a single Claude Code session.

## v0.2+ growth path

Stub — see `docs/v02-growth-path.md`. Out of v0.1 scope: multi-goal concurrency, TUI / Web dashboard, multi-host scheduling, cost / quota tracking, additional worker adapters (Codex / Gemini / local).

## Build / Install

### Prerequisites

- **Rust stable** (1.78+) — pinned via `rust-toolchain.toml`.
- **AE plugin** installed in Claude Code — required for an end-to-end `loom run` (Discovery phase invokes `ae:backlog` + `ae:analyze`). Without it, Loom still builds and the stub path runs; only the real discovery loop is gated.
- **git** — required for per-feature worktree isolation during dispatch.

### Build

```
cargo build --release
```

The binary lands at `target/release/loom`. Public CLI name is `loom`; the crate name `loom-rt` is an internal Cargo concern (the `-rt` suffix means "runtime"; see `docs/inspiration.md` § Naming).

### Install isolation

If you `cargo install loom-rt`, prefer `--root ~/.loom/` so the binary lands in a dedicated directory rather than in `~/.cargo/bin/` alongside other cargo-installed tools. The per-segment PATH-scrub (see "Recursion guard" above) strips the directory holding `loom` from each worker's `PATH` — that's the recursion prevention. When that directory is shared with other tools (`cargo`, `ripgrep`, etc.), those tools also become unreachable from worker subprocesses. The `LOOM_PARENT_PID` env-var guard still blocks recursion, but you lose access to co-installed tools inside workers. Installing into a dedicated dir avoids the UX cost.

```
cargo install --root ~/.loom/ loom-rt
export PATH="$HOME/.loom/bin:$PATH"
```

### Smoke test

```
target/release/loom version    # → loom-rt v0.1.0 (release)
target/release/loom status     # → status summary, exit 0
target/release/loom run "echo test goal"   # → end-to-end 6-phase loop
```

The `run` smoke test produces `.loom/dispatch-<timestamp>.log` (JSON) and `.loom/run-<timestamp>.log` (newline-delimited JSON, parseable with `jq -c . < .loom/run-*.log`). Without the AE plugin, Discovery emits a warning and the loop exits cleanly on the empty / single-feature path.

## Design references

- `docs/inspiration.md` — survey of prior-art harnesses (ccswarm, cosmix/loom, project-orchestrator, OpenAI Codex CLI, Meta orc).
- `docs/v02-growth-path.md` — out-of-v0.1 scope and reversibility hooks.
- `CHANGELOG.md` — Keep-a-Changelog history.
- AE feature workspace: `.ae/features/active/F-001-.../` (gitignored).

## License

MIT — see `LICENSE`.
