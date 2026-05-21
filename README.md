# Loom

> Ecosystem-agnostic meta-harness for [Agentic Engineering](https://github.com/xmkevinchen/agentic-engineering) (AE) workflows. Spawns headless agent workers, schedules feature work across an AE feature DAG, and aggregates per-feature verdicts.
>
> **v0.1 ships one worker adapter — Claude Code.** Codex / Gemini / local-model adapters are v0.2+ roadmap (see `docs/v02-growth-path.md`).

## Status

**Pre-alpha. v0.1 in development.** Implementation language: Rust (per `.ae/features/active/F-001-build-loom-v0-1-ai-agent-orchestrator-em/discussions/003-implementation-language/conclusion.md`).

## v0.1 user journey

```
$ loom run "add SSO login"
```

A typical session, end-to-end:

1. **Discovery** — Loom spawns a Claude Code headless session that invokes `ae:backlog` + `ae:analyze`, producing one or more AE features under `.ae/features/active/`.
2. **Scheduling** — Loom reads the resulting feature DAG (`index.md` `depends_on:`) and computes a ready set.
3. **Execution** — Loom dispatches the ready set in parallel (up to N=4 by default), each worker isolated in a per-feature git worktree. Workers run the full AE pipeline internally (`discuss → plan → work → review`).
4. **Aggregate + decide** — Loom watches each feature's `review.md`. Passing verdicts unblock downstream features; failing verdicts pause-and-notify.
5. **Iteration** — Loop Phases 2–4 until the DAG is exhausted or you `Ctrl-C`.
6. **Delivery** — Loom emits a structured dispatch log (`.loom/dispatch-<timestamp>.log`) — per-feature outcomes, cross-feature timing, worker identity, decision trace.

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

## What Loom is not

- **Not a Claude Code plugin** — AE plugin owns that surface. Loom is the outer harness.
- **Not a re-implementation of AE methodology** — AE plugin is the source of truth. Loom reads AE artifacts; it does not generate plans or reviews.
- **Not a general-purpose multi-agent runtime** — Loom is shaped for AE artifacts (feature DAG, `review.md` verdicts).

## Dependency direction

One-way: **Loom reads AE artifacts; AE plugin does not depend on Loom.** AE must continue to work standalone in a single Claude Code session.

## v0.2+ growth path

Stub — see `docs/v02-growth-path.md`. Out of v0.1 scope: multi-goal concurrency, TUI / Web dashboard, multi-host scheduling, cost / quota tracking, additional worker adapters (Codex / Gemini / local).

## Design references

- `docs/inspiration.md` — survey of prior-art harnesses (ccswarm, cosmix/loom, project-orchestrator, OpenAI Codex CLI, Meta orc).
- AE feature workspace: `.ae/features/active/F-001-.../` (gitignored).

## License

TBD.
