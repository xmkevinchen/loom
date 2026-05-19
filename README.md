# Loom

> Multi-agent execution runtime for [Agentic Engineering](https://github.com/xmkevinchen/agentic-engineering) (AE) workflows.

## Status

**Pre-alpha. Design phase. Not yet implemented.**

This repository currently holds design notes and project scaffolding. No runtime code yet.

## Concept

Loom is an opinionated execution runtime for AE methodology.

**AE is the methodology layer (reasoning).** It defines what to build, how to break it down, and how to verify it. It lives as a Claude Code plugin and produces durable artifacts (`plan.md`, `review.md`, feature `index.md`).

**Loom is the execution layer.** It reads AE artifacts, schedules feature work across multiple agent backends (Claude Code, Codex, Gemini, local models), and provides observability — all while preserving AE's per-feature quality gates (review verdict).

### What Loom does

- Reads the AE feature DAG from `.ae/features/active/*/index.md` (using `depends_on`)
- Spawns isolated worker sessions per feature in dedicated worktrees
- Each feature session runs the full AE pipeline internally (`discuss` / `plan` / `work` / `review`)
- A DAG node is marked done **only when its `review.md` carries a passing verdict**
- Read-only TUI / Web fanout for monitoring multi-feature runs

### What Loom is not

- Not a general-purpose multi-agent runtime — it's shaped for AE artifacts
- Not a replacement for the AE plugin — AE continues to work standalone in a single Claude Code session; Loom is a scale-out accelerator
- Not a re-implementation of AE methodology — it embeds AE as the source of truth

## Relationship to AE

```
L1: AE plugin (methodology) — Claude Code skills, GTD model, white-box pipeline
        ↑
        | spawns headless sessions
        |
L2: Loom (execution)         — DAG scheduler, worker dispatch, persistence
        ↑
        | dispatches
        |
L3: agent workers            — Claude Code / Codex / Gemini / local models
```

Dependency direction is one-way: Loom reads AE artifacts; AE plugin does **not** depend on Loom.

## Design notes

Current design thinking lives in `.ae/discussions/054-loom-design-note/draft.md` (local, gitignored). After the design discussion is formalized, decisions will be archived under `docs/decisions/`.

## Development

Loom uses the AE plugin itself for its own development (dogfooding). This means new work goes through the AE pipeline: backlog → roadmap → analyze → discuss → plan → work → review.

## License

TBD.
