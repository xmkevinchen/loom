# Loom Project Instructions

## Status

Pre-alpha. Design phase. No runtime code yet — currently only scaffolding + design notes.

## Language convention

Mirrors the AE plugin project:

- **Chat** — 中文（与 user 的对话）
- **Git-tracked docs** — English（README, CHANGELOG, design decisions, source code comments）
- **Non-archive docs** — 中文 OK（gitignored process artifacts under `.ae/`）

## Self-hosting (dogfooding)

Loom uses [AE plugin](../agentic-engineering) for its own development. This is intentional:

- Loom is AE's first real external user
- Friction discovered while developing Loom is signal for AE plugin improvements
- New Loom features go through the full AE pipeline: backlog → roadmap → analyze → discuss → plan → work → review

To set up: install the AE plugin in Claude Code, then operate in this repo. AE artifacts will accumulate under `.ae/` (gitignored).

## Relationship to AE plugin

- Loom is an **independent project**, not a fork or extension of the AE plugin
- Loom **depends on** AE plugin's artifact schema (one-way: Loom reads AE artifacts)
- AE plugin does **not** depend on Loom — AE must continue to work standalone
- If Loom needs a new AE artifact field, open a backlog item in the AE plugin repo, not a custom extension here
- See `.ae/discussions/054-loom-design-note/draft.md` for full boundary reasoning

## Git

- Feature branches, PR to main (when remote is set up)
- Never push to remote unless explicitly approved by user
- No `Co-Authored-By: Claude` trailer in commits

## Path conventions

- `docs/` — tracked design documents and decisions
- `.ae/` — AE methodology workspace, gitignored
- Runtime source code location TBD (depends on language choice — Rust vs Go, discuss item)
