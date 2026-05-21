# Inspiration — reference projects

> **Status**: placeholder. Populated during Step 3 of plan F-001 (read-references + Rust orientation pass).

This document captures the read-and-document pass over prior-art agent-orchestration harnesses. For each project, three sections:

1. **What it does** — brief description.
2. **What we copy** — patterns and idioms reused in Loom.
3. **What we deviate** — places Loom intentionally diverges, and why.

## Reference projects (Step 3 target list)

- **ccswarm** — Claude Code worktree orchestrator (Rust).
- **cosmix/loom** — Claude Code worktree orchestrator (Rust).
- **project-orchestrator** — Claude Code worktree orchestrator (Rust).
- **OpenAI Codex CLI** — agent harness state-machine + tracing approach (Rust, 96.1%).
- **Meta orc software** — Loom's reference implementation (Rust, user-supplied if accessible). **Skip condition**: if user cannot supply source / a public link by Step 3 kickoff, document the absence in the per-project notes section ("Meta orc — not read; reason: source not accessible to Step 3 pass") and proceed with the other 4 references. AC2 requires ≥4 substantive sections, not 5.

## Naming

> **Status**: placeholder. Populated during Step 2 of plan F-001 (crate name lock).

Crate name on crates.io: TBD (Step 2). Ordered candidates: `loom-rt` → `loomctl` → `loom-agent` → `loom-orchestrator`. Public product name remains `Loom`.

## Per-project notes

*(Empty until Step 3 reading pass.)*
