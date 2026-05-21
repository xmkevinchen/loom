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

> **Status**: locked at Step 2 of plan F-001 (2026-05-21).

**Crate name on crates.io: `loom-rt`** (first available from the ordered candidate list).

Lookup, 2026-05-21 via `https://crates.io/api/v1/crates/<name>`:

| Candidate | crates.io HTTP | Available? |
|---|---|---|
| `loom-rt` | 404 | ✅ taken first |
| `loomctl` | 404 | (would have been next) |
| `loom-agent` | 404 | (would have been next) |
| `loom-orchestrator` | 404 | (would have been next) |

Sanity check: `tokio` returns HTTP 200 from the same endpoint (API confirmed working).

**Rationale**: `loom-rt` reads as "Loom runtime" — fits L1=Loom execution-layer identity (`README.md`). The `-rt` suffix telegraphs the runtime/execution role without overcommitting to async runtime / TUI / other semantics. Public product name remains `Loom`; the `-rt` is internal Cargo concern only (binary name remains `loom` per `[[bin]]` config). Disc 001 T2 "名字不重要" user override applies — alternative candidate was selected mechanically.

## Per-project notes

*(Empty until Step 3 reading pass.)*
