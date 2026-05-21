# v0.2+ growth path

> **Status**: finalized at Step 9 of plan F-001 (v0.1 ship readiness, 2026-05-21).

Out of v0.1 scope; tracked here so they are not lost between releases.

## Out of v0.1 (explicit defers)

- **Multi-goal concurrency** — v0.1 handles a single goal at a time.
- **Real-time TUI / Web dashboard** — v0.1 emits `.loom/status.json` only.
- **Sophisticated re-plan logic** — Phase 4 just re-invokes `ae:plan` on failure; no custom merge logic.
- **Multi-host scheduling** — v0.1 is solo local single-machine.
- **Cost / quota tracking + budget enforcement** — none in v0.1.
- **Cross-goal worker pool management** — none.
- **TUI interactive-observation journey** — none.
- **Worker adapter implementations beyond ClaudeCodeAdapter** — Codex / Gemini / oMLX adapters deferred to v0.2+.
- **AE plugin schema-versioning policy + breaking-change protocol** — tracked as AE-plugin-BL #5; Loom v0.1 hardcodes against current implicit `ae:analyze → plan → work → review` sequence.
- **F1 / F2 / F3 quality / coordination / diversity aggregation analysis** — Step 5 emits raw structured-log events; cross-event aggregation = v0.2+ analysis tooling.
- **Parent-panic / SIGINT child cleanup** — orphan claude processes acceptable at v0.1 (Architect Consider C7).
- **Reconciliation Loop pattern beyond Semaphore** — Gemini Consider; v0.2+ if Semaphore proves insufficient.
- **`trace_id` propagation through filesystem artifacts** — Gemini Consider; v0.2+ if multi-host correlation needed.

## Reversibility hooks

### PARALLEL-to-CC positioning (qualitative reopen)

PARALLEL-to-CC positioning (UX expectation: run `loom run` outside any CC session) is the highest-reversal vector identified at v0.1. If end-to-end testing in Steps 4–6 reveals UX friction beyond user tolerance, PARALLEL stance reopens — see disc 002 Topic 2 + plan F-001 Step 1 Doodlestein-regret note.

**Reopen criterion**: qualitative. v0.1 deliberately does **not** define a numeric falsifiability metric (e.g. "reopen if N% of dogfood sessions report friction"). At v0.1 dogfood scale — single solo user, low session count — a quantitative threshold would be noise dressed as data. Disposition: WAIVED at Step 1; reaffirmed at Step 9. Reopen is triggered by the user's qualitative judgment after live dogfooding, not by a counter. A v0.2+ metric becomes useful only when multi-user dogfood data exists; until then, the reopen stance is "user calls it".

### Language decision

Language decision is medium-low reversibility — see disc 003 conclusion + Step 4 SHIP GATE Go fallback path.
