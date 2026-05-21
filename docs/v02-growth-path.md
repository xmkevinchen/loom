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

## Known v0.1 limitations (from /ae:review 2026-05-21)

### Verdict listener orphan — "6-phase loop" is effectively 5-phase at v0.1

`src/verdict.rs::watch_verdicts` is fully implemented and unit-tested but is **never instantiated in the iteration loop** (`src/iteration.rs:86-94` documents the gap with a TODO). Practical impact at v0.1: a single `loom run "<goal>"` invocation completes ONE dispatch cycle (Phase 2 + 3) + writes the dispatch log, then exits. The verdict-driven re-iterate-when-review-passes flow that the "6-phase loop" framing implies does NOT execute at v0.1.

v0.2 work: wire `verdict::watch_verdicts` into `LoomContext` as a tokio::select! arm alongside the cancellation token. Caught by /ae:review architect P2-1 + challenger C1.

### PATH-scrub safe only in dedicated bin dir

`src/spawn_env.rs::apply_scrubbed_path` uses **substring match** to filter PATH segments containing the loom binary's parent dir. Safe when loom lives in:

- `target/debug/loom` (dev) ✅
- `target/release/loom` (release dev) ✅
- a Loom-only install dir like `~/loom-bin/` ✅

**Unsafe** when loom is installed into a shared bin dir:

- `~/.cargo/bin/loom` (after `cargo install loom-rt`) ❌ — strips `~/.cargo/bin` from worker PATH, breaking `cargo`, `rustc`, `claude`, etc. for the spawned worker.
- `/usr/local/bin/loom` ❌ — same problem against the shared system bin.

v0.2 work: switch to exact-canonical-segment match with a sentinel-file probe fallback. Until then, document this constraint in install instructions and prefer Loom-only install dirs. Caught by /ae:review security P2.1.

### Other documented v0.1 limitations

- **`dispatch_metadata: serde_yaml::Value` typed-schema lock deferred to v0.2**. Will be replaced with `enum DispatchMetadata { ClaudeCode {...}, Codex {...}, ... }` (likely `#[serde(tag = "adapter")]`) when the second worker adapter ships. v0.1 contract: `Null` for ClaudeCodeAdapter (artifact.rs:18-23). Architect P2-4 + Challenger C4.
- **`drain_with_timeout` drops `JoinHandle` without `.abort()` on timeout**. Short-lived task leak — the underlying `read_to_end` task continues until pipe closes (usually milliseconds after child exits). Cancel-storm scenarios could amplify; v0.2 should use `tokio_util::task::AbortOnDropHandle`. Performance P1-A→P2.
- **Worktree leak on parent crash**. `Worktree::cleanup` is best-effort; SIGKILL/OOM-kill of the loom process leaves orphan worktrees under `.loom/worktrees/`. v0.2 should scan + clean at startup. Challenger C3.
- **`feature_id` not validated** against a safe regex before flowing into worktree filesystem paths. Threat surface low (requires repo-commit privilege), but defense-in-depth: v0.2 should add `^F-[0-9]+(-[a-z0-9-]+)?$` validation in `discovery::parse_frontmatter`. Security P2.2.
- **`verdict::process_event` uses `blocking_send` on a 64-slot bounded channel**, silently drops on full queue. Moot at v0.1 (verdict is unwired) but a v0.2 deadlock prerequisite if the listener lands without tightening this. Challenger C5.
- **Windows: cfg(not(unix)) `kill_process_group` is a no-op**. README does not explicitly say "Unix-only". v0.2 should add a `compile_error!` gate or platform-support doc note. Challenger C6.
- **`init_tracing` writes to `.loom/` relative to CWD**, not to a workspace-rooted location. Silent log-location drift if the user invokes `loom` from a directory other than the workspace root. Challenger C8.

These are documented as known limitations of v0.1.0; backlog items will be opened in `.ae/backlog/unscheduled/` for v0.2 work.

## Reversibility hooks

### PARALLEL-to-CC positioning (qualitative reopen)

PARALLEL-to-CC positioning (UX expectation: run `loom run` outside any CC session) is the highest-reversal vector identified at v0.1. If end-to-end testing in Steps 4–6 reveals UX friction beyond user tolerance, PARALLEL stance reopens — see disc 002 Topic 2 + plan F-001 Step 1 Doodlestein-regret note.

**Reopen criterion**: qualitative. v0.1 deliberately does **not** define a numeric falsifiability metric (e.g. "reopen if N% of dogfood sessions report friction"). At v0.1 dogfood scale — single solo user, low session count — a quantitative threshold would be noise dressed as data. Disposition: WAIVED at Step 1; reaffirmed at Step 9. Reopen is triggered by the user's qualitative judgment after live dogfooding, not by a counter. A v0.2+ metric becomes useful only when multi-user dogfood data exists; until then, the reopen stance is "user calls it".

### Language decision

Language decision is medium-low reversibility — see disc 003 conclusion + Step 4 SHIP GATE Go fallback path.
