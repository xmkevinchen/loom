# Inspiration — reference projects

> **Status**: Step 3 complete; frozen for v0.1 (last review 2026-05-21, plan F-001 Step 9). v0.2+ reference additions are tracked in `docs/v02-growth-path.md`.

This document captures the read-and-document pass over prior-art agent-orchestration harnesses. For each project, three sections:

1. **What it does** — brief description.
2. **What we copy** — patterns and idioms reused in Loom.
3. **What we deviate** — places Loom intentionally diverges, and why.

## Reference projects (Step 3 target list)

- **ccswarm** — Claude Code worktree orchestrator (Rust). `github.com/nwiizo/ccswarm`
- **cosmix/loom** — Claude Code worktree orchestrator (Rust). `github.com/cosmix/loom`
- **project-orchestrator** — Shared-knowledge AI agent coordinator (Rust). `github.com/this-rs/project-orchestrator`
- **OpenAI Codex CLI** — coding agent harness, Rust (~96% of repo). `github.com/openai/codex` (Rust source under `codex-rs/`)
- **Meta orc software** — not read; reason: source not accessible to Step 3 pass (per documented skip condition; AC2 requires ≥4 substantive sections, satisfied by ccswarm + cosmix-loom + project-orchestrator + OpenAI Codex CLI).

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

### ccswarm (`github.com/nwiizo/ccswarm`)

#### What it does

ccswarm is a Rust-native multi-agent orchestrator that drives Claude Code (and other providers) through specialized role agents — Frontend, Backend, DevOps, QA — each running in its own git worktree. Sessions are managed as native PTYs through the in-house `ai-session` crate rather than `tmux`. The product surface combines a CLI + a ratatui-based TUI with task delegation, template scaffolding, and OWASP-style security scanning. As of v0.4.3 ccswarm self-describes as a Proof of Concept; coordination is real (Apache ZooKeeper via `src/coordination/zk_adapter.rs`) rather than mocked, but the README explicitly flags the project as PoC stage. For Loom it is most useful as a module-layout + dep-baseline reference; the orchestration logic itself is more complex than v0.1 needs.

#### What we copy

- **Module hierarchy under one crate.** Top-level source under `crates/ccswarm/src/` is split into `orchestrator/`, `agent/`, `session/`, `subagent/`, `cli/`, `config/`, `coordination/`, `execution/`, `git/`, `hooks/`, `workspace/`, `template/`. Loom's `src/{worker, dispatch, state, artifact, verdict}.rs` is the v0.1-sized projection of the same family: `worker.rs ≈ agent + session`, `dispatch.rs ≈ orchestrator + coordination`, `state.rs ≈ workspace`, `artifact.rs ≈ template/output`, `verdict.rs ≈ hooks/QA`. Validates that we can stay flat (files-not-folders) at v0.1 and split into folders only when a module exceeds ~500 lines.
- **Channel-based orchestration over shared-state locking.** ccswarm uses `async-channel = 2.3` for actor-style message passing and explicitly notes "no Arc<Mutex<...>>" as a design goal. Loom v0.1 will follow the same posture — single owner per piece of state, communication via channels — to keep concurrency reasoning local.
- **Dependency core.** `tokio = 1.40`, `async-trait = 0.1`, `serde = 1`, `clap = 4.5`, `tracing = 0.1`, `anyhow = 1`, `thiserror = 2`, `ratatui = 0.29`, `crossterm = 0.29`. This is essentially the same kernel Step 2 picked for `loom-rt`; concur.
- **`async-trait`-shaped Worker abstraction.** ccswarm's `agent/` directory abstracts each role behind a trait so the orchestrator can dispatch generically. Loom's `Worker` trait in `worker.rs` will follow the same shape (`async fn run(&self, task: Task) -> Result<Artifact>`) so we can swap `ClaudeCodeAdapter` for mock workers in tests.

#### What we deviate

- **No PTY.** ccswarm centers PTY sessions (via `ai-session`) to give agents an interactive shell. Loom v0.1 is single-machine + solo + headless: a `tokio::process::Command` with piped stdout is sufficient, and PTY adds a dependency surface (terminal capabilities, signal forwarding) we don't need until live-attach UX is on the roadmap.
- **No role specialization.** ccswarm pre-defines Frontend/Backend/DevOps/QA archetypes. Loom's 6-phase loop (Discovery → Scheduling → Execution → Aggregate+decide → Iteration → Delivery) does not partition by role at v0.1 — every worker is a generic Claude Code adapter parameterized by prompt/system message. Role specialization is post-v0.1.
- **No security scanning, no template engine, no TUI.** Out of v0.1 scope. Loom keeps surface area small: CLI only, plain `tracing` to stderr.
- **PARALLEL positioning.** ccswarm wraps Claude Code as an internal implementation detail of its own agent abstraction. Loom positions Claude Code as a sibling tool — the user can use either; Loom is for goal-loop orchestration, not for replacing day-to-day Claude Code use.

### cosmix/loom (`github.com/cosmix/loom`)

#### What it does

cosmix/loom is a name-collision peer: a Rust-based orchestrator that swarms multiple Claude Code instances across git worktrees, with persistent state under `.work/`, stage-aware crash recovery, context handoffs between stages, and goal-backward verification (artifacts / wiring / wiring_tests / dead_code_check). Plans are authored as `doc/plans/PLAN-<name>.md` with YAML metadata; `loom init` parses them and instantiates stage state. Optional `--remote-control` mode talks to Claude Code via `~/.claude/.credentials.json`. Linux is primary, macOS is build-supported.

#### What we copy

- **Persistent state directory as the system of record.** `.work/{config.toml, stages/, sessions/, signals/, handoffs/}` is the same shape Loom needs for `state.rs` + `artifact.rs`. Specifically the `signals/` queue (inter-stage messages) and `handoffs/` (context transfer) generalize cleanly to our 6-phase loop's "decide → iterate" arrows. Loom v0.1 will likely adopt `.loom/` with a comparable layout (`stages/` → `phases/`, `sessions/` → `workers/`).
- **Stage-aware recovery.** Each stage owns its directory and can be replayed individually. Loom's "Aggregate + decide" phase wants the same property: a crash mid-phase must be replayable from on-disk state without re-running the worker.
- **Goal-backward verification vocabulary.** cosmix/loom uses `artifacts`, `wiring`, `wiring_tests` as named verification gates (verified in source — e.g., `src/wiring.rs`, `src/artifact.rs`). This vocabulary maps onto Loom's `verdict.rs` — we will reuse the names where semantics align, to avoid inventing parallel terminology. (Note: the original Step 3 draft included `dead_code_check` here; cross-family verification did not find it in the repo, so it is omitted.)
- **`nix` for process liveness checks.** `loom/src/process/mod.rs` exposes `is_process_alive(pid: u32) -> bool` via `nix::sys::signal::kill` with signal 0. Loom needs the same primitive for adopting orphaned worker PIDs across restarts; we will add `nix = "0.31"` when worker-resume lands (not v0.1.0, but v0.1.x).

#### What we deviate

- **Async-first.** cosmix/loom is **synchronous**: its `Cargo.toml` contains no `tokio`, no `async-trait`, and instead uses `wait-timeout = "0.2.1"` to bound blocking `Child::wait`. This is a clean design but assumes one worker per process and no concurrent stdout streaming. Loom v0.1 needs concurrent multi-worker execution (Scheduling phase fans out), so we commit to `tokio` and accept the cost.
- **Heavier module surface.** cosmix/loom's `src/` has 18+ modules (`cli/`, `commands/`, `daemon/`, `diagnosis/`, `fs/`, `git/`, `handoff/`, `hooks/`, `map/`, `models/`, `orchestrator/`, `parser/`, `plan/`, `process/`, `sandbox/`, `skills/`, `verify/`, …). At v0.1 Loom should not match this — many of these correspond to features (skills, daemon mode, diagnosis subcommands) that are post-v0.1.
- **No external plan-file format.** cosmix/loom requires the user to author `PLAN-<name>.md` first; the orchestrator only executes pre-existing plans. Loom is the opposite: the user supplies a *goal*, and Loom's Discovery phase produces the plan. Different entry point → different module emphasis (we need a Discovery worker; they don't).
- **No remote-control / credentials handling at v0.1.** cosmix/loom's `--remote-control` flag invokes claude ≥ 2.1.51 in a non-default mode. Loom's `ClaudeCodeAdapter` will use stock `claude -p` (or equivalent non-interactive flag) and read credentials from the user's environment without bespoke handling.

### project-orchestrator (`github.com/this-rs/project-orchestrator`)

#### What it does

project-orchestrator is structurally different from the other three: it does **not** orchestrate Claude Code child processes. Instead, it provides shared knowledge infrastructure that multiple AI agents query as a service — a Neo4j knowledge graph for plans/decisions/RFCs/task hierarchies, Meilisearch for semantic code retrieval, and Tree-sitter parsers for ~16 languages. It installs three binaries — `orchestrator` (server), `orch` (CLI), `mcp_server` (Claude Code MCP bridge) — and exposes the corpus via MCP, WebSocket, and REST. Claude Code, Cursor, etc. connect to it; it does not spawn them.

#### What we copy

- **Three-binary workspace shape.** The split `orchestrator` (long-lived server) + `orch` (CLI client) + `mcp_server` (protocol bridge) is a useful mental model even if Loom v0.1 ships only a single binary. When we eventually add a daemon mode + MCP server, this layout (separate binaries, shared core crate, communicate via a defined protocol) is the cleanest split.
- **MCP server as the integration point for "let other agents talk to Loom".** Out-of-v0.1, but worth flagging: if Loom v0.2 wants Claude Code or Codex to be able to query Loom state ("which phase is F-001 in?"), shipping an MCP server is the right pattern. We do not need to invent a new protocol.
- **Workspace dependency baseline.** `tokio = 1.49 (full)`, `tokio-util = 0.7`, `tokio-stream = 0.1`, `async-trait = 0.1`, `axum = 0.8`, `serde`, `clap = 4.5`. The `tokio-stream` and `tokio-util` additions are interesting — Loom may want them when stdout-line-streaming gets implemented in Step 4. Not adding pre-emptively; flag for Step 4.

#### What we deviate

- **No external services.** project-orchestrator requires Neo4j (`bolt://localhost:7687`), Meilisearch, and optionally NATS running on the host. Loom v0.1 is single-binary, no external runtime dependencies — state lives in plain files under `.loom/` (TOML + JSON). Adding a graph database is a non-starter for "install one binary and go".
- **No Tree-sitter at v0.1.** project-orchestrator parses source code into ASTs to feed its knowledge graph. Loom's Discovery phase reads code as text and delegates parsing to the worker (Claude Code already has its own code understanding). We do not need a parallel parser.
- **Inverted role.** project-orchestrator is a *server agents talk to*; Loom is a *client that drives agents*. Pulled in only because of the name in the Step 3 target list — the structural lesson is mostly negative ("don't take on Neo4j / Meilisearch / Tree-sitter at v0.1") rather than positive idiom borrowing.
- **No Tauri.** project-orchestrator includes a Tauri desktop GUI under `desktop/src-tauri/`. Loom v0.1 is CLI-only; GUI is post-v0.1 and would more likely be a separate frontend crate, not a Tauri wrap of the orchestrator binary.

### OpenAI Codex CLI (`github.com/openai/codex`, Rust under `codex-rs/`)

> **Highest-priority reference for Step 4.** Codex's `core/src/exec.rs` is production-grade subprocess management with concurrent stdout drain + timeout + SIGKILL escalation + I/O drain timeout. The exact idiom Step 4 pre-loads is implemented in `consume_output()`.

#### What it does

Codex CLI is OpenAI's terminal-resident coding agent — fullscreen Ratatui TUI (`codex tui`), headless `codex exec`, plus an experimental MCP server and client. It is distributed as a single binary so users can install once and use without a runtime. The Rust workspace (`codex-rs/`) replaced the legacy TypeScript implementation and is now the default; the workspace contains 130+ crates organized into core (`core/`, `protocol/`), interfaces (`cli/`, `tui/`, `exec/`), feature crates (`memories/`, `skills/`, `file-search/`, `hooks/`, `agent-graph-store/`), and infrastructure (`exec-server/`, `mcp-server/`, `cloud-tasks/`, `network-proxy/`).

#### What we copy

**The `consume_output` idiom — load-bearing for Step 4.** File: `codex-rs/core/src/exec.rs`, function `consume_output`, lines **1322–~1425** at commit `0b4f86095c8005d8f74e9c62b971d72c1670aa88`. Permalink: `https://github.com/openai/codex/blob/0b4f86095c8005d8f74e9c62b971d72c1670aa88/codex-rs/core/src/exec.rs#L1322-L1425`.

> **Citation discipline**: line numbers below verified against the pinned commit on 2026-05-21 via `gh api /repos/openai/codex/contents/codex-rs/core/src/exec.rs?ref=<SHA>`. They are accurate for this commit; future Codex HEAD will drift. Step 4 should `git clone` Codex CLI at the pinned SHA (or grep for `async fn consume_output(`) before transcribing — line numbers are a navigation aid, the function signature is the canonical anchor.

The exact pattern Loom's `ClaudeCodeAdapter` should mirror (real line numbers in comments):

```rust
// codex-rs/core/src/exec.rs:1332–1356  (take + spawn drain tasks)
let stdout_reader = child.stdout.take().ok_or_else(|| /* error */)?;
let stderr_reader = child.stderr.take().ok_or_else(|| /* error */)?;

let stdout_handle = tokio::spawn(read_output(
    BufReader::new(stdout_reader), stdout_stream.clone(), false, cap,
));
let stderr_handle = tokio::spawn(read_output(
    BufReader::new(stderr_reader), stdout_stream.clone(), true, cap,
));

// :1364 tokio::pin! expiration_wait, then :1365–1387 select!
tokio::pin!(expiration_wait);
let (exit_status, timed_out) = tokio::select! {
    status_result = child.wait() => { (status_result?, false) }
    outcome = &mut expiration_wait => {
        kill_child_process_group(&mut child)?;
        child.start_kill()?;
        /* synthesize timeout exit */
    }
    _ = tokio::signal::ctrl_c() => {
        kill_child_process_group(&mut child)?;
        child.start_kill()?;
        /* synthesize SIGKILL exit */
    }
};

// :1414–1415 — drain join with bounded timeout, abort() on elapse
let stdout = await_output(&mut stdout_handle, io_drain_timeout).await?;
let stderr = await_output(&mut stderr_handle, io_drain_timeout).await?;
```

Three load-bearing details to internalize before Step 4 (mapped to plan F-001 Step 4's documented Must-Fixes):

1. **`take()` on the `Option<ChildStdout>` is fallible** — if `Stdio::piped()` wasn't set, `take()` returns `None`. Loom should treat this as an I/O error, not a panic. The plan's draft snippet uses `.expect("piped")`; that's fine for a prototype but Codex's `ok_or_else` is the production-correct shape. **Maps to**: plan Step 4 "Anticipated first-error: `error[E0382]: borrow of partially moved value`" (Doodlestein adversarial).
2. **Drain handles outlive `child.wait()` and are joined with a bounded timeout.** Inner `await_output` (defined nested inside `consume_output` at ~line 1391) wraps the join in `tokio::time::timeout(io_drain_timeout, ...)` and `handle.abort()` on elapse. This is the guard against the long-output-deadlocks-parent-on-wait failure mode. **Maps to**: plan Step 4 Architect MF2 "Concurrent stdout drain in independent tokio task".
3. **Kill is two-step on timeout: process group THEN `start_kill`.** `kill_child_process_group(&mut child)?; child.start_kill()?;` at lines 1371, 1382. The group kill catches forked-grandchildren (sandbox / sudo / shell wrappers that re-spawn); `start_kill` is the tokio-native SIGKILL of the direct child. **Maps to**: plan Step 4 Architect MF3 "Timeout that actually kills the child". Loom on macOS needs the same two-step or grandchild-orphans will accumulate; this is **explicit grandchild handling** that Loom should adopt (Codex's pattern does cover the forked-grandchild case via the process-group kill, despite Step 4 having raised macOS grandchild orphaning as an MF1 concern).

Additional patterns worth copying:

- **`tokio::pin!` of the expiration future** (line 1364) so it can be `&mut`-borrowed across multiple `select!` arms / iterations.
- **Synthetic exit-status codes for non-natural exits** (`synthetic_exit_status(EXIT_CODE_SIGNAL_BASE + TIMEOUT_CODE)` inside the select! arms ~lines 1374, 1378, 1386). Lets downstream verdict code distinguish "process exited 137" from "we killed it on timeout" without a parallel boolean.
- **Workspace-shared dependency versions** via `[workspace.dependencies]` — Codex's root `codex-rs/Cargo.toml` declares core deps at workspace level. Loom is single-crate at v0.1; if/when we split, follow the workspace pattern. (Specific Codex CLI dep versions not pinned here — see workspace manifest at the SHA above for the source of truth.)

#### What we deviate

- **130 crates is post-v0.1 territory.** Codex's split (`memories/`, `skills/`, `file-search/`, `hooks/`, `cloud-tasks/`, `network-proxy/`, `agent-graph-store/`, …) reflects 18+ months of feature accretion. Loom v0.1 stays single-crate, ~5 modules. We earn splits by encountering compile-time pain, not by anticipating it.
- **`codex exec` vs Loom's worker model.** Codex's `exec` crate is a headless one-shot invocation. Loom's worker is a long-running entity that may issue multiple Claude Code invocations as a phase progresses. Same subprocess idiom, different lifecycle wrapper.
- **No sandbox at v0.1.** Codex ships `linux-sandbox/` (Landlock) and macOS Seatbelt configs to constrain tool execution. Loom v0.1 assumes the user trusts the worker — single-machine + solo + dogfooding. Sandbox is post-v0.1.
- **No MCP at v0.1.** Codex has both an MCP client (to call into other tools) and an experimental MCP server. Loom v0.1 spawns Claude Code via plain CLI args; MCP integration is post-v0.1 and would surface in a `loom-mcp` adjunct crate, not in `loom-rt` core.
- **PARALLEL positioning.** Codex is a sibling to Claude Code (different vendor, same niche). Loom is a sibling to *both* — it does not aim to replace either CLI; it drives them. Our worker abstraction should be trait-shaped so Codex CLI is a future Worker impl, not hardcoded around Claude Code.

## Summary: dependencies cross-reference

| Dep | Loom Step 2 | ccswarm | cosmix/loom | project-orchestrator | codex-rs |
|---|---|---|---|---|---|
| `tokio` | 1.x | 1.40 | — (sync) | 1.49 full | 1 |
| `async-trait` | — (TBD Step 4) | 0.1 | — | 0.1 | 0.1.89 |
| `serde` | 1 | 1.0 | 1 | 1 | 1 |
| `clap` | 4 | 4.5 | 4 | 4.5 | 4 |
| `tracing` | 0.1 | 0.1 | 0.1 | — (unverified) | 0.1.44 |
| `anyhow` | 1 | 1.0 | 1 | 1 | — (unverified) |
| `thiserror` | 1 | 2.0 | 2 | 1 | — (unverified) |
| `nix` | — | — | 0.31 | — | — (unverified) |
| `tokio-util` | — | — | — | 0.7 | 0.7.18 |

> Cells marked "— (unverified)" were `(likely)` in the original Step 3 draft; rather than imply parity that wasn't checked, this revision marks them as not-verified. Step 4 / Step 5 can fill them in if needed; not load-bearing for v0.1.

Step 4 will need to add `async-trait` (for the `Worker` trait). `tokio-util` and `tokio-stream` are flagged as likely Step 5–6 additions (line-buffered stdout streaming, cancellation token plumbing) — not pre-added.
