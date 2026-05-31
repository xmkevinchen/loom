//! `loom-rt` library root.
//!
//! Module skeleton seeded at Step 2 of plan F-001. Each module body is filled
//! in by later plan steps. See `.ae/features/active/F-001-build-loom-v0-1-ai-agent-orchestrator-em/plan.md`.

pub mod artifact;
pub mod atomic_write;
pub mod cli;
pub mod delivery;
pub mod discovery;
pub mod dispatch;
pub mod feature_id;
pub mod iteration;
pub mod spawn_env;
pub mod state;
pub mod verdict;
pub mod worker;
pub mod worker_claude_code;
