//! The `Worker` trait. One adapter per backend (Step 4 ships ClaudeCodeAdapter;
//! Codex / Gemini / oMLX adapters are v0.2+).

use crate::artifact::{Artifact, FeatureSpec};
use anyhow::Result;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

/// Backend-agnostic worker interface.
///
/// `async-trait` is used because v0.1's dispatch keeps `Vec<Box<dyn Worker>>`
/// to pick from at runtime; native `async fn` in trait is not yet `dyn`-safe.
/// Implementors must be `Send + Sync` so workers can be invoked across
/// `tokio::task::spawn` boundaries in Step 6's scheduler.
///
/// The `cancel` token is the dispatcher's pre-emption hook: when triggered,
/// the worker MUST stop work + return a `Cancelled` verdict promptly. This
/// parameter is on the trait from Step 4 to avoid a breaking API change in
/// Step 6 when the scheduler grows fail-fast / pipeline-cancellation features
/// (Track 1 architecture review).
#[async_trait]
pub trait Worker: Send + Sync {
    async fn run(&self, spec: FeatureSpec, cancel: CancellationToken) -> Result<Artifact>;
}
