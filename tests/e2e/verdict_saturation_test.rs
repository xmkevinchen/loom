//! Integration test for AC6.3 — verdict channel saturation contract.
//!
//! AC6.3 specifies: saturate the 256-slot channel by writing 300 review.md
//! events while the receiver is stalled. Assert:
//! (a) no panic, (b) no deadlock within 10s, (c) >=1 `warn!` log containing
//! "channel saturated", (d) watcher thread is still alive (drain a few
//! events post-test, confirm they arrive).
//!
//! Earlier unit tests in `src/verdict.rs` cover the `try_send` channel
//! mechanics in isolation. This test exercises the full spawn path:
//! `watch_verdicts` → notify::RecommendedWatcher → std::thread →
//! `try_send` into the 256-slot tokio channel → warn! on saturation.

use loom_rt::verdict::watch_verdicts;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// MakeWriter that appends every log line into a shared buffer. Used to
/// capture `warn!` output for assertion. Cloneable; each clone writes to the
/// same Arc<Mutex<Vec<u8>>>.
#[derive(Clone)]
struct BufferedWriter(Arc<Mutex<Vec<u8>>>);

impl Write for BufferedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut g = self.0.lock().unwrap();
        g.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for BufferedWriter {
    type Writer = BufferedWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saturation_300_events_no_deadlock_warns_and_recovers() {
    let buf: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let writer = BufferedWriter(buf.clone());

    // Global subscriber required: the verdict watcher spawns a std::thread
    // (verdict.rs:70) whose tracing events would NOT be captured by a
    // thread-local set_default() guard. This test binary contains a single
    // test, so set_global_default is safe (one-time per process).
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(writer)
                .with_ansi(false),
        )
        .init();

    let tmp = tempfile::tempdir().unwrap();
    let features_dir = tmp.path().to_path_buf();

    // Pre-create 300 feature dirs so the saturating writes are pure
    // review.md creates (each parent dir's notify Modify wouldn't fire a
    // verdict-read).
    //
    // F-SAT-POST is also pre-created here (NOT after the burst) for two
    // reasons:
    //   1. Production reality: when Loom dispatches a feature, its
    //      `.ae/features/active/F-X/` already exists; only `review.md` is
    //      the new event. Watching a pre-existing dir matches that flow.
    //   2. Linux inotify race: notify crate's `RecursiveMode::Recursive`
    //      on Linux works by parent-dir mkdir events → async add_watch on
    //      each new sub-dir. A mkdir-then-immediate-write sequence can
    //      win the race vs add_watch completing, dropping the write
    //      event. macOS FSEvents is OS-level recursive and not subject
    //      to this race. The Linux job in CI surfaced exactly this.
    for i in 0..300 {
        std::fs::create_dir_all(features_dir.join(format!("F-SAT-{i:03}"))).unwrap();
    }
    std::fs::create_dir_all(features_dir.join("F-SAT-POST")).unwrap();

    let (guard, mut rx) = watch_verdicts(&features_dir).expect("watch_verdicts");

    // Brief settle so the notify watcher is fully registered before the
    // burst lands. macOS FSEvents in particular need a few ms after
    // watcher.watch() returns before events are reliably delivered.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Saturating burst: write 300 valid review.md files in tight succession.
    // The receiver is held but not polled, so try_send into the 256-slot
    // tokio channel will saturate after ~256 events.
    let start = Instant::now();
    for i in 0..300 {
        let p = features_dir.join(format!("F-SAT-{i:03}")).join("review.md");
        std::fs::write(&p, "---\nverdict: pass\n---\n").unwrap();
    }
    let write_elapsed = start.elapsed();
    assert!(
        write_elapsed < Duration::from_secs(10),
        "300 writes should complete in <10s; took {write_elapsed:?}"
    );

    // Give notify+watcher thread time to drain the std::mpsc and saturate
    // the tokio channel. 1s is generous on macOS FSEvents debounce.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // (b) no deadlock: receive at least 1 event under a hard timeout. If the
    // watcher thread deadlocked, this would time out.
    let first = timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("rx.recv should not time out — would indicate watcher deadlock")
        .expect("rx should yield at least one event before close");
    assert!(
        first.feature_id.starts_with("F-SAT-"),
        "first event should be from saturation burst; got feature_id={}",
        first.feature_id
    );

    // Drain whatever is currently buffered to reduce future back-pressure
    // before the alive-check phase.
    let mut drained = 1usize;
    while let Ok(Some(_)) = timeout(Duration::from_millis(50), rx.recv()).await {
        drained += 1;
        if drained > 600 {
            // belt-and-suspenders cap; shouldn't be reached
            break;
        }
    }
    // Channel cap 256; drained should be <=256+slack. Exact value depends on
    // platform notify debouncing — what we care about is it's bounded.
    assert!(
        drained <= 600,
        "drain should be bounded by channel + slack, got {drained}"
    );

    // (c) saturation warn must have fired at least once.
    let log_bytes = buf.lock().unwrap().clone();
    let log_text = String::from_utf8_lossy(&log_bytes);
    assert!(
        log_text.contains("channel saturated"),
        "expected >=1 'channel saturated' warn in captured log; \
         drained={drained}; got log:\n{log_text}"
    );

    // (d) watcher thread alive: write one fresh review.md AFTER the burst,
    // verify it propagates to the receiver. If the thread died during
    // saturation, this would never arrive.
    //
    // F-SAT-POST/ was pre-created above (see earlier comment block) so this
    // is a pure write into an already-watched directory — same shape as
    // Loom's production review.md-arrival pattern.
    let post = features_dir.join("F-SAT-POST");
    std::fs::write(post.join("review.md"), "---\nverdict: pass\n---\n").unwrap();

    let post_event = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("post-burst event should arrive within 5s — watcher thread is dead");
    let post_event = post_event.expect("rx should yield post-burst event");
    assert_eq!(
        post_event.feature_id, "F-SAT-POST",
        "post-burst event should be F-SAT-POST; watcher thread proved alive"
    );

    // Explicit drop so the watcher tears down before the tempdir is removed.
    drop(guard);
}
