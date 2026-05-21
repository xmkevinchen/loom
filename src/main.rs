//! `loom` binary entry point.
//!
//! Step 5 of plan F-001 wires structured logging only. CLI subcommand
//! dispatch (`loom run`, `loom status`, etc.) is Step 7.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let log_path = init_tracing()?;
    tracing::info!(
        name = "loom-rt",
        version = env!("CARGO_PKG_VERSION"),
        log_file = %log_path.display(),
        "loom runtime starting",
    );
    println!(
        "loom-rt v{} — tracing initialized → {}",
        env!("CARGO_PKG_VERSION"),
        log_path.display()
    );
    Ok(())
}

/// Initialize the global tracing subscriber with a stdout fmt layer and a
/// JSON-line file layer at `.loom/run-<UTC-timestamp>.log`. Returns the log
/// file path so the caller can echo it to the user.
fn init_tracing() -> Result<PathBuf> {
    let log_dir = Path::new(".loom");
    std::fs::create_dir_all(log_dir)
        .with_context(|| format!("create log dir {:?}", log_dir))?;

    let timestamp = utc_timestamp(SystemTime::now());
    let log_path = log_dir.join(format!("run-{timestamp}.log"));

    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .with_context(|| format!("open log file {:?}", log_path))?;

    let stdout_layer = fmt::layer().with_target(false);
    let file_layer = fmt::layer()
        .json()
        .with_target(false)
        .with_writer(file);

    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(stdout_layer)
        .with(file_layer)
        .init();

    Ok(log_path)
}

/// Format `now` as a filesystem-safe UTC timestamp: `YYYYMMDDTHHMMSSZ`.
/// Pure-std implementation (no `chrono` / `time` crate) — civil-from-days
/// algorithm by Howard Hinnant (http://howardhinnant.github.io/date_algorithms.html).
fn utc_timestamp(now: SystemTime) -> String {
    let secs = now
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let time_of_day = secs.rem_euclid(86_400);
    let hour = (time_of_day / 3600) as u32;
    let minute = ((time_of_day % 3600) / 60) as u32;
    let second = (time_of_day % 60) as u32;

    // Days since 1970-01-01 → civil (year, month, day) per Hinnant.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y_offset = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = y_offset + if month <= 2 { 1 } else { 0 };

    format!(
        "{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z"
    )
}

#[cfg(test)]
mod tests {
    use super::utc_timestamp;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn utc_timestamp_epoch() {
        assert_eq!(utc_timestamp(UNIX_EPOCH), "19700101T000000Z");
    }

    #[test]
    fn utc_timestamp_known_moment() {
        // 2024-01-15T12:34:56Z = 1_705_322_096
        let t = UNIX_EPOCH + Duration::from_secs(1_705_322_096);
        assert_eq!(utc_timestamp(t), "20240115T123456Z");
    }

    #[test]
    fn utc_timestamp_leap_year_feb29() {
        // 2024-02-29T00:00:00Z = 1_709_164_800
        let t = UNIX_EPOCH + Duration::from_secs(1_709_164_800);
        assert_eq!(utc_timestamp(t), "20240229T000000Z");
    }
}
