//! Run-state tracking + status.json emission + minimal JSON encoder.
//!
//! Loom v0.1 status file (`.loom/status.json`) shape:
//!
//! ```json
//! {
//!   "schema": 1,
//!   "updated_at_ms": 1715000000000,
//!   "phase": "iteration",
//!   "cycle": 3,
//!   "features": { "F-001": "done", "F-002": "in_progress" }
//! }
//! ```
//!
//! The JSON encoder here is intentionally minimal (no external crate per Step
//! 6 budget) — supports strings, i/u64, bool, null, vec, BTreeMap. Atomic
//! writes go via [`crate::atomic_write::atomic_write`].

use crate::atomic_write::atomic_write;
use anyhow::Result;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// Minimal JSON value type.
#[derive(Debug, Clone)]
pub enum Json {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Str(String),
    Array(Vec<Json>),
    Object(BTreeMap<String, Json>),
}

impl Json {
    pub fn to_string_pretty(&self) -> String {
        let mut out = String::new();
        write_json(&mut out, self, 0, true);
        out
    }

    pub fn to_string_compact(&self) -> String {
        let mut out = String::new();
        write_json(&mut out, self, 0, false);
        out
    }
}

fn write_json(out: &mut String, v: &Json, indent: usize, pretty: bool) {
    match v {
        Json::Null => out.push_str("null"),
        Json::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Json::I64(n) => out.push_str(&n.to_string()),
        Json::U64(n) => out.push_str(&n.to_string()),
        Json::F64(n) => {
            if n.is_finite() {
                out.push_str(&n.to_string());
            } else {
                out.push_str("null");
            }
        }
        Json::Str(s) => {
            out.push('"');
            for c in s.chars() {
                match c {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\t' => out.push_str("\\t"),
                    c if (c as u32) < 0x20 => {
                        out.push_str(&format!("\\u{:04x}", c as u32));
                    }
                    c => out.push(c),
                }
            }
            out.push('"');
        }
        Json::Array(items) => {
            if items.is_empty() {
                out.push_str("[]");
                return;
            }
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                if pretty {
                    out.push('\n');
                    push_indent(out, indent + 1);
                }
                write_json(out, item, indent + 1, pretty);
            }
            if pretty {
                out.push('\n');
                push_indent(out, indent);
            }
            out.push(']');
        }
        Json::Object(map) => {
            if map.is_empty() {
                out.push_str("{}");
                return;
            }
            out.push('{');
            for (i, (k, v)) in map.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                if pretty {
                    out.push('\n');
                    push_indent(out, indent + 1);
                }
                write_json(out, &Json::Str(k.clone()), indent + 1, pretty);
                out.push(':');
                if pretty {
                    out.push(' ');
                }
                write_json(out, v, indent + 1, pretty);
            }
            if pretty {
                out.push('\n');
                push_indent(out, indent);
            }
            out.push('}');
        }
    }
}

fn push_indent(out: &mut String, level: usize) {
    for _ in 0..level {
        out.push_str("  ");
    }
}

/// Snapshot of orchestrator state, persisted to `.loom/status.json` once
/// per iteration cycle.
#[derive(Debug, Default, Clone)]
pub struct StatusSnapshot {
    pub phase: String,
    pub cycle: u64,
    pub features: BTreeMap<String, String>,
}

impl StatusSnapshot {
    pub fn write_to(&self, loom_dir: &Path) -> Result<()> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let mut top = BTreeMap::new();
        top.insert("schema".into(), Json::U64(1));
        top.insert("updated_at_ms".into(), Json::U64(now_ms));
        top.insert("phase".into(), Json::Str(self.phase.clone()));
        top.insert("cycle".into(), Json::U64(self.cycle));
        let mut features = BTreeMap::new();
        for (id, state) in &self.features {
            features.insert(id.clone(), Json::Str(state.clone()));
        }
        top.insert("features".into(), Json::Object(features));
        let json = Json::Object(top).to_string_pretty();
        atomic_write(&loom_dir.join("status.json"), json.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_simple_object() {
        let mut m = BTreeMap::new();
        m.insert("a".to_string(), Json::U64(1));
        m.insert("b".to_string(), Json::Str("hi".into()));
        let s = Json::Object(m).to_string_compact();
        assert_eq!(s, r#"{"a":1,"b":"hi"}"#);
    }

    #[test]
    fn escapes_strings() {
        let s = Json::Str("a\"b\nc".into()).to_string_compact();
        assert_eq!(s, r#""a\"b\nc""#);
    }

    #[test]
    fn status_snapshot_writes_file() {
        let dir = tempfile::tempdir().unwrap();
        let snap = StatusSnapshot {
            phase: "dispatch".into(),
            cycle: 2,
            features: {
                let mut m = BTreeMap::new();
                m.insert("F-001".into(), "done".into());
                m.insert("F-002".into(), "in_progress".into());
                m
            },
        };
        snap.write_to(dir.path()).unwrap();
        let content = std::fs::read_to_string(dir.path().join("status.json")).unwrap();
        assert!(content.contains("\"schema\""));
        assert!(content.contains("\"phase\": \"dispatch\""));
        assert!(content.contains("\"F-001\""));
    }
}
