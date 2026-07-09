// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use serde::Serialize;
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ObjectResult {
    pub object_index: usize,
    pub object_label: String,
    pub endpoint_url: String,
    pub configured_ordinal: Option<u32>,
    pub response_ordinal: Option<u32>,
    pub key: Option<String>,
    pub outcome: String,
    pub error: Option<String>,
    pub create_ms: Option<f64>,
    pub write_ms: Option<f64>,
    pub complete_ms: Option<f64>,
    pub wait_to_read_ms: Option<f64>,
    pub read_ms: Option<f64>,
    pub lifecycle_ms: f64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    #[serde(skip)]
    pub write_start_ms: Option<f64>,
    #[serde(skip)]
    pub write_end_ms: Option<f64>,
    #[serde(skip)]
    pub read_start_ms: Option<f64>,
    #[serde(skip)]
    pub read_end_ms: Option<f64>,
}

impl ObjectResult {
    pub fn failed(
        index: usize,
        label: String,
        endpoint_url: String,
        configured_ordinal: Option<u32>,
        lifecycle: Duration,
        err: String,
    ) -> Self {
        Self {
            object_index: index,
            object_label: label,
            endpoint_url,
            configured_ordinal,
            response_ordinal: None,
            key: None,
            outcome: "failure".into(),
            error: Some(err),
            create_ms: None,
            write_ms: None,
            complete_ms: None,
            wait_to_read_ms: None,
            read_ms: None,
            lifecycle_ms: ms(lifecycle),
            bytes_written: 0,
            bytes_read: 0,
            write_start_ms: None,
            write_end_ms: None,
            read_start_ms: None,
            read_end_ms: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Percentiles {
    pub p50_ms: Option<f64>,
    pub p95_ms: Option<f64>,
    pub max_ms: Option<f64>,
}
#[derive(Debug, Clone, Serialize)]
pub struct TimingSummary {
    pub create: Percentiles,
    pub write: Percentiles,
    pub complete: Percentiles,
    pub wait_to_read: Percentiles,
    pub read: Percentiles,
    pub lifecycle: Percentiles,
}
#[derive(Debug, Clone, Serialize)]
pub struct OrdinalSummary {
    pub objects: usize,
    pub successes: usize,
    pub failures: usize,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub write_active_ms: Option<f64>,
    pub read_active_ms: Option<f64>,
    pub write_active_mib_s: Option<f64>,
    pub read_active_mib_s: Option<f64>,
}
#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkSummary {
    pub total_objects: usize,
    pub successes: usize,
    pub failures: usize,
    pub total_bytes_written: u64,
    pub total_bytes_read: u64,
    pub wall_ms: f64,
    pub wall_mib_s: f64,
    pub write_active_ms: Option<f64>,
    pub write_active_mib_s: Option<f64>,
    pub read_active_ms: Option<f64>,
    pub read_active_mib_s: Option<f64>,
    pub read_mib_per_reader_second: Option<f64>,
    pub timings: TimingSummary,
    pub per_ordinal: BTreeMap<String, OrdinalSummary>,
    pub results: Vec<ObjectResult>,
}

pub fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}
pub fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

pub fn summarize(mut results: Vec<ObjectResult>, wall: Duration) -> BenchmarkSummary {
    results.sort_by_key(|r| r.object_index);
    let successes = results.iter().filter(|r| r.outcome == "success").count();
    let failures = results.len() - successes;
    let total_bytes_written = results
        .iter()
        .filter(|r| r.outcome == "success")
        .map(|r| r.bytes_written)
        .sum();
    let total_bytes_read = results
        .iter()
        .filter(|r| r.outcome == "success")
        .map(|r| r.bytes_read)
        .sum();
    let wall_ms = ms(wall);
    let (write_active_ms, write_active_mib_s) = active_window(&results, true);
    let (read_active_ms, read_active_mib_s) = active_window(&results, false);
    let read_secs: f64 = results
        .iter()
        .filter(|r| r.outcome == "success")
        .filter_map(|r| r.read_ms)
        .map(|m| m / 1000.0)
        .sum();
    let read_mib_per_reader_second = if read_secs > 0.0 {
        Some(bytes_to_mib(total_bytes_read) / read_secs)
    } else {
        None
    };
    let per_ordinal = per_ordinal(&results);
    BenchmarkSummary {
        total_objects: results.len(),
        successes,
        failures,
        total_bytes_written,
        total_bytes_read,
        wall_ms,
        wall_mib_s: rate(total_bytes_read, wall_ms),
        write_active_ms,
        write_active_mib_s,
        read_active_ms,
        read_active_mib_s,
        read_mib_per_reader_second,
        timings: TimingSummary {
            create: percentiles(results.iter().filter_map(|r| r.create_ms).collect()),
            write: percentiles(results.iter().filter_map(|r| r.write_ms).collect()),
            complete: percentiles(results.iter().filter_map(|r| r.complete_ms).collect()),
            wait_to_read: percentiles(results.iter().filter_map(|r| r.wait_to_read_ms).collect()),
            read: percentiles(results.iter().filter_map(|r| r.read_ms).collect()),
            lifecycle: percentiles(results.iter().map(|r| r.lifecycle_ms).collect()),
        },
        per_ordinal,
        results,
    }
}

fn per_ordinal(results: &[ObjectResult]) -> BTreeMap<String, OrdinalSummary> {
    let mut groups: BTreeMap<String, Vec<&ObjectResult>> = BTreeMap::new();
    for r in results {
        let key = r
            .response_ordinal
            .or(r.configured_ordinal)
            .map(|o| o.to_string())
            .unwrap_or_else(|| "unknown".into());
        groups.entry(key).or_default().push(r);
    }
    groups
        .into_iter()
        .map(|(k, v)| {
            let successes = v.iter().filter(|r| r.outcome == "success").count();
            let failures = v.len() - successes;
            let bytes_written = v
                .iter()
                .filter(|r| r.outcome == "success")
                .map(|r| r.bytes_written)
                .sum();
            let bytes_read = v
                .iter()
                .filter(|r| r.outcome == "success")
                .map(|r| r.bytes_read)
                .sum();
            let (wms, wr) = active_window_refs(&v, true);
            let (rms, rr) = active_window_refs(&v, false);
            (
                k,
                OrdinalSummary {
                    objects: v.len(),
                    successes,
                    failures,
                    bytes_written,
                    bytes_read,
                    write_active_ms: wms,
                    read_active_ms: rms,
                    write_active_mib_s: wr,
                    read_active_mib_s: rr,
                },
            )
        })
        .collect()
}

pub fn percentiles(mut vals: Vec<f64>) -> Percentiles {
    if vals.is_empty() {
        return Percentiles {
            p50_ms: None,
            p95_ms: None,
            max_ms: None,
        };
    }
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pick = |p: f64| -> f64 {
        let idx = ((vals.len() as f64 - 1.0) * p).ceil() as usize;
        vals[idx]
    };
    Percentiles {
        p50_ms: Some(pick(0.50)),
        p95_ms: Some(pick(0.95)),
        max_ms: vals.last().copied(),
    }
}

fn active_window(results: &[ObjectResult], write: bool) -> (Option<f64>, Option<f64>) {
    active_window_refs(&results.iter().collect::<Vec<_>>(), write)
}
fn active_window_refs(results: &[&ObjectResult], write: bool) -> (Option<f64>, Option<f64>) {
    let mut min_start: Option<f64> = None;
    let mut max_end: Option<f64> = None;
    let mut bytes = 0u64;
    for r in results.iter().filter(|r| r.outcome == "success") {
        let (s, e, b) = if write {
            (r.write_start_ms, r.write_end_ms, r.bytes_written)
        } else {
            (r.read_start_ms, r.read_end_ms, r.bytes_read)
        };
        if let (Some(s), Some(e)) = (s, e) {
            min_start = Some(min_start.map_or(s, |m| m.min(s)));
            max_end = Some(max_end.map_or(e, |m| m.max(e)));
            bytes += b;
        }
    }
    match (min_start, max_end) {
        (Some(s), Some(e)) if e > s => {
            let win = e - s;
            (Some(win), Some(rate(bytes, win)))
        }
        _ => (None, None),
    }
}
fn bytes_to_mib(b: u64) -> f64 {
    b as f64 / 1024.0 / 1024.0
}
fn rate(bytes: u64, ms: f64) -> f64 {
    if ms > 0.0 {
        bytes_to_mib(bytes) / (ms / 1000.0)
    } else {
        0.0
    }
}

pub fn render_summary_line(summary: &BenchmarkSummary) -> String {
    format!(
        "SUMMARY:{}",
        serde_json::to_string(summary).expect("summary json")
    )
}
pub fn render_human(summary: &BenchmarkSummary) -> String {
    format!(
        "objects={} successes={} failures={} bytes_read={} wall_mib_s={:.3}",
        summary.total_objects,
        summary.successes,
        summary.failures,
        summary.total_bytes_read,
        summary.wall_mib_s
    )
}

pub fn log_line(
    object_label: &str,
    key: Option<&str>,
    ordinal: Option<u32>,
    event: &str,
    extra: &str,
) -> String {
    format!(
        "object={} key={} ordinal={} event={} unix_ms={}{}{}",
        object_label,
        key.unwrap_or("-"),
        ordinal
            .map(|o| o.to_string())
            .unwrap_or_else(|| "unknown".into()),
        event,
        unix_ms(),
        if extra.is_empty() { "" } else { " " },
        extra
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ok(i: usize, ord: Option<u32>) -> ObjectResult {
        ObjectResult {
            object_index: i,
            object_label: format!("{i:06}"),
            endpoint_url: "http://x".into(),
            configured_ordinal: ord,
            response_ordinal: None,
            key: Some("k".into()),
            outcome: "success".into(),
            error: None,
            create_ms: Some(1.0),
            write_ms: Some(10.0 + i as f64),
            complete_ms: Some(2.0),
            wait_to_read_ms: Some(0.0),
            read_ms: Some(20.0),
            lifecycle_ms: 40.0,
            bytes_written: 1024 * 1024,
            bytes_read: 1024 * 1024,
            write_start_ms: Some(i as f64),
            write_end_ms: Some(10.0 + i as f64),
            read_start_ms: Some(20.0 + i as f64),
            read_end_ms: Some(40.0 + i as f64),
        }
    }
    #[test]
    fn percentile_calculation_and_empty() {
        assert_eq!(percentiles(vec![]).p50_ms, None);
        assert_eq!(percentiles(vec![1.0, 2.0, 3.0]).p50_ms, Some(2.0));
    }
    #[test]
    fn summary_excludes_failed_bytes_and_groups() {
        let mut f = ok(1, Some(1));
        f.outcome = "failure".into();
        f.bytes_read = 99;
        let s = summarize(vec![ok(0, Some(0)), f], Duration::from_secs(1));
        assert_eq!(s.successes, 1);
        assert_eq!(s.total_bytes_read, 1024 * 1024);
        assert!(s.per_ordinal.contains_key("0"));
    }
    #[test]
    fn stable_json_fields() {
        let s = summarize(vec![ok(0, Some(0))], Duration::from_secs(1));
        let j = serde_json::to_value(&s).unwrap();
        assert!(j.get("timings").unwrap().get("read").is_some());
        assert!(render_summary_line(&s).starts_with("SUMMARY:"));
    }
    #[test]
    fn log_line_fields() {
        let l = log_line(
            "000001",
            Some("k"),
            Some(2),
            "create_end",
            "status=201 duration_ms=1",
        );
        assert!(l.contains("object=000001"));
        assert!(l.contains("event=create_end"));
    }
}
