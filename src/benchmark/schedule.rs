// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use super::config::{normalize_endpoint, BenchmarkConfig, Endpoint, EndpointSpec};
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone, serde::Serialize)]
pub struct ObjectPlan {
    pub object_index: usize,
    pub object_label: String,
    pub endpoint: Endpoint,
    pub configured_ordinal: Option<u32>,
    pub object_bytes: u64,
    pub write_request_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct BenchmarkSchedule {
    pub plans: Vec<ObjectPlan>,
    pub barrier_delay: Duration,
    pub barrier_wall_time: SystemTime,
}

pub fn build_schedule(cfg: &BenchmarkConfig) -> Result<BenchmarkSchedule, String> {
    let mut plans = Vec::with_capacity(cfg.objects);
    for i in 0..cfg.objects {
        let (endpoint, ordinal) = match &cfg.endpoint {
            EndpointSpec::Single(e) => (e.clone(), None),
            EndpointSpec::Template { template, ordinals } => {
                let ord = ordinals[i % ordinals.len()];
                (
                    normalize_endpoint(&template.replace("{ordinal}", &ord.to_string()))?,
                    Some(ord),
                )
            }
        };
        plans.push(ObjectPlan {
            object_index: i,
            object_label: format!("{i:06}"),
            endpoint,
            configured_ordinal: ordinal,
            object_bytes: cfg.object_bytes,
            write_request_bytes: cfg.write_request_bytes.unwrap_or(cfg.object_bytes),
        });
    }
    Ok(BenchmarkSchedule {
        plans,
        barrier_delay: cfg.start_delay,
        barrier_wall_time: SystemTime::now() + cfg.start_delay,
    })
}

pub fn extract_ordinal_from_read_url(read_url: &str) -> Option<u32> {
    let after_scheme = read_url
        .split_once("://")
        .map(|(_, r)| r)
        .unwrap_or(read_url);
    let path = after_scheme
        .find('/')
        .map(|i| &after_scheme[i..])
        .unwrap_or(after_scheme);
    for segment in path.split('/') {
        if let Some((_, tail)) = segment.rsplit_once('-') {
            if !tail.is_empty() && tail.chars().all(|c| c.is_ascii_digit()) {
                if let Ok(n) = tail.parse::<u32>() {
                    return Some(n);
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::benchmark::config::{BenchmarkConfig, EndpointSpec};
    #[test]
    fn one_plan_per_object() {
        let c = BenchmarkConfig {
            objects: 4,
            endpoint: EndpointSpec::Single(normalize_endpoint("http://localhost:3000").unwrap()),
            ..Default::default()
        };
        assert_eq!(build_schedule(&c).unwrap().plans.len(), 4);
    }
    #[test]
    fn deterministic_endpoint_assignment() {
        let c = BenchmarkConfig {
            objects: 5,
            endpoint: EndpointSpec::Template {
                template: "http://bobs-{ordinal}:3000".into(),
                ordinals: vec![0, 1],
            },
            ..Default::default()
        };
        let s = build_schedule(&c).unwrap();
        assert_eq!(
            s.plans
                .iter()
                .map(|p| p.configured_ordinal)
                .collect::<Vec<_>>(),
            vec![Some(0), Some(1), Some(0), Some(1), Some(0)]
        );
    }
    #[test]
    fn response_ordinal_extraction() {
        assert_eq!(
            extract_ordinal_from_read_url("https://h/download-3/key"),
            Some(3)
        );
        assert_eq!(
            extract_ordinal_from_read_url("/download-4/api/v1/read/key"),
            Some(4)
        );
        assert_eq!(extract_ordinal_from_read_url("/api/v1/read/key"), None);
    }
}
