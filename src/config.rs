// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use crate::error::{BobsError, Result};
use crate::io::{MAX_IO_URING_IO_LEN, MAX_IO_URING_SHARDS};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

const DEFAULT_PAGE_SIZE: usize = 16 * 1024 * 1024;
const MAX_PAGE_SIZE_POLICY_BYTES: usize = 64 * 1024 * 1024;
/// Operational ceiling for one page and its per-request staging buffer.
///
/// The 64 MiB policy remains below the exported one-SQE io_uring length bound.
pub const MAX_PAGE_SIZE_BYTES: usize =
    if MAX_PAGE_SIZE_POLICY_BYTES <= MAX_IO_URING_IO_LEN {
        MAX_PAGE_SIZE_POLICY_BYTES
    } else {
        MAX_IO_URING_IO_LEN
    };
const DEFAULT_MAX_CACHE_BYTES: usize = 256 * 1024 * 1024;
fn derived_max_live_spools(page_size: usize, max_cache_bytes: usize) -> usize {
    max_cache_bytes.checked_div(page_size).unwrap_or(0).max(1)
}
// The chart's default PVC is 10 GiB; reserve 20% for sidecars and headroom.
const DEFAULT_MAX_SPOOL_BYTES: u64 = 8 * 1024 * 1024 * 1024;
/// Tokio's bounded MPSC channel stores capacity in a semaphore.
pub const MAX_IO_URING_QUEUE_CAPACITY: usize = tokio::sync::Semaphore::MAX_PERMITS;

pub(crate) fn validate_page_size(page_size: usize) -> Result<()> {
    if page_size == 0 {
        return Err(BobsError::ConfigurationError(
            "page_size must be greater than 0".to_string(),
        ));
    }

    if page_size > MAX_PAGE_SIZE_BYTES {
        return Err(BobsError::ConfigurationError(format!(
            "page_size must not exceed {MAX_PAGE_SIZE_BYTES} bytes (64 MiB)"
        )));
    }

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub data_dir: PathBuf,
    pub page_size: usize,
    pub max_cache_bytes: usize,
    /// Maximum number of spools admitted before their first complete read.
    /// When omitted from YAML, this is derived from `max_cache_bytes / page_size`
    /// (with a minimum of one). Explicit operator overrides are preserved.
    pub max_live_spools: usize,
    /// Maximum bytes accepted for one spool across all write requests.
    pub max_spool_bytes: u64,
    /// Maximum time create waits for a live-spool admission slot.
    pub create_admission_timeout_ms: u64,
    pub writer_inactivity_timeout_secs: u64,
    /// Expose the CPU profiler on the main HTTP listener. Disabled by default.
    pub enable_pprof: bool,
    /// Idle TTL (seconds) anchored on the time the spool became readable,
    /// refreshed whenever bytes are actually served. Default: 600.
    pub read_idle_ttl_secs: u64,
    /// Short TTL (seconds) that fires once aggregate read coverage has reached
    /// 100 % of the object's bytes. Default: 30.
    pub full_read_complete_ttl_secs: u64,
    /// Deprecated — kept for config-file backward compatibility only.
    /// No longer drives cleanup logic; use `read_idle_ttl_secs` instead.
    pub reader_done_ttl_secs: u64,
    /// Deprecated — kept for config-file backward compatibility only.
    /// No longer drives cleanup logic; use `read_idle_ttl_secs` instead.
    pub unread_ttl_secs: u64,
    pub cleanup_sweep_interval_secs: u64,
    pub long_poll_timeout_ms: u64,
    /// Explicit Linux ring-pool shard count. `None` uses `(num_cpus / 4).max(1)`;
    /// configured values must be in `1..=MAX_IO_URING_SHARDS`.
    pub io_uring_shards: Option<usize>,
    pub io_uring_queue_capacity: u64,
    pub host_prefix: String,
    pub domain: String,
    pub route_name: String,
    #[serde(default)]
    pub metrics: MetricsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    /// Enable OpenTelemetry metrics export.
    pub enabled: bool,
    /// Bind address for the Prometheus `/metrics` scrape endpoint.
    pub bind_address: String,
    /// Port for the Prometheus `/metrics` scrape endpoint.
    pub port: u16,
    /// Only these label keys are propagated as metric attributes.
    /// If empty, ALL caller-provided labels are propagated.
    pub allowed_labels: Vec<String>,
    /// Maximum length for label values. Values exceeding this are truncated.
    pub max_label_value_length: usize,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        MetricsConfig {
            enabled: false,
            bind_address: "127.0.0.1".to_string(),
            port: 9464,
            allowed_labels: Vec::new(),
            max_label_value_length: 128,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            host: "0.0.0.0".to_string(),
            port: 3000,
            data_dir: PathBuf::from("./data"),
            // 16 MiB pages and a global 256 MiB page cache admit 16 live
            // spools by default. YAML that changes either setting and omits
            // max_live_spools derives a matching admission limit at load time.
            page_size: DEFAULT_PAGE_SIZE,
            max_cache_bytes: DEFAULT_MAX_CACHE_BYTES,
            max_live_spools: derived_max_live_spools(DEFAULT_PAGE_SIZE, DEFAULT_MAX_CACHE_BYTES),
            max_spool_bytes: DEFAULT_MAX_SPOOL_BYTES,
            create_admission_timeout_ms: 5000,
            writer_inactivity_timeout_secs: 300,
            enable_pprof: false,
            read_idle_ttl_secs: 600,
            full_read_complete_ttl_secs: 30,
            reader_done_ttl_secs: 60,
            unread_ttl_secs: 3600,
            cleanup_sweep_interval_secs: 30,
            long_poll_timeout_ms: 25000,
            io_uring_shards: None,
            io_uring_queue_capacity: 1024,
            host_prefix: String::new(),
            domain: String::new(),
            route_name: String::new(),
            metrics: MetricsConfig::default(),
        }
    }
}

impl Config {
    pub fn from_file(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        #[derive(Deserialize)]
        struct AdmissionOverride {
            max_live_spools: Option<usize>,
        }

        let contents = std::fs::read_to_string(path)?;
        let mut config: Config = serde_norway::from_str(&contents)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let admission: AdmissionOverride = serde_norway::from_str(&contents)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        if admission.max_live_spools.is_none() {
            config.max_live_spools =
                derived_max_live_spools(config.page_size, config.max_cache_bytes);
        }
        Ok(config)
    }

    /// Resolve the configured queue capacity to Tokio's platform-sized channel bound.
    pub fn resolved_io_uring_queue_capacity(&self) -> Result<usize> {
        let capacity = usize::try_from(self.io_uring_queue_capacity).map_err(|_| {
            BobsError::ConfigurationError(format!(
                "io_uring_queue_capacity must not exceed {MAX_IO_URING_QUEUE_CAPACITY}"
            ))
        })?;

        if capacity == 0 {
            return Err(BobsError::ConfigurationError(
                "io_uring_queue_capacity must be greater than 0".to_string(),
            ));
        }

        if capacity > MAX_IO_URING_QUEUE_CAPACITY {
            return Err(BobsError::ConfigurationError(format!(
                "io_uring_queue_capacity must not exceed {MAX_IO_URING_QUEUE_CAPACITY}"
            )));
        }

        Ok(capacity)
    }

    pub fn validate(&self) -> Result<()> {
        validate_page_size(self.page_size)?;

        if self.max_live_spools == 0 {
            return Err(BobsError::ConfigurationError(
                "max_live_spools must be greater than 0".to_string(),
            ));
        }

        if self.max_live_spools > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(BobsError::ConfigurationError(format!(
                "max_live_spools must not exceed {}",
                tokio::sync::Semaphore::MAX_PERMITS
            )));
        }

        if self.max_spool_bytes == 0 {
            return Err(BobsError::ConfigurationError(
                "max_spool_bytes must be greater than 0".to_string(),
            ));
        }

        if self.page_size as u64 > self.max_spool_bytes {
            return Err(BobsError::ConfigurationError(
                "page_size must not exceed max_spool_bytes".to_string(),
            ));
        }

        if self.create_admission_timeout_ms == 0 {
            return Err(BobsError::ConfigurationError(
                "create_admission_timeout_ms must be greater than 0".to_string(),
            ));
        }

        if self.writer_inactivity_timeout_secs == 0 {
            return Err(BobsError::ConfigurationError(
                "writer_inactivity_timeout_secs must be greater than 0".to_string(),
            ));
        }

        if self.cleanup_sweep_interval_secs == 0 {
            return Err(BobsError::ConfigurationError(
                "cleanup_sweep_interval_secs must be greater than 0".to_string(),
            ));
        }

        if self.long_poll_timeout_ms == 0 {
            return Err(BobsError::ConfigurationError(
                "long_poll_timeout_ms must be greater than 0".to_string(),
            ));
        }

        if self.io_uring_shards == Some(0) {
            return Err(BobsError::ConfigurationError(
                "io_uring_shards must be greater than 0 when set".to_string(),
            ));
        }

        if self
            .io_uring_shards
            .is_some_and(|shards| shards > MAX_IO_URING_SHARDS)
        {
            return Err(BobsError::ConfigurationError(format!(
                "io_uring_shards must not exceed {MAX_IO_URING_SHARDS} when set"
            )));
        }

        self.resolved_io_uring_queue_capacity()?;

        if self.read_idle_ttl_secs == 0 {
            return Err(BobsError::ConfigurationError(
                "read_idle_ttl_secs must be greater than 0".to_string(),
            ));
        }

        if self.full_read_complete_ttl_secs == 0 {
            return Err(BobsError::ConfigurationError(
                "full_read_complete_ttl_secs must be greater than 0".to_string(),
            ));
        }

        // Cleanup policies are only meaningful when every configured deadline is
        // sampled at least once per interval. A longer sweep would make the stated
        // timeout impossible to honour within one additional configured window.
        let shortest_cleanup_deadline = self
            .writer_inactivity_timeout_secs
            .min(self.read_idle_ttl_secs)
            .min(self.full_read_complete_ttl_secs);
        if self.cleanup_sweep_interval_secs > shortest_cleanup_deadline {
            return Err(BobsError::ConfigurationError(
                "cleanup_sweep_interval_secs must not exceed any active cleanup timeout"
                    .to_string(),
            ));
        }

        if self.host_prefix.is_empty() {
            return Err(BobsError::ConfigurationError(
                "host_prefix must be set in config".to_string(),
            ));
        }

        if self.domain.is_empty() {
            return Err(BobsError::ConfigurationError(
                "domain must be set in config".to_string(),
            ));
        }

        if self.route_name.is_empty() {
            return Err(BobsError::ConfigurationError(
                "route_name must be set in config".to_string(),
            ));
        }

        Ok(())
    }

    /// Filter and truncate caller-provided labels according to config.
    pub fn filter_labels(&self, labels: &HashMap<String, String>) -> HashMap<String, String> {
        let max_len = self.metrics.max_label_value_length;
        let allowed = &self.metrics.allowed_labels;

        labels
            .iter()
            .filter(|(k, _)| allowed.is_empty() || allowed.contains(k))
            .map(|(k, v)| {
                let truncated = if v.len() > max_len {
                    v[..v.floor_char_boundary(max_len)].to_string()
                } else {
                    v.clone()
                };
                (k.clone(), truncated)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_defaults() {
        let config = Config::default();

        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 3000);
        assert_eq!(config.data_dir, PathBuf::from("./data"));
        assert_eq!(config.page_size, DEFAULT_PAGE_SIZE);
        assert_eq!(config.max_cache_bytes, DEFAULT_MAX_CACHE_BYTES);
        assert_eq!(config.max_live_spools, 16);
        assert_eq!(config.max_spool_bytes, DEFAULT_MAX_SPOOL_BYTES);
        assert_eq!(config.create_admission_timeout_ms, 5000);
        assert_eq!(config.writer_inactivity_timeout_secs, 300);
        assert!(!config.enable_pprof);
        assert_eq!(config.read_idle_ttl_secs, 600);
        assert_eq!(config.full_read_complete_ttl_secs, 30);
        assert_eq!(config.reader_done_ttl_secs, 60);
        assert_eq!(config.unread_ttl_secs, 3600);
        assert_eq!(config.cleanup_sweep_interval_secs, 30);
        assert_eq!(config.long_poll_timeout_ms, 25000);
        assert_eq!(config.io_uring_shards, None);
        assert_eq!(config.io_uring_queue_capacity, 1024);
    }

    #[test]
    fn test_from_file_yaml() {
        let tmp = tempdir().expect("tempdir");
        let path = tmp.path().join("bobs.yaml");
        std::fs::write(
            &path,
            r#"host: 127.0.0.1
port: 9000
data_dir: /tmp/yaml-data
page_size: 8192
max_cache_bytes: 131072
max_live_spools: 123
max_spool_bytes: 987654321
create_admission_timeout_ms: 777
enable_pprof: true
writer_inactivity_timeout_secs: 11
read_idle_ttl_secs: 120
full_read_complete_ttl_secs: 15
reader_done_ttl_secs: 22
unread_ttl_secs: 33
cleanup_sweep_interval_secs: 44
long_poll_timeout_ms: 555
io_uring_shards: 7
io_uring_queue_capacity: 2048
host_prefix: test-prefix
domain: test.example.com
route_name: test-route
"#,
        )
        .expect("write yaml");

        let cfg = Config::from_file(&path).expect("parse yaml");
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(cfg.port, 9000);
        assert_eq!(cfg.data_dir, PathBuf::from("/tmp/yaml-data"));
        assert_eq!(cfg.page_size, 8192);
        assert_eq!(cfg.max_cache_bytes, 131072);
        assert_eq!(cfg.max_live_spools, 123);
        assert_eq!(cfg.max_spool_bytes, 987654321);
        assert_eq!(cfg.create_admission_timeout_ms, 777);
        assert!(cfg.enable_pprof);
        assert_eq!(cfg.writer_inactivity_timeout_secs, 11);
        assert_eq!(cfg.read_idle_ttl_secs, 120);
        assert_eq!(cfg.full_read_complete_ttl_secs, 15);
        assert_eq!(cfg.reader_done_ttl_secs, 22);
        assert_eq!(cfg.unread_ttl_secs, 33);
        assert_eq!(cfg.cleanup_sweep_interval_secs, 44);
        assert_eq!(cfg.long_poll_timeout_ms, 555);
        assert_eq!(cfg.io_uring_shards, Some(7));
        assert_eq!(cfg.io_uring_queue_capacity, 2048);
        assert_eq!(cfg.host_prefix, "test-prefix");
        assert_eq!(cfg.domain, "test.example.com");
        assert_eq!(cfg.route_name, "test-route");
    }

    #[test]
    fn test_from_file_missing() {
        let result = Config::from_file("/nonexistent/path/bobs.yaml");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn test_from_file_invalid_yaml() {
        let tmp = tempdir().expect("tempdir");
        let path = tmp.path().join("bad.yaml");
        std::fs::write(&path, "{{{{not: valid: yaml: [[[").expect("write bad yaml");

        let result = Config::from_file(&path);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_validate_rejects_zero_page_size() {
        let config = Config {
            page_size: 0,
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
            ..Config::default()
        };

        let err = config.validate().expect_err("validation should fail");
        assert!(matches!(err, BobsError::ConfigurationError(_)));
    }

    #[test]
    fn test_validate_accepts_maximum_page_size() {
        let config = Config {
            page_size: MAX_PAGE_SIZE_BYTES,
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
            ..Config::default()
        };

        config.validate().expect("maximum page size should pass");
        assert_eq!(MAX_PAGE_SIZE_BYTES, 64 * 1024 * 1024);
        assert!(MAX_PAGE_SIZE_BYTES <= MAX_IO_URING_IO_LEN);
    }

    #[test]
    fn test_validate_rejects_page_size_above_maximum() {
        for page_size in [MAX_PAGE_SIZE_BYTES + 1, usize::MAX] {
            let config = Config {
                page_size,
                host_prefix: "test".into(),
                domain: "example.com".into(),
                route_name: "bobs".into(),
                ..Config::default()
            };

            let err = config.validate().expect_err("oversized page must fail");
            assert!(
                matches!(err, BobsError::ConfigurationError(ref message) if message.contains("page_size must not exceed") && message.contains(&MAX_PAGE_SIZE_BYTES.to_string()))
            );
        }
    }

    #[test]
    fn test_validate_rejects_page_larger_than_spool_limit() {
        let config = Config {
            page_size: 4096,
            max_spool_bytes: 4095,
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
            ..Config::default()
        };

        let err = config
            .validate()
            .expect_err("page larger than spool limit must fail");
        assert!(
            matches!(err, BobsError::ConfigurationError(ref message) if message.contains("page_size") && message.contains("max_spool_bytes"))
        );
    }

    #[test]
    fn test_validate_accepts_cache_smaller_than_page_size() {
        let config = Config {
            max_cache_bytes: 1024,
            page_size: 4096,
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
            ..Config::default()
        };

        config.validate().expect("validation should succeed");
    }

    #[test]
    fn test_validate_accepts_zero_cache_bytes() {
        let config = Config {
            max_cache_bytes: 0,
            page_size: 4096,
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
            ..Config::default()
        };

        config.validate().expect("validation should succeed");
    }

    #[test]
    fn test_validate_rejects_zero_max_live_spools() {
        let config = Config {
            max_live_spools: 0,
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
            ..Config::default()
        };

        let err = config.validate().expect_err("validation should fail");
        assert!(matches!(err, BobsError::ConfigurationError(_)));
    }

    #[test]
    fn test_validate_rejects_max_live_spools_above_semaphore_limit() {
        let config = Config {
            max_live_spools: tokio::sync::Semaphore::MAX_PERMITS + 1,
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
            ..Config::default()
        };

        let err = config.validate().expect_err("validation should fail");
        assert!(matches!(err, BobsError::ConfigurationError(_)));
        assert!(err.to_string().contains("max_live_spools"));
    }

    #[test]
    fn test_validate_rejects_zero_http_resource_limits() {
        for config in [
            Config {
                max_spool_bytes: 0,
                host_prefix: "test".into(),
                domain: "example.com".into(),
                route_name: "bobs".into(),
                ..Config::default()
            },
            Config {
                create_admission_timeout_ms: 0,
                host_prefix: "test".into(),
                domain: "example.com".into(),
                route_name: "bobs".into(),
                ..Config::default()
            },
        ] {
            let err = config.validate().expect_err("validation should fail");
            assert!(matches!(err, BobsError::ConfigurationError(_)));
        }
    }

    #[test]
    fn test_validate_rejects_missing_routing_fields() {
        let config = Config::default();
        let err = config.validate().expect_err("validation should fail");
        assert!(matches!(err, BobsError::ConfigurationError(_)));
    }

    #[test]
    fn test_validate_accepts_valid_config() {
        let config = Config {
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
            ..Config::default()
        };

        config.validate().expect("validation should succeed");
    }

    #[test]
    fn test_validate_rejects_zero_read_idle_ttl() {
        let config = Config {
            read_idle_ttl_secs: 0,
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
            ..Config::default()
        };

        let err = config.validate().expect_err("validation should fail");
        assert!(matches!(err, BobsError::ConfigurationError(_)));
    }

    #[test]
    fn test_validate_rejects_zero_full_read_complete_ttl() {
        let config = Config {
            full_read_complete_ttl_secs: 0,
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
            ..Config::default()
        };

        let err = config.validate().expect_err("validation should fail");
        assert!(matches!(err, BobsError::ConfigurationError(_)));
    }

    #[test]
    fn test_validate_rejects_zero_writer_inactivity_timeout() {
        let config = Config {
            writer_inactivity_timeout_secs: 0,
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
            ..Config::default()
        };

        let err = config.validate().expect_err("validation should fail");
        assert!(matches!(err, BobsError::ConfigurationError(_)));
        assert!(err.to_string().contains("writer_inactivity_timeout_secs"));
    }

    #[test]
    fn test_validate_rejects_cleanup_sweep_longer_than_a_cleanup_deadline() {
        let config = Config {
            cleanup_sweep_interval_secs: 31,
            full_read_complete_ttl_secs: 30,
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
            ..Config::default()
        };

        let err = config.validate().expect_err("validation should fail");
        assert!(matches!(err, BobsError::ConfigurationError(_)));
        assert!(err.to_string().contains("cleanup_sweep_interval_secs"));
    }

    #[test]
    fn config_io_uring_defaults_keep_auto_shards_and_bounded_queue_capacity() {
        let cfg = Config::default();

        assert_eq!(cfg.io_uring_shards, None);
        assert_eq!(cfg.io_uring_queue_capacity, 1024);
    }

    #[test]
    fn config_io_uring_from_file_yaml_parses_explicit_knobs() {
        let tmp = tempdir().expect("tempdir");
        let path = tmp.path().join("io-uring.yaml");
        std::fs::write(
            &path,
            r#"io_uring_shards: 3
io_uring_queue_capacity: 8
host_prefix: x
domain: y
route_name: z
"#,
        )
        .expect("write yaml");

        let cfg = Config::from_file(&path).expect("parse yaml");
        assert_eq!(cfg.io_uring_shards, Some(3));
        assert_eq!(cfg.io_uring_queue_capacity, 8);
        assert_eq!(cfg.host_prefix, "x");
        assert_eq!(cfg.domain, "y");
        assert_eq!(cfg.route_name, "z");
    }

    #[test]
    fn config_io_uring_from_file_yaml_defaults_to_auto_shards_when_omitted() {
        let tmp = tempdir().expect("tempdir");
        let path = tmp.path().join("io-uring-defaults.yaml");
        std::fs::write(
            &path,
            r#"host_prefix: x
domain: y
route_name: z
"#,
        )
        .expect("write yaml");

        let cfg = Config::from_file(&path).expect("parse yaml");
        assert_eq!(cfg.io_uring_shards, None);
        assert_eq!(cfg.io_uring_queue_capacity, 1024);
        assert_eq!(
            cfg.resolved_io_uring_queue_capacity()
                .expect("default queue capacity should be valid"),
            1024
        );
    }

    #[test]
    fn config_io_uring_validate_rejects_zero_shards() {
        let config = Config {
            io_uring_shards: Some(0),
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
            ..Config::default()
        };

        let err = config.validate().expect_err("validation should fail");
        assert!(matches!(err, BobsError::ConfigurationError(_)));
    }

    #[test]
    fn config_io_uring_validate_accepts_maximum_shards() {
        let config = Config {
            io_uring_shards: Some(MAX_IO_URING_SHARDS),
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
            ..Config::default()
        };

        config
            .validate()
            .expect("maximum io_uring shard count should be accepted");
    }

    #[test]
    fn config_io_uring_validate_rejects_oversized_shards() {
        for shards in [MAX_IO_URING_SHARDS + 1, usize::MAX] {
            let config = Config {
                io_uring_shards: Some(shards),
                host_prefix: "test".into(),
                domain: "example.com".into(),
                route_name: "bobs".into(),
                ..Config::default()
            };

            let err = config.validate().expect_err("validation should fail");
            assert!(
                matches!(err, BobsError::ConfigurationError(ref message) if message.contains("io_uring_shards") && message.contains(&MAX_IO_URING_SHARDS.to_string()))
            );
        }
    }

    #[test]
    fn config_io_uring_validate_rejects_zero_queue_capacity() {
        let config = Config {
            io_uring_queue_capacity: 0,
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
            ..Config::default()
        };

        let err = config.validate().expect_err("validation should fail");
        assert!(matches!(err, BobsError::ConfigurationError(_)));
    }

    #[test]
    fn config_io_uring_queue_capacity_accepts_tokio_boundary_and_converts_to_usize() {
        let config = Config {
            io_uring_queue_capacity: MAX_IO_URING_QUEUE_CAPACITY as u64,
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
            ..Config::default()
        };

        config.validate().expect("Tokio channel boundary is valid");
        assert_eq!(
            config
                .resolved_io_uring_queue_capacity()
                .expect("capacity should convert to usize"),
            MAX_IO_URING_QUEUE_CAPACITY
        );
    }

    #[test]
    fn config_io_uring_queue_capacity_rejects_values_above_tokio_boundary() {
        for queue_capacity in [(MAX_IO_URING_QUEUE_CAPACITY as u64) + 1, u64::MAX] {
            let config = Config {
                io_uring_queue_capacity: queue_capacity,
                host_prefix: "test".into(),
                domain: "example.com".into(),
                route_name: "bobs".into(),
                ..Config::default()
            };

            let err = config
                .validate()
                .expect_err("capacity above Tokio's channel limit must fail");
            assert!(
                matches!(err, BobsError::ConfigurationError(ref message) if message.contains("io_uring_queue_capacity") && message.contains(&MAX_IO_URING_QUEUE_CAPACITY.to_string()))
            );
        }
    }

    #[test]
    fn config_io_uring_from_file_parses_u64_max_for_validation() {
        let tmp = tempdir().expect("tempdir");
        let path = tmp.path().join("io-uring-u64-max.yaml");
        std::fs::write(
            &path,
            format!(
                "io_uring_queue_capacity: {}\nhost_prefix: x\ndomain: y\nroute_name: z\n",
                u64::MAX
            ),
        )
        .expect("write yaml");

        let config = Config::from_file(&path).expect("u64::MAX should parse before validation");
        assert_eq!(config.io_uring_queue_capacity, u64::MAX);
        assert!(matches!(
            config.validate(),
            Err(BobsError::ConfigurationError(message))
                if message.contains("io_uring_queue_capacity")
        ));
    }

    #[test]
    fn test_from_file_yaml_old_keys_parse_without_new_keys() {
        // Old YAML without the new keys should parse successfully, using defaults.
        let tmp = tempdir().expect("tempdir");
        let path = tmp.path().join("old.yaml");
        std::fs::write(
            &path,
            r#"reader_done_ttl_secs: 99
unread_ttl_secs: 888
host_prefix: x
domain: y
route_name: z
"#,
        )
        .expect("write yaml");

        let cfg = Config::from_file(&path).expect("parse yaml");
        // Current fields get defaults.
        assert_eq!(cfg.read_idle_ttl_secs, 600);
        assert_eq!(cfg.full_read_complete_ttl_secs, 30);
        assert_eq!(cfg.max_live_spools, 16);
        assert_eq!(cfg.io_uring_shards, None);
        assert_eq!(cfg.io_uring_queue_capacity, 1024);
        // Old fields still parsed.
        assert_eq!(cfg.reader_done_ttl_secs, 99);
        assert_eq!(cfg.unread_ttl_secs, 888);
    }

    #[test]
    fn test_from_file_partial_yaml_derives_admission_from_effective_cache_capacity() {
        let tmp = tempdir().expect("tempdir");
        let path = tmp.path().join("partial.yaml");
        std::fs::write(&path, "page_size: 8192\nmax_cache_bytes: 65536\n").expect("write yaml");

        let cfg = Config::from_file(&path).expect("parse yaml");
        assert_eq!(cfg.page_size, 8192);
        assert_eq!(cfg.max_cache_bytes, 65536);
        assert_eq!(cfg.max_live_spools, 8);
        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(cfg.port, 3000);
        assert_eq!(cfg.io_uring_shards, None);
        assert_eq!(cfg.io_uring_queue_capacity, 1024);
    }

    #[test]
    fn test_from_file_preserves_explicit_admission_override() {
        let tmp = tempdir().expect("tempdir");
        let path = tmp.path().join("explicit-admission.yaml");
        std::fs::write(
            &path,
            "page_size: 4096\nmax_cache_bytes: 1048576\nmax_live_spools: 73\n",
        )
        .expect("write yaml");

        let cfg = Config::from_file(&path).expect("parse yaml");
        assert_eq!(cfg.max_live_spools, 73);
    }

    #[test]
    fn test_from_file_derives_minimum_one_when_cache_cannot_hold_a_page() {
        let tmp = tempdir().expect("tempdir");
        let path = tmp.path().join("uncached.yaml");
        std::fs::write(&path, "page_size: 4096\nmax_cache_bytes: 0\n").expect("write yaml");

        let cfg = Config::from_file(&path).expect("parse yaml");
        assert_eq!(cfg.max_live_spools, 1);
    }

    #[test]
    fn test_from_file_rejects_derived_admission_above_semaphore_limit() {
        let tmp = tempdir().expect("tempdir");
        let path = tmp.path().join("derived-too-large.yaml");
        std::fs::write(
            &path,
            format!(
                "page_size: 1\nmax_cache_bytes: {}\nhost_prefix: x\ndomain: y\nroute_name: z\n",
                tokio::sync::Semaphore::MAX_PERMITS + 1
            ),
        )
        .expect("write yaml");

        let config = Config::from_file(&path).expect("parse yaml");
        assert_eq!(
            config.max_live_spools,
            tokio::sync::Semaphore::MAX_PERMITS + 1
        );
        let err = config
            .validate()
            .expect_err("derived admission above semaphore limit must fail");
        assert!(err.to_string().contains("max_live_spools"));
    }
}
