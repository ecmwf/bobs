use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub data_dir: PathBuf,
    pub page_size: usize,
    pub max_cache_bytes: usize,
    /// Admission limit: maximum number of spools that may concurrently hold an
    /// in-memory page cache (i.e. created but not yet fully read). `create`
    /// blocks until a slot frees, applying backpressure to writers instead of
    /// growing memory without bound. Default: 4096.
    pub max_live_spools: usize,
    pub writer_inactivity_timeout_secs: u64,
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
    pub host_prefix: String,
    pub domain: String,
    pub route_name: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            host: "0.0.0.0".to_string(),
            port: 3000,
            data_dir: PathBuf::from("./data"),
            // 16 MiB pages. BOBS fsyncs a redb metadata commit once per page on
            // write, so a small page (the old 4 KiB) capped writes at ~1.2 MB/s
            // (one fsync per 4 KiB to the PVC). 16 MiB amortises the fsync over
            // 4096x more data (~GB/s ceiling) while keeping per-page durability.
            // max_cache_bytes must be >= page_size; 256 MiB holds 16 pages.
            page_size: 16 * 1024 * 1024,
            max_cache_bytes: 256 * 1024 * 1024,
            max_live_spools: 4096,
            writer_inactivity_timeout_secs: 300,
            read_idle_ttl_secs: 600,
            full_read_complete_ttl_secs: 30,
            reader_done_ttl_secs: 60,
            unread_ttl_secs: 3600,
            cleanup_sweep_interval_secs: 30,
            long_poll_timeout_ms: 25000,
            host_prefix: String::new(),
            domain: String::new(),
            route_name: String::new(),
        }
    }
}

impl Config {
    pub fn from_file(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        serde_yml::from_str(&contents)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    pub fn validate(&self) -> std::io::Result<()> {
        if self.page_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "page_size must be greater than 0",
            ));
        }

        if self.max_cache_bytes < self.page_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "max_cache_bytes must be at least page_size",
            ));
        }

        if self.max_live_spools == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "max_live_spools must be greater than 0",
            ));
        }

        if self.cleanup_sweep_interval_secs == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "cleanup_sweep_interval_secs must be greater than 0",
            ));
        }

        if self.long_poll_timeout_ms == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "long_poll_timeout_ms must be greater than 0",
            ));
        }

        if self.read_idle_ttl_secs == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "read_idle_ttl_secs must be greater than 0",
            ));
        }

        if self.full_read_complete_ttl_secs == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "full_read_complete_ttl_secs must be greater than 0",
            ));
        }

        if self.host_prefix.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "host_prefix must be set in config",
            ));
        }

        if self.domain.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "domain must be set in config",
            ));
        }

        if self.route_name.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "route_name must be set in config",
            ));
        }

        Ok(())
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
        assert_eq!(config.page_size, 16 * 1024 * 1024);
        assert_eq!(config.max_cache_bytes, 256 * 1024 * 1024);
        assert_eq!(config.writer_inactivity_timeout_secs, 300);
        assert_eq!(config.read_idle_ttl_secs, 600);
        assert_eq!(config.full_read_complete_ttl_secs, 30);
        assert_eq!(config.reader_done_ttl_secs, 60);
        assert_eq!(config.unread_ttl_secs, 3600);
        assert_eq!(config.cleanup_sweep_interval_secs, 30);
        assert_eq!(config.long_poll_timeout_ms, 25000);
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
writer_inactivity_timeout_secs: 11
read_idle_ttl_secs: 120
full_read_complete_ttl_secs: 15
reader_done_ttl_secs: 22
unread_ttl_secs: 33
cleanup_sweep_interval_secs: 44
long_poll_timeout_ms: 555
host_prefix: test-prefix
domain: test.example.com
"#,
        )
        .expect("write yaml");

        let cfg = Config::from_file(&path).expect("parse yaml");
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(cfg.port, 9000);
        assert_eq!(cfg.data_dir, PathBuf::from("/tmp/yaml-data"));
        assert_eq!(cfg.page_size, 8192);
        assert_eq!(cfg.max_cache_bytes, 131072);
        assert_eq!(cfg.writer_inactivity_timeout_secs, 11);
        assert_eq!(cfg.read_idle_ttl_secs, 120);
        assert_eq!(cfg.full_read_complete_ttl_secs, 15);
        assert_eq!(cfg.reader_done_ttl_secs, 22);
        assert_eq!(cfg.unread_ttl_secs, 33);
        assert_eq!(cfg.cleanup_sweep_interval_secs, 44);
        assert_eq!(cfg.long_poll_timeout_ms, 555);
        assert_eq!(cfg.host_prefix, "test-prefix");
        assert_eq!(cfg.domain, "test.example.com");
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
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn test_validate_rejects_small_cache() {
        let config = Config {
            max_cache_bytes: 1024,
            page_size: 4096,
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
            ..Config::default()
        };

        let err = config.validate().expect_err("validation should fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn test_validate_rejects_missing_routing_fields() {
        let config = Config::default();
        let err = config.validate().expect_err("validation should fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
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
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
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
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
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
        // New fields get defaults.
        assert_eq!(cfg.read_idle_ttl_secs, 600);
        assert_eq!(cfg.full_read_complete_ttl_secs, 30);
        // Old fields still parsed.
        assert_eq!(cfg.reader_done_ttl_secs, 99);
        assert_eq!(cfg.unread_ttl_secs, 888);
    }

    #[test]
    fn test_from_file_partial_yaml() {
        let tmp = tempdir().expect("tempdir");
        let path = tmp.path().join("partial.yaml");
        std::fs::write(&path, "page_size: 8192\n").expect("write yaml");

        let cfg = Config::from_file(&path).expect("parse yaml");
        assert_eq!(cfg.page_size, 8192);
        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(cfg.port, 3000);
        assert_eq!(cfg.max_cache_bytes, 256 * 1024 * 1024);
    }
}
