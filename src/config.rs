use std::env;
use std::path::PathBuf;

/// Configuration for BOBS, loaded from environment variables.
///
/// All configuration is read from `BOBS_*` environment variables with sensible defaults.
/// This allows BOBS to be deployed in containerized environments (k8s) without config files.
#[derive(Debug, Clone)]
pub struct Config {
    /// Listen address for the HTTP server.
    /// Env: `BOBS_LISTEN_ADDR`, Default: `"0.0.0.0:3000"`
    pub listen_addr: String,

    /// Directory for storing spool data files.
    /// Env: `BOBS_DATA_DIR`, Default: `"./data"`
    pub data_dir: PathBuf,

    /// Page size in bytes for spool storage.
    /// Env: `BOBS_PAGE_SIZE`, Default: `4096`
    pub page_size: usize,

    /// Maximum number of pages to cache in memory per spool.
    /// Env: `BOBS_PAGE_CACHE_CAPACITY`, Default: `256`
    pub page_cache_capacity: usize,

    /// Timeout in seconds for writer inactivity before cleanup.
    /// Env: `BOBS_WRITER_INACTIVITY_TIMEOUT_SECS`, Default: `300`
    pub writer_inactivity_timeout_secs: u64,

    /// TTL in seconds for reader completion state.
    /// Env: `BOBS_READER_DONE_TTL_SECS`, Default: `60`
    pub reader_done_ttl_secs: u64,

    /// TTL in seconds for unread spools before cleanup.
    /// Env: `BOBS_UNREAD_TTL_SECS`, Default: `3600`
    pub unread_ttl_secs: u64,

    /// Interval in seconds for cleanup sweep operations.
    /// Env: `BOBS_CLEANUP_SWEEP_INTERVAL_SECS`, Default: `30`
    pub cleanup_sweep_interval_secs: u64,

    /// Unique identifier for this BOBS instance (pod hostname in k8s).
    /// Env: `BOBS_BOB_ID`, Default: system hostname or `"unknown"`
    pub bob_id: String,
}

impl Config {
    /// Load configuration from environment variables.
    ///
    /// All `BOBS_*` environment variables are read with documented defaults.
    /// Invalid values fall back to defaults rather than panicking.
    pub fn from_env() -> Self {
        Config {
            listen_addr: env::var("BOBS_LISTEN_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:3000".to_string()),
            data_dir: PathBuf::from(
                env::var("BOBS_DATA_DIR").unwrap_or_else(|_| "./data".to_string()),
            ),
            page_size: env::var("BOBS_PAGE_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(4096),
            page_cache_capacity: env::var("BOBS_PAGE_CACHE_CAPACITY")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(256),
            writer_inactivity_timeout_secs: env::var("BOBS_WRITER_INACTIVITY_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300),
            reader_done_ttl_secs: env::var("BOBS_READER_DONE_TTL_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(60),
            unread_ttl_secs: env::var("BOBS_UNREAD_TTL_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3600),
            cleanup_sweep_interval_secs: env::var("BOBS_CLEANUP_SWEEP_INTERVAL_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30),
            bob_id: env::var("BOBS_BOB_ID")
                .unwrap_or_else(|_| env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_defaults() {
        // Clear any BOBS_* env vars that might be set
        // (In a real test, we'd use a test harness to isolate env vars)
        // For now, just verify that from_env() returns a valid Config with expected defaults
        let config = Config::from_env();

        // Verify all defaults are present
        assert_eq!(config.listen_addr, "0.0.0.0:3000");
        assert_eq!(config.data_dir, PathBuf::from("./data"));
        assert_eq!(config.page_size, 4096);
        assert_eq!(config.page_cache_capacity, 256);
        assert_eq!(config.writer_inactivity_timeout_secs, 300);
        assert_eq!(config.reader_done_ttl_secs, 60);
        assert_eq!(config.unread_ttl_secs, 3600);
        assert_eq!(config.cleanup_sweep_interval_secs, 30);
        // bob_id should be either HOSTNAME or "unknown"
        assert!(!config.bob_id.is_empty());
    }

    #[test]
    #[serial]
    fn test_overrides() {
        // Set environment variables for this test
        // SAFETY: This is a test function, not concurrent with other tests
        unsafe {
            env::set_var("BOBS_LISTEN_ADDR", "127.0.0.1:8080");
            env::set_var("BOBS_DATA_DIR", "/tmp/data");
            env::set_var("BOBS_PAGE_SIZE", "8192");
            env::set_var("BOBS_PAGE_CACHE_CAPACITY", "512");
            env::set_var("BOBS_WRITER_INACTIVITY_TIMEOUT_SECS", "600");
            env::set_var("BOBS_READER_DONE_TTL_SECS", "120");
            env::set_var("BOBS_UNREAD_TTL_SECS", "7200");
            env::set_var("BOBS_CLEANUP_SWEEP_INTERVAL_SECS", "60");
            env::set_var("BOBS_BOB_ID", "test-pod-1");
        }

        let config = Config::from_env();

        // Verify overrides are applied
        assert_eq!(config.listen_addr, "127.0.0.1:8080");
        assert_eq!(config.data_dir, PathBuf::from("/tmp/data"));
        assert_eq!(config.page_size, 8192);
        assert_eq!(config.page_cache_capacity, 512);
        assert_eq!(config.writer_inactivity_timeout_secs, 600);
        assert_eq!(config.reader_done_ttl_secs, 120);
        assert_eq!(config.unread_ttl_secs, 7200);
        assert_eq!(config.cleanup_sweep_interval_secs, 60);
        assert_eq!(config.bob_id, "test-pod-1");

        // Clean up env vars
        // SAFETY: This is a test function, not concurrent with other tests
        unsafe {
            env::remove_var("BOBS_LISTEN_ADDR");
            env::remove_var("BOBS_DATA_DIR");
            env::remove_var("BOBS_PAGE_SIZE");
            env::remove_var("BOBS_PAGE_CACHE_CAPACITY");
            env::remove_var("BOBS_WRITER_INACTIVITY_TIMEOUT_SECS");
            env::remove_var("BOBS_READER_DONE_TTL_SECS");
            env::remove_var("BOBS_UNREAD_TTL_SECS");
            env::remove_var("BOBS_CLEANUP_SWEEP_INTERVAL_SECS");
            env::remove_var("BOBS_BOB_ID");
        }
    }
}
