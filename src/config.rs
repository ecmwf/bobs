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
    pub writer_inactivity_timeout_secs: u64,
    pub reader_done_ttl_secs: u64,
    pub unread_ttl_secs: u64,
    pub cleanup_sweep_interval_secs: u64,
    pub long_poll_timeout_ms: u64,
    pub bob_id: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            host: "0.0.0.0".to_string(),
            port: 3000,
            data_dir: PathBuf::from("./data"),
            page_size: 4096,
            max_cache_bytes: 1048576,
            writer_inactivity_timeout_secs: 300,
            reader_done_ttl_secs: 60,
            unread_ttl_secs: 3600,
            cleanup_sweep_interval_secs: 30,
            long_poll_timeout_ms: 25000,
            bob_id: "unknown".to_string(),
        }
    }
}

impl Config {
    pub fn from_file(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        serde_yml::from_str(&contents)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
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
        assert_eq!(config.page_size, 4096);
        assert_eq!(config.max_cache_bytes, 1048576);
        assert_eq!(config.writer_inactivity_timeout_secs, 300);
        assert_eq!(config.reader_done_ttl_secs, 60);
        assert_eq!(config.unread_ttl_secs, 3600);
        assert_eq!(config.cleanup_sweep_interval_secs, 30);
        assert_eq!(config.long_poll_timeout_ms, 25000);
        assert_eq!(config.bob_id, "unknown");
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
reader_done_ttl_secs: 22
unread_ttl_secs: 33
cleanup_sweep_interval_secs: 44
long_poll_timeout_ms: 555
bob_id: yaml-bob
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
        assert_eq!(cfg.reader_done_ttl_secs, 22);
        assert_eq!(cfg.unread_ttl_secs, 33);
        assert_eq!(cfg.cleanup_sweep_interval_secs, 44);
        assert_eq!(cfg.long_poll_timeout_ms, 555);
        assert_eq!(cfg.bob_id, "yaml-bob");
    }

    #[test]
    fn test_from_file_partial_yaml() {
        let tmp = tempdir().expect("tempdir");
        let path = tmp.path().join("partial.yaml");
        std::fs::write(&path, "bob_id: partial-bob\npage_size: 8192\n").expect("write yaml");

        let cfg = Config::from_file(&path).expect("parse yaml");
        assert_eq!(cfg.bob_id, "partial-bob");
        assert_eq!(cfg.page_size, 8192);
        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(cfg.port, 3000);
        assert_eq!(cfg.max_cache_bytes, 1048576);
    }
}
