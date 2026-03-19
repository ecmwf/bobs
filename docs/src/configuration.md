# Configuration

BOBS is configured via a YAML file passed as a CLI argument. All fields have sensible defaults — a partial file is fine, missing fields use defaults.

```bash
./target/release/bobs config.yaml
```

Without a config file, BOBS runs with defaults:

```bash
./target/release/bobs
```

## Fields

| Field | Default | Description |
|-------|---------|-------------|
| `host` | `0.0.0.0` | Address for the HTTP server to bind to. |
| `port` | `3000` | Port for the HTTP server to listen on. |
| `data_dir` | `./data` | File system path for storing spool files. |
| `page_size` | `4096` | Size of internal data pages in bytes. Writes are buffered until a page is full. |
| `max_cache_bytes` | `1048576` | Max bytes to keep in the per-spool memory cache. Effective page count is `max_cache_bytes / page_size`. |
| `writer_inactivity_timeout_secs` | `300` | Cleanup spool if the writer doesn't send data for this long. |
| `reader_done_ttl_secs` | `60` | TTL for a completed spool once the last reader disconnects. |
| `unread_ttl_secs` | `3600` | TTL for a completed spool that has never been read. |
| `cleanup_sweep_interval_secs` | `30` | How often the background cleanup task runs. |
| `long_poll_timeout_ms` | `25000` | Maximum time in ms to wait for new data during a read before redirecting. |
| `bob_id` | `unknown` | Unique ID for this instance. Set to the pod hostname in Kubernetes deployments. |

## Example

```yaml
host: 0.0.0.0
port: 3000
data_dir: /data/bobs
page_size: 4096
max_cache_bytes: 1048576
writer_inactivity_timeout_secs: 300
reader_done_ttl_secs: 60
unread_ttl_secs: 3600
cleanup_sweep_interval_secs: 30
long_poll_timeout_ms: 25000
bob_id: bobs-1
```

Only the fields you want to override need to be present:

```yaml
bob_id: bobs-prod-3
data_dir: /mnt/ssd/bobs
max_cache_bytes: 4194304
```
