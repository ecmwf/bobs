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
| `page_size` | `4096` | Size of internal data pages in bytes. Reader visibility is page-based: a page becomes visible only when it is full, or when `/complete` finalizes a trailing partial page. |
| `max_cache_bytes` | `1048576` | Max bytes to keep in the per-spool memory cache. Effective page count is `max_cache_bytes / page_size`. Increase this when testing larger pages if you want to preserve the number of cached pages. |
| `writer_inactivity_timeout_secs` | `300` | Cleanup spool if the writer doesn't send data for this long. |
| `read_idle_ttl_secs` | `600` | TTL for readable spools that are not actively serving bytes. Starts when the spool becomes readable and refreshes whenever bytes are served. |
| `full_read_complete_ttl_secs` | `30` | Short TTL after BOBS has served every byte of the object at least once, possibly across multiple range requests, and no further bytes have been served. |
| `reader_done_ttl_secs` | `60` | Deprecated compatibility field. Parsed but no longer drives cleanup. |
| `unread_ttl_secs` | `3600` | Deprecated compatibility field. Parsed but no longer drives cleanup. |
| `cleanup_sweep_interval_secs` | `30` | How often the background cleanup task runs. |
| `long_poll_timeout_ms` | `25000` | Maximum time in ms to wait for new data during a read before redirecting. |
| `host_prefix` | `""` | External download host prefix used when generating read URLs. |
| `domain` | `""` | External download domain used when generating read URLs. |
| `route_name` | `""` | External download route prefix, for example `download`. |

## Example

```yaml
host: 0.0.0.0
port: 3000
data_dir: /data/bobs
page_size: 4096
max_cache_bytes: 1048576
writer_inactivity_timeout_secs: 300
read_idle_ttl_secs: 600
full_read_complete_ttl_secs: 30
reader_done_ttl_secs: 60      # deprecated compatibility field
unread_ttl_secs: 3600         # deprecated compatibility field
cleanup_sweep_interval_secs: 30
long_poll_timeout_ms: 25000
host_prefix: polytope-example
domain: example.com
route_name: download
```

Only the fields you want to override need to be present:

```yaml
data_dir: /mnt/ssd/bobs
max_cache_bytes: 4194304
read_idle_ttl_secs: 600
```

## Page size tuning

The default `page_size` is intentionally kept at `4096`. It is a correctness-neutral default and should not be changed just because in-progress metadata commits have been removed from the write hot path.

Larger pages such as `1048576` (1 MiB), `4194304` (4 MiB), and `16777216` (16 MiB) may improve write/read throughput by reducing per-page overhead, but they change streaming behaviour:

- readers do not see in-progress bytes until a full page is available, so wider pages can increase reader-visible latency;
- `max_cache_bytes / page_size` determines the effective cached page count, so wider pages reduce cache reach unless `max_cache_bytes` is increased;
- benchmark representative object sizes and write chunk sizes before changing production defaults.

See the standalone benchmark guide for page-size comparison commands.
