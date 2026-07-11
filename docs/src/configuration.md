<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Configuration

BOBS is configured via a YAML file passed as a CLI argument. Missing fields use the binary defaults below, but `host_prefix`, `domain`, and `route_name` must be set to non-empty values for startup validation to succeed. The Helm chart intentionally overrides several binary defaults; those chart values are noted separately.

```bash
./target/release/bobs config.yaml
```

Without a config file, BOBS still loads binary defaults, but startup validation fails until the required routing fields are supplied in a config file.

Backend selection is build-feature based, not a YAML option: Linux builds without extra features use the `io_uring` FileIO and sidecar metadata backend; builds with `--features tokio-fileio-fallback`, and non-Linux builds, use the Tokio/blocking fallback backend with the same on-disk `<data_dir>/<key>/spool.dat` plus `<data_dir>/<key>/meta.json` layout. These backend settings do not change the HTTP API and do not require an on-disk migration.

Default Linux builds use sharded `io_uring` rings. `io_uring_shards` is optional. If `io_uring_shards` is unset, BOBS resolves it to `max(1, num_cpus / 4)`. File operations are assigned to shards by stable object-key hashing, and each object's data-file operations and metadata sidecar commits are routed to the same shard. CPU pinning is not enabled by default.

The default Linux backend requires Linux 5.11+ because BOBS submits operations against raw file descriptors, and it requires a container/runtime policy that permits `io_uring_setup`. If `io_uring_setup` is blocked, use a runtime seccomp/sysctl policy that permits it or build the fallback binary:

```bash
cargo build --release --bins --features tokio-fileio-fallback
```

## Fields

| Field | Binary default | Description |
| ------- | --------- | ------------- |
| `host` | `0.0.0.0` | Address for the HTTP server to bind to. |
| `port` | `3000` | Port for the HTTP server to listen on. |
| `data_dir` | `./data` | File system path for storing spool files. |
| `page_size` | `16777216` (16 MiB) | Size of internal data pages in bytes. Reader visibility is page-based: a page becomes visible only when it is full, or when `/complete` finalizes a trailing partial page. Must be greater than `0`. |
| `max_cache_bytes` | `268435456` (256 MiB) | Global byte budget for the in-memory FIFO page cache across all spools. Set to `0` to disable caching. If an individual page is larger than this cap, that page bypasses the cache and remains readable from disk. |
| `max_live_spools` | `4096` | Maximum spools in the first-read cache phase. Create requests wait for an admission slot when this limit is reached. Must be greater than `0`. |
| `writer_inactivity_timeout_secs` | `300` | Cleanup spool if the writer doesn't send data for this long. |
| `read_idle_ttl_secs` | `600` | TTL for readable spools that are not actively serving bytes. Starts when the spool becomes readable and refreshes whenever bytes are served. Must be greater than `0`. |
| `full_read_complete_ttl_secs` | `30` | Short TTL after BOBS has served every byte of the object at least once, possibly across multiple range requests, and no further bytes have been served. Must be greater than `0`. |
| `reader_done_ttl_secs` | `60` | Deprecated compatibility field. Parsed but no longer drives cleanup; use `read_idle_ttl_secs`. |
| `unread_ttl_secs` | `3600` | Deprecated compatibility field. Parsed but no longer drives cleanup; use `read_idle_ttl_secs`. |
| `cleanup_sweep_interval_secs` | `30` | How often the background cleanup task runs. Must be greater than `0`. |
| `long_poll_timeout_ms` | `25000` | Maximum time in ms to wait for new data during a follow read. Must be greater than `0`. |
| `io_uring_shards` | unset | Linux default-backend ring-pool shard count. Leave unset to resolve to `max(1, num_cpus / 4)`. Keys are mapped to shards with stable hashing. Must be greater than `0` when set. Ignored by fallback builds. |
| `io_uring_queue_capacity` | `1024` | Submission queue capacity for each Linux `io_uring` shard. Must be greater than `0`. Ignored by fallback builds. |
| `host_prefix` | `""` | External download host prefix used when generating read URLs. Must be set to a non-empty value. |
| `domain` | `""` | External download domain used when generating read URLs. Must be set to a non-empty value. |
| `route_name` | `""` | External download route prefix, for example `download`. Must be set to a non-empty value. |
| `metrics.enabled` | `false` | Enable OpenTelemetry metrics export. Requires a build with `--features telemetry`; has no effect without that feature. |
| `metrics.bind_address` | `127.0.0.1` | Bind address for the Prometheus `/metrics` scrape endpoint. Use `0.0.0.0` in Kubernetes so the pod is scrapable. |
| `metrics.port` | `9464` | Port for the Prometheus `/metrics` scrape endpoint (the conventional OTel Prometheus exporter port). Runs on a separate port from the main data port. |
| `metrics.allowed_labels` | `[]` | Caller-provided label keys forwarded as metric attributes. Empty list means all caller labels pass through. Set to a non-empty list to restrict label cardinality. |
| `metrics.max_label_value_length` | `128` | Maximum byte length for label values. Values exceeding this limit are truncated before recording. |

## Example

```yaml
host: 0.0.0.0
port: 3000
data_dir: /data/bobs
page_size: 16777216
max_cache_bytes: 268435456      # global page-cache byte budget; set to 0 to disable caching
max_live_spools: 4096
writer_inactivity_timeout_secs: 300
read_idle_ttl_secs: 600
full_read_complete_ttl_secs: 30
cleanup_sweep_interval_secs: 30
long_poll_timeout_ms: 25000
io_uring_queue_capacity: 1024
host_prefix: polytope-example
domain: example.com
route_name: download
metrics:
  enabled: false         # requires --features telemetry; see Metrics page
  bind_address: "127.0.0.1"  # loopback only; use 0.0.0.0 in k8s
  port: 9464             # separate Prometheus scrape port (OTel convention)
  allowed_labels: []     # empty = all caller labels; set a list to restrict cardinality
  max_label_value_length: 128
```

After the required routing fields are present, only values you want to override need to be added:

```yaml
data_dir: /mnt/ssd/bobs
max_cache_bytes: 4194304
read_idle_ttl_secs: 600
```

## Storage backend selection

There is no config-file field for the storage backend. The binary chooses its backend at compile time:

```bash
# Default Linux build: io_uring FileIO and io_uring sidecar commits.
cargo build --release --bins

# Portable/fallback build: Tokio blocking positional FileIO and sync sidecar commits.
cargo build --release --bins --features tokio-fileio-fallback
```

Both builds use the same config defaults and the same on-disk layout. The fallback build is useful for non-Linux development, Linux kernels older than 5.11, or container/Kubernetes environments where seccomp or sysctl policy blocks `io_uring_setup`.

Linux startup logs include `configured_shards`, `resolved_shards`, and `cpu_pinning_enabled`. `cpu_pinning_enabled` is currently `false` by default.

Future optimizations not implemented in the current backend are `IORING_REGISTER_FILES`, `IORING_REGISTER_BUFFERS`, and `IORING_SETUP_ATTACH_WQ`.

## Page size tuning

The binary default `page_size` is `16777216` (16 MiB), paired with a `268435456`-byte (256 MiB) binary cache default. The Helm chart intentionally uses a lower-latency profile of `page_size: 4096` and `max_cache_bytes: 1048576`; these are chart overrides, not Rust `Config::default()` values.

Page size changes streaming behaviour:

- readers do not see in-progress bytes until a full page is available, so wider pages can increase reader-visible latency;
- larger pages consume more of the global cache budget per cached page, so they can reduce cache reach unless `max_cache_bytes` is increased;
- benchmark representative object sizes and write chunk sizes before changing either deployment profile.

`page_size` does not need to be less than or equal to `max_cache_bytes`. Setting `max_cache_bytes` to `0` disables caching entirely. If a full page is larger than the cache cap, that page simply bypasses the cache while disk-backed reads continue to work.

See the standalone benchmark guide for page-size comparison commands.
