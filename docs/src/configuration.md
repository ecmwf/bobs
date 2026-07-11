<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Configuration

BOBS reads an optional YAML file passed as a CLI argument. Most fields have Rust defaults, so a partial file is fine, but startup validation requires `host_prefix`, `domain`, and `route_name`. The service also requires `HOSTNAME` with a StatefulSet-style numeric ordinal and a non-empty `BOBS_INTERNAL_BASE_URL_TEMPLATE`. The binary therefore does not run from its default configuration alone.

```bash
HOSTNAME=bobs-0 \
BOBS_INTERNAL_BASE_URL_TEMPLATE='http://bobs-{ordinal}:3000/api/v1' \
./target/release/bobs config.yaml
```

Backend selection is build-feature based, not a YAML option: Linux builds without extra features use the `io_uring` FileIO and sidecar metadata backend; builds with `--features tokio-fileio-fallback`, and non-Linux builds, use the Tokio/blocking fallback backend with the same on-disk `<data_dir>/<key>/spool.dat` plus `<data_dir>/<key>/meta.json` layout. These backend settings do not change the HTTP API and do not require an on-disk migration.

Default Linux builds use sharded `io_uring` rings. `io_uring_shards` is optional. If `io_uring_shards` is unset, BOBS resolves it to `max(1, num_cpus / 4)`. File operations are assigned to shards by stable object-key hashing, and each object's data-file operations and metadata sidecar commits are routed to the same shard. CPU pinning is not enabled by default.

The default Linux backend requires Linux 5.11+ because BOBS submits operations against raw file descriptors, and it requires a container/runtime policy that permits `io_uring_setup`. If `io_uring_setup` is blocked, use a runtime seccomp/sysctl policy that permits it or build the fallback binary:

```bash
cargo build --release --bins --features tokio-fileio-fallback
```

## Fields

The table distinguishes Rust defaults from chart overrides where they differ. Other listed defaults apply to both unless the chart's `values.yaml` says otherwise.

| Field | Default | Description |
| ------- | --------- | ------------- |
| `host` | `0.0.0.0` | Address for the HTTP server to bind to. |
| `port` | `3000` | Port for the HTTP server to listen on. |
| `data_dir` | binary: `./data`; chart: `/var/lib/bobs` | File system path for storing spool files. |
| `page_size` | binary: `16777216` (16 MiB); chart: `4096` (4 KiB) | Size of internal data pages in bytes. Reader visibility is page-based: a page becomes visible only when it is full, or when `/complete` finalizes a trailing partial page. |
| `max_cache_bytes` | binary: `268435456` (256 MiB); chart: `1048576` (1 MiB) | Global byte budget for the in-memory page cache across all spools. Set to `0` to disable caching. Pages larger than this cap bypass the cache and remain readable from disk. |
| `max_live_spools` | binary: derived as `max(1, max_cache_bytes / page_size)` (16); chart: `256` | Admission limit for spools not yet fully read. YAML omission derives it from the effective page/cache settings; explicit operator values are preserved. The chart value matches its 1 MiB/4 KiB capacity. |
| `max_spool_bytes` | `8589934592` | Maximum bytes accepted for one spool across write requests. The default leaves headroom on the chart's default 10 GiB volume. |
| `create_admission_timeout_ms` | `5000` | Maximum time `/create` waits for a `max_live_spools` admission slot before returning `503 Service Unavailable`. |
| `writer_inactivity_timeout_secs` | `300` | Cleanup spool if the writer doesn't send data for this long. |
| `enable_pprof` | `false` | Expose `/debug/pprof/profile` on the main listener. Keep disabled except during controlled profiling because profiling consumes CPU and the endpoint is unauthenticated. |
| `read_idle_ttl_secs` | `600` | TTL for readable spools that are not actively serving bytes. Starts when the spool becomes readable and refreshes whenever bytes are served. |
| `full_read_complete_ttl_secs` | `30` | Short TTL after BOBS has served every byte of the object at least once, possibly across multiple range requests, and no further bytes have been served. |
| `reader_done_ttl_secs` | `60` | Deprecated compatibility field. Parsed but no longer drives cleanup. |
| `unread_ttl_secs` | `3600` | Deprecated compatibility field. Parsed but no longer drives cleanup. |
| `cleanup_sweep_interval_secs` | `30` | How often the background cleanup task runs. Must not exceed `writer_inactivity_timeout_secs`, `read_idle_ttl_secs`, or `full_read_complete_ttl_secs`. |
| `long_poll_timeout_ms` | `25000` | Maximum time in ms to wait for new data during a read before redirecting. |
| `io_uring_shards` | unset | Linux default-backend ring-pool shard count. Leave unset to resolve to `max(1, num_cpus / 4)`. Keys are mapped to shards with stable hashing. Must be greater than `0` when set. Ignored by fallback builds. |
| `host_prefix` | `""` | External download host prefix used when generating read URLs. |
| `domain` | `""` | External download domain used when generating read URLs. |
| `route_name` | `""` | External download route prefix, for example `download`. |
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
page_size: 4096
max_cache_bytes: 1048576          # global page-cache byte budget; set to 0 to disable caching
# max_live_spools omitted: derives 256 from this page/cache combination
max_spool_bytes: 8589934592       # 8 GiB per spool
create_admission_timeout_ms: 5000 # return 503 rather than waiting indefinitely
writer_inactivity_timeout_secs: 300
enable_pprof: false               # only enable for controlled, trusted profiling
read_idle_ttl_secs: 600
full_read_complete_ttl_secs: 30
reader_done_ttl_secs: 60      # deprecated compatibility field
unread_ttl_secs: 3600         # deprecated compatibility field
cleanup_sweep_interval_secs: 30
long_poll_timeout_ms: 25000
io_uring_shards: 4              # optional; omit to use max(1, num_cpus / 4) on Linux default backend
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

Only the fields you want to override need to be present:

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

The Rust binary defaults to `16777216` (16 MiB); the Helm chart deliberately overrides this to `4096` (4 KiB) for lower streaming latency. Treat either value as a deployment choice and benchmark representative workloads before changing it.

Larger pages such as `1048576` (1 MiB), `4194304` (4 MiB), and `16777216` (16 MiB) may improve write/read throughput by reducing per-page overhead, but they change streaming behaviour:

- readers do not see in-progress bytes until a full page is available, so wider pages can increase reader-visible latency;
- larger pages consume more of the global cache budget per cached page, so they can reduce cache reach unless `max_cache_bytes` is increased;
- benchmark representative object sizes and write chunk sizes before changing production defaults.

`page_size` does not need to be less than or equal to `max_cache_bytes`. Setting `max_cache_bytes` to `0` disables caching entirely. If a full page is larger than the cache cap, that page simply bypasses the cache while disk-backed reads continue to work. When `max_live_spools` is omitted, BOBS derives it from the effective cache/page ratio with a minimum of one; set it explicitly when workflow concurrency should differ from cache page capacity. Derived and explicit values above Tokio's semaphore limit are rejected during startup validation.

See the standalone benchmark guide for page-size comparison commands.
