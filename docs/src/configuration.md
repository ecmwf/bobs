<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Configuration

BOBS reads an optional YAML file. Most fields have Rust defaults, but startup validation requires non-empty `host_prefix`, `domain`, and `route_name` values. The service also requires `HOSTNAME` with a StatefulSet-style numeric ordinal and a non-empty `BOBS_INTERNAL_BASE_URL_TEMPLATE`; the binary therefore cannot start from its default configuration alone.

```bash
HOSTNAME=bobs-0 \
BOBS_INTERNAL_BASE_URL_TEMPLATE='http://bobs-{ordinal}:3000/api/v1' \
  ./target/release/bobs config.yaml
```

Backend selection is build-feature based, not a YAML option: Linux builds without extra features use the `io_uring` FileIO and sidecar metadata backend; builds with `--features tokio-fileio-fallback`, and non-Linux builds, use the Tokio/blocking fallback backend with the same on-disk `<data_dir>/<key>/spool.dat` plus `<data_dir>/<key>/meta.json` layout. These backend settings do not change the HTTP API and do not require an on-disk migration.

Default Linux builds use sharded `io_uring` rings. `io_uring_shards` is optional and accepts explicit values from `1` through `256`, inclusive. If unset, BOBS resolves it to `max(1, num_cpus / 4)`. File operations are assigned to shards by stable object-key hashing, and each object's data-file operations and metadata sidecar commits are routed to the same shard. CPU pinning is not enabled by default. A read or write submitted as one SQE is limited to `u32::MAX` bytes (`4294967295`); oversized operations fail before routing or submission.

The default Linux backend requires Linux 5.11+ because BOBS submits operations against raw file descriptors, and it requires a container/runtime policy that permits `io_uring_setup`. If `io_uring_setup` is blocked, use a runtime seccomp/sysctl policy that permits it or build the fallback binary:

```bash
cargo build --release --bins --features tokio-fileio-fallback
```

## Fields

The table distinguishes Rust defaults from chart overrides where they differ.

| Field | Default | Description |
| ------- | --------- | ------------- |
| `host` | `0.0.0.0` | Address for the HTTP server to bind to. |
| `port` | `3000` | Port for the HTTP server to listen on. |
| `data_dir` | binary: `./data`; chart: `/var/lib/bobs` | File system path for storing spool files. The chart requires a non-empty absolute path because it mounts this value as a directory and rejects values resolving to the fixed `/etc/bobs` config mount or overlapping `/etc/bobs/config.yaml`; the binary also supports relative paths. |
| `page_size` | binary: `16777216` (16 MiB); chart: `4096` (4 KiB) | Size of internal data pages. Valid range: `1..=67108864` (64 MiB), and it must not exceed `max_spool_bytes`. A page becomes visible only when full; `/api/v1/complete/{key}` publishes a trailing partial page. |
| `max_cache_bytes` | binary: `268435456` (256 MiB); chart: `1048576` (1 MiB) | Global budget for the logical bytes of bounded cache-owned page allocations across all spools, excluding allocator overhead. It also derives the read-response permit budget as `max(1, floor(max_cache_bytes / page_size))`; weighted permits are held until response bodies are dropped. Cache insertion may copy a page solely to avoid retaining an oversized transport-frame backing; the frame-to-disk path remains zero-copy. Set to `0` to disable caching while retaining a one-page response bound. A page rejected because it exceeds the cap likewise bypasses the cache without an isolation copy and remains readable from disk. |
| `max_live_spools` | binary: derived as `max(1, max_cache_bytes / page_size)` (`16`); chart: explicit `256` | Admission limit for spools in the first-read cache phase and for startup recovery. Valid range: `1..=65536`; the bound applies to explicit and derived values. YAML omission derives it from effective page/cache settings. The first proven full-object read frees that spool's cache and admission slot while leaving it readable from disk. Startup uses a preferred candidate and handle set whose capacity is exactly this limit, and leaves excess entries unchanged for a later restart with more capacity. |
| `max_spool_bytes` | `8589934592` (8 GiB) | Maximum bytes accepted for one spool across write requests and maximum canonical payload length admitted during startup recovery. An over-limit recovered payload is quarantined unchanged before open or metadata migration. Must be greater than `0` and at least `page_size`. |
| `create_admission_timeout_ms` | `5000` | Maximum time `/api/v1/create` waits for a `max_live_spools` slot before returning `503 Service Unavailable`. Must be greater than `0`. |
| `writer_inactivity_timeout_secs` | `300` | Cleanup timeout for an unfinished spool whose writer has stopped sending data. Must be greater than `0`. |
| `enable_pprof` | `false` | Exposes unauthenticated `/debug/pprof/profile` on the main listener. Enable only for controlled profiling. |
| `read_idle_ttl_secs` | `600` | TTL for readable spools with no served bytes, anchored when they become readable and refreshed on read progress. Must be greater than `0`. |
| `full_read_complete_ttl_secs` | `30` | Short TTL after bounded coverage tracking proves every byte was served, refreshed by subsequent read activity. Adjacent and overlapping ranges coalesce. If fragmentation exceeds the interval cap, BOBS retains first-read admission and uses the idle TTL until a later completed contiguous full-object response proves coverage exactly. Must be greater than `0`. |
| `reader_done_ttl_secs` | `60` | Deprecated compatibility field. Parsed but ignored by cleanup; use `read_idle_ttl_secs`. |
| `unread_ttl_secs` | `3600` | Deprecated compatibility field. Parsed but ignored by cleanup; use `read_idle_ttl_secs`. |
| `cleanup_sweep_interval_secs` | `30` | Cleanup scan interval. Must be greater than `0` and must not exceed any active cleanup timeout. |
| `long_poll_timeout_ms` | `25000` | Maximum wait for new data during a follow read before redirecting. Must be greater than `0`. |
| `io_uring_shards` | unset | Linux ring-pool shard count. Omission resolves to `max(1, num_cpus / 4)`; explicit values must be between `1` and `256`, inclusive. Invalid values fail startup, including in fallback builds; fallback I/O otherwise ignores the setting. |
| `io_uring_queue_capacity` | `1024` | Bounded submission queue capacity for each Linux `io_uring` shard. Submitters wait when the queue is full, applying backpressure instead of growing an unbounded backlog. Must be between `1` and Tokio's `Semaphore::MAX_PERMITS` (`usize::MAX >> 3`): `2305843009213693951` on 64-bit targets or `536870911` on 32-bit targets. Invalid values fail startup with `ConfigurationError`, including in fallback builds; otherwise fallback I/O ignores this setting. |
| `host_prefix` | `""` | External download host prefix used in `read_url`. Must be non-empty. |
| `domain` | `""` | External download domain used in `read_url`. Must be non-empty. |
| `route_name` | `""` | External download route prefix. Must be non-empty. The chart requires one 1-63 character segment containing only ASCII letters, digits, `_`, or `-`, starting and ending with an alphanumeric character. |
| `metrics.enabled` | `false` | Enable OpenTelemetry metrics export. Requires a build with `--features telemetry`; has no effect without that feature. |
| `metrics.bind_address` | `127.0.0.1` | Bind address for the Prometheus `/metrics` scrape endpoint. Use `0.0.0.0` in Kubernetes so the pod is scrapable. |
| `metrics.port` | `9464` | Port for the Prometheus `/metrics` scrape endpoint (the conventional OTel Prometheus exporter port). When telemetry and metrics are enabled, this must differ from the main HTTP `port`; startup rejects a collision before either listener is started. |
| `metrics.allowed_labels` | `[]` | Caller-provided label keys forwarded as metric attributes. Empty list means all caller labels pass through. Set to a non-empty list to restrict label cardinality. |
| `metrics.max_label_value_length` | `128` | Maximum byte length for label values. Values exceeding this limit are truncated before recording. |

Recovery also has fixed metadata safety bounds. `meta.json` is limited to 1 MiB and its open-file size is checked before allocation. The directory scan uses a fixed-capacity channel, and the preferred summary set has exact `max_live_spools` capacity. Canonical payload length is checked against `max_spool_bytes`; over-limit candidates remain unchanged and are not opened. Eligible candidates are opened without following a final symlink and have their descriptor length verified before selection. Failed preflights leave the spool intact and the scan continues, so repeated newer failures cannot starve an older valid spool. Terminal `Complete` descriptors close immediately; only bounded active candidates retain handles through admission. Displaced or excess active handles are closed without payload reads or sidecar rewrites.

Persisted page layouts are brought under the current limits before admission. Completed data is resegmented arithmetically to the configured `page_size` without reading payload pages at startup. A zero-page legacy `Writing` payload within `max_spool_bytes` is treated as a contiguous terminal stream and resegmented as `Complete` across as many checked, current-size pages as its exact durable length requires; it is not limited to one page, and further writes are rejected. A legacy `Writing` sidecar that records pages but has no trustworthy historical stride stays quarantined unchanged. An active `Writing` spool with a known stride wider than the current bound is quarantined before payload open or tail loading. Missing- or oversized-stride `WriteLocked` spools within the payload bound are salvaged as bounded `Complete`. Since startup requires `page_size <= min(67108864, max_spool_bytes)`, no recovered page exceeds the 64 MiB or spool-size constraint.

## Helm storage and StatefulSet DNS

The chart requires `config.data_dir` to be a non-empty absolute path and mounts it as a filesystem directory. `/etc/bobs` is reserved for the read-only config volume, so the schema rejects that value and template validation also rejects path aliases that clean to it. The schema and template also reject `/etc/bobs/config.yaml` and its descendants because a directory mount there would hide or descend through the required config file. Nested mount paths not proven invalid by Kubernetes are otherwise left available. `persistence.volumeMode` is fixed to `Filesystem`; raw `Block` PVCs are rejected and the chart does not render `volumeDevices`. The chart also accepts `config.max_live_spools` only in `1..=65536`. When metrics are enabled, the template rejects `config.metrics.port == config.port`. JSON Schema validates each port's range but cannot express that comparison.

`headlessService` controls the StatefulSet governing Service. With `enabled: true`, an empty `name` keeps the managed `<fullname>-svc` default and a valid non-empty name overrides it. With `enabled: false`, the chart omits that Service and `name` must identify an existing headless Service in the release namespace. The resolved name drives `StatefulSet.spec.serviceName` and the chart's default pod URL, `http://<statefulset-name>-{ordinal}.<governing-service>:<port>/api/v1`. Per-replica Services remain managed separately for ingress routing. The chart rejects governing names that collide with its main or per-pod Services.

Kubernetes DNS labels generated by the chart stay within 63 characters. For a long release or `fullnameOverride`, suffixed names keep the suffix and include an eight-character digest of the unshortened base. Dots in a Helm release name are replaced with hyphens and a digest is appended to reduce collisions with literal hyphenated releases. The StatefulSet base reserves room for controller-generated pod and PVC ordinals, so multi-digit per-pod Service and Ingress names remain distinct. Every rendered Kubernetes label and selector value, including ServiceMonitor labels, is quoted and coerced to a string so release names such as `true`, `false`, `null`, or `123` remain strings.

## Helm ingress and shutdown settings

`config.route_name` must be a single safe path segment: 1-63 ASCII letters, digits, `_`, or `-`, starting and ending with an alphanumeric character. Quotes, slashes, control characters, and regex metacharacters are rejected by both schema and template validation; NGINX regex insertion is escaped defensively as well.

`ingress.forwardedPrefix.enabled` defaults to `false`. Enable it when an ingress rewrites a public per-pod route such as `/download-0/...` to `/api/v1/read/...`. The chart then supplies `X-Forwarded-Prefix: /download-0`, allowing a long-poll `307` to return `/download-0/api/v1/read/<key>`. NGINX Inc uses `nginx.org/location-snippets`; community ingress-nginx renders one Ingress per pod with the native `nginx.ingress.kubernetes.io/x-forwarded-prefix` annotation. This per-replica Ingress topology is an architectural change: when forwarded prefixes are enabled, each replica needs its own exact prefix annotation because community ingress-nginx cannot derive a dynamic prefix per regex match. Other entries in `ingress.annotations` are preserved on every rendered Ingress.

BOBS stops accepts and gracefully drains all HTTP connections for at most 25 seconds, then aborts leftovers. The chart leaves `terminationGracePeriodSeconds` unset, so standard Kubernetes uses its 30-second default. If a parent chart or platform sets it explicitly, keep it above 25 seconds so forced aborts and final storage/telemetry teardown can run before SIGKILL.

## Example

```yaml
host: 0.0.0.0
port: 3000
data_dir: /data/bobs
page_size: 16777216
max_cache_bytes: 268435456      # cache and slow-reader response budget; 0 disables cache
# max_live_spools omitted: derives 16; valid range 1..=65536 and bounds recovery
max_spool_bytes: 8589934592       # 8 GiB per spool
create_admission_timeout_ms: 5000 # return 503 rather than waiting indefinitely
writer_inactivity_timeout_secs: 300
enable_pprof: false            # only enable for controlled, trusted profiling
read_idle_ttl_secs: 600
full_read_complete_ttl_secs: 30
reader_done_ttl_secs: 60       # deprecated compatibility field
unread_ttl_secs: 3600          # deprecated compatibility field
cleanup_sweep_interval_secs: 30
long_poll_timeout_ms: 25000
io_uring_shards: 4             # optional; valid range 1..=256; omit for max(1, num_cpus / 4)
io_uring_queue_capacity: 1024  # 1..=Tokio Semaphore::MAX_PERMITS (usize::MAX >> 3)
host_prefix: polytope-example
domain: example.com
route_name: download
metrics:
  enabled: false         # requires --features telemetry; see Metrics page
  bind_address: "127.0.0.1"  # loopback only; use 0.0.0.0 in k8s
  port: 9464             # must differ from the main port when metrics are enabled
  allowed_labels: []     # empty = all caller labels; set a list to restrict cardinality
  max_label_value_length: 128
```

After the required routing fields are present, only values you want to override need to be added. If `max_live_spools` is omitted, it is re-derived from the effective page/cache settings; an explicit value remains unchanged:

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

The binary default `page_size` is `16777216` (16 MiB), paired with a `268435456`-byte (256 MiB) cache and a derived `max_live_spools` of 16. The Helm chart intentionally overrides these with `page_size: 4096`, `max_cache_bytes: 1048576`, and explicit `max_live_spools: 256`. The operational page maximum is `67108864` (64 MiB), which remains below the one-SQE `io_uring` limit and bounds page reads and lazy per-request partial-page staging. `page_size` must also be no larger than `max_spool_bytes`.

Page size changes streaming behaviour:

- readers do not see in-progress bytes until a full page is available, so wider pages can increase reader-visible latency;
- larger pages consume more of the global cache budget per cached page, so they can reduce cache reach unless `max_cache_bytes` is increased;
- benchmark representative object sizes and write chunk sizes before changing either deployment profile.

`page_size` does not need to be less than or equal to `max_cache_bytes`. Setting `max_cache_bytes` to `0` disables caching entirely, but the manager still admits one read response at a time so disk-backed page buffers remain bounded. Otherwise it admits `floor(max_cache_bytes / page_size)` configured-page responses, with a minimum of one. Admission is acquired before page lookup or disk I/O and stays with the response body, so slow or unconsumed clients queue rather than each retaining another page allocation. Recovery ensures admitted persisted layouts are no wider than the current configured page size. A page larger than the cache cap still bypasses the cache while disk-backed reads continue to work. This cache-skipping behaviour is independent of the required `page_size <= max_spool_bytes` relationship. When `max_live_spools` is omitted, BOBS derives it from the effective cache/page ratio with a minimum of one; set it explicitly when workflow concurrency should differ from cache page capacity. Derived and explicit values above Tokio's semaphore limit are rejected during startup validation.

See the standalone benchmark guide for page-size comparison commands.
