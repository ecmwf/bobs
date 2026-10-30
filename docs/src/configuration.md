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

Default Linux builds use sharded `io_uring` rings. `io_uring_shards` is optional and accepts explicit values from `1` through `256`, inclusive. If unset, BOBS resolves it to `max(1, num_cpus / 4)`. File operations are assigned to shards by stable object-key hashing, and each object's data-file operations and metadata sidecar commits are routed to the same shard. CPU pinning is not implemented. A read or write submitted as one SQE is limited to `u32::MAX` bytes (`4294967295`); oversized operations fail before routing or submission.

The default Linux backend requires Linux 5.11+ because BOBS submits the `RenameAt` and `UnlinkAt` operations that io_uring gained in 5.11 (there is no startup kernel check; on an older kernel those operations would fail at runtime), and it requires a container/runtime policy that permits `io_uring_setup`. If `io_uring_setup` is blocked, use a runtime seccomp/sysctl policy that permits it or build the fallback binary:

```bash
cargo build --release --bins --features tokio-fileio-fallback
```

## Fields

The table distinguishes Rust defaults from chart overrides where they differ.

| Field | Default | Description |
| ------- | --------- | ------------- |
| `host` | `0.0.0.0` | Address for the HTTP server to bind to. |
| `port` | `3000` | Port for the HTTP server to listen on. |
| `data_dir` | binary: `./data`; chart: `/var/lib/bobs` | File system path for storing spool files. The chart-managed data volume is restricted to `/var/lib/bobs` or a normalized descendant. Outside this chart, the binary accepts other runtime locations, including relative paths. |
| `page_size` | binary: `16777216` (16 MiB); chart: `4096` (4 KiB) | Size of internal data pages. Valid range: `1..=67108864` (64 MiB), and it must not exceed `max_spool_bytes`. A page becomes visible only when full; `/api/v1/complete/{key}` publishes a trailing partial page. Per-request memory scales with it; see [Memory behaviour and the glibc allocator](#memory-behaviour-and-the-glibc-allocator). |
| `max_cache_bytes` | binary: `268435456` (256 MiB); chart: `1048576` (1 MiB) | Global budget for the logical bytes of bounded cache-owned page allocations across all spools, excluding allocator overhead. It also derives the read-response permit budget as `max(1, floor(max_cache_bytes / page_size))`; weighted permits are held until response bodies are dropped. Cache insertion may copy a page solely to avoid retaining an oversized transport-frame backing; the frame-to-disk path remains zero-copy. Set to `0` to disable caching while retaining a one-page response bound. A page rejected because it exceeds the cap likewise bypasses the cache without an isolation copy and remains readable from disk. The cache adds up to this many resident bytes per pod on top of per-request buffers; see [Memory behaviour and the glibc allocator](#memory-behaviour-and-the-glibc-allocator). |
| `max_live_spools` | binary: derived as `max(1, max_cache_bytes / page_size)` (`16`); chart: explicit `256` | Admission limit for spools in the first-read cache phase and for startup recovery. Valid range: `1..=65536`; the bound applies to explicit and derived values. YAML omission derives it from effective page/cache settings. The first proven full-object read frees that spool's cache and admission slot while leaving it readable from disk. Startup uses a preferred candidate and handle set whose capacity is exactly this limit, and leaves excess entries unchanged for a later restart with more capacity. It bounds admission slots, not memory: live spools are disk-backed. |
| `max_spool_bytes` | `8589934592` (8 GiB) | Maximum bytes accepted for one spool across write requests and maximum canonical payload length admitted during startup recovery. An over-limit recovered payload is quarantined unchanged before open or metadata migration. Must be greater than `0` and at least `page_size`. |
| `create_admission_timeout_ms` | `5000` | Maximum time `/api/v1/create` waits for a `max_live_spools` slot before returning `503 Service Unavailable`. Must be greater than `0`. |
| `fsync_enabled` | `true` | Flush spool data, metadata, and directory updates across crash-durability boundaries. Set `false` only for explicitly ephemeral deployments that accept losing in-flight and retained spools on restart. |
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

The chart mounts its managed PVC or `emptyDir` only at `/var/lib/bobs` or a normalized descendant. The schema rejects other roots, and template validation enforces the same policy when schema validation is bypassed. `.` and `..` segments, repeated slashes, and trailing slashes are rejected rather than normalized, preventing the data volume from hiding the image root, executables, or system directories such as `/usr`, `/etc`, and `/tmp`. This restriction belongs to the chart's mount contract, not the BOBS runtime: deployments outside this chart may configure other absolute or relative `data_dir` locations. `persistence.volumeMode` is fixed to `Filesystem`; raw `Block` PVCs are rejected and the chart does not render `volumeDevices`. The chart schema accepts `config.max_live_spools` only in `1..=65536`; if schema validation is skipped, the template fallback checks only the upper bound and the binary rejects `0` at startup. The chart does not check that `cleanup_sweep_interval_secs` is within the cleanup timeouts, so a violation renders fine and fails at pod startup. When metrics are enabled, the template rejects `config.metrics.port == config.port`. JSON Schema validates each port's range but cannot express that comparison.

`headlessService` controls the StatefulSet governing Service. With `enabled: true`, an empty `name` keeps the managed `<fullname>-svc` default and a valid non-empty name overrides it. With `enabled: false`, the chart omits that Service and `name` must identify an existing headless Service in the release namespace. The resolved name drives `StatefulSet.spec.serviceName` and the chart's default pod URL, `http://<statefulset-name>-{ordinal}.<governing-service>:<service.port>/api/v1`. Note that this uses `service.port`, not `config.port`, although the URL addresses the pod directly; keep the two equal (the default, 3000) or override `internalBaseUrlTemplate`. Per-replica Services remain managed separately for ingress routing. The chart rejects governing names that collide with its main or per-pod Services.

Kubernetes DNS labels generated by the chart stay within 63 characters. For a long release or `fullnameOverride`, suffixed names keep the suffix and include an eight-character digest of the unshortened base. Dots in a Helm release name are replaced with hyphens and a digest is appended to reduce collisions with literal hyphenated releases. The StatefulSet base reserves room for controller-generated pod and PVC ordinals, so multi-digit per-pod Service and Ingress names remain distinct. Every rendered Kubernetes label and selector value, including ServiceMonitor labels, is quoted and coerced to a string so release names such as `true`, `false`, `null`, or `123` remain strings.

The StatefulSet pod template's `checksum/config` annotation is a deterministic SHA-256 digest of only the rendered `config.yaml` ConfigMap payload. Changing effective file-based runtime settings rolls every pod; changing unrelated image, persistence, replica, pod-annotation, or Kubernetes metadata values does not perturb this checksum. Other pod-template settings still trigger normal StatefulSet rollouts through their own template changes.

## Helm extra environment variables

`extraEnv` is a list of standard Kubernetes `EnvVar` entries (`name` plus `value` or `valueFrom`) appended after the chart's built-in container environment variables. It is empty by default, and changing it rolls the StatefulSet. It is the supported way to set process-level variables such as glibc malloc tunables without forking the chart:

```yaml
extraEnv:
  - name: MALLOC_MMAP_THRESHOLD_
    value: "1048576"
```

See [Memory behaviour and the glibc allocator](#memory-behaviour-and-the-glibc-allocator) for when this particular setting is useful.

## Helm ingress and shutdown settings

`config.route_name` must be a single safe path segment: 1-63 ASCII letters, digits, `_`, or `-`, starting and ending with an alphanumeric character. Quotes, slashes, control characters, and regex metacharacters are rejected by both schema and template validation; NGINX regex insertion is escaped defensively as well.

`ingress.forwardedPrefix.enabled` defaults to `true`, so an otherwise-default, schema-valid ingress preserves the public per-pod prefix without an extra opt-in. A public route such as `/download-0/...` is rewritten to `/api/v1/read/...` while BOBS receives `X-Forwarded-Prefix: /download-0`; a long-poll `307` therefore returns the routable `/download-0/api/v1/read/<key>` Location. NGINX Inc uses `nginx.org/location-snippets` to derive the exact `/{route_name}-N` prefix and keeps the standard forwarded headers explicit. Community ingress-nginx renders one Ingress per pod with the matching native `nginx.ingress.kubernetes.io/x-forwarded-prefix` annotation because that annotation is static per Ingress. In both modes, short public read URLs and redirected `/api/v1/read/` URLs rewrite to the corresponding per-replica Service, and unrelated user annotations are retained. Explicitly setting the flag to `false` while ingress is enabled is rejected by schema and template validation rather than silently rendering unroutable redirects.

BOBS stops accepting connections and gracefully drains all HTTP connections for at most 25 seconds, then aborts leftovers. The chart leaves `terminationGracePeriodSeconds` unset, so standard Kubernetes uses its 30-second default. If a parent chart or platform sets it explicitly, keep it above 25 seconds so forced aborts and final storage/telemetry teardown can run before SIGKILL.

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
fsync_enabled: true             # safe default; false is for ephemeral spools only
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

After the required routing fields are present, only values you want to override need to be added. If `max_live_spools` is omitted, it is re-derived from the effective page/cache settings; an explicit value remains unchanged. Unknown keys in the YAML file are silently ignored (a typo such as `page_sze` falls back to the default). The example below keeps the 16 MiB default `page_size`, so its 4 MiB cache is smaller than one page: it derives `max_live_spools: 1`, admits one read response at a time and effectively caches nothing. Raise `max_cache_bytes` together with `page_size`, or lower `page_size`:

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

Linux startup emits a `debug`-level event (visible with `RUST_LOG=debug`; the default filter is `info`) with `configured_shards`, `resolved_shards`, and `cpu_pinning_enabled`. `cpu_pinning_enabled` is always `false`.

Future optimizations not implemented in the current backend are `IORING_REGISTER_FILES`, `IORING_REGISTER_BUFFERS`, and `IORING_SETUP_ATTACH_WQ`.

## Page size tuning

The binary default `page_size` is `16777216` (16 MiB), paired with a `268435456`-byte (256 MiB) cache and a derived `max_live_spools` of 16. The Helm chart intentionally overrides these with `page_size: 4096`, `max_cache_bytes: 1048576`, and explicit `max_live_spools: 256`. The operational page maximum is `67108864` (64 MiB), which remains below the one-SQE `io_uring` limit and bounds page reads and lazy per-request partial-page staging. `page_size` must also be no larger than `max_spool_bytes`.

Page size changes streaming behaviour:

- readers do not see in-progress bytes until a full page is available, so wider pages can increase reader-visible latency;
- larger pages consume more of the global cache budget per cached page, so they can reduce cache reach unless `max_cache_bytes` is increased;
- benchmark representative object sizes and write chunk sizes before changing either deployment profile.

`page_size` does not need to be less than or equal to `max_cache_bytes`. Setting `max_cache_bytes` to `0` disables caching entirely, but the manager still admits one read response at a time so disk-backed page buffers remain bounded. Otherwise it admits `floor(max_cache_bytes / page_size)` configured-page responses, with a minimum of one. Admission is acquired before page lookup or disk I/O and stays with the response body, so slow or unconsumed clients queue rather than each retaining another page allocation. Recovery ensures admitted persisted layouts are no wider than the current configured page size. A page larger than the cache cap still bypasses the cache while disk-backed reads continue to work. This cache-skipping behaviour is independent of the required `page_size <= max_spool_bytes` relationship. When `max_live_spools` is omitted, BOBS derives it from the effective cache/page ratio with a minimum of one; set it explicitly when workflow concurrency should differ from cache page capacity. Derived and explicit `max_live_spools` values above `65536` are rejected during startup validation (the Tokio `Semaphore::MAX_PERMITS` bound applies to `io_uring_queue_capacity`; the read-response permit count is clamped to it internally).

See the standalone benchmark guide for page-size comparison commands.

## Restart behaviour and admission slots

Every spool found on disk at startup is a recovery candidate. Up to `max_live_spools` of them (the most recently active first) are recovered and each takes an admission slot, whether or not it had already been fully read before the restart: read state is not persisted, so recovered spools are treated as unread and their idle clocks restart at recovery. While all slots are held, `/api/v1/create` returns `503` after `create_admission_timeout_ms`. The slots free when each spool is fully read, or after `read_idle_ttl_secs` of read inactivity, so a pod that recovers `max_live_spools` spools can reject creates for up to `read_idle_ttl_secs` and then free them all at once. Recovered spools stay readable throughout.

Practical consequences:

- Keep `max_live_spools` comfortably above the number of spools a pod typically holds on disk (live spools, including recently fully read ones that are waiting out `full_read_complete_ttl_secs`, plus any quarantined or stale spools left in the data directory), or a restart (including a rolling update) can leave a pod unable to accept creates.
- A shorter `read_idle_ttl_secs` shortens that window but also deletes never-read and idle partly-read results sooner.
- `/api/v1/health` does not reflect admission state, so a pod in this state still passes its readiness probe and keeps receiving creates through the Service; clients should retry a `503` on `/api/v1/create`.
- `max_live_spools` bounds admission slots, not memory: live spools are disk-backed.

## Memory behaviour and the glibc allocator

**Symptom.** With large pages, resident memory of a BOBS process can climb with request churn until the container memory limit is reached and the pod is OOM-killed, even though little live data is held. It does not track the number of live spools or the amount of spool data: live spools are disk-backed, `max_live_spools` bounds admission slots rather than memory, and the page cache is bounded separately by `max_cache_bytes`.

**Cause.** BOBS allocates and frees page-sized buffers (`page_size`, up to 64 MiB) for every request: upload staging, disk reads, and cache copies. Under glibc `malloc`, large allocations initially use `mmap`. After the first free of an `mmap`ed chunk, glibc raises its dynamic `mmap` threshold to that chunk size (capped at 32 MiB on 64-bit), so later page-sized buffers are served from the `brk` heap and per-thread arenas instead. Memory freed there is returned to the OS only when it sits at the top of a heap, so holes stay resident. Resident anonymous memory therefore ratchets up with request churn.

**Observed.** These figures come from a local benchmark with BOBS 0.1.4 on Debian glibc 2.36, `page_size` 16 MiB and `max_cache_bytes` 256 MiB. They are indicative, not guarantees:

- 20 rounds of 16 x 64 MiB objects: default glibc settled at about 1.85 GiB of anonymous RSS with none returned at idle, and was OOM-killed after 12 rounds under a 2.5 GB cgroup limit. With `MALLOC_MMAP_THRESHOLD_=1048576` it rested at about 17 MB and survived 40 rounds under the same limit.
- At low concurrency (3 simultaneous uploads of 50 MiB objects, 150 rounds), default glibc grew from 17 to 621 MB and was still creeping; with the variable set it stayed at about 8 MB.
- In a production deployment (5 pods, 16 GiB limit, about 60k spools per day, roughly 3 concurrent uploads across all pods) anonymous RSS returned to about 15 GiB within about 11 hours of a restart.

**Diagnosing.** Distinguish anonymous memory from page cache in `/proc/<pid>/status`: the retention described here shows up as `RssAnon` (page cache and file-backed pages are `RssFile` and are reclaimable, so they are not the cause). In `/proc/<pid>/smaps`, look for `[heap]` plus many fully resident 64 MiB anonymous mappings (glibc per-thread arenas). Memory that keeps growing while the number of live spools stays flat points at allocator retention rather than a leak of live data.

**Mitigation.** Set `MALLOC_MMAP_THRESHOLD_=1048576` in the BOBS process environment. Any value below `page_size` works, and it must be at most `33554432` to be honoured. Setting any of `MALLOC_MMAP_THRESHOLD_`, `MALLOC_TRIM_THRESHOLD_`, `MALLOC_TOP_PAD_`, or `MALLOC_MMAP_MAX_` disables glibc's dynamic threshold adjustment, so page-sized buffers keep using `mmap` and are unmapped when freed. In the Helm chart, use [`extraEnv`](#helm-extra-environment-variables):

```yaml
extraEnv:
  - name: MALLOC_MMAP_THRESHOLD_
    value: "1048576"
```

This has not yet been validated on a real cluster. It applies only to glibc-based images; the official image is Debian-based, while musl, jemalloc, or mimalloc builds behave differently. The chart's default `page_size` of 4 KiB is far below glibc's initial `mmap` threshold, so the mechanism above is not expected to apply in the same way there; it matters for deployments that raise `page_size`.

**Trade-offs and alternatives.**

- The mitigation costs one `mmap`/`munmap` plus page faults for each allocation of 1 MiB or more.
- A threshold of `33554432` was worse than the default in the benchmark (about 2.1 GiB), so do not raise it towards the cap.
- `MALLOC_ARENA_MAX=2` reduced the benchmark's resting RSS to about 0.7 GiB but did not remove the retention.
- jemalloc via `LD_PRELOAD` gave about 0.39 GiB (0.24 GiB at idle) in the benchmark. Building BOBS with jemalloc or mimalloc as the global allocator, pooling page buffers, or choosing a smaller `page_size` are other options that were considered but are not implemented or recommended here.

**Per-request memory rule of thumb.** In-flight memory scales with `page_size` and with the size of the chunks the producer sends. In a local benchmark with HTTP/2 uploads (the protocol the Polytope workers use) and the cache off, peak memory was about 23-34 MB per concurrent upload at 16 MiB pages and about 12-23 MB at 4 MiB pages, depending on the producer chunk size (lowest with 1 MiB chunks). Earlier HTTP/1.1 runs measured about 21 MB per upload at 16 MiB. The page cache adds resident memory on top: a full `max_cache_bytes` cache costs about the same amount again (about 259 MiB for 256 MiB, at both 16 MiB and 4 MiB pages), but pages are inserted on write only and a full read releases them, so the cache usually holds only recent unread data. Size container memory limits from concurrency times per-request memory plus `max_cache_bytes`, and add headroom for allocator overhead. With `MALLOC_MMAP_THRESHOLD_` set, the same benchmark still showed roughly 60-70 MB of retained memory after heavy upload churn and more with producers that send chunks below 1 MiB; a long soak has not been measured.
