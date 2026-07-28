<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# BOBS — Big-Object Buffered Storage

A streaming spool service for buffering large producer responses for slow consumers.

[![Static Badge](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity/incubating_badge.svg)](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity#incubating)

> [!IMPORTANT]
> This software is **Incubating** and subject to ECMWF's guidelines on [Software Maturity](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity).

## Overview

BOBS acts as a temporary buffer between fast data producers and potentially slow or remote consumers. It's designed for scenarios where a single writer, such as an HPC task or a local application, appends data to a spool while a single consumer may open parallel connections across WAN or remote links.

The service provides a byte-range addressable interface, allowing readers to stream data as it arrives or request specific segments. BOBS manages temporary storage with automatic cleanup based on writer inactivity, read inactivity, or complete read coverage. It's typically deployed as Kubernetes pods, leveraging local storage for high-performance buffering.

## Quick Start

Build the project using Cargo:

```bash
cargo build --release
```

Run the service with a config file and the required environment variables:

```bash
export HOSTNAME=bobs-0
export BOBS_INTERNAL_BASE_URL_TEMPLATE=http://localhost:3000/api/v1
./target/release/bobs config.yaml
```

`HOSTNAME` must end with a numeric StatefulSet ordinal such as `bobs-0`. BOBS substitutes that ordinal for every `{ordinal}` placeholder in `BOBS_INTERNAL_BASE_URL_TEMPLATE` and returns the resulting base URL as `write_url` from `/api/v1/create`. Both environment variables must be set; the URL template must be non-empty.

Required config fields:

- `host_prefix`
- `domain`
- `route_name`

Operational constraints:

- `HOSTNAME` must be set and include a pod ordinal such as `bobs-0`.
- `BOBS_INTERNAL_BASE_URL_TEMPLATE` must be set and non-empty.
- `page_size` must be in `1..=67108864` bytes (64 MiB) and no larger than `max_spool_bytes`.
- `max_live_spools` must be in `1..=65536`; the same bound applies when the value is derived from page and cache settings.
- `max_spool_bytes`, `create_admission_timeout_ms`, `writer_inactivity_timeout_secs`, `read_idle_ttl_secs`, `full_read_complete_ttl_secs`, `cleanup_sweep_interval_secs`, `long_poll_timeout_ms`, and `io_uring_queue_capacity` must be greater than `0`; `io_uring_queue_capacity` must not exceed Tokio's `Semaphore::MAX_PERMITS` (`usize::MAX >> 3`). Invalid queue capacities fail startup with `ConfigurationError`.
- `io_uring_shards`, when set, must be in `1..=256`; omitted shards resolve to `max(1, num_cpus / 4)`.
- `cleanup_sweep_interval_secs` must not exceed any active cleanup timeout.
- `max_cache_bytes` may be smaller than `page_size`; `0` disables caching while one bounded disk-backed read response remains admitted.

Example:

```bash
./target/release/bobs config.yaml
```

## Usage Example

Follow this lifecycle to create, write, read, and delete a spool.

### 1. Create a spool

Without a valid `X-Polytope-Job-Id` header, the service generates a UUIDv4 key for the new spool. A valid header is a 26-character Crockford base32 request ID in either case; uppercase input is accepted and the returned canonical key is normalized to lowercase.

```bash
curl -X PUT http://localhost:3000/api/v1/create -d '{"content_type": "application/octet-stream"}'
# Response: {"key":"550e8400-e29b-41d4-a716-446655440000","read_url":"https://bobs.example.com/download-0/550e8400-e29b-41d4-a716-446655440000","write_url":"http://localhost:3000/api/v1"}
```

A `201 Created` response means the empty data file and key-directory link have crossed their fsync boundaries and live `Writing` or `WriteLocked` metadata is durable. The intermediate `Creating` marker is internal to crash recovery and is never exposed as an active spool. Interrupted creates are removed safely on restart; duplicate or concurrent creates for the same request ID return `409 Conflict` without truncating the existing spool.

### 2. Write data

Append data at a specific offset. Offset must match the current total bytes written.

```bash
curl -X POST http://localhost:3000/api/v1/write/unique-spool-key/0 --data-binary @file.dat
```

### 3. Complete the spool

Finalize the spool to signal readers that no more data is coming. Optional `expected_size` verification ensures integrity. Completion is idempotent, but a repeated request still rejects an `expected_size` that differs from the completed size.

```bash
curl -X POST http://localhost:3000/api/v1/complete/unique-spool-key -d '{"expected_size": 1048576}'
```

Writes and completion are single-flight per spool. A caller cancelled while waiting leaves no detached task; once admitted, owned mutation work continues to a stable result while holding the per-spool operation gate, so a retry cannot overlap unfinished I/O or completion publication.

### 4. Read data

**Bounded read**: Request a specific byte range via a standard HTTP `Range` header.

```bash
curl http://localhost:3000/api/v1/read/unique-spool-key -H "Range: bytes=0-1048575"
```

Both `bytes=X-Y` and `bytes=X-` are bounded reads (`206 Partial Content`). An open-ended `bytes=X-` request ends at the bytes available when the request is resolved; it does not wait for later writes. Suffix ranges (`bytes=-N`) require a completed spool.

**Follow mode**: Stream data as it's written by omitting the `Range` header.

```bash
curl http://localhost:3000/api/v1/read/unique-spool-key
```

If a follow read waits `long_poll_timeout_ms` before its first page, BOBS returns a `307 Temporary Redirect` for clients such as `curl -L` to retry. If that timeout occurs after bytes have started streaming, BOBS aborts the transfer with a response-body error rather than treating it as a successful end of stream.

### 5. Parallel reads

Multiple readers can consume different ranges simultaneously.

```bash
# Terminal 1
curl http://localhost:3000/api/v1/read/unique-spool-key -H "Range: bytes=0-524287"

# Terminal 2
curl http://localhost:3000/api/v1/read/unique-spool-key -H "Range: bytes=524288-1048575"
```

### 6. Delete the spool

Manually remove a spool when finished.

```bash
curl -X DELETE http://localhost:3000/api/v1/delete/unique-spool-key
```

A successful delete is acknowledged only after `meta.json` and the key directory are removed and the parent `data_dir` is fsynced. The manager retains cache, admission, and tracked deletion state until that durability boundary succeeds, allowing a failed delete to be retried.

## Health endpoint

```bash
curl http://localhost:3000/api/v1/health
```

## Standalone benchmark

For direct BOBS throughput validation, see the mdBook page: `docs/src/standalone-benchmark.md`.

## Configuration

BOBS reads an optional YAML file. Missing fields use the binary defaults below, but startup requires non-empty `host_prefix`, `domain`, and `route_name` values plus the routing environment variables shown in Quick Start.

```bash
HOSTNAME=bobs-0 \
BOBS_INTERNAL_BASE_URL_TEMPLATE=http://localhost:3000/api/v1 \
  ./target/release/bobs config.yaml
```

| Field | Binary default | Description |
| ------- | --------- | ------------- |
| `host` | `0.0.0.0` | Address to listen on. |
| `port` | `3000` | Port to listen on. |
| `data_dir` | `./data` | Directory for storing spool files. |
| `page_size` | binary: `16777216` (16 MiB); chart: `4096` (4 KiB) | Size of individual data pages. Valid range `1..=67108864` (64 MiB), no larger than `max_spool_bytes`. In-progress readers see a page only once it is full; `/api/v1/complete/{key}` publishes the final partial page. |
| `max_cache_bytes` | binary: `268435456` (256 MiB); chart: `1048576` (1 MiB) | Global FIFO budget for bounded cache-owned page allocations across all spools, excluding allocator overhead, and the source for `max(1, floor(max_cache_bytes / page_size))` weighted read-response permits held through response-body lifetime. Cache admission may copy solely to avoid retaining an oversized frame backing; disabled caching and pages rejected for exceeding the cap do not make that isolation copy. `0` still admits one bounded disk-backed response. |
| `max_live_spools` | binary: derived as `max(1, max_cache_bytes / page_size)` (`16`); chart: explicit `256` | Admission limit for the first-read cache phase and startup recovery. Valid range: `1..=65536`. Omitted values derive from effective page/cache settings; explicit values are preserved. Recovery no-follow open-preflights candidate payloads, uses a preferred summary and handle set whose capacity is exactly this limit, and closes displaced or excess handles without payload reads or sidecar rewrites. Failed preflights leave the spool intact and the scan continues. Proven full-object coverage frees a live spool's cache and admission slot while leaving it readable from disk. |
| `max_spool_bytes` | `8589934592` (8 GiB) | Maximum accepted size of one spool; must be at least `page_size`. An upload that crosses the limit returns `413` after the partial spool is durably deleted. |
| `create_admission_timeout_ms` | `5000` | Maximum `/api/v1/create` admission wait before `503 Service Unavailable`. |
| `writer_inactivity_timeout_secs` | `300` | Writer-silence interval after which an unfinished spool is eligible for cleanup. Must be greater than `0`. |
| `enable_pprof` | `false` | Enables unauthenticated `/debug/pprof/profile` on the main listener; use only in a controlled environment. |
| `read_idle_ttl_secs` | `600` | TTL for readable spools, anchored when the spool becomes readable and refreshed whenever bytes are served. Must be greater than `0`. |
| `full_read_complete_ttl_secs` | `30` | Short TTL after bounded aggregate coverage reaches every byte. Adjacent and overlapping ranges coalesce; excessive fragmentation retains first-read admission and uses the idle TTL until a later completed contiguous full-object response proves coverage exactly. Must be greater than `0`. |
| `reader_done_ttl_secs` | `60` | Deprecated compatibility field; parsed but ignored by cleanup. Use `read_idle_ttl_secs`. |
| `unread_ttl_secs` | `3600` | Deprecated compatibility field; parsed but ignored by cleanup. Use `read_idle_ttl_secs`. |
| `cleanup_sweep_interval_secs` | `30` | Cleanup scan frequency. Must be greater than `0` and no longer than any active cleanup timeout. |
| `long_poll_timeout_ms` | `25000` | Maximum wait for new data during a follow read. Must be greater than `0`. |
| `io_uring_shards` | unset | Linux ring count. Unset resolves to `max(1, num_cpus / 4)`; explicit values must be in `1..=256`. Invalid values fail startup, including in fallback builds; fallback I/O otherwise ignores the setting. |
| `io_uring_queue_capacity` | `1024` | Submission queue capacity for each Linux `io_uring` shard. Must be between `1` and Tokio's `Semaphore::MAX_PERMITS` (`usize::MAX >> 3`); invalid values fail startup with `ConfigurationError`, including in fallback builds. Otherwise ignored by fallback I/O. |
| `host_prefix` | `""` | External download host prefix used to build `read_url`; must be set. |
| `domain` | `""` | External download domain used to build `read_url`; must be set. |
| `route_name` | `""` | External download route prefix used to build `read_url`; must be set. The chart restricts it to one 1-63 character segment containing ASCII letters, digits, `_`, or `-`, starting and ending with an alphanumeric character. |
| `metrics.enabled` | `false` | Enables the Prometheus metrics endpoint in builds with the `telemetry` feature. |
| `metrics.bind_address` | `127.0.0.1` | Metrics endpoint bind address. |
| `metrics.port` | `9464` | Metrics listener port. When metrics are enabled, it must differ from the main HTTP `port`; startup rejects a conflict before either listener binds. |
| `metrics.allowed_labels` | `[]` | Caller label keys allowed as metric attributes. Empty allows all keys. |
| `metrics.max_label_value_length` | `128` | Maximum label-value byte length; longer values are truncated. |

The Helm chart overrides the binary profile with `page_size: 4096` (4 KiB), `max_cache_bytes: 1048576` (1 MiB), and an explicit `max_live_spools: 256`. There is no requirement that `max_cache_bytes` be at least `page_size`. Linux `io_uring` reads and writes submitted as one SQE are limited to `u32::MAX` bytes (`4294967295`); larger lengths are rejected before ring routing or submission.

Example `config.yaml`:

```yaml
host: 0.0.0.0
port: 3000
data_dir: /data/bobs
page_size: 16777216
max_cache_bytes: 268435456
# max_live_spools omitted: derives 16 from this page/cache combination
max_spool_bytes: 8589934592
create_admission_timeout_ms: 5000
writer_inactivity_timeout_secs: 300
enable_pprof: false
read_idle_ttl_secs: 600
full_read_complete_ttl_secs: 30
cleanup_sweep_interval_secs: 30
long_poll_timeout_ms: 25000
io_uring_queue_capacity: 1024
host_prefix: bobs
domain: example.com
route_name: download
```

## Helm chart

`chart/` is the source of truth for the BOBS Helm chart, published to
`oci://eccr.ecmwf.int/polytope/bobs-chart` on every release.

The chart restricts `config.data_dir` to `/var/lib/bobs` or a normalized
descendant and mounts the managed PVC or `emptyDir` there. `.` and `..` segments,
repeated slashes, and trailing slashes are rejected, so chart-managed storage
cannot hide the image root, executables, or system paths. This mount restriction
applies only to this Helm chart: BOBS itself still accepts other runtime
`data_dir` locations, including relative paths, when deployed without the chart.
The chart supports only `persistence.volumeMode: Filesystem`; raw `Block` PVCs
are rejected and are never rendered as `volumeDevices`. Chart values also limit
`config.max_live_spools` to `65536`. When metrics are enabled,
`config.metrics.port` must differ from `config.port`; Helm's template validation
enforces this cross-field rule because JSON Schema cannot compare the two ports.
`config.route_name` must match `[A-Za-z0-9]([A-Za-z0-9_-]{0,61}[A-Za-z0-9])?`;
the templates also validate it and quote YAML plus escape its NGINX regex use.

The StatefulSet governing Service is controlled by `headlessService`: with
`enabled: true`, an empty `name` preserves the managed `<fullname>-svc` default
and a non-empty name overrides it. With `enabled: false`, `name` is required and
must identify an existing headless Service in the release namespace. The chart
rejects governing names that collide with its main or per-replica Services.
Generated DNS labels stay within 63 characters. Long bases retain a short digest,
dotted release names are mapped to DNS labels, and per-replica names retain their
ordinal suffix. Ingress still uses the chart's per-replica Services. All rendered
Kubernetes label and selector values, including ServiceMonitor labels, are emitted
as YAML strings so boolean-, null-, and numeric-looking names cannot change type.

The StatefulSet pod template carries `checksum/config`, a deterministic digest of
the rendered `config.yaml` ConfigMap payload. Runtime configuration changes
therefore roll every pod, while unrelated release metadata and workload values do
not perturb the digest.

Ingress-enabled renders preserve each public `/{route_name}-N` prefix by default so
long-poll redirects remain routable. NGINX Inc derives the exact prefix in its
location snippet; community ingress-nginx renders one Ingress per replica with a
matching `nginx.ingress.kubernetes.io/x-forwarded-prefix`. Setting
`ingress.forwardedPrefix.enabled: false` while ingress is enabled is rejected rather
than rendering a broken redirect contract.

Validate chart changes with Helm and `kubeconform` v0.8.0:

```bash
./scripts/test-chart.sh
helm package chart --destination /tmp
```

## License

[Apache License 2.0](LICENSE). In applying this licence, ECMWF does not waive
the privileges and immunities granted to it by virtue of its status as an
intergovernmental organisation, nor does it submit to any jurisdiction.
