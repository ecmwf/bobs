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

`HOSTNAME` must end with a numeric StatefulSet ordinal such as `bobs-0`. BOBS substitutes that ordinal for every `{ordinal}` placeholder in `BOBS_INTERNAL_BASE_URL_TEMPLATE` and returns the resulting base URL as `write_url` from `/create`. Both environment variables must be set; the URL template must be non-empty.

Required config fields:

- `host_prefix`
- `domain`
- `route_name`

Operational constraints:

- `page_size`, `max_live_spools`, `read_idle_ttl_secs`, `full_read_complete_ttl_secs`, `cleanup_sweep_interval_secs`, `long_poll_timeout_ms`, and `io_uring_queue_capacity` must be greater than `0`.
- `io_uring_shards`, when set, must be greater than `0`.
- `max_cache_bytes` may be smaller than `page_size`; `0` disables caching.

## Usage Example

Follow this lifecycle to create, write, read, and delete a spool.

### 1. Create a spool

Without a valid `X-Polytope-Job-Id` header, the service generates a UUIDv4 key for the new spool. A valid header—a 26-character, lower-case Crockford base32 request ID—is instead used as the key.

```bash
curl -X PUT http://localhost:3000/api/v1/create -d '{"content_type": "application/octet-stream"}'
# Response: {"key":"550e8400-e29b-41d4-a716-446655440000","read_url":"https://bobs.example.com/download-0/550e8400-e29b-41d4-a716-446655440000","write_url":"http://localhost:3000/api/v1"}
```

### 2. Write data

Append data at a specific offset. Offset must match the current total bytes written.

```bash
curl -X POST http://localhost:3000/api/v1/write/unique-spool-key/0 --data-binary @file.dat
```

### 3. Complete the spool

Finalize the spool to signal readers that no more data is coming. Optional size verification ensures integrity.

```bash
curl -X POST http://localhost:3000/api/v1/complete/unique-spool-key -d '{"expected_size": 1048576}'
```

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

## Health endpoint

```bash
curl http://localhost:3000/api/v1/health
```

## Standalone benchmark

For direct BOBS throughput validation, see the mdBook page: `docs/src/standalone-benchmark.md`.

## Configuration

BOBS is configured via a YAML file passed as a CLI argument. Missing fields use the binary defaults below, but `host_prefix`, `domain`, and `route_name` must be set to non-empty values for validation to succeed.

```bash
./target/release/bobs config.yaml
```

| Field | Binary default | Description |
| ------- | --------- | ------------- |
| `host` | `0.0.0.0` | Address to listen on. |
| `port` | `3000` | Port to listen on. |
| `data_dir` | `./data` | Directory for storing spool files. |
| `page_size` | `16777216` (16 MiB) | Size of individual data pages. In-progress readers see a page only once it is full; `/complete` publishes the final partial page. Must be greater than `0`. |
| `max_cache_bytes` | `268435456` (256 MiB) | Global FIFO page-cache budget across all spools. `0` disables caching. A page larger than the budget bypasses the cache and remains readable from disk. |
| `max_live_spools` | `4096` | Maximum spools in the first-read cache phase. Create requests wait for a slot when the limit is reached. Must be greater than `0`. |
| `writer_inactivity_timeout_secs` | `300` | Writer-silence interval after which an unfinished spool is eligible for cleanup. |
| `read_idle_ttl_secs` | `600` | TTL for readable spools, anchored when the spool becomes readable and refreshed whenever bytes are served. Must be greater than `0`. |
| `full_read_complete_ttl_secs` | `30` | Short TTL after aggregate read coverage reaches every byte, refreshed by subsequent read activity. Must be greater than `0`. |
| `reader_done_ttl_secs` | `60` | Deprecated compatibility field; parsed but ignored by cleanup. Use `read_idle_ttl_secs`. |
| `unread_ttl_secs` | `3600` | Deprecated compatibility field; parsed but ignored by cleanup. Use `read_idle_ttl_secs`. |
| `cleanup_sweep_interval_secs` | `30` | Frequency of the background cleanup task. Must be greater than `0`. |
| `long_poll_timeout_ms` | `25000` | Maximum wait for new data during a follow read. Must be greater than `0`. |
| `io_uring_shards` | unset | Linux default-backend ring count. Unset resolves to `max(1, num_cpus / 4)`; a configured value must be greater than `0`. Ignored by fallback builds. |
| `io_uring_queue_capacity` | `1024` | Submission queue capacity for each Linux `io_uring` shard. Must be greater than `0`. Ignored by fallback builds. |
| `host_prefix` | `""` | External download host prefix used to build `read_url`; must be set. |
| `domain` | `""` | External download domain used to build `read_url`; must be set. |
| `route_name` | `""` | External download route prefix used to build `read_url`; must be set. |
| `metrics.enabled` | `false` | Enables the Prometheus metrics endpoint in builds with the `telemetry` feature. |
| `metrics.bind_address` | `127.0.0.1` | Metrics endpoint bind address. |
| `metrics.port` | `9464` | Metrics endpoint port. |
| `metrics.allowed_labels` | `[]` | Caller label keys allowed as metric attributes. Empty allows all keys. |
| `metrics.max_label_value_length` | `128` | Maximum label-value byte length; longer values are truncated. |

The Helm chart currently overrides the binary's page/cache defaults with `page_size: 4096` and `max_cache_bytes: 1048576`. There is no requirement that `max_cache_bytes` be at least `page_size`.

Example `config.yaml`:

```yaml
host: 0.0.0.0
port: 3000
data_dir: /data/bobs
page_size: 16777216
max_cache_bytes: 268435456
max_live_spools: 4096
writer_inactivity_timeout_secs: 300
read_idle_ttl_secs: 600
full_read_complete_ttl_secs: 30
cleanup_sweep_interval_secs: 30
long_poll_timeout_ms: 25000
io_uring_queue_capacity: 1024
host_prefix: bobs
domain: example.com
route_name: download
```

## License

[Apache License 2.0](LICENSE) In applying this licence, ECMWF does not waive the privileges and immunities granted to it by virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
