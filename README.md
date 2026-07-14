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

The service provides a byte-range addressable interface, allowing readers to stream data as it arrives or request specific segments. BOBS manages temporary storage with automatic cleanup based on inactivity, completion status, or unread timeouts. It's typically deployed as Kubernetes pods, leveraging local storage for high-performance buffering.

## Quick Start

Build the project using Cargo:

```bash
cargo build --release
```

Run the service with a config file and required environment:

```bash
export HOSTNAME=bobs-0
./target/release/bobs config.yaml
```

Required config fields:

- `host_prefix`
- `domain`
- `route_name`

Operational requirements:

- `HOSTNAME` must be set and include a pod ordinal like `bobs-0`
- `page_size` must be between `1` and `67108864` bytes (64 MiB), and no larger than `max_spool_bytes`
- `BOBS_INTERNAL_BASE_URL_TEMPLATE` must be set and non-empty
- `max_cache_bytes` may be `0` to disable caching; one bounded disk-backed read response remains admitted

Example:

```bash
./target/release/bobs config.yaml
```

## Usage Example

Follow this lifecycle to create, write, read, and delete a spool.

### 1. Create a spool

The service returns a unique key for the new spool.

```bash
curl -X PUT http://localhost:3000/api/v1/create -d '{"content_type": "application/octet-stream"}'
# Response: {"key": "unique-spool-key"}
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

**Follow mode**: Stream data as it's written (no `Range` header).

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

BOBS reads an optional YAML file passed as a CLI argument. Most fields have Rust defaults, but startup still requires `host_prefix`, `domain`, and `route_name`, plus the routing environment described above.

```bash
./target/release/bobs config.yaml
```

| Field | Default | Description |
| ------- | --------- | ------------- |
| `host` | `0.0.0.0` | Address to listen on. |
| `port` | `3000` | Port to listen on. |
| `data_dir` | `./data` | Directory for storing spool files. |
| `page_size` | binary: `16777216` (16 MiB); chart: `4096` (4 KiB) | Page size in bytes; valid range `1..=67108864` (64 MiB), and no larger than `max_spool_bytes`. |
| `max_cache_bytes` | binary: `268435456` (256 MiB); chart: `1048576` (1 MiB) | Global cache budget and source for the read-response bound: `max(1, floor(max_cache_bytes / page_size))` page leases held through response-body lifetime. `0` disables caching but still admits one bounded disk-backed response. |
| `max_live_spools` | binary: derived from cache/page capacity (16); chart: `256` | Admission and recovery limit. Derived and explicit values must be in `1..=65536`. |
| `max_spool_bytes` | `8589934592` (8 GiB) | Per-spool write limit; must be at least `page_size`. |
| `writer_inactivity_timeout_secs` | `300` | Seconds of writer silence before cleanup. |
| `reader_done_ttl_secs` | `60` | TTL after spool completion and reader finishes. |
| `unread_ttl_secs` | `3600` | TTL for completed spools that were never read. |
| `cleanup_sweep_interval_secs` | `30` | Frequency of the background cleanup task. |
| `long_poll_timeout_ms` | `25000` | Timeout for waiting on new data before redirect. |
| `metrics.enabled` | `false` | Enable the separate Prometheus metrics listener in telemetry builds. |
| `metrics.port` | `9464` | Metrics listener port; must differ from the main HTTP `port` when metrics are enabled. |
| `bob_id` | `unknown` | Unique ID for this instance (set to pod hostname in k8s). |

Example `config.yaml`:

```yaml
host: 0.0.0.0
port: 3000
data_dir: /data/bobs
page_size: 4096
max_cache_bytes: 1048576
bob_id: bobs-1
```

## License

[Apache License 2.0](LICENSE) In applying this licence, ECMWF does not waive the privileges and immunities granted to it by virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
