<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Metrics

BOBS exposes Prometheus metrics when built with the `telemetry` Cargo feature and
configured with `metrics.enabled: true`. Metrics are served on a dedicated HTTP
port (default `9464`) separate from the main data port.

## Enabling metrics

Build with the `telemetry` feature:

```bash
cargo build --release --bins --features telemetry
```

Add a `metrics:` section to your config:

```yaml
metrics:
  enabled: true
  bind_address: "127.0.0.1"     # loopback only; use 0.0.0.0 in k8s
  port: 9464                    # separate from the main data port
  allowed_labels: []            # empty = all caller labels pass through
  max_label_value_length: 128   # truncate long label values
```

When `metrics.enabled` is `false` (the default), all metric operations are
no-ops and no provider is installed, regardless of the build feature. The reverse is silent: `metrics.enabled: true` in a build without `--features telemetry` logs no warning and simply exports nothing.

## Scrape endpoint

```
GET http://<host>:<metrics.port>/metrics
Content-Type: text/plain; version=0.0.4
```

### Sample Kubernetes scrape config

The chart's own supported path is the ServiceMonitor it renders; the plain Prometheus config below is for setups without the Prometheus Operator. Chart pods carry the `app.kubernetes.io/name` label (not `app`) and declare both an `http` and a `metrics` container port, so the config keeps only the `metrics` port to avoid duplicate targets.

```yaml
scrape_configs:
  - job_name: bobs
    metrics_path: /metrics
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_name]
        regex: bobs
        action: keep
      - source_labels: [__meta_kubernetes_pod_container_port_name]
        regex: metrics
        action: keep
      - source_labels: [__address__]
        regex: (.+):\d+
        replacement: ${1}:9464
        target_label: __address__
```

## Label notes

- `otel_scope_name=bobs` appears on every custom metric sample.
- Resource attributes (`service_name`, `service_instance_id`, `service_version`,
  `deployment_environment`, `k8s_pod_name`, SDK info) are emitted as the
  exporter-generated `target_info` gauge, not repeated on every series.
- Caller-provided labels (e.g. `collection`) come from the `labels` object of the `PUT /api/v1/create` body. They are filtered once at create time by `allowed_labels` and `max_label_value_length`, stored in the spool's metadata and reused for every later metric of that spool. Callers should not use `reason`, `mode`, `outcome`, `state` or `status` as label keys; they are not reserved and would collide with the built-in attributes.
- The exporter also emits an `otel_scope_info{otel_scope_name="bobs"}` series.
- `reason`: deletion reason — `client`, `idle_ttl`, `full_read_ttl`, `writer_timeout`, `delete_retry` (cleanup retrying a spool already in `Deleting`). Deletions of oversize uploads (`413`) are not counted in `bobs.spools.deleted`.
- `mode`: read mode — `follow` (no `Range` header; stream until completion), `range` (any request carrying a `Range` header, including open-ended and suffix ranges).
- `outcome`: on read durations, `success`, `error`, `timeout` or `client_gone`; on create, complete and write durations, only `success` or `error`.
- `state`: active spool state — `writing`, `write_locked`, `complete`.
- `status`: startup recovery snapshot — `configured`, `recovered`, or `quarantined`.

## Metrics reference

Counters render with a `_total` suffix added by the exporter. Histograms render
as `_bucket`/`_sum`/`_count` series; duration histograms also receive a
`_seconds` suffix from the `s` unit annotation. Gauges render as-is.

| OTel instrument | Prometheus series | Type | Labels | What it is |
| --- | --- | --- | --- | --- |
| `bobs.spools.created` | `bobs_spools_created_total` | Counter | caller labels, `otel_scope_name` | Spools successfully created via `PUT /api/v1/create`. |
| `bobs.spools.completed` | `bobs_spools_completed_total` | Counter | caller labels, `otel_scope_name` | Spools successfully finalized via `POST /api/v1/complete/{key}`. |
| `bobs.spools.deleted` | `bobs_spools_deleted_total` | Counter | caller labels, `reason`, `otel_scope_name` | Spools deleted by explicit client request or cleanup (not oversize-upload removal). |
| `bobs.create.duration` | `bobs_create_duration_seconds_bucket`, `_sum`, `_count` | Histogram | caller labels, `outcome`, `otel_scope_name` | Wall time for the create handler, including admission wait, spool file creation, and initial metadata commit. |
| `bobs.complete.duration` | `bobs_complete_duration_seconds_bucket`, `_sum`, `_count` | Histogram | caller labels, `outcome`, `otel_scope_name` | Wall time for the complete request to flush and finalize a spool. |
| `bobs.write.bytes` | `bobs_write_bytes_total` | Counter | caller labels, `otel_scope_name` | Bytes written into spools. Recorded once per successful write request after its body is fully consumed; bytes of a failed or partial write are not counted. |
| `bobs.write.duration` | `bobs_write_duration_seconds_bucket`, `_sum`, `_count` | Histogram | caller labels, `outcome`, `otel_scope_name` | Wall time for a write handler to receive and append a streaming write body. |
| `bobs.read.bytes` | `bobs_read_bytes_total` | Counter | caller labels, `mode`, `otel_scope_name` | Bytes served from spools to clients. |
| `bobs.read.duration` | `bobs_read_duration_seconds_bucket`, `_sum`, `_count` | Histogram | caller labels, `mode`, `outcome`, `otel_scope_name` | Wall time for a read stream from acquisition to final outcome. |
| `bobs.read.active` | `bobs_read_active` | Gauge | caller labels, `otel_scope_name` | Current active readers. Incremented on reader acquisition, decremented on release. |
| `bobs.read.response_buffers.active` | `bobs_read_response_buffers_active` | Gauge | `otel_scope_name` | Read responses currently holding page-buffer admission, including slow or unconsumed bodies. |
| `bobs.read.response_permits.active` | `bobs_read_response_permits_active` | Gauge | `otel_scope_name` | Weighted configured-page permit units held by read responses. Compare with `max(1, floor(max_cache_bytes / page_size))`. |
| `bobs.spools.active` | `bobs_spools_active` | Gauge | `state`, `otel_scope_name` | Current active spools broken down by state. Updated on every state transition and spool removal. |
| `bobs.disk.usage.bytes` | `bobs_disk_usage_bytes` | Gauge | `otel_scope_name` | Disk usage of the spool data directory. Sampled asynchronously at the end of each cleanup sweep. |
| `bobs.recovery.spools` | `bobs_recovery_spools` | Gauge | `status`, `otel_scope_name` | Startup admission snapshot: configured capacity, successfully recovered spools, and durable spools left quarantined. |
| `bobs.pages.cache.hits` | `bobs_pages_cache_hits_total` | Counter | `otel_scope_name` | Page reads served from the in-memory page cache. |
| `bobs.pages.cache.misses` | `bobs_pages_cache_misses_total` | Counter | `otel_scope_name` | Page reads that missed the cache and were loaded from disk. |

## Label filtering

By default, all caller-provided labels are forwarded as metric attributes. In
production, restrict labels to a known-good list to prevent high cardinality
from arbitrary client input:

```yaml
metrics:
  enabled: true
  bind_address: "0.0.0.0"
  port: 9464
  allowed_labels:
    - collection
  max_label_value_length: 64
```

With `allowed_labels` set, any caller label not in the list is silently dropped.
With `max_label_value_length` set, values longer than the limit are truncated to
at most that many bytes (backed off to a UTF-8 character boundary, so the result can be slightly shorter) before recording.
