<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# BOBS Design

BOBS (Big-Object Buffered Storage) is a temporary HTTP spool service for large streaming responses. A producer writes one contiguous byte stream, and one or more reader connections consume it while it is still being produced or after it is complete.

BOBS is not long-term object storage. It has no replication layer, no authentication layer, and no guarantee that an in-progress object survives a node or storage crash before completion. It is designed for short-lived, single-writer streams in a trusted Kubernetes/internal network.

## Deployment model

BOBS runs as a set of pods. Producers create and write through the internal API. Reader URLs can route through ingress to the pod/route that owns or can see the key.

A create request uses a valid `X-Polytope-Job-Id` value as the spool key. Valid request IDs are 26-character, lower-case Crockford base32 strings; if the header is absent or invalid, BOBS allocates a UUIDv4 key instead. Keys do not include a host prefix. Routing information is carried in the returned URLs, not embedded in the key.

Correct routing remains important: create, write, complete, delete, and read traffic for a key must reach a BOBS instance that can access the key's directory under `data_dir`.

## HTTP API

The API is served under `/api/v1`.

| Method | Route | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/health` | Health check. |
| `GET`/`HEAD` | `/api/v1/status` | Status check. |
| `PUT` | `/api/v1/create` | Create a spool and return its request-ID or fallback UUIDv4 key plus `read_url` and `write_url`. Optional JSON fields include `content_type`, `content_encoding`, `write_locked`, and `labels`. |
| `POST` | `/api/v1/write/{key}/{offset}` | Append request-body bytes. `offset` must equal the current write head; gaps and overwrites are rejected. |
| `POST` | `/api/v1/complete/{key}` | Idempotently finalize the spool. Optional `expected_size` is validated on both initial and repeated completion calls. |
| `GET` | `/api/v1/read/{key}` | Stream bytes. No `Range` header follows the stream; `bytes=X-Y` and `bytes=X-` are bounded range reads. |
| `DELETE` | `/api/v1/delete/{key}` | Delete a spool early. |

`/api/v1/complete/{key}` is the writer finalization endpoint.

## On-disk layout and metadata

Each spool is stored in its own directory:

```text
<data_dir>/<key>/
  spool.dat
  meta.json
```

`spool.dat` contains accepted payload bytes. `meta.json` is a sidecar metadata file containing lifecycle state, content metadata, timestamps, byte counts, page counts, final partial-page size, and the data path.

Sidecar metadata commits are atomic at the file level: BOBS writes `meta.json.tmp`, syncs that file, renames it over `meta.json`, and syncs the spool directory. Recovery ignores leftover temporary metadata files.

Ordinary `/api/v1/write/{key}/{offset}` calls do not persist a metadata high-water mark. For in-progress spools, `spool.dat` is authoritative after a BOBS process restart; recovery recomputes length and page state from the data file.

## File I/O

BOBS uses positional file I/O through a `FileIO` abstraction.

On Linux, the default backend is a sharded `io_uring` pool. `io_uring_shards` defaults to unset, which resolves to `max(1, num_cpus / 4)`, and must be greater than `0` when configured. `io_uring_queue_capacity` defaults to `1024` per shard and must be greater than `0`. Non-Linux builds, and builds with the `tokio-fileio-fallback` feature, use the Tokio/blocking file backend and ignore those settings. Both backends read and write by explicit offset rather than a shared cursor.

Accepted write bytes are appended to `spool.dat` before `/api/v1/write/{key}/{offset}` returns, but they are not forced to stable storage per page. `/api/v1/complete/{key}` syncs the data file before committing final complete metadata.

## Paging and cache

The byte stream is divided into fixed-size pages (`page_size`, binary default 16777216 bytes / 16 MiB). The Helm chart overrides this with 4096-byte (4 KiB) pages. `page_size` must be greater than `0`.

Write path:

1. HTTP body bytes are accepted at the required sequential offset.
2. Bytes are written to `spool.dat` through `FileIO`.
3. Full pages become reader-visible.
4. Visible pages are offered to the global FIFO page cache and waiting readers are notified.

The page cache is global across all spools. Entries are keyed by `(spool_key, page_index)` and share the single `max_cache_bytes` budget (binary default 268435456 bytes / 256 MiB; current Helm chart value 1048576 bytes / 1 MiB). `max_cache_bytes` may be smaller than `page_size`: setting it to `0` disables caching, and pages larger than the cap bypass the cache while remaining readable from disk. Once every byte of an object has been served at least once, that spool's cached pages are freed; later reads come from disk.

`max_live_spools` limits the number of spools in the first-read cache phase. When omitted, it derives as `max(1, max_cache_bytes / page_size)`, which is 16 with the 16 MiB/256 MiB binary defaults. Explicit values are preserved; the chart sets 256 for its 4 KiB/1 MiB profile. `/api/v1/create` waits up to `create_admission_timeout_ms` (default 5000) for a slot, then returns `503 Service Unavailable`.

A trailing partial page may already be present in `spool.dat`, but it is not reader-visible until it becomes a full page or `/api/v1/complete/{key}` publishes it as the final page. A spool accepts at most `max_spool_bytes` (default 8 GiB); an upload that crosses the limit is durably deleted before the server returns `413 Payload Too Large`.

## Read behaviour

Reads first check the global page cache. Cache misses read the required page bytes from `spool.dat` using positional I/O.

Only a request without `Range` enters follow mode, starting at byte 0. If the next page has not been written yet, BOBS parks the request until more data arrives, the spool completes, the spool is deleted, or the long-poll timeout fires.

`Range: bytes=X-Y` and `Range: bytes=X-` are bounded requests and return `206 Partial Content`. An open-ended range snapshots its upper bound from the bytes currently servable when the request is resolved, so it does not wait for future writes. For an in-progress spool, a trailing partial page is not servable. Suffix ranges (`bytes=-N`) require a completed spool.

If the follow-mode timeout fires before the first page is available, BOBS returns `307 Temporary Redirect` to a read URL for the same key. If a trusted ingress supplies a valid `X-Forwarded-Prefix`, the redirect preserves that external prefix; otherwise it falls back to `/api/v1/read/{key}`. The redirect includes `Cache-Control: no-store` because the location can depend on request headers. A timeout after streaming has begun ends that response rather than redirecting it.

Range reads update aggregate read-coverage tracking so cleanup can detect when the whole object has been served, even across multiple range requests.

## Write-locked spools

A spool can be created with `write_locked: true`. In this state writes are accepted, but reads return `423 Locked` until `/api/v1/complete/{key}` succeeds. Completion makes the final object readable.

The write-lock state is lifecycle metadata in `meta.json` and is recovered on restart.

## Completion and durability boundary

`/api/v1/complete/{key}` validates the optional expected size before publishing final state. Repeating completion is idempotent, but any supplied `expected_size` is still checked against the completed length. Initial completion publishes any trailing partial page, syncs `spool.dat`, commits final metadata to `meta.json`, updates in-memory state/cache, and notifies readers.

After successful completion, `meta.json` is the durable completed-object record. Before completion, BOBS provides process-restart recovery from `spool.dat`, not stable-storage durability for each acknowledged page.

## Recovery

Startup recovery scans `data_dir` for spool directories with `meta.json` sidecars.

- `Writing` and `WriteLocked` spools are rebuilt from `spool.dat`; byte-derived metadata in the sidecar is advisory.
- `Complete` spools are accepted only if `spool.dat` satisfies the committed logical length.
- Interrupted metadata temp files are ignored.
- Unsafe or unrelated directories are not blindly removed. Orphan cleanup is restricted to recognised UUID or 26-character request-ID directories that contain BOBS spool markers.

## Cleanup rules

A background cleanup task removes expired spools and their key directories, including `spool.dat`, `meta.json`, and interrupted `meta.json.tmp` files.

Current cleanup triggers are:

- writer inactivity for producers that stop writing without completing (`writer_inactivity_timeout_secs`, default 300);
- read-idle TTL for readable spools that have not served bytes recently, with never-read spools anchored at `readable_at` (`read_idle_ttl_secs`, default 600);
- full-read-complete TTL once aggregate coverage shows every byte has been served at least once (`full_read_complete_ttl_secs`, default 30).

The legacy `reader_done_ttl_secs` and `unread_ttl_secs` fields are still parsed for config-file compatibility but no longer drive cleanup. `cleanup_sweep_interval_secs` defaults to 30 and must not exceed `writer_inactivity_timeout_secs`, `read_idle_ttl_secs`, or `full_read_complete_ttl_secs`.

Slow readers keep a spool alive only while they continue making read progress. Stalled connections do not protect a spool forever.

## HTTP content safety

`content_type` and `content_encoding` supplied to `/api/v1/create` must be valid HTTP header values; malformed values and unknown JSON fields return `400 Bad Request`. Downloads always use `Content-Disposition: attachment` and `X-Content-Type-Options: nosniff`; active document types also receive a restrictive sandbox policy. The unauthenticated `/debug/pprof/profile` endpoint is disabled by default through `enable_pprof: false`.
