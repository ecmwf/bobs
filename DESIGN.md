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
| `GET` | `/health` | Health check. |
| `GET`/`HEAD` | `/status` | Status check. |
| `PUT` | `/create` | Create a spool and return its request-ID or fallback UUIDv4 key plus `read_url` and `write_url`. Optional JSON fields include `content_type`, `content_encoding`, `write_locked`, and `labels`. |
| `POST` | `/write/{key}/{offset}` | Append request-body bytes. `offset` must equal the current write head; gaps and overwrites are rejected. |
| `POST` | `/complete/{key}` | Finalize the spool. Optional `expected_size` rejects completion if the written length differs. |
| `GET` | `/read/{key}` | Stream bytes. No `Range` header follows the stream; `bytes=X-Y` and `bytes=X-` are bounded range reads. |
| `DELETE` | `/delete/{key}` | Delete a spool early. |

`/complete` is the writer finalization endpoint.

## On-disk layout and metadata

Each spool is stored in its own directory:

```text
<data_dir>/<key>/
  spool.dat
  meta.json
```

`spool.dat` contains accepted payload bytes. `meta.json` is a sidecar metadata file containing lifecycle state, content metadata, timestamps, byte counts, page counts, final partial-page size, and the data path.

Sidecar metadata commits are atomic at the file level: BOBS writes `meta.json.tmp`, syncs that file, renames it over `meta.json`, and syncs the spool directory. Recovery ignores leftover temporary metadata files.

Ordinary `/write` calls do not persist a metadata high-water mark. For in-progress spools, `spool.dat` is authoritative after a BOBS process restart; recovery recomputes length and page state from the data file.

## File I/O

BOBS uses positional file I/O through a `FileIO` abstraction.

On Linux, the default backend is a sharded `io_uring` pool. `io_uring_shards` defaults to unset, which resolves to `max(1, num_cpus / 4)`, and must be greater than `0` when configured. `io_uring_queue_capacity` defaults to `1024` per shard and must be greater than `0`. Non-Linux builds, and builds with the `tokio-fileio-fallback` feature, use the Tokio/blocking file backend and ignore those settings. Both backends read and write by explicit offset rather than a shared cursor.

Accepted write bytes are appended to `spool.dat` before `/write` returns, but they are not forced to stable storage per page. `/complete` syncs the data file before committing final complete metadata.

## Paging and cache

The byte stream is divided into fixed-size pages (`page_size`, binary default 16777216 bytes / 16 MiB). The Helm chart currently overrides this with 4096-byte pages. `page_size` must be greater than `0`.

Write path:

1. HTTP body bytes are accepted at the required sequential offset.
2. Bytes are written to `spool.dat` through `FileIO`.
3. Full pages become reader-visible.
4. Visible pages are offered to the global FIFO page cache and waiting readers are notified.

The page cache is global across all spools. Entries are keyed by `(spool_key, page_index)` and share the single `max_cache_bytes` budget (binary default 268435456 bytes / 256 MiB; current Helm chart value 1048576 bytes / 1 MiB). `max_cache_bytes` may be smaller than `page_size`: setting it to `0` disables caching, and pages larger than the cap bypass the cache while remaining readable from disk. Once every byte of an object has been served at least once, that spool's cached pages are freed; later reads come from disk.

`max_live_spools` (default 4096) limits the number of spools in the first-read cache phase. Create requests wait for an admission slot when the limit is reached. It must be greater than `0`.

A trailing partial page may already be present in `spool.dat`, but it is not reader-visible until it becomes a full page or `/complete` publishes it as the final page.

## Read behaviour

Reads first check the global page cache. Cache misses read the required page bytes from `spool.dat` using positional I/O.

Only a request without `Range` enters follow mode, starting at byte 0. If the next page has not been written yet, BOBS parks the request until more data arrives, the spool completes, the spool is deleted, or the long-poll timeout fires.

`Range: bytes=X-Y` and `Range: bytes=X-` are bounded requests and return `206 Partial Content`. An open-ended range snapshots its upper bound from the bytes currently servable when the request is resolved, so it does not wait for future writes. For an in-progress spool, a trailing partial page is not servable. Suffix ranges (`bytes=-N`) require a completed spool.

If the follow-mode timeout fires before the first page is available, BOBS returns `307 Temporary Redirect` to a read URL for the same key. If a trusted ingress supplies a valid `X-Forwarded-Prefix`, the redirect preserves that external prefix; otherwise it falls back to `/api/v1/read/{key}`. The redirect includes `Cache-Control: no-store` because the location can depend on request headers. A timeout after streaming has begun ends that response rather than redirecting it.

Range reads update aggregate read-coverage tracking so cleanup can detect when the whole object has been served, even across multiple range requests.

## Write-locked spools

A spool can be created with `write_locked: true`. In this state writes are accepted, but reads return `423 Locked` until the spool is completed or made readable by a lifecycle transition. Completing a write-locked spool makes the final object readable.

The write-lock state is lifecycle metadata in `meta.json` and is recovered on restart.

## Completion and durability boundary

`/complete` validates the optional expected size before publishing final state. It then publishes any trailing partial page, syncs `spool.dat`, commits final metadata to `meta.json`, updates in-memory state/cache, and notifies readers.

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

The legacy `reader_done_ttl_secs` and `unread_ttl_secs` fields are still parsed for config-file compatibility but no longer drive cleanup.

Slow readers keep a spool alive only while they continue making read progress. Stalled connections do not protect a spool forever.
