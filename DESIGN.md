<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# BOBS Design

BOBS (Big-Object Buffered Storage) is a temporary HTTP spool service for large streaming responses. A producer writes one contiguous byte stream, and one or more reader connections consume it while it is still being produced or after it is complete.

BOBS is not long-term object storage. It has no replication layer, no authentication layer, and no guarantee that an in-progress object survives a node or storage crash before completion. It is designed for short-lived, single-writer streams in a trusted Kubernetes/internal network.

## Deployment model

BOBS runs as a set of pods. Producers create and write through the internal API. Reader URLs can route through ingress to the pod/route that owns or can see the key.

A create request uses a valid `X-Polytope-Job-Id` value as the spool key. Valid request IDs are 26-character Crockford base32 strings in either case; BOBS accepts uppercase input and normalizes the canonical key to lowercase. If the header is absent or invalid, BOBS allocates a UUIDv4 key instead. Keys do not include a host prefix. Routing information is carried in the returned URLs, not embedded in the key.

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

`spool.dat` contains accepted payload bytes. `meta.json` is a sidecar metadata file containing lifecycle state, content metadata, timestamps, byte counts, page counts, final partial-page size, and a compatibility `data_path` value. The persisted path is never filesystem authority during recovery.

Sidecar metadata commits use a transaction-private `meta.json.tmp.<uuid>` entry created without following links. BOBS writes and syncs the open temporary file, verifies that its pathname still identifies the opened file, renames it over `meta.json`, and syncs the spool directory. The `io_uring` backend does not submit rename and directory sync until write and file sync have completed successfully. Startup recovery and durable deletion unlink stale legacy `meta.json.tmp` and UUID transaction entries without following symlinks; only the fixed `meta.json` name is ever classified or parsed as a sidecar.

Creation uses a two-state publication protocol. BOBS syncs the empty `spool.dat` and key directory, commits an internal `Creating` sidecar, fsyncs `data_dir`, then commits live `Writing` or `WriteLocked` metadata before returning success or publishing the spool in memory. `Creating` is not an API-visible lifecycle state. A crash before live publication leaves an unambiguous incomplete marker that recovery can remove without guessing about ordinary or legacy spool data. Cancellation while waiting for admission leaves no key reservation; after directory reservation starts, a detached transaction finishes durable publication or rolls back even if the client disconnects. A client that loses the response should retry with the same request ID. A duplicate for any tracked lifecycle state, including retryable `Deleting`, returns `409 Conflict` before waiting for admission or the per-key gate.

Deletion removes sidecar metadata and the key directory, then fsyncs `data_dir` before acknowledging success. Cache entries, manager membership, and admission accounting remain held across that parent-directory durability boundary so failure leaves a tracked, retryable `Deleting` spool.

Ordinary `/api/v1/write/{key}/{offset}` calls do not persist a metadata high-water mark. For in-progress spools, `spool.dat` is authoritative after a BOBS process restart; recovery recomputes length and page state from the data file.

## File I/O

BOBS uses positional file I/O through a `FileIO` abstraction.

On Linux, the default backend is a sharded `io_uring` pool. `io_uring_shards` defaults to unset, which resolves to `max(1, num_cpus / 4)`, and accepts configured values in `1..=256`. `io_uring_queue_capacity` defaults to `1024` per shard and must be between `1` and Tokio's `Semaphore::MAX_PERMITS` (`usize::MAX >> 3`); an out-of-range shard or queue value returns `ConfigurationError` during startup validation. Each read or write submitted as one SQE is limited to `u32::MAX` bytes (`4294967295`); larger lengths are rejected before routing or submission. Non-Linux builds, and builds with the `tokio-fileio-fallback` feature, use the Tokio/blocking file backend and otherwise ignore ring settings. Both backends read and write by explicit offset rather than a shared cursor.

Accepted write bytes are appended to `spool.dat` before `/api/v1/write/{key}/{offset}` returns, but they are not forced to stable storage per page. `/api/v1/complete/{key}` syncs the data file before committing final complete metadata.

Writing and completion retain one manager-held handle through the terminal transition. Recovered terminal spools may likewise retain a lazily reopened handle until their first-read coverage completes. At the full-read transition BOBS drops manager ownership before releasing first-read cache admission; positional I/O already in flight remains safe because its response owns a cloned handle. Later cache-miss reads reopen the canonical `spool.dat` asynchronously through a single-flight gate and retain the shared handle only in admitted response permits. The spool keeps a weak reference so concurrent terminal responses can share that handle without extending its lifetime beyond the final response body. The handle-state mutex and reopen gate are released before positional I/O, so independent reads remain concurrent. Deletion invalidates manager and weak handle publication before unlinking while existing response-owned handles remain safe.

## Paging and cache

The byte stream is divided into fixed-size pages (`page_size`). The Rust binary defaults to 16777216 bytes (16 MiB); the Helm chart overrides this to 4096 bytes (4 KiB). Configuration bounds pages at 67108864 bytes (64 MiB), below the one-SQE `io_uring` I/O limit, and requires `page_size <= max_spool_bytes`.

Write path:

1. HTTP body bytes are accepted at the required sequential offset.
2. Bytes are written to `spool.dat` through `FileIO`.
3. Full pages become reader-visible.
4. Visible pages admitted to the global FIFO cache are copied once into page-sized cache-owned allocations, then waiting readers are notified. The preceding frame-to-disk append still uses the transport-backed `Bytes` directly.

HTTP write staging starts empty, ignores untrusted body-size hints for reservation, and only copies cross-frame partial pages. Its per-request staging allocation is therefore lazy and bounded by the 64 MiB page-size maximum.

The page cache is global across all spools. Entries are keyed by `(spool_key, page_index)` and share the single `max_cache_bytes` budget (binary default 268435456 bytes / 256 MiB; current Helm chart value 1048576 bytes / 1 MiB). That budget accounts the logical bytes of bounded cache-owned page allocations, excluding allocator overhead, so a small page slice cannot retain a much larger HTTP frame outside the accounting. Cache insertion may make this isolation copy solely to avoid retaining an oversized frame backing; the frame-to-disk path remains zero-copy. Setting `max_cache_bytes` to `0` disables caching without an isolation copy, and pages rejected because they are larger than the cap likewise bypass the cache without one while remaining readable from disk. Cache hits share the isolated `Bytes` without copying page contents. Once every byte of an object has been served at least once, that spool's cached pages are freed; later reads come from disk.

`max_live_spools` limits the number of spools in the first-read cache phase. When omitted, it derives as `max(1, max_cache_bytes / page_size)`, which is 16 with the 16 MiB/256 MiB binary defaults. Explicit values are preserved; the chart sets 256 for its 4 KiB/1 MiB profile. `/api/v1/create` waits up to `create_admission_timeout_ms` (default 5000) for a slot, then returns `503 Service Unavailable`; a key already tracked in any state returns `409 Conflict` immediately instead of entering that wait. The first transition to proven full-object coverage frees that spool's cache entries and releases its admission slot immediately; the spool remains readable from disk until cleanup.

Read responses use a separate manager-wide weighted semaphore derived from the same page/cache sizing: `max(1, floor(max_cache_bytes / page_size))` configured-page units. A response acquires its units before cache lookup or disk buffering and holds them until its streaming body is dropped, including while a yielded chunk is stalled at a slow client. Recovered spools with wider persisted pages acquire `ceil(persisted_page_size / configured_page_size)` units, capped at the full budget. Timeout, cancellation, and deletion release admission through the reader lease. When caching is disabled or smaller than one page, the one-unit minimum serializes page-backed responses rather than allowing unbounded cache-miss buffers.

A trailing partial page may already be present in `spool.dat`, but it is not reader-visible until it becomes a full page or `/api/v1/complete/{key}` publishes it as the final page. A spool accepts at most `max_spool_bytes` (default 8 GiB); an upload that crosses the limit is durably deleted before the server returns `413 Payload Too Large`.

## Read behaviour

Reads acquire response admission, then check the global page cache. Cache misses read the required page bytes from `spool.dat` using positional I/O. Cache entries and response buffers remain separate allocations and separate accounting; the response permit does not alter cache ownership or zero-copy cache-hit slicing.

Only a request without `Range` enters follow mode, starting at byte 0. If the next page has not been written yet, BOBS parks the request until more data arrives, the spool completes, the spool is deleted, or the long-poll timeout fires.

`Range: bytes=X-Y` and `Range: bytes=X-` are bounded requests and return `206 Partial Content`. An open-ended range snapshots its upper bound from the bytes currently servable when the request is resolved, so it does not wait for future writes. For an in-progress spool, a trailing partial page is not servable. Suffix ranges (`bytes=-N`) require a completed spool.

If the follow-mode timeout fires before the first page is available, BOBS returns `307 Temporary Redirect` to a read URL for the same key. If a trusted ingress supplies a valid `X-Forwarded-Prefix`, the redirect preserves that external prefix; otherwise it falls back to `/api/v1/read/{key}`. The redirect includes `Cache-Control: no-store` because the location can depend on request headers. A timeout after streaming has begun aborts the transfer with a response-body error rather than redirecting or reporting a clean end of stream.

Range reads update bounded aggregate coverage tracking so cleanup can detect when the whole object has been served across requests. Adjacent and overlapping progress is coalesced; genuinely fragmented access that exceeds the interval cap conservatively stops aggregate tracking and retains first-read admission. It cannot produce a false full-read result, but one later successfully completed contiguous full-object response provides exact evidence, frees first-read cache and admission, and restores the full-read transition.

## Write-locked spools

A spool can be created with `write_locked: true`. In this state writes are accepted, but reads return `423 Locked` until `/api/v1/complete/{key}` succeeds. Completion makes the final object readable.

The write-lock state is lifecycle metadata in `meta.json` and is recovered on restart.

## Completion and integrity boundary

Each write and completion call first waits on a one-permit, per-spool operation gate before spawning owned work. Waiting callers are cancellable and create no detached task. An admitted owned transaction retains the operation permit and then the lifecycle lock through backend I/O and all metadata, buffer, cache, and notification publication, so a cancelled request cannot overlap a retry with unfinished owned I/O.

The mutation lock order is operation gate, lifecycle lock, then write buffer. Cleanup revalidation, deletion, and read/write activity ordering take the lifecycle lock without taking the operation gate, so there is no reverse acquisition path. Readers release metadata, cache, and file-handle guards before recording lifecycle activity.

`/api/v1/complete/{key}` validates the optional expected size inside the admitted owned transaction, including on idempotent retries. Initial completion atomically commits `Complete` with the exact candidate page layout, length, and XXH3-64 checksum, updates in-memory metadata, and only then clears the volatile trailing buffer and optionally populates the page cache. The cache is never authoritative; readers reconstruct every page, including the exact tail, from `spool.dat`.

A failed metadata commit leaves completion retryable. No create, write, complete, or delete request waits for `fsync`, `fdatasync`, or directory sync. Successful requests establish filesystem-namespace and atomic-metadata state, not stable-storage durability; the always-on coalesced background flush narrows the crash-loss window.

## Recovery

Startup recovery scans `data_dir` for spool directories with `meta.json` sidecars.

Recovery derives the only usable payload path as `<data_dir>/<scanned-key>/spool.dat`; absolute, traversal, stale, and cross-spool `data_path` values from JSON are treated as untrusted metadata and are never statted, opened, written, or deleted. The canonical local entry must be a regular file and is opened without following a final symlink. Missing, symlink, and non-regular payload entries quarantine that key directory unchanged. If the persisted path is stale but the canonical local regular file exists, recovery atomically rewrites `meta.json` to the canonical path before admitting the spool. Corrupt-sidecar cleanup is likewise scoped to the scanned key directory.

Recovery uses one top-level directory scan, reads sidecars individually, and keeps only compact key/activity indexing plus a preferred candidate heap with exact `max_live_spools` capacity. Each sidecar is statted before allocation and is limited to 1 MiB. Oversized sidecars and sidecars with unknown fields are preserved unchanged as unsupported quarantine; malformed known-schema JSON is isolated to per-key corrupt cleanup. Startup applies the configured `max_spool_bytes` to recovery: a canonical payload above that length remains unchanged and unavailable without being opened or migrated. Every candidate within that bound that passes metadata-only lifecycle, migration, and layout checks is no-follow open-preflighted before selection. Failed preflights leave the spool unchanged and the scan continues. Terminal `Complete` descriptors are closed immediately and recovered in the deliberate closed state; only bounded active candidates retain their preflight descriptor through admission. Displaced or excess active handles are closed without payload reads or sidecar rewrites. Selected sidecars and payload sizes are checked again before admission.

- Current `Writing` and `WriteLocked` spools with a supported persisted stride are rebuilt from `spool.dat`; byte-derived metadata in the sidecar is advisory. Legacy and oversized-stride cases follow the salvage or quarantine rules below.
- `Complete` spools without integrity metadata are persistently quarantined as lost and are never served.
- `Complete` spools are accepted only if `spool.dat` satisfies the committed logical length.
- Interrupted metadata temp files are ignored.
- Internal `Creating` and `Deleting` sidecars identify interrupted lifecycle operations. Recovery removes those incomplete key directories; the states are never exposed through active spool APIs.
- Unsafe or unrelated directories are not blindly removed. A recognised UUID or request-ID directory with no BOBS marker is removed only when it is truly empty, as can happen after a pre-marker create crash. Non-empty markerless directories are retained unchanged and quarantined; ordinary orphan cleanup remains restricted to recognised directories containing BOBS spool markers.
- Sidecars now persist each spool's `page_size`. For a legacy sidecar without it, recovery derives a stride only when the sidecar and durable file length determine one safely, then atomically commits the upgraded sidecar before exposing or mutating the spool. A zero-page legacy `Writing` payload is contiguous terminal salvage even when it spans multiple configured pages: recovery resegments its exact file length arithmetically, commits `Complete`, and never loads payload bytes at startup. A legacy `Writing` sidecar with recorded pages but no trustworthy stride remains quarantined unchanged. An active `Writing` spool with a known stride wider than the configured bound is likewise quarantined before payload open or tail loading.
- The removed legacy `Readable` state is migrated to terminal `Complete`. Because its durable payload is terminal and contiguous, recovery resegments it using the currently configured `page_size` rather than inferring the old stride, reconstructs terminal byte/page metadata from `spool.dat`, clears the obsolete write lock, and atomically persists the migrated sidecar before serving it.

## Cleanup rules

A background cleanup task removes expired spools and their key directories, including `spool.dat`, `meta.json`, and interrupted `meta.json.tmp.<uuid>` transaction files. Startup recovery also cleans those private temp entries before marker classification.

Current cleanup triggers are:

- writer inactivity for producers that stop writing without completing (`writer_inactivity_timeout_secs`, default 300);
- read-idle TTL for readable spools that have not served bytes recently, with never-read spools anchored at `readable_at` (`read_idle_ttl_secs`, default 600);
- full-read-complete TTL once bounded aggregate coverage, or one completed contiguous full-object response after fragmented fallback, proves every byte has been served (`full_read_complete_ttl_secs`, default 30).

The legacy `reader_done_ttl_secs` and `unread_ttl_secs` fields are still parsed for config-file compatibility but no longer drive cleanup. `cleanup_sweep_interval_secs` defaults to 30 and must not exceed `writer_inactivity_timeout_secs`, `read_idle_ttl_secs`, or `full_read_complete_ttl_secs`.

Slow readers keep a spool alive only while they continue making read progress. Stalled connections do not protect a spool forever.

Before deleting an expired candidate, cleanup reacquires the spool lifecycle lock and revalidates its state and monotonic read/write activity. Every accepted non-empty HTTP body frame refreshes writer activity under that same lock, even while it remains in the batching buffer. A frame, write, completion, or served byte after the sweep snapshot therefore invalidates the stale deletion candidate.

## HTTP content safety

`content_type` and `content_encoding` supplied to `/api/v1/create` must be valid HTTP header values; malformed values and unknown JSON fields return `400 Bad Request`. Downloads always use `Content-Disposition: attachment` and `X-Content-Type-Options: nosniff`; active document types also receive a restrictive sandbox policy. The unauthenticated `/debug/pprof/profile` endpoint is disabled by default through `enable_pprof: false`.
