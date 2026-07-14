<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Key Behaviours

Understanding these behaviours is crucial for effectively using BOBS.

### 1. Page-based Streaming

Data is organized into fixed-size pages configured via `page_size`. The Rust binary default is `16777216` bytes (16 MiB); the Helm chart intentionally overrides it with `4096` bytes for lower reader-visible latency. Valid pages are in `1..=67108864` bytes (64 MiB), remain below the one-SQE `io_uring` I/O limit, and must not exceed `max_spool_bytes`. A successful `/api/v1/write/{key}/{offset}` has accepted the bytes into `<data_dir>/<key>/spool.dat` before it returns, including any trailing partial page.

Reader visibility is still page-based: a page is visible, cached, and used to notify parked readers only once it is full. Trailing partial-page bytes remain on disk but are not visible until more writes complete the page or `/api/v1/complete/{key}` finalizes the spool.

Larger pages such as 1 MiB, 4 MiB, or 16 MiB may improve throughput, but they also delay reader visibility until that larger page is full. They consume more of the global cache budget per cached page, so cache reach can fall unless `max_cache_bytes` is increased alongside `page_size`. Setting `max_cache_bytes` to `0` disables caching entirely; if a full page is larger than the cache cap, that page simply bypasses the cache and reads fall back to disk. Treat wider pages as a benchmarked tuning choice, not a durability or correctness requirement.

Read response memory is admitted separately from live-spool admission. The manager permits `max(1, floor(max_cache_bytes / page_size))` configured-page responses at once and retains each permit until the client body is consumed or dropped. Slow clients therefore queue instead of retaining one additional page buffer each. Recovery resegments, salvages, or quarantines persisted layouts before admission, so recovered pages are never wider than the current configured page size. With caching disabled, one response remains admitted so disk-backed reads still stream with bounded page memory.

### 2. Long-poll with Timeout

When a follow-mode reader requests a page that has not been written yet, BOBS parks the request using a notification system. If no first page arrives within `long_poll_timeout_ms`, BOBS returns a `307 Temporary Redirect`; clients such as `curl -L` automatically follow it and resume the poll. If the timeout occurs after bytes have started streaming, BOBS aborts the transfer with a response-body error instead of redirecting or returning a clean end of stream.

The initial timeout includes time waiting for read-response admission. A timeout redirect, client cancellation, or spool deletion drops the reader lease and any permit immediately. Long-poll and response admission do not hold metadata, lifecycle, cache, or live-spool admission locks while waiting.

When `ingress.forwardedPrefix.enabled` is enabled, the chart sends the exact public pod prefix (for example, `/download-0`) in `X-Forwarded-Prefix`. The redirect then remains on the public route, such as `/download-0/api/v1/read/<key>`, rather than exposing the rewritten internal route. The NGINX Inc controller derives the prefix in its location snippet. Community ingress-nginx uses its native `nginx.ingress.kubernetes.io/x-forwarded-prefix` annotation and therefore renders one Ingress per replica; its path regex accepts both the short download URL and the redirected public API URL. User-supplied annotations remain on the rendered Ingress resources.

### 3. Follow Mode

Only a read request without a `Range` header enters follow mode, starting at byte `0`. The server pushes new pages as they become available until the spool is finalized or a mid-stream long-poll timeout aborts the transfer with a body error. `Range: bytes=X-` is an open-ended bounded read through the bytes currently servable when the request is resolved; it does not follow later writes.

### 4. Write-locked Mode

A spool created with `write_locked: true` returns `423 Locked` to readers until `/api/v1/complete/{key}` succeeds. Completion is the only transition that makes a write-locked spool readable.

The `WriteLocked` state is lifecycle metadata committed to `<data_dir>/<key>/meta.json`. Current sidecars record their `page_size`, so recovery can reconstruct in-progress byte state from `spool.dat`. Legacy sidecars without that field follow the migration rules under Recovery and Cleanup below.

### 5. Parallel Reads

BOBS supports a single consumer opening multiple concurrent connections for the same spool. Each connection can request independent byte ranges. Read activity is tracked when bytes are actually served, so slow clients keep their spool alive while making progress but stalled connections do not protect a spool forever.

### 6. Sequential Writes

Writes must be strictly sequential. The offset in `/api/v1/write/{key}/{offset}` must exactly match the bytes currently stored. Random-access writes and overwrites are unsupported.

A successful write means BOBS accepted bytes through the kernel/file handle; it does not mean `sync_data()` forced them to stable storage. `/api/v1/complete/{key}` is the durability boundary: an owned transaction that survives caller cancellation syncs `spool.dat`, commits an exact-layout `Completing` marker, then commits `Complete`. `max_spool_bytes` defaults to 8 GiB. Oversize cleanup first atomically checks the write offset and lifecycle state, so a wrong offset or a spool that completed concurrently is rejected without deletion; only an oversized request at the current writable head durably removes the partial spool before returning `413`.

Write and completion mutations are single-flight per spool. Each caller waits on a one-permit operation gate before any owned task is spawned, so cancelling a waiter leaves no detached work or task amplification. Once admitted, the owned transaction keeps the gate and lifecycle lock until I/O and publication reach a stable result, even if its caller disconnects. Cleanup, deletion, and frame/read activity take only the lifecycle lock; no path takes the locks in reverse order.

### 7. Atomic Sidecar Metadata

Metadata is stored as `<data_dir>/<key>/meta.json`. Each commit writes `<data_dir>/<key>/meta.json.tmp`, syncs the temporary file's data, atomically renames it over `meta.json`, and syncs the spool directory. Recovery ignores leftover temporary files and accepts only complete `meta.json` sidecars.

This protocol keeps metadata off the write hot path while still making lifecycle transitions durable. It also suits shared filesystems because each key has its own directory and sidecar, with one writer per spool rather than one global metadata writer.

Create also crosses the key-directory durability boundary before acknowledgement: BOBS waits for admission before reserving the key, syncs the empty data file and key directory, commits an internal `Creating` sidecar, fsyncs `data_dir`, and only then commits and publishes live metadata. A key already tracked in any lifecycle state, including retryable `Deleting`, returns `409 Conflict` before admission or the per-key gate. Cancellation before admission leaves no reservation. After reservation, a detached transaction finishes publication or rollback even if the client disconnects, so callers that lose the response should retry with the same request ID.

Deletion removes sidecar metadata and the key directory, then fsyncs `data_dir` before returning success. Cache entries, admission, and manager membership remain held until that parent-directory sync succeeds, so failed deletion remains tracked and retryable.

Completion is fail-stop once it begins. The owned transaction continues if the request is cancelled, and internal `Completing` blocks writes while the final metadata commit is uncertain. A failed pre-marker attempt can be retried through `/api/v1/complete/{key}`; a durable marker is either finalized during recovery or quarantined unchanged if it disagrees with `spool.dat`. `Completing` is never client-selectable.

### 8. Recovery and Cleanup

On restart, BOBS scans `<data_dir>` for key directories containing `meta.json`. A recognised UUID or request-ID directory without any BOBS marker is removed and parent-fsynced only when it is truly empty; non-empty markerless directories are retained unchanged and quarantined. For current `Writing` and `WriteLocked` spools, `spool.dat` is authoritative for byte state and persisted byte counters are advisory. A valid `Completing` marker carries the exact candidate byte/page layout and is finalized to `Complete`; if that layout and `spool.dat` disagree, BOBS leaves both unchanged, logs the quarantine, and does not expose the key through spool APIs.

Every current sidecar records its spool's `page_size`. A completed spool is validated against its historical layout and resegmented to the current configured page size before serving; a changed terminal layout is committed atomically. The migration is arithmetic and does not read page payloads during startup, so even a persisted 128 MiB sparse page can migrate without a 128 MiB allocation or read request. The configured target is at most 64 MiB and cannot exceed `max_spool_bytes`.

An active `Writing` spool with a page size wider than the current configured bound is quarantined before BOBS opens its payload or loads a partial tail. Its `meta.json` and `spool.dat` remain unchanged. An oversized or missing-stride `WriteLocked` spool is safe to terminalise because it cannot accept more writes: BOBS salvages the exact file length as bounded `Complete`, preserves the write-lock flag, and persists the resegmented metadata. Legacy `Readable` data follows the same terminal migration. Ambiguous legacy `Writing` data is not guessed or deleted.

Persisted `data_path` is untrusted during recovery. BOBS derives the payload location solely from the scanned directory key as `<data_dir>/<key>/spool.dat`; it never follows an absolute, traversal, or cross-spool path stored in JSON. The local entry must be a regular file and is opened without following a final symlink. Missing, symlink, and non-regular entries are quarantined non-destructively. A stale path from a relocated `data_dir` is atomically rewritten only when the canonical local regular file exists, and corrupt cleanup remains confined to the scanned key directory.

Recovery admission uses the same non-zero `max_live_spools` bound as normal creation and applies it uniformly to in-progress and complete spools. Candidates with newer persisted activity are considered first, with the key as a deterministic tie-breaker. Metadata-only checks run before admission, and a fixed refill reserve replaces selected candidates that fail. Entries outside the bounded ordering window stay durable and unopened; increasing capacity on a later restart can admit more of this quarantined set.

The top-level directory scan is a true bounded stream. A fixed-capacity channel backpressures the blocking filesystem producer, the async consumer reads and drops one sidecar at a time, and candidate ordering stores at most `max_live_spools` summaries plus the fixed reserve. Recovery memory is O(`max_live_spools` + scan-channel capacity), independent of directory cardinality. `meta.json` remains limited to 1 MiB and its open-file size is checked before allocating a read buffer. Oversized or unknown-field sidecars are preserved unchanged as unsupported; malformed known-schema sidecars are handled as per-key corruption.

A background task periodically sweeps the spool manager and deletes spools based on three triggers:

- **Writer Inactivity**: The producer stopped writing without completing the spool (`writer_inactivity_timeout_secs`, default 300).
- **Read Idle TTL**: The spool has not served bytes for `read_idle_ttl_secs` (default 600); never-read spools are anchored when they become readable.
- **Full Read TTL**: Bounded coverage tracking, or an exact completed full-object response after fragmented fallback, has proved every byte was served; `full_read_complete_ttl_secs` (default 30) has elapsed since the latest read activity.

`cleanup_sweep_interval_secs` defaults to 30 and must not exceed any active cleanup timeout. The deprecated `reader_done_ttl_secs` and `unread_ttl_secs` fields remain parseable but do not drive cleanup. Expiry removes the key directory, including `spool.dat`, `meta.json`, and interrupted `meta.json.tmp`.

Cleanup revalidates each expired candidate under the spool lifecycle lock immediately before deletion. Every accepted non-empty HTTP body frame refreshes writer activity under the same lock before it enters the batching buffer. A frame, write, completion, or served byte after the sweep snapshot invalidates that candidate rather than deleting from stale eligibility data.

Coverage tracking uses missing byte ranges rather than per-byte state, so large objects do not require large memory allocations. Adjacent and overlapping pre-completion progress is coalesced before the interval cap is applied, keeping ordinary sequential follow reads O(1). If genuinely fragmented access exceeds the cap, BOBS conservatively falls back to the longer idle TTL, retains the first-read admission slot, and cannot falsely report completion. A later successfully completed contiguous full-object response restores exact complete coverage, frees the first-read cache and admission slot immediately, and starts the short TTL while the spool remains disk-readable.

### 9. HTTP/2 Support

The server supports both HTTP/1.1 and HTTP/2 (h2c cleartext). Using HTTP/2 is recommended for high-concurrency streaming to benefit from request multiplexing.

### 10. Graceful Shutdown

On SIGTERM or Ctrl+C, BOBS first stops accepting connections and asks Hyper to shut down every accepted connection gracefully. Idle HTTP/1.1 keep-alive sockets close promptly, HTTP/2 connections stop accepting new streams, and active request bodies, response streams, and handlers that own spool mutations can finish.

The HTTP drain has a fixed 25-second deadline. Connections still active at the deadline are aborted before storage and telemetry teardown, so a stalled peer cannot block process exit forever. The chart leaves Kubernetes' termination grace period unset; the standard 30-second default leaves time after the HTTP deadline for forced aborts and final teardown. Keep any deployment-level termination grace period greater than 25 seconds.
