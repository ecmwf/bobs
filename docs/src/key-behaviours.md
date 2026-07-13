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

### 2. Long-poll with Timeout

When a follow-mode reader requests a page that has not been written yet, BOBS parks the request using a notification system. If no first page arrives within `long_poll_timeout_ms`, BOBS returns a `307 Temporary Redirect`; clients such as `curl -L` automatically follow it and resume the poll. If the timeout occurs after bytes have started streaming, BOBS aborts the transfer with a response-body error instead of redirecting or returning a clean end of stream.

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

Each current sidecar records its spool's `page_size`. A legacy `Readable` sidecar is terminal and migrates to `Complete` by resegmenting the contiguous durable bytes with the configured page size. A legacy `WriteLocked` spool is likewise salvaged from its exact durable length as `Complete`, retains its write-lock provenance, and cannot accept further writes. Both migrations atomically persist their page size and reconstructed terminal metadata before serving.

An ambiguous legacy `Writing` partial spool is quarantined without mutation: BOBS leaves `meta.json` and `spool.dat` intact, does not expose the key through spool APIs, and continues startup. A malformed legacy `WriteLocked` sidecar is handled the same way. BOBS does not guess a stride or delete these bytes.

A background task periodically sweeps the spool manager and deletes spools based on three triggers:

- **Writer Inactivity**: The producer stopped writing without completing the spool (`writer_inactivity_timeout_secs`, default 300).
- **Read Idle TTL**: The spool has not served bytes for `read_idle_ttl_secs` (default 600); never-read spools are anchored when they become readable.
- **Full Read TTL**: Every byte has been served and `full_read_complete_ttl_secs` (default 30) has elapsed since the latest read activity.

`cleanup_sweep_interval_secs` defaults to 30 and must not exceed any active cleanup timeout. The deprecated `reader_done_ttl_secs` and `unread_ttl_secs` fields remain parseable but do not drive cleanup. Expiry removes the key directory, including `spool.dat`, `meta.json`, and interrupted `meta.json.tmp`.

Cleanup revalidates each expired candidate under the spool lifecycle lock immediately before deletion. Every accepted non-empty HTTP body frame refreshes writer activity under the same lock before it enters the batching buffer. A frame, write, completion, or served byte after the sweep snapshot invalidates that candidate rather than deleting from stale eligibility data.

Coverage tracking uses missing byte ranges rather than per-byte state, so large objects do not require large memory allocations. If range access is extremely fragmented, BOBS falls back to the longer idle TTL rather than risking early deletion.

### 9. HTTP/2 Support

The server supports both HTTP/1.1 and HTTP/2 (h2c cleartext). Using HTTP/2 is recommended for high-concurrency streaming to benefit from request multiplexing.
