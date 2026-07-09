<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Key Behaviours

Understanding these behaviours is crucial for effectively using BOBS.

### 1. Page-based Streaming

Data is organized into fixed-size pages, configured via `page_size` and defaulting to `4096`. A successful `/write` has already accepted the bytes into `<data_dir>/<key>/spool.dat` before it returns, including any trailing partial page.

Reader visibility is still page-based: a page is visible, cached, and used to notify parked readers only once it is completely full. Trailing partial-page bytes remain on disk in `spool.dat` but are not visible to readers until more writes complete the page or the writer calls `/complete` to finalize the spool. This ensures readers always receive consistent, non-torn data.

Larger pages such as 1 MiB, 4 MiB, or 16 MiB may improve throughput, but they also delay reader visibility until that larger page is full. They consume more of the global cache budget per cached page, so cache reach can fall unless `max_cache_bytes` is increased alongside `page_size`. Setting `max_cache_bytes` to `0` disables caching entirely; if a full page is larger than the cache cap, that page simply bypasses the cache and reads fall back to disk. Treat wider pages as a benchmarked tuning choice, not a durability or correctness requirement.

### 2. Long-poll with Timeout

When a reader requests data that has not been written yet, BOBS parks the request using a notification system. To prevent idle timeouts from network infrastructure, such as Kubernetes Ingress or load balancers, BOBS returns a `307 Temporary Redirect` if no data arrives within `long_poll_timeout_ms`. Clients like `curl -L` will automatically follow the redirect and resume the poll.

### 3. Follow Mode

A read request without a `Range` header enters follow mode from byte `0`, or from byte `X` with `Range: bytes=X-`. The server keeps the stream open and pushes new pages as they become available until the spool is finalized.

### 4. Write-locked Mode

A spool can be created with `write_locked: true`. In this state, any attempt to read results in a `423 Locked` error until the writer calls `/complete` or otherwise makes the spool readable. This is useful for preventing consumers from seeing any data until the payload is ready for consumption.

The write-locked/readable state is lifecycle metadata and is committed to `<data_dir>/<key>/meta.json` when it changes. If BOBS restarts while the spool is still `WriteLocked` or `Writing`, byte-derived metadata in the sidecar may be stale; recovery inspects `spool.dat` to determine the current length, page counts, and final page size.

### 5. Parallel Reads

BOBS supports a single consumer opening multiple concurrent connections for the same spool. Each connection can request independent byte ranges. Read activity is tracked when bytes are actually served, so slow clients keep their spool alive while making progress but stalled connections do not protect a spool forever.

### 6. Sequential Writes

Writes must be strictly sequential. The `offset` provided in the `/write` request must exactly match the total number of bytes currently stored in the spool. Random-access writes or overwrites are not supported.

A successful `/write` means BOBS accepted the bytes into `spool.dat` through the kernel/file handle before returning. It does not mean the data has been forced to stable storage with `sync_data()`: the in-progress spool contract is designed for BOBS restart recovery from `spool.dat`, not for a node or storage crash before completion. `/complete` remains the durability boundary and syncs `spool.dat` data before committing final metadata to `meta.json`.

### 7. Atomic Sidecar Metadata

Metadata is stored as `<data_dir>/<key>/meta.json`. Each commit writes `<data_dir>/<key>/meta.json.tmp`, syncs the temporary file's data, atomically renames it over `meta.json`, and syncs the spool directory. Recovery ignores leftover temporary files and accepts only complete `meta.json` sidecars.

This protocol keeps metadata off the write hot path while still making lifecycle transitions durable. It also suits shared filesystems because each key has its own directory and sidecar, with one writer per spool rather than one global metadata writer.

### 8. Recovery and Cleanup

On restart, BOBS scans `<data_dir>` for key directories containing `meta.json`. For in-progress `Writing` and `WriteLocked` spools, `spool.dat` is authoritative for byte state. Any persisted `total_bytes_written`, `total_pages`, or `final_page_size` for those states is advisory only and recovery recomputes it from the file.

A background task periodically sweeps the spool manager and deletes spools based on three triggers:

- **Writer Inactivity**: The producer stopped writing without completing the spool.
- **Read Idle TTL**: The spool is readable but has not served bytes for `read_idle_ttl_secs`. For never-read spools, this timer starts when the spool becomes readable.
- **Full Read TTL**: BOBS has served every byte of the object at least once, possibly across multiple range requests, and `full_read_complete_ttl_secs` has elapsed since the latest read activity.

Cleanup TTL semantics are unchanged by sidecar metadata. Expired cleanup removes the key directory, including `spool.dat`, `meta.json`, and any interrupted `meta.json.tmp`.

Coverage tracking uses missing byte ranges rather than per-byte state, so large objects do not require large memory allocations. If range access is extremely fragmented, BOBS falls back to the longer idle TTL rather than risking early deletion.

### 9. HTTP/2 Support

The server supports both HTTP/1.1 and HTTP/2 (h2c cleartext). Using HTTP/2 is recommended for high-concurrency streaming to benefit from request multiplexing.
