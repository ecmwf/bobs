<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Architecture

BOBS is built around one directory per object key and an asynchronous filesystem backend.

### Internal Components

- **Spool**: The core entity representing a data stream. A spool owns its byte file, lifecycle metadata, page visibility state, reader notifications, and cleanup timestamps.
- **On-disk layout**: Each key is stored under `<data_dir>/<key>/`. Payload bytes live in `<data_dir>/<key>/spool.dat`; lifecycle and byte-derived metadata live in `<data_dir>/<key>/meta.json`.
- **FileIO**: An abstraction for asynchronous disk I/O. On Linux, the default backend uses a sharded `io_uring` ring pool. It requires a Linux 5.11+ kernel because BOBS submits operations against raw file descriptors, plus a runtime policy that permits `io_uring_setup`. Builds with the `tokio-fileio-fallback` Cargo feature, and non-Linux builds, use the Tokio/blocking positional-file backend instead and ignore the `io_uring` tuning fields.
- **Metadata store**: Sidecar metadata is committed atomically by writing `meta.json.tmp`, syncing that temporary file's data, atomically renaming it over `meta.json`, and syncing the spool directory. Startup recovery ignores leftover temporary files and reads only complete sidecars.
- **SpoolManager**: A central registry, using `DashMap`, that tracks active spools and reconstructs them from sidecar files during startup.
- **Page Cache**: A global byte-capped FIFO cache for hot data. `max_cache_bytes` is shared across all spools; `0` disables caching and oversized pages bypass it. Omitted `max_live_spools` derives as `max(1, max_cache_bytes / page_size)` (16 with binary defaults), while the chart explicitly sets 256 for its 4 KiB/1 MiB profile. `/api/v1/create` waits at most `create_admission_timeout_ms` (default 5000) for admission.

### Linux `io_uring` routing

Default Linux builds route file operations through a fixed-size pool of `io_uring` shards. `io_uring_shards` can set the shard count explicitly from `1` through `256`, inclusive; when unset, BOBS resolves it to `max(1, num_cpus / 4)`. `io_uring_queue_capacity` defaults to `1024` per shard and must be between `1` and Tokio's `Semaphore::MAX_PERMITS` (`usize::MAX >> 3`). Invalid shard or queue bounds fail startup with `ConfigurationError` instead of allocating pathological ring/thread counts, overflowing, or panicking. Validation also runs in fallback builds, although fallback I/O otherwise ignores the settings. Key-to-shard assignment uses a stable SipHash-1-3 hash with fixed keys, not Rust's randomized `Hash` state, so the same object key maps to the same shard for a given shard count across restarts and builds.

Data-file operations and metadata sidecar commits for the same object are routed by the same object key and therefore use the same shard. This keeps a key's `spool.dat` work and its `meta.json` create/rename/fsync work on one ring while still allowing independent keys to spread across shards.

Each shard has a bounded submission channel. When it fills, submitters wait for capacity rather than growing an unbounded backlog. The driver drains that channel only while it can bound pending plus in-flight SQE work by the ring capacity, while keeping each two-SQE metadata phase atomic. Sustained arrivals are therefore backpressured without disabling batching.

CPU pinning is not enabled by default. If `io_uring_setup` is blocked, use a runtime seccomp/sysctl policy that permits it or build with `--features tokio-fileio-fallback`.

This backend change does not alter the HTTP API or the on-disk `<data_dir>/<key>/spool.dat` plus `<data_dir>/<key>/meta.json` layout, so it does not require an on-disk migration.

Future Linux optimizations that are intentionally not implemented yet include `IORING_REGISTER_FILES`, `IORING_REGISTER_BUFFERS`, and `IORING_SETUP_ATTACH_WQ`.

### Data Flow

1. **Create**: BOBS uses a valid `X-Polytope-Job-Id` request ID as the key or generates a fallback UUIDv4. Request IDs may use uppercase or lowercase Crockford base32; BOBS normalizes the canonical key to lowercase. A key already tracked in any lifecycle state, including retryable `Deleting`, returns `409 Conflict` before admission or per-key serialization. Otherwise BOBS waits for admission, atomically reserves `<data_dir>/<key>/`, creates and syncs `spool.dat`, syncs the key directory, commits an internal `Creating` recovery sidecar, syncs `data_dir`, and commits live `Writing` or `WriteLocked` metadata before returning `201 Created`. Once reservation starts, the create transaction finishes or rolls back even if the client disconnects.
2. **Write**: Data arrives through `/api/v1/write/{key}/{offset}`. Accepted bytes are appended to `spool.dat` before the request returns; the data file is authoritative for in-progress bytes.
3. **Page visibility**: Complete pages become reader-visible, enter the global FIFO cache when they fit, and notify parked readers.
4. **Read**: `/api/v1/read/{key}` checks the page cache and then `spool.dat`. A trailing partial page remains hidden until it fills or completion finalizes it.
5. **Complete**: `/api/v1/complete/{key}` validates any `expected_size`, including on idempotent retries. Initial completion publishes the trailing partial page, syncs data, and atomically commits completed metadata. Once completion starts, internal `Completing` state rejects writes until completion succeeds or is retried.
6. **Lifecycle**: Active spools move from `Writing` (or `WriteLocked`) through internal retryable `Completing` to `Complete`, then `Deleting`. `Creating` and `Completing` are not client-selectable states, and a write-locked spool becomes readable only through completion.

### Persistence Contract

BOBS persists two things with different authority:

- `spool.dat` stores accepted bytes. For in-progress `Writing` and `WriteLocked` spools, it is authoritative after a BOBS restart.
- `meta.json` stores lifecycle and completed-object byte metadata. It is committed atomically at creation and completion, and removed before durable key-directory deletion.

Ordinary writes deliberately do not update a durable high-water mark, and completed pages do not commit per-page metadata. Adding a mandatory per-write or per-page checkpoint would put metadata commits back on the write hot path, which this design avoids.

While a spool is still `Writing` or `WriteLocked`, persisted byte-derived metadata such as `total_bytes_written`, `total_pages`, and `final_page_size` is advisory and may be stale. Recovery derives those values from `spool.dat`: the logical accepted length comes from the data file length, and page counts and partial-page state are reconstructed from that length.

The in-progress durability invariant is recovery from a BOBS restart, not survival of a node or storage crash before `/api/v1/complete/{key}`. A successful `/api/v1/write/{key}/{offset}` requires kernel/file-handle acceptance but not `sync_data()`. Completion is the finished-object durability boundary.

Completed metadata is durable through the sidecar protocol: write `meta.json.tmp`, sync it, rename it to `meta.json`, and sync the parent directory. Recovery treats `meta.json` as all-or-nothing and ignores any leftover `meta.json.tmp` from an interrupted commit.

Create acknowledgement has an additional parent-directory durability boundary. BOBS first makes the empty data inode and its name durable, commits a `Creating` sidecar, and syncs `data_dir` so the key-directory link is durable. Only then does it commit live `Writing` or `WriteLocked` metadata and publish the spool in memory. Cancellation while waiting for admission leaves no reservation; cancellation after reservation does not stop the detached transaction, which either publishes durably or rolls back. A retry with the same request ID may therefore return `409 Conflict` after the original response was lost.

Delete acknowledgement is likewise delayed until removal is durable. BOBS removes sidecar metadata and the key directory, then fsyncs `data_dir`. Cache entries, manager membership, and admission accounting remain held until that parent-directory fsync succeeds, so a failed acknowledgement can be retried without exposing a falsely completed deletion.

### Recovery and Shared Filesystems

Startup recovery scans `<data_dir>` for key directories with `meta.json`. Internal `Creating` and `Deleting` sidecars are cleaned up as interrupted lifecycle operations, with the parent directory synced after removal. A recognised UUID or request-ID directory with no BOBS marker is removed and parent-fsynced only if it is truly empty; non-empty markerless directories are retained unchanged and quarantined. Current in-progress `Writing`, `WriteLocked`, and retryable `Completing` spools with a persisted `page_size` are rebuilt from `spool.dat`; completed spools validate that the data file still satisfies the committed logical length before serving.

Current sidecars persist the spool's `page_size`, so a later configuration change cannot reinterpret existing page offsets. When a legacy sidecar has no page size, recovery migrates only cases with a safe interpretation and atomically commits the upgraded sidecar before exposing or mutating the spool.

The removed legacy `Readable` state is terminal: recovery resegments its contiguous durable bytes using the currently configured `page_size`, reconstructs terminal byte/page metadata, changes the state to `Complete`, clears the obsolete write lock, and persists the migration before serving it. A legacy `WriteLocked` spool is also salvaged as `Complete` from its exact durable file length, preserving its write-lock provenance while preventing further writes.

A legacy `Writing` spool whose active page stride cannot be determined safely is quarantined instead of guessed. BOBS leaves its `meta.json` and `spool.dat` unchanged, excludes it from the live manager, and continues startup so other valid spools remain available. Malformed legacy `WriteLocked` sidecars receive the same non-destructive quarantine treatment. Other inconsistent terminal layouts are left intact and can still fail recovery rather than risk serving misinterpreted bytes.

The layout is friendly to shared filesystems and multi-BOBS deployments because each object has its own directory and sidecar, and each spool has a single writer. Independent keys can be created, completed, recovered, and deleted without a global metadata database or cross-key write serialization. Correct routing is still required: create, write, complete, and read traffic for a key must reach a BOBS instance that can see the same `<data_dir>/<key>` files.

Cleanup removes expired `spool.dat` and `meta.json` files after writer inactivity, read-idle, or full-read-complete deadlines. Immediately before deletion it revalidates lifecycle state and monotonic read/write activity under the spool lifecycle lock. Every accepted non-empty HTTP body frame refreshes writer activity under that lock before batching, so frame receipt or other activity after the sweep snapshot cancels stale eligibility. `cleanup_sweep_interval_secs` must not exceed any active cleanup timeout; deprecated `reader_done_ttl_secs` and `unread_ttl_secs` values remain parseable but do not drive cleanup.

### Metadata backend

Sidecar `meta.json` files are the only current metadata backend. Current BOBS does not read, migrate, export, or import legacy native redb state.
