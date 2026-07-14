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
- **Page Cache**: A global byte-capped FIFO cache for hot data, keyed by `(spool_key, page_idx)`. `max_cache_bytes` accounts the logical bytes of bounded cache-owned page allocations across all spools, excluding allocator overhead. Cache admission may copy a page solely to prevent a small slice from retaining an oversized HTTP frame backing; the preceding frame-to-disk append still uses the transport-backed bytes directly, and cache hits share the isolated allocation without another copy. Setting `max_cache_bytes` to `0` disables caching without copying, and a page rejected because it exceeds the cap likewise bypasses the cache without copying while remaining readable from disk. Omitted `max_live_spools` derives as `max(1, max_cache_bytes / page_size)` (16 with binary defaults), while the chart explicitly sets 256 for its 4 KiB/1 MiB profile. `/api/v1/create` waits at most `create_admission_timeout_ms` (default 5000) for admission.
- **Read-response admission**: A manager-wide weighted semaphore bounds page allocations retained by slow or unconsumed response bodies. Its configured-page budget is `max(1, floor(max_cache_bytes / page_size))`. A read acquires admission before cache lookup or disk buffering and keeps it until the HTTP body is dropped. Recovery either resegments, salvages, or quarantines persisted layouts before admission, so an admitted spool never has pages wider than the current configured page size. Long-poll timeout, cancellation, and deletion release queued or held admission without taking cache, lifecycle, or live-spool admission locks. Cache-disabled deployments therefore still allow one bounded page response rather than unbounded disk-read buffers.

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
4. **Read**: `/api/v1/read/{key}` checks the page cache and then `spool.dat`. A trailing partial page remains hidden until it fills or completion finalizes it. Adjacent and overlapping progress is coalesced in bounded coverage state. The first proven full-object read frees that spool's first-read cache and admission slot immediately while the object remains disk-readable; if fragmentation exceeds the interval cap, admission is retained until a completed contiguous full-object response provides exact recovery evidence.
5. **Complete**: `/api/v1/complete/{key}` validates any `expected_size`, including on idempotent retries, then launches an owned transaction that survives request cancellation. It syncs `spool.dat`, commits a durable `Completing` marker with the exact final page layout, commits `Complete`, updates memory, and only then discards the volatile tail buffer.
6. **Lifecycle**: Active spools move from `Writing` (or `WriteLocked`) through internal retryable `Completing` to `Complete`, then `Deleting`. `Creating` and `Completing` are not client-selectable states, and a write-locked spool becomes readable only through completion.

### Persistence Contract

BOBS persists two things with different authority:

- `spool.dat` stores accepted bytes. For in-progress `Writing` and `WriteLocked` spools, it is authoritative after a BOBS restart.
- `meta.json` stores lifecycle and completed-object byte metadata. It is committed atomically at creation and completion, and removed before durable key-directory deletion.

Ordinary writes deliberately do not update a durable high-water mark, and completed pages do not commit per-page metadata. Adding a mandatory per-write or per-page checkpoint would put metadata commits back on the write hot path, which this design avoids.

While a spool is still `Writing` or `WriteLocked`, persisted byte-derived metadata such as `total_bytes_written`, `total_pages`, and `final_page_size` is advisory and may be stale. Recovery derives those values from `spool.dat`: the logical accepted length comes from the data file length, and page counts and partial-page state are reconstructed from that length.

The in-progress durability invariant is recovery from a BOBS restart, not survival of a node or storage crash before `/api/v1/complete/{key}`. A successful `/api/v1/write/{key}/{offset}` requires kernel/file-handle acceptance but not `sync_data()`. Completion is the finished-object durability boundary.

Completion cannot be cancelled by a disconnected caller once its owned transaction starts. The transaction syncs `spool.dat` before atomically committing a `Completing` sidecar whose byte and page fields describe the exact terminal layout, then atomically commits `Complete`. Each sidecar commit writes `meta.json.tmp`, syncs it, renames it to `meta.json`, and syncs the spool directory. Final-page cache population is optional; disk remains authoritative for the exact tail.

Create acknowledgement has an additional parent-directory durability boundary. BOBS first makes the empty data inode and its name durable, commits a `Creating` sidecar, and syncs `data_dir` so the key-directory link is durable. Only then does it commit live `Writing` or `WriteLocked` metadata and publish the spool in memory. Cancellation while waiting for admission leaves no reservation; cancellation after reservation does not stop the detached transaction, which either publishes durably or rolls back. A retry with the same request ID may therefore return `409 Conflict` after the original response was lost.

Delete acknowledgement is likewise delayed until removal is durable. BOBS removes sidecar metadata and the key directory, then fsyncs `data_dir`. Cache entries, manager membership, and admission accounting remain held until that parent-directory fsync succeeds, so a failed acknowledgement can be retried without exposing a falsely completed deletion.

### Recovery and Shared Filesystems

Startup recovery scans `<data_dir>` for key directories with `meta.json`, orders every candidate by most recent persisted activity (`last_read_at`, `readable_at`, `last_write_at`, or `created_at`), newest first, then lexically by key, and admits at most `max_live_spools` entries. The same bound applies uniformly to in-progress and completed spools, making restarts after a capacity reduction deterministic.

The directory entry selected by the top-level scan is the recovery trust boundary. Recovery derives `<data_dir>/<scanned-key>/spool.dat` itself and never uses persisted `data_path` for a stat, open, write, or delete. Absolute paths, traversal paths, and paths naming another spool are inert compatibility metadata. The canonical local entry must be a regular file and is opened without following a final symlink; a missing, symlink, or non-regular entry leaves the local key directory quarantined without mutation. Data-directory relocation is supported: when the canonical local regular file exists, a stale persisted path is atomically rewritten to the current canonical path during admission. Corrupt-sidecar cleanup and all metadata migration stay scoped to the scanned key directory.

The top-level directory scan streams entries through a fixed-capacity channel. Recovery reads and drops one sidecar at a time, while candidate ordering retains at most `max_live_spools` summaries and successful preflight handles. Memory is therefore O(`max_live_spools` + scan-channel capacity), not O(the number of directories). Sidecar length is checked from the open file before allocating, with a fixed 1 MiB maximum. Sidecars above that limit or containing unknown fields are left byte-for-byte intact and unavailable. Malformed known-schema JSON remains isolated corrupt metadata and only its key directory is removed.

Recovery first performs metadata-only lifecycle, migration, and layout validation. Incomplete `Creating` and `Deleting` transactions and corrupt entries are cleaned without consuming admission; unsafe or ambiguous metadata remains intact but unavailable. A candidate that passes those checks has its canonical payload opened without following a final symlink and its descriptor length verified. A failed open or size preflight leaves the spool intact, does not occupy the preferred set, and the streaming scan continues.

The preferred heap and retained handle set have exact `max_live_spools` capacity. Displaced or excess candidates are closed after preflight without payload reads or sidecar rewrites. Selected sidecars and descriptor lengths are checked again before admission; a validation or bounded tail-read failure releases the handle and permit, then recovery continues with the next retained candidate. Only an admitted in-progress spool has its trailing partial page loaded.

A `Complete` sidecar is validated using its historical layout and then resegmented arithmetically to the current configured `page_size`; if the stride changes, BOBS atomically commits the new terminal metadata before serving it. This migration does not read the payload at startup, including for a sparse file whose old page size exceeds 64 MiB. Because configuration requires `page_size <= min(64 MiB, max_spool_bytes)`, no recovered page can exceed either bound.

An active `Writing` spool keeps its historical stride only when it is no wider than the current configured page size. A wider persisted stride is quarantined after no-follow metadata inspection but before the payload is opened or a sparse tail can be read, leaving `meta.json` and `spool.dat` unchanged for operator migration or a restart with a compatible configuration. A zero-page legacy `Writing` payload is contiguous terminal salvage even when its file spans multiple configured pages: BOBS arithmetically resegments the exact length, commits bounded `Complete` metadata, and rejects further writes without reading payload bytes at startup. An oversized or missing-stride `WriteLocked` spool is salvaged the same way because it cannot accept writes; its write-lock provenance is preserved.

A valid durable `Completing` marker is finalized to `Complete` from its exact candidate layout; marker/data disagreement leaves the sidecar and data untouched and excludes the key from the live manager. Metadata-valid candidates outside the exact preferred set likewise remain durable but unavailable after their bounded preflight handles are closed. A later restart with more capacity can recover more of that quarantined set. Startup emits per-key warnings and a summary warning with configured, recovered, and quarantined counts; rejected invalid candidates do not inflate the quarantine count, and `bobs.recovery.spools` exports the same snapshot.

Legacy sidecars without `page_size` are migrated only when the durable layout has a safe interpretation. Removed `Readable` state and zero-page legacy `Writing` data are terminal and are resegmented to the current configured page size. Legacy `WriteLocked` data is also salvaged as bounded `Complete`. A legacy `Writing` sidecar with recorded pages but no authoritative historical stride is never guessed: BOBS leaves its files unchanged, excludes the key from the live manager, and continues startup.

The layout is friendly to shared filesystems and multi-BOBS deployments because each object has its own directory and sidecar, and each spool has a single writer. Independent keys can be created, completed, recovered, and deleted without a global metadata database or cross-key write serialization. Correct routing is still required: create, write, complete, and read traffic for a key must reach a BOBS instance that can see the same `<data_dir>/<key>` files.

Cleanup removes expired `spool.dat` and `meta.json` files for admitted spools after writer inactivity, read-idle, or full-read-complete deadlines. Immediately before deletion it revalidates lifecycle state and monotonic read/write activity under the spool lifecycle lock. Every accepted non-empty HTTP body frame refreshes writer activity under that lock before batching, so frame receipt or other activity after the sweep snapshot cancels stale eligibility. `cleanup_sweep_interval_secs` must not exceed any active cleanup timeout; deprecated `reader_done_ttl_secs` and `unread_ttl_secs` values remain parseable but do not drive cleanup. Excess recovery candidates are absent from the live manager, so cleanup does not open or remove them during that process lifetime.

### Metadata backend

Sidecar `meta.json` files are the only current metadata backend. Current BOBS does not read, migrate, export, or import legacy native redb state.
