<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Architecture

BOBS is built around one directory per object key and an asynchronous filesystem backend.

### Internal Components

- **Spool**: The core entity representing a data stream. A spool owns its byte file, lifecycle metadata, page visibility state, reader notifications, and cleanup timestamps.
- **On-disk layout**: Each key is stored under `<data_dir>/<key>/`. Payload bytes live in `<data_dir>/<key>/spool.dat`; lifecycle and byte-derived metadata live in `<data_dir>/<key>/meta.json`.
- **FileIO**: An abstraction for asynchronous disk I/O. On Linux, the default backend uses a sharded `io_uring` ring pool. It requires a Linux 5.11+ kernel because BOBS submits operations against raw file descriptors, plus a runtime policy that permits `io_uring_setup`. Builds with the `tokio-fileio-fallback` Cargo feature, and non-Linux builds, use the Tokio/blocking positional-file backend instead.
- **Metadata store**: Sidecar metadata is committed atomically by writing `meta.json.tmp`, syncing that temporary file's data, atomically renaming it over `meta.json`, and syncing the spool directory. Startup recovery ignores leftover temporary files and reads only complete sidecars.
- **SpoolManager**: A central registry, using `DashMap`, that tracks active spools and reconstructs them from sidecar files during startup.
- **Page Cache**: A global byte-capped FIFO cache that minimizes disk reads for hot data being consumed immediately after it is written. Entries are keyed by `(spool_key, page_idx)`, and `max_cache_bytes` is the total cache budget across all spools. Admitted pages are copied once into dedicated page-sized allocations so slices cannot retain larger HTTP frames outside that budget; the disk write still uses the original transport-backed bytes. Cache hits share the isolated allocation without another copy. Setting `max_cache_bytes` to `0` disables caching without copying; pages larger than the byte cap are valid but likewise bypass the cache.
- **Read-response admission**: A manager-wide weighted semaphore bounds page allocations retained by slow or unconsumed response bodies. Its configured-page budget is `max(1, floor(max_cache_bytes / page_size))`. A read acquires admission before cache lookup or disk buffering and keeps it until the HTTP body is dropped. Recovered spools with wider persisted pages consume proportionally more units, capped at the full budget. Long-poll timeout, cancellation, and deletion release queued or held admission without taking cache, lifecycle, or live-spool admission locks. Cache-disabled deployments therefore still allow one bounded page response rather than unbounded disk-read buffers.

### Linux `io_uring` routing

Default Linux builds route file operations through a fixed-size pool of `io_uring` shards. `io_uring_shards` can set the shard count explicitly from `1` through `256`, inclusive. If `io_uring_shards` is unset, BOBS resolves it to `max(1, num_cpus / 4)`. Key-to-shard assignment uses a stable SipHash-1-3 hash with fixed keys, not Rust's randomized `Hash` state, so the same object key maps to the same shard for a given shard count across restarts and builds.

Data-file operations and metadata sidecar commits for the same object are routed by the same object key and therefore use the same shard. This keeps a key's `spool.dat` work and its `meta.json` create/rename/fsync work on one ring while still allowing independent keys to spread across shards.

CPU pinning is not enabled by default. If `io_uring_setup` is blocked, use a runtime seccomp/sysctl policy that permits it or build with `--features tokio-fileio-fallback`.

This backend change does not alter the HTTP API or the on-disk `<data_dir>/<key>/spool.dat` plus `<data_dir>/<key>/meta.json` layout, so it does not require an on-disk migration.

Future Linux optimizations that are intentionally not implemented yet include `IORING_REGISTER_FILES`, `IORING_REGISTER_BUFFERS`, and `IORING_SETUP_ATTACH_WQ`.

### Data Flow

1. **Create**: BOBS creates `<data_dir>/<key>/`, opens `<data_dir>/<key>/spool.dat`, and commits an initial `<data_dir>/<key>/meta.json` sidecar.
2. **Write**: Data arrives via POST. Accepted bytes are appended to `spool.dat` through the selected FileIO backend before `/write` returns. In-memory state tracks page assembly, but the data file is the source of truth for accepted bytes.
3. **Page visibility**: Once enough accepted bytes form a complete page, that page becomes reader-visible, is added to the global FIFO page cache, and any parked reader requests are notified.
4. **Read**: Reader requests a visible range -> check page cache -> if miss, read from `spool.dat` -> stream bytes to the HTTP response. Trailing partial-page bytes may already be present in `spool.dat`, but they are not reader-visible until they become a complete page or `/complete` finalizes the spool.
5. **Complete**: `/complete` publishes any trailing partial page, syncs `spool.dat` data, then commits final completed metadata to `meta.json` with the sidecar atomic commit protocol.
6. **Lifecycle**: Spool moves from `Creating` -> `Writing` (or `WriteLocked`) -> `Complete` -> `Deleting`.

### Persistence Contract

BOBS persists two things with different authority:

- `spool.dat` stores accepted bytes. For in-progress `Writing` and `WriteLocked` spools, it is authoritative after a BOBS restart.
- `meta.json` stores lifecycle metadata and completed-object byte metadata. It is committed atomically at lifecycle boundaries such as create, write-lock/readable transitions, complete, and delete.

Ordinary writes deliberately do not update a durable high-water mark, and completed pages do not commit per-page metadata. Adding a mandatory per-write or per-page checkpoint would put metadata commits back on the write hot path, which this design avoids.

While a spool is still `Writing` or `WriteLocked`, persisted byte-derived metadata such as `total_bytes_written`, `total_pages`, and `final_page_size` is advisory and may be stale. Recovery derives those values from `spool.dat`: the logical accepted length comes from the data file length, and page counts and partial-page state are reconstructed from that length.

The in-progress durability invariant is recovery from a BOBS restart, not survival of a node or storage crash before `/complete`. A successful `/write` therefore requires the bytes to have been accepted by the kernel/file handle before the handler returns, but it does not require `sync_data()`. `/complete` is the durability boundary for a finished object: BOBS syncs `spool.dat` data before committing final completed metadata.

Completed metadata is durable through the sidecar protocol: write `meta.json.tmp`, sync it, rename it to `meta.json`, and sync the parent directory. Recovery treats `meta.json` as all-or-nothing and ignores any leftover `meta.json.tmp` from an interrupted commit.

### Recovery and Shared Filesystems

Startup recovery scans `<data_dir>` for key directories with `meta.json`. Recovery admits at most `max_live_spools` entries, including completed spools. Candidates are ordered by most recent persisted activity (`last_read_at`, `readable_at`, `last_write_at`, or `created_at`), newest first, then lexically by key. This makes reduced-capacity restarts predictable.

Recovery first performs metadata-only validation in that order. Incomplete `Creating` and `Deleting` transactions and corrupt entries are cleaned without consuming admission; unsafe or ambiguous legacy metadata remains intact but unavailable. Recovery keeps examining candidates until it successfully admits `max_live_spools` valid spools or exhausts the ordered set.

Only a candidate with an available slot has `spool.dat` opened or its in-progress trailing partial page loaded. An open or tail-validation failure releases the slot immediately and recovery continues with the next candidate. Metadata-valid excess candidates are quarantined in place without opening or reading their data or rewriting their sidecars. A later restart with more capacity can recover them. Startup emits per-key warnings and a summary with configured, recovered, and quarantined counts.

The layout is friendly to shared filesystems and multi-BOBS deployments because each object has its own directory and sidecar, and each spool has a single writer. Independent keys can be created, completed, recovered, and deleted without a global metadata database or cross-key write serialization. Correct routing is still required: create, write, complete, and read traffic for a key must reach a BOBS instance that can see the same `<data_dir>/<key>` files.

Cleanup TTL behaviour is preserved with sidecar metadata. Writer inactivity, read-idle, and full-read-complete cleanup still remove both `spool.dat` and `meta.json` for admitted expired keys; TTL timestamps are committed at lifecycle boundaries and reconstructed conservatively on recovery. Quarantined excess entries are not present in the live manager and therefore are not cleanup candidates during that process lifetime.

### Legacy Metadata Migration

Older BOBS builds stored lifecycle metadata in a native redb database. Current BOBS uses sidecar `meta.json` files. Legacy native redb state is handled as an export/import migration path into sidecars rather than as the live metadata backend.
