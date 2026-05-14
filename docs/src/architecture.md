# Architecture

BOBS is built on a high-performance, asynchronous foundation.

### Internal Components

- **Spool**: The core entity representing a data stream. It contains metadata, a write buffer, and handles synchronization between the writer and readers.
- **FileIO**: An abstraction for asynchronous disk I/O. The default high-performance implementation, `TokioFileIO`, is currently Unix-only: it stores an `Arc<std::fs::File>`, opens files through `std::fs::OpenOptions` on Tokio blocking tasks, and performs reads and writes with Unix positional file APIs (`FileExt::read_at` / `write_at`, `pread`/`pwrite` style) so concurrent operations do not share a file cursor. Writes loop until the owned `Bytes` buffer has been fully accepted, and `/complete` durability is preserved with `File::sync_data` on a blocking task. Non-Unix builds are compile-gated until an explicit positional backend is added for those platforms.
- **SpoolManager**: A central registry (using `DashMap`) that tracks all active spools. It uses `redb` for lightweight persistence of lifecycle metadata, allowing the service to recover state after a BOBS process restart.
- **Page Cache**: A global byte-capped FIFO cache that minimizes disk reads for hot data being consumed immediately after it's written. Entries are keyed by `(spool_key, page_idx)`, and `max_cache_bytes` is the total cache budget across all spools. Setting `max_cache_bytes` to `0` disables caching; pages larger than the byte cap are valid but bypass the cache.

### Data Flow

1. **Write**: Data arrives via POST -> accepted bytes are appended to `spool.dat` through the kernel/file handle before `/write` returns. In-memory state tracks page assembly, but it is not the source of truth for accepted bytes.
2. **Page visibility**: Once enough accepted bytes form a complete page, that page becomes reader-visible, is added to the page cache, and any parked reader requests are notified.
3. **Read**: Reader requests a visible range -> check page cache -> if miss, read from `spool.dat` -> stream bytes to the HTTP response. Trailing partial-page bytes may already be present in `spool.dat`, but they are not reader-visible until they become a complete page or `/complete` finalizes the spool.
4. **Lifecycle**: Spool moves from `Creating` -> `Writing` (or `WriteLocked`) -> `Complete` -> `Deleting`.

### Persistence Contract

BOBS persists two things with different authority:

- `redb` stores lifecycle metadata. Metadata is committed when a spool is created, completed, deleted, and when it transitions between write-locked and readable states.
- `spool.dat` stores the bytes. For in-progress `Writing` and `WriteLocked` spools, `spool.dat` is the source of truth after a BOBS process restart.

Ordinary writes deliberately do not update a `redb` high-water mark, and completed pages do not commit per-page metadata. Adding a mandatory per-write or per-page checkpoint would put `redb` back on the write hot path, which this design avoids.

While a spool is still `Writing` or `WriteLocked`, persisted byte-derived metadata such as `total_bytes_written`, `total_pages`, `final_page_size`, and `checksum_crc32c` is advisory and may be stale. Recovery must derive those values from `spool.dat` rather than trusting `redb`: the logical accepted length comes from the data file length, page counts and partial-page state are reconstructed from that length, and the running CRC is recomputed by scanning the file bytes.

The durability invariant is recovery from a BOBS process restart, not survival of a node or storage crash before `/complete`. A successful `/write` therefore requires the bytes to have been accepted by the kernel/file handle before the handler returns, but it does not require `sync_data()`. `/complete` is the durability boundary for a finished object and must keep `sync_data()` before committing the completed lifecycle metadata.

If startup recovery time later becomes an operational concern, BOBS may add optional coarse-grained checkpoints. Such checkpoints must be an optimization only: they may be written periodically or at lifecycle transitions, but never per write or per completed page, and recovery correctness must not depend on them being present or current.
