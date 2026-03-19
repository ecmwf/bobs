# Architecture

BOBS is built on a high-performance, asynchronous foundation.

### Internal Components

- **Spool**: The core entity representing a data stream. It contains metadata, a page cache, a write buffer, and handles synchronization between the writer and readers.
- **FileIO**: An abstraction for asynchronous disk I/O. The default implementation uses Tokio's file system tasks, but it's designed to support future high-performance backends like `io_uring`.
- **SpoolManager**: A central registry (using `DashMap`) that tracks all active spools. It uses `redb` for lightweight persistence of metadata, allowing the service to recover state after a restart.
- **Page Cache**: A per-spool LRU-like cache that minimizes disk reads for hot data being consumed immediately after it's written. Capacity is derived from `max_cache_bytes / page_size`.

### Data Flow

1. **Write**: Data arrives via POST -> written to a memory buffer -> once a page is full, it's pushed to the page cache and queued for disk write.
2. **Notification**: Once a page is flushed, any parked reader requests are notified.
3. **Read**: Reader requests a range -> check page cache -> if miss, read from disk -> stream bytes to the HTTP response.
4. **Lifecycle**: Spool moves from `Creating` -> `Writing` (or `WriteLocked`) -> `Complete` -> `Deleting`.
