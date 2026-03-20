Concerns worth knowing about
Issue	Severity	Detail
No fsync on page writes	Medium	writer.rs:62 writes pages but only syncs on complete(). Crash mid-write → recovery corrects metadata to match disk, but you lose uncommitted pages. Acceptable since the writer can retry.
Metadata persist failure is warn-only	Medium	writer.rs:88-90, lifecycle.rs:90-92 — if redb write fails, in-memory and on-disk state diverge. On restart, metadata won't reflect last writes.
Seek-based I/O with global file mutex	Medium	tokio_fs.rs:14 — Arc<Mutex<File>> means readers and writers serialize on seek+read/write. Multiple concurrent readers on the same spool will contend. pread/pwrite would fix this.
No global memory cap	Low-Med	Each spool has its own page_cache bounded by max_cache_bytes. But with N spools, total memory is N × max_cache_bytes. No global limit.
No disk space checking	Low	Writes fail with IO error when disk is full, but no proactive check or backpressure.
No request body size limit	Low	write_spool accepts unbounded body. Fine internally, problematic if exposed.
No metrics	Low	Logs only. No Prometheus counters, histograms. You'll want this for production ops.
Recovery is O(total data)	Low	manager.rs:282-323 reads every page of every spool to recompute CRC32C on startup. Could make restarts slow with large data volumes.
No auth	Info	Relies entirely on K8s network policy. Fine for internal, just be aware.
What I'd add before relying on it heavily
1. Prometheus metrics — spool count, write/read throughput, active readers, cache hit rate. Cheapest win for operability.
2. pread/pwrite — replace the seek+mutex pattern to unlock read parallelism. Not urgent but will matter at scale.
3. Disk space monitoring — even a simple "reject creates when <5% free" check.