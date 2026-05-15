Concerns worth knowing about
Issue	Severity	Detail
No fsync on page writes	Medium	writer.rs:62 writes pages but only syncs on complete(). A BOBS process restart before `/complete` recovers from bytes present in `spool.dat`; a node/storage crash before `/complete` can lose acknowledged-but-unsynced bytes, may fail the job, and is outside the process-restart invariant. Do not assume the producer can rewind/retry.
Lifecycle metadata persist failures must surface	Medium	create/complete/delete/write-lock transitions rely on redb lifecycle metadata. If those commits fail, the operation should return an error rather than leave in-memory and on-disk lifecycle state divergent. Ordinary `/write` calls intentionally do not persist redb high-water marks; accepted bytes are recovered from `spool.dat`.
Seek-based I/O with global file mutex	Medium	tokio_fs.rs:14 — Arc<Mutex<File>> means readers and writers serialize on seek+read/write. Multiple concurrent readers on the same spool will contend. pread/pwrite would fix this.
No global memory cap	Low-Med	Each spool has its own page_cache bounded by max_cache_bytes. But with N spools, total memory is N × max_cache_bytes. No global limit.
No disk space checking	Low	Writes fail with IO error when disk is full, but no proactive check or backpressure.
No request body size limit	Low	write_spool accepts unbounded body. Fine internally, problematic if exposed.
No metrics	Low	Logs only. No Prometheus counters, histograms. You'll want this for production ops.
Recovery is O(total data)	Low	manager.rs:325-332 recomputes CRC32C from spool data during startup recovery. Could make restarts slow with large data volumes. If needed, consider optional coarse checkpoints written periodically or at lifecycle transitions only; they must not be required for correctness and must not add redb writes to each `/write` or completed page.
No auth	Info	Relies entirely on K8s network policy. Fine for internal, just be aware.
What I'd add before relying on it heavily
1. Prometheus metrics — spool count, write/read throughput, active readers, cache hit rate. Cheapest win for operability.
2. pread/pwrite — replace the seek+mutex pattern to unlock read parallelism. Not urgent but will matter at scale.
3. Disk space monitoring — even a simple "reject creates when <5% free" check.