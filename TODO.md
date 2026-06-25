Concerns worth knowing about

Issue	Severity	Detail
No fsync on page writes	Medium	A successful `/write` accepts bytes into `spool.dat`, but ordinary writes do not call `sync_data()` and do not commit per-page metadata. A BOBS process restart recovers in-progress bytes from `spool.dat`; a node/storage crash before `/complete` can still lose acknowledged-but-unsynced bytes and fail the job. `/complete` is the durability boundary for finished objects.
No disk space checking	Low	Writes fail with an I/O error when disk is full, but there is no proactive free-space check, create rejection, or backpressure based on available capacity.
No request body size limit	Low	`/write` streams an unbounded request body. This is acceptable for trusted internal callers, but risky if the write API is exposed outside that boundary.
No metrics	Low	Operational visibility is log-based. There are no Prometheus counters, gauges, or histograms for spool counts, throughput, active readers, cache hit rate, cleanup, or I/O latency.
Recovery is O(total data)	Low	Startup recovery recomputes byte-derived state and CRC32C for in-progress spools from `spool.dat`. Restarts with large in-progress data volumes can be slow.
No auth	Info	BOBS relies on Kubernetes/network isolation and trusted in-cluster callers. There is no application-level authentication or authorization.
