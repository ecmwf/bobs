# pest
Persistent Streaming

# todo list python
- locks on db for multi-read and compat with read (but forbid multi-write)
- standalone client
- dockerfile
- benchmarks / load tests
  - suite
  - one big file ingest
  - one big file ingest then read
  - many small files writes and reads
- page writes as a background task
- eviction algorithm improvements
- better tests for eviction
- consider either uring, or custom thread pool over the `async_files` -- we dont want a thread (and thus coroutine yield) per simple operations like close on readers

# todo list rust
- everything
