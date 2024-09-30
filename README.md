# pest
Persistent Streaming

# TODO
## python server
- fix the non trivial benchmarks
- locks on db for multi-read and compat with read (but forbid multi-write)
- dockerfile
- page writes as a background task
- eviction algorithm improvements
- better tests for eviction
- consider either uring, or custom thread pool over the `async_files` -- we dont want a thread (and thus coroutine yield) per simple operations like close on readers

## rust server
- everything
