# Key Behaviours

Understanding these internal behaviours is crucial for effectively using BOBS.

### 1. Page-based Streaming
Data is organized into fixed-size pages (configured via `page_size`). A page is only visible to readers once it is completely full and flushed to disk. The final partial page is flushed only when the writer calls `/complete`. This ensures readers always receive consistent, non-torn data.

### 2. Long-poll with Timeout
When a reader requests data that hasn't been written yet, BOBS parks the request using a notification system. To prevent idle timeouts from network infrastructure (like Kubernetes Ingress or Load Balancers), BOBS returns a `307 Temporary Redirect` if no data arrives within `long_poll_timeout_ms`. Clients like `curl -L` will automatically follow the redirect and resume the poll.

### 3. Follow Mode
A read request without a `Range` header enters follow mode from byte `0` (or from byte `X` with `Range: bytes=X-`). The server keeps the stream open and pushes new pages as they become available until the spool is finalized.

### 4. Write-locked Mode
A spool can be created with `write_locked: true`. In this state, any attempt to read results in a `423 Locked` error until the writer calls `/complete`. This is useful for preventing consumers from seeing any data until the entire payload is successfully buffered.

### 5. Parallel Reads
BOBS supports a single consumer opening multiple concurrent connections for the same spool. Each connection can request independent byte ranges. Read activity is tracked when bytes are actually served, so slow clients keep their spool alive while making progress but stalled connections do not protect a spool forever.

### 6. Sequential Writes
Writes must be strictly sequential. The `offset` provided in the `/write` request must exactly match the total number of bytes currently stored in the spool. Random-access writes or overwrites are not supported.

### 7. Automatic Cleanup
A background task periodically sweeps the spool manager and deletes spools based on three triggers:
- **Writer Inactivity**: The producer stopped writing without completing the spool.
- **Read Idle TTL**: The spool is readable but has not served bytes for `read_idle_ttl_secs`. For never-read spools, this timer starts when the spool becomes readable.
- **Full Read TTL**: BOBS has served every byte of the object at least once, possibly across multiple range requests, and `full_read_complete_ttl_secs` has elapsed since the latest read activity.

Coverage tracking uses missing byte ranges rather than per-byte state, so large objects do not require large memory allocations. If range access is extremely fragmented, BOBS falls back to the longer idle TTL rather than risking early deletion.

### 8. HTTP/2 Support
The server supports both HTTP/1.1 and HTTP/2 (h2c cleartext). Using HTTP/2 is recommended for high-concurrency streaming to benefit from request multiplexing.
