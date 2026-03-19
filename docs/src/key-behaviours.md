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
BOBS supports a single consumer opening multiple concurrent connections for the same spool. Each connection can request independent byte ranges. The service tracks active readers to protect the spool from cleanup while data is still being transmitted.

### 6. CRC-32C Integrity
BOBS calculates a running CRC-32C checksum using hardware-accelerated instructions (where available) as data is written. Once a spool is complete, every read response includes the `X-Checksum-CRC32C` header containing the Base64-encoded checksum of the entire object. This allows clients to verify data integrity, which is especially important when reassembling parallel range reads.

### 7. Sequential Writes
Writes must be strictly sequential. The `offset` provided in the `/write` request must exactly match the total number of bytes currently stored in the spool. Random-access writes or overwrites are not supported.

### 8. Automatic Cleanup
A background task periodically sweeps the spool manager and deletes spools based on four triggers:
- **Writer Inactivity**: The producer stopped writing without completing the spool.
- **Reader Done TTL**: The spool is complete, and a configured time has passed since the last reader finished.
- **Unread TTL**: The spool was completed but never accessed by a reader.
- **Active Protection**: Spools with active readers are never cleaned up.

### 9. HTTP/2 Support
The server supports both HTTP/1.1 and HTTP/2 (h2c cleartext). Using HTTP/2 is recommended for high-concurrency streaming to benefit from request multiplexing.
