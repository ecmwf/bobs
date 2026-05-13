# HTTP API Reference

BOBS exposes a RESTful API for managing spools.

## Endpoints

| Method | Route | Description |
|--------|-------|-------------|
| GET | `/status` | Health check and instance ID. |
| PUT | `/create` | Create a new spool. |
| POST | `/write/{key}/{offset}` | Append data to a spool. |
| POST | `/complete/{key}` | Finalize a spool. |
| GET | `/read/{key}` | Read or stream data. |
| DELETE | `/delete/{key}` | Delete a spool. |

---

### GET /status

Returns the health status and hostname of the instance.

**Response (200 OK)**:
```json
{
  "status": "ok",
  "hostname": "pod-a-123"
}
```

---

### PUT /create

Creates a new spool and returns a unique key.

**Request Body**:
- `content_type` (string, optional): Default `application/octet-stream`.
- `content_encoding` (string, optional): Optional encoding header for readers.
- `write_locked` (boolean, optional): Default `false`. If `true`, reads return `423 Locked` until the spool is completed.

**Example**:
```bash
curl -X PUT http://localhost:3000/create -d '{"content_type": "text/plain", "write_locked": false}'
```

**Response (201 Created)**:
```json
{
  "key": "spool-xyz-789"
}
```

---

### POST /write/{key}/{offset}

Appends binary data to the spool.

- `key`: The spool key.
- `offset`: The current byte offset. This must match the total bytes already written.

**Response (200 OK)**: Empty body on success.

**Error Cases**:
- `404 Not Found`: Spool does not exist.
- `400 Bad Request`: Offset mismatch (e.g., trying to write at 100 when only 50 bytes were written).
- `409 Conflict`: Spool is already marked as complete.

---

### POST /complete/{key}

Finalizes the spool. After this, no more writes are allowed.

**Request Body (Optional)**:
- `expected_size` (integer): Verification that the total written bytes match this value.

**Example**:
```bash
curl -X POST http://localhost:3000/complete/YOUR_KEY -d '{"expected_size": 5000}'
```

**Response (200 OK)**: Empty body on success.

**Error Cases**:
- `400 Bad Request`: Size mismatch between `expected_size` and actual bytes written.

---

### GET /read/{key}

Reads a bounded range or follows a live stream using the standard HTTP `Range` header.

Range behavior:
- No `Range` header: follow mode from byte `0` (`200 OK`, streaming).
- `Range: bytes=X-Y`: bounded read of bytes `[X, Y]` inclusive (`206 Partial Content`).
- `Range: bytes=X-`: follow mode from byte `X` (`200 OK`, streaming).

For follow mode, the connection remains open and BOBS streams pages as they are flushed by the writer. If the writer is idle, BOBS may issue a `307 Temporary Redirect` to the same `/read/{key}` URL for long-poll refresh.

**Response Headers**:
- `Accept-Ranges: bytes`: Advertises byte-range support.
- `Content-Range: bytes X-Y/*` for bounded reads (or `bytes X-Y/TOTAL` once complete).
- `Content-Type`: As defined during creation.
- `Content-Encoding`: As defined during creation (if provided).
- `X-Checksum-CRC32C`: Base64-encoded CRC-32C of the **full object**. Only available after completion.
- `X-Accel-Buffering`: `no` (disables proxy buffering).

Examples:

```bash
# Bounded range
curl http://localhost:3000/read/YOUR_KEY -H "Range: bytes=0-1023"

# Follow from start
curl -L http://localhost:3000/read/YOUR_KEY

# Follow from offset
curl -L http://localhost:3000/read/YOUR_KEY -H "Range: bytes=1048576-"
```

**Error Cases**:
- `423 Locked`: Spool was created with `write_locked: true` and is not yet complete.

---

### DELETE /delete/{key}

Manually deletes the spool and its associated data files.

**Response (200 OK)**: Success.
**Error Cases**:
- `404 Not Found`: Spool does not exist.
