<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# HTTP API Reference

BOBS exposes its spool API under `/api/v1`; every spool route below shows the full path.

## Endpoints

| Method | Route | Description |
| -------- | ------- | ------------- |
| GET | `/api/v1/health` | Health check returning status and hostname. |
| GET/HEAD | `/api/v1/status` | Status check; GET returns status and hostname. |
| PUT | `/api/v1/create` | Create a new spool. |
| POST | `/api/v1/write/{key}/{offset}` | Append data to a spool. |
| POST | `/api/v1/complete/{key}` | Finalize a spool. |
| GET | `/api/v1/read/{key}` | Read or stream data. |
| DELETE | `/api/v1/delete/{key}` | Delete a spool. |
| GET | `/debug/pprof/profile` | CPU profile, only when `enable_pprof: true`; this route is outside `/api/v1`. |

---

### GET /api/v1/status

Returns the health status and hostname of the instance.

**Response (200 OK)**:

```json
{
  "status": "ok",
  "hostname": "pod-a-123"
}
```

---

### PUT /api/v1/create

Creates a new spool and returns its key. A valid `X-Polytope-Job-Id` is a 26-character Crockford base32 request ID in either case; uppercase input is normalized to the lowercase canonical key. Otherwise BOBS generates a UUIDv4 key. Create is not idempotent: if that key already exists or is being created, BOBS returns `409 Conflict` without truncating the existing bytes or changing admission accounting. Exactly one concurrent create can succeed, and a successfully deleted key may be created again.

**Request Body**:

- `content_type` (string, optional): Reader content type; defaults to `application/octet-stream` when served.
- `content_encoding` (string, optional): Optional encoding header for readers.
- `write_locked` (boolean, optional): Default `false`. If `true`, reads return `423 Locked` until the spool is completed.
- `labels` (object of string values, optional): Caller labels filtered and propagated to metrics.

BOBS rejects unknown JSON fields and `content_type` or `content_encoding` values that cannot safely be represented as HTTP headers with `400 Bad Request`. Repeating or concurrently issuing create with the same valid `X-Polytope-Job-Id` returns `409 Conflict`.

**Example**:

```bash
curl -X PUT http://localhost:3000/api/v1/create -d '{"content_type": "text/plain", "write_locked": false}'
```

**Response (201 Created)**:

```json
{
  "key": "550e8400-e29b-41d4-a716-446655440000",
  "read_url": "https://bobs.example.com/download-0/550e8400-e29b-41d4-a716-446655440000",
  "write_url": "http://localhost:3000/api/v1"
}
```

BOBS returns `201 Created` only after `spool.dat`, the key-directory link, and live `Writing` or `WriteLocked` metadata cross their fsync boundaries. The intermediate `Creating` sidecar is internal to crash recovery and is never visible as an active spool. An interrupted create is removed safely during startup recovery.

---

### POST /api/v1/write/{key}/{offset}

Appends binary data to the spool.

- `key`: The spool key.
- `offset`: The current byte offset. This must match the total bytes already written.

**Response (200 OK)**: Empty body on success.

Writes that would take a spool beyond `max_spool_bytes` return `413 Payload Too Large`. Before deleting an oversized spool, BOBS atomically verifies under the lifecycle/write gate that the request offset is still the current write head and the spool remains writable. An offset mismatch therefore returns `400`, a completed spool returns `409`, and a deleting or deleted spool returns `404`, without the oversize path deleting or otherwise changing that object.

For a valid write head, both known-length rejection and a chunked upload crossing the limit durably delete the partial spool and release its admission slot before returning `413`; a cleanup failure returns a server error instead. Known-length oversize requests are rejected without polling the body.

**Error Cases**:

- `400 Bad Request`: Offset mismatch (for example, writing at 100 when only 50 bytes exist).
- `404 Not Found`: Spool does not exist.
- `409 Conflict`: Spool is already complete.
- `413 Payload Too Large`: The write would exceed `max_spool_bytes`; the spool is no longer available.

---

### POST /api/v1/complete/{key}

Idempotently finalizes the spool. After this, no more writes are allowed. Repeated completion requests succeed, but a supplied `expected_size` is always checked against the actual completed size.

**Request Body (Optional)**:

- `expected_size` (integer): Verification that the total written bytes match this value.

**Example**:

```bash
curl -X POST http://localhost:3000/api/v1/complete/YOUR_KEY -d '{"expected_size": 5000}'
```

**Response (200 OK)**: Empty body on success.

**Error Cases**:

- `400 Bad Request`: `expected_size` differs from the actual bytes written, including on an otherwise idempotent repeated request.

---

### GET /api/v1/read/{key}

Reads a bounded range or follows a live stream using the standard HTTP `Range` header.

Range behavior:

- No `Range` header: follow mode from byte `0` (`200 OK`, streaming).
- `Range: bytes=X-Y`: bounded read of bytes `[X, Y]` inclusive (`206 Partial Content`).
- `Range: bytes=X-`: bounded read from `X` through the bytes currently servable when the request is resolved (`206 Partial Content`); it does not wait for future writes.
- `Range: bytes=-N`: suffix read of the final `N` bytes; requires a completed spool.

In follow mode, BOBS streams pages as they become visible. If no first page arrives within `long_poll_timeout_ms`, BOBS may issue a `307 Temporary Redirect` to the same `/api/v1/read/{key}` URL for long-poll refresh. If the timeout occurs after bytes have started streaming, BOBS aborts the transfer with a response-body error rather than redirecting or returning a clean end of stream.

**Response Headers**:

- `Accept-Ranges: bytes`: Advertises byte-range support.
- `Content-Range: bytes X-Y/*` for bounded reads (or `bytes X-Y/TOTAL` once complete).
- `Content-Type`: Validated value supplied during creation, otherwise `application/octet-stream`.
- `Content-Encoding`: Validated value supplied during creation, if any.
- `Content-Disposition: attachment`: Forces download rather than inline rendering of producer-controlled content.
- `X-Content-Type-Options: nosniff`: Prevents content sniffing.
- `Content-Security-Policy: sandbox`: Added for active document content types as defence in depth.
- `X-Accel-Buffering: no`: Disables proxy buffering.

Examples:

```bash
# Bounded range
curl http://localhost:3000/api/v1/read/YOUR_KEY -H "Range: bytes=0-1023"

# Follow from start
curl -L http://localhost:3000/api/v1/read/YOUR_KEY

# Open-ended bounded range
curl http://localhost:3000/api/v1/read/YOUR_KEY -H "Range: bytes=1048576-"
```

**Error Cases**:

- `400 Bad Request`: Invalid or unsupported range syntax.
- `416 Range Not Satisfiable`: Range cannot be served; `Content-Range` reports the known total or `*`.
- `423 Locked`: Spool was created with `write_locked: true` and is not yet complete.

---

### DELETE /api/v1/delete/{key}

Manually deletes the spool and its associated data files. Success is acknowledged only after sidecar removal, key-directory removal, and an fsync of the parent `data_dir`. If that durability boundary fails, BOBS returns a server error and retains tracked `Deleting` state, cache entries, and admission accounting so the operation can be retried.

**Response (200 OK)**: Success.
**Error Cases**:

- `404 Not Found`: Spool does not exist.
