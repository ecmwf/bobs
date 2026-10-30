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
| GET/HEAD | `/api/v1/status` | Status check; GET returns the same status and hostname body as `/health`, HEAD returns an empty `200`. |
| PUT | `/api/v1/create` | Create a new spool. |
| POST | `/api/v1/write/{key}/{offset}` | Append data to a spool. |
| POST | `/api/v1/complete/{key}` | Finalize a spool. |
| GET | `/api/v1/read/{key}` | Read or stream data. |
| DELETE | `/api/v1/delete/{key}` | Delete a spool. |
| GET | `/debug/pprof/profile` | CPU flamegraph (SVG; `?seconds=N`, clamped to 1-120, default 30). The route is always registered but returns a bare `404` unless `enable_pprof: true`; it is outside `/api/v1`. |

Every error response from an API handler carries a JSON body of the form `{"error": "<message>"}`. Statuses map as follows: `400` invalid request, offset or size mismatch, or invalid range; `404` unknown or deleting spool; `409` duplicate create, or a write against a completed/completing spool; `413` spool too large; `416` range not satisfiable; `423` spool not readable yet; `503` create admission timeout; anything else is `500`. Request bodies for `create` and `complete` use axum's default 2 MiB limit; framework-level rejections (for example a non-numeric `{offset}`) return plain-text `400`/`413` rather than the JSON error body. `HEAD` is also answered by the other `GET` routes through axum's default behaviour (the handler runs and the body is dropped), so `HEAD /api/v1/read/{key}` takes a full read lease and is not a cheap existence check.

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

Creates a new spool and returns its key. A valid `X-Polytope-Job-Id` is a 26-character request ID over the lowercase Crockford alphabet (`0-9 a-h j k m n p-t v-z`; `i`, `l`, `o` and `u` are not accepted), in either case; uppercase input is normalized to the lowercase canonical key. An invalid header is silently ignored and BOBS generates a UUIDv4 key. Only `create` reads the header: every later call must use the lowercase `key` returned by create, because key lookup is exact and case-sensitive. Create is not idempotent: if the key is tracked in any lifecycle state, including retryable `Deleting`, BOBS returns `409 Conflict` before waiting for admission or the per-key gate. An existing reserved key directory also conflicts. Duplicate create never truncates existing bytes or changes admission accounting; exactly one concurrent create can succeed, and a durably deleted key may be created again.

**Request Body**:

- `content_type` (string, optional): Reader content type; defaults to `application/octet-stream` when served.
- `content_encoding` (string, optional): Optional encoding header for readers.
- `write_locked` (boolean, optional): Default `false`. If `true`, reads return `423 Locked` until the spool is completed.
- `labels` (object of string values, optional): Caller labels filtered and propagated to metrics.

BOBS rejects unknown JSON fields, and `content_type` or `content_encoding` values that are empty, whitespace-only or cannot be represented as HTTP header values, with `400 Bad Request`. No MIME syntax check is applied beyond that. If no slot becomes free within `create_admission_timeout_ms`, create returns `503 Service Unavailable`. Repeating or concurrently issuing create with the same valid `X-Polytope-Job-Id` returns `409 Conflict`; this immediate response applies to every tracked state, not only writable or complete spools.

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

BOBS returns `201 Created` only after `spool.dat`, the key-directory link, and live `Writing` or `WriteLocked` metadata cross their fsync boundaries. The intermediate `Creating` sidecar is internal to crash recovery and is never visible as an active spool. Startup recovery removes a recognised pre-marker key directory only when it is truly empty and then fsyncs `data_dir`; non-empty markerless directories are retained unchanged.

If the client disconnects while waiting for admission, no key is reserved. After reservation starts, BOBS completes the create transaction or rolls it back despite client cancellation. A missing response is therefore an unknown outcome: retry with the same `X-Polytope-Job-Id`; `409 Conflict` means the spool now exists.

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

Idempotently finalizes the spool. A supplied `expected_size` is checked on initial and repeated requests. Completion runs as an owned transaction: if the client disconnects after finalization starts, BOBS continues syncing `spool.dat`, committing the durable `Completing` recovery marker, and committing `Complete`. A missing response is therefore an unknown outcome; retry completion with the same `expected_size`.

Once completion starts, BOBS rejects further writes. Before the marker is durable, an internal failure leaves completion retryable. A valid marker found during startup is finalized to `Complete`; marker/data disagreement quarantines the key unchanged and never reopens it for writing. The internal `Completing` state is not an API choice.

**Request Body (Optional)**:

- `expected_size` (integer): Verification that the total written bytes match this value.

**Example**:

```bash
curl -X POST http://localhost:3000/api/v1/complete/YOUR_KEY -d '{"expected_size": 5000}'
```

**Response (200 OK)**: Empty body on success.

**Error Cases**:

- `400 Bad Request`: `expected_size` differs from the actual bytes written, including on an otherwise idempotent repeated request; also malformed JSON or unknown fields in the body.
- `404 Not Found`: Spool does not exist.
- `409 Conflict`: Spool is not in a state that can be completed.

---

### GET /api/v1/read/{key}

Reads a bounded range or follows a live stream using the standard HTTP `Range` header.

Range behavior:

- No `Range` header: follow mode from byte `0` (`200 OK`). While the spool is in progress the response is chunked; once the spool is `Complete` it carries a `Content-Length` and no `Content-Range`.
- `Range: bytes=X-Y`: bounded read of bytes `[X, Y]` inclusive (`206 Partial Content`). `Y` is clipped to the bytes currently servable (full pages while in progress), so the returned `Content-Range` may be shorter than requested; `X > Y` is `400`, and `X` at or beyond the servable bytes is `416`.
- `Range: bytes=X-`: bounded read from `X` through the bytes currently servable when the request is resolved (`206 Partial Content`); it does not wait for future writes.
- `Range: bytes=-N`: suffix read of the final `N` bytes; requires a completed spool.

In follow mode, BOBS streams pages as they become visible. If no first page arrives within `long_poll_timeout_ms` (a window that also includes time spent waiting for read-response admission, and therefore applies to ranged reads too), BOBS issues a `307 Temporary Redirect` to the same `/api/v1/read/{key}` URL (prefixed with a validated `X-Forwarded-Prefix` when one is supplied) with `Cache-Control: no-store`. If the timeout occurs after bytes have started streaming, BOBS aborts the transfer with a response-body error rather than redirecting or returning a clean end of stream.

**Response Headers**:

- `Accept-Ranges: bytes`: Advertises byte-range support.
- `Content-Range: bytes X-Y/*` for bounded reads (or `bytes X-Y/TOTAL` once complete).
- `Content-Type`: Validated value supplied during creation, otherwise `application/octet-stream`.
- `Content-Encoding`: Validated value supplied during creation, if any.
- `Content-Disposition: attachment; filename="<key>.<ext>"`: Forces download rather than inline rendering of producer-controlled content. The stem is the spool key reduced to ASCII letters, digits, `-` and `_` (fallback `data`); the extension is `grib` for `application/x-grib`, `covjson` for `application/prs.coverage+json`, and `bin` otherwise.
- `X-Content-Type-Options: nosniff`: Prevents content sniffing.
- `Content-Security-Policy: sandbox; default-src 'none'; frame-ancestors 'none'`: Added defence in depth when the base content type (parameters stripped, case-insensitive) is exactly `text/html`, `application/xhtml+xml` or `image/svg+xml`. Other types, including `text/xml`, do not receive it.
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
- `404 Not Found`: Spool does not exist or is being deleted.
- `416 Range Not Satisfiable`: Range cannot be served; `Content-Range` is `bytes */TOTAL` when the total is known and the literal `bytes */*` otherwise (suffix ranges on an in-progress spool, or a start beyond the servable bytes; the `*/*` form is not valid per RFC 9110).
- `423 Locked`: The spool is not readable: it was created with `write_locked: true` and is not yet complete, or (transiently) it is in the internal `Completing` state.

---

### DELETE /api/v1/delete/{key}

Manually deletes the spool and its associated data files. Success is acknowledged only after sidecar removal, key-directory removal, and an fsync of the parent `data_dir`. If that durability boundary fails, BOBS returns a server error and retains tracked `Deleting` state, cache entries, and admission accounting so the operation can be retried.

**Response (200 OK)**: Empty body on success.

**Error Cases**:

- `404 Not Found`: Spool does not exist (a second delete of the same key therefore returns `404`).
