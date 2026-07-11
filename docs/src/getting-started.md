<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Getting Started

## Build

Ensure you have the Rust toolchain installed. Build the release binary:

```bash
cargo build --release
```

The binary will be available at `./target/release/bobs`.

## Run

Create a minimal config with the required routing fields:

```yaml
host_prefix: bobs
domain: example.com
route_name: download
```

Start the service with the required environment variables:

```bash
export HOSTNAME=bobs-0
export BOBS_INTERNAL_BASE_URL_TEMPLATE=http://localhost:3000/api/v1
./target/release/bobs config.yaml
```

`HOSTNAME` must end with a numeric ordinal, and `BOBS_INTERNAL_BASE_URL_TEMPLATE` must be non-empty. The URL template is returned as `write_url` after replacing any `{ordinal}` placeholder. Other omitted fields use the Rust binary defaults; see [Configuration](configuration.md).

## Walkthrough

### 1. Create a Spool

Initialize a new spool. The response contains `key`, `read_url`, and `write_url`. A valid `X-Polytope-Job-Id` is a 26-character Crockford base32 value in either case; uppercase is accepted and normalized to the lowercase canonical `key`. Without one, `key` is a generated UUIDv4.

```bash
curl -X PUT http://localhost:3000/api/v1/create \
     -H "Content-Type: application/json" \
     -d '{"content_type": "application/octet-stream"}'
```

A successful response includes every URL needed for subsequent traffic:

```json
{
  "key": "550e8400-e29b-41d4-a716-446655440000",
  "read_url": "https://bobs.example.com/download-0/550e8400-e29b-41d4-a716-446655440000",
  "write_url": "http://localhost:3000/api/v1"
}
```

**Write-Locked Mode**: To prevent reads until `/api/v1/complete/{key}` succeeds, set `write_locked` to `true`.

```bash
curl -X PUT http://localhost:3000/api/v1/create \
     -d '{"content_type": "application/pdf", "write_locked": true}'
```

### 2. Write Data

Append data at a specific offset. BOBS enforces sequential writes; the offset must match the total bytes written so far.

```bash
curl -X POST http://localhost:3000/api/v1/write/YOUR_KEY/0 --data-binary @part1.dat
curl -X POST http://localhost:3000/api/v1/write/YOUR_KEY/1024 --data-binary @part2.dat
```

### 3. Complete the Spool

Signal that the upload is finished. You can optionally provide an `expected_size` for server-side verification. Completion is idempotent, but repeated requests still reject a mismatched `expected_size`.

```bash
curl -X POST http://localhost:3000/api/v1/complete/YOUR_KEY \
     -d '{"expected_size": 2048}'
```

### 4. Read Data

**Streaming (Follow Mode)**: If the writer is still active, you can follow the stream from the beginning.

```bash
curl -L http://localhost:3000/api/v1/read/YOUR_KEY
```

*Note: The `-L` flag is important because BOBS uses 307 redirects when a long-poll timeout occurs before the first page. A timeout after streaming has begun aborts the transfer with a response-body error.*

**Range Request**: Once complete, or for pages already visible while writing, request specific bytes with the standard `Range` header. Both `bytes=X-Y` and open-ended `bytes=X-` requests are bounded and do not wait for future writes.

```bash
curl http://localhost:3000/api/v1/read/YOUR_KEY -H "Range: bytes=0-1023"
```

### 5. Delete

Remove the spool manually.

```bash
curl -X DELETE http://localhost:3000/api/v1/delete/YOUR_KEY
```
