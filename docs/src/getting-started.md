# Getting Started

## Build

Ensure you have the Rust toolchain installed. Build the release binary:

```bash
cargo build --release
```

The binary will be available at `./target/release/bobs`.

## Run

Start the service with default settings (port 3000, `./data` for storage):

```bash
./target/release/bobs
```

Or provide a YAML config file:

```bash
./target/release/bobs config.yaml
```

## Walkthrough

### 1. Create a Spool

Initialize a new spool. This returns a unique `key`.

```bash
curl -X PUT http://localhost:3000/create \
     -H "Content-Type: application/json" \
     -d '{"content_type": "application/octet-stream"}'
```

**Write-Locked Mode**: If you want to prevent anyone from reading the spool until it is complete, set `write_locked` to `true`.

```bash
curl -X PUT http://localhost:3000/create \
     -d '{"content_type": "application/pdf", "write_locked": true}'
```

### 2. Write Data

Append data at a specific offset. BOBS enforces sequential writes; the offset must match the total bytes written so far.

```bash
curl -X POST http://localhost:3000/write/YOUR_KEY/0 --data-binary @part1.dat
curl -X POST http://localhost:3000/write/YOUR_KEY/1024 --data-binary @part2.dat
```

### 3. Complete the Spool

Signal that the upload is finished. You can optionally provide an `expected_size` for server-side verification.

```bash
curl -X POST http://localhost:3000/complete/YOUR_KEY \
     -d '{"expected_size": 2048}'
```

### 4. Read Data

**Streaming (Follow Mode)**: If the writer is still active, you can follow the stream from the beginning.

```bash
curl -L http://localhost:3000/read/YOUR_KEY
```
*Note: The `-L` flag is important as BOBS uses 307 redirects for long-poll timeouts.*

**Range Request**: Once complete (or for available pages), request specific bytes with the standard `Range` header.

```bash
curl http://localhost:3000/read/YOUR_KEY -H "Range: bytes=0-1023"
```

### 5. Delete

Remove the spool manually.

```bash
curl -X DELETE http://localhost:3000/delete/YOUR_KEY
```
