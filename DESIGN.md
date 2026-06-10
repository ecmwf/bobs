# BOBS Design

BOBS (Big-Object Buffered Storage) is a temporary HTTP spool service for large streaming responses. A producer writes one contiguous byte stream, and one or more reader connections consume it while it is still being produced or after it is complete.

BOBS is not long-term object storage. It has no replication layer, no authentication layer, and no guarantee that an in-progress object survives a node or storage crash before completion. It is designed for short-lived, single-writer streams in a trusted Kubernetes/internal network.

## Deployment model

BOBS runs as a set of pods. Producers create and write through the internal API. Reader URLs can route through ingress to the pod/route that owns or can see the key.

A create request allocates a UUIDv4 key. Keys do not include a host prefix. Routing information is carried in the returned URLs, not embedded in the key.

Correct routing remains important: create, write, complete, delete, and read traffic for a key must reach a BOBS instance that can access the key's directory under `data_dir`.

## HTTP API

The API is served under `/api/v1`.

| Method | Route | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Health check. |
| `GET`/`HEAD` | `/status` | Status check. |
| `PUT` | `/create` | Create a spool and return its UUIDv4 key plus read/write URLs. Optional JSON fields include `content_type`, `content_encoding`, and `write_locked`. |
| `POST` | `/write/{key}/{offset}` | Append request-body bytes. `offset` must equal the current write head; gaps and overwrites are rejected. |
| `POST` | `/complete/{key}` | Finalize the spool. Optional `expected_size` rejects completion if the written length differs. |
| `GET` | `/read/{key}` | Stream bytes. Supports HTTP `Range`; no range or `bytes=X-` follows the stream. |
| `DELETE` | `/delete/{key}` | Delete a spool early. |

`/complete` is the writer finalization endpoint.

## On-disk layout and metadata

Each spool is stored in its own directory:

```text
<data_dir>/<key>/
  spool.dat
  meta.json
```

`spool.dat` contains accepted payload bytes. `meta.json` is a sidecar metadata file containing lifecycle state, content metadata, timestamps, byte counts, page counts, final partial-page size, checksum, and the data path.

Sidecar metadata commits are atomic at the file level: BOBS writes `meta.json.tmp`, syncs that file, renames it over `meta.json`, and syncs the spool directory. Recovery ignores leftover temporary metadata files.

Ordinary `/write` calls do not persist a metadata high-water mark. For in-progress spools, `spool.dat` is authoritative after a BOBS process restart; recovery recomputes length, page state, and CRC from the data file.

## File I/O

BOBS uses positional file I/O through a `FileIO` abstraction.

On Linux, the default backend is a sharded `io_uring` pool. Non-Linux builds, and builds with the `tokio-fileio-fallback` feature, use the Tokio/blocking file backend. Both backends read and write by explicit offset rather than a shared cursor.

Accepted write bytes are appended to `spool.dat` before `/write` returns, but they are not forced to stable storage per page. `/complete` syncs the data file before committing final complete metadata.

## Paging and cache

The byte stream is divided into fixed-size pages (`page_size`, default 4096 bytes).

Write path:

1. HTTP body bytes are accepted at the required sequential offset.
2. Bytes are written to `spool.dat` through `FileIO`.
3. Full pages become reader-visible.
4. Visible pages are inserted into a global FIFO page cache and waiting readers are notified.

The page cache is global across all spools. Entries are keyed by `(spool_key, page_index)` and share the single `max_cache_bytes` budget. Setting `max_cache_bytes` to `0` disables caching. Pages larger than the cap bypass the cache.

A trailing partial page may already be present in `spool.dat`, but it is not reader-visible until it becomes a full page or `/complete` publishes it as the final page.

## Read behaviour

Reads first check the global page cache. Cache misses read the required page bytes from `spool.dat` using positional I/O.

A request without `Range`, or with `Range: bytes=X-`, enters follow mode. If the requested byte has not been written yet, BOBS parks the request until more data arrives, the spool completes, the spool is deleted, or the long-poll timeout fires.

When the long-poll timeout fires, BOBS returns `307 Temporary Redirect` to a read URL for the same key. If a trusted ingress supplies a valid `X-Forwarded-Prefix`, the redirect preserves that external prefix; otherwise it falls back to `/api/v1/read/{key}`. The redirect is temporary and includes `Cache-Control: no-store` because the location can depend on request headers.

Completed reads include the full-object CRC-32C checksum header when available. Range reads update aggregate read-coverage tracking so cleanup can detect when the whole object has been served, even across multiple range requests.

## Write-locked spools

A spool can be created with `write_locked: true`. In this state writes are accepted, but reads return `423 Locked` until the spool is completed or made readable by a lifecycle transition. Completing a write-locked spool makes the final object readable.

The write-lock state is lifecycle metadata in `meta.json` and is recovered on restart.

## Completion and durability boundary

`/complete` validates the optional expected size before publishing final state. It then publishes any trailing partial page, syncs `spool.dat`, commits final metadata to `meta.json`, updates in-memory state/cache, and notifies readers.

After successful completion, `meta.json` is the durable completed-object record. Before completion, BOBS provides process-restart recovery from `spool.dat`, not stable-storage durability for each acknowledged page.

## Recovery

Startup recovery scans `data_dir` for spool directories with `meta.json` sidecars.

- `Writing` and `WriteLocked` spools are rebuilt from `spool.dat`; byte-derived metadata in the sidecar is advisory.
- `Complete` spools are accepted only if `spool.dat` satisfies the committed logical length.
- Interrupted metadata temp files are ignored.
- Unsafe or unrelated directories are not blindly removed. Orphan cleanup is restricted to UUID-shaped spool directories that look like BOBS spool directories.

## Cleanup rules

A background cleanup task removes expired spools and their key directories, including `spool.dat`, `meta.json`, and interrupted `meta.json.tmp` files.

Current cleanup triggers are:

- writer inactivity for producers that stop writing without completing;
- read-idle TTL for readable spools that have not served bytes recently, with never-read spools anchored at `readable_at`;
- full-read-complete TTL once aggregate coverage shows every byte has been served at least once.

Slow readers keep a spool alive only while they continue making read progress. Stalled connections do not protect a spool forever.
