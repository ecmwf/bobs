# BOBS I/O Zero-Copy Positional Access and Global Cache

## TL;DR
> **Summary**: Replace the hot write/read file path with positional I/O that avoids buffer memcpy and spool-wide file cursor locking, switch HTTP read chunking to `Bytes` slicing, and make `max_cache_bytes` a true process-wide page-cache cap. The work keeps the public HTTP API and restart-recovery contract unchanged while making larger page sizes safe to benchmark later.
> **Estimated Effort**: Large

## Context
### Original Request
Plan four related BOBS performance/correctness changes together: remove `TokioFileIO::write_at`'s `data.to_vec()` clone, remove shared file seek/mutex serialization via positional I/O, replace HTTP read chunk copies with `Bytes::slice`, and replace per-spool page caches with one byte-capped global cache.

### Key Findings
- `src/io/tokio_fs.rs` currently uses `Arc<tokio::sync::Mutex<tokio::fs::File>>`, then `seek + write_all` / `seek + read`; `write_at` clones the whole request buffer with `data.to_vec()` before awaiting.
- `src/io/mod.rs` exposes `FileIO::write_at(&[u8])` and `FileIO::read_at(&mut [u8])`; those borrowed-buffer shapes are awkward for `tokio::task::spawn_blocking` because blocking closures must be `'static`.
- `src/spool/writer.rs` writes bytes to disk before advancing visible metadata, then publishes full pages into `spool.page_cache`. This is the invariant that makes an acknowledged `/write` recoverable after a BOBS process restart.
- `src/spool/reader.rs` reads cache first and disk second, and deliberately exposes only `meta.total_pages` pages; recovered trailing partial bytes stay hidden until more writes complete a page or `/complete` finalizes them.
- `src/spool/lifecycle.rs` keeps `/complete` as the durability boundary by publishing a trailing partial page, calling `F::sync_data`, then persisting completed metadata.
- `src/http/mod.rs::read_spool` currently yields each response chunk via `Bytes::copy_from_slice(&page[slice_start..slice_end])`; `Bytes::slice` can yield the same range without copying.
- `src/manager.rs::SpoolManager::new` converts `max_cache_bytes / page_size` to an entry count and gives that capacity to every new/recovered spool. This means memory can scale as `active_spool_count * max_cache_bytes`.
- Docs currently describe the cache as per-spool in `docs/src/architecture.md`, `docs/src/configuration.md`, `docs/src/key-behaviours.md`, and `docs/src/standalone-benchmark.md`.

## Objectives
### Core Objective
Make BOBS's data path use no-copy writes into positional file I/O, no-copy HTTP chunk slicing, and a single byte-capped page cache shared across all spools, without weakening restart recovery, completion durability, reader visibility, cleanup, or the public HTTP API.

### New I/O Contract
- [ ] `FileIO` should operate on owned/ref-counted write buffers (`bytes::Bytes`) so the default implementation can move data into `spawn_blocking` without `data.to_vec()`.
- [ ] `FileIO` should expose positional reads that return owned bytes for a requested `(offset, len)`, avoiding shared file cursor state and avoiding a caller-provided mutable borrow across a blocking task.
- [ ] The default Unix implementation should hold `Arc<std::fs::File>` and use `std::os::unix::fs::FileExt::{read_at, write_at}` inside `tokio::task::spawn_blocking`.
- [ ] `write_at` should loop until the full `Bytes` buffer is written or return an error; short successful writes must not be silently accepted.
- [ ] `read_at` may return fewer bytes at EOF, but callers that know a logical range must exist must convert unexpected short reads to `UnexpectedEof`.
- [ ] `sync_data` should continue to run before completed metadata is persisted.
- [ ] Non-Unix target behavior should be explicit: either keep a documented correctness-preserving fallback or gate the positional implementation with clear compile/docs language before merging.

### New Cache Model
- [ ] Replace per-spool `PageCache` ownership with one shared cache keyed by `(spool_key, page_idx)`.
- [ ] Track capacity in bytes using `max_cache_bytes`, not entry count.
- [ ] Treat `max_cache_bytes` as a global cap; if a single page is larger than the cap, do not cache that page rather than exceeding the cap.
- [ ] Use FIFO eviction across all spools. Justification: BOBS pages are append-once and mostly consumed sequentially; FIFO matches the current streaming access pattern, avoids read-touch churn, and avoids LRU keeping old range rereads at the expense of newly published writer pages.
- [ ] Remove all cached entries for a spool during `delete_spool`.
- [ ] Allow `page_size > max_cache_bytes`; reads still work through disk misses when pages are too large to cache.

### Deliverables
- [ ] Revised `FileIO` trait and `TokioFileIO` implementation with no write-buffer clone, no seek, and no shared file mutex on Unix.
- [ ] Write path adjusted to pass `Bytes` through to file I/O and publish full-page `Bytes` slices without unnecessary full-page copies where practical.
- [ ] HTTP read stream chunking changed to zero-copy `Bytes::slice`.
- [ ] Global byte-capped FIFO page cache shared by all spools.
- [ ] Config and manager validation updated so `max_cache_bytes` is a true global cap and no longer must be at least `page_size`.
- [ ] Unit, integration, and smoke benchmark coverage for the four requested changes and the required invariants.
- [ ] Docs updated for the I/O OS scope, global cache semantics, and benchmark guidance.

### Definition of Done
- [ ] `cargo fmt --all -- --check` passes.
- [ ] `cargo test` passes.
- [ ] `cargo test test_slow_reader_receiving_bytes_not_cleaned_up` passes.
- [ ] `cargo test recovery` passes.
- [ ] `cargo test benchmark` passes.
- [ ] Local `bobs-benchmark` smoke runs at 512 × 16 MiB for 1 MiB, 4 MiB, and 16 MiB pages report `successes == 512` and `failures == 0`.
- [ ] No public route or request/response shape changes for `/create`, `/write`, `/complete`, `/read`, or `/delete`.
- [ ] Existing ack-then-restart recovery tests still prove producers do not need to rewind after a `200 OK` from `/write`.

### Guardrails (Must NOT)
- [ ] Do not add new dependencies without explicit approval.
- [ ] Do not break the public HTTP API.
- [ ] Do not require producers to rewind acknowledged bytes after restart.
- [ ] Do not reintroduce per-write or per-page `redb` metadata commits.
- [ ] Do not weaken `/complete`: it must keep `sync_data` before final metadata persistence.
- [ ] Do not expose trailing partial bytes before a full page exists or `/complete` finalizes them.
- [ ] Do not break the cleanup contract or slow-reader activity tracking.
- [ ] Do not change `Config::default().page_size`; it must remain `4096`.
- [ ] Do not use process-ephemeral wording in code comments, plan task names, or commits.
- [ ] Do not use task titles containing “Phase”, “Step N”, or “Round”.

## TODOs

- [x] 1. Rewrite the `FileIO` contract around positional owned-buffer I/O
  **What**: Change `src/io/mod.rs` so writes accept `bytes::Bytes` and reads request a byte length and return owned bytes. Preserve `create`, `open`, `sync_data`, `close`, and `remove`. Update trait docs to state that reads/writes are positional and must not depend on or mutate a shared file cursor.
  **Files**: `src/io/mod.rs`, `src/io/tokio_fs.rs`, `src/spool/lifecycle.rs` test helper `CountingFileIO`
  **Acceptance**: All implementors compile against the new trait; callers no longer pass `&mut [u8]` into `FileIO`; docs on the trait name the positional contract.

- [x] 2. Implement Unix positional `TokioFileIO` without file mutex or write clone
  **What**: Make the Unix default handle `Arc<std::fs::File>`. Use `std::fs::OpenOptions` in `spawn_blocking` for create/open, `FileExt::write_at` in a full-write loop for writes, `FileExt::read_at` for reads, and `File::sync_data` in `spawn_blocking` for completion durability. Move `Bytes` into the blocking write closure by refcount, not memcpy. Decide and document the non-Unix fallback/gating before implementation lands.
  **Files**: `src/io/tokio_fs.rs`, `docs/src/architecture.md`
  **Acceptance**: `src/io/tokio_fs.rs` contains no `Arc<Mutex<File>>`, no `seek`, and no `data.to_vec()` on Unix; direct `TokioFileIO` tests prove positional behavior.

- [x] 3. Update write callers to pass `Bytes` and avoid full-page publication copies
  **What**: Change `Spool::write` to accept `Bytes` (or an equivalent owned/ref-counted buffer) and pass it directly to `F::write_at`. In `write_spool`, freeze page-sized `BytesMut` batches and pass the resulting `Bytes` to the spool. In `Spool::write`, continue writing to disk before advancing metadata; update CRC from the same bytes; publish full pages via `Bytes::slice` when the incoming buffer contains complete pages; use `write_buffer` only for cross-call partial page assembly.
  **Files**: `src/http/mod.rs`, `src/spool/writer.rs`, `src/spool/lifecycle.rs`, `src/spool/reader.rs`, `src/manager.rs`, relevant unit tests under `src/spool/*`
  **Acceptance**: A successful `/write` still appends to `spool.dat` before metadata advances; existing offset-mismatch and ack-then-restart tests pass; the hot path no longer performs the former whole-buffer `to_vec()` clone.

- [x] 4. Replace per-spool cache with a global byte-capped FIFO cache
  **What**: Rework `PageCache` into a shared global cache keyed by `(spool_key, page_idx)`, using a `HashMap` for entries, a FIFO queue for eviction order, `current_bytes`, and `max_bytes`. Insertions should evict oldest valid entries until `current_bytes <= max_bytes`. Updating an existing key must adjust byte accounting. Oversized pages must not be cached. Add `remove_spool(&str)`, `current_bytes()`, `max_bytes()`, and test-only inspection helpers as needed.
  **Files**: `src/spool/page_cache.rs`, `src/spool/mod.rs`, `src/spool/writer.rs`, `src/spool/reader.rs`, `src/spool/lifecycle.rs`, `src/manager.rs`
  **Acceptance**: Only one cache instance is created by `SpoolManager`; all spools receive an `Arc` to it; total cached bytes never exceed `max_cache_bytes`; `delete_spool` drops that spool's cached entries.

- [x] 5. Adjust manager construction, recovery, and deletion for shared cache ownership
  **What**: Replace `SpoolManager::page_cache_capacity` with a shared cache field. Construct it once in `SpoolManager::new(max_cache_bytes)`, pass it to `Spool::new` for create and recover, and call cache removal from `delete_spool` when a spool is removed. Ensure recovery does not prepopulate stale pages and does not trust stale byte-derived `redb` metadata.
  **Files**: `src/manager.rs`, `src/spool/mod.rs`, `src/spool/writer.rs`, `src/spool/reader.rs`, `src/spool/lifecycle.rs`
  **Acceptance**: Create, recover, complete, and delete flows compile with the shared cache; recovery still derives in-progress byte state from `spool.dat`; delete removes cache entries even if directory removal reports `NotFound`.

- [x] 6. Relax cache-size validation while preserving page-size validation
  **What**: Remove the `max_cache_bytes < page_size` rejection from both `Config::validate` and `SpoolManager::new`. Keep rejecting `page_size == 0`. Decide whether `max_cache_bytes == 0` explicitly disables caching; if accepted, document and test that behavior.
  **Files**: `src/config.rs`, `src/manager.rs`, `docs/src/configuration.md`, `docs/src/key-behaviours.md`
  **Acceptance**: Configs with `page_size > max_cache_bytes` validate; default config remains unchanged; tests cover larger page than cache and optional zero-cache behavior if implemented.

- [x] 7. Change HTTP read chunks to zero-copy slices
  **What**: Replace `Bytes::copy_from_slice(&page[slice_start..slice_end])` in `read_spool` with `page.slice(slice_start..slice_end)`. Keep `offset`, `last_read_activity_at`, missing-range coverage, and `last_read_at` updates exactly tied to yielded byte ranges. Add a small helper if needed so the zero-copy behavior is directly unit-testable.
  **Files**: `src/http/mod.rs`
  **Acceptance**: A read-path unit test proves the yielded chunk shares the cached page allocation using `Bytes::ptr_eq` if suitable, or pointer-range equality (`chunk.as_ptr() == page.as_ptr().add(slice_start)`) as a fallback assertion.

- [x] 8. Add direct positional file I/O tests
  **What**: Extend `src/io/tokio_fs.rs` tests for positional read/write correctness, concurrent disjoint writes without shared seek state, parallel reads of the same page from disk, and parallel reads while writes append to later offsets. Include read-beyond-EOF behavior under the new owned-read contract.
  **Files**: `src/io/tokio_fs.rs`
  **Acceptance**: `cargo test io::tokio_fs` passes and fails against a seek/shared-cursor implementation under concurrency-sensitive assertions where feasible.

- [x] 9. Add global cache unit tests
  **What**: Replace existing per-spool cache tests with byte-accurate global cache tests: insert/get by `(spool_key, page_idx)`, byte capacity accounting, FIFO eviction across spools, update-existing accounting, oversized-page skip, eviction triggered by writes from any spool, `remove_spool`, and interleaved hot/cold spool behavior for the chosen FIFO policy.
  **Files**: `src/spool/page_cache.rs`, affected tests in `src/spool/writer.rs`, `src/spool/reader.rs`, `src/spool/lifecycle.rs`, `src/manager.rs`
  **Acceptance**: Cache tests assert `current_bytes <= max_bytes` after every insertion and prove entries from different spools compete under one cap.

- [x] 10. Add read-path and spool behavior tests for zero-copy and visibility invariants
  **What**: Add or update tests showing cached full pages are returned as shared `Bytes`, disk pages are read correctly after cache eviction, recovered trailing partial bytes are not returned before completion, trailing partial bytes are returned after completion, and `/complete` still publishes the final partial page then persists metadata after `sync_data`.
  **Files**: `src/http/mod.rs`, `src/spool/reader.rs`, `src/spool/writer.rs`, `src/spool/lifecycle.rs`, `src/manager.rs`
  **Acceptance**: Tests cover the four required invariants: no producer rewind, unchanged HTTP API, `/complete` sync/final metadata, and page-based reader visibility.

- [x] 11. Add integration coverage for concurrency and global cap behavior
  **What**: Extend `tests/integration.rs` with an in-flight stream test where a writer continues to complete while multiple parallel readers read ranges or follow the object, and with a many-spool test that writes enough full pages through HTTP to exceed `max_cache_bytes` while asserting via test server state that the shared cache cap is honored. Keep the existing ack-then-restart partial-page recovery test and cleanup tests intact.
  **Files**: `tests/integration.rs`
  **Acceptance**: New integration tests pass repeatedly with generous timeouts; writer completion is not blocked behind active readers; total cached bytes remains within the configured global cap under many simultaneously active spools.

- [x] 12. Update docs for OS scope, global cache semantics, and benchmark guidance
  **What**: Document that the default high-performance positional implementation uses Unix `FileExt` (`pread`/`pwrite` style) and explain non-Unix behavior. Update cache descriptions from per-spool entry-count capacity to global byte capacity. Update page-size tuning text so `page_size` no longer needs to be less than or equal to `max_cache_bytes`; larger pages may simply bypass cache if each page exceeds the cap.
  **Files**: `docs/src/architecture.md`, `docs/src/configuration.md`, `docs/src/key-behaviours.md`, `docs/src/standalone-benchmark.md`
  **Acceptance**: Docs no longer say the cache is per-spool or `max_cache_bytes / page_size` per spool; docs explicitly name Unix scope for positional I/O; benchmark guide keeps the default page size at `4096`.

- [x] 13. Run verification and benchmark smoke checks
  **What**: Run formatting, tests, targeted recovery/cleanup/benchmark test filters, and the local 512 × 16 MiB benchmark smoke runs for 1 MiB, 4 MiB, and 16 MiB page sizes. Compare wall throughput against the prior local sweep and confirm no failures.
  **Files**: no source files; uses `Cargo.toml`, `src/bin/bobs-benchmark.rs`, docs benchmark configs under `/tmp`
  **Acceptance**: All verification commands below pass; benchmark summaries show `successes == 512` and `failures == 0` for each page size.

## Verification
- [ ] `cargo fmt --all -- --check`
- [ ] `cargo test`
- [ ] `cargo test test_slow_reader_receiving_bytes_not_cleaned_up`
- [ ] `cargo test recovery`
- [ ] `cargo test benchmark`
- [ ] Start BOBS for 1 MiB pages with a config equivalent to:
  ```bash
  cat >/tmp/bobs-bench-1m.yaml <<'YAML'
  host: 127.0.0.1
  port: 3000
  data_dir: /tmp/bobs-bench-data-1m
  page_size: 1048576
  max_cache_bytes: 268435456
  host_prefix: bobs
  route_name: download
  domain: 127.0.0.1:3000
  YAML
  rm -rf /tmp/bobs-bench-data-1m
  HOSTNAME=bobs-0 BOBS_INTERNAL_BASE_URL_TEMPLATE=http://127.0.0.1:3000/api/v1 \
    cargo run --release --bin bobs -- /tmp/bobs-bench-1m.yaml
  ```
- [ ] In another terminal, run the 1 MiB benchmark smoke:
  ```bash
  cargo run --release --bin bobs-benchmark -- \
    --base-url http://127.0.0.1:3000 \
    --objects 512 \
    --object-bytes 16777216 \
    --write-body-chunk-bytes 1048576 \
    --read-body-chunk-bytes 1048576 \
    --start-delay-ms 2000 \
    --request-timeout-ms 900000 \
    --summary-json /tmp/bobs-summary-page-1m.json
  ```
- [ ] Repeat the server/benchmark smoke for 4 MiB pages using `page_size: 4194304`, `max_cache_bytes: 1073741824`, `data_dir: /tmp/bobs-bench-data-4m`, `--write-body-chunk-bytes 4194304`, `--read-body-chunk-bytes 4194304`, and summary `/tmp/bobs-summary-page-4m.json`.
- [ ] Repeat the server/benchmark smoke for 16 MiB pages using `page_size: 16777216`, `max_cache_bytes: 4294967296`, `data_dir: /tmp/bobs-bench-data-16m`, `--write-body-chunk-bytes 16777216`, `--read-body-chunk-bytes 16777216`, and summary `/tmp/bobs-summary-page-16m.json`.
- [ ] Confirm each summary has `successes == 512` and `failures == 0`, for example:
  ```bash
  python - <<'PY'
  import json
  for label in ['1m', '4m', '16m']:
      path = f'/tmp/bobs-summary-page-{label}.json'
      s = json.load(open(path))
      assert s['successes'] == 512, (path, s['successes'])
      assert s['failures'] == 0, (path, s['failures'])
      print(label, 'wall_mib_s=', s.get('wall_mib_s'), 'write_active_mib_s=', s.get('write_active_mib_s'))
  PY
  ```
- [ ] Inspect benchmark output for improved wall throughput and no read/write failures compared with the previous local profile sweep.
- [ ] Verify docs build or render path if the project has a docs command; otherwise review changed Markdown manually.
