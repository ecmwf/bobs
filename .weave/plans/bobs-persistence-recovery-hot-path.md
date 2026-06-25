# BOBS Persistence and Recovery Hot-Path Metadata Removal

## TL;DR
> **Summary**: Make `spool.dat` the source of truth for in-progress writes so `/write/{key}/{offset}` no longer commits redb metadata per accepted body, while recovery reconstructs byte counts, page visibility, partial-page buffer state, and CRC progress from disk. Keep redb commits for create, complete, delete, and write-lock/readability state transitions; keep `complete` syncing and persisting durable completion metadata.
> **Estimated Effort**: Large

## Context
### Original Request
Plan a change to BOBS persistence and recovery so per-write metadata commits are removed from the `Spool::write()` hot path without data-loss or corruption risk for producers that cannot rewind. The required invariant is that a `200 OK` from `/write/{key}/{offset}` means BOBS can account for those bytes after a BOBS process restart without asking the producer to resend; `200 OK` from `/complete/{key}` remains durable through process restart. The plan must address data-file-based recovery, durability semantics, optional high-water marks, page sizing, read-path correctness, cleanup interactions, the known failing slow-reader cleanup integration test, docs, tests, and benchmark verification.

### Key Findings
- `src/spool/writer.rs` currently calls `persist_metadata()` after every `write()`. `persist_metadata()` in `src/spool/mod.rs` serializes `SpoolMetadata` and performs `redb begin_write/open_table/insert/commit`, matching the benchmark bottleneck described in the request.
- `src/http/mod.rs::write_spool()` batches HTTP body frames into `config.page_size` chunks before calling `spool.write()`, but the final batch can be smaller than a page.
- `src/spool/writer.rs::write()` only writes full pages to `spool.dat`; partial-page bytes remain in `write_buffer` until another write fills the page or `/complete` flushes them. Removing redb commits without changing this would violate the required invariant for any `200 OK` that accepted a partial trailing page.
- `src/spool/lifecycle.rs::complete()` currently flushes the in-memory partial page, calls `F::sync_data()`, sets `state = Complete`, stores `checksum_crc32c`, and persists metadata. This is already the right durability boundary for completed spools.
- `src/manager.rs::recover()` currently trusts persisted `total_pages`/`total_bytes_written` unless the data file is shorter than metadata. This must change for `Writing` and `WriteLocked`, where persisted offsets will intentionally be stale.
- `src/spool/reader.rs::read_page()` uses `meta.total_pages` to decide which pages are visible. For in-progress spools, `total_pages` must mean fully visible pages only; a recovered trailing partial page must not become readable until `/complete`.
- `src/cleanup.rs` uses in-memory metadata for `last_write_at`; fewer redb writes do not affect cleanup while the process is alive, but recovery must seed `last_write_at = now` for in-progress spools so stale persisted metadata cannot cause immediate deletion.
- `tests/integration.rs::test_slow_reader_receiving_bytes_not_cleaned_up` is a known failing starting point: it expects post-idle `404 NOT_FOUND` but currently gets `206 PARTIAL_CONTENT`. This should be fixed by addressing cleanup semantics/timing, not by skipping or loosening the test.
- Config defaults currently use `page_size = 4096` in `src/config.rs` and docs. Larger pages improve throughput but change reader visibility latency and cache entry sizing, so page-size default changes should be separated from the persistence safety change unless benchmarks justify them.

## Objectives
### Core Objective
Remove redb metadata commits from the `/write` hot path while preserving the public API and guaranteeing that every byte acknowledged by `/write/{key}/{offset}` can be reconstructed from `spool.dat` after a BOBS process restart.

### Deliverables
- [ ] New persistence model documented in code comments and `docs/src/architecture.md` / `docs/src/key-behaviours.md`: redb is authoritative for lifecycle state; `spool.dat` is authoritative for in-progress byte counts and CRC progress.
- [ ] `Spool::write()` writes every non-empty accepted body to `spool.dat` before returning, updates only in-memory metadata/cache/notifications, and does not call `persist_metadata()`.
- [ ] `Spool::complete()` preserves durable completion semantics while adapting to the new always-on-disk trailing partial-page model.
- [ ] Recovery derives in-progress `total_bytes_written`, visible `total_pages`, trailing `write_buffer`, `final_page_size`, and `running_crc32c` from `spool.dat`.
- [ ] Read path remains correct for recovered in-progress and complete spools, including partial trailing pages and range validation.
- [ ] Cleanup remains safe with stale redb write offsets/timestamps and does not delete recovered in-progress spools immediately.
- [ ] Direct tests cover ack-then-restart consistency, partial-page recovery, continued writing after recovery, complete durability, CRC reconstruction equivalence, and cleanup safety.
- [ ] Existing tests are updated to reflect the new metadata persistence model without disabling or weakening coverage.
- [ ] Benchmark documentation and optional `bobs-benchmark` commands are added for before/after validation.

### Definition of Done
- [ ] `cargo fmt --all -- --check` passes.
- [ ] `cargo test` passes, including `tests/integration.rs::test_slow_reader_receiving_bytes_not_cleaned_up`.
- [ ] `cargo test benchmark` passes.
- [ ] A targeted recovery test proves bytes accepted by `/write` before restart can be completed/read after restart without producer rewind.
- [ ] A targeted metadata test proves redb `total_bytes_written` is not updated by ordinary writes and is updated by `complete`.
- [ ] Documentation under `docs/src/` states the new process-restart and storage-crash durability semantics.

### Guardrails (Must NOT)
- [ ] Do not change the public HTTP API routes or request/response formats for `/create`, `/write`, `/complete`, `/read`, or `/delete`.
- [ ] Do not require producer rewind after a BOBS process restart for any write that returned `200 OK`.
- [ ] Do not add new dependencies without explicit approval.
- [ ] Do not skip, disable, ignore, or paper over failing tests.
- [ ] Do not weaken checksum correctness or allow torn/incomplete pages to become readable before they are complete.
- [ ] Do not move redb commits to another per-write/per-page path such as a mandatory high-water-mark table.
- [ ] Do not change the default `page_size` in the same safety change unless benchmark and latency tradeoffs are explicitly reviewed.
- [ ] Do not use process-ephemeral wording such as “Phase 1” or “Step 2” in code comments, plan task names, or commits.

## TODOs

- [x] 1. Codify the persistence contract
  **What**: Define the exact model before modifying code: redb stores lifecycle metadata committed on create, complete, delete, and write-lock/readability transitions; for `Writing`/`WriteLocked`, persisted `total_bytes_written`, `total_pages`, `final_page_size`, and `checksum_crc32c` are advisory/stale and recovery must derive them from `spool.dat`. State that `/write` requires the bytes to be accepted by the kernel/file handle before returning, but does not require `sync_data()` because the required invariant is BOBS process restart, not node/storage crash before `complete`. Keep `sync_data()` in `/complete`.
  **Files**: `docs/src/architecture.md`, `docs/src/key-behaviours.md`, `docs/src/standalone-benchmark.md`
  **Acceptance**: Docs clearly distinguish process restart from node/storage crash and explain why `spool.dat` is the source of truth for in-progress spools.

- [x] 2. Add recovery helpers for disk-derived spool progress
  **What**: Add internal helpers that compute progress from file length and page size. For `Writing`/`WriteLocked`: `total_bytes_written = file_size`, `total_pages = file_size / page_size` (visible full pages only), `final_page_size = None`, trailing partial length is `file_size % page_size`. Load the trailing partial bytes into `write_buffer` during recovery so subsequent writes can complete the page correctly. Add a CRC helper that scans exactly `file_size` bytes for in-progress spools and the completed logical size for complete spools when checksum must be reconstructed.
  **Files**: `src/manager.rs` primarily; optionally `src/spool/mod.rs` or a new Rust module under `src/spool/` if helper placement is cleaner
  **Acceptance**: Unit tests can call or exercise recovery against a file with full pages plus a trailing partial and observe reconstructed metadata, buffer length/content, and CRC.

- [x] 3. Rewrite the write path around append-to-file-first semantics
  **What**: Change `Spool::write()` so every non-empty accepted body is written to `spool.dat` at the requested offset before returning. Only after a successful `F::write_at()` should in-memory CRC, `write_buffer`, `total_bytes_written`, `total_pages`, `last_write_at`, page cache, and notifications advance. Remove the final `persist_metadata()` call. Preserve strict offset validation and state validation. When buffered bytes produce one or more full pages, insert those pages into the cache, increment `total_pages`, and notify readers exactly as today. Do not call `F::sync_data()` per write.
  **Files**: `src/spool/writer.rs`, affected unit tests in `src/spool/writer.rs`, HTTP write-path comments in `src/http/mod.rs`
  **Acceptance**: A direct unit test writes a partial page, verifies file length equals acknowledged bytes before `complete`, verifies redb metadata remains at create-time offsets, then writes the remainder and verifies the completed page becomes readable.

- [x] 4. Adapt complete to metadata finalization rather than partial-page persistence
  **What**: Update `Spool::complete()` for the new invariant that trailing partial bytes are already on disk. It should use the existing `write_buffer` only to determine/cache the final partial page and mark it visible, not as the sole source of disk persistence. Preserve expected-size validation, CRC assignment, `state = Complete`, `readable_at`, `missing_ranges.initialize(total_size)`, `F::sync_data()`, `persist_metadata()`, and notification behavior. Ensure the order remains safe: validate state, finalize visible page metadata/cache, sync file data, validate expected size, then persist complete metadata.
  **Files**: `src/spool/lifecycle.rs`, lifecycle unit tests in `src/spool/lifecycle.rs`
  **Acceptance**: Completing a spool with a partial trailing page does not rewrite required bytes from memory as the only persistence mechanism; after restart, complete metadata and checksum are present and reads return the full object.

- [x] 5. Make manager recovery trust disk for in-progress spools
  **What**: Replace the current `file_size < expected_full_pages_bytes` correction logic for `Writing`/`WriteLocked` with disk-derived reconstruction. On recovery of in-progress spools, set `last_write_at = now_secs()` in memory, recompute `running_crc32c` over all `file_size` bytes, preload the trailing partial page into `write_buffer`, keep `final_page_size = None`, and leave redb unchanged unless a lifecycle/backfill field such as `readable_at` genuinely needs persistence. For `Complete`/`Readable`, continue to use persisted complete metadata as authoritative, but verify the file is not shorter than the persisted logical size; if shorter, mark checksum invalid/recompute or discard according to existing corruption policy without silently exposing unavailable bytes.
  **Files**: `src/manager.rs`, manager recovery tests in `src/manager.rs`
  **Acceptance**: A redb record with `state = Writing`, `total_bytes_written = 0`, and a non-empty `spool.dat` recovers with `total_bytes_written = file_size`, correct visible page count, correct trailing buffer, and correct CRC.

- [x] 6. Keep the read path correct with reconstructed metadata
  **What**: Audit `read_spool()` and `read_page()` against the new semantics. In-progress recovered trailing partial bytes may count toward `total_bytes_written` for offset validation but must not be returned by `read_page()` until a full page is available or the spool is completed. Complete spools must expose the final partial page using `final_page_size` and `total_pages` after completion or recovery. Bounded range handling should not report bytes as available unless the corresponding page can be served or the request is allowed to long-poll consistently with existing behavior.
  **Files**: `src/spool/reader.rs`, `src/http/mod.rs`, read-related tests in `src/spool/reader.rs`, `src/http/mod.rs`, `tests/integration.rs`
  **Acceptance**: Tests cover reading a recovered full page from disk, not reading a recovered trailing partial page before complete, and reading that same partial after complete.

- [x] 7. Keep cleanup safe with stale persisted write metadata
  **What**: Ensure cleanup uses live in-memory `last_write_at` updates during writes and that recovery seeds in-progress spools with a fresh `last_write_at` so stale redb timestamps cannot trigger immediate `writer_inactivity` deletion. Add tests for recovered in-progress spools with stale persisted timestamps and active post-recovery writes. Investigate and fix the root cause of `tests/integration.rs::test_slow_reader_receiving_bytes_not_cleaned_up` expecting `404` but receiving `206`; keep its intent and assertions unless the root cause proves it is testing the wrong contract.
  **Files**: `src/cleanup.rs`, `src/manager.rs`, `tests/integration.rs`, cleanup tests in `src/cleanup.rs`
  **Acceptance**: Recovered in-progress spools are not deleted on the first cleanup sweep solely because redb `last_write_at` is old; active writes refresh the in-memory idle anchor; the known slow-reader integration test passes without skips.

- [x] 8. Replace write-metadata persistence tests with new invariant tests
  **What**: Update tests that currently assert per-write redb persistence, especially `src/manager.rs::test_metadata_persists_total_pages_after_write`, to assert the new behavior: redb offsets remain stale after write and are persisted on complete. Add direct manager tests for ack-then-restart consistency, continued appends at recovered offsets, CRC equivalence across restart, and complete durability. Use existing `tempfile`, `redb`, and `TokioFileIO`; do not add dependencies.
  **Files**: `src/manager.rs`, `src/spool/writer.rs`, `src/spool/lifecycle.rs`, `tests/integration.rs`
  **Acceptance**: Tests fail against the current implementation for the right reasons and pass after the persistence/recovery changes.

- [x] 9. Add HTTP-level simulated restart coverage
  **What**: Add an integration helper that starts BOBS with a caller-owned temp storage root, performs `/create` and `/write`, stops/drops the server without `/complete`, creates a new `SpoolManager` over the same redb/data paths, calls `recover()`, then verifies the producer can continue from the acknowledged offset and complete/read the object. Include a partial-page case because that is the case the current implementation cannot recover safely without per-write redb commits.
  **Files**: `tests/integration.rs`
  **Acceptance**: An integration test proves `200 OK` from `/write` followed by BOBS process restart does not require producer rewind and yields byte-for-byte correct final reads.

- [x] 10. Decide against mandatory redb high-water marks in this change
  **What**: Do not add a per-write or per-page high-water-mark table because it recreates the redb hot-path cost. Document that recovery uses file length and CRC scan. If recovery time later becomes an operational concern, add a separate optional periodic checkpoint design that commits at coarse intervals or lifecycle transitions only; it must never be required for correctness.
  **Files**: `docs/src/architecture.md`, optionally `TODO.md` if the project tracks follow-up ideas there
  **Acceptance**: No new mandatory redb write occurs from `Spool::write()` or per completed page; documentation states checkpoints are an optimization, not a correctness dependency.

- [x] 11. Keep page-size default unchanged and benchmark wider-page recommendations
  **What**: Leave `Config::default().page_size` at `4096` for the correctness change to avoid mixing durability semantics with reader-latency/cache behavior. Document that larger page sizes such as `1 MiB`, `4 MiB`, and `16 MiB` should be benchmarked after metadata commits are removed; wider pages may improve throughput but delay reader visibility until a full page is available and reduce effective cache page count unless `max_cache_bytes` is increased.
  **Files**: `src/config.rs` only if comments need clarification; `docs/src/configuration.md`, `docs/src/key-behaviours.md`, `docs/src/standalone-benchmark.md`
  **Acceptance**: Defaults remain stable; docs explain the tradeoff and provide benchmark commands for comparing page sizes.

- [x] 12. Update generated docs if the repository expects checked-in book output
  **What**: After editing `docs/src/*.md`, update generated files under `docs/book/` if that is the repo’s documented workflow. If mdBook is not available or generated docs are not meant to be committed, leave `docs/book/` untouched and note this in the implementation summary.
  **Files**: `docs/book/architecture.html`, `docs/book/key-behaviours.html`, `docs/book/configuration.html`, `docs/book/standalone-benchmark.html`, `docs/book/print.html`, search index files as generated by mdBook
  **Acceptance**: Documentation source and generated output are consistent with project practice.

- [x] 13. Run correctness verification
  **What**: Run the full formatting and test suite, including focused tests for new invariants and the known failing cleanup test.
  **Files**: No source files; verification only
  **Acceptance**: The following commands pass:
  - `cargo fmt --all -- --check`
  - `cargo test`
  - `cargo test test_slow_reader_receiving_bytes_not_cleaned_up`
  - `cargo test recovery`
  - `cargo test benchmark`

- [x] 14. Run optional throughput verification with bobs-benchmark
  **What**: Use the standalone benchmark to compare the old bottleneck profiles against the new write path. Use a local BOBS config with representative `page_size` values and `max_cache_bytes` large enough to maintain cache capacity. Confirm write throughput improves and no correctness failures occur.
  **Files**: `docs/src/standalone-benchmark.md` for command examples and result interpretation
  **Acceptance**: Optional commands complete successfully and produce `SUMMARY:` lines with zero failures:
  - `cargo build --release --bin bobs --bin bobs-benchmark`
  - `cargo run --release --bin bobs-benchmark -- --base-url http://127.0.0.1:3000 --objects 512 --object-bytes 16777216 --write-request-bytes 16777216 --write-body-chunk-bytes 1048576 --read-body-chunk-bytes 1048576 --request-timeout-ms 300000`
  - Repeat with server `page_size` set to `1048576`, `4194304`, and `16777216` as needed.

## Verification
- [ ] All tests pass with `cargo test`.
- [ ] Formatting passes with `cargo fmt --all -- --check`.
- [ ] Benchmark smoke tests pass with `cargo test benchmark`.
- [ ] The known failing `tests/integration.rs::test_slow_reader_receiving_bytes_not_cleaned_up` passes for the intended cleanup contract.
- [ ] A simulated restart test proves a partial-page `/write` that returned `200 OK` can be recovered, appended to, completed, and read without producer rewind.
- [ ] A redb inspection test proves ordinary writes no longer persist offset/page metadata, while `/complete` persists final metadata.
- [ ] CRC tests prove restart reconstruction produces the same complete checksum as an uninterrupted write.
- [ ] Cleanup tests prove stale persisted write timestamps cannot delete a recovered in-progress spool immediately.
- [ ] No new dependencies are added.
- [ ] Public HTTP API compatibility is preserved.
- [ ] Documentation under `docs/src/` describes durability semantics, page visibility, and benchmark guidance.
