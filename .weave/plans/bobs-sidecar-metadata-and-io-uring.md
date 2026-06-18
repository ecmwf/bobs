# BOBS Sidecar Metadata and io_uring

## TL;DR
> **Summary**: Replace the redb metadata database with per-spool `meta.json` sidecars committed by atomic write/rename/directory-fsync, then make Linux default file I/O use `io_uring` while preserving the current `FileIO` trait and HTTP API. Add tests-first coverage for crash-boundary metadata, recovery parity, legacy migration, linked SQE ordering, cleanup safety, and benchmark the final build against the Tokio fallback.
> **Estimated Effort**: XL

## Context
### Original Request
Plan a substantial BOBS change delivered as one ordered plan file:

- Group 1: replace redb with per-spool sidecar metadata files.
- Group 2: switch `FileIO` from Unix `FileExt` plus `tokio::task::spawn_blocking` to Linux `io_uring`, keeping the trait shape unchanged and retaining `TokioFileIO` as an opt-in fallback.
- Group 3: benchmark and report using the prior A/B/C/D sweep shape and rust-gdb stack sampling.

The request requires preserving public `/create`, `/write`, `/complete`, `/read`, and `/delete` API shapes; preserving acknowledged-write restart safety; preserving `/complete` ordering with `spool.dat` `sync_data` before final metadata; keeping `Config::default()` unchanged; keeping cleanup TTL semantics; keeping page visibility rules; and avoiding forbidden sequencing labels in code, comments, plan task names, and commits.

### Key Findings
- `Cargo.toml` currently depends on `redb = "2"`; `src/error.rs` exposes `BobsError::StorageError(Box<redb::Error>)`, so removing redb requires error-shape changes.
- `src/manager.rs` owns `Arc<redb::Database>`, exposes `SPOOL_TABLE`, writes create/delete metadata through redb transactions, lists redb rows for recovery, and batches stale-row deletion after recovery.
- `src/spool/mod.rs::Spool` stores `Arc<redb::Database>`, and `persist_metadata()` serializes `SpoolMetadata` then performs `begin_write/open_table/insert/commit`.
- `src/spool/lifecycle.rs::complete()` already preserves the critical order: publish/cache final visible bytes, call `F::sync_data()` on `spool.dat`, then persist `state = Complete` metadata.
- `src/spool/writer.rs::write()` already appends every accepted non-empty body to `spool.dat` before advancing in-memory metadata; ordinary writes no longer call `persist_metadata()`. This is the base invariant for producer no-rewind recovery.
- `src/manager.rs::recover()` already reconstructs in-progress byte counts, page counts, trailing partial buffer, and CRC from `spool.dat`; the data source for lifecycle rows must change from redb rows to `<data_dir>/<key>/meta.json`.
- `src/cleanup.rs` and many unit tests inspect or rewrite redb rows directly; these helpers must move to `MetadataStore` or sidecar file helpers.
- `src/io/mod.rs::FileIO` already uses owned `bytes::Bytes` for writes and reads, matching `io_uring` buffer-lifetime needs.
- `src/io/tokio_fs.rs::TokioFileIO` is Unix-only and uses `Arc<std::fs::File>`, `FileExt::read_at/write_at`, and `spawn_blocking`; it has useful positional-correctness tests that should be reused for the `io_uring` backend.
- `src/main.rs`, `tests/integration.rs`, and `tests/standalone_benchmark.rs` hardcode `TokioFileIO` and redb paths; they need generic default backend aliases and sidecar storage roots.
- Docs under `docs/src/architecture.md`, `docs/src/key-behaviours.md`, and `docs/src/standalone-benchmark.md` describe redb as the lifecycle metadata store and must be rewritten.
- `/tmp/bobs_runs_abcd.py` and `/tmp/bobs_run_d_gdb.py` are present and encode the prior benchmark sweep and stack-sampling workflow.

## Objectives
### Core Objective
Remove redb from the runtime metadata path by using one `meta.json` sidecar per spool, then make Linux default disk I/O use a low-level `io_uring` driver with linked SQE support for metadata commits, without changing the public HTTP API or acknowledged-write/recovery semantics.

### Deliverables
- [ ] A `MetadataStore` abstraction analogous to `FileIO`, with `write`, `read`, `delete`, and startup-only `list` operations.
- [ ] A synchronous sidecar metadata store for tests and non-Linux Unix fallback.
- [ ] A Linux `io_uring` sidecar metadata store whose hot commit path submits write, fdatasync, rename, and directory fsync as one linked SQE chain.
- [ ] A one-shot legacy redb-to-sidecar migration that is idempotent, preserves byte-equal serialized `SpoolMetadata`, removes the legacy DB only after all sidecars are durable, and does not require the `redb` crate in the final build.
- [ ] `SpoolManager`, `Spool`, recovery, and cleanup rewritten to use `MetadataStore` rather than `Arc<redb::Database>` and `SPOOL_TABLE`.
- [ ] A Linux default `UringFileIO` implementation of the existing `FileIO` trait, with `TokioFileIO` retained behind an opt-in feature for benchmarking and fallback.
- [ ] Tests-first coverage for sidecar atomicity, migration, recovery parity, cleanup safety, no double-fsync, io_uring positional correctness, linked-chain fault injection, and HTTP restart invariants.
- [ ] Docs updated to describe sidecar layout, durability, migration, default Linux io_uring behaviour, fallback builds, and benchmark commands.
- [ ] Benchmark artifacts comparing wall/write/read MiB/s, CPU seconds, syscall counts, RSS, p50/p95 timings, and stack-sample histograms.

### Definition of Done
- [ ] `cargo fmt --all -- --check` passes.
- [ ] `cargo test` passes on Linux with the default io_uring backend.
- [ ] `cargo test --features tokio-fileio-fallback` passes on Linux.
- [ ] `cargo test` passes on a non-Linux Unix target if available, using the synchronous sidecar store and `TokioFileIO` fallback.
- [ ] `cargo tree -i redb` returns no dependency path in the final default build.
- [ ] `cargo test metadata_store_atomic_rename` proves crash-boundary sidecar invariants.
- [ ] `cargo test migration_from_redb` proves N legacy rows migrate to N sidecars and re-running migration is a no-op.
- [ ] `cargo test io_uring_fileio` proves positional correctness, EOF reads, short-write retry, and fsync error propagation.
- [ ] `cargo test io_uring_linked_chain` proves an fdatasync failure cancels the rename operation in the linked metadata chain.
- [ ] `cargo test test_slow_reader_receiving_bytes_not_cleaned_up` and other cleanup tests pass.
- [ ] `cargo test test_http_restart_after_acknowledged` proves a `200 OK` write can be continued after restart without producer rewind.
- [ ] `cargo test io_uring_16m_page_smoke_not_slower_than_tokio_fileio -- --ignored` passes or produces an explicit investigated benchmark note.
- [ ] The A/B/C/D benchmark sweep completes with zero object failures and a written comparison report.

### Guardrails (Must NOT)
- [ ] Do not change public HTTP routes, methods, request bodies, response bodies, status-code contracts, or headers.
- [ ] Do not require producers to resend bytes after any `/write` response that returned `200 OK`.
- [ ] Do not weaken `/complete`: `spool.dat` must still be `sync_data`-ed before final `state = Complete` metadata is committed.
- [ ] Do not change `Config::default().page_size` or add surprise default config changes.
- [ ] Do not change cleanup TTL semantics or page visibility semantics.
- [ ] Do not add direct dependencies other than the chosen `io_uring` crate; any required transitive crates must arrive through it.
- [ ] Do not add a hidden incompatible async runtime; the io_uring implementation must cooperate with the existing tokio/axum/hyper stack.
- [ ] Do not invest in kernels older than Linux 5.11 for the io_uring rename operation.
- [ ] Do not remove migration support in the same delivery that removes runtime redb metadata; the final build must keep a read-only legacy migration path without depending on the redb crate.
- [ ] Do not use forbidden sequencing labels in code comments, plan task names, or commit messages.

## TODOs

- [x] 1. Choose and codify the io_uring crate and backend feature layout
  **What**: Use the low-level `io-uring` crate as the only new direct dependency, target-gated to Linux. Reject `tokio-uring` because it introduces a separate runtime model and does not expose enough linked-SQE control for the metadata commit; reject `compio` for the same runtime/control concerns; reject `rio` because it is narrower, less aligned with general filesystem ops, and does not satisfy the rename/open/unlink chain requirements. Add features so Linux defaults to `UringFileIO` plus `UringSidecarMetadataStore`, while `--features tokio-fileio-fallback` uses `TokioFileIO` plus the synchronous sidecar store for baseline benchmarking. Use `io-uring` directly because it exposes SQE flags such as `IOSQE_IO_LINK` and opcodes for read/write/fsync/openat/rename/unlink on modern kernels while avoiding an extra async runtime.
  **Files**: `Cargo.toml`, `src/io/mod.rs`, new `src/io/uring_fs.rs`, new `src/metadata.rs` or `src/metadata/mod.rs`, `src/lib.rs`, `src/main.rs`
  **Acceptance**: `cargo tree -i io-uring` shows the approved crate; `cargo tree -i redb` still exists only until the migration/removal task; Linux default type aliases select uring backends; `cargo build --features tokio-fileio-fallback` selects the fallback backends without changing config defaults.

- [x] 2. Add sidecar metadata tests and a filesystem fault-injection harness before implementation
  **What**: Create tests first for the sidecar protocol. Add a small internal harness that models `meta.json.tmp` creation/write, tmp fdatasync, atomic rename to `meta.json`, and parent directory fsync, with injected stops/failures around the three required crash points: after write before tmp fdatasync, after tmp fdatasync before rename, and after rename before directory fsync. Tests must verify recovery never parses a torn `meta.json`, ignores leftover `meta.json.tmp`, and observes either the old durable metadata or the new complete metadata according to whether the rename is represented in the test fixture.
  **Files**: new `src/metadata.rs` or `src/metadata/mod.rs`, optional `src/metadata/testing.rs` behind `#[cfg(test)]`
  **Acceptance**: New tests fail against the current redb implementation and pass only once a sidecar store exists; planned test filters include `cargo test metadata_store_atomic_rename` and `cargo test sidecar_ignores_tmp_on_recovery`.

- [x] 3. Implement the `MetadataStore` trait and synchronous sidecar store
  **What**: Define `MetadataStore` with future-returning `write`, `read`, and `delete`, plus a sync startup-only `list` using an associated iterator or owned iterator result. Implement `SyncSidecarMetadataStore` using `<data_dir>/<key>/meta.json`, writing `meta.json.tmp`, `sync_data` on the tmp file, `std::fs::rename`, and fsync of the spool directory. Ensure JSON serialization uses the current `serde_json::to_vec` field order and that read returns `Ok(None)` for missing sidecars and `StorageError`/`SerializationError` for corrupt sidecars. Delete should remove both final and tmp metadata where present and tolerate `NotFound`.
  **Files**: new `src/metadata.rs` or `src/metadata/mod.rs`, `src/error.rs`, `src/lib.rs`
  **Acceptance**: Unit tests cover write/read/delete/list, atomic replacement preserving the old final file until rename, tmp cleanup tolerance, corrupt JSON handling, directory fsync errors, and non-Linux compilation of the sync store.

- [x] 4. Add legacy migration tests and byte-preservation fixtures before migration implementation
  **What**: Add tests that create a legacy redb database with N `SPOOL_TABLE` rows using the current serialization, then assert migration writes N `<data_dir>/<key>/meta.json` files whose bytes exactly equal the legacy row values, recovers the same `SpoolMetadata`, and removes the legacy DB only after all sidecars are durable. Add idempotence tests where some sidecars already exist, where a previous run left `meta.json.tmp`, and where re-running migration after DB removal is a no-op.
  **Files**: new `src/metadata/legacy_redb.rs` or equivalent, tests in `src/metadata.rs` and `src/manager.rs`
  **Acceptance**: `cargo test migration_from_redb` fails before migration exists; tests compare raw redb row bytes to `meta.json` bytes and assert no duplicate or missing sidecars after a repeat run.

- [x] 5. Implement one-shot redb-to-sidecar migration without final redb dependency
  **What**: Isolate legacy reading in a read-only migration module. Because final `Cargo.toml` must drop `redb` while startup migration must remain, vendor the minimal read-only redb table-walking code needed for `TableDefinition<&str, &[u8]>` rows into `src/metadata/legacy_redb.rs` with upstream license attribution, or keep a temporary redb-backed migration only in an intermediate commit and replace it with the vendored reader before removing the dependency. Migration should scan legacy candidates such as `<data_dir>/spools.redb` and `<data_dir>/bobs.redb`, validate each row as `SpoolMetadata`, write sidecars using an internal raw-byte migration path for byte equality, fsync affected spool directories, fsync `data_dir`, then remove the legacy DB and fsync `data_dir` again. Corrupt rows should be logged and left for explicit operator review rather than silently producing partial sidecars.
  **Files**: `src/metadata/legacy_redb.rs`, `src/metadata.rs`, `src/manager.rs`, `Cargo.toml`, `LICENSE` or `NOTICE` if vendored code requires attribution
  **Acceptance**: Final `cargo tree -i redb` has no dependency path; startup with a legacy DB migrates rows before `recover()` lists sidecars; migration is safe to retry after interruption; the legacy DB is removed only after all migrated sidecars have durable final files.

- [x] 6. Rewrite manager and spool construction around `MetadataStore`
  **What**: Change `SpoolManager` to be generic over `F: FileIO` and `M: MetadataStore`, store `metadata_store: M` instead of `db: Arc<Database>`, and derive sidecar paths from `data_dir`. Change `Spool::new` to receive the metadata store clone instead of the redb handle, and change `Spool::persist_metadata` into an async `MetadataStore::write` call. Update `create_spool` to create the spool directory and `spool.dat`, write initial metadata sidecar, then insert the in-memory spool. Update lifecycle transitions to await metadata writes. Keep `complete` ordering with `F::sync_data()` before final metadata write.
  **Files**: `src/manager.rs`, `src/spool/mod.rs`, `src/spool/lifecycle.rs`, `src/spool/writer.rs`, `src/spool/reader.rs`, `src/http/mod.rs`, `src/main.rs`, `tests/integration.rs`, `tests/standalone_benchmark.rs`
  **Acceptance**: Manager and spool code compile without `SPOOL_TABLE`; create, set-write-locked, complete, and delete metadata writes go through `MetadataStore`; `/complete` still performs exactly one `sync_data` on `spool.dat` before final metadata.

- [x] 7. Rewrite recovery and cleanup for sidecar parity
  **What**: Replace redb table scanning with `metadata_store.list()`. Preserve all current recovery semantics: remove `Creating` and `Deleting` spools, seed fresh `last_write_at` for `Writing`/`WriteLocked`, backfill `readable_at` for old complete/readable metadata, derive in-progress byte counts and trailing buffer from `spool.dat`, recompute CRC as today, discard complete/readable spools whose files are shorter than persisted logical size, and remove orphan spool directories with no sidecar. Replace batched redb stale-row deletion with per-key sidecar deletion plus directory cleanup. Update cleanup tests that currently rewrite redb rows to rewrite sidecars.
  **Files**: `src/manager.rs`, `src/cleanup.rs`, `src/spool/lifecycle.rs`, `tests/integration.rs`
  **Acceptance**: Existing recovery properties are covered by sidecar tests: states, byte counts, recovered timestamps, metadata correction, readable-at backfill, corrupt metadata discard, missing data files, orphan dirs, and fresh recovered `last_write_at`. `cargo test recovery` and `cargo test cleanup` pass.

- [x] 8. Add no-double-fsync and completion-order tests before uring metadata implementation
  **What**: Add counting test backends for `FileIO` and `MetadataStore` that record `spool.dat` `sync_data`, metadata write, tmp fdatasync, rename, and directory fsync events. Assert `/complete` performs one data-file `sync_data`, then one metadata commit containing the four metadata operations, with no extra data-file syncs and no final metadata before `sync_data`.
  **Files**: `src/spool/lifecycle.rs`, `src/metadata.rs`, `tests/integration.rs` if HTTP-level verification is useful
  **Acceptance**: `cargo test complete_does_one_data_sync_and_one_metadata_commit` fails before instrumentation and passes once completion uses the store correctly.

- [x] 9. Add io_uring driver and `FileIO` tests before data-path implementation
  **What**: Add Linux-only tests for the future `UringFileIO`: parallel disjoint writes, parallel reads from the same file, read beyond EOF returning empty `Bytes`, short-write completion retry until the full buffer is accepted, fsync error propagation, remove/unlink behavior, and close/drop safety. Add a fake submitter for short completions and fsync errors so tests do not depend on real disk errors. Reuse the existing `TokioFileIO` test cases where possible by parameterizing a common `FileIO` test module.
  **Files**: `src/io/mod.rs`, `src/io/tokio_fs.rs`, new `src/io/uring_fs.rs`
  **Acceptance**: `cargo test io_uring_fileio` exists and fails before `UringFileIO` is implemented; `cargo test tokio_fileio` still passes for the fallback implementation.

- [x] 10. Implement the Linux `io_uring` data path behind the existing `FileIO` trait
  **What**: Implement `UringFileIO` using a dedicated ring driver, not a second async runtime. The driver should own `io_uring::IoUring`, accept operation requests from tokio tasks, keep owned buffers alive in an in-flight map until CQE completion, and resolve tokio oneshot senders. Implement create/open/remove with io_uring openat/unlinkat where practical, positional read/write with owned buffers and explicit offsets, write retry on short positive completions, `sync_data` using fdatasync semantics, and close by dropping the owned file descriptor only after no operation references it. Use Linux 5.11+ assumptions for rename support elsewhere; do not add older-kernel fallbacks.
  **Files**: `src/io/uring_fs.rs`, `src/io/mod.rs`, `Cargo.toml`
  **Acceptance**: Linux default build uses `UringFileIO`; all `FileIO` tests pass; the fallback feature still builds `TokioFileIO`; no per-operation `tokio::task::spawn_blocking` remains in the default data path.

- [x] 11. Add linked-chain metadata tests before uring sidecar implementation
  **What**: Add a fake ring/submitter around the metadata commit engine that records SQE order, linked flags, paths/fds, and completion results. Tests must assert the hot commit emits write, fdatasync, rename, and directory fsync in order, with link flags on the first three entries. Inject an error completion for the fdatasync operation and assert the fake reports the rename and directory fsync as cancelled/not performed, `meta.json` remains at the old value, and the error propagates.
  **Files**: `src/metadata.rs`, `src/metadata/uring.rs`, optional `src/metadata/testing.rs`
  **Acceptance**: `cargo test io_uring_linked_chain` proves linked ordering and cancellation without relying on real disk failures.

- [x] 12. Implement the Linux `io_uring` sidecar metadata store
  **What**: Implement `UringSidecarMetadataStore` using the same driver or a metadata-specific ring wrapper. Open/create `meta.json.tmp` and the parent directory fd, then submit the four-operation linked chain: write the full JSON bytes to tmp at offset 0, fdatasync tmp, rename tmp to final, fsync the parent directory. Keep `Bytes`/`Vec<u8>` and path CStrings alive until all CQEs arrive. Treat any negative CQE or linked cancellation as failure, clean up tmp best-effort, and never report success unless all four operations completed. Reads and startup `list` may use sync filesystem calls if they are not on the hot path, but writes must use the linked chain on Linux default.
  **Files**: `src/metadata/uring.rs`, `src/metadata.rs`, `src/main.rs`
  **Acceptance**: Sidecar write tests pass for both `SyncSidecarMetadataStore` and `UringSidecarMetadataStore`; metadata commit logs/counting tests show the linked four-operation chain on Linux default.

- [x] 13. Integrate default backend selection across main, HTTP tests, and benchmark tests
  **What**: Add exported aliases such as `DefaultFileIO` and `DefaultMetadataStore`, then update `main`, `AppState`, `router`, test server helpers, and benchmark smoke tests to be generic over both `FileIO` and `MetadataStore`. Keep `TokioFileIO` public for explicit fallback tests and benchmarks. Ensure `Config::default()` and config parsing do not gain hidden backend fields unless explicitly needed; prefer compile-time feature selection for fallback.
  **Files**: `src/io/mod.rs`, `src/metadata.rs`, `src/http/mod.rs`, `src/main.rs`, `tests/integration.rs`, `tests/standalone_benchmark.rs`, `src/config.rs` only if comments need clarification
  **Acceptance**: `cargo test` uses uring defaults on Linux; `cargo test --features tokio-fileio-fallback` uses `TokioFileIO`; test helpers can instantiate `SpoolManager<TokioFileIO, SyncSidecarMetadataStore>` and `SpoolManager<UringFileIO, UringSidecarMetadataStore>` where appropriate.

- [x] 14. Remove redb runtime usage and update error/tests/docs references
  **What**: Delete `SPOOL_TABLE`, remove `db` fields and redb helpers, replace redb-specific errors with store/metadata errors, and update tests that inspect persisted metadata to read sidecar JSON. Remove `redb = "2"` from `Cargo.toml` only after the migration module no longer uses the crate. Update comments and test names that mention redb offsets to sidecar metadata. Ensure no forbidden sequencing labels are introduced while renaming tests or comments.
  **Files**: `Cargo.toml`, `Cargo.lock`, `src/error.rs`, `src/manager.rs`, `src/spool/mod.rs`, `src/spool/lifecycle.rs`, `src/spool/writer.rs`, `src/cleanup.rs`, `tests/integration.rs`, `tests/standalone_benchmark.rs`
  **Acceptance**: `rg "redb|SPOOL_TABLE|Database::|spools.redb|bobs.redb" src tests Cargo.toml` returns only legacy migration references and migration tests; `cargo tree -i redb` returns no dependency path; all non-migration code uses `MetadataStore`.

- [x] 15. Add integration coverage for public API, cleanup safety, and performance smoke
  **What**: Add or update HTTP integration tests for create/write/complete/read/delete unchanged shapes, ack-then-restart with sidecars, recovery of full and trailing partial pages, write-locked/readable metadata persistence, cleanup slow-reader safety, and sidecar deletion. Add an ignored Linux smoke benchmark test that starts one server with `UringFileIO/UringSidecarMetadataStore` and one with `TokioFileIO/SyncSidecarMetadataStore` at 16 MiB pages, runs the existing in-process benchmark shape, and asserts uring throughput is not meaningfully below the fallback baseline while allowing a small noise band documented in the test.
  **Files**: `tests/integration.rs`, `tests/standalone_benchmark.rs`, `src/http/mod.rs` tests
  **Acceptance**: `cargo test test_http_restart_after_acknowledged`, `cargo test test_slow_reader_receiving_bytes_not_cleaned_up`, `cargo test standalone_benchmark_smoke`, and `cargo test io_uring_16m_page_smoke_not_slower_than_tokio_fileio -- --ignored` pass on a suitable Linux host.

- [x] 16. Update docs and deployment notes
  **What**: Rewrite architecture/key-behaviour docs to describe `<data_dir>/<key>/spool.dat` plus `<data_dir>/<key>/meta.json`, the sidecar atomic commit protocol, in-progress recovery from `spool.dat`, completed durability via data sync before final metadata, cleanup TTL preservation, global FIFO page cache unchanged, and shared-FS multi-BOBS benefits from one-writer-per-spool sidecars. Update configuration docs to state defaults are unchanged and backend fallback is build-feature based. Update benchmark docs with uring vs fallback commands. Check Docker/Kubernetes runtime notes for `io_uring_setup` seccomp restrictions; update Dockerfile or add chart-team notes only if runtime validation shows default container policy blocks io_uring.
  **Files**: `docs/src/architecture.md`, `docs/src/configuration.md`, `docs/src/key-behaviours.md`, `docs/src/standalone-benchmark.md`, `Dockerfile` only if validation requires it, chart notes if maintained in repo
  **Acceptance**: `rg "redb" docs/src` returns only legacy migration notes; docs explain Linux 5.11+ requirement and fallback build; container validation records whether seccomp/sysctl changes are required.

- [x] 17. Run benchmark sweep and write comparison report
  **What**: Build both fallback and default binaries, run the prior A/B/C/D workload shape, run rust-gdb stack sampling for the 512-object 16 MiB-page case, and compare against the Tokio fallback baseline. Capture wall MiB/s, write-active MiB/s, read-active MiB/s, CPU seconds, user/sys split, syscall counts (`syscr/syscw` and read/write bytes), max RSS, per-stage p50/p95 timings from summary JSON, and stack histograms. Confirm `redb::Database::begin_write` is absent from gdb samples and identify the new top stack entries.
  **Files**: no source files; benchmark artifacts under `/tmp` or a committed report location if the project has one; `docs/src/standalone-benchmark.md` for any command corrections
  **Acceptance**: Commands complete with zero benchmark failures and produce a concise report:
  - `cargo build --release --bin bobs --bin bobs-benchmark --features tokio-fileio-fallback`
  - `cp target/release/bobs /tmp/bobs-tokio-fileio-baseline`
  - `cargo build --release --bin bobs --bin bobs-benchmark`
  - `python3 /tmp/bobs_runs_abcd.py | tee /tmp/bobs-sidecar-uring-abcd.log`
  - `python3 /tmp/bobs_run_d_gdb.py | tee /tmp/bobs-sidecar-uring-rund-gdb.log`
  - Explicit single-run commands, when not using the helper script, use `bobs-benchmark --base-url http://127.0.0.1:$PORT --objects 64 --object-bytes 16777216 --write-body-chunk-bytes $PAGE --read-body-chunk-bytes $PAGE --summary-json /tmp/bobs-summary-A-$PAGE.json` for `$PAGE` in `4096 1048576 4194304 16777216` with `max_cache_bytes = 32 * page_size` in the server config.
  - Run B uses `--objects 64 --object-bytes 16777216 --write-body-chunk-bytes 4194304 --read-body-chunk-bytes 4194304` with `page_size = 4194304` and `max_cache_bytes = 4194304`.
  - Run C uses `--objects 512 --object-bytes 16777216 --write-body-chunk-bytes 4194304 --read-body-chunk-bytes 4194304 --request-timeout-ms 900000` with `page_size = 4194304` and `max_cache_bytes = 268435456`.
  - Run D uses `--objects 512 --object-bytes 16777216 --write-body-chunk-bytes 16777216 --read-body-chunk-bytes 16777216 --request-timeout-ms 900000` with `page_size = 16777216`, `max_cache_bytes = 536870912`, plus rust-gdb sampling via `/tmp/bobs_run_d_gdb.py`.

- [x] 18. Final verification and release readiness check
  **What**: Run formatting, full tests, targeted tests, dependency checks, doc grep, and benchmark smoke. Review `git diff` to ensure no HTTP API changes, no config default changes, no extra dependencies, no redb runtime usage, no forbidden sequencing labels, and no weakened `/complete` ordering.
  **Files**: whole repository review
  **Acceptance**: All verification commands pass and the implementation summary includes migration results, fallback build status, benchmark artifact paths, and any Docker/chart runtime notes.

## Verification
- [ ] `cargo fmt --all -- --check`
- [ ] `cargo test`
- [ ] `cargo test --features tokio-fileio-fallback`
- [ ] `cargo test metadata_store_atomic_rename`
- [ ] `cargo test sidecar_ignores_tmp_on_recovery`
- [ ] `cargo test migration_from_redb`
- [ ] `cargo test recovery`
- [ ] `cargo test cleanup`
- [ ] `cargo test complete_does_one_data_sync_and_one_metadata_commit`
- [ ] `cargo test io_uring_fileio`
- [ ] `cargo test io_uring_linked_chain`
- [ ] `cargo test test_http_restart_after_acknowledged`
- [ ] `cargo test test_slow_reader_receiving_bytes_not_cleaned_up`
- [ ] `cargo test standalone_benchmark_smoke`
- [ ] `cargo test io_uring_16m_page_smoke_not_slower_than_tokio_fileio -- --ignored`
- [ ] `cargo tree -i redb` returns no dependency path in the final default build.
- [ ] `rg "redb|SPOOL_TABLE|Database::|spools.redb|bobs.redb" src tests Cargo.toml` returns only legacy migration module/tests where expected.
- [ ] `rg "redb" docs/src` returns only legacy migration notes.
- [ ] `rg "P[h]ase|S[t]ep|R(ou|oun)d" src tests docs Cargo.toml .weave/plans/bobs-sidecar-metadata-and-io-uring.md` returns no unintended matches.
- [ ] `cargo build --release --bin bobs --bin bobs-benchmark --features tokio-fileio-fallback`
- [ ] `cp target/release/bobs /tmp/bobs-tokio-fileio-baseline`
- [ ] `cargo build --release --bin bobs --bin bobs-benchmark`
- [ ] `python3 /tmp/bobs_runs_abcd.py | tee /tmp/bobs-sidecar-uring-abcd.log`
- [ ] `python3 /tmp/bobs_run_d_gdb.py | tee /tmp/bobs-sidecar-uring-rund-gdb.log`
- [ ] Compare benchmark summaries for wall/write-active/read-active MiB/s, CPU seconds, syscall counts, RSS, per-stage p50/p95, and gdb stack histograms.
- [ ] Confirm `redb::Database::begin_write` and `persist_metadata` redb frames are absent from Run D stack samples.
- [ ] Confirm the new top stack entries are documented with artifact paths.

## Migration Acceptance Criteria
- [ ] Startup detects legacy `<data_dir>/spools.redb`, `<data_dir>/bobs.redb`, or the configured equivalent before sidecar recovery.
- [ ] A legacy DB with N valid spool rows produces exactly N `<data_dir>/<key>/meta.json` files.
- [ ] Each migrated `meta.json` is byte-equal to the legacy serialized `SpoolMetadata` row.
- [ ] Migration is idempotent after success, after partial sidecar creation, and with leftover `meta.json.tmp` files.
- [ ] The legacy DB is removed only after all sidecars are durably committed and `data_dir` has been fsynced.
- [ ] A corrupt legacy row does not produce a bogus sidecar and is reported with enough context for operator action.
- [ ] Final default build does not depend on the `redb` crate.

## io_uring Fault-Injection Acceptance Criteria
- [ ] Fake data-path submitter can return short positive write completions; `UringFileIO::write_at` retries the remaining slice and resolves to the full input length.
- [ ] Fake data-path submitter can return an fsync error; `UringFileIO::sync_data` propagates the error without masking it.
- [ ] Fake linked-chain submitter records write, fdatasync, rename, and directory fsync in order with link flags on the first three SQEs.
- [ ] If fdatasync completion is an error, rename and directory fsync are cancelled/not performed and final metadata remains the old complete JSON.
- [ ] If rename succeeds but directory fsync fails, the write reports an error, recovery still observes only valid old or valid new JSON, never torn bytes.
- [ ] The real Linux uring metadata store passes the same high-level sidecar atomicity tests as the synchronous store.
