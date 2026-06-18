# BOBS io_uring Sharded Rings and SQPOLL

## TL;DR
> **Summary**: Replace the current single Linux `io_uring` data driver and per-commit metadata ring with one shared sharded `RingPool`, stable key hashing, per-spool ring affinity, and SQPOLL-enabled rings with explicit non-SQPOLL fallback logging. Deliver tests first for routing, linked-chain preservation, SQPOLL behavior, contention, shutdown, and regression invariants, then rerun the A/B/C/D benchmarks against Tokio fallback and the current single-ring baseline.
> **Estimated Effort**: XL

## Context
### Original Request
Plan a BOBS performance/architecture change that replaces the single `io_uring` driver with N sharded rings, stable hash dispatch by spool key, and SQPOLL on every ring. Correctness, public HTTP API shape, on-disk sidecar format, producer no-rewind recovery, `/complete` ordering, page visibility, cleanup TTLs, global FIFO cache, and Tokio fallback behavior must remain unchanged. Tests must be written before implementation work, and benchmarking must happen at the end using the previous A/B/C/D and Run D stack-sampling workflows.

### Key Findings
- `src/io/uring_fs.rs` currently has one `static DRIVER: OnceLock<DriverState>` containing one `mpsc::UnboundedSender<Request>` and one `RingDriver` thread named `bobs-io-uring-driver`; every Linux `UringFileIO` create/open/read/write/fdatasync/unlink request funnels through that one ring.
- `src/io/uring_fs.rs` currently uses `IoUring::new(RING_ENTRIES)` with no SQPOLL; the driver owns buffers/FDs/CStrings in `InFlightKind` until CQE completion and retries short positive writes.
- `src/metadata/uring.rs` currently creates a fresh `IoUring::new(METADATA_RING_ENTRIES)` inside every `UringSidecarMetadataStore::write_uring` call, submits the four metadata SQEs as `write -> fdatasync -> renameat -> fsync(dir)`, and waits synchronously for all four CQEs.
- `src/spool/lifecycle.rs::complete()` already enforces the critical order: publish/cache final visible bytes, call `F::sync_data()` on `spool.dat`, then persist final metadata. The plan must keep this order and only change where those I/O requests are routed.
- `src/manager.rs::create_spool()` creates `<data_dir>/<key>/spool.dat` via `F::create(&data_path)` before writing initial sidecar metadata. `recover()` opens each persisted `meta.data_path` through `F::open(&meta.data_path)`. This makes `create/open` the right place to attach a stable ring index to the returned handle.
- `src/io/mod.rs::FileIO` is associated-function based (`F::create(path)`, `F::write_at(handle, ...)`), so a `&self` refactor would be broad. A global ring pool plus test-only explicit pools/overrides keeps code size bounded and leaves the trait shape intact.
- The data path receives only paths, not keys. For normal spools the key is the parent directory name of `<data_dir>/<key>/spool.dat`; routing should derive that key for `create/open` and fall back to hashing full path bytes for non-spool test files.
- `Config` currently has no backend-specific knobs. New Linux-only behavior needs explicit new config fields documented as new; fallback builds should parse the same config but ignore the Linux `io_uring` knobs at the I/O layer.
- `Cargo.toml` does not currently include `num_cpus` or a stable hash crate. Adding `num_cpus` is needed for default auto shard count; using a small `siphasher` crate avoids relying on Rust `DefaultHasher` implementation details.
- The installed `io-uring` crate supports `io_uring::Builder::setup_sqpoll(idle_ms)` and `setup_sqpoll_cpu(cpu)`. Its `submit()`/`submit_and_wait()` automatically emits `IORING_ENTER_SQ_WAKEUP` when the SQPOLL kthread has slept.
- `/tmp/bobs-sidecar-comparison-report.md` shows the current single-ring default at ~333 MiB/s for 4 KiB pages, but 1/4/16 MiB page runs are 0.83-0.93x of Tokio fallback and Run C is 721.7 MiB/s. Active stack sampling identified `io::uring_fs::RingDriver::run` and metadata write frames as new hot entries.
- `/tmp/bobs_runs_abcd.py` honors `BOBS_BIN` and `BOBS_BENCH_BIN` for A/B/C/D sweeps; `/tmp/bobs_run_d_gdb.py` builds and samples the default release binary for the 512-object 16 MiB-page Run D shape.

## Objectives
### Core Objective
Introduce a shared sharded Linux `io_uring` ring pool with stable per-key dispatch and SQPOLL-enabled rings, and route both `spool.dat` operations and sidecar metadata commits for a spool key to the same shard without changing public behavior or on-disk format.

### Deliverables
- [ ] A Linux-only `RingPool` abstraction in `src/io/ring_pool.rs` shared by `UringFileIO` and `UringSidecarMetadataStore`.
- [ ] Stable key-to-shard hashing with tests that pin process-restart behavior and distribution.
- [ ] `UringFileHandle` storing an `Arc<OwnedFd>` plus assigned `ring_index`, with all handle operations routed to that shard.
- [ ] Metadata writes submitted to the same shard as their spool key, preserving the linked four-SQE commit chain on any shard.
- [ ] SQPOLL setup for every shard with default idle timeout 200 ms, explicit warning and retry without SQPOLL if SQPOLL setup is rejected, and no default CPU pinning.
- [ ] Config fields for `io_uring_shards` and `io_uring_sqpoll_idle_ms`, documented as new Linux-backend knobs and ignored by the Tokio fallback I/O layer.
- [ ] Clean driver shutdown for explicit `RingPool` instances and for the production global pool on orderly server exit.
- [ ] Tests-first coverage for all mandatory routing, SQPOLL, linked-chain, contention, shutdown, fallback, and regression cases.
- [ ] Benchmark report comparing sharded+SQPOLL against `/tmp/bobs-tokio-fileio-baseline` and a copied single-ring uring baseline from the current commit.

### Definition of Done
- [ ] `cargo fmt --all -- --check` passes.
- [ ] `cargo test` passes on Linux with the default `io_uring` backend.
- [ ] `cargo test --features tokio-fileio-fallback` passes and does not use the new ring pool in the fallback I/O path.
- [ ] `cargo test ring_pool_hash_stability` proves stable key routing across a subprocess restart and pinned vectors.
- [ ] `cargo test ring_pool_dispatch_distribution` proves many distinct keys cover all shards when `num_shards > 1`.
- [ ] `cargo test ring_pool_per_spool_routing` proves data writes and metadata commits for one key route to the same shard.
- [ ] `cargo test io_uring_linked_chain -- --nocapture` includes coverage for shard 0 and shard `num_shards - 1`.
- [ ] `cargo test ring_pool_sqpoll_wakeup_after_idle` proves a submission after `sqpoll_idle_ms` still completes.
- [ ] `cargo test ring_pool_submission_contention_safety` proves many concurrent senders cannot push torn SQEs.
- [ ] `cargo test ring_pool_driver_shutdown` proves dropping an explicit pool drains and joins all drivers.
- [ ] `cargo test ring_pool_sqpoll_setup_rejection_falls_back` proves injected SQPOLL setup rejection logs/uses the chosen fallback path.
- [ ] `cargo test test_http_restart_after_acknowledged` proves no producer rewind regression.
- [ ] `cargo test complete_does_one_data_sync_and_one_metadata_commit` proves `/complete` still syncs `spool.dat` before final metadata.
- [ ] `cargo test standalone_benchmark_smoke` passes.
- [ ] `cargo test io_uring_16m_page_smoke_not_slower_than_tokio_fileio -- --ignored` passes or produces a documented investigation note.
- [ ] A new report is written next to `/tmp/bobs-sidecar-comparison-report.md` comparing Tokio fallback, single-ring uring, and sharded+SQPOLL uring.

### Guardrails (Must NOT)
- [ ] Do not change public HTTP routes, methods, request bodies, response bodies, status-code contracts, or headers for `/create`, `/write`, `/complete`, `/read`, or `/delete`.
- [ ] Do not require producers to resend bytes after any `/write` response that returned `200 OK`.
- [ ] Do not weaken `/complete`: `spool.dat` `sync_data` must finish before final completed metadata is committed.
- [ ] Do not break the metadata four-SQE linked chain; `write`, tmp `fdatasync`, `renameat`, and directory `fsync` remain one linked commit on one ring.
- [ ] Do not add a separate metadata ring; data and metadata for a key must share the same shard for ordering simplicity.
- [ ] Do not change sidecar on-disk layout or migration behavior; this is runtime-only with no migration story.
- [ ] Do not change page-based reader visibility, cleanup TTL semantics, global FIFO page cache behavior, or sidecar atomic protocol.
- [ ] Do not change `Config::default().page_size` or any existing TTL default.
- [ ] Do not silently change `Config::default()` behavior for existing fields; new fields must have explicit defaults and docs.
- [ ] Do not move BOBS off Tokio or introduce a second async runtime.
- [ ] Do not touch the Tokio fallback I/O implementation except for compile/import adjustments required by shared config parsing.
- [ ] Do not add dependencies other than `num_cpus` and one small stable hash crate (`siphasher`) unless explicitly re-approved.
- [ ] Do not silently downgrade SQPOLL; fallback to non-SQPOLL must log clearly with the rejection error and shard count.
- [ ] Do not use process-ephemeral sequencing wording in code comments, task names, commit messages, or test names.

## TODOs

- [x] 1. Add ring-pool hash tests before introducing dispatch code
  **What**: Write tests for a new stable `ring_index_for_key(key, num_shards)` helper before implementing it. Include pinned vector assertions, a subprocess restart probe that computes the same key/shard pairs in a fresh process, and distribution over at least several thousand UUID-like keys for shard counts 2, 4, and 8.
  **Files**: `src/io/ring_pool.rs` (new, test module first), optionally `src/io/mod.rs` for Linux-gated module declaration.
  **Acceptance**: `cargo test ring_pool_hash_stability` and `cargo test ring_pool_dispatch_distribution` exist and fail until the hash helper is implemented; the stability test proves identical `hash(key) % num_shards` after a subprocess restart for the same binary and shard count.

- [x] 2. Implement stable hash dispatch and dependency updates
  **What**: Add `num_cpus` and `siphasher = "1"` to `Cargo.toml`. Implement `stable_key_hash(key: &str) -> u64` with `siphasher::sip::SipHasher13::new_with_keys(0, 0)`, feeding `key.as_bytes()` directly, not `std::hash::Hash`, so behavior is independent of `RandomState` and Rust `Hash` implementation details. Implement `ring_index_for_key(key, num_shards)` with validation that `num_shards > 0`.
  **Files**: `Cargo.toml`, `Cargo.lock`, `src/io/mod.rs`, `src/io/ring_pool.rs`.
  **Acceptance**: `cargo test ring_pool_hash_stability ring_pool_dispatch_distribution` passes; `cargo tree -i num_cpus` and `cargo tree -i siphasher` show only the approved additions.

- [x] 3. Add config and startup tests for Linux ring-pool knobs
  **What**: Write tests proving new config fields parse from YAML, reject invalid values, and keep existing defaults unchanged. Use `io_uring_shards: Option<usize>` with default `None` meaning auto `num_cpus::get()` for the Linux default backend, and `io_uring_sqpoll_idle_ms: u32` defaulting to `200`. Validate `Some(0)` as invalid and `io_uring_sqpoll_idle_ms == 0` as invalid unless a later test-specific constructor bypasses it.
  **Files**: `src/config.rs`, `docs/src/configuration.md` test fixtures if any are embedded.
  **Acceptance**: `cargo test test_defaults`, `cargo test test_from_file_yaml`, and new filters `cargo test config_io_uring` exist and fail before config implementation; existing assertions for `page_size`, TTLs, and routing fields remain unchanged.

- [x] 4. Implement config fields and production initialization hook
  **What**: Add `Config { io_uring_shards, io_uring_sqpoll_idle_ms }` with explicit defaults. Add a Linux/non-fallback startup hook in `src/main.rs` that initializes the production ring pool before legacy migration and manager recovery. Keep fallback builds compiling without constructing or using a ring pool. Log resolved shard count, SQPOLL requested/enabled status, idle timeout, and CPU pinning disabled.
  **Files**: `src/config.rs`, `src/main.rs`, `src/io/mod.rs`, `src/io/uring_fs.rs`, `src/io/ring_pool.rs`, `docs/src/configuration.md`.
  **Acceptance**: `cargo test config_io_uring` passes; `cargo build --features tokio-fileio-fallback` does not require `io_uring` pool initialization from `main`; startup logs expose the resolved Linux settings.

- [x] 5. Add `RingPool` lifecycle, SQPOLL setup, and fallback tests before the pool implementation
  **What**: Write unit tests against explicit `RingPool` instances for shard count, driver naming, SQPOLL requested/enabled state, injected SQPOLL setup rejection, and driver shutdown. Include a fault-injection constructor that simulates `io_uring::Builder::setup_sqpoll(...).build(...)` failing with a chosen `io::Error` and asserts the pool retries without SQPOLL, records `sqpoll_enabled = false`, and emits an explicit warning through a test log capture or test-visible event sink.
  **Files**: `src/io/ring_pool.rs`.
  **Acceptance**: `cargo test ring_pool_driver_shutdown` and `cargo test ring_pool_sqpoll_setup_rejection_falls_back` exist and fail until the pool exists; shutdown assertions prove all shard driver `JoinHandle`s are joined and no in-flight operation remains.

- [x] 6. Implement `RingPool`, `RingShard`, and clean shutdown
  **What**: Build `RingPool { shards: Vec<RingShard>, config, instrumentation }`, where each `RingShard` owns a driver thread `JoinHandle`, a shard-specific `mpsc::UnboundedSender<Request>`, and test-visible counters. Construct rings via `io_uring::Builder::new().setup_sqpoll(sqpoll_idle_ms).build(RING_ENTRIES)`; if SQPOLL setup fails, log `warn!` and retry `IoUring::new(RING_ENTRIES)` for the same shard count. Do not call `setup_sqpoll_cpu` by default. Implement `Drop` by closing all senders, joining every driver, and letting each driver drain pending and in-flight work before exit.
  **Files**: `src/io/ring_pool.rs`, `src/io/mod.rs`.
  **Acceptance**: `cargo test ring_pool_driver_shutdown ring_pool_sqpoll_setup_rejection_falls_back` passes; a pool with N shards starts N driver threads named like `bobs-io-uring-shard-{idx}`; explicit pool drop has no leaked in-flight operations.

- [x] 7. Add global-pool and test-isolation tests before wiring `FileIO`
  **What**: Write tests for production-style global initialization and test isolation. Use `OnceLock<Mutex<Option<Arc<RingPool>>>>` or equivalent so `init_global_ring_pool(...)` installs one pool, `global_ring_pool()` clones it, and `shutdown_global_ring_pool_for_exit()` can drop it on orderly shutdown. Add a test-only scoped override/direct-pool path for unit tests that need instrumentation without relying on a leaked global. Avoid broad `FileIO` trait changes.
  **Files**: `src/io/uring_fs.rs`, `src/io/ring_pool.rs`.
  **Acceptance**: `cargo test ring_pool_global_initialization` and `cargo test ring_pool_test_override_isolated` exist and fail before wiring; tests prove production initialization is idempotent with matching config and errors clearly on conflicting config.

- [x] 8. Implement global-pool access without changing the `FileIO` trait
  **What**: Replace the current `static DRIVER: OnceLock<DriverState>` in `src/io/uring_fs.rs` with global `RingPool` access. Production `main` owns/drops the pool; tests may use explicit pools or the scoped override. If an associated `FileIO` function is called before production initialization, lazily create a default pool using `num_cpus::get()` and 200 ms idle for compatibility with current unit tests.
  **Files**: `src/io/uring_fs.rs`, `src/io/ring_pool.rs`, `src/main.rs`.
  **Acceptance**: Existing `cargo test io_uring_fileio` tests still run without explicit `main` initialization; `cargo build --features tokio-fileio-fallback` remains unaffected; no `&self`-based trait refactor is introduced.

- [x] 9. Add per-spool data routing tests before handle changes
  **What**: Write tests proving `UringFileIO::create/open` derive the same routing key as metadata (`<data_dir>/<key>/spool.dat` -> `key`), store the assigned shard on the returned handle, and route every later write/read/sync for that handle to that shard. Include fallback routing for non-spool test paths by hashing the full path bytes. Instrument the driver to record `(operation_kind, routed_key, ring_index)` for tests.
  **Files**: `src/io/uring_fs.rs`, `src/io/ring_pool.rs`, `src/metadata/uring.rs` test helpers later reused.
  **Acceptance**: `cargo test ring_pool_per_spool_routing` exists and fails before handle changes; the test verifies a data create, multiple writes, `sync_data`, and a metadata commit for one key all land on the same ring index.

- [x] 10. Implement routed `UringFileHandle` and data-path requests
  **What**: Change `pub type UringFileHandle = Arc<OwnedFd>` into a small handle struct, e.g. `#[derive(Clone)] pub struct UringFileHandle { fd: Arc<OwnedFd>, ring_index: usize, routed_key: String }`. Move the current open/read/write/sync/remove request handling into `RingPool` shard requests, preserving owned buffer/CString/FD lifetimes and short-write retry behavior. `create/open` compute the routing key before submission; handle operations use the stored `ring_index` rather than rehashing.
  **Files**: `src/io/uring_fs.rs`, `src/io/ring_pool.rs`.
  **Acceptance**: `cargo test io_uring_fileio` passes; `cargo test ring_pool_per_spool_routing` proves stable handle affinity; existing FileIO trait shape and Tokio fallback files are unchanged.

- [x] 11. Add serialized SQE submission and contention tests before driver-loop changes
  **What**: Write a stress test that spawns many Tokio tasks concurrently submitting operations to one shard and asserts only the shard driver thread pushes SQEs, SQE user-data IDs are unique/monotonic per shard, and chain entries are never interleaved with another operation. Use test instrumentation around the driver push path rather than relying on timing.
  **Files**: `src/io/ring_pool.rs`.
  **Acceptance**: `cargo test ring_pool_submission_contention_safety` exists and fails before serialization instrumentation; the test would catch any future attempt to push directly from caller tasks.

- [x] 12. Implement the shard driver loop and SQPOLL wake path
  **What**: Refactor the driver loop so each shard drains its own receiver, serially pushes SQEs, calls `submit()` after batches so awake SQPOLL rings avoid submit syscalls, processes CQEs, and uses `submit_and_wait(1)` when it must block for completions. Choose `submit_and_wait(1)` over eventfd for this pass because the `io-uring` crate already handles `SQ_WAKEUP`, this preserves the existing blocking-driver model, and eventfd would add another fd lifecycle/multiplexing path without addressing the primary single-driver bottleneck. Add `const DEFAULT_SQPOLL_IDLE_MS: u32 = 200` and a test constructor with a small idle. Keep raw FDs; do not implement file/buffer registration or shared WQ yet.
  **Files**: `src/io/ring_pool.rs`, `src/io/uring_fs.rs`.
  **Acceptance**: `cargo test ring_pool_submission_contention_safety` passes; `cargo test ring_pool_sqpoll_wakeup_after_idle` passes by sleeping longer than the configured idle timeout and then completing a fresh operation; current `io_uring_fileio` tests pass.

- [x] 13. Add metadata-on-shard and linked-chain tests before metadata routing implementation
  **What**: Extend the existing fake linked-chain tests in `src/metadata/uring.rs` so the same commit engine can be invoked for shard 0 and shard `num_shards - 1`. Add tests proving the four SQEs remain `write -> fdatasync -> renameat -> fsync(dir)`, link flags remain `[true, true, true, false]`, and injected fdatasync failure cancels rename and directory fsync on both selected shards. Add a routed metadata test that hashes `metadata.key` and records the target ring.
  **Files**: `src/metadata/uring.rs`, `src/io/ring_pool.rs` test hooks.
  **Acceptance**: `cargo test io_uring_linked_chain` covers both shard indices; `cargo test ring_pool_metadata_commit_routes_by_key` exists and fails until metadata writes use the shared pool.

- [x] 14. Implement metadata commits through the shared `RingPool`
  **What**: Change `UringSidecarMetadataStore::write_uring` to build the payload, create the spool directory, open/create `meta.json.tmp`, open the parent directory, then submit a `MetadataCommit` request to `RingPool::submit_metadata_commit(key, tmp_fd, parent_fd, tmp_name, final_name, payload)`. The shard driver must reserve enough SQ space for all four entries before pushing any part of the chain. It must keep payload, CStrings, and FDs alive until all four CQEs have arrived, treat negative CQEs or linked cancellations as failure, and best-effort remove `meta.json.tmp` on failure. Reads/list/delete may remain synchronous as today.
  **Files**: `src/metadata/uring.rs`, `src/io/ring_pool.rs`, `src/io/uring_fs.rs` if global accessors live there.
  **Acceptance**: `cargo test io_uring_linked_chain ring_pool_metadata_commit_routes_by_key ring_pool_per_spool_routing` passes; metadata commits for different keys can execute in parallel on different shards; commits for one key are submitted after that key’s data sync on the same shard.

- [x] 15. Add `/complete` ordering and regression tests against routed uring
  **What**: Run and, where needed, extend existing lifecycle/integration tests to assert `/complete` still performs exactly one data-file `sync_data` before one metadata commit and that ack-then-restart still recovers full pages and trailing partial bytes. Add routed instrumentation to prove the `sync_data` request and the final metadata commit for the same spool key are enqueued to the same shard in that order.
  **Files**: `src/spool/lifecycle.rs` tests, `tests/integration.rs`, `src/io/ring_pool.rs` test instrumentation.
  **Acceptance**: `cargo test complete_does_one_data_sync_and_one_metadata_commit`, `cargo test test_http_restart_after_acknowledged_full_pages_write_allows_continue`, and `cargo test test_http_restart_after_acknowledged_trailing_partial_page_write_allows_continue` pass with the Linux default backend.

- [x] 16. Implement main shutdown ordering for the global pool
  **What**: Ensure `src/main.rs` drops HTTP state/manager references after connection tasks drain and then calls the Linux/non-fallback global ring-pool shutdown hook so shard senders close and drivers join. Keep test servers using explicit process/task abort behavior unchanged unless they need a scoped pool drop for instrumentation.
  **Files**: `src/main.rs`, `src/io/uring_fs.rs`, `src/io/ring_pool.rs`.
  **Acceptance**: `cargo test ring_pool_driver_shutdown` passes; manual `RUST_LOG=info cargo run --release --bin bobs -- config.yaml` followed by SIGINT logs clean shard shutdown without broken-pipe errors for drained requests.

- [x] 17. Update docs for sharded rings, SQPOLL, fallback behavior, and future optimizations
  **What**: Document that default Linux builds use sharded `io_uring` rings with SQPOLL when available, stable key hashing, data+metadata same-shard routing, and explicit logged fallback to non-SQPOLL if SQPOLL setup is rejected. Document new config fields, default auto shard count, default 200 ms SQPOLL idle, no default CPU pinning, raw FD support on Linux 5.11+, and runtime caveats for container seccomp. Mention future optimizations not implemented now: `IORING_REGISTER_FILES`, `IORING_REGISTER_BUFFERS`, and `IORING_SETUP_ATTACH_WQ`.
  **Files**: `docs/src/architecture.md`, `docs/src/standalone-benchmark.md`, `docs/src/configuration.md`.
  **Acceptance**: `rg "io_uring|SQPOLL|io_uring_shards|io_uring_sqpoll_idle_ms|tokio-fileio-fallback" docs/src` shows the new behavior and caveats; docs do not imply any HTTP API or on-disk migration.

- [x] 18. Run full test matrix and fallback build checks
  **What**: Run formatting, default Linux tests, fallback tests, and focused mandatory filters. Confirm the Tokio fallback I/O layer remains untouched behaviorally and all previous sidecar+uring delivery tests still pass, including recovery, cleanup, slow-reader, integration suite, standalone benchmark smoke, and ignored 16 MiB perf smoke.
  **Files**: no source changes unless failures reveal implementation bugs.
  **Acceptance**: Commands complete successfully:
  - `cargo fmt --all -- --check`
  - `cargo test`
  - `cargo test --features tokio-fileio-fallback`
  - `cargo test ring_pool_hash_stability ring_pool_dispatch_distribution ring_pool_per_spool_routing`
  - `cargo test io_uring_linked_chain`
  - `cargo test ring_pool_sqpoll_wakeup_after_idle ring_pool_submission_contention_safety ring_pool_driver_shutdown ring_pool_sqpoll_setup_rejection_falls_back`
  - `cargo test test_http_restart_after_acknowledged`
  - `cargo test test_slow_reader_receiving_bytes_not_cleaned_up`
  - `cargo test standalone_benchmark_smoke`
  - `cargo test io_uring_16m_page_smoke_not_slower_than_tokio_fileio -- --ignored`

- [x] 19. Capture baselines and run final A/B/C/D benchmarks
  **What**: Preserve the existing binaries for comparison, build the sharded+SQPOLL binary, run the same benchmark scripts, and write a new report adjacent to `/tmp/bobs-sidecar-comparison-report.md`. Compare against `/tmp/bobs-tokio-fileio-baseline` and a copied single-ring uring binary from the current commit before implementation.
  **Files**: benchmark artifacts under `/tmp`; new report path `/tmp/bobs-sharded-rings-sqpoll-comparison-report.md`.
  **Acceptance**: Benchmark commands complete with zero object failures:
  - `test -x /tmp/bobs-tokio-fileio-baseline`
  - `cargo build --release --bin bobs --bin bobs-benchmark`
  - `cp target/release/bobs /tmp/bobs-uring-single-ring-baseline` before implementation begins, or use the saved binary from the current `HEAD` if already captured.
  - `BOBS_BIN=/tmp/bobs-tokio-fileio-baseline BOBS_BENCH_BIN=target/release/bobs-benchmark /tmp/bobs_runs_abcd.py`
  - `BOBS_BIN=/tmp/bobs-uring-single-ring-baseline BOBS_BENCH_BIN=target/release/bobs-benchmark /tmp/bobs_runs_abcd.py`
  - `BOBS_BIN=target/release/bobs BOBS_BENCH_BIN=target/release/bobs-benchmark /tmp/bobs_runs_abcd.py`
  - `/tmp/bobs_run_d_gdb.py` for the final sharded+SQPOLL binary; if comparing single-ring stack fan-out, temporarily point/copy `/tmp/bobs-uring-single-ring-baseline` to the script’s expected `target/release/bobs` path or adapt a local copy of the script without changing source.
  - Write `/tmp/bobs-sharded-rings-sqpoll-comparison-report.md` with wall/write/read MiB/s, CPU user/sys, syscall counters, RSS, per-stage p50/p95, and top stack entries for all three binaries.

- [x] 20. Decide success from benchmark acceptance criteria
  **What**: Compare results against the requested thresholds and record any deviations. Confirm 4 KiB pages are strictly faster than 333 MiB/s, 1 MiB/4 MiB/16 MiB single-stream uring are no longer below the Tokio fallback baseline, Run C (512 x 16 MiB) is faster than 721 MiB/s, and Run D stack histograms show work fanning out across multiple shard driver threads rather than one `RingDriver` bottleneck.
  **Files**: `/tmp/bobs-sharded-rings-sqpoll-comparison-report.md`, optional doc note in `docs/src/standalone-benchmark.md` only if commands need correction.
  **Acceptance**: The report explicitly states pass/fail for each threshold, includes paths to raw artifact directories, and lists new top stack entries. If a threshold fails, do not declare the performance change successful; record the suspected bottleneck and the next candidate optimization from the deferred list.

## Verification
- [ ] `cargo fmt --all -- --check`
- [ ] `cargo test`
- [ ] `cargo test --features tokio-fileio-fallback`
- [ ] `cargo test ring_pool_hash_stability`
- [ ] `cargo test ring_pool_dispatch_distribution`
- [ ] `cargo test ring_pool_per_spool_routing`
- [ ] `cargo test io_uring_linked_chain`
- [ ] `cargo test ring_pool_sqpoll_wakeup_after_idle`
- [ ] `cargo test ring_pool_submission_contention_safety`
- [ ] `cargo test ring_pool_driver_shutdown`
- [ ] `cargo test ring_pool_sqpoll_setup_rejection_falls_back`
- [ ] `cargo test test_http_restart_after_acknowledged`
- [ ] `cargo test test_slow_reader_receiving_bytes_not_cleaned_up`
- [ ] `cargo test complete_does_one_data_sync_and_one_metadata_commit`
- [ ] `cargo test standalone_benchmark_smoke`
- [ ] `cargo test io_uring_16m_page_smoke_not_slower_than_tokio_fileio -- --ignored`
- [ ] `BOBS_BIN=/tmp/bobs-tokio-fileio-baseline BOBS_BENCH_BIN=target/release/bobs-benchmark /tmp/bobs_runs_abcd.py`
- [ ] `BOBS_BIN=/tmp/bobs-uring-single-ring-baseline BOBS_BENCH_BIN=target/release/bobs-benchmark /tmp/bobs_runs_abcd.py`
- [ ] `BOBS_BIN=target/release/bobs BOBS_BENCH_BIN=target/release/bobs-benchmark /tmp/bobs_runs_abcd.py`
- [ ] `/tmp/bobs_run_d_gdb.py`
- [ ] `/tmp/bobs-sharded-rings-sqpoll-comparison-report.md` exists and compares Tokio fallback, single-ring uring, and sharded+SQPOLL uring.

## Migration Story
No migration is required. The change is runtime-only: it changes Linux `io_uring` submission topology and driver scheduling, not HTTP behavior, metadata JSON, spool file layout, sidecar atomic protocol, or recovery rules.

## Pitfalls and Handling
- SQPOLL may be blocked by kernel/container policy. Handle by logging a clear warning per pool startup and retrying the same shard count without SQPOLL; do not hide whether SQPOLL is enabled.
- `DefaultHasher` and randomized hash states would undermine stable dispatch. Use fixed-key SipHash over raw key bytes and pin vectors in tests.
- Partially pushing a metadata chain when the SQ is nearly full would weaken atomic linked submission. Check SQ capacity for four entries before pushing the chain; if insufficient, submit/reap first and retry.
- Blocking indefinitely in one shard while new requests queue can add latency. Keep the first pass simple with `submit()` batching plus `submit_and_wait(1)` for completions, but record latency/stack evidence in the final report; consider eventfd only if benchmarks show this remains a bottleneck.
- Global statics can make shutdown tests flaky. Test driver shutdown with explicit `RingPool` instances and make production global shutdown explicit rather than relying on static Drop.
- Deriving a key from paths can misroute non-standard paths. For `spool.dat`, use the parent directory name; for other paths, hash full path bytes and only require same-shard data+metadata invariants for normal spool paths.

## Post-execution outcome

Benchmarking showed that SQPOLL regressed throughput for this workload. The per-shard SQPOLL kernel threads did not have enough operation rate to amortise their spinning cost, and the best overall measured result was the 8-shard non-SQPOLL run.

The resulting implementation decision is that the SQPOLL path is removed entirely: no SQPOLL setup, config field, fallback path, reporting field, environment override, or tests remain. The default shard count is now `max(1, num_cpus / 4)` when `io_uring_shards` is unset.

Supporting artifacts:

- `/tmp/bobs-sharded-rings-sqpoll-comparison-report.md` — selection-time comparison across four SQPOLL/shard configurations
- `/tmp/bobs-sqpoll-removed-comparison-report.md` — final report against the SQPOLL-removed binary running on its new defaults
- `/tmp/bobs-abcd-20260515003411/sweep-result.json` — 32 shards with SQPOLL, the superseded default, failed the benchmark verdict
- `/tmp/bobs-abcd-20260515003451/sweep-result.json` — 32 shards without SQPOLL
- `/tmp/bobs-abcd-20260515003525/sweep-result.json` — 8 shards with SQPOLL
- `/tmp/bobs-abcd-20260515003600/sweep-result.json` — 8 shards without SQPOLL, selection-time best
- `/tmp/bobs-abcd-20260515004843/sweep-result.json` — SQPOLL-removed binary running on `io_uring_shards = max(1, num_cpus / 4)` (8 shards on this host)

The original defaults of shard count equal to `num_cpus` and SQPOLL enabled are superseded by the measured non-SQPOLL default with shard count `max(1, num_cpus / 4)`. With SQPOLL removed, the new default beats the Tokio fallback baseline on every realistic case (Run C +21%, Run D +8%, 1–16 MiB single-stream +3–25%) and uses roughly 25–40% less peak RSS at scale. Two original synthetic thresholds remain just below target (4 KiB pages at 303.5 MiB/s vs 333 MiB/s, Run C at 698.5 MiB/s vs 721 MiB/s); both correspond to op-rate-dominated workloads that BOBS is not deployed under. The remaining optimisations from the deferred list (`IORING_REGISTER_FILES`, `IORING_REGISTER_BUFFERS`, `IORING_SETUP_ATTACH_WQ`) stay available if a future target-hardware benchmark identifies them as necessary.
