# Fix BOBS Review Findings

## TL;DR
> **Summary**: Fix confirmed deployment, HTTP, spool lifecycle, recovery, range, io_uring backpressure, hardening, dependency, and cleanup/doc findings in BOBS. Implement as a sequence of small conventional commits, with chart+code changes kept together where behaviour depends on both.
> **Estimated Effort**: Large

## Context
### Original Request
Plan fixes for confirmed review findings F1-F5, F8-F14 and nits N1-N7 in `/home/james/work/code/bobs`, a Rust/axum single-writer multi-reader HTTP spool service using io_uring file I/O and deployed by `chart/`. Do not re-review; produce an implementation plan grouped into logically scoped commits.

### Key Findings
- [x] `src/main.rs` requires `BOBS_INTERNAL_BASE_URL_TEMPLATE`, but `chart/templates/statefulset.yaml` has no `env:` block; chart defaults live in `chart/values.yaml` and helper names in `chart/templates/_helpers.tpl`.
- [x] `src/http/mod.rs` has `long_poll_redirect(key)` hard-coded to `/api/v1/read/{key}`; ingress rewrites external paths like `/{route_name}-N/{key}` and already supports `/{route_name}-N/api/v1/read/{key}`.
- [x] `src/spool/lifecycle.rs::complete()` currently mutates the write buffer/page metadata and sets in-memory `Complete` before `persist_metadata()`, causing F3/F4.
- [x] `src/spool/reader.rs::read_page()` leaks `self.data_path` on cancellation and uses one `F::read_at()` call for disk pages; `src/manager.rs` already has a read-exact loop pattern.
- [x] `src/manager.rs::recover()` deletes any unrecognized directory in `data_dir`; spool keys are UUIDv4 strings created in `src/http/mod.rs`.
- [x] `src/http/mod.rs::parse_range()` only supports standard/open-ended ranges, maps all range failures to 400, and currently rejects suffix ranges.
- [x] `src/io/ring_pool.rs` uses per-shard `mpsc::unbounded_channel`; requests allocate read buffers driver-side (`vec![0u8; len]`), so bounding accepted queue depth bounds memory risk.
- [x] Chart hardening gaps are in `chart/templates/statefulset.yaml`, `chart/values.yaml`, and `Dockerfile`.
- [x] YAML config parsing is isolated to `src/config.rs`, with dependency `serde_yml = "0.0.12"` in `Cargo.toml`.
- [x] Duplicated `now_secs()` exists in `src/http/mod.rs`, `src/manager.rs`, `src/cleanup.rs`, `src/spool/writer.rs`, and `src/spool/lifecycle.rs`.
- [x] Generated docs exist under `docs/book/`; `.gitignore` does not ignore that path.

## Objectives
### Core Objective
Fix all confirmed review findings without changing the core BOBS deployment model, Linux io_uring default, or fallback feature semantics.

### Deliverables
- [x] F1-F5, F8-F14 fixed in source/chart/Dockerfile/dependency files.
- [x] N1-N7 cleanup/doc/test hygiene completed in separate cleanup commits.
- [x] Regression tests added/updated for lifecycle corruption, metadata-persist retry, ordinal parsing, read error leakage, recovery orphan safety, range conformance, short reads, redirect prefix handling, config YAML parsing, and chart rendering where practical.
- [x] Suggested conventional commit boundaries documented and followed by implementer.

### Definition of Done
- [x] `cargo fmt --check` passes.
- [x] `cargo clippy --all-targets -- -D warnings` passes after applying `cargo clippy --fix` where safe.
- [x] `cargo test` passes.
- [x] `cargo test --features tokio-fileio-fallback` passes or any feature-specific failure is explicitly investigated and documented.
- [x] `cargo build --release --locked --bins` passes.
- [x] `helm template bobs ./chart --set config.host_prefix=test --set config.domain=example.com --set ingress.enabled=true --set replicaCount=2` renders expected env/probes/resources/security/ingress headers.
- [x] `docker build --target release .` reaches the build stage using `--locked` and contains non-root `USER` in final image.

### Guardrails (Must NOT)
- [x] Do not add Prometheus metrics, auth, or proactive disk-space checks.
- [x] Do not change `tokio-fileio-fallback` feature semantics.
- [x] Do not make Tokio file I/O the Linux default; io_uring remains default on Linux.
- [x] Do not introduce per-write metadata commits for ordinary `/write` calls.
- [x] Do not remove the existing ingress path shapes; preserve compatibility for direct `/api/v1/read/{key}` and external `/{route_name}-N/...` paths.

## TODOs

- [x] 1. Commit `fix(chart): provide internal base URL env`
  **What**: Add chart support for `BOBS_INTERNAL_BASE_URL_TEMPLATE`. Prefer a values override rendered with Helm `tpl`, with a default value of `http://{{ include "bobs.fullname" . }}-{ordinal}.{{ .Values.headlessService.name }}:{{ .Values.service.port }}/api/v1`; emit it in the StatefulSet container `env:` block.
  **Files**: `chart/values.yaml`, `chart/templates/statefulset.yaml`
  **Acceptance**: `helm template ...` shows `env.name: BOBS_INTERNAL_BASE_URL_TEMPLATE` and a value that contains the StatefulSet pod-name prefix, headless service name, service port, `/api/v1`, and literal `{ordinal}`.

- [x] 2. Commit `fix(http): preserve ingress prefix on long-poll redirects`
  **What**: Change redirect construction to use a validated `X-Forwarded-Prefix` header when present: `Location: {prefix}/api/v1/read/{key}`; otherwise keep `Location: /api/v1/read/{key}` for direct/internal callers. Pass request headers into the redirect helper or otherwise make the helper able to inspect `X-Forwarded-Prefix`. Validate with an allowlist regex equivalent to `^(/[A-Za-z0-9._~-]+)+$` and length <= 64. Explicitly reject bytes `0x00`-`0x1F` and `0x7F`, backslash `\`, `..` path segments, `%` percent-encoding, `@`, `//` double slash, `?`, `#`, and scheme-relative/absolute-looking values such as `https://evil`. On any rejection, ignore the header and fall back to the plain `/api/v1/read/{key}` Location; never return an error for a bad prefix. Add `Cache-Control: no-store` to the 307 response because `Location` now depends on a request header. Document the trust assumption in a code/chart comment or nearby docs: in-cluster callers can set `X-Forwarded-Prefix` directly; this is accepted because the in-cluster write/read surface is trusted-by-design, and validation bounds the damage. Update ingress `nginx.org/location-snippets` to set `X-Forwarded-Prefix` from the external route prefix before rewriting, e.g. capture `^(/{{ $routeName }}-\d+)(?:/|$)` into an nginx variable and `proxy_set_header X-Forwarded-Prefix $bobs_forwarded_prefix;`.
  **Files**: `src/http/mod.rs`, `chart/templates/ingress.yaml`
  **Acceptance**: Unit tests cover no prefix, valid `/download-3`, invalid `//evil`, invalid `/\evil`, invalid `/x/../evil`, invalid `/foo%2Fevil`, invalid `/x@evil.com`, CRLF bytes, NUL, over-length, and scheme-relative/absolute-looking `https://evil`; all invalid cases produce the fallback `/api/v1/read/{key}` redirect, not an error, and the 307 includes `Cache-Control: no-store`. `helm template ... --set ingress.enabled=true --set replicaCount=2` shows both the route-prefix capture and `proxy_set_header X-Forwarded-Prefix ...` rendered in the same `location` block as the rewrites; because adding any `proxy_set_header` inside a location drops inherited `proxy_set_header` directives from outer scopes in nginx, verify required forwarded headers are not silently lost or explicitly re-set them in the same location block if needed.

- [x] 3. Commit `fix(spool): validate complete size before publishing partial page`
  **What**: Rework `Spool::complete()` so `expected_size` is checked against `metadata.total_bytes_written` before draining `write_buffer`, incrementing `total_pages`, setting `final_page_size`, syncing, or updating cache. Keep the write-buffer lock across completion to serialize against concurrent writes. Add a regression test: write a partial body, call `complete(Some(400))`, assert `SizeMismatch` and no mutation, continue writing at the correct offset, complete with the true size, and read the full body intact.
  **Files**: `src/spool/lifecycle.rs`
  **Acceptance**: New lifecycle test fails on the current mutation-after-mismatch bug and passes after the fix; metadata remains `Writing`, `total_pages` unchanged, and `write_buffer` preserved after wrong expected size.

- [x] 4. Commit `fix(spool): commit complete metadata durably before in-memory Complete`
  **What**: Restructure `Spool::complete()` to build a candidate `SpoolMetadata` clone, fsync the data file, persist the candidate metadata, and only then commit in-memory state/cache/write-buffer changes and notify readers. For idempotent early return, verify durable sidecar state via `metadata_store.read(&self.key).await`; if in-memory is `Complete`/`Deleting` but sidecar is missing or not complete, re-persist the in-memory complete metadata instead of returning success silently. Update the existing sync-failure regression `test_complete_does_not_persist_final_metadata_if_sync_data_fails` so it matches the new ordering: rename/re-document it to state that sync failure does not publish the final partial page, and invert the current cache assertion from `.expect("final partial page is published before sync")` to `cache.get(&spool.key, 0).is_none()`.
  **Files**: `src/spool/lifecycle.rs`
  **Acceptance**: Add a fault-injection metadata store test that fails the first completion metadata write, verifies first `complete()` returns `StorageError` and in-memory/durable state are not falsely complete, then retries successfully and verifies durable sidecar state is `Complete`. The updated sync-data-failure test asserts the cache remains unpopulated after the failed sync and would fail/panic with the old pre-publish behaviour. Existing completion-order tests still show one data sync and one metadata commit on successful idempotent completion.

- [x] 5. Commit `fix(main): require numeric StatefulSet ordinal`
  **What**: Tighten `parse_ordinal()` to require at least one `-` and a non-empty final segment containing only ASCII digits. Return `InvalidInput` for `bobs`, `bobs-`, `bobs-a`, and similar malformed hostnames.
  **Files**: `src/main.rs`
  **Acceptance**: Add/extend unit tests in `src/main.rs` for `bobs-0`, `release-bobs-12`, `bobs`, `bobs-`, `bobs-a`, and `bobs-١` (non-ASCII digit rejected).

- [x] 6. Commit `fix(reader): avoid internal path disclosure`
  **What**: In `read_page()` cancellation, return `BobsError::SpoolNotFound { key: self.key.clone() }` instead of `self.data_path.to_string_lossy()`.
  **Files**: `src/spool/reader.rs`
  **Acceptance**: Update `test_long_poll_cancelled` or add an assertion that the error key is the public spool key and does not contain `spool.dat` or the temp/data directory path.

- [x] 7. Commit `fix(recovery): restrict orphan directory deletion`
  **What**: Change the final orphan sweep in `SpoolManager::recover()` to only remove unrecognized entries whose names parse as UUIDs, whose `entry.file_type()` reports a real directory, which are not symlinks, and which are shaped like spool directories by containing `spool.dat` or `meta.json`. Log a warning and skip anything else. Decide and document in comments/tests that UUID-named directories without `spool.dat` or `meta.json` are skipped rather than deleted. Update the existing `test_recovery_removes_creating_deleting_and_orphan_sidecar_dirs`: its current orphan fixture `orphan-no-sidecar` is not a UUID and must no longer be asserted deleted; rename that fixture to a valid UUID so the existing deletion scenario still covers a UUID orphan with `spool.dat`, and/or split the test into separate cases matching the new semantics (UUID orphan with `spool.dat` removed; non-UUID directory preserved).
  **Files**: `src/manager.rs`
  **Acceptance**: Tests verify a non-UUID directory survives recovery, a UUID-named empty directory survives recovery, a UUID-named orphan directory with `spool.dat` is still removed and logged/counts as orphan deletion, and a UUID-named symlink to a directory outside `data_dir` is skipped so `remove_dir_all` cannot traverse the target.

- [x] 8. Commit `fix(http): conform Range handling to RFC 9110`
  **What**: Extend `ReadRequestRange` with suffix ranges and update `parse_range()` to parse `bytes=-N` while still rejecting multi-range and malformed headers as 400. Resolve suffix ranges only for complete spools; for in-progress spools return 416 with `Content-Range: bytes */*`. Add `BobsError::RangeNotSatisfiable { total: Option<u64>, reason: String }` (or equivalent) and map it in `ApiError` to `StatusCode::RANGE_NOT_SATISFIABLE` with `Content-Range: bytes */{total}` or `bytes */*`. Return 416 for `start >= servable_bytes`, zero-length suffix, and other unsatisfiable-but-parseable cases; keep syntax errors as 400.
  **Files**: `src/error.rs`, `src/http/mod.rs`
  **Acceptance**: Tests cover `bytes=-500` on a complete spool, suffix larger than total returns whole object, `bytes=-0` returns 416, `bytes=999999-` on complete returns 416 with `bytes */{total}`, in-progress suffix returns 416 with `bytes */*`, malformed ranges remain 400, and multi-range remains rejected.

- [x] 9. Commit `fix(reader): loop until disk page reads are complete`
  **What**: Add a shared read-exact helper for `FileIO::read_at()` that loops until the requested length is accumulated or returns `UnexpectedEof` on a zero-byte read before the expected logical length. Use it from `src/spool/reader.rs` disk-page reads and from `src/manager.rs` trailing-partial recovery if feasible, replacing the local duplicate loop.
  **Files**: `src/io/mod.rs` (or a new `src/io/read_exact.rs` plus `src/io/mod.rs` export), `src/spool/reader.rs`, `src/manager.rs`
  **Acceptance**: Add a mock `FileIO` test in `src/spool/reader.rs` that returns a 4096-byte page as several short reads and verifies `read_page()` returns the full page. Add an EOF test that returns fewer bytes than metadata requires and verifies an `IoError(UnexpectedEof)` instead of a short page.

- [x] 10. Commit `fix(io_uring): bound ring-pool submission queues`
  **What**: Replace per-shard `mpsc::unbounded_channel` with bounded `mpsc::channel`. Add queue capacity to `RingPoolOptions` with default 1024 per shard, and thread runtime configuration through `Config` as `io_uring_queue_capacity` with default 1024 and validation > 0. Make `RingPool::submit_to_ring()` async and use `sender.send(req).await`; do not use `blocking_send` from async contexts. Update all async callers: `submit_metadata_commit`, `UringFileIO::{write_at,read_at,sync_data,remove}`, and `submit_open`. Keep driver `blocking_recv()/try_recv()` and sender-drop shutdown semantics intact.
  **Files**: `src/config.rs`, `src/main.rs`, `src/io/ring_pool.rs`, `src/io/uring_fs.rs`, `src/metadata/uring.rs`, `chart/values.yaml`, `chart/templates/configmap.yaml`
  **Acceptance**: Existing ring-pool routing/submission/shutdown tests compile and pass after all `.await` call sites are updated. Add/adjust a ring-pool test with capacity 1 that demonstrates a second submit waits until driver capacity drains (use `tokio::timeout`/controlled fake where practical) without deadlocking.

- [x] 11. Commit `fix(chart,docker): harden production deployment defaults`
  **What**: Populate default resources in `values.yaml` (`requests.memory=16Gi`, `requests.cpu=4`, `limits.memory=16Gi`, `limits.cpu=8`) and template them with `toYaml`. Add readiness and liveness probes on `GET /api/v1/health` using the `http` port (readiness initialDelay 5s period 10s; liveness initialDelay 15s period 20s). Distinguish pod-level and container-level security contexts in values/templates: `podSecurityContext` defaults to `runAsNonRoot: true`, `runAsUser: 10001`, `runAsGroup: 10001`, and `fsGroup: 10001`; `containerSecurityContext` defaults to `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, and `capabilities: { drop: ["ALL"] }`. Before enabling `readOnlyRootFilesystem`, verify BOBS writes only to `config.data_dir`; if any runtime path outside `data_dir` needs writes, add an `emptyDir` volume/mount for that path. Do not set `seccompProfile: RuntimeDefault` blindly: default container seccomp profiles have historically blocked `io_uring_setup`, `io_uring_enter`, and `io_uring_register`, which would break BOBS' default Linux I/O backend. Record the explicit decision to leave `seccompProfile` unset by default for this chart, add a parameterised `values.yaml` override plus comments explaining the io_uring rationale, and allow operators to provide a seccomp profile explicitly when they have validated it. Add non-root user UID/GID 10001 to Dockerfile release and debug stages and set `USER 10001:10001`. Change builder command to `cargo build --release --locked --bins`.
  **Files**: `chart/values.yaml`, `chart/templates/statefulset.yaml`, `Dockerfile`
  **Acceptance**: `helm template ...` renders resources, probes, pod security context, container security context, read-only root filesystem, dropped capabilities, and no default `RuntimeDefault` seccomp profile unless the chart value is explicitly set. Writable mounts are limited to `data_dir` plus any justified `emptyDir` runtime paths. `docker build --target release .` uses locked Cargo dependency resolution and final stage declares non-root user.

- [x] 12. Commit `fix(config): replace serde_yml with serde_norway`
  **What**: Replace `serde_yml` dependency with `serde_norway` and update `Config::from_file()` to call `serde_norway::from_str`. Update lockfile by running Cargo.
  **Files**: `Cargo.toml`, `Cargo.lock`, `src/config.rs`
  **Acceptance**: `cargo test config` and full `cargo test` pass; `rg serde_yml` returns no source/dependency declaration uses except historical lockfile removal no longer present.

- [x] 13. Commit `refactor(http): simplify job-id logging and read response helpers`
  **What**: Apply N1/N2. Remove the dead `value.len() > 64` clause from `extract_job_id()`. Stop duplicating `job.id` fields in event macros where the request span already records it; rely on `observability.rs` span-field merging. For stream completion outside the handler future, create/enter a span containing `job.id`, key, and range for the final event instead of putting `job.id` directly on the event. Extract helper functions for range resolution and response header assembly to reduce `read_spool()` length.
  **Files**: `src/http/mod.rs`, `tests/observability.rs` if assertions need minor updates
  **Acceptance**: `tests/observability.rs` still proves events have `job.id` attributes when the header is valid and no `job.id` when absent; `read_spool()` is shorter and no longer has repeated `if let Some(job_id) ... else ...` event pairs for ordinary in-span events.

- [x] 14. Commit `refactor(time): centralize now_secs helper`
  **What**: Add a single crate time helper and replace duplicated local definitions. Prefer `src/time.rs` with `pub fn now_secs() -> u64`, exported from `src/lib.rs` as `pub mod time;`. Update comments referencing local helpers.
  **Files**: `src/time.rs`, `src/lib.rs`, `src/http/mod.rs`, `src/manager.rs`, `src/cleanup.rs`, `src/spool/writer.rs`, `src/spool/lifecycle.rs`
  **Acceptance**: `rg "fn now_secs" src` finds only `src/time.rs`; tests that manipulate timestamps still pass.

- [x] 15. Commit `docs: align design and todo with current BOBS`
  **What**: Rewrite `DESIGN.md` to match current implementation: `/complete` not `/close`, UUIDv4 keys without host prefix, global page cache, sidecar `meta.json` metadata, positional io_uring/Tokio FileIO, read follow/redirect behaviour, write-locked readability, cleanup rules. Rewrite `TODO.md`: remove fixed items (redb, pread/pwrite, global cache cap), keep still-valid items (no fsync per page write, no metrics, no disk-space checks, no auth, request body limit if still relevant), and do not introduce out-of-scope implementation tasks as part of this fix.
  **Files**: `DESIGN.md`, `TODO.md`
  **Acceptance**: Documents no longer mention `/close`, host-uuid keys, per-spool cache, redb as current store, or seek-based I/O as current implementation.

- [x] 16. Commit `chore(docs): stop tracking generated book artifacts`
  **What**: Remove generated `docs/book/` artifacts from git tracking and add `docs/book/` to `.gitignore`. Use `git rm -r --cached docs/book` during implementation if the directory is tracked; keep local files if present.
  **Files**: `.gitignore`, `docs/book/` index state only
  **Acceptance**: `git status` shows `docs/book/*` removed from the index (if tracked) and `.gitignore` contains `docs/book/`; source docs under `docs/src/` remain tracked.

- [x] 17. Commit `test(http): isolate ttl config data dir`
  **What**: Apply N6 by changing `test_config_ttl` to accept/use the temp data directory created in `app_with_ttl_config()`, matching `test_config(dir)`.
  **Files**: `src/http/mod.rs`
  **Acceptance**: TTL tests no longer reference `./data` and still pass under `cargo test http::tests::test_short_ttl_cleanup_fires_after_full_read`.

- [x] 18. Commit `chore: apply clippy fixes and formatting`
  **What**: Run `cargo clippy --fix --all-targets --allow-dirty` only after behavioural commits are in place, review the diff, keep safe modernizations such as `io::Error::other`, and run `cargo fmt`.
  **Files**: Any Rust source files with mechanical clippy/fmt changes
  **Acceptance**: `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` pass cleanly.

## Verification
- [x] Run `cargo fmt --check`.
- [x] Run `cargo clippy --all-targets -- -D warnings`.
- [x] Run `cargo test`.
- [x] Run `cargo test --features tokio-fileio-fallback` to verify fallback semantics still compile/pass.
- [x] Run `cargo build --release --locked --bins`.
- [x] Run `helm template bobs ./chart --set config.host_prefix=test --set config.domain=example.com --set ingress.enabled=true --set replicaCount=2` and inspect env, ingress `X-Forwarded-Prefix` capture/header in the same location block, inherited forwarded-header handling, resources, probes, security contexts, read-only root filesystem, and seccomp override behaviour.
- [x] Run `docker build --target release .`.
- [x] Confirm `rg serde_yml Cargo.toml Cargo.lock src` returns no active dependency/use.
- [x] Confirm `rg "fn now_secs" src` returns only the centralized helper.
- [x] Confirm `rg "/close|host-uuid|redb|seek-based|per-spool" DESIGN.md TODO.md` has no stale-current-design claims.
- [x] Confirm `git status --short` groups cleanly into the suggested commits and contains no accidental generated `docs/book/` additions.
