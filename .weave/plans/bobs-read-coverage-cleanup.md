# BOBS Read-Coverage Based Cleanup Semantics

## TL;DR
> **Summary**: Replace BOBS's coarse `reader_done_ttl_secs`/`unread_ttl_secs` cleanup with precise read-coverage semantics: a timer anchored on readable time, activity-refreshed idle TTL, and a fast short TTL triggered only after aggregate byte coverage proves the entire object was served. Includes a fragmentation-safe missing-ranges algorithm for objects up to 100 GB+.
> **Estimated Effort**: Large

---

## Context

### Original Request
Add read-coverage-based lifecycle cleanup to BOBS:
1. Idle cleanup starts from when the spool becomes readable (not created_at).
2. Bytes-served activity refreshes the idle timer; stalled connections do not protect forever.
3. A separate short TTL fires once BOBS has served every byte of the spool at least once (aggregate across Range requests).
4. Missing-ranges algorithm tracks coverage in O(log gaps) space, safe for 100 GB+ objects; includes a fragmentation cap that falls back safely to idle TTL.

### Key Findings

**Current cleanup (`src/cleanup.rs`)**
Three independent triggers, all comparing against config thresholds:
- `writer_inactive`: `now - last_write_at > writer_inactivity_timeout_secs` while still Writing/WriteLocked.
- `reader_done_expired`: state Complete/Readable, `last_read_at` set, `reader_count == 0`, `now - last_read_at > reader_done_ttl_secs`.
- `unread_expired`: state Complete/Readable, `last_read_at == None`, `now - created_at > unread_ttl_secs`.

**Bugs / gaps**:
- `unread_ttl_secs` timer anchors on `created_at`, but writes can be long-running (hours). A spool can be deleted before it's even used if writing takes longer than `unread_ttl_secs`.
- `reader_count > 0` is an absolute guard — any open connection (even a stalled one that has served zero bytes for days) prevents cleanup forever.
- No mechanism to detect that all bytes have been served; only "was read at some point."
- No per-range coverage tracking.

**`SpoolMetadata` (`src/spool/types.rs`)**
Relevant fields: `created_at`, `last_write_at`, `last_read_at: Option<u64>`, `total_bytes_written`, `state`. Serialized as JSON into redb on every `write()` call (via `persist_metadata`). `#[serde(default)]` is not on `SpoolMetadata` itself — new optional fields need `#[serde(default)]` per-field.

**`Spool` struct (`src/spool/mod.rs`)**
In-memory fields: `metadata: Arc<Mutex<SpoolMetadata>>`, `reader_count: Arc<AtomicUsize>`, `cancel: CancellationToken`. We can add new `Arc<AtomicU64>` and `Arc<Mutex<...>>` fields here without any persistence impact.

**Read path (`src/http/mod.rs` `read_spool`)**
- No Range header → `ReadRequestRange::Follow` → streams from offset 0 to EOF, following the writer.
- Range header → `ReadRequestRange::Bounded { start, end_inclusive }` → reads a fixed byte window and returns 206 Partial Content.
- Per chunk: locks `spool.metadata`, sets `meta.last_read_at = Some(now_secs())`.
- RAII `ReaderLease` drops `reader_count` on stream end/cancel.

**`complete()` (`src/spool/lifecycle.rs`)**
Flushes partial page, fsyncs, sets `meta.state = SpoolState::Complete`, persists metadata, notifies readers. This is the correct hook for `readable_at` and `missing_ranges.initialize()`.

**`set_write_locked(false)` (`src/spool/lifecycle.rs`)**
Transitions `WriteLocked → Readable`. This is the other hook for `readable_at` (spool becomes readable before it's Complete when write-lock is released).

**`manager.rs` `recover()`**
Rehydrates spools from redb. New in-memory fields (`last_read_activity_at`, `full_object_read_at`, `missing_ranges`) are zero/empty on every restart — their absence safely delays cleanup, never causes premature deletion.

**Chart (`chart/`)**
`chart/values.yaml` lists all config keys under `config:`. `chart/templates/configmap.yaml` renders them verbatim into `config.yaml`. Both need new keys added.

---

## Objectives

### Core Objective
Replace the broken `unread_ttl_secs`/`reader_done_ttl_secs` pair with three clean rules:
1. **Idle TTL** (`read_idle_ttl_secs`, default 600s): timer anchors on `readable_at`; refreshed whenever bytes are actually served; stalled connections do not extend it.
2. **Full-read TTL** (`full_read_complete_ttl_secs`, default 30s): fires after aggregate Range/GET coverage has reached 100% of object bytes.
3. **Writer-inactive** (unchanged): protects against abandoned uploads.

### Deliverables
- [x] `src/spool/coverage.rs` — `MissingRanges` struct with unit tests.
- [x] `src/spool/types.rs` — `readable_at: Option<u64>` field on `SpoolMetadata`.
- [x] `src/config.rs` — `read_idle_ttl_secs`, `full_read_complete_ttl_secs`; old fields kept for backward compat.
- [x] `src/spool/mod.rs` — new in-memory fields on `Spool`; `coverage` module exported.
- [x] `src/spool/lifecycle.rs` — set `readable_at`; call `missing_ranges.initialize()`; trigger `full_object_read_at` if immediately complete.
- [x] `src/manager.rs` — init new in-memory fields; on recovery of Complete spools, call `initialize()`; seed `last_read_activity_at = now_secs()` on restart.
- [x] `src/http/mod.rs` — instrument `read_spool` to track served ranges and update activity timestamp.
- [x] `src/cleanup.rs` — new cleanup rules, updated unit tests.
- [x] `chart/values.yaml` and `chart/templates/configmap.yaml` — new config keys.
- [x] `tests/integration.rs` — new behavioral integration tests.

### Definition of Done
- [x] `cargo fmt --all -- --check` passes.
- [x] `cargo test` passes with zero failures or skips.
- [x] `cargo clippy -- -D warnings` passes.
- [x] All new unit tests (coverage, cleanup, http) are deterministic and use `tokio::time::pause()` for TTL assertions.
- [x] Integration tests in `tests/integration.rs` pass for range-coverage and full-read-TTL behavior.
- [x] Old config keys (`reader_done_ttl_secs`, `unread_ttl_secs`) still parse without error (backward compat).

### Guardrails (Must NOT)
- Must NOT add a user-triggered delete API.
- Must NOT call `persist_metadata` / redb write on every served chunk.
- Must NOT allocate per-byte data structures (e.g., `Vec<bool>` of object size).
- Must NOT prematurely delete a spool due to a lost in-memory state after restart.
- Must NOT skip or `#[ignore]` any test.

---

## TODOs

- [x] 1. Create `src/spool/coverage.rs` — `MissingRanges` struct
  **What**: New file. Implements the missing-intervals coverage tracker. All logic is synchronous (no async). No I/O. Exposed as `pub` from `src/spool/mod.rs`.

  **Struct design**:
  ```rust
  pub struct MissingRanges {
      // Sorted non-overlapping half-open intervals [start, end) not yet served.
      // Empty AND total_size is Some → full coverage achieved.
      gaps: BTreeMap<u64, u64>,      // gap_start → gap_end
      pub total_size: Option<u64>,   // None until initialize() is called
      fragmentation_cap: usize,      // default 1024
      capped: bool,                  // true = cap exceeded; is_complete() always false
      // Ranges served before total_size was known, buffered for replay at initialize().
      pending: Vec<(u64, u64)>,
  }
  ```

  **`MissingRanges::new(fragmentation_cap: usize) -> Self`**:
  gaps empty, total_size None, capped false, pending empty.

  **`MissingRanges::initialize(&mut self, total_size: u64)`**:
  ```
  if self.total_size.is_some() { return; }
  self.total_size = Some(total_size);
  if total_size == 0 { return; }  // zero-byte object is immediately "complete"
  self.gaps.insert(0, total_size);
  let pending = mem::take(&mut self.pending);
  for (s, e) in pending { self._apply(s, e); }
  ```

  **`MissingRanges::mark_served(&mut self, start: u64, end: u64)`**:
  ```
  if self.capped || start >= end { return; }
  if self.total_size.is_none() {
      self.pending.push((start, end));
      if self.pending.len() > self.fragmentation_cap {
          self.capped = true; self.pending.clear();
      }
      return;
  }
  self._apply(start, end);
  ```

  **`MissingRanges::_apply(&mut self, start: u64, end: u64)`** (private):
  ```
  // Collect gaps whose [gap_start, gap_end) overlaps [start, end):
  //   gap_start < end  (from BTreeMap range ..end)
  //   gap_end   > start (filter)
  let overlapping: Vec<(u64,u64)> = self.gaps.range(..end)
      .filter(|(_, &ge)| ge > start)
      .map(|(&gs, &ge)| (gs, ge))
      .collect();
  for (gs, ge) in overlapping {
      self.gaps.remove(&gs);
      if gs < start { self.gaps.insert(gs, start); }
      if ge > end   { self.gaps.insert(end, ge);   }
  }
  if self.gaps.len() > self.fragmentation_cap {
      self.capped = true;
      self.gaps.clear();
  }
  ```
  Complexity: O((k + 1) log n) where k = number of overlapping gaps, n = total gap count.

  **`MissingRanges::is_complete(&self) -> bool`**:
  `!self.capped && self.total_size.is_some() && self.gaps.is_empty()`

  **`MissingRanges::gap_count(&self) -> usize`**: `self.gaps.len()` (for tests/debug).

  **Files**: `src/spool/coverage.rs` (create)

  **Acceptance**: File compiles; all unit tests in the file pass (see TODO 10 for test list).

---

- [x] 2. Add `readable_at` to `SpoolMetadata`
  **What**: Add one new field to `SpoolMetadata` in `src/spool/types.rs`. Requires `#[serde(default)]` so existing redb JSON without this field deserializes as `None`.

  ```rust
  #[serde(default)]
  pub readable_at: Option<u64>,  // unix secs; set when spool first becomes readable
  ```

  Place it after `last_read_at` for logical grouping. No other field changes to `SpoolMetadata`.

  Also update every `SpoolMetadata { .. }` literal in tests within `types.rs`, `lifecycle.rs`, `reader.rs`, `writer.rs`, `manager.rs` — add `readable_at: None` to all struct literal initialisations (or use `..` spread if a `Default` impl exists; it doesn't currently, so add the field explicitly). Check: `src/spool/lifecycle.rs` `make_spool()`, `src/spool/reader.rs` `make_spool()`, `src/spool/writer.rs` `make_spool()`, `src/manager.rs` `create_spool()` and recovery section.

  **Files**: `src/spool/types.rs`; all files that construct `SpoolMetadata` literals.

  **Acceptance**: `cargo test` compiles and passes; `cargo test -- --test-output immediate 2>&1 | grep readable_at` finds the field in serialized JSON round-trip test.

---

- [x] 3. Add new config fields
  **What**: Add two fields to `Config` in `src/config.rs`. Both are `u64` with `#[serde(default)]`. Add corresponding `Default` values.

  ```rust
  pub read_idle_ttl_secs: u64,          // default 600
  pub full_read_complete_ttl_secs: u64, // default 30
  ```

  In `impl Default for Config`:
  ```rust
  read_idle_ttl_secs: 600,
  full_read_complete_ttl_secs: 30,
  ```

  Keep `reader_done_ttl_secs` and `unread_ttl_secs` in Config untouched. They will be parsed but no longer drive cleanup. Add doc comments marking them deprecated.

  Add `validate()` checks: both new fields must be > 0.

  Update `test_defaults()` to assert new field values. Update `test_from_file_yaml()` to include the new keys in the test YAML. Update `test_validate_accepts_valid_config()` to keep passing.

  Update `test_config()` helper in `src/cleanup.rs` and `src/http/mod.rs` and `tests/integration.rs` to include the new fields (can default to short values like 2s in tests).

  **Files**: `src/config.rs`, `src/cleanup.rs`, `src/http/mod.rs`, `tests/integration.rs`

  **Acceptance**: `cargo test config` passes; old YAML without the new keys still parses successfully (defaults apply).

---

- [x] 4. Add in-memory coverage fields to `Spool`
  **What**: Extend `Spool<F>` in `src/spool/mod.rs` with three new runtime-only fields. These are never persisted.

  ```rust
  // In Spool<F>:
  pub missing_ranges: Arc<Mutex<MissingRanges>>,
  /// Unix secs of last byte-served event. 0 = never served. AtomicU64 for
  /// lock-free updates from the hot read path.
  pub last_read_activity_at: Arc<AtomicU64>,
  /// Unix secs when full object coverage was first detected. 0 = not yet.
  pub full_object_read_at: Arc<AtomicU64>,
  ```

  In `Spool::new(...)`:
  ```rust
  missing_ranges: Arc::new(Mutex::new(MissingRanges::new(1024))),
  last_read_activity_at: Arc::new(AtomicU64::new(0)),
  full_object_read_at: Arc::new(AtomicU64::new(0)),
  ```

  Add `pub mod coverage;` and `pub use coverage::MissingRanges;` to `src/spool/mod.rs`.

  Add `use std::sync::atomic::AtomicU64;` import.

  **Files**: `src/spool/mod.rs`, `src/spool/coverage.rs` (already created)

  **Acceptance**: Compiles; existing tests still pass (new fields are zero-initialised and invisible to old tests).

---

- [x] 5. Instrument `lifecycle.rs` — set `readable_at` and call `initialize()`
  **What**: Two hook points in `src/spool/lifecycle.rs`.

  **In `complete()`**, after `meta.state = SpoolState::Complete` is set and before `persist_metadata`:
  ```rust
  // Set readable_at if not already set (e.g., spool went Writing → Complete
  // without a Readable intermediate).
  meta.readable_at.get_or_insert_with(now_secs);
  let total_size = meta.total_bytes_written;
  ```
  Then, after `persist_metadata` returns OK (so we're past the potential SizeMismatch error):
  ```rust
  {
      let mut mr = self.missing_ranges.lock().await;
      mr.initialize(total_size);
      if mr.is_complete() {
          // Zero-byte or immediately fully-covered object.
          self.full_object_read_at
              .compare_exchange(0, now_secs(), Ordering::SeqCst, Ordering::SeqCst)
              .ok();
      }
  }
  ```

  **In `set_write_locked(false)`** (the `WriteLocked → Readable` branch):
  ```rust
  meta.state = SpoolState::Readable;
  meta.readable_at.get_or_insert_with(now_secs);
  ```
  The `Readable` state spool is not yet complete, so `missing_ranges` has no size yet — no `initialize()` call needed here.

  **Note on persist**: `readable_at` is written to redb here because it's part of `SpoolMetadata`. This single persist happens at completion/unlock time, which is already an existing fsync point. No hot-path impact.

  **Files**: `src/spool/lifecycle.rs`

  **Acceptance**: After calling `spool.complete()` in tests, `spool.metadata.lock().readable_at` is `Some(_)` and `spool.missing_ranges.lock().total_size` is `Some(total_bytes_written)`. For a zero-byte spool, `spool.full_object_read_at.load()` is nonzero after `complete()`.

---

- [x] 6. Init new fields in `manager.rs`
  **What**: Two sites in `src/manager.rs`.

  **`create_spool()`**: After `Spool::new(metadata, ...)`, no extra action needed — fields are zero-init in `Spool::new`. Nothing to add here.

  **`recover()`**: After `Spool::new(meta, ...)` (the line that builds `spool`), add:
  ```rust
  // Seed last_read_activity_at to now so spools that survived a restart
  // get a full read_idle_ttl_secs grace period before cleanup can fire.
  spool.last_read_activity_at.store(now_secs(), Ordering::SeqCst);

  // Re-initialize missing ranges for complete spools. No served ranges are
  // known after restart, so coverage starts fresh from [0, total_size).
  // This means full_read_complete_ttl_secs won't trigger until the object
  // is re-served — safe by design.
  if matches!(meta.state, SpoolState::Complete) {
      spool.missing_ranges.lock().await.initialize(meta.total_bytes_written);
  }
  ```
  Note: `spool` is not yet `Arc`-wrapped at this line; wrap after this init block, or clone the `Arc` just long enough to call `initialize`. Inspect exact code structure in `recover()` — the `let spool = Arc::new(Spool::new(...).await)` line is followed by `*spool.running_crc32c.lock().await = crc;`. Add the new init after that line.

  Also: if a recovered spool's `meta.readable_at` is `None` (old metadata from before this change), write it back:
  ```rust
  if matches!(meta.state, SpoolState::Complete | SpoolState::Readable)
      && meta.readable_at.is_none()
  {
      meta.readable_at = Some(now_secs());
      metadata_corrected = true;  // triggers the existing re-persist block
  }
  ```
  This prevents premature deletion of old spools on first startup after upgrade.

  **Files**: `src/manager.rs`

  **Acceptance**: Recovery test `test_recovery` still passes; new test (TODO 11) verifies `last_read_activity_at` is nonzero after recovery and `missing_ranges.total_size` is Some for a Complete spool.

---

- [x] 7. Instrument `read_spool` in `src/http/mod.rs`
  **What**: Add coverage tracking inside the `stream!` block in `read_spool`. Two responsibilities:
  (a) Update `last_read_activity_at` (atomic, no lock) whenever a chunk is yielded.
  (b) Call `missing_ranges.mark_served(chunk_start, chunk_end)` and check `is_complete()`.

  **Inside the stream, in the branch that yields a chunk** (just after `let chunk = Bytes::copy_from_slice(...)`):

  ```rust
  // Record that bytes [offset, offset + chunk.len()) have been served.
  let chunk_start = offset;
  // (offset is updated on the next line in existing code)
  offset += chunk.len() as u64;
  let chunk_end = offset;

  // 1. Refresh activity timestamp (atomic, lock-free).
  let now = now_secs();
  spool.last_read_activity_at.store(now, Ordering::Relaxed);

  // 2. Record coverage and detect full-read completion.
  {
      let mut mr = spool.missing_ranges.lock().await;
      mr.mark_served(chunk_start, chunk_end);
      if mr.is_complete()
          && spool.full_object_read_at.load(Ordering::Relaxed) == 0
      {
          spool.full_object_read_at
              .compare_exchange(0, now, Ordering::SeqCst, Ordering::SeqCst)
              .ok();
      }
  }

  // 3. Keep legacy last_read_at for observability (not used in new cleanup).
  {
      let mut meta = spool.metadata.lock().await;
      meta.last_read_at = Some(now);
  }

  yield Ok::<Bytes, BobsError>(chunk);
  ```

  Ordering note: `Ordering::Relaxed` is sufficient for `last_read_activity_at` store — the cleanup loop reads it with a short staleness window (one sweep interval). `compare_exchange` for `full_object_read_at` uses `SeqCst` to prevent duplicate first-set races.

  The `missing_ranges` mutex and `metadata` mutex are separate — no deadlock risk. However, both are acquired per chunk. This is acceptable: both locks are uncontested in the common single-reader case, and the operations are microsecond-scale. Add a comment noting this can be batched if profiling reveals contention.

  **No redb persist here.** `last_read_at` update is already purely in-memory. `last_read_activity_at` and `full_object_read_at` are also purely in-memory. ✓

  **Files**: `src/http/mod.rs`

  **Acceptance**: After a full Range read in tests, `spool.full_object_read_at.load()` is nonzero; after a partial Range read it remains 0.

---

- [x] 8. Rewrite cleanup logic in `src/cleanup.rs`
  **What**: Replace the three-condition body of `run_cleanup_loop` with the new four-condition logic. Remove the `readers_active` / `reader_count` guard entirely (stalled readers no longer get absolute protection).

  **New body inside the spool loop**:
  ```rust
  let (state, last_write_at, readable_at) = {
      let meta = spool.metadata.lock().await;
      (meta.state.clone(), meta.last_write_at, meta.readable_at)
  };

  // --- Rule 1: Writer abandoned the spool (unchanged). ---
  let writer_inactive = matches!(state, SpoolState::Writing | SpoolState::WriteLocked)
      && now.saturating_sub(last_write_at) > config.writer_inactivity_timeout_secs;

  let last_activity = spool.last_read_activity_at.load(Ordering::Relaxed);

  // --- Rule 2: Full-read short TTL. ---
  // full_object_read_at > 0 means every byte has been served at least once.
  // Later byte-serving activity refreshes this anchor.
  let full_read_at = spool.full_object_read_at.load(Ordering::Relaxed);
  let full_read_expired = full_read_at > 0 && {
      let anchor = full_read_at.max(last_activity);
      now.saturating_sub(anchor) > config.full_read_complete_ttl_secs
  };

  // --- Rule 3: Idle TTL. ---
  // Anchor: last byte-served activity, or (if never served) readable_at,
  // or (if readable_at unknown, i.e. old metadata) now (safe: won't delete).
  let idle_expired = matches!(state, SpoolState::Complete | SpoolState::Readable) && {
      let anchor = if last_activity > 0 {
          last_activity
      } else {
          // Never served since last restart. Use readable_at if we have it;
          // otherwise use 'now' (conservative — won't delete on this sweep).
          readable_at.unwrap_or(now)
      };
      now.saturating_sub(anchor) > config.read_idle_ttl_secs
  };

  if writer_inactive || full_read_expired || idle_expired {
      to_delete.push(key);
  }
  ```

  Remove the old `reader_done_expired` and `unread_expired` variables entirely.

  **Fields read from `Config`**: `writer_inactivity_timeout_secs`, `full_read_complete_ttl_secs`, `read_idle_ttl_secs`. Fields `reader_done_ttl_secs` and `unread_ttl_secs` are no longer read here (keep in struct for parse compat).

  **Files**: `src/cleanup.rs`

  **Acceptance**: All updated cleanup unit tests pass (see TODO 11).

---

- [x] 9. Update chart config
  **What**: Add two new keys to chart config. Keep old keys for a transition release (operators may have them set in values overrides).

  **`chart/values.yaml`** — add under `config:`:
  ```yaml
  read_idle_ttl_secs: 600
  full_read_complete_ttl_secs: 30
  ```

  **`chart/templates/configmap.yaml`** — add two lines at end of `config.yaml:` block:
  ```yaml
  read_idle_ttl_secs: {{ .Values.config.read_idle_ttl_secs }}
  full_read_complete_ttl_secs: {{ .Values.config.full_read_complete_ttl_secs }}
  ```

  Leave `reader_done_ttl_secs` and `unread_ttl_secs` lines in place for now. Add a comment `# deprecated — no longer drives cleanup` above them in the configmap template.

  **Files**: `chart/values.yaml`, `chart/templates/configmap.yaml`

  **Acceptance**: `helm template . --set config.host_prefix=x --set config.domain=y --set config.route_name=z` renders both new keys in the ConfigMap output.

---

- [x] 10. Unit tests — `MissingRanges` (in `src/spool/coverage.rs`)
  **What**: Add the following tests in the `#[cfg(test)] mod tests` block. All are synchronous, no async, no I/O, no large allocations.

  - [x] `test_empty_object_is_immediately_complete` — `initialize(0)`, `is_complete()` = true.
  - [x] `test_single_full_range` — `initialize(100)`, `mark_served(0, 100)`, `is_complete()` = true.
  - [x] `test_partial_range_not_complete` — `initialize(100)`, `mark_served(0, 50)`, `is_complete()` = false, `gap_count()` = 1.
  - [x] `test_two_adjacent_ranges` — `mark_served(0,50)`, `mark_served(50,100)`, `is_complete()` = true.
  - [x] `test_two_overlapping_ranges` — `mark_served(0,60)`, `mark_served(40,100)`, `is_complete()` = true.
  - [x] `test_out_of_order_ranges` — `mark_served(50,100)` then `mark_served(0,50)`, `is_complete()` = true.
  - [x] `test_covering_superset_range` — `initialize(100)`, `mark_served(0,200)`, `is_complete()` = true (range beyond end is safe).
  - [x] `test_non_contiguous_partial` — `initialize(100)`, `mark_served(0,30)`, `mark_served(70,100)`, `is_complete()` = false, `gap_count()` = 1 (one gap: [30,70)).
  - [x] `test_huge_object_no_large_alloc` — `initialize(100 * 1024 * 1024 * 1024)` (100 GiB), serve 10 000 random non-overlapping ranges, check gap_count stays ≤ 1024; assert no OOM (test just runs without allocating GiB of memory). Use a deterministic PRNG (e.g., simple LCG) for reproducibility, no `rand` crate dependency needed.
  - [x] `test_fragmentation_cap_triggers_fallback` — `initialize(100)`, insert 1025 distinct 1-byte gaps (e.g., all odd bytes), observe `capped = true`, `is_complete()` = false even after `mark_served(0,100)`.
  - [x] `test_pending_ranges_applied_on_init` — `mark_served(0,50)`, then `initialize(100)`, then `mark_served(50,100)`, `is_complete()` = true.
  - [x] `test_pending_ranges_cover_full_on_init` — `mark_served(0,100)`, then `initialize(100)`, `is_complete()` = true immediately after init (pending fully applied).
  - [x] `test_empty_range_is_noop` — `mark_served(5,5)` does nothing; `gap_count()` unchanged.
  - [x] `test_inverted_range_is_noop` — `mark_served(10,5)` does nothing (start >= end).
  - [x] `test_double_initialize_is_idempotent` — `initialize(100)` twice does not duplicate gaps.
  - [x] `test_pending_cap_triggers_fallback` — with `fragmentation_cap=3`, add 4 pending ranges before init, `capped = true`.

  **Files**: `src/spool/coverage.rs`

  **Acceptance**: `cargo test spool::coverage` all green.

---

- [x] 11. Unit tests — cleanup and manager (in `src/cleanup.rs` and `src/manager.rs`)
  **What**: Update/add tests in `src/cleanup.rs`. Update `test_config()` helper to include `read_idle_ttl_secs: 1, full_read_complete_ttl_secs: 1`.

  **Existing tests to update** (change field names in `test_config()` and update assertions to match new behavior):
  - `test_writer_inactivity_cleanup` — regression, unchanged behavior. ✓
  - `test_active_reader_not_deleted` — **change**: remove `reader_count.fetch_add` setup; instead set `last_read_activity_at` to `now_secs()` and assert spool is NOT deleted within the TTL. Without activity, it SHOULD be deleted now. Update test name to `test_recent_activity_prevents_idle_cleanup`.
  - `test_reader_done_ttl_cleanup` — replace with `test_idle_ttl_after_read_activity`: set `readable_at = 0` (past), `last_read_activity_at = 0` (never), state = Complete, verify deletion after sweep.
  - `test_unread_ttl_cleanup` — replace with `test_idle_ttl_anchors_on_readable_at`: set `readable_at` to an old timestamp, `last_read_activity_at = 0`, verify deletion. Crucially: set `created_at` to a recent timestamp to prove the anchor is `readable_at`, not `created_at`.

  **New tests**:
  - [x] `test_readable_at_none_uses_now_as_safe_anchor` — `readable_at = None`, `last_read_activity_at = 0`, state = Complete; after `read_idle_ttl_secs + 1` advance, spool is NOT deleted (None → now, so anchor is fresh). Verify spool is protected on first sweep.
  - [x] `test_full_object_read_triggers_short_ttl` — state = Complete, set `spool.full_object_read_at = old_ts`, advance `full_read_complete_ttl_secs + 1` secs, verify deletion.
  - [x] `test_full_object_read_short_ttl_not_triggered_before_expiry` — `full_object_read_at` set to `now`, advance only `full_read_complete_ttl_secs / 2` secs, verify NOT deleted.
  - [x] `test_stalled_reader_no_activity_idle_expires` — `reader_count.fetch_add(1)`, state = Complete, `readable_at = 0` (old), `last_read_activity_at = 0` (never), advance idle TTL, verify DELETED (stalled reader no longer protects).
  - [x] `test_active_reader_with_recent_bytes_refreshes_idle` — set `last_read_activity_at = now_secs()`, advance `read_idle_ttl_secs - 5`, set `last_read_activity_at = now_secs()` again (simulate re-serve), advance 5 more secs, verify NOT deleted.
  - [x] `test_writing_spool_not_deleted_by_idle` — state = Writing; idle TTL logic does not apply (only writer_inactive applies to Writing state). Verify idle rule doesn't fire.

  **In `src/manager.rs` tests**:
  - [x] `test_recovery_seeds_last_read_activity_at` — create a complete spool, recover in a new manager, verify `spool.last_read_activity_at.load() > 0`.
  - [x] `test_recovery_initializes_missing_ranges_for_complete_spool` — create + complete a spool with 8192 bytes, recover, verify `spool.missing_ranges.lock().total_size == Some(8192)` and `gap_count() == 1` (no bytes re-served yet).
  - [x] `test_recovery_sets_readable_at_for_old_metadata` — manually write metadata with `readable_at: null` to redb, recover, verify `readable_at` is now `Some(_)` in persisted metadata.

  **Files**: `src/cleanup.rs`, `src/manager.rs`

  **Acceptance**: `cargo test cleanup`, `cargo test manager` all green.

---

- [x] 12. Unit tests — HTTP layer (in `src/http/mod.rs`)
  **What**: Add tests using the in-process `tower::ServiceExt::oneshot` test harness (same pattern as existing tests). These tests can use `tokio::time::pause()` for TTL assertions.

  - [x] `test_full_range_sets_full_object_read_at` — write 8192 bytes, complete, do `Range: bytes=0-8191` read, verify `spool.full_object_read_at.load() > 0`.
  - [x] `test_partial_range_does_not_set_full_object_read_at` — write 8192 bytes, complete, do `Range: bytes=0-4095`, verify `spool.full_object_read_at.load() == 0`.
  - [x] `test_two_ranges_covering_full_object_sets_flag` — write 8192 bytes, complete, do `bytes=0-4095` then `bytes=4096-8191`, verify `full_object_read_at > 0` after second read.
  - [x] `test_out_of_order_ranges_set_flag_on_completion` — write 8192 bytes, complete, do `bytes=4096-8191` then `bytes=0-4095`, verify `full_object_read_at > 0` only after second read completes.
  - [x] `test_overlapping_ranges_do_not_double_count` — write 4096 bytes, complete, do `bytes=0-3000` then `bytes=2000-4095`, verify `full_object_read_at > 0` (overlapping is fine).
  - [x] `test_follow_read_sets_full_object_read_at` — write 4096 bytes, complete, do follow GET (no Range header), consume full body, verify `full_object_read_at > 0`.
  - [x] `test_last_read_activity_updated_on_chunk_yield` — write 4096 bytes, complete, do range read, verify `spool.last_read_activity_at.load() > 0`.
  - [x] `test_short_ttl_cleanup_fires_after_full_read` — use `tokio::time::pause()`. Write/complete 4096-byte spool (config: `full_read_complete_ttl_secs: 2`, `read_idle_ttl_secs: 3600`). Full range read. Start cleanup task. Advance 3 secs. Verify spool deleted.
  - [x] `test_idle_ttl_fires_without_reads` — use `tokio::time::pause()`. Write/complete spool (config: `read_idle_ttl_secs: 2`). Do NOT read. Start cleanup task. Advance 3 secs. Verify spool deleted.
  - [x] `test_idle_ttl_refreshed_prevents_deletion` — use `tokio::time::pause()`. Write/complete. Config `read_idle_ttl_secs: 5`. Start cleanup. Advance 3 secs. Do range read. Advance 3 more secs. Verify spool NOT deleted (last activity was only 3 secs ago). Advance 3 more secs. Verify DELETED.

  **Files**: `src/http/mod.rs`

  **Acceptance**: `cargo test http::tests` all green with `tokio::time::pause()` determinism.

---

- [x] 13. Integration tests (in `tests/integration.rs`)
  **What**: Add the following tests to `tests/integration.rs`. These use real TCP + reqwest. For TTL-sensitive tests, configure the server with very short TTLs (1–2 s) and use `tokio::time::sleep`. Also add a helper `start_server_with_config(config)` that accepts a `Config` to allow per-test TTL overrides without rewriting `start_server()`.

  ```rust
  async fn start_server_with_config(config: Arc<Config>) -> TestServer { ... }
  ```

  Tests:
  - [x] `test_multiple_ranges_covering_full_object_detected` — write 8192 bytes, complete, do `bytes=0-4095` then `bytes=4096-8191`; then use a very short `full_read_complete_ttl_secs=1` config, wait 2s, verify spool returns 404.
  - [x] `test_partial_range_does_not_trigger_short_ttl` — write 8192 bytes, complete, do `bytes=0-4095` only; `full_read_complete_ttl_secs=1`, `read_idle_ttl_secs=60`; wait 2s; verify spool still returns 206 (not deleted by short TTL; idle TTL hasn't fired).
  - [x] `test_never_read_spool_cleaned_up_after_idle_ttl` — write/complete, `read_idle_ttl_secs=1`, no reads; wait 2s; 404.
  - [x] `test_idle_ttl_not_anchored_on_created_at` — write, then sleep 2s during writing (simulating slow write), complete, then immediately read; verify spool is NOT deleted even though `created_at` was 2s ago (demonstrates timer starts from `readable_at`, not `created_at`).
  - [x] `test_slow_reader_receiving_bytes_not_cleaned_up` — write 8192 bytes, complete, config `read_idle_ttl_secs=2`; do a follow-read (no Range header); in parallel, advance time by 1.5s between chunk pulls by using `tokio::time::sleep` inside the read loop; verify the spool is NOT deleted while chunks are actively being served.

  **Files**: `tests/integration.rs`

  **Acceptance**: `cargo test --test integration` all green.

---

## Missing-Ranges Algorithm — Annotated Pseudocode

For reference during implementation review:

```
State: BTreeMap<u64,u64> gaps  (sorted by gap_start, values are gap_end)

Example after initialize(100):
  gaps = { 0 → 100 }

After mark_served(20, 60):
  overlapping = [ (0, 100) ]   // gap [0,100) overlaps [20,60)
  remove (0,100)
  insert left remainder: (0, 20)   // gap_start=0 < start=20
  insert right remainder: (60, 100) // gap_end=100 > end=60
  gaps = { 0→20, 60→100 }

After mark_served(0, 20):
  overlapping = [ (0,20) ]
  remove (0,20); no left or right remainder
  gaps = { 60→100 }

After mark_served(60, 100):
  overlapping = [ (60,100) ]
  remove (60,100)
  gaps = {}   → is_complete() = true ✓

Fragmentation check:
  After each _apply(), if len(gaps) > cap → set capped=true, clear gaps.
  is_complete() returns false when capped, preventing premature short TTL.
  Safe false negative: idle TTL still cleans up after read_idle_ttl_secs.
```

---

## Implementation Order and Dependencies

```
1. coverage.rs (no deps)
          ↓
2. types.rs (readable_at field) ← needed by lifecycle, manager
          ↓
3. config.rs (new fields) ← needed by cleanup, http tests
          ↓
4. spool/mod.rs (new Spool fields) ← depends on coverage.rs
          ↓
5. lifecycle.rs (readable_at + initialize) ← depends on 2, 4
          ↓
6. manager.rs (init + recovery) ← depends on 2, 4, 5
          ↓
7. http/mod.rs (read-path instrumentation) ← depends on 3, 4
          ↓
8. cleanup.rs (new rules) ← depends on 3, 4
          ↓
9. chart/ ← depends on 3
         ↓
10-13. Tests ← interleaved with each step above
```

Steps 1–3 are independent and can be drafted in parallel.

---

## Potential Pitfalls

**P1 — `readable_at = None` for old spools after upgrade**
Old redb JSON has no `readable_at` key. `serde(default)` gives `None`. Without the `recover()` backfill (TODO 6), cleanup would use `readable_at.unwrap_or(now)` and grant a full fresh idle TTL — safe but the spool becomes "sticky" until next restart. The `recover()` backfill fixes this correctly by writing a current timestamp and setting `metadata_corrected = true` to trigger re-persist.

**P2 — `missing_ranges` lost on restart = safe false negative**
After a restart, `full_object_read_at == 0` and `missing_ranges` starts empty (then `initialize()` sets it to `[0, total_size)`). The short TTL won't fire until bytes are re-served. Idle TTL still works. This is correct behavior per constraints.

**P3 — stalled connections now expire**
Removing `reader_count > 0` as an absolute guard means that an open connection with no bytes served will eventually get its spool deleted. `delete_spool()` calls `spool.cancel.cancel()`, which causes `read_page()` to return `Err(SpoolNotFound)`, which propagates as a stream error to axum, which closes the connection. Callers will see a connection reset. This is intentional and acceptable. Document in DESIGN.md / README.

**P4 — lock per chunk in hot path**
Two mutex acquires per chunk: `missing_ranges` + `metadata`. Both are uncontested in the single-reader case and are tokio async mutexes (cooperative, not OS mutexes). Profiling should confirm this is not a bottleneck; add a comment flagging batch-update as a future optimization if needed.

**P5 — follow reads and initialization timing**
A follow reader serves bytes while the spool is still `Writing`. `missing_ranges.total_size` is `None`. Served ranges go to `pending`. When `complete()` fires, `initialize(total_bytes_written)` drains `pending`. If the reader has already served all bytes (buffered in `pending`), `is_complete()` returns true immediately. If the reader finishes after `complete()`, `mark_served` applies directly to `gaps`. Both paths are correct.

**P6 — fragmentation from streaming clients**
Video/media clients often issue hundreds of small Range requests in sequence. These will be sequential (each picks up where the last left off), so they'll coalesce into a single gap that shrinks monotonically. Fragmentation only occurs with random-access patterns. The cap of 1024 is very generous for any realistic client.

**P7 — `Ordering::Relaxed` vs `SeqCst`**
`last_read_activity_at` uses `Relaxed` for stores (cleanup reads it with sufficient staleness tolerance). `full_object_read_at` `compare_exchange` uses `SeqCst` because multiple concurrent readers may all detect `is_complete()` simultaneously and we want exactly one winner. The store value doesn't need to be precisely `now` — any nonzero value suffices.

**P8 — `metadata.last_read_at` still being set**
`last_read_at` in `SpoolMetadata` is kept updated for observability but is no longer read by the cleanup loop. A future PR can remove it. Keep the update to avoid surprising log gaps.

---

## Verification

- [x] `cargo fmt --all -- --check` — no formatting diffs.
- [x] `cargo test` — zero failures, zero panics. Count: existing ~70 tests + ~40 new tests.
- [x] `cargo clippy -- -D warnings` — no warnings.
- [x] `cargo test spool::coverage` — all MissingRanges unit tests pass deterministically.
- [x] `cargo test cleanup` — new cleanup semantics confirmed by time-paused unit tests.
- [x] `cargo test http::tests` — full-read detection and TTL tests pass.
- [x] `cargo test --test integration` — integration behavioral tests pass.
- [x] Helm template renders new keys: `helm template chart/ --set config.host_prefix=x --set config.domain=y --set config.route_name=z | grep read_idle_ttl`.
- [ ] Manual smoke: deploy to a test cluster, write a 1 MB spool, verify it is deleted ~30 s after a complete GET read (with `full_read_complete_ttl_secs: 30`).
- [ ] Manual smoke: write a spool but do not read it; verify it is deleted after `read_idle_ttl_secs` from completion time (not from creation time).

---

## Rollout Notes

- **Schema change**: `readable_at` is additive and `serde(default)` — safe rolling restart. Old BOBS pods writing metadata without `readable_at` will have it backfilled on recovery by new pods. No migration script needed.
- **Config change**: Old deployments without `read_idle_ttl_secs`/`full_read_complete_ttl_secs` in their Helm values will use Rust defaults (600s / 30s). Old keys `reader_done_ttl_secs` / `unread_ttl_secs` are still accepted but ignored by cleanup. No operator action required on upgrade unless they want to tune the new TTLs.
- **Behavior change**: Any stalled reader connections (open but serving no bytes) that previously kept spools alive indefinitely will now be subject to the idle TTL. Clients that genuinely need > 600 s of idle time between reads will see their spool deleted. This is intentional and correct; document in release notes.
