# Observability Logging

## TL;DR
> **Summary**: Implement Phase 4 structured JSON logging for BOBS with a local `src/observability.rs` module, wire it into startup and BOBS HTTP/spool lifecycle events, propagate `job.id` only from strictly validated `X-Polytope-Job-Id`, and pin the conventions with per-package tests.
> **Estimated Effort**: Medium

## Context
### Original Request
Plan implementation of structured logging in BOBS for Phase 4 of `/home/james/work/code/polytope-config/docs/observability-plan.md`, following `/home/james/work/code/polytope-config/docs/observability.md`. BOBS must reimplement observability locally, not depend on a shared polytope-server crate. Metrics and cross-repo integration tests are out of scope.

### Key Findings
- BOBS currently initialises `tracing_subscriber::fmt()` directly in `src/main.rs` with a simple `EnvFilter::try_from_default_env().unwrap_or_else(...)`; empty `RUST_LOG` currently parses as an empty filter instead of the required `info` fallback.
- `Cargo.toml` already has `tracing`, `tracing-subscriber` with `env-filter`, and `serde_json`; it lacks a timestamp formatting dependency and a regex/redaction helper dependency.
- `src/lib.rs` exports modules but does not yet expose `observability`.
- `src/main.rs` has unstructured startup/shutdown lines, including `tracing::info!("listening on {}", addr)` and `"shutdown signal received..."`.
- `src/http/mod.rs` owns the create/write/complete/read/delete handlers and currently logs unstructured request/lifecycle messages at `INFO`. Handlers do not extract `X-Polytope-Job-Id` except for existing Range parsing on reads.
- The `/api/v1/read/{key}` endpoint is ingress/end-user-reachable; `X-Polytope-Job-Id` must therefore be treated as untrusted input and strictly validated before it can become `attributes.job.id`.
- The authoritative BITS job ID grammar comes from `bits/src/request_id.rs`: lowercase Crockford base32, exactly 26 characters, alphabet `0-9a-hjkmnp-tv-z` excluding `i`, `l`, `o`, and `u`. BOBS must reimplement only this minimal validator locally with no `bits` crate dependency.
- `src/manager.rs` owns delete and recovery paths. It currently logs deletion, recovery scan/load, corrupt metadata, missing files, and orphan directory cleanup with ad-hoc messages.
- `src/cleanup.rs` owns TTL cleanup sweeps and calls `manager.delete_spool(&key)` without a deletion reason; this needs reason-aware deletion to emit `bobs.spool.deleted` with `reason = "ttl"`.
- `src/spool/*.rs` currently has no `tracing::` calls, so the required replacement work there is an audit/no-op unless implementation introduces new low-level DEBUG logs.
- Existing HTTP tests already use in-process Axum routers and `tower::ServiceExt`; `src/http/mod.rs` has helper patterns for create/write/complete/read/delete that `bobs/tests/observability.rs` can mirror.
- Known test credential values found under `polytope-config/location/*/config.yaml` and needing explicit redaction coverage include `32eff194-66bd`, `lAYFsKT9xYeraMbeH2Sn4RPL7iJgNaxY`, `vv7pGSEZcFFB87`, `BaThQ7cKxG5NuJ`, `WQrRuQn4fvssgGYCiZTt`, `POLY-4a7bb966e4a51b9c25b429bc96cf25dd`, and `3..izBAd75`.

## Objectives
### Core Objective
Make BOBS emit OTel-shaped, redacted, structured JSON logs using the locked Polytope event taxonomy, with `job.id` propagated only from strictly validated worker-supplied HTTP headers and verified by local tests.

### Deliverables
- [ ] New local observability module at `src/observability.rs` with production initialisation, test capture, formatter, redaction, resource fields, robust `RUST_LOG` parsing, and assertion helpers.
- [ ] `src/main.rs` initialises the new subscriber and emits structured startup/shutdown events.
- [ ] `src/http/mod.rs` extracts `X-Polytope-Job-Id`, attaches it to per-request spans, and emits locked BOBS spool events with required attributes.
- [ ] `src/manager.rs` and `src/cleanup.rs` emit structured recovery, cleanup, and deletion events with deletion reasons.
- [ ] Existing chatty request/path internals are demoted to `DEBUG` or replaced by boundary `INFO` events.
- [ ] Unit and integration tests under `src/observability.rs` and `tests/observability.rs` pin JSON shape, event names, required fields, job header propagation, env filter fallback, and redaction.

### Definition of Done
- [ ] `cargo fmt --check` passes.
- [ ] `cargo test observability` passes.
- [ ] `cargo test --test observability` passes.
- [ ] `cargo test` passes.
- [ ] Manual stdout inspection from `cargo run` or a focused binary smoke shows JSON lines with top-level `timestamp`, `severityText`, `severityNumber`, `body`, `resource`, and `attributes`.

### Guardrails (Must NOT)
- [ ] Do not introduce a dependency on polytope-server or any cross-repo/shared observability crate.
- [ ] Do not implement metrics.
- [ ] Do not add cross-repo integration tests.
- [ ] Do not change the locked event-name taxonomy.
- [ ] Do not reject requests solely because `X-Polytope-Job-Id` is absent or invalid; tolerate missing/invalid headers for rolling-deploy back-compat.

## Notes / Follow-up
- The `/api/v1/read/{key}` endpoint is ingress/end-user-reachable, unlike worker-only create/write/complete/delete; strict 64-character cap plus BITS grammar validation of `X-Polytope-Job-Id` is the mitigation against forged or bloated correlation values.
- Header absence is tolerated only for rolling-deploy back-compat. After polytope-server Phase 3 has shipped `X-Polytope-Job-Id`-setting workers everywhere, create a follow-up task to add `tracing::warn!` with `event_name = "bobs.spool.access.missing_job_id"` or similar when the header is absent on worker-only create/write/complete/delete; do not warn for read because end users legitimately omit the header.

## TODOs

- [ ] 1. Add observability dependencies and module export
  **What**: Add only the dependencies needed by the local implementation, then expose the new module from the library. Prefer small direct dependencies: `time = { version = "0.3", features = ["formatting"] }` for RFC3339 UTC timestamps and `regex = "1"` for redaction patterns. Use `std::sync::OnceLock` instead of adding `once_cell` unless implementation proves otherwise.
  **Files**: `Cargo.toml`, `src/lib.rs`
  **Acceptance**: `cargo check` sees `bobs::observability`; no dependency points at `/home/james/work/code/polytope-server` or a git/path shared observability crate.

- [ ] 2. Implement `src/observability.rs` public API
  **What**: Create the local module with production and test APIs:
  - `pub fn init_tracing(service_name: &'static str)` for production, used as `init_tracing("bobs")`.
  - A writer-based builder for tests, e.g. `pub fn subscriber_with_writer<W>(service_name: &'static str, writer: W) -> impl Subscriber` or `Dispatch`.
  - A capturing helper, e.g. `pub fn capturing_subscriber(service_name: &'static str) -> (test_helper::CapturedLogs, Dispatch)`.
  - `pub mod test_helper` containing `CapturedLogs`, `MakeWriter`, `json_lines()`, raw line access, and assertions such as `assert_required_fields`, `assert_event_emitted`, `assert_event_attribute`, `assert_no_substring`, and `events_named`.
  **Files**: `src/observability.rs`
  **Acceptance**: Unit tests can install a local dispatch without touching the process-global subscriber; production can call `init_tracing("bobs")` exactly once.

- [ ] 3. Implement OTel JSON formatter and span field capture
  **What**: Implement an Aviso-style formatter that writes one JSON object per event with:
  - top-level `timestamp` in UTC RFC3339, `severityText`, `severityNumber` using TRACE=1, DEBUG=5, INFO=9, WARN=13, ERROR=17, and string `body` from the `message` field;
  - `resource.service.name`, `resource.service.version = env!("CARGO_PKG_VERSION")`, `resource.k8s.namespace.name`, `resource.k8s.pod.name`, and `resource.deployment.environment` from env vars;
  - `attributes.code.target` from `event.metadata().target()`;
  - all event fields except `message` under `attributes`;
  - current span fields merged into `attributes` so per-request span fields like `job.id` and `bobs.spool.key` appear on child events.
  Implement custom span field storage rather than relying on formatted span strings; event fields should override span fields if both are present. Do not synthesize `event_name` for events that did not provide one.
  **Files**: `src/observability.rs`
  **Acceptance**: A unit test emitting an event inside a span produces valid JSON with span attributes, event attributes, required resource fields, and no fallback `event_name` on an unnamed internal event.

- [ ] 4. Implement robust `RUST_LOG` parsing
  **What**: Add a small helper such as `env_filter_from_env()` that reads `RUST_LOG`, trims it, and returns `EnvFilter::new("info")` when unset, empty, or unparseable. Optionally include conservative dependency mutes only if they do not hide BOBS application logs.
  **Files**: `src/observability.rs`, `src/main.rs`
  **Acceptance**: Unit tests cover unset, empty string, valid directive, and invalid directive; invalid/empty values do not panic and still allow INFO events.

- [ ] 5. Implement full redaction in the formatter visitor
  **What**: Redact before serialising both structured string fields and free-form message/body strings. Cover the full conventions list:
  - HTTP `Authorization` header values by key name, regardless of value form;
  - `Bearer <token>` anywhere;
  - assignment-style `password=...`, `token=...`, `api_key=...` values;
  - JWT-shaped values beginning with `eyJ` and having three dot-separated base64url-ish segments;
  - URL userinfo such as `https://user:pass@example.com/path`;
  - known config/test credentials: `32eff194-66bd`, `lAYFsKT9xYeraMbeH2Sn4RPL7iJgNaxY`, `vv7pGSEZcFFB87`, `BaThQ7cKxG5NuJ`, `WQrRuQn4fvssgGYCiZTt`, `POLY-4a7bb966e4a51b9c25b429bc96cf25dd`, `3..izBAd75`.
  Replace secret values with `[REDACTED]` and preserve non-secret surrounding context where practical.
  **Files**: `src/observability.rs`
  **Acceptance**: Unit tests exercise every redaction category as both a message and/or a structured field; captured raw lines do not contain the probe secrets.

- [ ] 6. Wire production initialisation and startup/shutdown events
  **What**: Replace the direct `tracing_subscriber::fmt()` setup in `main` with `bobs::observability::init_tracing("bobs")`. Replace ad-hoc startup/shutdown logs with structured events:
  - `startup.config.loaded` after config validation, with config fields and `outcome = "success"`;
  - `startup.config.failed` when config load/validation fails, with `outcome = "error"` and `error`;
  - `startup.server.listening` after bind, with `addr`, `host`, `port`, `outcome = "success"`;
  - `startup.shutdown.received` when the shutdown signal branch fires;
  - `startup.shutdown.complete` after listener/app cleanup and io_uring shutdown handling completes.
  Leave low-level connection warnings and signal-installation errors as internal logs unless a locked event name applies.
  **Files**: `src/main.rs`, `src/shutdown.rs` if needed only for level/message cleanup
  **Acceptance**: Grep no longer finds `tracing_subscriber::fmt()` or `tracing::info!("listening on` in `src/main.rs`; startup events include `event_name` and required JSON fields.

- [ ] 7. Add strict job-id extraction and request spans in HTTP handlers
  **What**: Add a local header helper in `src/http/mod.rs` for `X-Polytope-Job-Id` (case-insensitive via `HeaderMap::get`) and apply it uniformly to create/write/complete/read/delete. The helper must reimplement only the minimal BITS request-id grammar inline: first cap accepted header text at 64 characters maximum, then accept only lowercase Crockford base32 values matching regex `^[0-9a-hjkmnp-tv-z]{26}$` (exactly 26 characters; excludes `i`, `l`, `o`, and `u`). Do not add a `bits` dependency. Reject missing, empty, non-ASCII/invalid `HeaderValue::to_str()`, too-long, wrong-length, or forbidden-character values by returning no job id; invalid values must be dropped and must never be echoed in `attributes.job.id`, log bodies, or error fields. Modify handler signatures to accept `HeaderMap` and instrument each handler body with a per-request span carrying `job.id` only when the helper returns a valid value, plus request-specific fields such as `bobs.spool.key`, `offset`, and `range`.
  Avoid holding `span.enter()` guards across `.await`; use `tracing::Instrument` or an inner async function pattern.
  **Files**: `src/http/mod.rs`
  **Acceptance**: A test request with valid `X-Polytope-Job-Id: 0123456789abcdefghjkmnpqrs` produces BOBS events whose `attributes.job.id` is that exact value; requests with the header absent or invalid still succeed but omit `attributes.job.id` on resulting BOBS events.

- [ ] 8. Replace HTTP spool logs with locked structured events
  **What**: Replace current ad-hoc HTTP `INFO` logs with boundary events:
  - `bobs.spool.created` after successful create, with `job.id` if known, `bobs.spool.key`, `content_type`, `content_encoding`, `write_locked`, `outcome = "success"`;
  - `bobs.spool.write.completed` after a write request fully succeeds, with `job.id`, `bobs.spool.key`, `offset`, `bytes`, and `outcome = "success"`;
  - `bobs.spool.completed` after successful complete, with `job.id`, `bobs.spool.key`, `expected_size`, total bytes if cheaply available, checksum if already available, and `outcome = "success"`;
  - `bobs.spool.read.started` before a read starts serving/long-polling, with `job.id`, `bobs.spool.key`, `range`, `start`, `end`, and `follow`;
  - `bobs.spool.read.timeout` when the first-page long-poll timeout returns the redirect, with `outcome = "error"` or `outcome = "client_gone"` only if that is semantically justified; otherwise use `outcome = "error"` consistently;
  - `bobs.spool.read.completed` from the response stream when it finishes, with `bytes`, `range`, and `outcome = "success"` or `"error"` on yielded stream errors;
  - explicit delete should call the reason-aware manager deletion path and emit/propagate `bobs.spool.deleted` with `reason = "explicit"`.
  For handler errors before a success event, emit one boundary `WARN`/`ERROR` with the same locked event name where useful and `outcome = "error"`; avoid double-logging the same failure at multiple layers.
  **Files**: `src/http/mod.rs`
  **Acceptance**: `rg 'create spool request|write spool request|spool created|spool completed|read spool request|delete spool request' src/http/mod.rs` is clean or only appears in tests/comments; observability tests see all expected success event names.

- [ ] 9. Make manager deletion reason-aware and structured
  **What**: Introduce a small deletion reason representation, e.g. `enum DeleteReason { Explicit, Ttl, Orphan, Corrupt }` with string output `explicit | ttl | orphan | corrupt`. Replace or wrap `delete_spool(&self, key)` with `delete_spool_with_reason(&self, key, reason, job_id)` while preserving a compatibility wrapper if many tests call `delete_spool` directly. Emit `bobs.spool.deleted` once per successful deletion with `bobs.spool.key`, `reason`, optional `job.id`, and `outcome = "success"`; emit one error event on failure with `outcome = "error"`.
  **Files**: `src/manager.rs`, `src/http/mod.rs`, `src/cleanup.rs`, affected tests in `src/**` if signatures change
  **Acceptance**: Explicit HTTP delete logs `reason = "explicit"` and cleanup delete logs `reason = "ttl"`; no duplicate successful delete events are emitted for one delete.

- [ ] 10. Structure recovery logging
  **What**: Replace recovery scan/load `INFO` lines with a single terminal `bobs.recovery.completed` event after recovery finishes, carrying `recovered`, `stale`, `orphan_deleted`, `corrupt_deleted`/similar counts, `duration_ms`, and `outcome = "success"`. Keep recoverable per-spool anomalies as `WARN`/`DEBUG` internal logs or map actual removals to `bobs.spool.deleted` with `reason = "corrupt"` for corrupt/stale metadata/data and `reason = "orphan"` for orphan directories. Do not introduce event names outside the locked taxonomy.
  **Files**: `src/manager.rs`
  **Acceptance**: Recovery success emits `bobs.recovery.completed`; orphan/corrupt removals emit `bobs.spool.deleted` with the appropriate reason.

- [ ] 11. Structure cleanup sweep logging
  **What**: Add cleanup sweep boundary events:
  - `bobs.cleanup.run.started` at the start of each interval tick, with current spool count if cheap;
  - `bobs.cleanup.run.completed` at the end, with inspected count, deleted count, failed delete count, `duration_ms`, and `outcome = "success"` unless delete failures occurred.
  Use `delete_spool_with_reason(..., DeleteReason::Ttl, None)` for TTL-triggered deletions. Keep per-delete failures at `DEBUG` or one structured warning to avoid log storms.
  **Files**: `src/cleanup.rs`, `src/manager.rs`
  **Acceptance**: A cleanup test can trigger a sweep and capture started/completed events plus TTL deletion reason without changing cleanup semantics.

- [ ] 12. Audit `src/spool/*.rs` and chatty internals for level discipline
  **What**: Confirm there are no existing `tracing::` calls in `src/spool/*.rs`. If implementation adds low-level page/cache/read/write diagnostics, keep them at `DEBUG`, never `INFO`. Review remaining `INFO` in `src/main.rs`, `src/http/mod.rs`, `src/manager.rs`, and `src/cleanup.rs`; only lifecycle/boundary events should remain at `INFO`.
  **Files**: `src/spool/*.rs`, `src/main.rs`, `src/http/mod.rs`, `src/manager.rs`, `src/cleanup.rs`
  **Acceptance**: `rg 'tracing::info!' src` shows only locked lifecycle/boundary events or clearly justified startup events.

- [ ] 13. Add `src/observability.rs` unit tests
  **What**: Add unit tests inside the module for formatter shape, severity mapping, resource env fields, span field merging, unnamed internal event behaviour, every redaction category, and `RUST_LOG` fallback. For env-var tests, guard mutations with a static mutex to avoid parallel-test races.
  **Files**: `src/observability.rs`
  **Acceptance**: `cargo test observability::` passes reliably with default parallel test execution.

- [ ] 14. Add in-process HTTP observability integration test
  **What**: Create `tests/observability.rs`. Build an isolated `AppState` with `DefaultFileIO`/`DefaultMetadataStore`, `tempfile`, and `router::<DefaultFileIO, DefaultMetadataStore>().with_state(...)`. Install the capturing subscriber using a current-thread Tokio test to keep thread-local dispatch stable. Drive `create → write → complete → read → delete` with `tower::ServiceExt`, carrying valid `X-Polytope-Job-Id: 0123456789abcdefghjkmnpqrs` on every request and draining the read body so `bobs.spool.read.completed` fires.
  **Files**: `tests/observability.rs`
  **Acceptance**: The test asserts required fields on every captured JSON line and finds `bobs.spool.created`, `bobs.spool.write.completed`, `bobs.spool.completed`, `bobs.spool.read.started`, `bobs.spool.read.completed`, and `bobs.spool.deleted` with `job.id = "0123456789abcdefghjkmnpqrs"`, the expected `bobs.spool.key`, byte/range fields, `outcome`, and `reason = "explicit"` on delete.

- [ ] 15. Add back-compat, header-validation, and redaction integration tests
  **What**: In `tests/observability.rs`, add a no-header flow that proves create/write/complete/read/delete still succeeds and emitted events omit `job.id`. Add focused read-endpoint header probes after creating/completing a spool: a 10 KiB junk header value, a 27-character value using otherwise valid Crockford characters, a 26-character value containing forbidden Crockford letter `i`, and valid `0123456789abcdefghjkmnpqrs`. For each invalid probe, assert the resulting `bobs.spool.read.started` and `bobs.spool.read.completed` log lines omit `attributes.job.id`; for the valid probe, assert those read events include `job.id = "0123456789abcdefghjkmnpqrs"`. Add a redaction probe by sending a logged field such as create `content_type`/`content_encoding` containing `Bearer FAKETOKEN_OBSERVABILITY_PROBE`, `password=...`, or a known test credential, then assert no captured raw line contains the secret substring.
  **Files**: `tests/observability.rs`
  **Acceptance**: `cargo test --test observability` passes; invalid read header probes for 10 KiB junk, 27 characters, and forbidden `i` all produce `bobs.spool.read.*` events without `attributes.job.id`; the valid 26-character Crockford probe produces read events with the expected `job.id`; captured raw logs contain `[REDACTED]` and do not contain the probe token or known credential value.

- [ ] 16. Final grep and regression pass
  **What**: Run focused greps and the full test suite. Check for old unstructured messages, accidental secret literals in expected output, and accidental event-name drift.
  **Files**: Whole repo, especially `src/**/*.rs`, `tests/observability.rs`
  **Acceptance**: `rg 'listening on \{|create spool request|write spool request|read spool request|delete spool request|spool deleted \(' src` is clean or only finds non-log comments/tests; `cargo fmt --check && cargo test` passes.

## Verification
- [ ] `cargo fmt --check`
- [ ] `cargo test observability`
- [ ] `cargo test --test observability`
- [ ] `cargo test`
- [ ] `rg 'polytope-server.*observability|observability.*polytope-server' Cargo.toml src tests` finds no shared-crate dependency.
- [ ] `rg 'tracing_subscriber::fmt\(\)|listening on \{\}|create spool request|write spool request|read spool request|delete spool request' src` finds no old unstructured logging setup/messages.
- [ ] Captured test logs include required resource fields and attributes for the locked BOBS event names.
- [ ] Captured read-probe logs omit `attributes.job.id` for invalid `X-Polytope-Job-Id` values: 10 KiB junk, 27 characters, and forbidden `i`.
- [ ] Captured read-probe logs include `attributes.job.id = "0123456789abcdefghjkmnpqrs"` for the valid 26-character Crockford value.
- [ ] Captured test logs do not contain `FAKETOKEN_OBSERVABILITY_PROBE` or any known test credential value.
