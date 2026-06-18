# Standalone BOBS Benchmarks

## TL;DR
> **Summary**: Add a local-first standalone BOBS benchmark binary that drives independent object lifecycles directly against BOBS HTTP endpoints, then package the same binary into the existing container image for pod/container execution. The tool will report separate create/write/complete/read timings, aggregate throughput windows, percentiles, per-ordinal metrics, and grep-friendly logs.
> **Estimated Effort**: Medium

## Context
### Original Request
Plan implementation for standalone BOBS benchmarks to investigate low throughput in Polytope's in-cluster BOBS-backed download benchmark. The benchmark must run directly against BOBS, first locally against a locally started BOBS server, then from a container/pod against a BOBS endpoint. It should model independent users/objects starting from a shared barrier, with several writers and one complete-object reader per object, cheap deterministic byte generation, detailed timing/throughput reporting, grep-friendly logging, proper tests, and docs/example usage. Do not implement code.

### Key Findings
- BOBS is a Rust crate with one existing service binary (`src/main.rs`) and reusable library modules exposed from `src/lib.rs`.
- The service exposes `/api/v1/create`, `/api/v1/write/{key}/{offset}`, `/api/v1/complete/{key}`, `/api/v1/read/{key}`, and `/api/v1/delete/{key}` in `src/http/mod.rs`.
- Local service startup currently requires `HOSTNAME` with a pod-like ordinal and `BOBS_INTERNAL_BASE_URL_TEMPLATE`; `Config::validate()` requires `host_prefix`, `domain`, and `route_name`.
- Tests are conventional Rust unit tests plus `tests/integration.rs`; CI runs `cargo fmt --all -- --check` and `cargo test` from `.github/workflows/ci.yaml`.
- The Dockerfile currently copies only `/build/target/release/bobs`; a second benchmark binary will need to be copied into release/debug images.
- Existing docs are mdBook under `docs/src/`, with `docs/src/SUMMARY.md` as the navigation index. Some docs have stale non-`/api/v1` examples, so new benchmark docs should use current `/api/v1` semantics and avoid relying on those stale snippets.
- Nearby Polytope stress tooling (`polytope-config/tests/stress/bobs_download_runner.py`) uses `SUMMARY:{json}` lines, deterministic work assignment from a barrier, per-ordinal extraction from BOBS read URLs, and merged throughput/percentile reporting. Those patterns are useful, but this BOBS repo should keep the tool standalone and direct-to-BOBS.

## Objectives
### Core Objective
Introduce a standalone BOBS benchmark tool that directly exercises BOBS object create/write/complete/read lifecycles with many independent objects starting from a shared barrier, suitable for local runs and the same image running inside a container/pod.

### Deliverables
- [x] A new Rust benchmark binary, packaged alongside the existing `bobs` service binary.
- [x] A testable benchmark core module covering CLI parsing/validation, endpoint/ordinal parsing, scheduling, concurrency coordination, summary aggregation, and result rendering.
- [x] Local-first usage documentation, including exact commands to start a local BOBS server and run the benchmark.
- [x] Container/pod usage documentation, including endpoint-template guidance for multi-BOBS deployments.
- [x] Dockerfile updates so the release/debug images contain both `bobs` and the benchmark binary.

### Definition of Done
- [x] `cargo fmt --all -- --check` passes.
- [ ] `cargo test` passes.
- [x] `cargo run --release --bin bobs-benchmark -- --help` prints usage without starting the benchmark.
- [x] A local BOBS server can be started with a temporary config, and `cargo run --release --bin bobs-benchmark -- --base-url http://127.0.0.1:3000 --objects 8 --object-bytes 1048576 --write-body-chunk-bytes 1048576 --read-body-chunk-bytes 1048576 --start-delay-ms 2000` completes with all objects successful.
- [x] `docker build --target release -t bobs:bench .` produces an image containing both `/usr/local/bin/bobs` and `/usr/local/bin/bobs-benchmark`.
- [x] The same image can run `bobs-benchmark` in a container/pod against an HTTP BOBS endpoint and emit a `SUMMARY:{json}` line.

### Guardrails (Must NOT)
- [ ] Must not route through Polytope frontend, workers, broker, or any full-stack Polytope component.
- [ ] Must not add new dependencies unless the change explicitly calls out that approval is required first.
- [ ] Must not make readers/downloaders artificially scarce via a small lifecycle-slot pool; model one independent object task and one reader per object.
- [ ] Must not use CPU-heavy synthetic byte loops per byte; generate a reusable deterministic buffer cheaply and stream it repeatedly.
- [ ] Must not read full object bodies into memory; readers must discard streamed chunks as they arrive.
- [ ] Must not require HTTPS/TLS for the first implementation; if HTTPS is needed later, request approval for an HTTP client dependency or feature expansion.
- [ ] Must not use process-ephemeral wording such as “phase”, “step”, or “round” in new code/docs/task names.

## TODOs

- [x] 1. Add a benchmark core module
  **What**: Create a testable library module for all non-trivial benchmark logic. Keep the binary thin so parsing, endpoint normalization, scheduling, timing summaries, and rendering can be unit-tested without spawning a process.

  Proposed module shape:
  - `src/benchmark/mod.rs` exports submodules/types used by the binary.
  - `src/benchmark/config.rs` defines `BenchmarkConfig`, `EndpointSpec`, validated defaults, and `parse_args_from<I>()`.
  - `src/benchmark/schedule.rs` defines one `ObjectPlan` per object, deterministic endpoint selection, barrier timestamp calculation, and ordinal metadata.
  - `src/benchmark/summary.rs` defines per-object results, percentile helpers, throughput windows, per-ordinal aggregation, and `SUMMARY:{json}` rendering.
  - `src/benchmark/http_client.rs` contains the minimal HTTP client abstraction used by runtime code.
  - `src/benchmark/run.rs` coordinates async execution.

  Avoid new runtime dependencies. Use existing dependencies (`tokio`, `serde`, `serde_json`, `bytes`, `tracing`) and standard library code.

  **Files**: `src/lib.rs`; create `src/benchmark/mod.rs`, `src/benchmark/config.rs`, `src/benchmark/schedule.rs`, `src/benchmark/summary.rs`, `src/benchmark/http_client.rs`, `src/benchmark/run.rs`

  **Acceptance**: `cargo test benchmark` compiles and at least config/schedule/summary unit tests can run without a live BOBS server.

- [x] 2. Implement CLI parsing and validation without new dependencies
  **What**: Add manual CLI parsing in `BenchmarkConfig::parse_args_from<I>()` with clear errors and `--help` output. Keep arguments explicit and stable for local and container use.

  Proposed CLI options:
  - `--base-url http://host:port` for a single BOBS endpoint.
  - `--base-url-template http://host-{ordinal}:3000 --ordinals 0,1,2,3` for multi-BOBS deployments.
  - `--objects N` total independent objects/users.
  - `--object-bytes N` bytes per object.
  - `--write-body-chunk-bytes N` reusable client buffer chunk size for streaming request bodies.
  - `--read-body-chunk-bytes N` discard-buffer size for readers.
  - `--write-request-bytes N` optional split size for multiple write POSTs per object; default should be one write POST per object to minimize client-side overhead.
  - `--start-delay-ms N` barrier delay from process start.
  - `--delete-after-read` optional cleanup.
  - `--summary-json PATH` optional summary artifact.
  - `--results-jsonl PATH` optional per-object artifact.
  - `--log-chunks` optional per-write-request logging when split writes are used.
  - `--connect-timeout-ms`, `--request-timeout-ms` if implementing timeouts in the raw client.

  Validation rules:
  - exactly one of `--base-url` or `--base-url-template` must be provided;
  - template mode requires a non-empty `--ordinals` list and the literal `{ordinal}` placeholder;
  - URLs must be `http://` for the first implementation;
  - `objects`, `object-bytes`, `write-body-chunk-bytes`, and `read-body-chunk-bytes` must be greater than zero;
  - `write-request-bytes` must be greater than zero when provided;
  - reject unsupported schemes with an error explaining that HTTPS requires an approved dependency/feature change.

  **Files**: `src/benchmark/config.rs`; `src/bin/bobs-benchmark.rs`

  **Acceptance**: Unit tests cover valid single-endpoint args, valid template args, `--help`, missing endpoint, invalid scheme, zero sizes, missing ordinals, malformed ordinal lists, and template without `{ordinal}`.

- [x] 3. Add endpoint normalization and ordinal extraction
  **What**: Implement pure helpers to normalize BOBS endpoint URLs and extract per-BOBS ordinal when available.

  Details:
  - Normalize `http://host:port`, optional trailing slash, and optional `/api/v1` suffix into an internal endpoint with a root URL plus generated `/api/v1/...` paths.
  - Preserve the HTTP `Host` header value for raw HTTP requests.
  - In template mode, expand `{ordinal}` per object and store the configured ordinal on the object plan.
  - Parse ordinals from create response `read_url` path forms such as `/download-3/<key>` and legacy-like `/download-3/api/v1/read/<key>` when present.
  - Record both configured ordinal and response ordinal; warn in logs if they disagree.

  **Files**: `src/benchmark/config.rs`; `src/benchmark/schedule.rs`; `src/benchmark/summary.rs`

  **Acceptance**: Unit tests cover localhost URLs, URLs ending in `/api/v1`, trailing slashes, template expansion, response `read_url` ordinal extraction, no-ordinal URLs, and mismatch handling.

- [x] 4. Implement a streaming HTTP/1.1 client for direct BOBS calls
  **What**: Add a small HTTP client tailored to BOBS over plain HTTP so the benchmark does not require moving `reqwest` into runtime dependencies. The client should open one TCP connection per request with `Connection: close`, parse status/headers, and stream request/response bodies without accumulating large objects.

  Required operations:
  - `PUT /api/v1/create` with JSON body and small JSON response parsing (`key`, `read_url`, `write_url`).
  - `POST /api/v1/write/{key}/{offset}` with a known `Content-Length` and a repeated deterministic buffer written in chunks.
  - `POST /api/v1/complete/{key}` with `expected_size` JSON.
  - `GET /api/v1/read/{key}` with `Range: bytes=0-{object_bytes-1}` after completion, discarding response chunks and checking the byte count.
  - `DELETE /api/v1/delete/{key}` only when `--delete-after-read` is set.

  Implementation notes:
  - Generate deterministic bytes by filling one reusable buffer cheaply per object or per writer (for example `vec![object_index as u8; chunk_size]`) and repeatedly writing slices from it.
  - Do not compute checksums or validate byte contents in the benchmark path; BOBS already returns CRC headers and the benchmark objective is throughput.
  - Treat 200/201/206 as appropriate per endpoint and surface status/body snippets in errors.
  - Use bounded header/body reads for small JSON/error responses.
  - Explicitly document that this first version supports internal/plain HTTP endpoints, not TLS.

  **Files**: `src/benchmark/http_client.rs`; `src/benchmark/run.rs`

  **Acceptance**: Unit tests cover response parsing, header lookup, content-length handling, status validation, and bounded error-body capture. A live-server smoke test can create/write/complete/read a tiny object without buffering the read body.

- [x] 5. Coordinate independent object lifecycles from a shared barrier
  **What**: Implement runtime coordination so every object has its own async task and starts from the same barrier timestamp. Each object should create its spool, stream writes as fast as possible, complete it, then have its dedicated reader read the complete object as fast as possible.

  Required behavior:
  - Build all `ObjectPlan`s before the barrier; do not recycle a small set of lifecycle slots.
  - At barrier release, spawn/activate all object tasks; each object independently runs create → write → complete → read.
  - Represent the reader as a per-object reader future that waits for that object’s completion signal, so the model is one reader per object even though the HTTP GET starts after completion.
  - Use `tokio::time::Instant` for durations and wall-clock timestamps for log correlation.
  - Ensure failed create/write/complete prevents that object’s read but does not abort unrelated objects.
  - Keep object result ordering deterministic in the final summary by sorting on object index.

  **Files**: `src/benchmark/run.rs`; `src/benchmark/schedule.rs`; `src/benchmark/summary.rs`

  **Acceptance**: Scheduling/concurrency tests prove there is exactly one plan/result slot per object, endpoint assignment is deterministic, barrier timestamps are shared, and failures for one object are represented without dropping other object results.

- [x] 6. Add grep-friendly structured logging
  **What**: Emit line-oriented logs that make it easy to trace one object/key across benchmark writer logs, BOBS service logs, and benchmark reader logs.

  Required log fields:
  - `object=<zero-padded-index>` on every line.
  - `key=<uuid>` after create succeeds; `key=-` before it is known.
  - `ordinal=<n|unknown>` from configured endpoint or `read_url` parsing.
  - `event=create_start|create_end|write_start|write_end|complete_start|complete_end|read_start|read_end|delete_start|delete_end|object_error`.
  - `offset=<n>` and `bytes=<n>` for write request logging when split writes or `--log-chunks` is enabled.
  - `status=<http-status>`, `duration_ms=<n>`, and `unix_ms=<n>` where applicable.

  Use `println!`/stdout for benchmark event lines and preserve existing BOBS server `tracing` behavior. Emit one final `SUMMARY:{json}` line for easy parsing by shell, CI, or Kubernetes log collection.

  **Files**: `src/benchmark/run.rs`; `src/benchmark/summary.rs`; `src/bin/bobs-benchmark.rs`

  **Acceptance**: A smoke run log can be grepped by object index and key to show create/write/complete/read timings; tests can assert key event names and required fields in rendered log lines.

- [x] 7. Report separate timings, throughput windows, percentiles, and per-ordinal metrics
  **What**: Implement summary aggregation over per-object results with metrics requested for diagnosis.

  Per-object recorded timings:
  - create duration;
  - write duration covering all write POSTs for the object;
  - complete duration;
  - wait-to-read duration, defined as read start minus complete end;
  - read duration;
  - lifecycle duration, defined as object active start to read end or terminal error;
  - bytes written/read;
  - endpoint URL, configured ordinal, response ordinal, key, outcome, and error string.

  Aggregate metrics:
  - total objects, successes, failures, total bytes written/read;
  - wall duration and wall throughput (`successful read bytes / full benchmark wall window`);
  - write-active throughput (`successful written bytes / [max(write_end) - min(write_start)]`);
  - read-active throughput (`successful read bytes / [max(read_end) - min(read_start)]`);
  - optional read MiB per reader-second (`read bytes / sum(read durations)`) to detect reader-side bottlenecks;
  - p50/p95/max for create, write, complete, wait-to-read, read, and lifecycle durations;
  - per-BOBS ordinal counts, bytes written/read, write/read active windows, and failures.

  Render both human-readable text and a machine-readable JSON object. Prefix the machine-readable line with `SUMMARY:` to match the nearby Polytope stress-runner convention.

  **Files**: `src/benchmark/summary.rs`; `src/benchmark/run.rs`

  **Acceptance**: Unit tests cover percentile calculation, empty-value handling, active-window throughput, wall throughput, per-ordinal grouping, failed-object exclusion from byte throughput, and stable JSON fields.

- [x] 8. Add the benchmark binary entry point
  **What**: Create `src/bin/bobs-benchmark.rs` as a thin entry point that initializes logging if needed, parses CLI args, prints usage/errors, calls the benchmark runner, writes optional artifacts, prints the final summary, and exits non-zero if any object failed.

  Behavior:
  - `--help` exits 0.
  - CLI validation errors print a concise message and exit 2.
  - Runtime benchmark failures for any object still emit `SUMMARY:{json}` and exit 1.
  - All-success runs emit `SUMMARY:{json}` and exit 0.

  **Files**: `src/bin/bobs-benchmark.rs`

  **Acceptance**: `cargo run --release --bin bobs-benchmark -- --help` works; unit tests cover parser behavior; a local smoke run exits 0 with all objects successful.

- [x] 9. Package the benchmark in the existing Docker image
  **What**: Update the Dockerfile to copy the benchmark binary into both release and debug images while keeping the service `CMD ["bobs"]` unchanged.

  Required changes:
  - Ensure the builder stage builds both binaries.
  - Copy `/build/target/release/bobs-benchmark` to `/usr/local/bin/bobs-benchmark` in `release` and `debug` stages.
  - Do not change the default service command.

  **Files**: `Dockerfile`

  **Acceptance**: `docker build --target release -t bobs:bench .` succeeds; `docker run --rm bobs:bench bobs-benchmark --help` prints benchmark usage; `docker run --rm bobs:bench bobs --help` behavior remains compatible with current service startup expectations.

- [x] 10. Add local-first and container/pod documentation
  **What**: Document exact usage in mdBook and add a README pointer if desired.

  Suggested docs content:
  - A local BOBS config example with `host: 127.0.0.1`, `port: 3000`, `data_dir: /tmp/bobs-bench-data`, `page_size: 1048576`, `max_cache_bytes: 268435456`, `host_prefix`, `domain`, and `route_name`.
  - Local service command:
    `HOSTNAME=bobs-0 BOBS_INTERNAL_BASE_URL_TEMPLATE=http://127.0.0.1:3000/api/v1 cargo run --release --bin bobs -- /tmp/bobs-bench.yaml`
  - Local benchmark command:
    `cargo run --release --bin bobs-benchmark -- --base-url http://127.0.0.1:3000 --objects 32 --object-bytes 16777216 --write-body-chunk-bytes 1048576 --read-body-chunk-bytes 1048576 --start-delay-ms 2000`
  - Container command using host networking for local Linux validation:
    `docker run --rm --network host bobs:bench bobs-benchmark --base-url http://127.0.0.1:3000 ...`
  - Kubernetes/pod command using one endpoint:
    `kubectl run bobs-benchmark --rm -i --restart=Never --image=<image> -- bobs-benchmark --base-url http://<release>-bobs-0:3000 ...`
  - Kubernetes/pod command using endpoint template:
    `kubectl run bobs-benchmark --rm -i --restart=Never --image=<image> -- bobs-benchmark --base-url-template http://<release>-bobs-{ordinal}:3000 --ordinals 0,1,2,3 ...`
  - Warning that an aggregate multi-pod service may route create/write/read to different pods; use per-pod services/templates for multi-BOBS runs unless the deployment guarantees routing affinity.
  - Explanation of summary fields and how to grep a key/object across benchmark and BOBS logs.
  - Note that the first implementation supports plain HTTP endpoints; HTTPS support requires an approved dependency or feature change.

  **Files**: create `docs/src/standalone-benchmark.md`; modify `docs/src/SUMMARY.md`; optionally modify `README.md` with a short link to the benchmark docs.

  **Acceptance**: `mdbook build docs` succeeds where mdBook is installed; docs include clearly separated local-first and container/pod validation sections.

- [x] 11. Add benchmark-focused tests
  **What**: Add unit and integration tests aligned with the requested reliability areas.

  Required unit tests:
  - CLI parsing and validation (`src/benchmark/config.rs`).
  - URL normalization and ordinal parsing (`src/benchmark/config.rs` or `src/benchmark/schedule.rs`).
  - Scheduling/concurrency model creates one object plan per object and deterministic endpoint assignment (`src/benchmark/schedule.rs`).
  - Summary merging, active-window throughput, percentiles, failed-result handling, per-ordinal aggregation, and stable JSON fields (`src/benchmark/summary.rs`).
  - HTTP response parser behavior without live network (`src/benchmark/http_client.rs`).

  Required live smoke test:
  - Start an in-process BOBS router using the existing `tests/integration.rs` pattern (`bobs::http::router`, `SpoolManager`, `TokioFileIO`, `TempDir`, `TcpListener`).
  - Run the benchmark core against it with a small workload, for example 4 objects × 64 KiB.
  - Assert all objects succeed, bytes written/read match expected totals, and the summary contains p50/p95/max plus ordinal metadata where available.

  Keep the smoke test modest so `cargo test` remains suitable for CI.

  **Files**: module-local unit tests under `src/benchmark/*.rs`; create `tests/standalone_benchmark.rs` if a separate integration test is clearer.

  **Acceptance**: `cargo test benchmark` and `cargo test standalone_benchmark` pass; `cargo test` remains stable and not materially slower.

- [x] 12. Validate local execution against a real local BOBS server
  **What**: After implementation, run an end-to-end local validation outside the test harness to exercise the actual service binary and actual benchmark binary.

  Suggested commands:
  ```bash
  cat >/tmp/bobs-bench.yaml <<'YAML'
  host: 127.0.0.1
  port: 3000
  data_dir: /tmp/bobs-bench-data
  page_size: 1048576
  max_cache_bytes: 268435456
  writer_inactivity_timeout_secs: 300
  read_idle_ttl_secs: 600
  full_read_complete_ttl_secs: 30
  cleanup_sweep_interval_secs: 30
  long_poll_timeout_ms: 25000
  host_prefix: bobs
  domain: local
  route_name: download
  YAML

  rm -rf /tmp/bobs-bench-data
  HOSTNAME=bobs-0 \
  BOBS_INTERNAL_BASE_URL_TEMPLATE=http://127.0.0.1:3000/api/v1 \
  cargo run --release --bin bobs -- /tmp/bobs-bench.yaml
  ```

  In another shell:
  ```bash
  cargo run --release --bin bobs-benchmark -- \
    --base-url http://127.0.0.1:3000 \
    --objects 8 \
    --object-bytes 1048576 \
    --write-body-chunk-bytes 1048576 \
    --read-body-chunk-bytes 1048576 \
    --start-delay-ms 2000
  ```

  **Files**: no source files beyond implementation; this is a validation activity.

  **Acceptance**: Benchmark exits 0, emits `SUMMARY:{json}`, reports 8 successes, and BOBS logs can be grepped by a reported key to find create/write/complete/read events.

- [ ] 13. Validate container and pod execution separately
  **What**: Verify the packaged binary runs in the same image locally and in a pod-like environment against an HTTP BOBS endpoint.

  Local container validation:
  ```bash
  docker build --target release -t bobs:bench .
  docker run --rm bobs:bench bobs-benchmark --help
  docker run --rm --network host bobs:bench bobs-benchmark \
    --base-url http://127.0.0.1:3000 \
    --objects 8 \
    --object-bytes 1048576 \
    --write-body-chunk-bytes 1048576 \
    --read-body-chunk-bytes 1048576 \
    --start-delay-ms 2000
  ```

  Pod validation, single endpoint:
  ```bash
  kubectl run bobs-benchmark --rm -i --restart=Never --image=<image> -- \
    bobs-benchmark \
    --base-url http://<release>-bobs-0:3000 \
    --objects 32 \
    --object-bytes 16777216 \
    --write-body-chunk-bytes 1048576 \
    --read-body-chunk-bytes 1048576 \
    --start-delay-ms 5000
  ```

  Pod validation, multiple endpoints:
  ```bash
  kubectl run bobs-benchmark --rm -i --restart=Never --image=<image> -- \
    bobs-benchmark \
    --base-url-template http://<release>-bobs-{ordinal}:3000 \
    --ordinals 0,1,2,3 \
    --objects 128 \
    --object-bytes 16777216 \
    --write-body-chunk-bytes 1048576 \
    --read-body-chunk-bytes 1048576 \
    --start-delay-ms 5000
  ```

  Adjust endpoint names to match the deployed chart; the BOBS chart also creates per-pod services named from `{{ include "bobs.fullname" . }}-<ordinal>`.

  **Files**: `Dockerfile`; `docs/src/standalone-benchmark.md`

  **Acceptance**: Container validation emits a successful `SUMMARY:{json}`; pod validation emits per-ordinal metrics when using endpoint-template mode.

## Verification
- [x] `cargo fmt --all -- --check`
- [ ] `cargo test`
- [x] `cargo test benchmark`
- [x] `cargo test standalone_benchmark`
- [x] `cargo run --release --bin bobs-benchmark -- --help`
- [x] Local-first validation: run the service binary with a temporary config, then run `bobs-benchmark` against `http://127.0.0.1:3000` and confirm all objects succeed.
- [x] Container validation: build the release image and run `bobs-benchmark --help` inside it.
- [x] Container-to-local validation: run the benchmark image against the local BOBS server and confirm `SUMMARY:{json}` reports all objects successful.
- [ ] Pod validation: run the image in Kubernetes against a single BOBS endpoint and confirm success.
- [ ] Pod template validation: run the image in Kubernetes with `--base-url-template` and confirm per-ordinal counts/bytes are reported.
- [x] `mdbook build docs` if mdBook is available.
- [x] No new dependencies are added; if implementation discovers HTTPS/TLS or a richer client is required, stop and request approval before changing dependencies or dependency features.
