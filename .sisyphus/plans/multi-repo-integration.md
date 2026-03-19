# BOBS Ecosystem Integration + Polytope Streaming Push

## TL;DR

> **Quick Summary**: Integrate BOBS into the polytope deployment stack (Dockerfile, skaffold, Helm, config), then build a streaming result delivery abstraction in polytope-server so workers can push results to BOBS or S3 instead of streaming through the broker. Add zstd/gzip encoding support for workers.
> 
> **Deliverables**:
> - BOBS: Dockerfile with release/debug targets, skaffold.yaml
> - polytope-chart: bobs as Helm dependency
> - polytope-config/majh-dev.yaml: bobs section
> - polytope-server workers/common: ResultDelivery trait (BOBSPush, S3Push), EncodingCodec (zstd, gzip)
> - polytope-server frontend: Accept-Encoding extraction, compression middleware for EDR/OpenMeteo
> - polytope-server workers: integrate delivery + encoding into fdb-worker and polytope-fe-worker
> 
> **Estimated Effort**: Large
> **Parallel Execution**: YES - 4 waves
> **Critical Path**: Wave 1 (infra) → Wave 2 (abstractions) → Wave 3 (worker integration) → Wave 4 (frontend middleware)

---

## Context

### Original Request
Multi-repo integration: BOBS Dockerfile/skaffold, Helm chart dependency in polytope-chart, config in polytope-config. Stream push abstraction in polytope-server with S3 and BOBS backends. Content encoding (zstd minimum). Accept-Encoding propagation from job submission. Worker redirect URLs. EDR/openmeteo encoding at routing level.

### Interview Summary
**Key Decisions**:
- Push abstraction lives in `workers/common` — workers own the delivery decision
- Push target configured per worker pool via delivery config (CLI arg `--delivery-config`)
- Workers handle encoding before pushing (not frontend, not streaming wrapper)
- BOBS already stores `content_type` and `content_encoding` and serves them on reads — redirect flow is clean
- `DirectStream` is the existing `Completion::Complete` behavior — no new implementation needed

**Research Findings**:
- Workers implement `Processor` trait, return `Completion` enum (Complete, Redirect, Reject, Error)
- Workers configured via CLI args (clap), NOT YAML — need `--delivery-config` for complex delivery settings
- `Completion::Redirect { location, message }` already exists; frontend returns HTTP 303
- No existing delivery or encoding abstraction
- BOBS write API: PUT /create → POST /write/{key}/{offset} → POST /complete/{key} → client reads GET /read/{key}
- polytope-chart has no dependencies section currently
- Skaffold: eccr.ecmwf.int/polytope/{name}, FIXED_TAG or PREFIX+GIT_TAG

### Metis Review
**Identified Gaps (addressed)**:
- Workers use CLI not YAML → solved with `--delivery-config path.yaml` CLI arg
- Encoding metadata in redirect flow → already solved: BOBS stores/serves content_encoding
- emptyDir persistence → acceptable for MVP; PVC noted as prod upgrade
- Double compression risk → frontend middleware must check Content-Encoding before compressing
- BOBS-down fallback → worker returns Completion::Error, job fails (explicit policy)

---

## Work Objectives

### Core Objective
Enable polytope-server workers to push results to BOBS (or S3) with optional encoding, returning redirect URLs so clients fetch directly from the storage backend instead of streaming through the broker.

### Concrete Deliverables
- `bobs/Dockerfile` — release/debug build targets
- `bobs/skaffold.yaml` — push to eccr.ecmwf.int/polytope/bobs
- `polytope-chart/Chart.yaml` — bobs dependency
- `polytope-chart/values.yaml` — bobs section
- `polytope-config/majh-dev.yaml` — bobs section
- `workers/common/src/delivery.rs` — ResultDelivery trait with BOBSPush, S3Push
- `workers/common/src/encoding.rs` — encode_stream() with zstd, gzip support
- `workers/common/src/delivery_config.rs` — DeliveryConfig struct (YAML-deserializable)
- Updated `fdb-worker` and `polytope-fe-worker` using delivery + encoding
- Frontend: Accept-Encoding extraction into job metadata
- Frontend: tower-http CompressionLayer for direct-streaming routes

### Definition of Done
- [ ] `skaffold build -b eccr.ecmwf.int/polytope/bobs` succeeds
- [ ] `helm template` with bobs dependency renders valid manifests
- [ ] `cargo test -p polytope-worker-common` passes with delivery and encoding tests
- [ ] E2E: job → worker pushes to BOBS → redirect → client reads compressed data with correct headers
- [ ] Existing workers without delivery config still work (backward compatible)

### Must Have
- BOBSPush implementation that creates a spool, writes data, completes, returns read URL
- S3Push implementation that uploads, generates presigned GET URL
- zstd and gzip encoding codecs
- Accept-Encoding header extracted from job submission into metadata
- Worker returns Completion::Redirect with storage read URL
- Content-Encoding propagated through BOBS (already implemented)
- All worker pools MUST have a delivery config (no default, no optional)

### Must NOT Have (Guardrails)
- No modification to the Processor trait signature
- No new Completion enum variants (extend existing ones only)
- No encoding negotiation protocol (fixed codec per pool, identity default)
- No new crate — all push/encoding code in workers/common
- No metrics/observability in this scope
- No PVC storage for BOBS (emptyDir is MVP; PVC is a follow-up)
- No abstraction for DirectStream (it's just the existing behavior)
- Do NOT double-compress: frontend middleware must skip responses with existing Content-Encoding
- Do NOT mix infrastructure changes (Dockerfile, Helm) with Rust code changes in same commit

---

## Verification Strategy

> **ZERO HUMAN INTERVENTION** — ALL verification is agent-executed. No exceptions.

### Test Decision
- **Infrastructure exists**: YES (cargo test in polytope-server, helm template for charts)
- **Automated tests**: YES (Tests-after — unit tests for new modules, integration tests for redirect flow)
- **Framework**: cargo test (unit), helm template (chart validation)

### QA Policy
Every task MUST include agent-executed QA scenarios.

- **Rust code**: cargo test + cargo build --release
- **Dockerfile**: docker build succeeds
- **Helm**: helm template renders valid YAML
- **Integration**: curl against test server

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 1 (Infra — all independent, parallel):
├── Task 1: BOBS Dockerfile release/debug targets [quick]
├── Task 2: BOBS skaffold.yaml [quick]
├── Task 3: polytope-chart bobs dependency [quick]
├── Task 4: polytope-config/majh-dev.yaml bobs section [quick]
└── Task 5: BOBS chart: update for YAML config (remove env vars) [quick]

Wave 2 (Abstractions — parallel within polytope-server):
├── Task 6: workers/common delivery_config.rs (DeliveryConfig struct + CLI arg) [unspecified-high]
├── Task 7: workers/common encoding.rs (zstd + gzip stream wrappers) [unspecified-high]
├── Task 8: workers/common delivery.rs (ResultDelivery trait + BOBSPush) [deep]
└── Task 9: workers/common delivery.rs (S3Push implementation) [deep]

Wave 3 (Integration — depends on Wave 2):
├── Task 10: Frontend Accept-Encoding extraction into job metadata [unspecified-high]
├── Task 11: fdb-worker: integrate delivery + encoding [unspecified-high]
├── Task 12: polytope-fe-worker: integrate delivery + encoding [unspecified-high]
└── Task 13: workers/common: update WorkerConfig with --delivery-config CLI arg [quick]

Wave 4 (Frontend middleware + polish):
├── Task 14: Frontend tower-http CompressionLayer for direct responses [unspecified-high]
└── Task 15: Integration test: full redirect flow with encoding [deep]

Wave FINAL (Review):
├── Task F1: Plan compliance audit (oracle)
├── Task F2: Code quality review (unspecified-high)
├── Task F3: Real manual QA (unspecified-high)
└── Task F4: Scope fidelity check (deep)
-> Present results -> Get explicit user okay

Critical Path: Task 1 → Task 8 → Task 11 → Task 15 → F1-F4 → user okay
Parallel Speedup: ~60% faster than sequential
Max Concurrent: 5 (Wave 1)
```

### Dependency Matrix

| Task | Depends On | Blocks |
|------|-----------|--------|
| 1-5  | None      | 8 (BOBS API contract) |
| 6    | None      | 8, 9, 11, 12, 13 |
| 7    | None      | 8, 9, 11, 12 |
| 8    | 6, 7      | 11, 12, 15 |
| 9    | 6, 7      | 11, 12, 15 |
| 10   | None      | 11, 12 |
| 11   | 8, 9, 10, 13 | 15 |
| 12   | 8, 9, 10, 13 | 15 |
| 13   | 6         | 11, 12 |
| 14   | None      | 15 |
| 15   | 11, 12, 14 | F1-F4 |

### Agent Dispatch Summary

- **Wave 1**: 5 tasks → all `quick`
- **Wave 2**: 4 tasks → T6,T7 `unspecified-high`, T8,T9 `deep`
- **Wave 3**: 4 tasks → T10,T11,T12 `unspecified-high`, T13 `quick`
- **Wave 4**: 2 tasks → T14 `unspecified-high`, T15 `deep`
- **FINAL**: 4 tasks → F1 `oracle`, F2-F3 `unspecified-high`, F4 `deep`

---

## TODOs

- [x] 1. BOBS Dockerfile: Add release/debug build targets

  **What to do**:
  - Add named targets `release` (debian-slim, minimal) and `debug` (debian-slim + bash) to existing Dockerfile
  - Keep the existing build stage, add `FROM debian:bookworm-slim AS release` and `FROM debian:bookworm-slim AS debug`

  **Must NOT do**: Do not add cargo-chef (overkill for this crate). Do not change the binary name.

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**: Can Run In Parallel: YES | Wave 1 | Blocks: nothing | Blocked By: none

  **References**:
  - `bobs/Dockerfile` — current simple two-stage build
  - `polytope-server/frontend/Dockerfile` — release/debug target pattern to follow

  **Acceptance Criteria**:
  - [ ] `docker build --target release .` succeeds in bobs/
  - [ ] `docker build --target debug .` succeeds in bobs/
  - [ ] Release image has no bash; debug image has bash

  **QA Scenarios**:
  ```
  Scenario: Docker builds both targets
    Tool: Bash
    Steps:
      1. docker build --target release -t bobs-test-release . (in bobs/)
      2. docker build --target debug -t bobs-test-debug . (in bobs/)
      3. docker run --rm bobs-test-debug bash -c "echo ok"
    Expected Result: Both builds succeed, bash works in debug
    Evidence: .sisyphus/evidence/task-1-docker-build.txt
  ```

  **Commit**: YES — `bobs: add release/debug Dockerfile targets`

- [x] 2. BOBS skaffold.yaml

  **What to do**:
  - Create `bobs/skaffold.yaml` with single artifact `eccr.ecmwf.int/polytope/bobs`
  - Use docker build target `release`, tag policy matching polytope-server (FIXED_TAG or PREFIX+GIT_TAG)
  - No local profile needed
  - Platforms: linux/amd64

  **Must NOT do**: No local profile. No debug artifact (can be added later).

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**: Can Run In Parallel: YES | Wave 1 | Blocks: nothing | Blocked By: none

  **References**:
  - `polytope-server/skaffold.yaml` — tag policy pattern, artifact structure

  **Acceptance Criteria**:
  - [ ] `skaffold build -b eccr.ecmwf.int/polytope/bobs --dry-run` parses without errors

  **QA Scenarios**:
  ```
  Scenario: Skaffold config is valid
    Tool: Bash
    Steps:
      1. skaffold diagnose (in bobs/)
    Expected Result: No errors in config parsing
    Evidence: .sisyphus/evidence/task-2-skaffold-diagnose.txt
  ```

  **Commit**: YES — groups with Task 1: `bobs: add release/debug Dockerfile targets and skaffold.yaml`

- [x] 3. polytope-chart: add bobs as Helm dependency

  **What to do**:
  - Add `dependencies:` section to `polytope-chart/Chart.yaml` with bobs chart (repository: `file://../bobs/chart`, condition: `bobs.enabled`)
  - Add `bobs:` section to `polytope-chart/values.yaml` with enabled, replicaCount, image, service, persistence defaults
  - Ensure `helm dependency update` works with the file reference

  **Must NOT do**: Do not modify existing templates. Do not change BOBS chart structure.

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**: Can Run In Parallel: YES | Wave 1 | Blocks: nothing | Blocked By: none

  **References**:
  - `polytope-chart/Chart.yaml` — current chart definition (no dependencies section)
  - `polytope-chart/values.yaml` — existing values structure
  - `bobs/chart/Chart.yaml` — bobs chart name and version

  **Acceptance Criteria**:
  - [ ] `helm template polytope-chart ./polytope-chart` renders bobs deployment and service

  **QA Scenarios**:
  ```
  Scenario: Helm template includes bobs
    Tool: Bash
    Steps:
      1. helm dependency update polytope-chart/ (with bobs chart available)
      2. helm template test polytope-chart/ --set bobs.enabled=true
      3. Verify output contains bobs Deployment and Service resources
    Expected Result: BOBS resources present in rendered YAML
    Evidence: .sisyphus/evidence/task-3-helm-template.txt
  ```

  **Commit**: YES — `polytope-chart: add bobs as Helm dependency`

- [x] 4. polytope-config/majh-dev.yaml: add bobs section

  **What to do**:
  - Add `bobs:` section to majh-dev.yaml with: enabled: true, replicaCount: 1, image (same registry pattern), persistence size, page_size, max_cache_bytes, bob_id
  - Follow the existing config pattern (image.repository, image.tag, etc.)

  **Must NOT do**: Do not change any existing config sections.

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**: Can Run In Parallel: YES | Wave 1 | Blocks: nothing | Blocked By: none

  **References**:
  - `polytope-config/majh-dev.yaml` — existing config structure
  - `bobs/chart/values.yaml` — bobs values schema

  **Acceptance Criteria**:
  - [ ] `helm template` with majh-dev.yaml overlay produces valid bobs resources

  **Commit**: YES — `polytope-config: add bobs section to majh-dev.yaml`

- [x] 5. BOBS chart: update deployment for YAML config file

  **What to do**:
  - BOBS binary now takes a config file as CLI arg, not env vars
  - Update `bobs/chart/templates/deployment.yaml`: remove env vars, add ConfigMap mount with bobs config YAML, set command to `["bobs", "/etc/bobs/config.yaml"]`
  - Add `bobs/chart/templates/configmap.yaml` that renders config from values
  - Update `bobs/chart/values.yaml`: replace `env:` section with `config:` section containing host, port, data_dir, page_size, max_cache_bytes, bob_id, etc.

  **Must NOT do**: Do not change the service or headless-service templates.

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**: Can Run In Parallel: YES | Wave 1 | Blocks: nothing | Blocked By: none

  **References**:
  - `bobs/chart/templates/deployment.yaml` — current env var-based deployment
  - `bobs/chart/values.yaml` — current values with env section
  - `bobs/src/config.rs` — Config struct fields and defaults

  **Acceptance Criteria**:
  - [ ] `helm template bobs ./bobs/chart` renders ConfigMap with valid YAML config
  - [ ] Deployment mounts ConfigMap and passes config path as arg

  **Commit**: YES — `bobs/chart: switch from env vars to YAML config file`

- [x] 6. workers/common: DeliveryConfig struct and CLI arg

  **What to do**:
  - Add `delivery_config.rs` to `polytope-server/workers/common/src/`
  - Define `DeliveryConfig` struct (serde Deserialize): `delivery_type` (enum: direct/bobs/s3), `bobs_url` (Option), `s3_bucket` (Option), `s3_region` (Option), `encoding` (Option: zstd/gzip/identity), `encoding_threshold_bytes` (Option, default 1024)
  - Add `--delivery-config <path>` CLI arg to WorkerConfig (REQUIRED — worker refuses to start without it)
  - Load DeliveryConfig from YAML file at startup

  **Must NOT do**: Do not modify the Processor trait. Do not change existing WorkerConfig fields.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: []

  **Parallelization**: Can Run In Parallel: YES | Wave 2 | Blocks: T8, T9, T11, T12, T13 | Blocked By: none

  **References**:
  - `polytope-server/workers/common/src/lib.rs` — WorkerConfig struct with clap
  - `bobs/src/config.rs` — serde YAML config pattern

  **Acceptance Criteria**:
  - [ ] DeliveryConfig deserializes from YAML
  - [ ] WorkerConfig accepts --delivery-config arg
  - [ ] Missing --delivery-config causes worker to exit with error at startup

  **QA Scenarios**:
  ```
  Scenario: Parse delivery config
    Tool: Bash (cargo test)
    Steps:
      1. Write temp YAML: {delivery_type: bobs, bobs_url: "http://bobs:3000", encoding: zstd}
      2. Deserialize into DeliveryConfig
      3. Assert fields match
    Expected Result: All fields parsed correctly
    Evidence: .sisyphus/evidence/task-6-config-test.txt

  Scenario: Missing config exits with error
    Tool: Bash (cargo test)
    Steps:
      1. Attempt to start worker without --delivery-config
      2. Assert process exits with non-zero status and descriptive error
    Expected Result: Error message indicating --delivery-config is required
    Evidence: .sisyphus/evidence/task-6-required-test.txt
  ```

  **Commit**: YES — `workers/common: add DeliveryConfig with CLI arg support`

- [x] 7. workers/common: encoding module (zstd + gzip)

  **What to do**:
  - Add `encoding.rs` to `polytope-server/workers/common/src/`
  - Define `Codec` enum: Zstd, Gzip, Identity
  - Implement `encode_stream(stream: impl Stream<Item=Result<Bytes>>, codec: Codec) -> impl Stream<Item=Result<Bytes>>`
  - Use `async-compression` crate for streaming zstd and gzip compression
  - Add `content_encoding_header(codec: Codec) -> &str` helper (returns "zstd", "gzip", or "identity")
  - Skip compression for payloads below threshold (pass-through)

  **Must NOT do**: Do not create a new crate. Do not implement brotli or deflate.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: []

  **Parallelization**: Can Run In Parallel: YES | Wave 2 | Blocks: T8, T9, T11, T12 | Blocked By: none

  **References**:
  - `async-compression` crate docs — streaming zstd/gzip compression
  - `workers/common/Cargo.toml` — add async-compression dependency

  **Acceptance Criteria**:
  - [ ] `encode_stream` with Zstd produces valid zstd output
  - [ ] `encode_stream` with Gzip produces valid gzip output
  - [ ] Identity codec passes through unchanged
  - [ ] Roundtrip test: compress → decompress → matches original

  **QA Scenarios**:
  ```
  Scenario: Zstd roundtrip
    Tool: Bash (cargo test)
    Steps:
      1. Create stream of 10KB random data
      2. encode_stream with Zstd
      3. Decompress with zstd decoder
      4. Compare with original
    Expected Result: Decompressed data matches original
    Evidence: .sisyphus/evidence/task-7-zstd-roundtrip.txt

  Scenario: Gzip roundtrip
    Tool: Bash (cargo test)
    Steps:
      1. Same as above with Gzip
    Expected Result: Decompressed data matches original
    Evidence: .sisyphus/evidence/task-7-gzip-roundtrip.txt
  ```

  **Commit**: YES — `workers/common: add encoding module (zstd + gzip stream wrappers)`

- [x] 8. workers/common: ResultDelivery trait + BOBSPush

  **What to do**:
  - Add `delivery.rs` to `polytope-server/workers/common/src/`
  - Define `ResultDelivery` trait:
    ```
    async fn deliver(config, content_type, content_encoding, body_stream) -> Result<Completion>
    ```
    Returns `Completion::Redirect { location }` for BOBS/S3, or `Completion::Complete { body }` for direct
  - Implement `BOBSPush`:
    1. PUT /create on BOBS with content_type, content_encoding → get key
    2. POST /write/{key}/0 with body stream (chunked)
    3. POST /complete/{key} with expected_size
    4. Return Completion::Redirect { location: "{bobs_url}/read/{key}" }
  - Handle BOBS-down: return Completion::Error

  **Must NOT do**: Do not modify the Processor trait. Do not add new Completion variants.

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**: Can Run In Parallel: NO | Wave 2 (after T6, T7 start) | Blocks: T11, T12, T15 | Blocked By: T6, T7

  **References**:
  - `workers/common/src/lib.rs` — Completion enum, worker loop
  - `bobs/src/http/mod.rs` — BOBS API endpoints (create, write, complete, read)
  - BOBS API: PUT /create → POST /write/{key}/{offset} → POST /complete/{key} → GET /read/{key}

  **Acceptance Criteria**:
  - [ ] BOBSPush creates spool, writes data, completes, returns redirect URL
  - [ ] Redirect URL format: `{bobs_url}/read/{key}`
  - [ ] BOBS-down returns Completion::Error with descriptive message
  - [ ] Unit test with mock HTTP server for BOBS endpoints

  **QA Scenarios**:
  ```
  Scenario: Successful BOBS push
    Tool: Bash (cargo test)
    Steps:
      1. Start mock HTTP server simulating BOBS (create returns key, write returns 200, complete returns 200)
      2. Call BOBSPush::deliver with 4KB body
      3. Assert returns Completion::Redirect with correct URL
      4. Assert mock received create, write, complete in order
    Expected Result: Redirect URL = "{mock_url}/read/{key}"
    Evidence: .sisyphus/evidence/task-8-bobs-push.txt

  Scenario: BOBS unreachable
    Tool: Bash (cargo test)
    Steps:
      1. Configure BOBSPush with unreachable URL
      2. Call deliver
      3. Assert returns Completion::Error
    Expected Result: Error with connection failure message
    Evidence: .sisyphus/evidence/task-8-bobs-down.txt
  ```

  **Commit**: YES — `workers/common: add ResultDelivery trait with BOBSPush implementation`

- [x] 9. workers/common: S3Push implementation

  **What to do**:
  - Implement `S3Push` for `ResultDelivery` trait
  - Use `aws-sdk-s3` crate for multipart upload
  - Generate presigned GET URL after upload completes
  - Return Completion::Redirect { location: presigned_url }
  - S3 config from DeliveryConfig: bucket, region, key prefix
  - Credentials via standard AWS chain (env vars, IRSA, instance profile)

  **Must NOT do**: Do not hardcode credentials. Do not implement custom signing.

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**: Can Run In Parallel: YES (with T8) | Wave 2 | Blocks: T11, T12, T15 | Blocked By: T6, T7

  **References**:
  - `aws-sdk-s3` crate docs — PutObject, presigned URLs
  - `workers/common/src/delivery_config.rs` — S3 config fields

  **Acceptance Criteria**:
  - [ ] S3Push uploads data, generates presigned GET URL
  - [ ] Presigned URL has maximum expiry (7 days / 604800s — S3 max for IAM credentials)
  - [ ] Content-Type and Content-Encoding set on S3 object metadata

  **QA Scenarios**:
  ```
  Scenario: S3 upload with presigned URL
    Tool: Bash (cargo test)
    Steps:
      1. Use mock S3 (wiremock or localstack) or unit test with mocked client
      2. Call S3Push::deliver with test data
      3. Assert returns Completion::Redirect with presigned URL
    Expected Result: URL contains bucket, key, and signature parameters
    Evidence: .sisyphus/evidence/task-9-s3-push.txt
  ```

  **Commit**: YES — `workers/common: add S3Push delivery implementation`

- [x] 10. Frontend: extract Accept-Encoding into job metadata

  **What to do**:
  - In `polytope-server/frontend/src/api/v2.rs` submit handler, extract `Accept-Encoding` header
  - Store in `job.metadata` as `{"accept_encoding": "zstd, gzip"}` (or null if not present)
  - Workers can read this from `work.metadata` to choose codec

  **Must NOT do**: Do not negotiate encoding. Do not modify the broker. Do not change v1 API.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: []

  **Parallelization**: Can Run In Parallel: YES | Wave 3 | Blocks: T11, T12 | Blocked By: none

  **References**:
  - `frontend/src/api/v2.rs` — submit handler, job construction
  - `bits/bits/src/job.rs` — Job struct, metadata field

  **Acceptance Criteria**:
  - [ ] Accept-Encoding header value stored in job.metadata.accept_encoding
  - [ ] Missing header → no accept_encoding in metadata

  **Commit**: YES — `frontend: extract Accept-Encoding into job metadata`

- [x] 11. fdb-worker: integrate delivery + encoding

  **What to do**:
  - In fdb-worker's Processor implementation, after producing the result stream:
    1. Read accept_encoding from work.metadata
    2. Choose codec based on DeliveryConfig encoding setting (or identity if not configured)
    3. Wrap stream with encode_stream if codec != identity
    4. Call ResultDelivery::deliver with the encoded stream
    5. Return the Completion from deliver (Redirect for BOBS/S3, Complete for direct)
  - Delivery config is always present (required CLI arg)

  **Must NOT do**: Do not change the FDB query logic. Do not make delivery mandatory.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: []

  **Parallelization**: Can Run In Parallel: YES (with T12) | Wave 3 | Blocks: T15 | Blocked By: T8, T9, T10, T13

  **References**:
  - `polytope-server/workers/fdb-worker/src/main.rs` — Processor impl
  - `workers/common/src/delivery.rs` — ResultDelivery trait
  - `workers/common/src/encoding.rs` — encode_stream

  **Acceptance Criteria**:
  - [ ] Worker with --delivery-config (bobs) pushes to BOBS and returns Redirect
  - [ ] Worker with delivery_type: direct returns Complete (streams through broker)
  - [ ] Encoded data has correct content_encoding metadata

  **Commit**: YES — `fdb-worker: integrate delivery + encoding`

- [x] 12. polytope-fe-worker: integrate delivery + encoding

  **What to do**: Same as Task 11 but for polytope-fe-worker.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: []

  **Parallelization**: Can Run In Parallel: YES (with T11) | Wave 3 | Blocks: T15 | Blocked By: T8, T9, T10, T13

  **References**:
  - `polytope-server/workers/polytope-fe-worker/src/main.rs` — Processor impl

  **Acceptance Criteria**:
  - [ ] Same as Task 11 but for polytope-fe-worker

  **Commit**: YES — `polytope-fe-worker: integrate delivery + encoding`

- [x] 13. workers/common: wire --delivery-config into worker loop

  **What to do**:
  - Update `run_worker_loop` or equivalent to load DeliveryConfig from the CLI arg path
  - Make DeliveryConfig available to Processor implementations (pass via WorkItem or as shared state)
  - If --delivery-config not provided, workers use None (direct streaming via existing path)

  **Must NOT do**: Do not break the existing worker loop contract.

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**: Can Run In Parallel: NO | Wave 3 | Blocks: T11, T12 | Blocked By: T6

  **References**:
  - `workers/common/src/lib.rs` — run_worker_loop, WorkerConfig

  **Acceptance Criteria**:
  - [ ] DeliveryConfig accessible in worker process() method
  - [ ] None delivery config = unchanged behavior

  **Commit**: YES — groups with Task 6 commit

- [x] 14. Frontend: tower-http CompressionLayer

  **What to do**:
  - Add `tower-http` with `compression` feature to frontend Cargo.toml
  - Add `CompressionLayer` to the axum router for v1/v2 poll endpoints and EDR/OpenMeteo routes
  - Ensure middleware skips responses that already have Content-Encoding set (to prevent double compression on redirect-fetched data)
  - Supports gzip and zstd at minimum

  **Must NOT do**: Do not add compression to non-streaming endpoints (status, create, etc.). Do not compress redirect responses.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: []

  **Parallelization**: Can Run In Parallel: YES | Wave 4 | Blocks: T15 | Blocked By: none

  **References**:
  - `tower-http` crate — CompressionLayer, predicate for skipping
  - `frontend/src/main.rs` — router construction
  - `frontend/src/api/v2.rs` — poll handler

  **Acceptance Criteria**:
  - [ ] `curl -H "Accept-Encoding: gzip" /api/v2/...` returns gzip-encoded response
  - [ ] Response without Accept-Encoding has no Content-Encoding (identity)
  - [ ] Responses with existing Content-Encoding are NOT double-compressed

  **Commit**: YES — `frontend: add CompressionLayer for direct-streaming responses`

- [x] 15. Integration test: full redirect flow with encoding

  **What to do**:
  - Write an integration test in polytope-server that exercises the complete path:
    1. Start a mock BOBS server
    2. Configure worker with DeliveryConfig (bobs, zstd encoding)
    3. Submit a job
    4. Worker encodes + pushes to mock BOBS
    5. Worker returns Completion::Redirect with BOBS read URL
    6. Frontend returns 303 with Location header
    7. Verify Location URL points to BOBS read endpoint
  - Test backward compatibility: worker without delivery config returns Complete (no redirect)

  **Must NOT do**: Do not require real BOBS or S3 for this test.

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**: Can Run In Parallel: NO | Wave 4 | Blocks: F1-F4 | Blocked By: T11, T12, T14

  **References**:
  - `polytope-server/tests/` — existing integration test patterns
  - `workers/common/src/delivery.rs` — BOBSPush implementation

  **Acceptance Criteria**:
  - [ ] Test passes: redirect flow with encoding produces correct Location header
  - [ ] Test passes: delivery_type: direct = streams through broker

  **Commit**: YES — `polytope-server: add integration test for delivery redirect flow`

---

## Final Verification Wave

- [x] F1. **Plan Compliance Audit** — `oracle`
  Read the plan end-to-end. For each "Must Have": verify implementation exists. For each "Must NOT Have": search codebase for forbidden patterns. Compare deliverables against plan.
  Output: `Must Have [N/N] | Must NOT Have [N/N] | VERDICT: APPROVE/REJECT`

- [x] F2. **Code Quality Review** — `unspecified-high`
  Run `cargo build` + `cargo test` across all modified repos. Check for: unwrap in prod code, unused imports, dead code. Check backward compatibility: old worker configs still work.
  Output: `Build [PASS/FAIL] | Tests [N pass/N fail] | VERDICT`

- [x] F3. **Real Manual QA** — `unspecified-high`
  Start BOBS locally, submit a job, verify worker pushes to BOBS, verify redirect URL, verify client can read with correct Content-Encoding. Test with and without encoding. Test with missing --delivery-config (fallback to direct).
  Output: `Scenarios [N/N pass] | VERDICT`

- [x] F4. **Scope Fidelity Check** — `deep`
  For each task: verify diff matches spec. No scope creep. No unaccounted changes. Check "Must NOT do" compliance.
  Output: `Tasks [N/N compliant] | VERDICT`

---

## Commit Strategy

**DO NOT COMMIT.** Stage all changes in current branches only. No new branches, no pushes.

---

## Success Criteria

### Verification Commands
```bash
# Wave 1
skaffold build -b eccr.ecmwf.int/polytope/bobs  # Expected: image pushed
helm template bobs ./bobs/chart                    # Expected: valid YAML

# Wave 2
cargo test -p polytope-worker-common               # Expected: all pass

# Wave 3
cargo build -p fdb-worker                          # Expected: compiles
cargo build -p polytope-fe-worker                  # Expected: compiles

# Wave 4
cargo build -p polytope-server                     # Expected: compiles with CompressionLayer
```

### Final Checklist
- [ ] All "Must Have" present
- [ ] All "Must NOT Have" absent
- [ ] All tests pass across all repos
- [ ] Workers without --delivery-config refuse to start
- [ ] BOBS serves Content-Encoding on redirect reads
