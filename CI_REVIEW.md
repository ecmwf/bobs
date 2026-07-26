<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# bobs CI review — `feat/ci-publish-workflows`

Critical review of the GitHub Actions CI/publish setup. Items are ordered by
severity. Check them off as we work through them.

**Legend:** `[x]` done in this PR · `[ ]` open · items under _Deferred_ are
intentionally punted to a follow-up PR with reasoning.

## Summary

Solid, thoughtful setup — version-gated publishing, idempotent tag gates, a
rigorous chart-contract test, SHA-checksummed kubeconform download, and
`persist-credentials: false` on read-only checkouts. But there are real gaps,
several significant. Top three to fix first: **#1, #2, #3**.
(Status: #2 done; #3 deferred to a follow-up PR — see _Deferred_ section.
#5 and #13 also done.)

---

## High severity

- [x] **0. `publish-image.yml` was invalid YAML — the workflow could never run.**
  The `detect` job embedded multi-line Python in `run: |` with the code dedented
  to column 0, which terminates the YAML block scalar. PyYAML (and GitHub's
  parser) reject the whole file (`could not find expected ':'`), so the image
  publish workflow would fail to load — nothing about item #1's gating mattered
  because the workflow never started.
  _Done:_ replaced the multi-line `python3 -c "..."` with a single-line
  invocation. All six workflows now pass `yaml.safe_load`; extractor verified to
  return `0.1.0`.

- [x] **1. Publishing is not gated on tests passing.**
  `publish-image.yml` and `helm-publish.yaml` triggered on push to `main` and ran
  completely independently of `ci.yaml` (no `needs`/`workflow_run` link). The
  Docker build compiled but never ran `cargo test`/`cargo fmt`; the chart publish
  never ran the chart contract test. A commit that landed on `main` with failing
  tests still built+published.
  _Done (approach b — inline gate):_ added a `test` job to each publish workflow,
  gated on `should_publish == 'true'` and required by `publish`
  (`needs: [detect, test]`):
  - image: `cargo fmt --all -- --check` + `cargo test --locked` (default) +
    `cargo test --locked --features telemetry` (matches the image's telemetry build).
  - chart: installs Helm + SHA-verified kubeconform and runs `scripts/test-chart.sh`
    (same gate as `chart.yaml`).
  Skip semantics verified: when `should_publish=false`, `test` is skipped and
  `publish`'s own `if` is also false, so nothing runs; when true, a failing
  `test` blocks `publish`. Chose (b) over `workflow_run` for determinism and
  self-containment (accepts a duplicate build only when actually publishing a new
  version).

- [x] **2. `cargo test` runs without `--locked`; the release build uses `--locked`.**
  CI regenerated a stale `Cargo.lock` and stayed green, but `Dockerfile` builds
  with `cargo build --release --locked` and fails on lock drift → PR green, then
  publish fails after merge.
  _Done:_ added `--locked` to both `cargo test` invocations in `ci.yaml`.
  Verified `Cargo.lock` is currently in sync (`cargo tree --locked` passes), so
  the gate lands green.

- [ ] **3. No clippy anywhere.** — _deferred to follow-up PR (see below)._
  Zero `cargo clippy` in any workflow. Workspace convention (`/verify`) is
  fmt + clippy + test; only fmt is checked.
  _Intended fix:_ `cargo clippy --locked --all-targets --all-features -- -D warnings`.

## Medium severity

- [x] **4. Inconsistent action pinning (supply chain).**
  `ci.yaml` pinned only `actions/checkout`; the rest floated:
  `dtolnay/rust-toolchain@stable`, `Swatinem/rust-cache@v2`, `docker/*@v3/v6`,
  `softprops/action-gh-release@v2`, and `docs-sites.yaml`
  `actions/checkout|cache|upload-artifact@v4`.
  _Done:_ pinned every third-party action to a 40-char commit SHA with a
  version comment, across `ci.yaml`, `publish-image.yml`, `helm-publish.yaml`,
  and `docs-sites.yaml`:
  - `dtolnay/rust-toolchain` → `4cda84d5` (the `stable` branch tip, whose
    `action.yml` bakes in `default: stable`, so it keeps installing stable with
    no extra input)
  - `Swatinem/rust-cache` → `f13886b9` (v2.8.1)
  - `docker/setup-buildx-action` → `8d2750c6` (v3.12.0),
    `docker/login-action` → `c94ce9fb` (v3.7.0),
    `docker/build-push-action` → `10e90e36` (v6.19.2)
  - `softprops/action-gh-release` → `3bb12739` (v2.6.2)
  - `actions/cache` → `0057852b` (v4.3.0),
    `actions/upload-artifact` → `ea165f8d` (v4.6.2),
    `docs-sites.yaml` `actions/checkout` → `34e11487` (v4.3.1, matching the
    other workflows)
  Verified no floating action refs remain (`grep` for non-40-hex `@refs`).
  Note: this pins the action _code_ only; `dtolnay/rust-toolchain` still installs
  whatever "stable" rust is at run time (the rust-version pinning concern lives
  with the deferred clippy work in #3).

- [x] **5. No `concurrency` guards on publish workflows.**
  Two quick pushes to `main` can launch overlapping publish runs that race on
  `git tag`/`git push origin v<ver>` and re-push the same mutable registry tag.
  _Done:_ added `concurrency` (per-workflow group, `cancel-in-progress: false`)
  to `publish-image.yml` and `helm-publish.yaml` so publishes serialize instead
  of racing.

- [x] **6. Mutable image tag as the published artifact.**
  Image was published only as `eccr.ecmwf.int/polytope/bobs:<ver>`. Workspace
  `AGENTS.md` mandates digest pinning (the mn5 mirror has served stale manifests
  for a reused tag).
  _Done:_ the build now also pushes an immutable, per-commit
  `eccr.ecmwf.int/polytope/bobs:sha-<github.sha>` tag alongside the version tag,
  and the job summary prints the version tag, the immutable sha tag, and the
  digest. A per-commit tag is content-addressable in practice (a given commit
  is never re-pushed with different content), giving deployments a stable
  reference.
  _Remaining (follow-up):_ wire the resulting digest into
  `chart/values.yaml image.digest` so the chart deploys by digest by default —
  left out here because it needs a commit-back / cross-artifact step.

- [x] **7. TOCTOU / non-atomic publish.**
  `detect` decides, `publish` builds+pushes, then creates the tag last. If the
  push succeeds but tag creation fails, the image is out but untagged, so the
  next push overwrites the same registry tag.
  _Reviewed — no code change beyond item 6:_ the build→tag order is actually
  correct (you must not create the `v<ver>` release tag before the image push
  succeeds). The residual risk is a mutable version tag being overwritten on a
  retry, which the immutable `sha-<commit>` tag from item 6 now mitigates:
  every published build keeps a durable, content-addressable reference
  regardless of version-tag state.

## Low severity / polish

- [x] **8. Untested feature.** `Cargo.toml` declared three features; CI tested
  `default` and `telemetry` but never `tokio-fileio-fallback`.
  _Done:_ added `cargo test --locked --features tokio-fileio-fallback` to the
  `build-and-test` job (verified locally: builds clean, 12 tests pass).

- [x] **9. Chart registry namespace — accepted as-is.** Image publishes to
  `eccr.ecmwf.int/polytope/bobs`; the chart publishes to
  `oci://eccr.ecmwf.int/bobs`. Confirmed intentional — the chart lives under the
  top-level `bobs` OCI namespace, separate from the `polytope/` image project.
  No change.

- [x] **10. Hardcoded versions in `test-chart.sh`.** The two `bobs:0.1.0` image
  assertions were pinned literals needing hand-editing on every version bump.
  _Done:_ derive `app_version` from `chart/Chart.yaml::appVersion` (awk) and use
  it in both assertions. Verified end-to-end: full chart contract test passes
  (`BOBS chart contract tests passed`, kubeconform 35/35 valid).

- [x] **11. No dependency vulnerability scanning.**
  _Done:_ added an `audit` job to `ci.yaml` that installs `cargo-audit` (via
  SHA-pinned `taiki-e/install-action` v2.85.2) and runs `cargo audit`. Two
  pre-existing high-severity advisories in transitive `quick-xml 0.26`
  (`pprof -> inferno -> quick-xml`) block a naive gate; they are documented and
  ignored in `.cargo/audit.toml` with justification (quick-xml is used only to
  *generate* flamegraph SVGs — bobs never parses untrusted XML — and `pprof`
  0.14.1 pins the version so `cargo update` can't fix it). Yanked `spin` remains
  a non-failing warning. Verified `cargo audit` exits 0 with the config.
  _Tradeoff:_ audit runs on every PR/push, so a newly published advisory can
  block an unrelated PR (intended — surfaces vulns promptly); move to a cron
  schedule if that friction is unwanted.

- [~] **12. `docs-sites.yaml` publishes on every push to `main`** — _skipped
  (won't fix)._ Republishing `latest` docs for unrelated changes is minor waste
  and acceptable.

- [x] **13. No `concurrency` on `ci.yaml`.** Rapid PR pushes run redundant full
  CI. _Done:_ added `concurrency` keyed on workflow+ref with
  `cancel-in-progress: true`.

- [~] **14. No `CODEOWNERS`.** — _skipped (won't fix)._ Its enforcement depends
  on a branch-protection setting only a repo admin can toggle; the team opted
  not to add one.

- [~] **15. REUSE job depends on Docker Hub at runtime (flaky).** — _skipped
  (won't fix)._ Observed 2026-07-26: the PR run failed with
  `registry-1.docker.io ... i/o timeout` while the push run passed; a plain
  re-run then passed in 18s. Transient Docker Hub rate-limiting, not a
  compliance issue. Left on `fsfe/reuse-action` for cross-repo consistency;
  handle with a re-run when it flakes.

- [x] **16. Node 20 action deprecation.** GitHub was force-running several
  Node-20 actions on Node 24 (annotation observed on the CI run). Bumped every
  Node-based action to its latest Node-24 release: `actions/checkout` v7.0.1,
  `actions/cache` v6.1.0, `actions/upload-artifact` v7.0.1,
  `Swatinem/rust-cache` v2.9.1, `azure/setup-helm` v5.0.1,
  `docker/setup-buildx-action` v4.2.0, `docker/login-action` v4.5.1,
  `docker/build-push-action` v7.3.0, `softprops/action-gh-release` v3.0.2.
  Composite/Docker actions (`dtolnay/rust-toolchain`, `taiki-e/install-action`,
  `fsfe/reuse-action`) don't use Node and were left as-is. Caveat: the
  `docker/*` and `gh-release` bumps live in main-only publish workflows, so they
  are first exercised on merge, not on this PR branch.

## Deferred to follow-up PR

- [ ] **3. Enable a clippy gate — deferred.**
  The step itself is trivial (and was drafted), but enabling
  `cargo clippy ... -D warnings` today would fail CI on the **first run**: the
  tree carries **25 pre-existing warnings** that `-D warnings` promotes to hard
  errors. Fixing production code + cleaning up test scaffolding is out of scope
  for a CI-only PR and deserves its own review. Breakdown:

  | Count | Kind | Where |
  |---|---|---|
  | ~20 | rustc `dead_code` (never used/constructed) | `src/manager.rs` `#[cfg(test)]` scaffolding — `REOPEN_*`, `LIMITED_*` statics/structs/fns (looks like orphaned test infra — confirm before deleting) |
  | 1 | `clippy::assertions_on_constants` | `src/config.rs:475` |
  | 1 | `clippy::large_enum_variant` | `src/metadata.rs:758` (`RecoveredMetadata`) |
  | 1 | `clippy::too_many_arguments` | `src/manager.rs:126` (`async fn run`, 8/7) |
  | 1 | `clippy::manual_async_fn` | `src/manager.rs:2190` (test `open`) |

  **Follow-up PR must also address toolchain brittleness:** the drafted gate used
  `dtolnay/rust-toolchain@stable` with `-D warnings`. clippy adds new lints on
  each stable release, so a floating `@stable` + `-D warnings` will
  intermittently break CI on unrelated PRs. Pin the lint job's toolchain to a
  fixed version (match the Dockerfile's `1.93`) before enabling the gate.

  _Plan:_ (1) triage/remove the orphaned `manager.rs` test scaffolding, (2) fix
  or `#[allow]` the 4 clippy lints with justification, (3) add the clippy step
  with a pinned toolchain, (4) verify green locally.

## Done well (keep)

- Version-consistency gate (`check-versions.sh`) is thorough; correctly treats
  chart `version` as independent from `appVersion`.
- Idempotent publish gates via `git ls-remote --tags` — no duplicate releases.
- kubeconform downloaded with pinned version **and** SHA-256 verification.
- `chart/test-chart.sh` is an excellent positive+negative contract test.
- Correct `persist-credentials: false` on read-only checkouts; omitted only
  where tag pushes need the token.
- Tags pushed by `GITHUB_TOKEN` won't recursively trigger `docs-sites.yaml`'s
  `tags: "*"` trigger — no publish loop (worth a clarifying comment though).
