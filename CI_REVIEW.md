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

- [ ] **4. Inconsistent action pinning (supply chain).**
  `ci.yaml` pins `actions/checkout` to a SHA but leaves the rest floating:
  `dtolnay/rust-toolchain@stable` (moving branch), `Swatinem/rust-cache@v2`,
  `docker/*@v3/v6` (has registry secrets + `contents: write`),
  `softprops/action-gh-release@v2` (`contents: write`), and
  `docs-sites.yaml` `actions/checkout|cache|upload-artifact@v4`. The
  security-sensitive workflows are the ones left unpinned.
  _Fix:_ pin everything to SHAs (or accept floating everywhere — no half-and-half).

- [x] **5. No `concurrency` guards on publish workflows.**
  Two quick pushes to `main` can launch overlapping publish runs that race on
  `git tag`/`git push origin v<ver>` and re-push the same mutable registry tag.
  _Done:_ added `concurrency` (per-workflow group, `cancel-in-progress: false`)
  to `publish-image.yml` and `helm-publish.yaml` so publishes serialize instead
  of racing.

- [ ] **6. Mutable image tag as the published artifact.**
  Image published only as `eccr.ecmwf.int/polytope/bobs:<ver>`. Workspace
  `AGENTS.md` mandates digest pinning (mn5 mirror has served stale manifests).
  Digest is captured to the job summary but never propagated to anything
  consumable.
  _Fix:_ also push an immutable `sha-<commit>` tag and/or wire digest into
  `chart/values.yaml image.digest`.

- [ ] **7. TOCTOU / non-atomic publish.**
  `detect` decides, `publish` builds+pushes, then creates the tag last. If the
  push succeeds but tag creation fails, the image is out but untagged, so the
  next push overwrites the same registry tag. The "released" source of truth is
  written last.

## Low severity / polish

- [ ] **8. Untested feature.** `tokio-fileio-fallback` is declared but never
  built/tested in CI (only `default` and `telemetry` are).

- [ ] **9. Chart registry namespace inconsistency.** Image →
  `eccr.ecmwf.int/polytope/bobs`; chart → `oci://eccr.ecmwf.int/bobs` (no
  `polytope/`). Confirm deliberate — `AGENTS.md` says everything lives under
  `polytope/`.

- [ ] **10. Hardcoded versions in `test-chart.sh`.** `bobs:0.1.0`, byte-exact
  bounds, etc. are pinned literals; every appVersion bump needs hand-editing.
  _Fix:_ derive expected version from `Cargo.toml`.

- [ ] **11. No dependency vulnerability scanning.** No `cargo audit`/`cargo-deny`,
  no `deny.toml`. Cheap, high-value addition given the supply-chain effort.

- [ ] **12. `docs-sites.yaml` publishes on every push to `main`** (no path filter
  on the push trigger), republishing `latest` docs for unrelated changes.

- [x] **13. No `concurrency` on `ci.yaml`.** Rapid PR pushes run redundant full
  CI. _Done:_ added `concurrency` keyed on workflow+ref with
  `cancel-in-progress: true`.

- [ ] **14. No `CODEOWNERS`.** Nothing enforces review ownership on workflow
  files that hold the release keys.

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
