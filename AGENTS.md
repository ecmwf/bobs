<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# AGENTS.md — bobs

## Rust workflow

After any change to `Cargo.toml` or Rust source:

```bash
cargo fmt --all
cargo test          # also updates Cargo.lock — commit it
```

Always commit `Cargo.lock` alongside `Cargo.toml` changes. `cargo check` does
not update the lock file; `cargo test` does.

## Version fields

Four fields must stay in sync (checked by `scripts/check-versions.sh`):

| File | Field |
|---|---|
| `Cargo.toml` | `package.version` |
| `chart/Chart.yaml` | `appVersion` |
| `chart/values.yaml` | `image.tag` |
| `CITATION.cff` | `version` |

`chart/Chart.yaml::version` (the Helm chart version) is managed separately —
see CI gate logic below.

## CI gate logic

The CI (`.github/workflows/ci.yaml`, on pull requests and pushes to `main`) has two
release paths, decided on each push to `main` by `scripts/gate.sh` in the `gate` job
(`scripts/test-gate.sh` is the offline test harness for it). The registry is the
source of truth: the gate asks whether image `eccr.ecmwf.int/bobs/bobs:<Cargo version>`
and chart `bobs-chart:<chart version>` already exist. Git tags are only a marker.

| Registry state | Outcome |
|---|---|
| Image and chart both published | Nothing to publish, skip |
| Image missing, chart published | **Error** — bump `chart/Chart.yaml::version` to match |
| Both missing, versions match | Full code+chart release: image + chart + tag |
| Image published, chart missing | Chart-only release: chart + tag only; `Cargo.toml` must be unchanged in the push |

Consequences: a release that failed or was only half published is retried on the
next push to `main`, whatever that push touches (the gate overrides the path
filters below); the publish jobs refuse to overwrite an existing image or chart
version; a chart-only release cannot move `image.tag` to a different published
image.

Release tags use a `.dev0` suffix (e.g. `0.1.5.dev0`) to satisfy the org-level
tag-protection ruleset. Image tags and chart versions stay as bare `X.X.X`.

On a **code+chart release**, `chart/Chart.yaml::version` must equal
`Cargo.toml::version`. On a **chart-only release**, only
`chart/Chart.yaml::version` is bumped; since chart versions and code versions share
one number line, the next code release must use a version above the last chart-only one.

## Path-based job skipping

`build-and-test` only runs when `src/**`, `tests/**`, `Cargo.toml`,
`Cargo.lock`, `Dockerfile`, `skaffold.yaml`, or `.cargo/**` change, or when the
gate has an image to publish. `lint-chart` only runs when `chart/**` or
`scripts/test-chart.sh` change, or when the gate has a chart to publish.
Both are also triggered by changes to `.github/workflows/ci.yaml`.
