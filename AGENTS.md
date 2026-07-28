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

The CI (`feat/ci-publish-workflows`, eventually `main`) has two release paths,
decided at push time by `scripts/test-gate.sh` logic in the `gate` job:

| State | Outcome |
|---|---|
| Both cargo tag and chart tag exist | Nothing to publish, skip |
| Cargo tag missing, chart tag present | **Error** — bump `chart/Chart.yaml::version` to match |
| Both tags missing, versions match | Full code+chart release: image + chart + tag |
| Cargo tag present, chart tag missing | Chart-only release: chart + tag only |

Release tags use a `.dev0` suffix (e.g. `0.1.5.dev0`) to satisfy the org-level
tag-protection ruleset. Image tags and chart versions stay as bare `X.X.X`.

On a **code+chart release**, `chart/Chart.yaml::version` must equal
`Cargo.toml::version`. On a **chart-only release**, only
`chart/Chart.yaml::version` is bumped.

## Path-based job skipping

`build-and-test` only runs when `src/**`, `tests/**`, `Cargo.toml`,
`Cargo.lock`, `Dockerfile`, `skaffold.yaml`, or `.cargo/**` change.
`lint-chart` only runs when `chart/**` or `scripts/test-chart.sh` change.
Both are also triggered by changes to `.github/workflows/ci.yaml`.
