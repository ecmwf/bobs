#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
chart="$repo_root/chart"
tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

common_values=(--set config.host_prefix=bobs --set config.domain=example.test)

assert_contains() {
  local expected=$1 file=$2
  if ! grep -Fq -- "$expected" "$file"; then
    printf 'Expected rendered chart to contain: %s\n' "$expected" >&2
    exit 1
  fi
}

assert_schema_rejects() {
  local name=$1
  shift
  if helm template bobs "$chart" "${common_values[@]}" "$@" >"$tmpdir/$name.yaml" 2>"$tmpdir/$name.log"; then
    printf 'Expected schema validation to reject %s\n' "$name" >&2
    exit 1
  fi
  assert_contains "values don't meet the specifications" "$tmpdir/$name.log"
}

# Exercise standalone defaults and both supported ingress controllers. The
# community forwarded-prefix fixture uses two replicas and verifies that each pod
# gets its own ingress; the NGINX Inc lint uses the single-replica mode.
helm lint "$chart" --strict "${common_values[@]}" \
  --set ingress.enabled=true \
  --set ingress.forwardedPrefix.enabled=true \
  --set global.ingress.controller=nginx-inc \
  --set replicaCount=1
helm lint "$chart" --strict --values "$chart/tests/forwarded-prefix-values.yaml"
"$chart/tests/forwarded-prefix-render.sh"

helm template bobs "$chart" "${common_values[@]}" >"$tmpdir/default.yaml"
assert_contains 'image: "eccr.ecmwf.int/polytope/bobs:0.1.0"' "$tmpdir/default.yaml"
assert_contains 'value: "info"' "$tmpdir/default.yaml"
assert_contains 'max_live_spools: 256' "$tmpdir/default.yaml"
assert_contains 'max_spool_bytes: 8589934592' "$tmpdir/default.yaml"
assert_contains 'create_admission_timeout_ms: 5000' "$tmpdir/default.yaml"
assert_contains 'enable_pprof: false' "$tmpdir/default.yaml"
assert_contains 'whenDeleted: Retain' "$tmpdir/default.yaml"
assert_contains 'whenScaled: Delete' "$tmpdir/default.yaml"
assert_contains '- ReadWriteOnce' "$tmpdir/default.yaml"
assert_contains 'volumeMode: "Filesystem"' "$tmpdir/default.yaml"

helm template bobs "$chart" "${common_values[@]}" \
  --set global.imageRegistry=registry.example.com/team \
  --set image.repository=eccr.ecmwf.int/polytope/bobs \
  --set-string image.tag= \
  --set-string image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --set-string rustLog=bobs=debug \
  --set persistence.enabled=false \
  >"$tmpdir/overrides.yaml"
assert_contains 'image: "registry.example.com/team/polytope/bobs@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"' "$tmpdir/overrides.yaml"
assert_contains 'value: "bobs=debug"' "$tmpdir/overrides.yaml"
assert_contains 'sizeLimit: "10Gi"' "$tmpdir/overrides.yaml"

helm template bobs "$chart" "${common_values[@]}" \
  --set persistence.accessModes[0]=ReadWriteMany \
  --set persistence.volumeMode=Block \
  --set-string 'persistence.annotations.storage\.example\.com/owner=bobs' \
  --set persistence.retentionPolicy.whenDeleted=Delete \
  --set persistence.retentionPolicy.whenScaled=Retain \
  >"$tmpdir/persistence.yaml"
assert_contains 'storage.example.com/owner: bobs' "$tmpdir/persistence.yaml"
assert_contains '- ReadWriteMany' "$tmpdir/persistence.yaml"
assert_contains 'volumeMode: "Block"' "$tmpdir/persistence.yaml"
assert_contains 'whenDeleted: Delete' "$tmpdir/persistence.yaml"
assert_contains 'whenScaled: Retain' "$tmpdir/persistence.yaml"

# Validate the exact final runtime bounds represented by the chart contract.
helm template bobs "$chart" "${common_values[@]}" \
  --set config.page_size=67108864 \
  --set config.max_cache_bytes=0 \
  --set config.max_live_spools=2305843009213693951 \
  --set config.max_spool_bytes=67108864 \
  --set config.io_uring_shards=256 \
  --set config.io_uring_queue_capacity=2305843009213693951 \
  >"$tmpdir/bounds.yaml"

assert_schema_rejects page-size-high --set config.page_size=67108865
assert_schema_rejects cache-negative --set config.max_cache_bytes=-1
assert_schema_rejects live-spools-zero --set config.max_live_spools=0
assert_schema_rejects live-spools-high --set config.max_live_spools=2305843009213693952
assert_schema_rejects spool-size-zero --set config.max_spool_bytes=0
assert_schema_rejects admission-timeout-zero --set config.create_admission_timeout_ms=0
assert_schema_rejects pprof-type --set-string config.enable_pprof=no
assert_schema_rejects shards-zero --set config.io_uring_shards=0
assert_schema_rejects shards-high --set config.io_uring_shards=257
assert_schema_rejects queue-zero --set config.io_uring_queue_capacity=0
assert_schema_rejects queue-high --set config.io_uring_queue_capacity=2305843009213693952
assert_schema_rejects missing-host-prefix --set-string config.host_prefix=
assert_schema_rejects missing-domain --set-string config.domain=
assert_schema_rejects missing-route --set-string config.route_name=
assert_schema_rejects missing-image --set-string image.tag= --set-string image.digest=

echo "BOBS chart contract tests passed"
