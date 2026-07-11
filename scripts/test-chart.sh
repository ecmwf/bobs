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

helm lint "$chart" --strict "${common_values[@]}"

helm template bobs "$chart" "${common_values[@]}" >"$tmpdir/default.yaml"
assert_contains 'image: "eccr.ecmwf.int/polytope/bobs:0.1.0"' "$tmpdir/default.yaml"
assert_contains 'value: "info"' "$tmpdir/default.yaml"
assert_contains 'whenDeleted: Retain' "$tmpdir/default.yaml"
assert_contains 'whenScaled: Delete' "$tmpdir/default.yaml"
assert_contains '- ReadWriteOnce' "$tmpdir/default.yaml"
assert_contains 'volumeMode: "Filesystem"' "$tmpdir/default.yaml"

helm template bobs "$chart" "${common_values[@]}" \
  --set global.imageRegistry=registry.example.com \
  --set image.repository=eccr.ecmwf.int/polytope/bobs \
  --set-string image.tag= \
  --set-string image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --set-string rustLog=bobs=debug \
  --set persistence.enabled=false \
  >"$tmpdir/overrides.yaml"
assert_contains 'image: "registry.example.com/polytope/bobs@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"' "$tmpdir/overrides.yaml"
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

if helm template bobs "$chart" "${common_values[@]}" \
  --set-string image.tag= --set-string image.digest= \
  >"$tmpdir/invalid.yaml" 2>"$tmpdir/invalid.log"; then
  echo "Expected an empty image tag and digest to fail schema validation" >&2
  exit 1
fi
assert_contains "values don't meet the specifications" "$tmpdir/invalid.log"

echo "BOBS chart contract tests passed"
