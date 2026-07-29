#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
chart="$repo_root/chart"
# Image tag the chart renders by default; kept in sync with Cargo.toml and
# CITATION.cff by scripts/check-versions.sh. Derive it so these assertions do not
# need hand-editing on every version bump.
app_version=$(awk '/^appVersion:/{print $2; exit}' "$chart/Chart.yaml" | tr -d "'\"")
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

assert_not_contains() {
  local unexpected=$1 file=$2
  if grep -Fq -- "$unexpected" "$file"; then
    printf 'Expected rendered chart not to contain: %s\n' "$unexpected" >&2
    exit 1
  fi
}

assert_count() {
  local expected=$1 needle=$2 file=$3
  local actual
  actual=$(grep -Fc -- "$needle" "$file" || true)
  if [[ "$actual" != "$expected" ]]; then
    printf 'Expected %s occurrences of %s, found %s\n' "$expected" "$needle" "$actual" >&2
    exit 1
  fi
}

assert_template_rejects() {
  local name=$1 expected=$2
  shift 2
  if helm template bobs "$chart" --skip-schema-validation "${common_values[@]}" "$@" >"$tmpdir/$name.yaml" 2>"$tmpdir/$name.log"; then
    printf 'Expected template validation to reject %s with schema validation bypassed\n' "$name" >&2
    exit 1
  fi
  assert_contains "$expected" "$tmpdir/$name.log"
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

# Exercise schema-valid defaults and both supported ingress controllers. Neither
# render opts into forwarded-prefix handling: ingress-enabled defaults must make
# long-poll redirect Locations routable. The community fixture uses two replicas
# and verifies that each pod gets its own Ingress; NGINX Inc uses one Ingress.
helm lint "$chart" --strict "${common_values[@]}" \
  --set ingress.enabled=true \
  --set global.ingress.controller=nginx-inc \
  --set replicaCount=1
helm lint "$chart" --strict --values "$chart/tests/forwarded-prefix-values.yaml"
"$chart/tests/forwarded-prefix-render.sh"
"$chart/tests/chart-contract-render.sh"

helm template bobs "$chart" "${common_values[@]}" >"$tmpdir/default.yaml"
assert_contains "image: \"eccr.ecmwf.int/bobs/bobs:${app_version}\"" "$tmpdir/default.yaml"
assert_contains 'value: "info"' "$tmpdir/default.yaml"
assert_contains 'max_live_spools: 256' "$tmpdir/default.yaml"
assert_contains 'max_spool_bytes: 8589934592' "$tmpdir/default.yaml"
assert_contains 'create_admission_timeout_ms: 5000' "$tmpdir/default.yaml"
assert_contains 'enable_pprof: false' "$tmpdir/default.yaml"
assert_contains 'whenDeleted: Retain' "$tmpdir/default.yaml"
assert_contains 'whenScaled: Delete' "$tmpdir/default.yaml"
assert_contains '- ReadWriteOnce' "$tmpdir/default.yaml"
assert_contains 'volumeMode: Filesystem' "$tmpdir/default.yaml"
assert_not_contains 'volumeDevices:' "$tmpdir/default.yaml"

helm template bobs "$chart" "${common_values[@]}" \
  --set global.imageRegistry=registry.example.com/team \
  --set image.repository=registry.internal/nested/bobs \
  --set-string image.tag= \
  --set-string image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --set-string rustLog=bobs=debug \
  --set persistence.enabled=false \
  >"$tmpdir/overrides.yaml"
assert_contains 'image: "registry.example.com/team/nested/bobs@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"' "$tmpdir/overrides.yaml"
assert_contains 'value: "bobs=debug"' "$tmpdir/overrides.yaml"
assert_contains 'sizeLimit: "10Gi"' "$tmpdir/overrides.yaml"

# A fully-qualified repository already under global.imageRegistry must not
# receive the registry project path a second time.
helm template bobs "$chart" "${common_values[@]}" \
  --set global.imageRegistry=eccr.ecmwf.int \
  --set image.repository=eccr.ecmwf.int/bobs \
  >"$tmpdir/qualified-repository.yaml"
assert_contains "image: \"eccr.ecmwf.int/bobs:${app_version}\"" "$tmpdir/qualified-repository.yaml"

helm template bobs "$chart" "${common_values[@]}" \
  --set persistence.accessModes[0]=ReadWriteMany \
  --set persistence.volumeMode=Filesystem \
  --set-string 'persistence.annotations.storage\.example\.com/owner=bobs' \
  --set persistence.retentionPolicy.whenDeleted=Delete \
  --set persistence.retentionPolicy.whenScaled=Retain \
  >"$tmpdir/persistence.yaml"
assert_contains 'storage.example.com/owner: bobs' "$tmpdir/persistence.yaml"
assert_contains '- ReadWriteMany' "$tmpdir/persistence.yaml"
assert_contains 'volumeMode: Filesystem' "$tmpdir/persistence.yaml"
assert_not_contains 'volumeDevices:' "$tmpdir/persistence.yaml"
assert_contains 'whenDeleted: Delete' "$tmpdir/persistence.yaml"
assert_contains 'whenScaled: Retain' "$tmpdir/persistence.yaml"

assert_schema_rejects block-volume --set persistence.volumeMode=Block
assert_template_rejects block-volume-template \
  'persistence.volumeMode must be Filesystem because BOBS requires a filesystem directory; raw Block volumes are unsupported' \
  --set persistence.volumeMode=Block

# Default and explicit-enabled renders preserve the existing <fullname>-svc name.
assert_count 1 'clusterIP: None' "$tmpdir/default.yaml"
assert_contains "  serviceName: 'bobs-svc'" "$tmpdir/default.yaml"
assert_contains "value: 'http://bobs-{ordinal}.bobs-svc:3000/api/v1'" "$tmpdir/default.yaml"
helm template bobs "$chart" "${common_values[@]}" \
  --set headlessService.enabled=true \
  --set-string headlessService.name= \
  >"$tmpdir/headless-enabled.yaml"
assert_contains '  name: "bobs-svc"' "$tmpdir/headless-enabled.yaml"
assert_contains "  serviceName: 'bobs-svc'" "$tmpdir/headless-enabled.yaml"

# A custom governing Service name drives the managed Service, StatefulSet, and
# stable pod DNS. Per-pod Services remain the ingress backends.
helm template bobs "$chart" "${common_values[@]}" \
  --set replicaCount=2 \
  --set headlessService.name=custom-governing \
  --set ingress.enabled=true \
  --set global.ingress.controller=nginx-inc \
  >"$tmpdir/headless-custom.yaml"
assert_contains '  name: "custom-governing"' "$tmpdir/headless-custom.yaml"
assert_contains "  serviceName: 'custom-governing'" "$tmpdir/headless-custom.yaml"
assert_contains "value: 'http://bobs-{ordinal}.custom-governing:3000/api/v1'" "$tmpdir/headless-custom.yaml"
assert_contains "name: 'bobs-0'" "$tmpdir/headless-custom.yaml"
assert_contains "name: 'bobs-1'" "$tmpdir/headless-custom.yaml"
assert_not_contains "name: 'custom-governing-0'" "$tmpdir/headless-custom.yaml"

# Disabling management omits only the headless Service and uses an existing
# external headless Service for StatefulSet identity and pod DNS.
helm template bobs "$chart" "${common_values[@]}" \
  --set replicaCount=2 \
  --set headlessService.enabled=false \
  --set headlessService.name=external-governing \
  --set ingress.enabled=true \
  --set global.ingress.controller=nginx-community \
  >"$tmpdir/headless-external.yaml"
assert_count 0 'clusterIP: None' "$tmpdir/headless-external.yaml"
assert_not_contains '  name: "external-governing"' "$tmpdir/headless-external.yaml"
assert_contains "  serviceName: 'external-governing'" "$tmpdir/headless-external.yaml"
assert_contains "value: 'http://bobs-{ordinal}.external-governing:3000/api/v1'" "$tmpdir/headless-external.yaml"
assert_contains "name: 'bobs-0'" "$tmpdir/headless-external.yaml"
assert_contains "name: 'bobs-1'" "$tmpdir/headless-external.yaml"

assert_schema_rejects external-service-missing \
  --set headlessService.enabled=false --set-string headlessService.name=
assert_template_rejects external-service-missing-template \
  'headlessService.name must be a non-empty external governing Service name when headlessService.enabled=false' \
  --set headlessService.enabled=false --set-string headlessService.name=
assert_schema_rejects governing-service-invalid --set-string headlessService.name=Bad_Name
assert_template_rejects governing-service-invalid-template \
  "headlessService.name must be a valid DNS-1123 label" \
  --set-string headlessService.name=Bad_Name

# Validate the exact final runtime bounds represented by the chart contract.
helm template bobs "$chart" "${common_values[@]}" \
  --set config.page_size=67108864 \
  --set config.max_cache_bytes=0 \
  --set config.max_live_spools=65536 \
  --set config.max_spool_bytes=67108864 \
  --set config.io_uring_shards=256 \
  --set config.io_uring_queue_capacity=2305843009213693951 \
  >"$tmpdir/bounds.yaml"

assert_schema_rejects page-size-high --set config.page_size=67108865
assert_schema_rejects cache-negative --set config.max_cache_bytes=-1
assert_schema_rejects live-spools-zero --set config.max_live_spools=0
assert_schema_rejects live-spools-high --set config.max_live_spools=65537
assert_schema_rejects spool-size-zero --set config.max_spool_bytes=0
assert_template_rejects page-larger-than-spool-template \
  'config.page_size must not exceed config.max_spool_bytes' \
  --set config.page_size=4096 --set config.max_spool_bytes=4095
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
