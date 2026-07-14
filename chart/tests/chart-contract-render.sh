#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

chart_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

common_values=(--set config.host_prefix=bobs --set config.domain=example.test)

assert_contains() {
	local expected=$1 file=$2
	if ! grep -Fq -- "$expected" "$file"; then
		printf 'Expected rendered chart to contain: %s\n' "$expected" >&2
		exit 1
	fi
}

assert_template_rejects() {
	local name=$1 expected=$2
	shift 2
	if helm template bobs "$chart_dir" --skip-schema-validation "${common_values[@]}" "$@" >"$tmp_dir/$name.yaml" 2>"$tmp_dir/$name.log"; then
		printf 'Expected template validation to reject %s with schema validation bypassed\n' "$name" >&2
		exit 1
	fi
	assert_contains "$expected" "$tmp_dir/$name.log"
}

assert_schema_rejects() {
	local name=$1
	shift
	if helm template bobs "$chart_dir" "${common_values[@]}" "$@" >"$tmp_dir/$name.yaml" 2>"$tmp_dir/$name.log"; then
		printf 'Expected schema validation to reject %s\n' "$name" >&2
		exit 1
	fi
	assert_contains "values don't meet the specifications" "$tmp_dir/$name.log"
}

assert_generated_name_contract() {
	local file=$1 fullname=$2 governing_name=$3 replicas=$4
	python3 - "$file" "$fullname" "$governing_name" "$replicas" <<'PY'
import re
import sys

path, fullname, governing_name, replicas_raw = sys.argv[1:]
replicas = int(replicas_raw)
text = open(path, encoding="utf-8").read()

resources = []
for document in re.split(r"^---\s*$", text, flags=re.MULTILINE):
    kind_match = re.search(r"^kind:\s*([^\s]+)\s*$", document, re.MULTILINE)
    name_match = re.search(r"^metadata:\s*\n(?:^[ ]{2}[^\n]*\n)*?^[ ]{2}name:\s*['\"]?([^'\"\s]+)['\"]?\s*$", document, re.MULTILINE)
    if kind_match and name_match:
        resources.append((kind_match.group(1), name_match.group(1)))

if not resources:
    raise SystemExit("no rendered Kubernetes resource names found")

dns_label = re.compile(r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$")

seen = set()
for kind, name in resources:
    if len(name) > 63 or not dns_label.fullmatch(name):
        raise SystemExit(f"invalid {kind} metadata.name {name!r} ({len(name)} characters)")
    identity = (kind, name)
    if identity in seen:
        raise SystemExit(f"duplicate rendered resource {kind}/{name}")
    seen.add(identity)

statefulsets = [name for kind, name in resources if kind == "StatefulSet"]
if len(statefulsets) != 1:
    raise SystemExit(f"expected one StatefulSet, found {statefulsets}")
statefulset = statefulsets[0]
if len(statefulset) > 47:
    raise SystemExit(f"StatefulSet base leaves insufficient ordinal/PVC suffix space: {statefulset}")

pod_names = [f"{statefulset}-{ordinal}" for ordinal in range(replicas)]
service_names = [name for kind, name in resources if kind == "Service"]
expected_services = [fullname, governing_name, *pod_names]
if sorted(service_names) != sorted(expected_services):
    raise SystemExit(f"unexpected Service names: {service_names}")

ingress_names = [name for kind, name in resources if kind == "Ingress"]
if sorted(ingress_names) != sorted(pod_names):
    raise SystemExit(f"per-pod Ingress names do not match Services: {ingress_names}")

for ordinal, pod_name in enumerate(pod_names):
    if not pod_name.endswith(f"-{ordinal}"):
        raise SystemExit(f"ordinal suffix was not preserved: {pod_name}")
    pvc_name = f"data-{pod_name}"
    if len(pvc_name) > 63 or not dns_label.fullmatch(pvc_name):
        raise SystemExit(f"controller-generated PVC name is invalid: {pvc_name}")
PY
}

assert_schema_rejects data-dir-empty --set-string config.data_dir=
assert_schema_rejects data-dir-relative --set-string config.data_dir=./data
assert_template_rejects data-dir-empty-template \
	'config.data_dir must be a non-empty absolute filesystem path' \
	--set-string config.data_dir=
assert_template_rejects data-dir-relative-template \
	'config.data_dir must be a non-empty absolute filesystem path' \
	--set-string config.data_dir=./data

assert_schema_rejects fullname-invalid --set-string fullnameOverride=Bad_Name
assert_template_rejects fullname-invalid-template \
	'fullnameOverride must be a valid DNS-1123 label' \
	--set-string fullnameOverride=Bad_Name

printf -v long_fullname '%*s' 63 ''
long_fullname=${long_fullname// /a}
too_long_fullname=$(printf '%s%s' "$long_fullname" a)
assert_schema_rejects fullname-too-long --set-string fullnameOverride="$too_long_fullname"
assert_template_rejects fullname-too-long-template \
	'fullnameOverride must be a valid DNS-1123 label' \
	--set-string fullnameOverride="$too_long_fullname"
printf -v long_headless_name '%*s' 63 ''
long_headless_name=${long_headless_name// /c}
helm template bobs "$chart_dir" "${common_values[@]}" \
	--set replicaCount=12 \
	--set-string fullnameOverride="$long_fullname" \
	--set-string headlessService.name="$long_headless_name" \
	--set ingress.enabled=true \
	--set ingress.forwardedPrefix.enabled=true \
	--set global.ingress.controller=nginx-community \
	>"$tmp_dir/long-names.yaml"
assert_generated_name_contract "$tmp_dir/long-names.yaml" "$long_fullname" "$long_headless_name" 12
assert_contains "  serviceName: '$long_headless_name'" "$tmp_dir/long-names.yaml"

printf -v long_release '%*s' 53 ''
long_release=${long_release// /r}
long_release_fullname=${long_release}-bobs
helm template "$long_release" "$chart_dir" "${common_values[@]}" \
	--set replicaCount=12 \
	--set-string headlessService.name="$long_headless_name" \
	--set ingress.enabled=true \
	--set ingress.forwardedPrefix.enabled=true \
	--set global.ingress.controller=nginx-community \
	>"$tmp_dir/long-release.yaml"
assert_generated_name_contract "$tmp_dir/long-release.yaml" "$long_release_fullname" "$long_headless_name" 12

dotted_release=prod.v1
dotted_release_digest=$(printf '%s' "$dotted_release" | sha256sum)
dotted_release_digest=${dotted_release_digest%% *}
dotted_release_digest=${dotted_release_digest:0:8}
dotted_release_name=prod-v1-${dotted_release_digest}
dotted_release_fullname=${dotted_release_name}-bobs
helm template "$dotted_release" "$chart_dir" "${common_values[@]}" \
	--set replicaCount=12 \
	--set ingress.enabled=true \
	--set ingress.forwardedPrefix.enabled=true \
	--set global.ingress.controller=nginx-community \
	>"$tmp_dir/dotted-release.yaml"
assert_generated_name_contract \
	"$tmp_dir/dotted-release.yaml" \
	"$dotted_release_fullname" \
	"$dotted_release_fullname-svc" \
	12

assert_template_rejects headless-main-service-collision \
	'headlessService.name resolves to "shared" and collides with the generated main Service' \
	--set-string fullnameOverride=shared --set-string headlessService.name=shared
assert_template_rejects headless-pod-service-collision \
	'headlessService.name resolves to "bobs-10" and collides with the generated per-pod Service for ordinal 10' \
	--set replicaCount=12 --set-string headlessService.name=bobs-10

helm template bobs "$chart_dir" "${common_values[@]}" \
	--set config.max_live_spools=65536 \
	>"$tmp_dir/live-spools-bound.yaml"
assert_contains 'max_live_spools: 65536' "$tmp_dir/live-spools-bound.yaml"
assert_schema_rejects live-spools-high --set config.max_live_spools=65537
assert_template_rejects live-spools-high-template \
	'config.max_live_spools must not exceed 65536' \
	--set config.max_live_spools=65537

helm template bobs "$chart_dir" "${common_values[@]}" \
	--set config.port=4321 \
	--set service.port=4321 \
	--set config.metrics.enabled=true \
	--set config.metrics.port=4322 \
	>"$tmp_dir/metrics-ports.yaml"
assert_contains 'containerPort: 4321' "$tmp_dir/metrics-ports.yaml"
assert_contains 'containerPort: 4322' "$tmp_dir/metrics-ports.yaml"
assert_template_rejects metrics-port-collision \
	'config.metrics.port must differ from config.port when metrics are enabled' \
	--set config.port=4321 --set config.metrics.enabled=true --set config.metrics.port=4321
