#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

chart_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

chart_version=$(awk '/^version:/{print $2; exit}' "$chart_dir/Chart.yaml" | tr -d "'"")
app_version=$(awk '/^appVersion:/{print $2; exit}' "$chart_dir/Chart.yaml" | tr -d "'"")

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

render_config_checksum() {
	local name=$1
	shift
	local render="$tmp_dir/checksum-$name.yaml"
	helm template bobs "$chart_dir" "${common_values[@]}" \
		--show-only templates/statefulset.yaml "$@" >"$render"
	python3 - "$render" <<'PY'
import re
import sys

text = open(sys.argv[1], encoding="utf-8").read()
checksums = re.findall(
    r'^\s+checksum/config:\s*["\x27]?([0-9a-f]{64})["\x27]?\s*$',
    text,
    flags=re.MULTILINE,
 )
if len(checksums) != 1:
    raise SystemExit(f"expected exactly one checksum/config annotation, found {checksums}")
print(checksums[0])
PY
}

assert_checksum_differs() {
	local baseline=$1 actual=$2 description=$3
	if [[ "$baseline" == "$actual" ]]; then
		printf 'Expected %s to alter checksum/config (%s)\n' "$description" "$baseline" >&2
		exit 1
	fi
}

assert_checksum_matches() {
	local baseline=$1 actual=$2 description=$3
	if [[ "$baseline" != "$actual" ]]; then
		printf 'Expected %s not to alter checksum/config (%s != %s)\n' \
			"$description" "$baseline" "$actual" >&2
		exit 1
	fi
}

assert_label_strings() {
	local file=$1 release_name=$2 app_name=$3
	assert_contains "app.kubernetes.io/instance: \"$release_name\"" "$file"
	assert_contains "app.kubernetes.io/name: \"$app_name\"" "$file"
	assert_contains "helm.sh/chart: \"bobs-chart-${chart_version}\"" "$file"
	assert_contains "app.kubernetes.io/version: \"${app_version}\"" "$file"
	assert_contains 'app.kubernetes.io/managed-by: "Helm"' "$file"
	assert_contains 'boolean-like: "true"' "$file"
	assert_contains 'false-like: "false"' "$file"
	assert_contains 'null-like: "null"' "$file"
	assert_contains 'numeric-like: "123"' "$file"
	if grep -Fq -- "app.kubernetes.io/instance: $release_name" "$file"; then
		printf 'Found unquoted release label value %s in %s\n' "$release_name" "$file" >&2
		exit 1
	fi
	if grep -Fq -- "app.kubernetes.io/name: $app_name" "$file"; then
		printf 'Found unquoted app label value %s in %s\n' "$app_name" "$file" >&2
		exit 1
	fi
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

# checksum/config is a digest of the canonical ConfigMap payload only. Every
# represented runtime-config category below changes it, while repeat renders and
# values outside ConfigMap data do not.
baseline_checksum=$(render_config_checksum baseline)
assert_checksum_matches "$baseline_checksum" "$(render_config_checksum repeat)" "an identical render"
assert_checksum_differs "$baseline_checksum" "$(render_config_checksum page --set config.page_size=8192)" "config.page_size"
assert_checksum_differs "$baseline_checksum" "$(render_config_checksum ttl --set config.read_idle_ttl_secs=601)" "config.read_idle_ttl_secs"
assert_checksum_differs "$baseline_checksum" "$(render_config_checksum routing --set-string config.route_name=download-v2)" "config.route_name"
assert_checksum_differs "$baseline_checksum" "$(render_config_checksum pprof --set config.enable_pprof=true)" "config.enable_pprof"
assert_checksum_matches "$baseline_checksum" "$(render_config_checksum replicas --set replicaCount=2)" "replicaCount"
assert_checksum_matches "$baseline_checksum" "$(render_config_checksum image --set-string image.tag=unrelated)" "image.tag"
assert_checksum_matches "$baseline_checksum" "$(render_config_checksum persistence --set-string persistence.size=20Gi)" "persistence.size"
assert_checksum_matches "$baseline_checksum" "$(render_config_checksum fullname --set-string fullnameOverride=renamed-bobs)" "ConfigMap metadata/name changes"
assert_checksum_matches "$baseline_checksum" "$(render_config_checksum annotation --set-string 'podAnnotations.example\.com/unrelated=yes')" "podAnnotations"
assert_checksum_matches "$baseline_checksum" "$(render_config_checksum reserved-annotation --set-string 'podAnnotations.checksum/config=override')" "a user checksum/config override"

kubeconform_bin=${KUBECONFORM_BIN:-kubeconform}
if ! command -v "$kubeconform_bin" >/dev/null 2>&1; then
	printf 'kubeconform is required for strict Kubernetes schema tests\n' >&2
	exit 1
fi

label_renders=()
for release_name in true false null 123 1e3; do
	label_render="$tmp_dir/labels-$release_name.yaml"
	service_monitor_render="$tmp_dir/service-monitor-labels-$release_name.yaml"
	ambiguous_label_values=(
		--set-string nameOverride="$release_name"
		--set-string fullnameOverride="$release_name-full"
		--set ingress.enabled=true
		--set global.ingress.controller=nginx-community
		--set config.metrics.enabled=true
		--set config.metrics.serviceMonitor.enabled=true
		--set-string config.metrics.serviceMonitor.labels.boolean-like=true
		--set-string config.metrics.serviceMonitor.labels.false-like=false
		--set-string config.metrics.serviceMonitor.labels.null-like=null
		--set-string config.metrics.serviceMonitor.labels.numeric-like=123
	)
	helm template "$release_name" "$chart_dir" "${common_values[@]}" \
		"${ambiguous_label_values[@]}" >"$label_render"
	helm template "$release_name" "$chart_dir" "${common_values[@]}" \
		"${ambiguous_label_values[@]}" \
		--show-only templates/servicemonitor.yaml >"$service_monitor_render"
	assert_label_strings "$label_render" "$release_name" "$release_name"
	assert_label_strings "$service_monitor_render" "$release_name" "$release_name"
	label_renders+=("$label_render")
done
crd_schema_location='https://raw.githubusercontent.com/datreeio/CRDs-catalog/34cef0fc2698bbb611f475515e555a2d7ca85b6c/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json'
cat "${label_renders[@]}" | "$kubeconform_bin" \
	-strict -kubernetes-version 1.31.0 -summary \
	-schema-location default -schema-location "$crd_schema_location"

safe_data_dirs=(
	/var/lib/bobs
	/var/lib/bobs/spools
	/var/lib/bobs/tenant-a/spools.v1
)
for index in "${!safe_data_dirs[@]}"; do
	safe_data_dir=${safe_data_dirs[$index]}
	safe_render="$tmp_dir/data-dir-safe-$index.yaml"
	helm template bobs "$chart_dir" "${common_values[@]}" \
		--set-string config.data_dir="$safe_data_dir" >"$safe_render"
	assert_contains "mountPath: \"$safe_data_dir\"" "$safe_render"
done

unsafe_data_dir_names=(
	empty
	relative
	root
	usr
	usr-local-bin
	etc
	tmp
	prefix-lookalike
	parent-alias
	dot-alias
	repeated-slash-root
	repeated-slash-descendant
	trailing-slash
)
unsafe_data_dirs=(
	""
	./data
	/
	/usr
	/usr/local/bin
	/etc
	/tmp
	/var/lib/bobs-data
	/var/lib/bobs/../bobs
	/var/lib/bobs/./spools
	/var/lib//bobs
	/var/lib/bobs//spools
	/var/lib/bobs/
)
for index in "${!unsafe_data_dirs[@]}"; do
	name=${unsafe_data_dir_names[$index]}
	unsafe_data_dir=${unsafe_data_dirs[$index]}
	assert_schema_rejects "data-dir-$name" \
		--set-string config.data_dir="$unsafe_data_dir"
	assert_template_rejects "data-dir-$name-template" \
		'config.data_dir must be /var/lib/bobs or a normalized descendant' \
		--show-only templates/statefulset.yaml \
		--set-string config.data_dir="$unsafe_data_dir"
done

valid_route=$(printf 'r%.0s' {1..63})
helm template bobs "$chart_dir" "${common_values[@]}" \
	--set-string config.route_name="$valid_route" \
	--set ingress.enabled=true \
	--set global.ingress.controller=nginx-inc \
	>"$tmp_dir/route-valid.yaml"
assert_contains "route_name: \"$valid_route\"" "$tmp_dir/route-valid.yaml"
assert_contains "path: \"/$valid_route-0\"" "$tmp_dir/route-valid.yaml"

too_long_route=${valid_route}r
invalid_routes=(
	'bad/route'
	'bad.route'
	'bad*route'
	'bad$route'
	'bad"route'
	"bad'route"
	'bad-route-'
	"$too_long_route"
	$'bad\nroute'
)
for index in "${!invalid_routes[@]}"; do
	invalid_route=${invalid_routes[$index]}
	assert_schema_rejects "route-invalid-$index" --set-string config.route_name="$invalid_route"
	assert_template_rejects "route-invalid-$index-template" \
		"config.route_name must be 1-63 characters" \
		--set-string config.route_name="$invalid_route"
done

assert_schema_rejects ingress-forwarded-prefix-disabled \
	--set ingress.enabled=true --set ingress.forwardedPrefix.enabled=false
assert_template_rejects ingress-forwarded-prefix-disabled-template \
	'ingress.forwardedPrefix.enabled must be true when ingress.enabled=true so long-poll redirect Location paths remain routable' \
	--set ingress.enabled=true --set ingress.forwardedPrefix.enabled=false

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
