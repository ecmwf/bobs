#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

chart_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
values_file="$chart_dir/tests/forwarded-prefix-values.yaml"
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

assert_count() {
	local expected=$1
	local needle=$2
	local file=$3
	local actual
	actual=$(grep -Fc -- "$needle" "$file" || true)
	if [[ "$actual" != "$expected" ]]; then
		printf 'expected %s occurrences of %q in %s, found %s\n' \
			"$expected" "$needle" "$file" "$actual" >&2
		exit 1
	fi
}

community="$tmp_dir/community.yaml"
helm template forwarded-prefix "$chart_dir" \
	--values "$values_file" \
	--show-only templates/ingress.yaml >"$community"

# ingress-nginx's native prefix annotation is per Ingress, so each replica gets
# its exact public route. User annotations survive on every generated object.
assert_count 2 'kind: Ingress' "$community"
assert_count 1 'nginx.ingress.kubernetes.io/x-forwarded-prefix: /download-0' "$community"
assert_count 1 'nginx.ingress.kubernetes.io/x-forwarded-prefix: /download-1' "$community"
assert_count 2 'example.com/preserved: kept' "$community"
assert_count 1 'path: "/download-0/(api/v1/read/|api/v1/)?([0-9a-zA-Z-]+)$"' "$community"
assert_count 1 'path: "/download-1/(api/v1/read/|api/v1/)?([0-9a-zA-Z-]+)$"' "$community"
assert_count 2 'nginx.ingress.kubernetes.io/rewrite-target: /api/v1/read/$2' "$community"

# Exercise the rendered redirect contract: the public Location produced by BOBS
# must match the ingress path and rewrite back to the internal read endpoint.
python3 - "$community" <<'PY'
import re
import sys

rendered = open(sys.argv[1], encoding="utf-8").read().splitlines()
path_line = next(
    line.strip()
    for line in rendered
    if line.strip().startswith('- path: "/download-0/')
)
pattern = path_line.split('"', 2)[1]
rewrite_line = next(
    line.strip()
    for line in rendered
    if line.strip().startswith("nginx.ingress.kubernetes.io/rewrite-target:")
)
rewrite = rewrite_line.split(": ", 1)[1]
public_location = "/download-0/api/v1/read/abc-123"
match = re.fullmatch(pattern, public_location)
assert match is not None, (pattern, public_location)
internal_location = rewrite.replace("$2", match.group(2))
assert internal_location == "/api/v1/read/abc-123", internal_location
PY

nginx_inc="$tmp_dir/nginx-inc.yaml"
helm template forwarded-prefix "$chart_dir" \
	--values "$values_file" \
	--set global.ingress.controller=nginx-inc \
	--show-only templates/ingress.yaml >"$nginx_inc"

# NGINX Inc supports a location snippet, which derives the same exact pod prefix
# dynamically while retaining unrelated user annotations.
assert_count 1 'if ($uri ~ ^(/download-\d+)(?:/|$)) {' "$nginx_inc"
assert_count 1 'proxy_set_header X-Forwarded-Prefix $bobs_forwarded_prefix;' "$nginx_inc"
assert_count 1 'example.com/preserved: kept' "$nginx_inc"
