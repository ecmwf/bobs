#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0
#
# check-versions.sh — verify that all version fields that must agree actually do.
#
# Single-version policy: the image (Cargo.toml) and the chart ship together, so
# every version field below must equal Cargo.toml::package.version:
#   chart/Chart.yaml::version        (chart package version)
#   chart/Chart.yaml::appVersion
#   chart/values.yaml::image.tag
#   CITATION.cff::version

set -euo pipefail

# ── Parse versions ─────────────────────────────────────────────────────────────

CARGO_VERSION=$(python3 -c "
import tomllib
with open('Cargo.toml', 'rb') as f:
    print(tomllib.load(f)['package']['version'])
")

CHART_VERSION=$(awk '/^version:/{print $2; exit}' chart/Chart.yaml | tr -d "'\"")
CHART_APP_VERSION=$(awk '/^appVersion:/{print $2; exit}' chart/Chart.yaml | tr -d "'\"")
VALUES_IMAGE_TAG=$(awk '/^image:/{f=1} f && /^[[:space:]]+tag:/{gsub(/[[:space:]]/, "", $2); print $2; exit}' chart/values.yaml | tr -d "'\"")
CITATION_VERSION=$(awk '/^version:/{print $2; exit}' CITATION.cff | tr -d "'\"")

# ── Validate extraction ────────────────────────────────────────────────────────

for _var in CARGO_VERSION CHART_VERSION CHART_APP_VERSION VALUES_IMAGE_TAG CITATION_VERSION; do
    _val="${!_var}"
    if [ -z "${_val}" ]; then
        echo "ERROR: Failed to extract ${_var} (empty result — check the source file)"
        exit 1
    fi
done

# ── Report ─────────────────────────────────────────────────────────────────────

echo "Versions found:"
printf '  %-40s %s\n' 'Cargo.toml::package.version'       "${CARGO_VERSION}"
printf '  %-40s %s\n' 'chart/Chart.yaml::version'          "${CHART_VERSION}"
printf '  %-40s %s\n' 'chart/Chart.yaml::appVersion'       "${CHART_APP_VERSION}"
printf '  %-40s %s\n' 'chart/values.yaml::image.tag'       "${VALUES_IMAGE_TAG}"
printf '  %-40s %s\n' 'CITATION.cff::version'              "${CITATION_VERSION}"
echo

# ── Check ──────────────────────────────────────────────────────────────────────

FAIL=0

check() {
    local name_a="$1" val_a="$2" name_b="$3" val_b="$4"
    if [ "$val_a" != "$val_b" ]; then
        echo "MISMATCH: ${name_a} (${val_a}) != ${name_b} (${val_b})"
        FAIL=1
    else
        echo "OK:       ${name_a} == ${name_b} == ${val_a}"
    fi
}

check 'Cargo.toml::package.version' "${CARGO_VERSION}" \
      'chart/Chart.yaml::version'      "${CHART_VERSION}"

check 'Cargo.toml::package.version' "${CARGO_VERSION}" \
      'chart/Chart.yaml::appVersion'  "${CHART_APP_VERSION}"

check 'Cargo.toml::package.version' "${CARGO_VERSION}" \
      'chart/values.yaml::image.tag'  "${VALUES_IMAGE_TAG}"

check 'Cargo.toml::package.version' "${CARGO_VERSION}" \
      'CITATION.cff::version'         "${CITATION_VERSION}"

if [ "${FAIL}" -ne 0 ]; then
    echo
    echo "Version mismatch(es) detected. All five fields must agree before merging."
    exit 1
fi

echo
echo "All version fields match: ${CARGO_VERSION}"
