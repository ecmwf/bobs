#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0
#
# gate.sh — CI publish gate.
#
# Reads Cargo.toml and chart/Chart.yaml from the current directory, checks
# whether release tags already exist, and writes publish_image, publish_chart,
# cargo_version, and chart_version to $GITHUB_OUTPUT.
#
# Required environment variables:
#   GITHUB_OUTPUT      — path to the GHA output file (set automatically by
#                        GitHub Actions; mocked by test-gate.sh locally)
#   GITHUB_REPOSITORY  — owner/repo slug (e.g. ecmwf/bobs)
#   GH_TOKEN           — GitHub token with repo read access (for gh api calls)

set -euo pipefail

CARGO_VERSION=$(python3 -c 'import tomllib; print(tomllib.load(open("Cargo.toml", "rb"))["package"]["version"])')
CHART_VERSION=$(awk '/^version:/{print $2; exit}' chart/Chart.yaml | tr -d "'\"")
echo "cargo_version=${CARGO_VERSION}" >> "${GITHUB_OUTPUT}"
echo "chart_version=${CHART_VERSION}" >> "${GITHUB_OUTPUT}"

cargo_released=$(gh api "repos/${GITHUB_REPOSITORY}/git/matching-refs/tags/${CARGO_VERSION}.dev0" --jq 'length')
chart_released=$(gh api "repos/${GITHUB_REPOSITORY}/git/matching-refs/tags/${CHART_VERSION}.dev0" --jq 'length')

if [ "${cargo_released}" -gt 0 ] && [ "${chart_released}" -gt 0 ]; then
  echo "Nothing to publish: cargo ${CARGO_VERSION} and chart ${CHART_VERSION} are both already tagged."
  echo "publish_image=false" >> "${GITHUB_OUTPUT}"
  echo "publish_chart=false" >> "${GITHUB_OUTPUT}"
elif [ "${cargo_released}" -eq 0 ] && [ "${chart_released}" -gt 0 ]; then
  echo "ERROR: Cargo version ${CARGO_VERSION} was bumped but chart version ${CHART_VERSION} already has a release tag." >&2
  echo "Bump chart/Chart.yaml version alongside the code." >&2
  exit 1
elif [ "${cargo_released}" -eq 0 ] && [ "${chart_released}" -eq 0 ]; then
  if [ "${CHART_VERSION}" != "${CARGO_VERSION}" ]; then
    echo "ERROR: Both code and chart were bumped, but versions differ: Cargo=${CARGO_VERSION}, chart=${CHART_VERSION}." >&2
    echo "They must match on a full code+chart release." >&2
    exit 1
  fi
  echo "Code+chart release at ${CARGO_VERSION}: publishing image and chart."
  echo "publish_image=true" >> "${GITHUB_OUTPUT}"
  echo "publish_chart=true" >> "${GITHUB_OUTPUT}"
else
  echo "Chart-only release: publishing chart at ${CHART_VERSION} (image stays at ${CARGO_VERSION})."
  echo "publish_image=false" >> "${GITHUB_OUTPUT}"
  echo "publish_chart=true" >> "${GITHUB_OUTPUT}"
fi
