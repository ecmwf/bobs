#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0
#
# gate.sh — CI publish gate.
#
# Reads Cargo.toml and chart/Chart.yaml from the current directory, asks the
# registry whether the image and the chart at those versions already exist,
# and writes publish_image, publish_chart, cargo_version, and chart_version
# to $GITHUB_OUTPUT.
#
# The registry is the source of truth. Git tags only mark a completed release
# and are never consulted, so a partially published or untagged release is
# retried on the next push and a chart-only version number cannot shadow a
# later code version.
#
# Required environment variables:
#   GITHUB_OUTPUT           — path to the GHA output file (set by GitHub Actions;
#                             mocked by test-gate.sh locally)
# Optional:
#   IMAGE_REPOSITORY        — default eccr.ecmwf.int/bobs/bobs
#   CHART_REPOSITORY        — default oci://eccr.ecmwf.int/bobs/bobs-chart
#   PREVIOUS_CARGO_VERSION  — Cargo version before this push; derived from
#                             `git show HEAD~1:Cargo.toml` when unset

set -euo pipefail

IMAGE_REPOSITORY="${IMAGE_REPOSITORY:-eccr.ecmwf.int/bobs/bobs}"
CHART_REPOSITORY="${CHART_REPOSITORY:-oci://eccr.ecmwf.int/bobs/bobs-chart}"

cargo_version_of() {
  python3 -c 'import sys, tomllib; print(tomllib.loads(sys.stdin.read())["package"]["version"])'
}

CARGO_VERSION=$(cargo_version_of < Cargo.toml)
CHART_VERSION=$(awk '/^version:/{print $2; exit}' chart/Chart.yaml | tr -d "'\"")
if [ -z "${PREVIOUS_CARGO_VERSION:-}" ]; then
  if git rev-parse --verify --quiet HEAD~1 >/dev/null; then
    PREVIOUS_CARGO_VERSION=$(git show HEAD~1:Cargo.toml | cargo_version_of)
  else
    PREVIOUS_CARGO_VERSION="${CARGO_VERSION}"
  fi
fi
echo "cargo_version=${CARGO_VERSION}" >> "${GITHUB_OUTPUT}"
echo "chart_version=${CHART_VERSION}" >> "${GITHUB_OUTPUT}"

# published CMD... → 0 if the artifact exists, 1 if the registry says it does
# not, and aborts on any other failure (auth, network) so an unreachable
# registry can never be mistaken for "not published" and lead to an overwrite.
published() {
  local out
  if out=$("$@" 2>&1); then
    return 0
  fi
  if grep -qiE 'not found|MANIFEST_UNKNOWN|NAME_UNKNOWN|no such manifest' <<< "${out}"; then
    return 1
  fi
  echo "ERROR: could not determine whether the artifact is published:" >&2
  echo "${out}" >&2
  exit 1
}

image_published=0
chart_published=0
if published docker manifest inspect "${IMAGE_REPOSITORY}:${CARGO_VERSION}"; then image_published=1; fi
if published helm show chart "${CHART_REPOSITORY}" --version "${CHART_VERSION}"; then chart_published=1; fi

if [ "${image_published}" -eq 1 ] && [ "${chart_published}" -eq 1 ]; then
  echo "Nothing to publish: image ${CARGO_VERSION} and chart ${CHART_VERSION} are both in the registry."
  echo "publish_image=false" >> "${GITHUB_OUTPUT}"
  echo "publish_chart=false" >> "${GITHUB_OUTPUT}"
elif [ "${image_published}" -eq 0 ] && [ "${chart_published}" -eq 1 ]; then
  echo "ERROR: image ${CARGO_VERSION} is not published but chart ${CHART_VERSION} already is." >&2
  echo "A code release must republish the chart with the new image tag: bump chart/Chart.yaml version to ${CARGO_VERSION}." >&2
  exit 1
elif [ "${image_published}" -eq 0 ] && [ "${chart_published}" -eq 0 ]; then
  if [ "${CHART_VERSION}" != "${CARGO_VERSION}" ]; then
    echo "ERROR: code+chart release, but versions differ: Cargo=${CARGO_VERSION}, chart=${CHART_VERSION}." >&2
    echo "They must match on a code+chart release." >&2
    exit 1
  fi
  echo "Code+chart release at ${CARGO_VERSION}: publishing image and chart."
  echo "publish_image=true" >> "${GITHUB_OUTPUT}"
  echo "publish_chart=true" >> "${GITHUB_OUTPUT}"
else
  # Chart-only: the chart must keep pointing at the image it already pointed
  # at. Changing Cargo.toml to a version that is already published would ship
  # this commit's chart against an image built from other source.
  if [ "${CARGO_VERSION}" != "${PREVIOUS_CARGO_VERSION}" ]; then
    echo "ERROR: Cargo version changed ${PREVIOUS_CARGO_VERSION} -> ${CARGO_VERSION}, but image ${CARGO_VERSION} is already published." >&2
    echo "A code release needs a new, unpublished version; a chart-only release must leave Cargo.toml unchanged." >&2
    exit 1
  fi
  echo "Chart-only release: publishing chart ${CHART_VERSION} (image stays at ${CARGO_VERSION})."
  echo "publish_image=false" >> "${GITHUB_OUTPUT}"
  echo "publish_chart=true" >> "${GITHUB_OUTPUT}"
fi
