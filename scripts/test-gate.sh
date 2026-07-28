#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0
#
# test-gate.sh — local unit test for the CI gate logic.
#
# Stubs out `gh api` and version file reads so all four gate cases can be
# exercised without a real GitHub token or registry.

set -euo pipefail

PASS=0
FAIL=0

# ── Helpers ────────────────────────────────────────────────────────────────────

# run_gate CARGO_VERSION CHART_VERSION CARGO_RELEASED CHART_RELEASED
#   CARGO_RELEASED / CHART_RELEASED: 0 = not yet released, 1 = already released
#
# Prints the GITHUB_OUTPUT lines to stdout; prints the exit code as
# __exit__=N on the last line.  Stderr (gate error messages) is left on the
# terminal so failures are visible.
run_gate() {
    local cargo_version="$1"
    local chart_version="$2"
    local cargo_released="$3"
    local chart_released="$4"

    local tmpdir
    tmpdir=$(mktemp -d)
    # shellcheck disable=SC2064
    trap "rm -rf '${tmpdir}'" RETURN

    # Fake Cargo.toml
    cat > "${tmpdir}/Cargo.toml" <<EOF
[package]
name = "bobs"
version = "${cargo_version}"
edition = "2021"
EOF

    # Fake chart/Chart.yaml
    mkdir -p "${tmpdir}/chart"
    cat > "${tmpdir}/chart/Chart.yaml" <<EOF
apiVersion: v2
name: bobs-chart
version: ${chart_version}
appVersion: ${cargo_version}
EOF

    # Fake `gh` — returns the pre-set count for each version tag
    mkdir -p "${tmpdir}/bin"
    cat > "${tmpdir}/bin/gh" <<EOF
#!/usr/bin/env bash
url="\$2"
version="\${url##*/tags/}"
# The gate queries the .dev0-suffixed tag (see release-tag job); strip it.
version="\${version%.dev0}"
if   [ "\${version}" = "${cargo_version}" ]; then echo "${cargo_released}"
elif [ "\${version}" = "${chart_version}" ]; then echo "${chart_released}"
else echo "0"
fi
EOF
    chmod +x "${tmpdir}/bin/gh"

    local gha_output="${tmpdir}/gha_output"
    touch "${gha_output}"

    # Run the gate in a subshell with set -e disabled so we can capture the
    # exit code cleanly.  Stdout (GITHUB_OUTPUT content) goes to tmpdir/out;
    # stderr (gate error messages) is left on the terminal.
    local out_file="${tmpdir}/out"
    local exit_code=0
    (
        set +e
        cd "${tmpdir}"
        export PATH="${tmpdir}/bin:${PATH}"
        export GITHUB_OUTPUT="${gha_output}"
        export GITHUB_REPOSITORY="ecmwf/bobs"

        bash -s <<'GATE'
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
GATE
    ) > "${out_file}" 2>&1
    exit_code=$?

    # Emit the gate's stdout (the console log line, not GITHUB_OUTPUT)
    cat "${out_file}"
    # Emit GITHUB_OUTPUT contents
    cat "${gha_output}"
    # Emit exit code as a parseable sentinel
    echo "__exit__=${exit_code}"
}

# check LABEL OUTPUT EXPECTED_EXIT [key=value ...]
check() {
    local label="$1"
    local output="$2"
    local expect_exit="$3"
    shift 3

    local actual_exit
    actual_exit=$(printf '%s\n' "${output}" | grep '^__exit__=' | cut -d= -f2)

    local ok=1

    if [ "${actual_exit}" != "${expect_exit}" ]; then
        echo "FAIL [${label}]: expected exit ${expect_exit}, got ${actual_exit}"
        ok=0
    fi

    for kv in "$@"; do
        local key="${kv%%=*}" val="${kv#*=}"
        if ! printf '%s\n' "${output}" | grep -q "^${key}=${val}$"; then
            echo "FAIL [${label}]: expected output '${kv}' not found"
            printf '  outputs: %s\n' "$(printf '%s\n' "${output}" | grep -v '^__exit__')"
            ok=0
        fi
    done

    if [ "${ok}" -eq 1 ]; then
        echo "PASS [${label}]"
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
    fi
}

# ── Tests ──────────────────────────────────────────────────────────────────────

echo "Running gate logic tests..."
echo

# Case 1: both already tagged → skip everything
out=$(run_gate "0.1.3" "0.1.3" 1 1)
check "both already tagged → skip" "${out}" 0 \
    "publish_image=false" "publish_chart=false"

# Case 2: code bumped, chart not bumped → error
out=$(run_gate "0.1.4" "0.1.3" 0 1)
check "code bumped, chart not bumped → error" "${out}" 1

# Case 3: both bumped to matching versions → full release
out=$(run_gate "0.1.4" "0.1.4" 0 0)
check "both bumped, versions match → full release" "${out}" 0 \
    "publish_image=true" "publish_chart=true"

# Case 4: both bumped but versions differ → error
out=$(run_gate "0.1.4" "0.1.5" 0 0)
check "both bumped, versions differ → error" "${out}" 1

# Case 5: chart-only bump → publish chart only
out=$(run_gate "0.1.3" "0.1.4" 1 0)
check "chart-only bump → chart only" "${out}" 0 \
    "publish_image=false" "publish_chart=true"

# ── Summary ────────────────────────────────────────────────────────────────────

echo
echo "${PASS} passed, ${FAIL} failed"
[ "${FAIL}" -eq 0 ]
