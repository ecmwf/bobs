#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0
#
# test-gate.sh — local unit test for the CI gate logic.
#
# Stubs out the registry lookups (`docker manifest inspect`, `helm show chart`)
# and the version files so every gate case can be exercised offline.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

PASS=0
FAIL=0

# ── Helpers ────────────────────────────────────────────────────────────────────

# run_gate CARGO_VERSION CHART_VERSION IMAGE_PUBLISHED CHART_PUBLISHED [PREVIOUS_CARGO_VERSION]
#   IMAGE_PUBLISHED / CHART_PUBLISHED: 0 = not in the registry, 1 = published,
#   "error" = registry unreachable (any failure other than "not found")
#   PREVIOUS_CARGO_VERSION defaults to CARGO_VERSION (no Cargo change in the push).
#
# Prints the GITHUB_OUTPUT lines to stdout; prints the exit code as
# __exit__=N on the last line.  Stderr (gate error messages) is left on the
# terminal so failures are visible.
run_gate() {
    local cargo_version="$1"
    local chart_version="$2"
    local image_published="$3"
    local chart_published="$4"
    local previous_cargo_version="${5:-$1}"

    local tmpdir
    tmpdir=$(mktemp -d)
    # shellcheck disable=SC2064
    trap "rm -rf '${tmpdir}'" RETURN

    # Fake Cargo.toml
    cat > "${tmpdir}/Cargo.toml" <<EOF2
[package]
name = "bobs"
version = "${cargo_version}"
edition = "2021"
EOF2

    # Fake chart/Chart.yaml
    mkdir -p "${tmpdir}/chart"
    cat > "${tmpdir}/chart/Chart.yaml" <<EOF2
apiVersion: v2
name: bobs-chart
version: ${chart_version}
appVersion: ${cargo_version}
EOF2

    # Fake `docker manifest inspect` and `helm show chart`: answer like the
    # registry would for the pre-set publication state.
    mkdir -p "${tmpdir}/bin"
    cat > "${tmpdir}/bin/registry-stub" <<EOF2
#!/usr/bin/env bash
state="\$1"
case "\${state}" in
  1) echo "{}" ;;
  0) echo "unknown: artifact not found" >&2; exit 1 ;;
  *) echo "Get https://registry/v2/: unauthorized" >&2; exit 1 ;;
esac
EOF2
    chmod +x "${tmpdir}/bin/registry-stub"
    printf '#!/usr/bin/env bash\nexec registry-stub "%s"\n' "${image_published}" > "${tmpdir}/bin/docker"
    printf '#!/usr/bin/env bash\nexec registry-stub "%s"\n' "${chart_published}" > "${tmpdir}/bin/helm"
    chmod +x "${tmpdir}/bin/docker" "${tmpdir}/bin/helm"

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
        export PREVIOUS_CARGO_VERSION="${previous_cargo_version}"

        bash "${SCRIPT_DIR}/gate.sh"
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

# Case 1: both already published → skip everything
out=$(run_gate "0.1.3" "0.1.3" 1 1)
check "both published → skip" "${out}" 0 \
    "publish_image=false" "publish_chart=false"

# Case 2: code bumped, chart not bumped → error
out=$(run_gate "0.1.4" "0.1.3" 0 1 "0.1.3")
check "code bumped, chart not bumped → error" "${out}" 1

# Case 3: both bumped to matching versions → full release
out=$(run_gate "0.1.4" "0.1.4" 0 0 "0.1.3")
check "both bumped, versions match → full release" "${out}" 0 \
    "publish_image=true" "publish_chart=true"

# Case 4: both bumped but versions differ → error
out=$(run_gate "0.1.4" "0.1.5" 0 0 "0.1.3")
check "both bumped, versions differ → error" "${out}" 1

# Case 5: chart-only bump → publish chart only
out=$(run_gate "0.1.3" "0.1.4" 1 0)
check "chart-only bump → chart only" "${out}" 0 \
    "publish_image=false" "publish_chart=true"

# Case 6: a release that was bumped in an earlier push but never published
# (nothing version-related changed in this push) → still a full release
out=$(run_gate "0.1.4" "0.1.4" 0 0)
check "unpublished release retried on a later push" "${out}" 0 \
    "publish_image=true" "publish_chart=true"

# Case 7: chart bumped and Cargo moved to a version whose image already exists
# (e.g. a reverted version number) → error, not a chart-only release
out=$(run_gate "0.1.3" "0.1.5" 1 0 "0.1.4")
check "Cargo changed to a published version → error" "${out}" 1

# Case 8: registry unreachable → error, never "not published"
out=$(run_gate "0.1.4" "0.1.4" error 0)
check "image lookup fails → error" "${out}" 1
out=$(run_gate "0.1.4" "0.1.4" 0 error)
check "chart lookup fails → error" "${out}" 1

# ── Summary ────────────────────────────────────────────────────────────────────

echo
echo "${PASS} passed, ${FAIL} failed"
[ "${FAIL}" -eq 0 ]
