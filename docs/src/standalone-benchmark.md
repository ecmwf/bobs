# Standalone BOBS benchmark

The `bobs-benchmark` binary drives complete BOBS object lifecycles directly over the BOBS HTTP API:

1. create an object;
2. write deterministic bytes;
3. complete the object;
4. read the complete object once;
5. optionally delete it.

Use it to validate BOBS itself before adding Polytope, ingress, or other clients to the path. Start local, then repeat the same workload from a container or Kubernetes pod.

Persistence expectations for this benchmark match the BOBS contract: each object is stored as `<data_dir>/<key>/spool.dat` plus `<data_dir>/<key>/meta.json`. Before `/complete`, recovery is required across a BOBS restart by reconstructing in-progress byte state from `spool.dat`, not across a node or storage crash. `/write` must append accepted bytes to `spool.dat` through the kernel/file handle before returning, but does not force them to stable storage with `sync_data()`. Full pages become reader-visible as they are completed; trailing partial-page bytes may already be in `spool.dat` but remain invisible until more writes complete the page or `/complete` finalizes the spool. `/complete` is the durability boundary: BOBS syncs `spool.dat` data before committing final metadata to `meta.json` with the sidecar atomic commit protocol.

> Plain HTTP only: this first benchmark implementation accepts `http://` BOBS endpoints. `https://` support requires an approved dependency or feature change.

## Build

Default Linux `io_uring` binary. This build uses sharded rings:

```bash
cargo build --release --bin bobs --bin bobs-benchmark
```

Fallback binary, for non-Linux development, Linux kernels older than 5.11, or runtimes that block `io_uring_setup` entirely:

```bash
cargo build --release --bin bobs --bin bobs-benchmark --features tokio-fileio-fallback
```

Container image, after the image has been built with the benchmark binary included:

```bash
docker build --target release -t bobs:bench .
docker run --rm bobs:bench bobs-benchmark --help
```

The Dockerfile builds the default Linux backend. Build a fallback image by passing the fallback feature in a dedicated build stage or by building the binaries outside Docker with `--features tokio-fileio-fallback` and packaging those artifacts.

## Local-first validation

Run a local BOBS server first. This keeps network and Kubernetes scheduling effects out of the initial measurement.

Create a temporary config. The Linux `io_uring_shards` field is omitted here so BOBS resolves it to `max(1, num_cpus / 4)`. CPU pinning is not enabled by default.

```bash
cat >/tmp/bobs-bench.yaml <<'YAML'
host: 127.0.0.1
port: 3000
data_dir: /tmp/bobs-bench-data
page_size: 4096
max_cache_bytes: 1048576
host_prefix: bobs
route_name: download
domain: 127.0.0.1:3000
YAML
```

Start BOBS in one terminal with the default Linux `io_uring` backend:

```bash
rm -rf /tmp/bobs-bench-data
HOSTNAME=bobs-0 \
BOBS_INTERNAL_BASE_URL_TEMPLATE=http://127.0.0.1:3000/api/v1 \
cargo run --release --bin bobs -- /tmp/bobs-bench.yaml
```

Or start the fallback backend:

```bash
rm -rf /tmp/bobs-bench-data
HOSTNAME=bobs-0 \
BOBS_INTERNAL_BASE_URL_TEMPLATE=http://127.0.0.1:3000/api/v1 \
cargo run --release --bin bobs --features tokio-fileio-fallback -- /tmp/bobs-bench.yaml
```

Run the benchmark in another terminal:

```bash
cargo run --release --bin bobs-benchmark -- \
  --base-url http://127.0.0.1:3000 \
  --objects 32 \
  --object-bytes 16777216 \
  --write-body-chunk-bytes 1048576 \
  --read-body-chunk-bytes 1048576 \
  --start-delay-ms 2000
```

Useful local variations:

```bash
# Split each object into multiple write requests.
cargo run --release --bin bobs-benchmark -- \
  --base-url http://127.0.0.1:3000 \
  --objects 32 \
  --object-bytes 16777216 \
  --write-request-bytes 1048576 \
  --write-body-chunk-bytes 1048576 \
  --read-body-chunk-bytes 1048576

# Delete benchmark objects after successful reads.
cargo run --release --bin bobs-benchmark -- \
  --base-url http://127.0.0.1:3000 \
  --objects 8 \
  --object-bytes 1048576 \
  --delete-after-read
```

## Page size comparison

Keep the runtime default at `4096` unless benchmark evidence says otherwise. Wider pages can reduce per-page overhead and may improve throughput, especially after removal of write-hot-path metadata commits. They also delay reader visibility until a full page is available and consume more of the global cache budget per cached page, so cache reach may fall unless `max_cache_bytes` is increased. `page_size` may be larger than `max_cache_bytes`; oversized pages simply bypass the cache.

Compare at least the default, 1 MiB, 4 MiB, and 16 MiB pages with representative object sizes. The example below uses a global cache budget sized to hold roughly 256 pages for each run, if the workload and eviction order allow.

Start one BOBS server per page size, run the matching benchmark, then stop the server before moving to the next size:

```bash
# 4 KiB default-page run
cat >/tmp/bobs-bench-4096.yaml <<'YAML'
host: 127.0.0.1
port: 3000
data_dir: /tmp/bobs-bench-data-4096
page_size: 4096
max_cache_bytes: 1048576
host_prefix: bobs
route_name: download
domain: 127.0.0.1:3000
YAML
rm -rf /tmp/bobs-bench-data-4096
HOSTNAME=bobs-0 BOBS_INTERNAL_BASE_URL_TEMPLATE=http://127.0.0.1:3000/api/v1 \
  cargo run --release --bin bobs -- /tmp/bobs-bench-4096.yaml
```

In another terminal:

```bash
cargo run --release --bin bobs-benchmark -- \
  --base-url http://127.0.0.1:3000 \
  --objects 32 \
  --object-bytes 67108864 \
  --write-body-chunk-bytes 1048576 \
  --read-body-chunk-bytes 1048576 \
  --start-delay-ms 2000 \
  --summary-json /tmp/bobs-summary-page-4096.json
```

Repeat with wider page configs and matching labels:

```bash
# 1 MiB pages
cat >/tmp/bobs-bench-1m.yaml <<'YAML'
host: 127.0.0.1
port: 3000
data_dir: /tmp/bobs-bench-data-1m
page_size: 1048576
max_cache_bytes: 268435456
host_prefix: bobs
route_name: download
domain: 127.0.0.1:3000
YAML

# 4 MiB pages
cat >/tmp/bobs-bench-4m.yaml <<'YAML'
host: 127.0.0.1
port: 3000
data_dir: /tmp/bobs-bench-data-4m
page_size: 4194304
max_cache_bytes: 1073741824
host_prefix: bobs
route_name: download
domain: 127.0.0.1:3000
YAML

# 16 MiB pages
cat >/tmp/bobs-bench-16m.yaml <<'YAML'
host: 127.0.0.1
port: 3000
data_dir: /tmp/bobs-bench-data-16m
page_size: 16777216
max_cache_bytes: 4294967296
host_prefix: bobs
route_name: download
domain: 127.0.0.1:3000
YAML
```

For each config, start BOBS with that file and run the same benchmark command, changing only the summary path, for example `/tmp/bobs-summary-page-1m.json`, `/tmp/bobs-summary-page-4m.json`, or `/tmp/bobs-summary-page-16m.json`. Compare `write_active_mib_s`, `read_active_mib_s`, `wall_mib_s`, and `wait-to-read`/read timing percentiles from the `SUMMARY` output. If reader-latency percentiles regress, a throughput improvement may not be worth the wider page.

## Container validation

Container validation checks the packaged binary, container networking, and whether the runtime permits the default Linux `io_uring` backend.

The default backend requires Linux 5.11+ because it submits `io_uring` operations against raw file descriptors, and it requires access to the `io_uring_setup` syscall. If the policy blocks `io_uring_setup`, use an io_uring-capable seccomp/sysctl policy or a `tokio-fileio-fallback` build. On this development host, validation showed:

```text
host io_uring_setup: allowed
docker default seccomp: io_uring_setup returned ENOSYS
docker --security-opt seccomp=unconfined: allowed
```

So the default Docker seccomp profile blocked `io_uring_setup` here, even though the host kernel supported it. The Dockerfile was not changed because this is a runtime security policy, not an image-layer setting.

If your default container policy blocks `io_uring_setup`, either run the default image with a seccomp profile that permits io_uring, or use a fallback build/image:

```bash
# Default backend with an unconfined seccomp policy for local validation.
docker run --rm --security-opt seccomp=unconfined bobs:bench bobs --help

# Fallback backend build outside the default Dockerfile path.
cargo build --release --bins --features tokio-fileio-fallback
```

On Linux, use host networking to reach the local server:

```bash
docker run --rm --network host bobs:bench bobs-benchmark \
  --base-url http://127.0.0.1:3000 \
  --objects 32 \
  --object-bytes 16777216 \
  --write-body-chunk-bytes 1048576 \
  --read-body-chunk-bytes 1048576 \
  --start-delay-ms 2000
```

If host networking is unavailable, publish or otherwise route the BOBS server and replace `127.0.0.1` with an address reachable from the benchmark container.

## Kubernetes/pod validation

Run from inside the cluster after local and container validation pass. This measures in-cluster DNS, pod networking, and BOBS pod placement effects.

For the default Linux image, ask the chart/platform team whether the cluster runtime seccomp profile and node sysctls permit `io_uring_setup`. In particular, check the effective seccomp profile and node settings such as `/proc/sys/kernel/io_uring_disabled`. If the profile is `RuntimeDefault` and blocks io_uring, use a chart override or pod security context that applies an io_uring-capable seccomp profile, or deploy a `tokio-fileio-fallback` image instead. No Kubernetes YAML setting can compensate for a node kernel older than Linux 5.11.

### Single endpoint mode

Use single endpoint mode when all benchmark objects should target one BOBS pod or a service with guaranteed routing affinity.

```bash
kubectl run bobs-benchmark \
  --rm -i \
  --restart=Never \
  --image=<image> \
  -- bobs-benchmark \
  --base-url http://<release>-bobs-0:3000 \
  --objects 32 \
  --object-bytes 16777216 \
  --write-body-chunk-bytes 1048576 \
  --read-body-chunk-bytes 1048576 \
  --start-delay-ms 2000
```

### Endpoint-template mode

Use template mode for multi-BOBS deployments where each ordinal has a stable, directly addressable endpoint. Objects are assigned deterministically across the configured ordinals.

```bash
kubectl run bobs-benchmark \
  --rm -i \
  --restart=Never \
  --image=<image> \
  -- bobs-benchmark \
  --base-url-template http://<release>-bobs-{ordinal}:3000 \
  --ordinals 0,1,2,3 \
  --objects 64 \
  --object-bytes 16777216 \
  --write-body-chunk-bytes 1048576 \
  --read-body-chunk-bytes 1048576 \
  --start-delay-ms 2000
```

Avoid an aggregate multi-pod Kubernetes service for multi-BOBS benchmark runs unless the deployment guarantees that create, write, complete, and read for an object route to the same pod. Prefer per-pod services or pod DNS names with `--base-url-template`. Within one BOBS instance, data and metadata `io_uring` work for the same key is routed to the same shard by stable key hashing; that does not replace the need for correct HTTP-level routing between pods.

## Output and summary fields

The benchmark writes grep-friendly event lines during the run and one final machine-readable line:

```text
SUMMARY:{...json...}
```

Important summary fields:

- `total_objects`, `successes`, `failures`: workload size and final outcome counts.
- `total_bytes_written`, `total_bytes_read`: successful object bytes only; failed objects are excluded from throughput byte totals.
- `wall_ms`, `wall_mib_s`: whole benchmark wall-clock duration and read throughput over that full window.
- `write_active_ms`, `write_active_mib_s`: aggregate write throughput over the active write window, from earliest write start to latest write end.
- `read_active_ms`, `read_active_mib_s`: aggregate read throughput over the active read window.
- `read_mib_per_reader_second`: sum of successful read bytes divided by summed per-object read time.
- `timings`: p50, p95, and max timings for create, write, complete, wait-to-read, read, and lifecycle phases.
- `per_ordinal`: object counts, bytes, active windows, and throughput grouped by response ordinal when available, otherwise configured ordinal, otherwise `unknown`.
- `results`: per-object records including `object_index`, `object_label`, endpoint URL, ordinals, key, outcome, error, timings, and bytes.

To extract the summary from logs:

```bash
grep '^SUMMARY:' benchmark.log | tail -n1 | sed 's/^SUMMARY://'
```

Optional file outputs, if supported by the binary version you are running:

```bash
bobs-benchmark \
  --base-url http://127.0.0.1:3000 \
  --objects 8 \
  --object-bytes 1048576 \
  --summary-json /tmp/bobs-summary.json \
  --results-jsonl /tmp/bobs-results.jsonl
```

## Grep and correlation guidance

Benchmark event lines include stable fields such as:

```text
object=000007 key=<bobs-key> ordinal=0 event=write_end unix_ms=<ms> status=200 duration_ms=<ms> bytes=<n>
```

Useful searches:

```bash
# Follow one benchmark object through create/write/complete/read.
grep 'object=000007 ' benchmark.log

# After create has emitted the BOBS key, correlate benchmark lines by key.
grep 'key=<bobs-key>' benchmark.log

# Search BOBS pod logs for the same key.
kubectl logs <bobs-pod> | grep '<bobs-key>'

# In template mode, compare configured/response ordinal grouping.
grep 'ordinal=2 ' benchmark.log
```

If `configured_ordinal` and `response_ordinal` disagree, the benchmark emits an `object_error` event with `error=ordinal_mismatch`. Treat that as a routing or read URL generation issue before comparing throughput numbers.

## Cleanup notes

Local cleanup:

```bash
# Stop the local BOBS process, then remove benchmark data.
rm -rf /tmp/bobs-bench-data /tmp/bobs-bench.yaml
```

Kubernetes cleanup:

```bash
# kubectl run --rm removes the benchmark pod after completion.
# If a failed run leaves a pod behind:
kubectl delete pod bobs-benchmark --ignore-not-found
```

Use `--delete-after-read` when you want the benchmark to delete objects immediately after successful reads. Without it, BOBS cleanup is governed by the server TTL settings in the BOBS config.
