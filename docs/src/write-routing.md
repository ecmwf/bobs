<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Write Routing

BOBS supports horizontal scaling of writes across a StatefulSet. Each pod owns the spools it created, and clients are directed to the owning pod for all body-path operations via the `write_url` returned at create time.

---

## Key Format

A valid `X-Polytope-Job-Id` header is used as the spool key. Valid request IDs are 26-character, lower-case Crockford base32 strings. If the header is absent or invalid, BOBS generates an opaque UUIDv4 key, for example:

```
550e8400-e29b-41d4-a716-446655440000
```

Keys carry no ownership information. The owning pod is determined entirely by which pod handled `/api/v1/create`, not by parsing the key.

---

## Create Flow

Clients send a `PUT /api/v1/create` request to the **cluster Service** (the ordinary round-robin load-balanced endpoint). Any pod may handle the create:

```
PUT /api/v1/create
→ 201 Created
{
  "key":       "550e8400-e29b-41d4-a716-446655440000",
  "read_url":  "https://example.com/download-2/550e8400-e29b-41d4-a716-446655440000",
  "write_url": "http://release-bobs-2:3000/api/v1"
}
```

- `key` — the spool identifier; embed it in all subsequent requests.
- `read_url` — the public URL through which consumers can stream the spool once writing begins. It is an opaque download link; the BOBS internal `/api/v1/read/{key}` endpoint is not part of this public URL.
- `write_url` — the **per-pod internal base URL** of the owning pod. Clients must use this URL as the base for all write and complete calls.

The handling pod selects the validated request ID or generates a fallback UUIDv4 key, creates the spool directory and initial `meta.json` sidecar, and returns its resolved `internal_base_url` as `write_url`. No network calls are made to other pods during `/api/v1/create`.

---

## Write and Complete Flow

After receiving the create response the client routes all body-path traffic directly to `write_url`:

```
POST {write_url}/write/{key}/{offset}
POST {write_url}/complete/{key}
```

These requests bypass the cluster load balancer and land on the owning pod every time. Workers must use the `write_url` from the create response — it is the only correct destination for writes.

---

## No Fallback for Misrouted Writes

If an `/api/v1/write/{key}/{offset}` or `/api/v1/complete/{key}` request reaches a pod that does not own the spool, that pod returns `404 SpoolNotFound`. There is no redirect path; workers must use `write_url`.

---

## `BOBS_INTERNAL_BASE_URL_TEMPLATE` Environment Variable

Each BOBS pod requires `HOSTNAME` ending in a numeric ordinal and a non-empty `BOBS_INTERNAL_BASE_URL_TEMPLATE`:

```
BOBS_INTERNAL_BASE_URL_TEMPLATE=http://release-bobs-{ordinal}:3000/api/v1
```

The literal placeholder `{ordinal}` (single braces) is **not** expanded by Helm — it is replaced at runtime by the BOBS process itself.

On startup BOBS reads this variable, substitutes its own ordinal to derive `internal_base_url`, and fails fast with a clear error if the variable is absent or empty. It logs the resolved value:

```
INFO internal_base_url = http://release-bobs-2:3000/api/v1
```

The chart injects the variable automatically into the StatefulSet. No per-replica manual configuration is required.

---

## Pod Loss Mid-Write

If the owning pod dies while a write is in progress:

1. The worker receives an HTTP error on the next `/api/v1/write/{key}/{offset}` call.
2. The worker abandons that spool and retries from `/api/v1/create` on a surviving pod.
3. The orphan remains in `Writing` state on the failed pod's PVC.
4. After restart, recovery reconstructs in-progress byte state from `spool.dat`; cleanup reaps the spool after `writer_inactivity_timeout_secs` (default 300).

No manual intervention or cross-pod coordination is needed.

---

## Out of Scope

**Read-path HA** is not covered by this feature. The `read_url` returned by `/api/v1/create` routes through the per-pod ingress path; reads fail while the owning pod is unavailable. Shared-storage or sidecar-based read HA is a planned follow-on.
