# StatefulSet + PVC + Recovery

## TL;DR

> **Quick Summary**: Convert BOBS from Deployment to StatefulSet with PVC storage. Fix recovery so writers and readers can rejoin after pod restart. Add per-pod ingress routing.
>
> **Deliverables**:
> - BOBS code: metadata persistence on write, disk-validated recovery, CRC recalculation, hostname auto-detect, cleanup grace period
> - BOBS chart: StatefulSet, volumeClaimTemplates, per-pod Services, path-based ingress
> - Create response includes read_url prefix
>
> **Estimated Effort**: Large
> **Critical Path**: Code changes (T1-T6) → Chart changes (T7-T11) → Create response (T12)

---

## Context

BOBS currently uses a Deployment with emptyDir storage. Data is lost on pod restart. With StatefulSet + PVC, spool data survives restarts. But the recovery logic has gaps: metadata lies after crash (write_buffer bytes counted but not flushed), CRC is lost, reader_count resets to 0, and redb is only written on create.

### Key Architecture Decision
- StatefulSet with stable pod names (bobs-0, bobs-1, ...)
- PVC per pod via volumeClaimTemplates
- Per-pod Services using `statefulset.kubernetes.io/pod-name` label selector
- Path-based ingress: `/bobs-0/read/{key}` → bobs-0 pod
- bob_id = pod hostname (auto-detected from HOSTNAME env var)
- Writers rejoin by querying current offset; readers rejoin with Range header

---

## Work Objectives

### Must Have
- Metadata persisted to redb on every page flush (not just create)
- Recovery validates disk file size against metadata, corrects mismatches
- Writing/WriteLocked spools preserved on recovery (not normalized to Complete)
- CRC32C recalculated from disk for recovered Writing spools
- bob_id auto-detected from HOSTNAME env var
- Recovered spools get cleanup grace period
- StatefulSet with PVC in chart
- Per-pod Services and path-based ingress
- Create response includes read_url

### Must NOT Have
- No changes to the FileIO trait
- No changes to the Processor trait in polytope-server
- No PVC for redb (redb lives on the same PVC as spool data)
- No custom DNS service
- No changes to the read/write HTTP API paths
- Do NOT commit — stage only

---

## Execution Strategy

### Wave 1: BOBS Code (parallel where possible)

- [x] T1. **Persist metadata to redb on page flush**

  **What to do**:
  - SpoolManager needs to expose a method: `pub fn persist_metadata(&self, key: &str, metadata: &SpoolMetadata) -> Result<()>` that writes to redb
  - Spool needs access to this persist function. Add a `db: Arc<Database>` field to Spool struct. Pass it from SpoolManager::create_spool and recover.
  - In writer.rs write(), after incrementing total_pages and updating total_bytes_written, persist metadata to redb
  - In lifecycle.rs complete(), persist metadata after setting state to Complete

  **Files**: src/spool/mod.rs, src/spool/writer.rs, src/spool/lifecycle.rs, src/manager.rs
  **Verify**: cargo test — all 43 tests pass. Write test: create spool, write data, verify redb has updated total_pages.

- [x] T2. **Recovery: validate disk, correct metadata, handle states**

  **What to do**:
  - In manager.rs recover(), after loading SpoolMetadata from redb:
    - Creating → delete entry and spool dir (incomplete creation)
    - Deleting → delete entry and spool dir (interrupted deletion)
    - Writing/WriteLocked → KEEP state (writer can rejoin). Validate disk:
      - Get file size via std::fs::metadata
      - Expected flushed bytes = total_pages * page_size
      - If file_size < expected: reduce total_pages to file_size / page_size, set total_bytes_written = corrected total_pages * page_size, clear final_page_size
      - If file_size > expected but state is not Complete: truncate total_bytes_written to match flushed pages (drop lost write_buffer bytes)
      - Update corrected metadata back to redb
    - Complete → validate: expected bytes = (total_pages - 1) * page_size + final_page_size.unwrap_or(page_size). If mismatch, log warning.
    - Set last_write_at = now() for recovered Writing/WriteLocked spools (cleanup grace period)
  - Scan data_dir for directories not in redb → delete orphans

  **Files**: src/manager.rs
  **Verify**: cargo test. Add test: create spool, write 2 pages, simulate crash (drop manager without complete), recover with new manager, verify corrected metadata.

- [x] T3. **Recovery: recalculate CRC32C from disk**

  **What to do**:
  - In recover(), for Writing/WriteLocked spools after disk validation:
    - Read all flushed pages from disk sequentially
    - Compute CRC32C incrementally using crc32c::crc32c_append
    - Store in spool.running_crc32c
  - For Complete spools: checksum_crc32c is already in metadata, set running_crc32c to that value

  **Files**: src/manager.rs
  **Verify**: cargo test. Add test: create spool, write data, recover, verify running_crc32c matches expected CRC of written pages.

- [x] T4. **Auto-detect bob_id from HOSTNAME**

  **What to do**:
  - In main.rs, after loading config: if config.bob_id is "unknown" or empty, override with std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string())
  - This is a 3-line change in main.rs

  **Files**: src/main.rs
  **Verify**: cargo build

- [x] T5. **Create response includes read_url**

  **What to do**:
  - In http/mod.rs create_spool handler, the response currently returns: {"key": "bobs-0-abc"}
  - Change to: {"key": "bobs-0-abc", "read_url": "/{bob_id}/read/bobs-0-abc"}
  - The bob_id prefix in the URL allows the ingress to route to the correct pod
  - Read bob_id from state.config.bob_id (or from the key prefix, since key = {bob_id}-{uuid})

  **Files**: src/http/mod.rs
  **Verify**: cargo test. Update http tests to assert read_url in create response.

- [x] T6. **Persist metadata in complete() and update redb on delete**

  **What to do**:
  - complete() already transitions state to Complete. After this, persist the final metadata to redb (including checksum_crc32c, final_page_size, state=Complete).
  - This is part of T1 but listed separately because complete() has its own persistence needs (CRC, final_page_size).
  - Verify that delete_spool still removes from redb (it already does).

  **Files**: src/spool/lifecycle.rs, src/manager.rs
  **Verify**: cargo test. Verify redb has state=Complete after calling complete().

### Wave 2: Chart Changes (all independent)

- [x] T7. **Deployment → StatefulSet with volumeClaimTemplates**

  **What to do**:
  - Replace chart/templates/deployment.yaml with chart/templates/statefulset.yaml
  - Add spec.serviceName matching headless service name
  - Add volumeClaimTemplates with persistence.size and persistence.storageClass from values
  - Add persistentVolumeClaimRetentionPolicy: whenScaled: Delete, whenDeleted: Retain
  - Container volumeMount changes: data volume from emptyDir → VCT name
  - Both config ConfigMap mount AND data PVC mount

  **Files**: chart/templates/statefulset.yaml (new), chart/templates/deployment.yaml (delete), chart/values.yaml
  **Verify**: helm template bobs ./chart renders valid StatefulSet

- [x] T8. **Per-pod Services via template loop**

  **What to do**:
  - Create chart/templates/pod-services.yaml
  - Loop range .Values.replicaCount, create a Service per pod:
    - name: bobs-{i}
    - selector: statefulset.kubernetes.io/pod-name: bobs-{i}
    - port: .Values.service.port

  **Files**: chart/templates/pod-services.yaml (new)
  **Verify**: helm template renders N services matching replicaCount

- [x] T9. **Path-based ingress with rewrite**

  **What to do**:
  - Create chart/templates/ingress.yaml (or update if exists)
  - For each replica, add an ingress rule:
    - path: /bobs-{i}(/|$)(.*)
    - rewrite-target: /$2
    - backend: bobs-{i} service
  - Annotation: nginx.ingress.kubernetes.io/rewrite-target: /$2
  - Host from values: .Values.ingress.host

  **Files**: chart/templates/ingress.yaml (new), chart/values.yaml
  **Verify**: helm template renders ingress with correct paths and rewrites

- [x] T10. **Update values.yaml for PVC and ingress**

  **What to do**:
  - Add/update persistence section: enabled: true, size: 10Gi, storageClass: "" (cluster default)
  - Add ingress section: enabled: false, host: "", className: nginx
  - Remove emptyDir references

  **Files**: chart/values.yaml
  **Verify**: helm template with default values renders correctly

- [x] T11. **Update polytope-chart values.yaml and majh-dev.yaml**

  **What to do**:
  - Update bobs section in polytope-chart/values.yaml: add persistence and ingress settings
  - Update bobs section in polytope-config/majh-dev.yaml: add persistence and ingress settings for dev

  **Files**: polytope-chart/values.yaml, polytope-config/majh-dev.yaml
  **Verify**: Values are consistent with chart schema

### Wave 3: Verification

- [x] F1. **Full test suite passes**: cargo test in bobs, cargo test -p polytope-worker-common, cargo check -p fdb-worker, cargo check -p polytope-fe-worker
- [x] F2. **Helm renders correctly**: helm template bobs ./chart produces valid StatefulSet, ConfigMap, per-pod Services, Ingress
- [x] F3. **Recovery test**: unit test simulating crash mid-write → restart → writer rejoin → reader reads correct data

---

## Commit Strategy

**DO NOT COMMIT.** Stage all changes in current branches only.

---

## Success Criteria

- [ ] Writer can resume after pod restart (correct offset, CRC continues)
- [ ] Reader can reconnect after pod restart (Range header, spool still exists)
- [ ] Metadata in redb matches disk reality after recovery
- [ ] Cleanup doesn't delete recently-recovered spools
- [ ] Per-pod ingress routing works (helm template verification)
- [ ] All existing tests pass
