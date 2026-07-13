// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use crate::error::{BobsError, Result};
use crate::io::{read_exact_at, FileIO};
use crate::metadata::{MetadataStore, SyncSidecarMetadataStore};
use crate::metrics::BobsMetrics;
use crate::spool::{CleanupAnchors, PageCache, Spool, SpoolMetadata, SpoolState};
use crate::time::now_secs;
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteReason {
    Explicit,
    Ttl,
    Orphan,
    Corrupt,
}

impl DeleteReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::Ttl => "ttl",
            Self::Orphan => "orphan",
            Self::Corrupt => "corrupt",
        }
    }
}

fn metric_state_label(state: &SpoolState, write_locked: bool) -> &'static str {
    match state {
        SpoolState::Creating | SpoolState::Writing => crate::metrics::state::WRITING,
        SpoolState::WriteLocked => crate::metrics::state::WRITE_LOCKED,
        SpoolState::Completing if write_locked => crate::metrics::state::WRITE_LOCKED,
        SpoolState::Completing => crate::metrics::state::WRITING,
        SpoolState::Complete | SpoolState::Deleting => crate::metrics::state::COMPLETE,
    }
}

/// Number of configured-page units available to in-flight read responses.
/// Cache-disabled and sub-page cache budgets still allow exactly one response.
fn read_response_permit_limit(page_size: usize, max_cache_bytes: usize) -> usize {
    let max_permits = Semaphore::MAX_PERMITS.min(u32::MAX as usize);
    (max_cache_bytes / page_size).clamp(1, max_permits)
}

/// Charge recovered layouts proportionally when their persisted page size is wider
/// than the current configured page. One oversized response consumes the whole budget.
fn read_response_permits_for_page(
    configured_page_size: usize,
    spool_page_size: usize,
    permit_limit: usize,
) -> u32 {
    spool_page_size
        .div_ceil(configured_page_size)
        .min(permit_limit) as u32
}

pub struct SpoolManager<F: FileIO, M: MetadataStore = SyncSidecarMetadataStore> {
    pub spools: Arc<DashMap<String, Arc<Spool<F, M>>>>,
    pub metadata_store: M,
    pub data_dir: PathBuf,
    pub page_size: usize,
    pub max_cache_bytes: usize,
    pub page_cache: Arc<Mutex<PageCache>>,
    pub metrics: Arc<BobsMetrics>,
    /// Bounds live spool admission during creation and startup recovery.
    pub admission: Arc<Semaphore>,
    pub max_live_spools: usize,
    /// Weighted configured-page admission retained for each HTTP response lifetime.
    read_response_admission: Arc<Semaphore>,
    read_response_active: Arc<AtomicUsize>,
    read_response_permits_active: Arc<AtomicUsize>,
    read_response_permit_limit: usize,
}

struct CreateTransaction<F: FileIO, M: MetadataStore> {
    spools: Arc<DashMap<String, Arc<Spool<F, M>>>>,
    metadata_store: M,
    data_dir: PathBuf,
    page_size: usize,
    page_cache: Arc<Mutex<PageCache>>,
    metrics: Arc<BobsMetrics>,
    read_response_admission: Arc<Semaphore>,
    read_response_active: Arc<AtomicUsize>,
    read_response_permits_active: Arc<AtomicUsize>,
}

impl<F, M> CreateTransaction<F, M>
where
    F: FileIO,
    M: MetadataStore + Clone + Send + Sync + 'static,
{
    async fn run(
        self,
        key: String,
        content_type: Option<String>,
        content_encoding: Option<String>,
        write_locked: bool,
        labels: HashMap<String, String>,
        permit: OwnedSemaphorePermit,
    ) -> Result<()> {
        let spool_dir = self.data_dir.join(&key);
        let data_path = spool_dir.join("spool.dat");

        match tokio::fs::create_dir(&spool_dir).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(BobsError::SpoolAlreadyExists { key });
            }
            Err(error) => return Err(BobsError::IoError(error)),
        }

        let handle = match F::create(&data_path).await {
            Ok(handle) => handle,
            Err(error) => {
                self.cleanup_failed_creation(&key, None).await;
                return Err(BobsError::IoError(error));
            }
        };

        let now = now_secs();
        let metadata = SpoolMetadata {
            key: key.clone(),
            content_type,
            content_encoding,
            state: if write_locked {
                SpoolState::WriteLocked
            } else {
                SpoolState::Writing
            },
            write_locked,
            created_at: now,
            last_write_at: now,
            last_read_at: None,
            readable_at: None,
            page_size: self.page_size as u64,
            total_bytes_written: 0,
            total_pages: 0,
            final_page_size: None,
            data_path,
            labels,
        };

        let durability_result = async {
            // Persist the empty data inode and its name before publishing any
            // metadata that can make this spool recoverable.
            F::sync_data(&handle).await.map_err(BobsError::IoError)?;
            F::sync_directory(&spool_dir)
                .await
                .map_err(BobsError::IoError)?;

            // First make a recoverable tombstone durable. If creation crashes
            // before the final sidecar commit, recovery removes Creating spools.
            let mut creating = metadata.clone();
            creating.state = SpoolState::Creating;
            self.metadata_store.write(&creating).await?;
            F::sync_directory(&self.data_dir)
                .await
                .map_err(BobsError::IoError)?;

            // This key directory is now durably linked from data_dir. Publishing
            // the live state only changes names inside the already-durable key dir.
            self.metadata_store.write(&metadata).await
        }
        .await;

        if let Err(error) = durability_result {
            self.cleanup_failed_creation(&key, Some(handle)).await;
            return Err(error);
        }

        let spool = Arc::new(Spool::new_with_admission(
            metadata,
            handle,
            self.page_size,
            Arc::clone(&self.page_cache),
            self.metadata_store.clone(),
            Arc::clone(&self.metrics),
            Some(permit),
            Arc::clone(&self.read_response_admission),
            Arc::clone(&self.read_response_active),
            Arc::clone(&self.read_response_permits_active),
            1,
        ));
        self.spools.insert(key, spool);

        let initial_state = if write_locked {
            crate::metrics::state::WRITE_LOCKED
        } else {
            crate::metrics::state::WRITING
        };
        self.metrics.record_state_transition(None, initial_state);
        Ok(())
    }

    async fn cleanup_failed_creation(&self, key: &str, handle: Option<F::Handle>) {
        if let Some(handle) = handle {
            if let Err(error) = F::close(handle).await {
                tracing::warn!(key = %key, error = %error, "failed to close data file while rolling back spool creation");
            }
        }

        if let Err(error) = self.metadata_store.delete(key).await {
            tracing::warn!(key = %key, error = %error, "failed to remove sidecar while rolling back spool creation");
        }

        let spool_dir = self.data_dir.join(key);
        match tokio::fs::remove_dir_all(&spool_dir).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(key = %key, error = %error, "failed to remove spool directory while rolling back creation");
            }
        }

        // Sync even when the directory is already absent: a previous removal may
        // have reached the filesystem but failed at this durability boundary.
        if let Err(error) = F::sync_directory(&self.data_dir).await {
            tracing::warn!(key = %key, error = %error, "failed to sync data directory while rolling back spool creation");
        }
    }
}

impl<F: FileIO> SpoolManager<F, SyncSidecarMetadataStore> {
    pub fn new(
        data_dir: impl AsRef<Path>,
        page_size: usize,
        max_cache_bytes: usize,
        max_live_spools: usize,
    ) -> Result<Self> {
        Self::with_metadata_store(
            SyncSidecarMetadataStore::new(data_dir.as_ref()),
            data_dir,
            page_size,
            max_cache_bytes,
            max_live_spools,
        )
    }
}

impl<F, M> SpoolManager<F, M>
where
    F: FileIO,
    M: MetadataStore + Clone + Send + Sync + 'static,
{
    pub fn with_metadata_store(
        metadata_store: M,
        data_dir: impl AsRef<Path>,
        page_size: usize,
        max_cache_bytes: usize,
        max_live_spools: usize,
    ) -> Result<Self> {
        if page_size == 0 {
            return Err(BobsError::ConfigurationError(
                "page_size must be greater than 0".to_string(),
            ));
        }
        if max_live_spools == 0 {
            return Err(BobsError::ConfigurationError(
                "max_live_spools must be greater than 0".to_string(),
            ));
        }
        std::fs::create_dir_all(data_dir.as_ref()).map_err(BobsError::IoError)?;
        let page_cache = Arc::new(Mutex::new(PageCache::new(max_cache_bytes)));

        let read_response_permit_limit = read_response_permit_limit(page_size, max_cache_bytes);
        let read_response_admission = Arc::new(Semaphore::new(read_response_permit_limit));
        Ok(Self {
            spools: Arc::new(DashMap::new()),
            metadata_store,
            data_dir: data_dir.as_ref().to_path_buf(),
            page_size,
            max_cache_bytes,
            page_cache,
            metrics: Arc::new(BobsMetrics::new(false)),
            admission: Arc::new(Semaphore::new(max_live_spools)),
            max_live_spools,
            read_response_admission,
            read_response_active: Arc::new(AtomicUsize::new(0)),
            read_response_permits_active: Arc::new(AtomicUsize::new(0)),
            read_response_permit_limit,
        })
    }

    /// Set the metrics handle (replaces the default no-op).
    pub fn set_metrics(&mut self, metrics: Arc<BobsMetrics>) {
        self.metrics = metrics;
    }

    /// Total weighted configured-page units available to read responses.
    pub fn read_response_permit_limit(&self) -> usize {
        self.read_response_permit_limit
    }

    pub fn read_response_available_permits(&self) -> usize {
        self.read_response_admission.available_permits()
    }

    pub fn active_read_responses(&self) -> usize {
        self.read_response_active.load(Ordering::Acquire)
    }

    pub fn active_read_response_permits(&self) -> usize {
        self.read_response_permits_active.load(Ordering::Acquire)
    }

    pub async fn create_spool(
        &self,
        key: String,
        content_type: Option<String>,
        content_encoding: Option<String>,
        write_locked: bool,
        labels: HashMap<String, String>,
    ) -> Result<()> {
        let spool_dir = self.data_dir.join(&key);
        if self.spools.contains_key(&key)
            || tokio::fs::try_exists(&spool_dir)
                .await
                .map_err(BobsError::IoError)?
        {
            return Err(BobsError::SpoolAlreadyExists { key });
        }

        // Waiting for admission is cancellation-safe: no caller-key filesystem
        // reservation exists yet, and dropping this future releases any acquired permit.
        let permit = Arc::clone(&self.admission)
            .acquire_owned()
            .await
            .map_err(|_| BobsError::IoError(std::io::Error::other("admission semaphore closed")))?;

        // Once the atomic directory reservation starts, the transaction must outlive
        // its caller. A cancelled HTTP request drops only this JoinHandle; the detached
        // task either publishes a fully durable live spool or rolls the reservation back.
        let transaction = CreateTransaction::<F, M> {
            spools: Arc::clone(&self.spools),
            metadata_store: self.metadata_store.clone(),
            data_dir: self.data_dir.clone(),
            page_size: self.page_size,
            page_cache: Arc::clone(&self.page_cache),
            metrics: Arc::clone(&self.metrics),
            read_response_admission: Arc::clone(&self.read_response_admission),
            read_response_active: Arc::clone(&self.read_response_active),
            read_response_permits_active: Arc::clone(&self.read_response_permits_active),
        };
        tokio::spawn(transaction.run(
            key,
            content_type,
            content_encoding,
            write_locked,
            labels,
            permit,
        ))
        .await
        .map_err(|error| {
            BobsError::IoError(std::io::Error::other(format!(
                "spool creation transaction task failed: {error}"
            )))
        })?
    }

    pub fn get_spool(&self, key: &str) -> Option<Arc<Spool<F, M>>> {
        self.spools.get(key).map(|entry| Arc::clone(entry.value()))
    }

    pub fn spool_keys(&self) -> Vec<String> {
        self.spools
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    pub async fn delete_spool(&self, key: &str) -> Result<()> {
        self.delete_spool_with_reason(key, DeleteReason::Explicit, None)
            .await
    }

    pub async fn delete_spool_with_reason(
        &self,
        key: &str,
        reason: DeleteReason,
        job_id: Option<&str>,
    ) -> Result<()> {
        let result = self.delete_spool_inner(key).await;
        match &result {
            Ok(()) => {
                if let Some(job_id) = job_id {
                    tracing::info!("event.name" = "bobs.spool.deleted", "bobs.spool.key" = %key, "request.id" = %job_id, reason = reason.as_str(), outcome = "success", "spool deleted");
                } else {
                    tracing::info!("event.name" = "bobs.spool.deleted", "bobs.spool.key" = %key, reason = reason.as_str(), outcome = "success", "spool deleted");
                }
            }
            Err(error) => {
                if let Some(job_id) = job_id {
                    tracing::error!("event.name" = "bobs.spool.deleted", "bobs.spool.key" = %key, "request.id" = %job_id, reason = reason.as_str(), outcome = "error", error = %error, "spool deletion failed");
                } else {
                    tracing::error!("event.name" = "bobs.spool.deleted", "bobs.spool.key" = %key, reason = reason.as_str(), outcome = "error", error = %error, "spool deletion failed");
                }
            }
        }
        result
    }

    /// Delete only if a candidate snapshot is still current while holding the
    /// spool lifecycle lock. Returning `Ok(None)` means activity or a state
    /// transition invalidated the candidate and no deletion was attempted.
    pub(crate) async fn delete_spool_with_reason_if<P>(
        &self,
        key: &str,
        reason: DeleteReason,
        predicate: P,
    ) -> Result<Option<HashMap<String, String>>>
    where
        P: FnOnce(&SpoolMetadata, CleanupAnchors) -> bool,
    {
        let Some(spool) = self.spools.get(key).map(|entry| Arc::clone(entry.value())) else {
            return Ok(None);
        };
        let _lifecycle_guard = spool.lifecycle_lock.lock().await;

        let labels = {
            let meta = spool.metadata.lock().await;
            let anchors = spool.cleanup_anchors();
            if !predicate(&meta, anchors) {
                return Ok(None);
            }
            meta.labels.clone()
        };

        let result = self.delete_spool_locked(key, &spool).await;
        match &result {
            Ok(()) => {
                tracing::info!("event.name" = "bobs.spool.deleted", "bobs.spool.key" = %key, reason = reason.as_str(), outcome = "success", "spool deleted");
            }
            Err(error) => {
                tracing::error!("event.name" = "bobs.spool.deleted", "bobs.spool.key" = %key, reason = reason.as_str(), outcome = "error", error = %error, "spool deletion failed");
            }
        }
        result.map(|()| Some(labels))
    }

    async fn delete_spool_inner(&self, key: &str) -> Result<()> {
        let spool = self
            .spools
            .get(key)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| BobsError::SpoolNotFound {
                key: key.to_string(),
            })?;
        let _lifecycle_guard = spool.lifecycle_lock.lock().await;
        self.delete_spool_locked(key, &spool).await
    }

    async fn delete_spool_locked(&self, key: &str, spool: &Arc<Spool<F, M>>) -> Result<()> {
        let old_label = {
            let mut meta = spool.metadata.lock().await;
            if meta.state == SpoolState::Deleting {
                None
            } else {
                let old_label = metric_state_label(&meta.state, meta.write_locked);
                meta.state = SpoolState::Deleting;
                Some(old_label)
            }
        };
        if let Some(old_label) = old_label {
            if old_label != crate::metrics::state::COMPLETE {
                self.metrics
                    .record_state_transition(Some(old_label), crate::metrics::state::COMPLETE);
            }
        }

        spool.cancel.cancel();
        // Keep the spool and its direct handle reachable until durable deletion
        // succeeds. A failed delete remains visible as Deleting and can be retried;
        // successful removal drops the manager's handle only after metadata and the
        // directory have both gone.
        self.metadata_store.delete(key).await?;
        self.remove_spool_directory_durably(key).await?;

        self.page_cache.lock().await.free_spool(key);
        spool.release_admission();
        if self
            .spools
            .remove_if(key, |_, current| Arc::ptr_eq(current, spool))
            .is_some()
        {
            self.metrics
                .record_spool_removed(crate::metrics::state::COMPLETE);
        }
        Ok(())
    }

    async fn remove_spool_directory_durably(&self, key: &str) -> Result<()> {
        let spool_dir = self.data_dir.join(key);
        match tokio::fs::remove_dir_all(&spool_dir).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(BobsError::IoError(error)),
        }

        // This is the commit point for deletion. Cache entries, map membership,
        // and admission remain held until the directory unlink is durable.
        F::sync_directory(&self.data_dir)
            .await
            .map_err(BobsError::IoError)
    }

    pub async fn recover(&self) -> Result<()> {
        self.recover_with_counts().await.map(|_| ())
    }

    async fn recover_with_counts(&self) -> Result<(usize, usize)> {
        let started = Instant::now();
        let mut recovered: Vec<(String, SpoolMetadata)> = Vec::new();
        let mut stale_keys: Vec<String> = Vec::new();
        let mut corrupt_deleted = 0_u64;
        let mut orphan_deleted = 0_u64;

        for (key, result) in self.metadata_store.list().await? {
            match result {
                Ok(meta) if meta.key == key => recovered.push((key, meta)),
                Ok(meta) => {
                    tracing::warn!(directory_key = %key, metadata_key = %meta.key, "recovery: sidecar key does not match directory, discarding");
                    stale_keys.push(key);
                }
                Err(error) => {
                    tracing::warn!(key = %key, error = %error, "recovery: corrupt sidecar metadata, discarding affected spool directory");
                    stale_keys.push(key);
                }
            }
        }

        let recovered_keys: HashSet<String> =
            recovered.iter().map(|(key, _)| key.clone()).collect();

        // Most-recently-active spools win; lexical key order breaks ties so the same
        // on-disk set and configuration always produce the same recovery order.
        recovered.sort_by(|(left_key, left), (right_key, right)| {
            recovery_activity_at(right)
                .cmp(&recovery_activity_at(left))
                .then_with(|| left_key.cmp(right_key))
        });

        let mut recovered_count = 0_usize;
        let mut quarantined_count = 0_usize;
        let mut rejected_count = 0_usize;

        // Validate candidates in deterministic order. Metadata-only checks happen
        // before admission, so invalid newer entries cannot displace older valid
        // spools. Once capacity is full, valid excess entries remain unopened.
        for (key, mut meta) in recovered {
            match meta.state {
                SpoolState::Creating => {
                    tracing::debug!(key = %key, "recovery removing incomplete spool (Creating)");
                    stale_keys.push(key);
                    continue;
                }
                SpoolState::Deleting => {
                    tracing::debug!(key = %key, "recovery removing incomplete spool (Deleting)");
                    stale_keys.push(key);
                    continue;
                }
                SpoolState::Writing
                | SpoolState::WriteLocked
                | SpoolState::Completing
                | SpoolState::Complete => {}
            }

            let file_size = match std::fs::metadata(&meta.data_path) {
                Ok(metadata) => metadata.len(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    tracing::warn!(key = %key, "recovery: data file missing, discarding");
                    stale_keys.push(key);
                    continue;
                }
                Err(error) => {
                    tracing::warn!(
                        key = %key,
                        error = %error,
                        "recovery: failed to inspect data file metadata; leaving spool intact and unavailable"
                    );
                    rejected_count += 1;
                    continue;
                }
            };

            let finalized_completing = meta.state == SpoolState::Completing;
            if finalized_completing {
                if let Err(reason) = validate_completing_marker(&meta, file_size) {
                    tracing::warn!(
                        key = %key,
                        file_size = file_size,
                        reason = %reason,
                        "recovery: inconsistent Completing marker; leaving sidecar and data intact and unavailable"
                    );
                    rejected_count += 1;
                    continue;
                }

                // The marker follows spool.dat fdatasync and carries the exact final
                // layout. Finalization is persisted only if this candidate passes
                // data validation and is admitted.
                meta.state = SpoolState::Complete;
                meta.readable_at.get_or_insert_with(now_secs);
            }

            let migration = match prepare_recovery_metadata(&mut meta, file_size, self.page_size) {
                Ok(migration) => migration,
                Err(reason) => {
                    tracing::warn!(
                        key = %key,
                        state = ?meta.state,
                        file_size = file_size,
                        reason = %reason,
                        "recovery: candidate metadata cannot be recovered safely; leaving sidecar and data intact and unavailable"
                    );
                    rejected_count += 1;
                    continue;
                }
            };
            let spool_page_size = migration.page_size;
            let mut metadata_needs_persist = finalized_completing || migration.persist;
            let mut trailing_partial_len = 0;

            // Backfill readable_at for spools persisted before this field existed.
            // The write is deferred until admission and data validation succeed.
            if meta.state == SpoolState::Complete && meta.readable_at.is_none() {
                meta.readable_at = Some(now_secs());
                metadata_needs_persist = true;
            }

            if matches!(meta.state, SpoolState::Writing | SpoolState::WriteLocked) {
                let progress = in_progress_progress_from_file(file_size, spool_page_size);
                trailing_partial_len = progress.trailing_partial_len;

                if meta.total_bytes_written != progress.total_bytes_written
                    || meta.total_pages != progress.total_pages
                    || meta.final_page_size.is_some()
                {
                    tracing::warn!(
                        key = %key,
                        file_size = file_size,
                        total_pages = progress.total_pages,
                        trailing_partial_len = trailing_partial_len,
                        "recovery: deriving in-progress spool metadata from disk"
                    );
                    meta.total_bytes_written = progress.total_bytes_written;
                    meta.total_pages = progress.total_pages;
                    meta.final_page_size = None;
                }
            } else {
                let expected_full_pages_bytes = match complete_layout_bytes(&meta, meta.page_size) {
                    Ok(expected) => expected,
                    Err(reason) => {
                        tracing::warn!(
                            key = %key,
                            reason = %reason,
                            "recovery: completed spool metadata is invalid; leaving sidecar and data intact and unavailable"
                        );
                        rejected_count += 1;
                        continue;
                    }
                };

                if file_size < expected_full_pages_bytes {
                    tracing::warn!(
                        key = %key,
                        expected_pages = meta.total_pages,
                        expected_bytes = expected_full_pages_bytes,
                        file_size = file_size,
                        "recovery: completed spool data file shorter than persisted logical size, discarding"
                    );
                    stale_keys.push(key);
                    continue;
                }
            }

            if recovered_count >= self.max_live_spools {
                quarantined_count += 1;
                tracing::warn!(
                    "event.name" = "bobs.recovery.spool_quarantined",
                    key = %key,
                    configured = self.max_live_spools,
                    activity_at = recovery_activity_at(&meta),
                    reason = "admission_capacity",
                    "recovery: valid excess candidate left durable and unopened for a later restart"
                );
                continue;
            }

            let permit = match Arc::clone(&self.admission).try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    quarantined_count += 1;
                    tracing::warn!(
                        "event.name" = "bobs.recovery.spool_quarantined",
                        key = %key,
                        configured = self.max_live_spools,
                        activity_at = recovery_activity_at(&meta),
                        reason = "admission_capacity",
                        "recovery: valid excess candidate left durable and unopened for a later restart"
                    );
                    continue;
                }
            };

            // Opening and loading a partial tail are data validation, so they happen
            // only while this candidate owns a slot. Any failure drops the permit
            // immediately and recovery continues with the next ordered candidate.
            let handle = match F::open(&meta.data_path).await {
                Ok(handle) => handle,
                Err(error) => {
                    tracing::warn!(
                        key = %key,
                        error = %error,
                        "recovery: failed to open data file; leaving spool intact and trying the next candidate"
                    );
                    rejected_count += 1;
                    drop(permit);
                    continue;
                }
            };

            let trailing_partial = if trailing_partial_len > 0 {
                match read_exact_at::<F>(
                    &handle,
                    meta.total_bytes_written - trailing_partial_len,
                    trailing_partial_len as usize,
                    "loading trailing partial page during recovery",
                )
                .await
                {
                    Ok(partial) => Some(partial),
                    Err(error) => {
                        tracing::warn!(
                            key = %key,
                            error = %error,
                            "recovery: failed to validate trailing partial page; leaving spool intact and trying the next candidate"
                        );
                        if let Err(close_error) = F::close(handle).await {
                            tracing::warn!(key = %key, error = %close_error, "recovery: failed to close rejected data file handle");
                        }
                        rejected_count += 1;
                        drop(permit);
                        continue;
                    }
                }
            } else {
                None
            };

            // A legacy or Completing sidecar becomes self-describing/terminal only
            // after the admitted candidate's data has passed validation.
            if metadata_needs_persist {
                if migration.write_locked_salvage {
                    tracing::warn!(
                        key = %key,
                        page_size = spool_page_size,
                        file_size = file_size,
                        "recovery: atomically migrating legacy WriteLocked spool to Complete salvage; further writes are rejected, reads and idempotent completion remain available"
                    );
                } else if migration.persist {
                    tracing::info!(
                        key = %key,
                        page_size = spool_page_size,
                        "recovery: atomically migrating legacy sidecar metadata"
                    );
                }
                if finalized_completing {
                    tracing::info!(
                        key = %key,
                        total_bytes = meta.total_bytes_written,
                        total_pages = meta.total_pages,
                        "recovery: finalizing durable Completing marker"
                    );
                }
                if let Err(error) = self.metadata_store.write(&meta).await {
                    if let Err(close_error) = F::close(handle).await {
                        tracing::warn!(key = %key, error = %close_error, "recovery: failed to close data file after metadata persistence failure");
                    }
                    return Err(error);
                }
            }

            // Capture fields needed for post-init before meta is moved.
            let meta_state_for_init = meta.state.clone();
            let meta_write_locked_for_init = meta.write_locked;
            let meta_total_bytes_for_init = meta.total_bytes_written;
            let read_response_permits = read_response_permits_for_page(
                self.page_size,
                spool_page_size,
                self.read_response_permit_limit,
            );

            let spool = Arc::new(Spool::new_with_admission(
                meta,
                handle,
                spool_page_size,
                Arc::clone(&self.page_cache),
                self.metadata_store.clone(),
                Arc::clone(&self.metrics),
                Some(permit),
                Arc::clone(&self.read_response_admission),
                Arc::clone(&self.read_response_active),
                Arc::clone(&self.read_response_permits_active),
                read_response_permits,
            ));

            if let Some(partial) = trailing_partial {
                spool.write_buffer.lock().await.extend_from_slice(&partial);
            }

            // Spool construction reseeds monotonic cleanup anchors. Recovered
            // writers and readable objects therefore receive a full TTL grace period,
            // independent of persisted wall-clock timestamps.

            // No served ranges are known after restart, so complete spool coverage
            // resets to [0, total_size).
            if matches!(meta_state_for_init, SpoolState::Complete) {
                spool
                    .missing_ranges
                    .lock()
                    .await
                    .initialize(meta_total_bytes_for_init);
            }

            self.spools.insert(key, spool);
            recovered_count += 1;

            let recovered_label =
                metric_state_label(&meta_state_for_init, meta_write_locked_for_init);
            self.metrics.record_state_transition(None, recovered_label);
        }

        stale_keys.sort();
        stale_keys.dedup();

        for key in &stale_keys {
            self.metadata_store.delete(key).await?;
            self.remove_spool_directory_durably(key).await?;
            corrupt_deleted += 1;
            tracing::info!("event.name" = "bobs.spool.deleted", "bobs.spool.key" = %key, reason = DeleteReason::Corrupt.as_str(), outcome = "success", "spool deleted during recovery");
        }

        let stale_key_set: HashSet<String> = stale_keys.iter().cloned().collect();
        let mut entries = tokio::fs::read_dir(&self.data_dir)
            .await
            .map_err(BobsError::IoError)?;
        while let Some(entry) = entries.next_entry().await.map_err(BobsError::IoError)? {
            let name = entry.file_name().to_string_lossy().to_string();
            let file_type = entry.file_type().await.map_err(BobsError::IoError)?;
            if file_type.is_symlink() {
                tracing::warn!(orphan = %name, "recovery: skipping symlink during orphan sweep");
                continue;
            }
            if !file_type.is_dir() {
                tracing::warn!(orphan = %name, "recovery: skipping non-directory entry during orphan sweep");
                continue;
            }
            if !is_recognised_spool_key(&name) {
                tracing::warn!(orphan = %name, "recovery: skipping unrecognised directory during orphan sweep");
                continue;
            }
            if recovered_keys.contains(&name)
                || self.spools.contains_key(&name)
                || stale_key_set.contains(&name)
            {
                continue;
            }

            let entry_path = entry.path();
            // A crash immediately after create_dir can leave a recognised key
            // directory before any spool marker exists. remove_dir is the atomic
            // emptiness check: if data appears after inspection it fails rather
            // than recursively deleting that data.
            let shaped_like_spool =
                entry_path.join("spool.dat").exists() || entry_path.join("meta.json").exists();
            if !shaped_like_spool {
                match tokio::fs::remove_dir(&entry_path).await {
                    Ok(()) => {
                        orphan_deleted += 1;
                        tracing::debug!(orphan = %name, "recovery: removed empty pre-marker spool directory; parent sync pending");
                    }
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::DirectoryNotEmpty | std::io::ErrorKind::NotFound
                        ) =>
                    {
                        if error.kind() == std::io::ErrorKind::DirectoryNotEmpty {
                            tracing::warn!(orphan = %name, "recovery: preserving non-empty spool-key directory without spool markers");
                        }
                    }
                    Err(error) => {
                        tracing::warn!(orphan = %name, error = %error, "recovery: failed to inspect/remove empty pre-marker spool directory");
                    }
                }
                continue;
            }

            tracing::warn!(orphan = %name, "removing orphan spool directory");
            match self.remove_spool_directory_durably(&name).await {
                Ok(()) => {
                    orphan_deleted += 1;
                    tracing::info!("event.name" = "bobs.spool.deleted", "bobs.spool.key" = %name, reason = DeleteReason::Orphan.as_str(), outcome = "success", "orphan spool deleted during recovery");
                }
                Err(error) => {
                    tracing::warn!(orphan = %name, error = %error, "failed to durably remove orphan spool directory");
                }
            }
        }

        // Commit empty pre-marker removals, and retry any parent-directory
        // durability boundary left uncertain by an earlier failed recovery.
        F::sync_directory(&self.data_dir)
            .await
            .map_err(BobsError::IoError)?;

        self.metrics.record_recovery_snapshot(
            self.max_live_spools,
            recovered_count,
            quarantined_count,
        );

        if quarantined_count > 0 {
            tracing::warn!(
                "event.name" = "bobs.recovery.quarantine_summary",
                configured = self.max_live_spools,
                recovered = recovered_count,
                quarantined = quarantined_count,
                rejected = rejected_count,
                "recovery completed with valid durable spools quarantined"
            );
        }

        tracing::info!(
            "event.name" = "bobs.recovery.completed",
            configured = self.max_live_spools,
            recovered = recovered_count,
            quarantined = quarantined_count,
            rejected = rejected_count,
            active = self.spools.len(),
            stale = stale_keys.len(),
            orphan_deleted = orphan_deleted,
            corrupt_deleted = corrupt_deleted,
            duration_ms = started.elapsed().as_millis() as u64,
            outcome = "success",
            "recovery completed"
        );
        Ok((recovered_count, quarantined_count))
    }
}

fn recovery_activity_at(metadata: &SpoolMetadata) -> u64 {
    metadata
        .last_read_at
        .into_iter()
        .chain(metadata.readable_at)
        .chain([metadata.last_write_at, metadata.created_at])
        .max()
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecoveryMigration {
    page_size: usize,
    persist: bool,
    write_locked_salvage: bool,
}

/// Make old-main sidecars self-describing. Legacy `Readable` and
/// `WriteLocked` objects cannot safely resume writes after a page-size change,
/// but their files are contiguous durable byte streams. Resegment them with the
/// current configured page size and terminalize them for read/completion salvage.
/// Legacy `Writing` objects still require an unambiguous old stride; callers
/// quarantine ambiguous layouts rather than mutating or deleting them.
fn prepare_recovery_metadata(
    meta: &mut SpoolMetadata,
    file_size: u64,
    configured_page_size: usize,
) -> std::result::Result<RecoveryMigration, String> {
    let missing_page_size = meta.page_size == 0;
    let legacy_readable = meta.state == SpoolState::Complete && meta.final_page_size == Some(0);
    let legacy_write_locked = missing_page_size && meta.state == SpoolState::WriteLocked;
    let resegment_terminal = legacy_readable || legacy_write_locked;

    let page_size_u64 = if resegment_terminal {
        u64::try_from(configured_page_size)
            .ok()
            .filter(|size| *size > 0)
            .ok_or_else(|| {
                "configured page size cannot describe this legacy terminal salvage spool on disk"
                    .to_string()
            })?
    } else if missing_page_size {
        derive_legacy_page_size(meta, file_size)?
    } else {
        meta.page_size
    };
    let page_size = usize::try_from(page_size_u64)
        .ok()
        .filter(|size| *size > 0)
        .ok_or_else(|| {
            "persisted or derived page size is unsupported on this platform".to_string()
        })?;

    if legacy_readable {
        terminalize_legacy_readable(meta, file_size, page_size_u64)?;
    } else if legacy_write_locked {
        terminalize_legacy_write_locked(meta, file_size, page_size_u64)?;
    }

    let persist = missing_page_size || legacy_readable;
    if persist {
        meta.page_size = page_size_u64;
    }

    Ok(RecoveryMigration {
        page_size,
        persist,
        write_locked_salvage: legacy_write_locked,
    })
}

fn derive_legacy_page_size(
    meta: &SpoolMetadata,
    file_size: u64,
) -> std::result::Result<u64, String> {
    if matches!(meta.state, SpoolState::Writing | SpoolState::WriteLocked) {
        validate_legacy_active_layout(meta, file_size)?;
        if meta.total_pages == 0 {
            // No page-1 offset exists. Keep every durable byte in the trailing
            // buffer by choosing a stride strictly larger than a non-empty file.
            return if file_size == 0 {
                Ok(1)
            } else {
                file_size
                    .checked_add(1)
                    .ok_or_else(|| "durable file is too large to choose a safe stride".into())
            };
        }
        if file_size == 0 || !file_size.is_multiple_of(meta.total_pages) {
            return Err(
                "durable file length is not exactly divisible by the active page count".into(),
            );
        }
        return Ok(file_size / meta.total_pages);
    }

    if meta.state != SpoolState::Complete {
        return Err("legacy page size is unavailable for this lifecycle state".into());
    }
    if meta.total_bytes_written != file_size {
        return Err("terminal byte count does not equal durable file length".into());
    }

    match meta.total_pages {
        0 => {
            if file_size != 0 || meta.final_page_size.is_some() {
                Err("zero-page terminal metadata describes non-empty data".into())
            } else {
                // Empty objects have no page offset; one is the minimal safe
                // positive stride and cannot affect the byte stream.
                Ok(1)
            }
        }
        1 => {
            if file_size == 0 {
                return Err("one-page terminal metadata describes an empty file".into());
            }
            if let Some(final_size) = meta.final_page_size {
                if final_size != file_size {
                    return Err(
                        "one-page terminal final size does not equal durable file length".into(),
                    );
                }
            }
            // Page zero always starts at offset zero, so its exact durable length
            // is a safe stride even though no second-page offset exists.
            Ok(file_size)
        }
        total_pages => match meta.final_page_size {
            Some(final_size) => {
                if final_size == 0 || final_size > file_size {
                    return Err("terminal final page size is outside the durable file".into());
                }
                let full_page_count = total_pages - 1;
                let full_bytes = file_size - final_size;
                if !full_bytes.is_multiple_of(full_page_count) {
                    return Err(
                        "durable prefix is not exactly divisible by the full-page count".into(),
                    );
                }
                let page_size = full_bytes / full_page_count;
                if page_size == 0 || final_size > page_size {
                    return Err("derived terminal page stride is inconsistent".into());
                }
                Ok(page_size)
            }
            None => {
                if !file_size.is_multiple_of(total_pages) {
                    return Err(
                        "durable file length is not exactly divisible by the terminal page count"
                            .into(),
                    );
                }
                let page_size = file_size / total_pages;
                if page_size == 0 {
                    Err("derived terminal page stride is zero".into())
                } else {
                    Ok(page_size)
                }
            }
        },
    }
}

fn validate_legacy_active_layout(
    meta: &SpoolMetadata,
    file_size: u64,
) -> std::result::Result<(), String> {
    if meta.final_page_size.is_some() {
        return Err("active legacy metadata unexpectedly records a final page size".into());
    }
    if meta.total_bytes_written > file_size {
        return Err("persisted byte count exceeds durable file length".into());
    }

    let full_pages = meta.total_pages;
    if full_pages == 0 {
        return Ok(());
    }
    if file_size == 0 || full_pages > file_size {
        return Err("persisted full-page count cannot fit in the durable file".into());
    }

    // Check that at least one positive old stride could produce exactly this
    // count of full pages. This validates the sidecar without selecting a stride
    // that could later be mistaken for the historical layout.
    let maximum_stride = file_size / full_pages;
    let minimum_stride = if full_pages == u64::MAX {
        1
    } else {
        file_size / (full_pages + 1) + 1
    };
    if minimum_stride > maximum_stride {
        return Err("persisted full-page count is impossible for the durable file length".into());
    }

    Ok(())
}

fn terminalize_legacy_readable(
    meta: &mut SpoolMetadata,
    file_size: u64,
    page_size: u64,
) -> std::result::Result<(), String> {
    if meta.final_page_size != Some(0) {
        return Err("legacy Readable recovery marker is missing".into());
    }
    resegment_complete_bytes(meta, file_size, page_size, false, "legacy Readable")
}

fn terminalize_legacy_write_locked(
    meta: &mut SpoolMetadata,
    file_size: u64,
    page_size: u64,
) -> std::result::Result<(), String> {
    validate_legacy_active_layout(meta, file_size)?;
    if !meta.write_locked {
        return Err("legacy WriteLocked metadata has its write-lock flag cleared".into());
    }

    resegment_complete_bytes(meta, file_size, page_size, true, "legacy WriteLocked")
}

fn resegment_complete_bytes(
    meta: &mut SpoolMetadata,
    file_size: u64,
    page_size: u64,
    write_locked: bool,
    description: &str,
) -> std::result::Result<(), String> {
    if page_size == 0 {
        return Err(format!("{description} target page size is zero"));
    }

    let final_size = file_size % page_size;
    let full_pages = file_size / page_size;
    let total_pages = full_pages
        .checked_add(u64::from(final_size != 0))
        .ok_or_else(|| format!("{description} page count overflows u64"))?;

    meta.state = SpoolState::Complete;
    // Preserve the historical write-lock flag for WriteLocked provenance. The
    // Complete state is authoritative: writes fail, while reads and idempotent
    // completion (including expected-size validation) remain available.
    meta.write_locked = write_locked;
    meta.total_bytes_written = file_size;
    meta.total_pages = total_pages;
    meta.final_page_size = (final_size != 0).then_some(final_size);
    Ok(())
}

fn complete_layout_bytes(meta: &SpoolMetadata, page_size: u64) -> std::result::Result<u64, String> {
    if page_size == 0 {
        return Err("complete spool page size is zero".into());
    }
    if meta.total_pages == 0 {
        return if meta.final_page_size.is_none() {
            Ok(0)
        } else {
            Err("zero-page complete metadata records a final page size".into())
        };
    }

    match meta.final_page_size {
        Some(final_size) => {
            if final_size == 0 || final_size > page_size {
                return Err("complete spool final page size exceeds its page stride".into());
            }
            (meta.total_pages - 1)
                .checked_mul(page_size)
                .and_then(|bytes| bytes.checked_add(final_size))
                .ok_or_else(|| "complete spool byte layout overflows u64".into())
        }
        None => meta
            .total_pages
            .checked_mul(page_size)
            .ok_or_else(|| "complete spool byte layout overflows u64".into()),
    }
}

fn validate_completing_marker(
    meta: &SpoolMetadata,
    file_size: u64,
) -> std::result::Result<(), String> {
    if meta.state != SpoolState::Completing {
        return Err("metadata is not a Completing marker".into());
    }
    let candidate_bytes = complete_layout_bytes(meta, meta.page_size)?;
    if candidate_bytes != meta.total_bytes_written {
        return Err(format!(
            "candidate layout describes {candidate_bytes} bytes but marker records {}",
            meta.total_bytes_written
        ));
    }
    if file_size != candidate_bytes {
        return Err(format!(
            "data file has {file_size} bytes but candidate layout requires exactly {candidate_bytes}"
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InProgressProgress {
    total_bytes_written: u64,
    total_pages: u64,
    trailing_partial_len: u64,
}

/// A data-directory entry is a recognised spool key if it is a UUID (anonymous
/// spools) or a 26-character Crockford base32 request ID (spools keyed by the
/// originating request, see the `/create` handler). The orphan sweep only
/// touches recognised keys, so unrelated operator directories are left alone.
pub(crate) fn is_recognised_spool_key(name: &str) -> bool {
    uuid::Uuid::parse_str(name).is_ok() || is_request_id_key(name)
}

/// True for a 26-character lower-case Crockford base32 request ID (the format
/// BITS mints and clients quote). Crockford base32 excludes i, l, o and u.
pub(crate) fn is_request_id_key(name: &str) -> bool {
    name.len() == 26
        && name.bytes().all(|b| {
            matches!(
                b,
                b'0'..=b'9' | b'a'..=b'h' | b'j' | b'k' | b'm' | b'n' | b'p'..=b't' | b'v'..=b'z'
            )
        })
}

fn in_progress_progress_from_file(file_size: u64, page_size: usize) -> InProgressProgress {
    let page_size = page_size as u64;
    InProgressProgress {
        total_bytes_written: file_size,
        total_pages: file_size / page_size,
        trailing_partial_len: file_size % page_size,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::TokioFileIO;
    use bytes::Bytes;
    use std::sync::{Arc, OnceLock};
    use tempfile::tempdir;

    #[derive(Clone)]
    struct BlockingWriteFileIO;

    struct BlockingWriteHandle {
        inner: <TokioFileIO as FileIO>::Handle,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    static BLOCKING_WRITE_CONTROL: OnceLock<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)> =
        OnceLock::new();

    impl FileIO for BlockingWriteFileIO {
        type Handle = BlockingWriteHandle;

        async fn create(path: &Path) -> std::io::Result<Self::Handle> {
            let (entered, release) = BLOCKING_WRITE_CONTROL
                .get()
                .expect("blocking write control initialized");
            Ok(BlockingWriteHandle {
                inner: TokioFileIO::create(path).await?,
                entered: Arc::clone(entered),
                release: Arc::clone(release),
            })
        }

        async fn open(path: &Path) -> std::io::Result<Self::Handle> {
            let (entered, release) = BLOCKING_WRITE_CONTROL
                .get()
                .expect("blocking write control initialized");
            Ok(BlockingWriteHandle {
                inner: TokioFileIO::open(path).await?,
                entered: Arc::clone(entered),
                release: Arc::clone(release),
            })
        }

        async fn write_at(
            handle: &Self::Handle,
            offset: u64,
            data: Bytes,
        ) -> std::io::Result<usize> {
            handle.entered.notify_one();
            handle.release.notified().await;
            TokioFileIO::write_at(&handle.inner, offset, data).await
        }

        async fn read_at(handle: &Self::Handle, offset: u64, len: usize) -> std::io::Result<Bytes> {
            TokioFileIO::read_at(&handle.inner, offset, len).await
        }

        async fn sync_data(handle: &Self::Handle) -> std::io::Result<()> {
            TokioFileIO::sync_data(&handle.inner).await
        }

        async fn sync_directory(path: &Path) -> std::io::Result<()> {
            TokioFileIO::sync_directory(path).await
        }

        async fn close(handle: Self::Handle) -> std::io::Result<()> {
            TokioFileIO::close(handle.inner).await
        }

        async fn remove(path: &Path) -> std::io::Result<()> {
            TokioFileIO::remove(path).await
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum ProtocolEvent {
        DataCreate,
        DataSync,
        SpoolDirectorySync,
        ParentDirectorySync,
        MetadataWrite(SpoolState),
        MetadataDelete,
        Close,
    }

    static PROTOCOL_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    static PROTOCOL_EVENTS: OnceLock<std::sync::Mutex<Vec<ProtocolEvent>>> = OnceLock::new();
    static FAIL_NEXT_PARENT_SYNC: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);
    static FAIL_NEXT_LIVE_METADATA_WRITE: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);

    fn protocol_events() -> &'static std::sync::Mutex<Vec<ProtocolEvent>> {
        PROTOCOL_EVENTS.get_or_init(|| std::sync::Mutex::new(Vec::new()))
    }

    fn record_protocol_event(event: ProtocolEvent) {
        protocol_events()
            .lock()
            .expect("protocol event log mutex poisoned")
            .push(event);
    }

    fn take_protocol_events() -> Vec<ProtocolEvent> {
        std::mem::take(
            &mut *protocol_events()
                .lock()
                .expect("protocol event log mutex poisoned"),
        )
    }

    #[derive(Clone)]
    struct ProtocolFileIO;

    impl FileIO for ProtocolFileIO {
        type Handle = <TokioFileIO as FileIO>::Handle;

        fn create(
            path: &Path,
        ) -> impl std::future::Future<Output = std::io::Result<Self::Handle>> + Send {
            record_protocol_event(ProtocolEvent::DataCreate);
            TokioFileIO::create(path)
        }

        fn open(
            path: &Path,
        ) -> impl std::future::Future<Output = std::io::Result<Self::Handle>> + Send {
            TokioFileIO::open(path)
        }

        fn write_at(
            handle: &Self::Handle,
            offset: u64,
            data: Bytes,
        ) -> impl std::future::Future<Output = std::io::Result<usize>> + Send {
            TokioFileIO::write_at(handle, offset, data)
        }

        fn read_at(
            handle: &Self::Handle,
            offset: u64,
            len: usize,
        ) -> impl std::future::Future<Output = std::io::Result<Bytes>> + Send {
            TokioFileIO::read_at(handle, offset, len)
        }

        fn sync_data(
            handle: &Self::Handle,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            record_protocol_event(ProtocolEvent::DataSync);
            TokioFileIO::sync_data(handle)
        }

        fn sync_directory(
            path: &Path,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            let path = path.to_path_buf();
            async move {
                let parent = path.ends_with("data");
                record_protocol_event(if parent {
                    ProtocolEvent::ParentDirectorySync
                } else {
                    ProtocolEvent::SpoolDirectorySync
                });
                if parent && FAIL_NEXT_PARENT_SYNC.swap(false, std::sync::atomic::Ordering::SeqCst)
                {
                    return Err(std::io::Error::other(
                        "injected parent directory fsync failure",
                    ));
                }
                TokioFileIO::sync_directory(&path).await
            }
        }

        fn close(
            handle: Self::Handle,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            record_protocol_event(ProtocolEvent::Close);
            TokioFileIO::close(handle)
        }

        fn remove(path: &Path) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            TokioFileIO::remove(path)
        }
    }

    static RECOVERY_IO_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    static RECOVERY_OPEN_ATTEMPTS: std::sync::atomic::AtomicUsize =
        std::sync::atomic::AtomicUsize::new(0);
    static RECOVERY_OPEN_COUNT: std::sync::atomic::AtomicUsize =
        std::sync::atomic::AtomicUsize::new(0);
    static RECOVERY_READ_BYTES: std::sync::atomic::AtomicUsize =
        std::sync::atomic::AtomicUsize::new(0);
    const RECOVERY_FAIL_OPEN_KEY: &str = "ffffffff-ffff-4fff-8fff-ffffffffff05";

    #[derive(Clone)]
    struct RecoveryCountingFileIO;

    impl FileIO for RecoveryCountingFileIO {
        type Handle = <TokioFileIO as FileIO>::Handle;

        fn create(
            path: &Path,
        ) -> impl std::future::Future<Output = std::io::Result<Self::Handle>> + Send {
            TokioFileIO::create(path)
        }

        fn open(
            path: &Path,
        ) -> impl std::future::Future<Output = std::io::Result<Self::Handle>> + Send {
            async move {
                RECOVERY_OPEN_ATTEMPTS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if path
                    .parent()
                    .and_then(Path::file_name)
                    .is_some_and(|key| key == RECOVERY_FAIL_OPEN_KEY)
                {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "injected corrupt recovery candidate",
                    ));
                }
                let handle = TokioFileIO::open(path).await?;
                RECOVERY_OPEN_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(handle)
            }
        }

        fn write_at(
            handle: &Self::Handle,
            offset: u64,
            data: Bytes,
        ) -> impl std::future::Future<Output = std::io::Result<usize>> + Send {
            TokioFileIO::write_at(handle, offset, data)
        }

        fn read_at(
            handle: &Self::Handle,
            offset: u64,
            len: usize,
        ) -> impl std::future::Future<Output = std::io::Result<Bytes>> + Send {
            async move {
                let bytes = TokioFileIO::read_at(handle, offset, len).await?;
                RECOVERY_READ_BYTES.fetch_add(bytes.len(), std::sync::atomic::Ordering::SeqCst);
                Ok(bytes)
            }
        }

        fn sync_data(
            handle: &Self::Handle,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            TokioFileIO::sync_data(handle)
        }

        fn sync_directory(
            path: &Path,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            TokioFileIO::sync_directory(path)
        }

        fn close(
            handle: Self::Handle,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            TokioFileIO::close(handle)
        }

        fn remove(path: &Path) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            TokioFileIO::remove(path)
        }
    }

    const CREATE_GATE_OPEN: usize = 1;
    const CREATE_GATE_SYNC: usize = 2;
    static CREATE_GATE_STAGE: std::sync::atomic::AtomicUsize =
        std::sync::atomic::AtomicUsize::new(0);
    static CREATE_GATE_STARTED: tokio::sync::Notify = tokio::sync::Notify::const_new();
    static CREATE_GATE_RELEASE: tokio::sync::Notify = tokio::sync::Notify::const_new();

    async fn wait_at_create_gate(stage: usize) {
        if CREATE_GATE_STAGE
            .compare_exchange(
                stage,
                0,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .is_ok()
        {
            CREATE_GATE_STARTED.notify_waiters();
            CREATE_GATE_RELEASE.notified().await;
        }
    }

    #[derive(Clone)]
    struct CancellationFileIO;

    impl FileIO for CancellationFileIO {
        type Handle = <TokioFileIO as FileIO>::Handle;

        fn create(
            path: &Path,
        ) -> impl std::future::Future<Output = std::io::Result<Self::Handle>> + Send {
            let path = path.to_path_buf();
            async move {
                let handle = TokioFileIO::create(&path).await?;
                wait_at_create_gate(CREATE_GATE_OPEN).await;
                Ok(handle)
            }
        }

        fn open(
            path: &Path,
        ) -> impl std::future::Future<Output = std::io::Result<Self::Handle>> + Send {
            TokioFileIO::open(path)
        }

        fn write_at(
            handle: &Self::Handle,
            offset: u64,
            data: Bytes,
        ) -> impl std::future::Future<Output = std::io::Result<usize>> + Send {
            TokioFileIO::write_at(handle, offset, data)
        }

        fn read_at(
            handle: &Self::Handle,
            offset: u64,
            len: usize,
        ) -> impl std::future::Future<Output = std::io::Result<Bytes>> + Send {
            TokioFileIO::read_at(handle, offset, len)
        }

        fn sync_data(
            handle: &Self::Handle,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            let handle = Arc::clone(handle);
            async move {
                wait_at_create_gate(CREATE_GATE_SYNC).await;
                TokioFileIO::sync_data(&handle).await
            }
        }

        fn sync_directory(
            path: &Path,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            TokioFileIO::sync_directory(path)
        }

        fn close(
            handle: Self::Handle,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            TokioFileIO::close(handle)
        }

        fn remove(path: &Path) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            TokioFileIO::remove(path)
        }
    }

    #[derive(Clone)]
    struct ProtocolMetadataStore {
        inner: SyncSidecarMetadataStore,
    }

    impl ProtocolMetadataStore {
        fn new(data_dir: &Path) -> Self {
            Self {
                inner: SyncSidecarMetadataStore::new(data_dir),
            }
        }
    }

    impl MetadataStore for ProtocolMetadataStore {
        async fn write(&self, metadata: &SpoolMetadata) -> Result<()> {
            record_protocol_event(ProtocolEvent::MetadataWrite(metadata.state.clone()));
            if metadata.state != SpoolState::Creating
                && FAIL_NEXT_LIVE_METADATA_WRITE.swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(BobsError::StorageError(Box::new(std::io::Error::other(
                    "injected live metadata commit failure",
                ))));
            }
            self.inner.write(metadata).await
        }

        async fn read(&self, key: &str) -> Result<Option<SpoolMetadata>> {
            self.inner.read(key).await
        }

        async fn delete(&self, key: &str) -> Result<()> {
            record_protocol_event(ProtocolEvent::MetadataDelete);
            self.inner.delete(key).await
        }

        async fn list(&self) -> Result<Vec<(String, Result<SpoolMetadata>)>> {
            self.inner.list().await
        }
    }

    #[test]
    fn recognised_spool_keys_accept_uuid_and_request_id() {
        // Anonymous UUID spools and request-ID-keyed spools are both recognised.
        assert!(is_recognised_spool_key(&uuid::Uuid::new_v4().to_string()));
        assert!(is_recognised_spool_key("0123456789abcdefghjkmnpqrs"));
        assert!(is_request_id_key("0123456789abcdefghjkmnpqrs"));
        // Junk directory names are not recognised (orphan sweep leaves them).
        assert!(!is_recognised_spool_key("not-a-key"));
        assert!(!is_request_id_key("0123456789abcdefghjkmnpqr")); // 25 chars
        assert!(!is_request_id_key("0123456789abcdefghijklmnop")); // i, l, o excluded
        assert!(!is_request_id_key("0123456789ABCDEFGHJKMNPQRS")); // upper-case excluded
    }

    async fn persisted_metadata(manager: &SpoolManager<TokioFileIO>, key: &str) -> SpoolMetadata {
        manager
            .metadata_store
            .read(key)
            .await
            .expect("read metadata")
            .expect("entry exists")
    }

    fn sidecar_fixture_metadata(
        data_dir: &Path,
        key: &str,
        state: SpoolState,
        logical_size: u64,
    ) -> SpoolMetadata {
        let page_size = 4096u64;
        let total_pages = if logical_size == 0 {
            0
        } else {
            logical_size.div_ceil(page_size)
        };
        let final_page_size = if total_pages == 0 || logical_size.is_multiple_of(page_size) {
            None
        } else {
            Some(logical_size % page_size)
        };
        SpoolMetadata {
            key: key.to_string(),
            content_type: None,
            content_encoding: None,
            state,
            write_locked: false,
            created_at: 11,
            last_write_at: 12,
            last_read_at: None,
            readable_at: Some(13),
            page_size,
            total_bytes_written: logical_size,
            total_pages,
            final_page_size,
            data_path: data_dir.join(key).join("spool.dat"),
            labels: HashMap::new(),
        }
    }

    async fn write_sidecar_fixture(
        manager: &SpoolManager<TokioFileIO>,
        metadata: SpoolMetadata,
        data: &[u8],
    ) {
        let spool_dir = manager.data_dir.join(&metadata.key);
        tokio::fs::create_dir_all(&spool_dir)
            .await
            .expect("create spool dir");
        tokio::fs::write(&metadata.data_path, data)
            .await
            .expect("write spool data");
        manager
            .metadata_store
            .write(&metadata)
            .await
            .expect("write sidecar metadata");
    }

    /// Write the exact sidecar shape emitted by main before `page_size` was
    /// added. `state` intentionally remains a JSON string so fixtures can cover
    /// the removed `Readable` variant without reintroducing it to SpoolState.
    async fn write_old_main_sidecar_fixture(
        data_dir: &Path,
        key: &str,
        state: &str,
        data: &[u8],
        total_pages: u64,
        final_page_size: Option<u64>,
    ) {
        let spool_dir = data_dir.join(key);
        let data_path = spool_dir.join("spool.dat");
        tokio::fs::create_dir_all(&spool_dir)
            .await
            .expect("create legacy fixture directory");
        tokio::fs::write(&data_path, data)
            .await
            .expect("write legacy fixture data");
        let sidecar = serde_json::json!({
            "key": key,
            "content_type": null,
            "content_encoding": null,
            "state": state,
            "write_locked": state == "WriteLocked",
            "created_at": 11,
            "last_write_at": 12,
            "last_read_at": null,
            "readable_at": 13,
            "total_bytes_written": data.len() as u64,
            "total_pages": total_pages,
            "final_page_size": final_page_size,
            "data_path": data_path,
            "labels": {},
        });
        tokio::fs::write(
            spool_dir.join("meta.json"),
            serde_json::to_vec(&sidecar).expect("serialize old-main fixture"),
        )
        .await
        .expect("write old-main sidecar");
    }

    #[tokio::test]
    async fn test_new_accepts_cache_smaller_than_page_size() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");

        let manager =
            SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 1024, 256).expect("manager init");

        assert_eq!(manager.page_size, 4096);
        assert_eq!(manager.max_cache_bytes, 1024);
        assert_eq!(manager.page_cache.lock().await.max_bytes(), 1024);
        assert_eq!(manager.read_response_permit_limit(), 1);
        assert_eq!(manager.read_response_available_permits(), 1);
    }

    #[tokio::test]
    async fn test_new_accepts_zero_cache_bytes() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");

        let manager =
            SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 0, 256).expect("manager init");

        assert_eq!(manager.max_cache_bytes, 0);
        assert_eq!(manager.page_cache.lock().await.max_bytes(), 0);
        assert_eq!(manager.read_response_permit_limit(), 1);
        assert_eq!(manager.read_response_available_permits(), 1);
    }

    #[test]
    fn read_response_budget_uses_cache_page_units_and_weights_recovered_layouts() {
        assert_eq!(
            read_response_permit_limit(4 * 1024 * 1024, 256 * 1024 * 1024),
            64
        );
        assert_eq!(read_response_permits_for_page(4, 4, 8), 1);
        assert_eq!(read_response_permits_for_page(4, 9, 8), 3);
        assert_eq!(
            read_response_permits_for_page(4, usize::MAX, 8),
            8,
            "a page wider than the whole budget must serialize responses"
        );
    }

    #[tokio::test]
    async fn test_manager_shares_one_global_page_cache_and_delete_drops_entries() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let manager =
            SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 8192, 256).expect("manager init");

        manager
            .create_spool("a".to_string(), None, None, false, HashMap::new())
            .await
            .expect("create spool a");
        manager
            .create_spool("b".to_string(), None, None, false, HashMap::new())
            .await
            .expect("create spool b");

        let spool_a = manager.get_spool("a").expect("spool a");
        let spool_b = manager.get_spool("b").expect("spool b");
        assert!(Arc::ptr_eq(&manager.page_cache, &spool_a.page_cache));
        assert!(Arc::ptr_eq(&manager.page_cache, &spool_b.page_cache));

        {
            let mut cache = manager.page_cache.lock().await;
            cache.insert("a", 0, bytes::Bytes::from_static(b"aaaa"));
            assert!(cache.current_bytes() <= cache.max_bytes());
            cache.insert("b", 0, bytes::Bytes::from_static(b"bbbb"));
            assert!(cache.current_bytes() <= cache.max_bytes());
            assert_eq!(cache.current_bytes(), 8);
        }

        manager.delete_spool("a").await.expect("delete spool a");

        let cache = manager.page_cache.lock().await;
        assert!(!cache.contains("a", 0));
        assert!(cache.contains("b", 0));
        assert_eq!(cache.current_bytes(), 4);
    }

    #[tokio::test]
    async fn test_writes_from_different_spools_compete_under_one_cache_cap() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let manager = SpoolManager::<TokioFileIO>::new(&data_dir, 4, 8, 256).expect("manager init");

        manager
            .create_spool("a".to_string(), None, None, false, HashMap::new())
            .await
            .expect("create spool a");
        manager
            .create_spool("b".to_string(), None, None, false, HashMap::new())
            .await
            .expect("create spool b");

        let spool_a = manager.get_spool("a").expect("spool a");
        let spool_b = manager.get_spool("b").expect("spool b");

        spool_a
            .write(0, bytes::Bytes::from_static(b"aaaa"))
            .await
            .expect("write a page 0");
        {
            let cache = manager.page_cache.lock().await;
            assert!(cache.current_bytes() <= cache.max_bytes());
            assert!(cache.contains("a", 0));
        }

        spool_b
            .write(0, bytes::Bytes::from_static(b"bbbb"))
            .await
            .expect("write b page 0");
        {
            let cache = manager.page_cache.lock().await;
            assert!(cache.current_bytes() <= cache.max_bytes());
            assert!(cache.contains("a", 0));
            assert!(cache.contains("b", 0));
            assert_eq!(cache.current_bytes(), 8);
        }

        spool_b
            .write(4, bytes::Bytes::from_static(b"cccc"))
            .await
            .expect("write b page 1");
        let cache = manager.page_cache.lock().await;
        assert!(cache.current_bytes() <= cache.max_bytes());
        assert!(
            !cache.contains("a", 0),
            "write by spool b should evict global oldest from spool a"
        );
        assert!(cache.contains("b", 0));
        assert!(cache.contains("b", 1));
        assert_eq!(cache.current_bytes(), 8);
    }

    #[tokio::test]
    async fn test_new_rejects_zero_page_size() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");

        let result = SpoolManager::<TokioFileIO>::new(&data_dir, 0, 1024, 256);

        assert!(matches!(result, Err(BobsError::ConfigurationError(_))));
    }

    #[tokio::test]
    async fn test_new_rejects_zero_max_live_spools() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");

        let result = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 1024, 0);

        assert!(matches!(result, Err(BobsError::ConfigurationError(_))));
    }

    #[tokio::test]
    async fn test_admission_blocks_at_capacity_and_releases_on_delete() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let manager = Arc::new(
            SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 1).expect("manager init"),
        );

        manager
            .create_spool("a".into(), None, None, false, HashMap::new())
            .await
            .expect("first create succeeds");

        let manager2 = Arc::clone(&manager);
        let mut create2 = tokio::spawn(async move {
            manager2
                .create_spool("b".into(), None, None, false, HashMap::new())
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !create2.is_finished(),
            "second create must block while admission is at capacity"
        );

        manager.delete_spool("a").await.expect("delete first");
        tokio::time::timeout(std::time::Duration::from_secs(5), &mut create2)
            .await
            .expect("second create should unblock once a slot frees")
            .expect("join")
            .expect("second create succeeds");
        assert!(manager.get_spool("b").is_some());
    }

    #[tokio::test]
    async fn cancelled_while_waiting_for_admission_leaves_no_reservation_and_retries() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let manager = Arc::new(
            SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 1).expect("manager init"),
        );
        manager
            .create_spool("admitted".into(), None, None, false, HashMap::new())
            .await
            .expect("occupy admission");

        let waiting_manager = Arc::clone(&manager);
        let waiting = tokio::spawn(async move {
            waiting_manager
                .create_spool("waiting".into(), None, None, false, HashMap::new())
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            !data_dir.join("waiting").exists(),
            "waiting for admission must not reserve the caller key"
        );
        waiting.abort();
        assert!(waiting
            .await
            .expect_err("caller task is cancelled")
            .is_cancelled());
        assert!(!data_dir.join("waiting").exists());

        manager
            .delete_spool("admitted")
            .await
            .expect("release admission");
        manager
            .create_spool("waiting".into(), None, None, false, HashMap::new())
            .await
            .expect("same-process retry succeeds");
        assert!(manager.get_spool("waiting").is_some());

        drop(manager);
        let restarted = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 1)
            .expect("restart manager init");
        restarted.recover().await.expect("restart recovery");
        assert!(restarted.get_spool("waiting").is_some());
        assert!(matches!(
            restarted
                .create_spool("waiting".into(), None, None, false, HashMap::new())
                .await,
            Err(BobsError::SpoolAlreadyExists { .. })
        ));
    }

    #[tokio::test]
    async fn cancellation_after_reservation_finishes_durable_transaction_at_open_and_sync() {
        for (index, stage) in [CREATE_GATE_OPEN, CREATE_GATE_SYNC].into_iter().enumerate() {
            let dir = tempdir().expect("create tempdir");
            let data_dir = dir.path().join("data");
            let key = format!("cancelled-stage-{index}");
            let manager = Arc::new(
                SpoolManager::<CancellationFileIO>::new(&data_dir, 4096, 16 * 4096, 1)
                    .expect("manager init"),
            );

            let started = CREATE_GATE_STARTED.notified();
            CREATE_GATE_STAGE.store(stage, std::sync::atomic::Ordering::SeqCst);
            let caller_manager = Arc::clone(&manager);
            let caller_key = key.clone();
            let caller = tokio::spawn(async move {
                caller_manager
                    .create_spool(caller_key, None, None, false, HashMap::new())
                    .await
            });
            started.await;
            assert!(
                data_dir.join(&key).exists(),
                "reservation must exist at gate"
            );
            caller.abort();
            assert!(caller
                .await
                .expect_err("caller task cancelled")
                .is_cancelled());
            CREATE_GATE_RELEASE.notify_one();

            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while manager.get_spool(&key).is_none() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("detached create transaction finishes");
            let durable = manager
                .metadata_store
                .read(&key)
                .await
                .expect("read metadata")
                .expect("metadata exists");
            assert_eq!(durable.state, SpoolState::Writing);
            assert_eq!(manager.admission.available_permits(), 0);

            let retry = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                manager.create_spool(key.clone(), None, None, false, HashMap::new()),
            )
            .await
            .expect("retry must report existing instead of waiting for admission");
            assert!(matches!(retry, Err(BobsError::SpoolAlreadyExists { .. })));

            drop(manager);
            let restarted = SpoolManager::<CancellationFileIO>::new(&data_dir, 4096, 16 * 4096, 1)
                .expect("restart manager init");
            restarted.recover().await.expect("restart recovery");
            assert_eq!(
                restarted
                    .get_spool(&key)
                    .expect("durable spool recovers")
                    .metadata
                    .lock()
                    .await
                    .state,
                SpoolState::Writing
            );
            assert!(matches!(
                restarted
                    .create_spool(key.clone(), None, None, false, HashMap::new())
                    .await,
                Err(BobsError::SpoolAlreadyExists { .. })
            ));
            restarted
                .delete_spool(&key)
                .await
                .expect("delete recovered spool");
            assert_eq!(restarted.admission.available_permits(), 1);
        }
    }

    #[tokio::test]
    async fn test_full_read_transition_releases_admission_before_delete() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let manager =
            SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 1).expect("manager init");

        manager
            .create_spool("a".into(), None, None, false, HashMap::new())
            .await
            .expect("first create succeeds");
        let spool = manager.get_spool("a").expect("spool a exists");
        spool.on_fully_read().await;

        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            manager.create_spool("b".into(), None, None, false, HashMap::new()),
        )
        .await
        .expect("create must not block after full-read transition releases admission")
        .expect("second create succeeds");
        assert!(manager.get_spool("a").is_some());
        assert!(manager.get_spool("b").is_some());
    }

    #[tokio::test]
    async fn sequential_follow_over_fragment_cap_releases_single_admission_slot() {
        const PAGE_SIZE: usize = 4096;
        const PAGES: u64 = 1025;
        let total_size = PAGES * PAGE_SIZE as u64;
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let manager = SpoolManager::<TokioFileIO>::new(&data_dir, PAGE_SIZE, 16 * PAGE_SIZE, 1)
            .expect("manager init");

        manager
            .create_spool("followed".into(), None, None, false, HashMap::new())
            .await
            .expect("create followed spool");
        let spool = manager.get_spool("followed").expect("spool exists");
        spool
            .write(0, Bytes::from(vec![0xA5; total_size as usize]))
            .await
            .expect("write pages");

        // A follow response reports one contiguous chunk per page while total size
        // is still unknown. This must remain one pending interval, not hit the cap.
        for page in 0..PAGES {
            let start = page * PAGE_SIZE as u64;
            spool
                .mark_served_and_maybe_fully_read(start, start + PAGE_SIZE as u64)
                .await;
        }
        assert_eq!(manager.admission.available_permits(), 0);

        spool
            .complete(Some(total_size))
            .await
            .expect("complete followed spool");
        assert!(spool.cleanup_anchors().full_object_read_at.is_some());
        assert_eq!(
            manager.admission.available_permits(),
            1,
            "completion must release max_live_spools=1 admission after full follow coverage"
        );

        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            manager.create_spool("next".into(), None, None, false, HashMap::new()),
        )
        .await
        .expect("next create must not remain blocked")
        .expect("next create succeeds");
    }

    #[tokio::test]
    async fn test_create_and_get() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");

        let manager = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
            .expect("manager init");

        let key = uuid::Uuid::new_v4().to_string();
        manager
            .create_spool(
                key.clone(),
                Some("application/octet-stream".into()),
                None,
                false,
                HashMap::new(),
            )
            .await
            .expect("create spool");

        let spool = manager.get_spool(&key).expect("spool should exist");
        let meta = spool.metadata.lock().await;
        assert_eq!(meta.key, key);
        assert_eq!(meta.state, SpoolState::Writing);
    }

    #[tokio::test]
    async fn test_directory_durability_ordering_failures_and_retry_release() {
        let _protocol_guard = PROTOCOL_TEST_LOCK.lock().await;
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let manager = SpoolManager::<ProtocolFileIO, ProtocolMetadataStore>::with_metadata_store(
            ProtocolMetadataStore::new(&data_dir),
            &data_dir,
            4,
            64,
            1,
        )
        .expect("manager init");

        take_protocol_events();
        let first_key = uuid::Uuid::new_v4().to_string();
        manager
            .create_spool(first_key.clone(), None, None, false, HashMap::new())
            .await
            .expect("durable create succeeds");
        assert_eq!(
            take_protocol_events(),
            vec![
                ProtocolEvent::DataCreate,
                ProtocolEvent::DataSync,
                ProtocolEvent::SpoolDirectorySync,
                ProtocolEvent::MetadataWrite(SpoolState::Creating),
                ProtocolEvent::ParentDirectorySync,
                ProtocolEvent::MetadataWrite(SpoolState::Writing),
            ],
            "create must durably publish the key directory with a Creating tombstone before the live sidecar"
        );

        manager
            .delete_spool(&first_key)
            .await
            .expect("durable delete succeeds");
        assert_eq!(
            take_protocol_events(),
            vec![
                ProtocolEvent::MetadataDelete,
                ProtocolEvent::ParentDirectorySync,
            ],
            "delete must remove and key-dir-sync the sidecar before committing the key-dir unlink in data_dir"
        );

        let failed_create_key = uuid::Uuid::new_v4().to_string();
        FAIL_NEXT_PARENT_SYNC.store(true, std::sync::atomic::Ordering::SeqCst);
        let error = manager
            .create_spool(failed_create_key.clone(), None, None, false, HashMap::new())
            .await
            .expect_err("parent fsync failure must not acknowledge create");
        assert!(matches!(error, BobsError::IoError(_)));
        let failed_create_events = take_protocol_events();
        assert!(failed_create_events.contains(&ProtocolEvent::MetadataWrite(SpoolState::Creating)));
        assert!(
            !failed_create_events.contains(&ProtocolEvent::MetadataWrite(SpoolState::Writing)),
            "a failed parent fsync must not publish live metadata"
        );
        assert!(failed_create_events.contains(&ProtocolEvent::MetadataDelete));
        assert!(manager.get_spool(&failed_create_key).is_none());
        assert!(!data_dir.join(&failed_create_key).exists());
        assert_eq!(manager.admission.available_permits(), 1);

        let failed_live_key = uuid::Uuid::new_v4().to_string();
        FAIL_NEXT_LIVE_METADATA_WRITE.store(true, std::sync::atomic::Ordering::SeqCst);
        let error = manager
            .create_spool(failed_live_key.clone(), None, None, false, HashMap::new())
            .await
            .expect_err("final metadata failure must not acknowledge create");
        assert!(matches!(error, BobsError::StorageError(_)));
        let failed_live_events = take_protocol_events();
        assert_eq!(
            &failed_live_events[..5],
            &[
                ProtocolEvent::DataCreate,
                ProtocolEvent::DataSync,
                ProtocolEvent::SpoolDirectorySync,
                ProtocolEvent::MetadataWrite(SpoolState::Creating),
                ProtocolEvent::ParentDirectorySync,
            ],
            "the crash-safe Creating sidecar and parent link precede the live commit attempt"
        );
        assert!(failed_live_events.contains(&ProtocolEvent::MetadataDelete));
        assert!(manager.get_spool(&failed_live_key).is_none());
        assert!(!data_dir.join(&failed_live_key).exists());
        assert_eq!(manager.admission.available_permits(), 1);

        let retry_key = uuid::Uuid::new_v4().to_string();
        manager
            .create_spool(retry_key.clone(), None, None, false, HashMap::new())
            .await
            .expect("create retry fixture");
        take_protocol_events();
        manager
            .page_cache
            .lock()
            .await
            .insert(&retry_key, 0, Bytes::from_static(b"data"));
        assert!(manager.page_cache.lock().await.contains(&retry_key, 0));

        FAIL_NEXT_PARENT_SYNC.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(
            manager.delete_spool(&retry_key).await.is_err(),
            "parent fsync failure must not acknowledge deletion"
        );
        assert!(!data_dir.join(&retry_key).exists());
        let deleting = manager
            .get_spool(&retry_key)
            .expect("failed deletion remains tracked");
        assert_eq!(deleting.metadata.lock().await.state, SpoolState::Deleting);
        assert_eq!(manager.admission.available_permits(), 0);
        assert!(manager.page_cache.lock().await.contains(&retry_key, 0));
        assert_eq!(
            take_protocol_events(),
            vec![
                ProtocolEvent::MetadataDelete,
                ProtocolEvent::ParentDirectorySync,
            ]
        );

        manager
            .delete_spool(&retry_key)
            .await
            .expect("retry syncs parent even though key directory is already absent");
        assert_eq!(
            take_protocol_events(),
            vec![
                ProtocolEvent::MetadataDelete,
                ProtocolEvent::ParentDirectorySync,
            ]
        );
        assert!(manager.get_spool(&retry_key).is_none());
        assert!(!manager.page_cache.lock().await.contains(&retry_key, 0));
        assert_eq!(manager.admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn test_delete() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");

        let manager = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
            .expect("manager init");

        let key = uuid::Uuid::new_v4().to_string();
        manager
            .create_spool(key.clone(), None, None, false, HashMap::new())
            .await
            .expect("create spool");
        let spool_dir = data_dir.join(&key);
        assert!(spool_dir.exists());

        manager.delete_spool(&key).await.expect("delete spool");
        assert!(manager.get_spool(&key).is_none());
        assert!(!spool_dir.exists());
    }

    #[tokio::test]
    async fn test_delete_serializes_with_in_flight_and_queued_writes() {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        BLOCKING_WRITE_CONTROL
            .set((Arc::clone(&entered), Arc::clone(&release)))
            .expect("blocking write control set once");

        let dir = tempdir().expect("create tempdir");
        let manager = Arc::new(
            SpoolManager::<BlockingWriteFileIO>::new(dir.path(), 4096, 16 * 4096, 256)
                .expect("manager init"),
        );
        let key = uuid::Uuid::new_v4().to_string();
        manager
            .create_spool(key.clone(), None, None, false, HashMap::new())
            .await
            .expect("create spool");
        let spool = manager.get_spool(&key).expect("spool exists");

        let first_spool = Arc::clone(&spool);
        let first_write = tokio::spawn(async move {
            first_spool
                .write(0, bytes::Bytes::from(vec![1; 4096]))
                .await
        });
        entered.notified().await;

        let delete_manager = Arc::clone(&manager);
        let delete_key = key.clone();
        let delete_task =
            tokio::spawn(async move { delete_manager.delete_spool(&delete_key).await });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(
            !delete_task.is_finished(),
            "deletion must wait for the earlier write to finish publication"
        );

        let queued_spool = Arc::clone(&spool);
        let queued_write = tokio::spawn(async move {
            queued_spool
                .write(4096, bytes::Bytes::from(vec![2; 4096]))
                .await
        });
        release.notify_one();

        first_write
            .await
            .expect("first write task join")
            .expect("write linearized before deletion succeeds");
        delete_task
            .await
            .expect("delete task join")
            .expect("delete succeeds");
        assert!(matches!(
            queued_write.await.expect("queued write task join"),
            Err(BobsError::InvalidState { .. })
        ));
    }

    #[tokio::test]
    async fn test_delete_nonexistent_key() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");

        let manager = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
            .expect("manager init");

        let result = manager.delete_spool("nonexistent-key").await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            BobsError::SpoolNotFound { .. }
        ));
    }

    #[tokio::test]
    async fn test_recovery() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");

        let key = {
            let manager1 = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
                .expect("manager1 init");

            let key = uuid::Uuid::new_v4().to_string();
            manager1
                .create_spool(key.clone(), None, None, true, HashMap::new())
                .await
                .expect("create spool");
            key
        };

        let manager2 = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
            .expect("manager2 init");

        manager2.recover().await.expect("recover should succeed");
        let spool = manager2.get_spool(&key).expect("recovered spool exists");
        let meta = spool.metadata.lock().await;
        assert_eq!(meta.key, key);
        assert_eq!(meta.state, SpoolState::WriteLocked);
    }

    #[tokio::test]
    async fn recovery_admission_bounds_partial_tail_reads_and_preserves_excess() {
        let _recovery_io_guard = RECOVERY_IO_TEST_LOCK.lock().await;
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let page_size = 4096_usize;
        let partial_len = 3073_usize;
        let keys: Vec<String> = (0..64)
            .map(|index| format!("00000000-0000-4000-8000-{index:012}"))
            .collect();
        let fixture_manager =
            SpoolManager::<TokioFileIO>::new(&data_dir, page_size, 16 * page_size, keys.len())
                .expect("fixture manager init");
        let mut durable_sidecars = HashMap::new();

        for (index, key) in keys.iter().enumerate() {
            let mut metadata =
                sidecar_fixture_metadata(&data_dir, key, SpoolState::Writing, partial_len as u64);
            metadata.last_write_at = if index == 3 || index == 7 {
                1_000
            } else {
                100 + index as u64
            };
            write_sidecar_fixture(&fixture_manager, metadata, &vec![index as u8; partial_len])
                .await;
            durable_sidecars.insert(
                key.clone(),
                tokio::fs::read(data_dir.join(key).join("meta.json"))
                    .await
                    .expect("snapshot durable sidecar"),
            );
        }

        let corrupt_key = "ffffffff-ffff-4fff-8fff-ffffffffffff";
        let corrupt_dir = data_dir.join(corrupt_key);
        tokio::fs::create_dir_all(&corrupt_dir)
            .await
            .expect("create corrupt spool directory");
        tokio::fs::write(corrupt_dir.join("meta.json"), b"{not json")
            .await
            .expect("write corrupt sidecar");
        tokio::fs::write(corrupt_dir.join("spool.dat"), b"bad")
            .await
            .expect("write corrupt data");
        drop(fixture_manager);

        RECOVERY_OPEN_COUNT.store(0, std::sync::atomic::Ordering::SeqCst);
        RECOVERY_READ_BYTES.store(0, std::sync::atomic::Ordering::SeqCst);
        let limited =
            SpoolManager::<RecoveryCountingFileIO>::new(&data_dir, page_size, 16 * page_size, 3)
                .expect("limited manager init");
        limited.recover().await.expect("limited recovery succeeds");

        let expected_admitted = [&keys[3], &keys[7], &keys[63]];
        let mut actual_admitted = limited.spool_keys();
        actual_admitted.sort();
        let mut expected_admitted_sorted: Vec<String> =
            expected_admitted.into_iter().cloned().collect();
        expected_admitted_sorted.sort();
        assert_eq!(actual_admitted, expected_admitted_sorted);
        assert_eq!(
            RECOVERY_OPEN_COUNT.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "only admitted spool data files may be opened"
        );
        assert_eq!(
            RECOVERY_READ_BYTES.load(std::sync::atomic::Ordering::SeqCst),
            3 * partial_len,
            "only admitted trailing partial pages may be loaded"
        );
        assert_eq!(limited.admission.available_permits(), 0);
        assert!(
            !corrupt_dir.exists(),
            "a malformed sidecar remains a per-key deletion and must not affect valid candidates"
        );

        for key in &keys {
            assert!(data_dir.join(key).join("spool.dat").exists());
            if limited.get_spool(key).is_none() {
                assert_eq!(
                    tokio::fs::read(data_dir.join(key).join("meta.json"))
                        .await
                        .expect("read quarantined sidecar"),
                    durable_sidecars[key],
                    "excess durable metadata must remain untouched"
                );
            }
        }
        drop(limited);

        RECOVERY_OPEN_COUNT.store(0, std::sync::atomic::Ordering::SeqCst);
        RECOVERY_READ_BYTES.store(0, std::sync::atomic::Ordering::SeqCst);
        let expanded = SpoolManager::<RecoveryCountingFileIO>::new(
            &data_dir,
            page_size,
            16 * page_size,
            keys.len(),
        )
        .expect("expanded manager init");
        expanded
            .recover()
            .await
            .expect("expanded recovery succeeds");
        assert_eq!(expanded.spools.len(), keys.len());
        assert!(keys.iter().all(|key| expanded.get_spool(key).is_some()));
        assert_eq!(
            RECOVERY_OPEN_COUNT.load(std::sync::atomic::Ordering::SeqCst),
            keys.len()
        );
        assert_eq!(
            RECOVERY_READ_BYTES.load(std::sync::atomic::Ordering::SeqCst),
            keys.len() * partial_len
        );
        assert_eq!(expanded.admission.available_permits(), 0);
    }

    #[tokio::test]
    async fn recovery_skips_newer_metadata_invalid_candidates_before_complete_spool() {
        let _recovery_io_guard = RECOVERY_IO_TEST_LOCK.lock().await;
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let page_size = 4096_usize;
        let creating_key = "10000000-0000-4000-8000-000000000001";
        let deleting_key = "10000000-0000-4000-8000-000000000002";
        let corrupt_key = "10000000-0000-4000-8000-000000000003";
        let ambiguous_key = "10000000-0000-4000-8000-000000000004";
        let complete_key = "10000000-0000-4000-8000-000000000005";
        let fixture = SpoolManager::<TokioFileIO>::new(&data_dir, page_size, 65536, 8)
            .expect("fixture manager");

        let mut creating =
            sidecar_fixture_metadata(&data_dir, creating_key, SpoolState::Creating, 0);
        creating.last_write_at = 1_000;
        write_sidecar_fixture(&fixture, creating, b"").await;

        let mut deleting =
            sidecar_fixture_metadata(&data_dir, deleting_key, SpoolState::Deleting, 0);
        deleting.last_write_at = 900;
        write_sidecar_fixture(&fixture, deleting, b"").await;

        let mut corrupt = sidecar_fixture_metadata(&data_dir, corrupt_key, SpoolState::Complete, 8);
        corrupt.last_write_at = 800;
        write_sidecar_fixture(&fixture, corrupt, b"bad").await;

        let mut ambiguous =
            sidecar_fixture_metadata(&data_dir, ambiguous_key, SpoolState::Writing, 9);
        ambiguous.page_size = 0;
        ambiguous.total_pages = 2;
        ambiguous.final_page_size = None;
        ambiguous.last_write_at = 700;
        write_sidecar_fixture(&fixture, ambiguous, b"abcdefghi").await;
        let ambiguous_sidecar = tokio::fs::read(data_dir.join(ambiguous_key).join("meta.json"))
            .await
            .expect("snapshot ambiguous sidecar");

        let mut complete =
            sidecar_fixture_metadata(&data_dir, complete_key, SpoolState::Complete, 4);
        complete.last_write_at = 600;
        write_sidecar_fixture(&fixture, complete, b"good").await;
        drop(fixture);

        RECOVERY_OPEN_ATTEMPTS.store(0, std::sync::atomic::Ordering::SeqCst);
        RECOVERY_OPEN_COUNT.store(0, std::sync::atomic::Ordering::SeqCst);
        RECOVERY_READ_BYTES.store(0, std::sync::atomic::Ordering::SeqCst);
        let limited = SpoolManager::<RecoveryCountingFileIO>::new(&data_dir, page_size, 65536, 1)
            .expect("limited manager");
        let counts = limited
            .recover_with_counts()
            .await
            .expect("bounded recovery succeeds");

        assert_eq!(
            counts,
            (1, 0),
            "recovery metric source counts only successfully recovered and valid excess spools"
        );
        assert!(limited.get_spool(complete_key).is_some());
        assert_eq!(limited.spool_keys(), vec![complete_key.to_string()]);
        assert_eq!(
            RECOVERY_OPEN_ATTEMPTS.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "metadata-invalid candidates must not consume data admission"
        );
        assert_eq!(
            RECOVERY_READ_BYTES.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "complete recovery needs no tail read"
        );
        assert!(!data_dir.join(creating_key).exists());
        assert!(!data_dir.join(deleting_key).exists());
        assert!(!data_dir.join(corrupt_key).exists());
        assert_eq!(
            tokio::fs::read(data_dir.join(ambiguous_key).join("meta.json"))
                .await
                .expect("ambiguous sidecar remains"),
            ambiguous_sidecar,
            "rejected ambiguous metadata is preserved but is not capacity quarantine"
        );
    }

    #[tokio::test]
    async fn recovery_releases_failed_permit_and_does_not_read_valid_excess_tail() {
        let _recovery_io_guard = RECOVERY_IO_TEST_LOCK.lock().await;
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let page_size = 4096_usize;
        let active_key = "ffffffff-ffff-4fff-8fff-ffffffffff06";
        let excess_key = "ffffffff-ffff-4fff-8fff-ffffffffff07";
        let fixture = SpoolManager::<TokioFileIO>::new(&data_dir, page_size, 65536, 3)
            .expect("fixture manager");

        let mut failed =
            sidecar_fixture_metadata(&data_dir, RECOVERY_FAIL_OPEN_KEY, SpoolState::Writing, 17);
        failed.last_write_at = 1_000;
        write_sidecar_fixture(&fixture, failed, b"newer-but-unopenable").await;

        let active_data = b"older-active";
        let mut active = sidecar_fixture_metadata(
            &data_dir,
            active_key,
            SpoolState::Writing,
            active_data.len() as u64,
        );
        active.last_write_at = 900;
        write_sidecar_fixture(&fixture, active, active_data).await;

        let excess_data = b"valid-excess-tail";
        let mut excess = sidecar_fixture_metadata(
            &data_dir,
            excess_key,
            SpoolState::Writing,
            excess_data.len() as u64,
        );
        excess.last_write_at = 800;
        write_sidecar_fixture(&fixture, excess, excess_data).await;
        let excess_sidecar = tokio::fs::read(data_dir.join(excess_key).join("meta.json"))
            .await
            .expect("snapshot excess sidecar");
        drop(fixture);

        RECOVERY_OPEN_ATTEMPTS.store(0, std::sync::atomic::Ordering::SeqCst);
        RECOVERY_OPEN_COUNT.store(0, std::sync::atomic::Ordering::SeqCst);
        RECOVERY_READ_BYTES.store(0, std::sync::atomic::Ordering::SeqCst);
        let limited = SpoolManager::<RecoveryCountingFileIO>::new(&data_dir, page_size, 65536, 1)
            .expect("limited manager");
        let counts = limited
            .recover_with_counts()
            .await
            .expect("bounded recovery succeeds");

        assert_eq!(
            counts,
            (1, 1),
            "only the metadata-valid excess spool contributes to quarantine metrics"
        );
        assert!(limited.get_spool(active_key).is_some());
        assert!(limited.get_spool(RECOVERY_FAIL_OPEN_KEY).is_none());
        assert!(limited.get_spool(excess_key).is_none());
        assert_eq!(
            RECOVERY_OPEN_ATTEMPTS.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "failed candidate releases its permit so the older candidate is attempted"
        );
        assert_eq!(
            RECOVERY_OPEN_COUNT.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            RECOVERY_READ_BYTES.load(std::sync::atomic::Ordering::SeqCst),
            active_data.len(),
            "the valid excess tail must remain unopened and unread"
        );
        assert_eq!(limited.admission.available_permits(), 0);
        assert!(data_dir.join(RECOVERY_FAIL_OPEN_KEY).exists());
        assert_eq!(
            tokio::fs::read(data_dir.join(excess_key).join("meta.json"))
                .await
                .expect("excess sidecar remains"),
            excess_sidecar,
            "valid excess metadata remains untouched"
        );
    }

    #[tokio::test]
    async fn test_write_ack_leaves_sidecar_offsets_stale_until_complete() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");

        let manager = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
            .expect("manager init");

        let key = uuid::Uuid::new_v4().to_string();
        manager
            .create_spool(key.clone(), None, None, false, HashMap::new())
            .await
            .expect("create spool");

        let spool = manager.get_spool(&key).expect("spool exists");
        let data = vec![0x5Au8; 8192 + 123];
        spool
            .write(0, bytes::Bytes::copy_from_slice(&data))
            .await
            .expect("write succeeds");

        let in_memory = spool.metadata.lock().await.clone();
        assert_eq!(in_memory.total_bytes_written, data.len() as u64);
        assert_eq!(in_memory.total_pages, 2);

        let persisted = persisted_metadata(&manager, &key).await;
        assert_eq!(persisted.total_bytes_written, 0);
        assert_eq!(persisted.total_pages, 0);
        assert_eq!(persisted.final_page_size, None);
        assert_eq!(persisted.state, SpoolState::Writing);

        spool.complete(None).await.expect("complete succeeds");

        let persisted = persisted_metadata(&manager, &key).await;
        assert_eq!(persisted.total_bytes_written, data.len() as u64);
        assert_eq!(persisted.total_pages, 3);
        assert_eq!(persisted.final_page_size, Some(123));
        assert_eq!(persisted.state, SpoolState::Complete);
    }

    #[tokio::test]
    async fn test_ack_then_restart_recovers_consistent_in_progress_state_without_sidecar_update() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let page_size = 4096usize;
        let data = vec![0xA5u8; page_size * 2 + 777];

        let key = {
            let manager = SpoolManager::<TokioFileIO>::new(&data_dir, page_size, 16 * 4096, 256)
                .expect("manager init");
            let key = uuid::Uuid::new_v4().to_string();
            manager
                .create_spool(key.clone(), None, None, false, HashMap::new())
                .await
                .expect("create spool");
            let spool = manager.get_spool(&key).expect("spool exists");
            spool
                .write(0, bytes::Bytes::copy_from_slice(&data))
                .await
                .expect("write ack succeeds");

            let persisted = persisted_metadata(&manager, &key).await;
            assert_eq!(persisted.total_bytes_written, 0);
            assert_eq!(persisted.total_pages, 0);
            key
        };

        let manager2 = SpoolManager::<TokioFileIO>::new(&data_dir, page_size, 16 * 4096, 256)
            .expect("manager2 init");
        manager2.recover().await.expect("recover succeeds");

        let spool = manager2.get_spool(&key).expect("recovered spool exists");
        let meta = spool.metadata.lock().await.clone();
        assert_eq!(meta.state, SpoolState::Writing);
        assert_eq!(meta.total_bytes_written, data.len() as u64);
        assert_eq!(meta.total_pages, 2);
        assert_eq!(meta.final_page_size, None);

        let trailing = spool.write_buffer.lock().await.clone();
        assert_eq!(trailing.as_ref(), &data[page_size * 2..]);
        assert_eq!(
            spool
                .read_page_for_test(0)
                .await
                .expect("read page 0")
                .unwrap()
                .as_ref(),
            &data[..page_size]
        );
        assert_eq!(
            spool
                .read_page_for_test(1)
                .await
                .expect("read page 1")
                .unwrap()
                .as_ref(),
            &data[page_size..page_size * 2]
        );

        let persisted = persisted_metadata(&manager2, &key).await;
        assert_eq!(persisted.total_bytes_written, 0);
        assert_eq!(persisted.total_pages, 0);
        assert_eq!(persisted.final_page_size, None);
    }

    #[tokio::test]
    async fn test_complete_durability_survives_restart_with_final_metadata_and_pages() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let page_size = 4096usize;
        let data = vec![0x3Cu8; page_size * 2 + 19];

        let key = {
            let manager = SpoolManager::<TokioFileIO>::new(&data_dir, page_size, 16 * 4096, 256)
                .expect("manager init");
            let key = uuid::Uuid::new_v4().to_string();
            manager
                .create_spool(key.clone(), None, None, false, HashMap::new())
                .await
                .expect("create spool");
            let spool = manager.get_spool(&key).expect("spool exists");
            spool
                .write(0, bytes::Bytes::copy_from_slice(&data))
                .await
                .expect("write succeeds");
            spool
                .complete(Some(data.len() as u64))
                .await
                .expect("complete succeeds");

            let persisted = persisted_metadata(&manager, &key).await;
            assert_eq!(persisted.state, SpoolState::Complete);
            assert_eq!(persisted.total_bytes_written, data.len() as u64);
            assert_eq!(persisted.total_pages, 3);
            assert_eq!(persisted.final_page_size, Some(19));
            key
        };

        let manager2 = SpoolManager::<TokioFileIO>::new(&data_dir, page_size, 16 * 4096, 256)
            .expect("manager2 init");
        manager2.recover().await.expect("recover succeeds");
        let spool = manager2.get_spool(&key).expect("recovered spool exists");

        let meta = spool.metadata.lock().await.clone();
        assert_eq!(meta.state, SpoolState::Complete);
        assert_eq!(meta.total_bytes_written, data.len() as u64);
        assert_eq!(meta.total_pages, 3);
        assert_eq!(meta.final_page_size, Some(19));

        assert_eq!(
            spool
                .read_page_for_test(0)
                .await
                .expect("page 0")
                .unwrap()
                .as_ref(),
            &data[..page_size]
        );
        assert_eq!(
            spool
                .read_page_for_test(1)
                .await
                .expect("page 1")
                .unwrap()
                .as_ref(),
            &data[page_size..page_size * 2]
        );
        assert_eq!(
            spool
                .read_page_for_test(2)
                .await
                .expect("page 2")
                .unwrap()
                .as_ref(),
            &data[page_size * 2..]
        );
        assert!(spool
            .read_page_for_test(3)
            .await
            .expect("end of spool")
            .is_none());
    }

    #[tokio::test]
    async fn crash_restart_at_completing_marker_finalizes_exact_candidate() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let manager =
            SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 16).expect("manager init");
        let key = uuid::Uuid::new_v4().to_string();
        let data = vec![0xD4u8; 4096 + 37];
        let marker =
            sidecar_fixture_metadata(&data_dir, &key, SpoolState::Completing, data.len() as u64);
        write_sidecar_fixture(&manager, marker, &data).await;

        manager.recover().await.expect("recover marker");
        let spool = manager
            .get_spool(&key)
            .expect("valid marker must finalize and recover");
        let durable = persisted_metadata(&manager, &key).await;
        assert_eq!(durable.state, SpoolState::Complete);
        assert_eq!(durable.total_bytes_written, data.len() as u64);
        assert_eq!(durable.total_pages, 2);
        assert_eq!(durable.final_page_size, Some(37));
        assert_eq!(
            spool
                .read_page_for_test(0)
                .await
                .expect("page 0")
                .unwrap()
                .as_ref(),
            &data[..4096]
        );
        assert_eq!(
            spool
                .read_page_for_test(1)
                .await
                .expect("page 1")
                .unwrap()
                .as_ref(),
            &data[4096..]
        );
        assert!(spool
            .read_page_for_test(2)
            .await
            .expect("exact EOF")
            .is_none());
        assert!(matches!(
            spool
                .write(data.len() as u64, Bytes::from_static(b"tail"))
                .await,
            Err(BobsError::SpoolClosed)
        ));
        spool
            .complete(Some(data.len() as u64))
            .await
            .expect("idempotent completion after recovery");
    }

    #[tokio::test]
    async fn inconsistent_completing_markers_are_quarantined_without_mutation() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let manager =
            SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 16).expect("manager init");

        let short_key = uuid::Uuid::new_v4().to_string();
        let short_data = vec![0x51u8; 200];
        let short_marker =
            sidecar_fixture_metadata(&data_dir, &short_key, SpoolState::Completing, 300);
        write_sidecar_fixture(&manager, short_marker, &short_data).await;

        let extra_key = uuid::Uuid::new_v4().to_string();
        let extra_data = vec![0xA7u8; 301];
        let extra_marker =
            sidecar_fixture_metadata(&data_dir, &extra_key, SpoolState::Completing, 300);
        write_sidecar_fixture(&manager, extra_marker, &extra_data).await;

        manager
            .recover()
            .await
            .expect("quarantine is a successful recovery outcome");

        for (key, original) in [(&short_key, &short_data), (&extra_key, &extra_data)] {
            assert!(
                manager.get_spool(key).is_none(),
                "quarantined marker must not expose a writable spool"
            );
            let durable = persisted_metadata(&manager, key).await;
            assert_eq!(durable.state, SpoolState::Completing);
            assert_eq!(
                tokio::fs::read(data_dir.join(key).join("spool.dat"))
                    .await
                    .expect("quarantined data remains"),
                *original
            );
        }
    }

    #[tokio::test]
    async fn test_recovery_preserves_writing_state() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");

        let key = {
            let manager = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
                .expect("manager init");

            let key = uuid::Uuid::new_v4().to_string();
            manager
                .create_spool(key.clone(), None, None, false, HashMap::new())
                .await
                .expect("create spool");

            let spool = manager.get_spool(&key).expect("spool exists");
            spool
                .write(0, bytes::Bytes::from(vec![0x22u8; 4096]))
                .await
                .expect("write succeeds");

            key
        };

        let manager2 = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
            .expect("manager2 init");
        manager2.recover().await.expect("recover succeeds");

        let spool = manager2.get_spool(&key).expect("recovered spool exists");
        let meta = spool.metadata.lock().await;
        assert_eq!(meta.state, SpoolState::Writing);
    }

    // -----------------------------------------------------------------------
    // New tests — recovery seeds in-memory fields
    // -----------------------------------------------------------------------

    /// Recovery reseeds monotonic cleanup anchors instead of deriving TTL age
    /// from persisted wall-clock timestamps.
    #[tokio::test]
    async fn test_recovery_reseeds_cleanup_anchors() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");

        let key = {
            let manager = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
                .expect("manager init");
            let key = uuid::Uuid::new_v4().to_string();
            manager
                .create_spool(key.clone(), None, None, true, HashMap::new())
                .await
                .expect("create spool");
            key
        };

        let before_recovery = Instant::now();
        let manager2 = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
            .expect("manager2 init");
        manager2.recover().await.expect("recover should succeed");

        let spool = manager2.get_spool(&key).expect("recovered spool exists");
        let anchors = spool.cleanup_anchors();
        assert!(anchors.last_write_at >= before_recovery);
        assert!(anchors.last_read_activity_at.is_none());
        assert!(anchors.full_object_read_at.is_none());
    }

    /// After recovering a Complete spool, monotonic cleanup anchors must grant a
    /// fresh idle-TTL grace period and `missing_ranges` must cover the full object
    /// until its bytes are re-served.
    #[tokio::test]
    async fn test_recovery_initializes_missing_ranges_for_complete_spool() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");

        let key = {
            let manager = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
                .expect("manager init");
            let key = uuid::Uuid::new_v4().to_string();
            manager
                .create_spool(key.clone(), None, None, false, HashMap::new())
                .await
                .expect("create spool");
            let spool = manager.get_spool(&key).expect("spool exists");
            spool
                .write(0, bytes::Bytes::from(vec![0xABu8; 8192]))
                .await
                .expect("write succeeds");
            spool.complete(None).await.expect("complete succeeds");
            key
        };

        let before_recovery = Instant::now();
        let manager2 = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
            .expect("manager2 init");
        manager2.recover().await.expect("recover should succeed");

        let spool = manager2.get_spool(&key).expect("recovered spool exists");
        let anchors = spool.cleanup_anchors();
        assert!(
            anchors
                .readable_at
                .is_some_and(|anchor| anchor >= before_recovery),
            "recovered complete spool must receive a fresh monotonic readable anchor"
        );
        assert!(anchors.last_read_activity_at.is_none());
        assert!(anchors.full_object_read_at.is_none());
        let mr = spool.missing_ranges.lock().await;
        assert_eq!(
            mr.total_size,
            Some(8192),
            "missing_ranges total_size must be Some(8192) after recovery"
        );
        assert_eq!(
            mr.gap_count(),
            1,
            "missing_ranges must have exactly one gap [0, 8192) after recovery (no bytes re-served yet)"
        );
    }

    /// When a spool's persisted metadata has `readable_at: null` (old format from
    /// before the field was introduced), recovery must backfill it to `Some(now)`
    /// and persist the corrected metadata so that the spool is not immediately
    /// deleted by the idle-TTL rule.

    #[tokio::test]
    async fn test_recovery_backfills_readable_at_in_sidecar() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let manager = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
            .expect("manager init");
        let key = uuid::Uuid::new_v4().to_string();
        let data = vec![0x44; 4096];
        let mut metadata =
            sidecar_fixture_metadata(&data_dir, &key, SpoolState::Complete, data.len() as u64);
        metadata.readable_at = None;
        write_sidecar_fixture(&manager, metadata, &data).await;

        let manager2 = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
            .expect("manager2 init");
        manager2.recover().await.expect("recover succeeds");

        let recovered = manager2.get_spool(&key).expect("spool recovered");
        assert!(recovered.metadata.lock().await.readable_at.is_some());
        assert!(persisted_metadata(&manager2, &key)
            .await
            .readable_at
            .is_some());
    }

    #[tokio::test]
    async fn test_recovery_removes_creating_deleting_and_orphan_sidecar_dirs() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let manager = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
            .expect("manager init");
        let creating = "creating-spool";
        let deleting = "deleting-spool";
        write_sidecar_fixture(
            &manager,
            sidecar_fixture_metadata(&data_dir, creating, SpoolState::Creating, 0),
            b"",
        )
        .await;
        write_sidecar_fixture(
            &manager,
            sidecar_fixture_metadata(&data_dir, deleting, SpoolState::Deleting, 0),
            b"",
        )
        .await;
        let orphan_key = uuid::Uuid::new_v4().to_string();
        let orphan_dir = data_dir.join(&orphan_key);
        tokio::fs::create_dir_all(&orphan_dir)
            .await
            .expect("create UUID orphan dir");
        tokio::fs::write(orphan_dir.join("spool.dat"), b"orphan")
            .await
            .expect("write orphan data");
        let non_uuid_orphan_dir = data_dir.join("orphan-no-sidecar");
        tokio::fs::create_dir_all(&non_uuid_orphan_dir)
            .await
            .expect("create non-UUID orphan dir");
        tokio::fs::write(non_uuid_orphan_dir.join("spool.dat"), b"not a spool key")
            .await
            .expect("write non-UUID orphan data");

        let manager2 = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
            .expect("manager2 init");
        manager2.recover().await.expect("recover succeeds");

        assert!(!data_dir.join(creating).exists());
        assert!(!data_dir.join(deleting).exists());
        assert!(!orphan_dir.exists());
        assert!(
            non_uuid_orphan_dir.exists(),
            "non-UUID orphan directories must be preserved"
        );
        assert!(manager2.spool_keys().is_empty());
    }

    #[tokio::test]
    async fn recovery_removes_empty_pre_marker_dir_and_preserves_nonempty_and_symlink_dirs() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        tokio::fs::create_dir_all(&data_dir)
            .await
            .expect("create data dir");

        let empty_uuid_key = uuid::Uuid::new_v4().to_string();
        let empty_uuid_dir = data_dir.join(&empty_uuid_key);
        tokio::fs::create_dir(&empty_uuid_dir)
            .await
            .expect("create empty UUID dir");

        let nonempty_uuid_key = uuid::Uuid::new_v4().to_string();
        let nonempty_uuid_dir = data_dir.join(&nonempty_uuid_key);
        tokio::fs::create_dir(&nonempty_uuid_dir)
            .await
            .expect("create nonempty UUID dir");
        tokio::fs::write(nonempty_uuid_dir.join("operator-data"), b"retain")
            .await
            .expect("write unknown data");

        #[cfg(unix)]
        let (symlink_path, target_dir) = {
            let symlink_key = uuid::Uuid::new_v4().to_string();
            let target_dir = dir.path().join("outside-target");
            tokio::fs::create_dir_all(&target_dir)
                .await
                .expect("create symlink target");
            tokio::fs::write(target_dir.join("spool.dat"), b"outside")
                .await
                .expect("write target marker");
            let symlink_path = data_dir.join(&symlink_key);
            std::os::unix::fs::symlink(&target_dir, &symlink_path).expect("create UUID symlink");
            (Some(symlink_path), Some(target_dir))
        };
        #[cfg(not(unix))]
        let (symlink_path, target_dir): (Option<PathBuf>, Option<PathBuf>) = (None, None);

        let manager = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
            .expect("manager init");
        manager.recover().await.expect("recover succeeds");

        assert!(
            !empty_uuid_dir.exists(),
            "empty pre-marker key directory must be removed on restart"
        );
        assert!(
            nonempty_uuid_dir.join("operator-data").exists(),
            "non-empty markerless key directory must be quarantined without mutation"
        );
        manager
            .create_spool(empty_uuid_key.clone(), None, None, false, HashMap::new())
            .await
            .expect("the original key can be created after recovery removes its empty directory");
        assert!(manager.get_spool(&empty_uuid_key).is_some());

        if let Some(symlink_path) = symlink_path {
            assert!(
                tokio::fs::symlink_metadata(&symlink_path)
                    .await
                    .expect("symlink metadata")
                    .file_type()
                    .is_symlink(),
                "UUID symlink must be preserved"
            );
        }
        if let Some(target_dir) = target_dir {
            assert!(
                target_dir.join("spool.dat").exists(),
                "orphan sweep must not traverse a skipped symlink target"
            );
        }
    }

    #[tokio::test]
    async fn empty_pre_marker_parent_sync_failure_is_retryable() {
        let _protocol_guard = PROTOCOL_TEST_LOCK.lock().await;
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        tokio::fs::create_dir_all(&data_dir)
            .await
            .expect("create data dir");
        let key = uuid::Uuid::new_v4().to_string();
        tokio::fs::create_dir(data_dir.join(&key))
            .await
            .expect("create empty pre-marker key dir");
        let manager = SpoolManager::<ProtocolFileIO, ProtocolMetadataStore>::with_metadata_store(
            ProtocolMetadataStore::new(&data_dir),
            &data_dir,
            4096,
            16 * 4096,
            256,
        )
        .expect("manager init");

        take_protocol_events();
        FAIL_NEXT_PARENT_SYNC.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(matches!(
            manager.recover().await,
            Err(BobsError::IoError(_))
        ));
        assert!(
            !data_dir.join(&key).exists(),
            "the atomic empty-dir removal may precede an uncertain parent sync"
        );
        assert_eq!(
            take_protocol_events(),
            vec![ProtocolEvent::ParentDirectorySync]
        );

        manager
            .recover()
            .await
            .expect("retry re-syncs the parent even though the empty directory is gone");
        assert_eq!(
            take_protocol_events(),
            vec![ProtocolEvent::ParentDirectorySync]
        );
        manager
            .create_spool(key.clone(), None, None, false, HashMap::new())
            .await
            .expect("create succeeds after the recovery durability retry");
        assert!(manager.get_spool(&key).is_some());
    }

    #[tokio::test]
    async fn test_recovery_discards_complete_sidecar_when_data_file_too_short() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let manager = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
            .expect("manager init");
        let key = uuid::Uuid::new_v4().to_string();
        write_sidecar_fixture(
            &manager,
            sidecar_fixture_metadata(&data_dir, &key, SpoolState::Complete, 8192),
            &[0x55; 4096],
        )
        .await;

        let manager2 = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
            .expect("manager2 init");
        manager2.recover().await.expect("recover succeeds");

        assert!(manager2.get_spool(&key).is_none());
        assert!(!data_dir.join(&key).exists());
        assert!(manager2
            .metadata_store
            .read(&key)
            .await
            .expect("read sidecar")
            .is_none());
    }

    #[tokio::test]
    async fn test_recovery_discards_missing_data_file_sidecar() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let manager = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
            .expect("manager init");
        let key = uuid::Uuid::new_v4().to_string();
        let metadata = sidecar_fixture_metadata(&data_dir, &key, SpoolState::Complete, 4096);
        tokio::fs::create_dir_all(data_dir.join(&key))
            .await
            .expect("create spool dir");
        manager
            .metadata_store
            .write(&metadata)
            .await
            .expect("write sidecar");

        let manager2 = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
            .expect("manager2 init");
        manager2.recover().await.expect("recover succeeds");

        assert!(manager2.get_spool(&key).is_none());
        assert!(!data_dir.join(&key).exists());
    }

    #[tokio::test]
    async fn test_recovery_discards_corrupt_metadata_sidecar() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let key = uuid::Uuid::new_v4().to_string();
        let spool_dir = data_dir.join(&key);
        tokio::fs::create_dir_all(&spool_dir)
            .await
            .expect("create spool dir");
        tokio::fs::write(spool_dir.join("meta.json"), b"{not valid json")
            .await
            .expect("write corrupt sidecar");
        tokio::fs::write(spool_dir.join("spool.dat"), b"data")
            .await
            .expect("write data");

        let manager = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
            .expect("manager init");
        manager.recover().await.expect("recover succeeds");

        assert!(manager.get_spool(&key).is_none());
        assert!(!spool_dir.exists());
    }

    #[tokio::test]
    async fn test_recovery_does_not_prepopulate_page_cache() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let page_size = 4096usize;
        let data = vec![0x7Bu8; page_size * 2];

        let key = {
            let manager =
                SpoolManager::<TokioFileIO>::new(&data_dir, page_size, 16 * page_size, 256)
                    .expect("manager init");
            let key = uuid::Uuid::new_v4().to_string();
            manager
                .create_spool(key.clone(), None, None, false, HashMap::new())
                .await
                .expect("create spool");
            let spool = manager.get_spool(&key).expect("spool exists");
            spool
                .write(0, bytes::Bytes::copy_from_slice(&data))
                .await
                .expect("write succeeds");
            spool.complete(None).await.expect("complete succeeds");
            key
        };

        let manager2 = SpoolManager::<TokioFileIO>::new(&data_dir, page_size, 16 * page_size, 256)
            .expect("manager2 init");
        manager2.recover().await.expect("recover succeeds");

        let spool = manager2.get_spool(&key).expect("recovered spool exists");
        assert!(
            Arc::ptr_eq(&manager2.page_cache, &spool.page_cache),
            "recovered spool must use the manager's shared cache"
        );
        assert_eq!(
            manager2.page_cache.lock().await.len(),
            0,
            "recovery must not seed cache with pages reconstructed/read from spool.dat"
        );
    }

    #[tokio::test]
    async fn test_delete_removes_cache_entries_when_directory_already_missing() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let manager =
            SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 8192, 256).expect("manager init");

        let key = uuid::Uuid::new_v4().to_string();
        manager
            .create_spool(key.clone(), None, None, false, HashMap::new())
            .await
            .expect("create spool");
        {
            let mut cache = manager.page_cache.lock().await;
            cache.insert(&key, 0, bytes::Bytes::from_static(b"cached"));
            assert!(cache.contains(&key, 0));
        }

        let spool_dir = data_dir.join(&key);
        tokio::fs::remove_dir_all(&spool_dir)
            .await
            .expect("remove spool dir before delete");

        manager
            .delete_spool(&key)
            .await
            .expect("delete succeeds despite missing directory");

        assert!(manager.get_spool(&key).is_none());
        assert!(!manager.page_cache.lock().await.contains(&key, 0));
    }

    #[tokio::test]
    async fn test_recovery_uses_persisted_page_size_after_config_change() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let key = uuid::Uuid::new_v4().to_string();
        let data = bytes::Bytes::from_static(b"abcdef");

        {
            let manager =
                SpoolManager::<TokioFileIO>::new(&data_dir, 4, 64, 256).expect("manager init");
            manager
                .create_spool(key.clone(), None, None, false, HashMap::new())
                .await
                .expect("create spool");
            let spool = manager.get_spool(&key).expect("spool exists");
            spool.write(0, data.clone()).await.expect("write data");
            spool.complete(Some(6)).await.expect("complete spool");
            assert_eq!(persisted_metadata(&manager, &key).await.page_size, 4);
        }

        let manager = SpoolManager::<TokioFileIO>::new(&data_dir, 8, 64, 256)
            .expect("manager after config change");
        manager
            .recover()
            .await
            .expect("recover with persisted page size");
        let spool = manager.get_spool(&key).expect("recovered spool");
        assert_eq!(spool.page_size, 4);
        assert_eq!(
            spool.read_page_for_test(0).await.unwrap().unwrap().as_ref(),
            b"abcd"
        );
        assert_eq!(
            spool.read_page_for_test(1).await.unwrap().unwrap().as_ref(),
            b"ef"
        );
    }

    #[tokio::test]
    async fn test_recovery_migrates_old_main_complete_multi_page_after_config_change() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let key = uuid::Uuid::new_v4().to_string();
        let data = b"abcdefghij";
        write_old_main_sidecar_fixture(&data_dir, &key, "Complete", data, 3, Some(2)).await;

        let manager = SpoolManager::<TokioFileIO>::new(&data_dir, 8192, 65536, 256)
            .expect("restart manager with changed config");
        manager.recover().await.expect("migrate complete spool");
        let spool = manager.get_spool(&key).expect("spool recovered");
        assert_eq!(spool.page_size, 4);
        assert_eq!(
            spool.read_page_for_test(0).await.unwrap().unwrap().as_ref(),
            b"abcd"
        );
        assert_eq!(
            spool.read_page_for_test(1).await.unwrap().unwrap().as_ref(),
            b"efgh"
        );
        assert_eq!(
            spool.read_page_for_test(2).await.unwrap().unwrap().as_ref(),
            b"ij"
        );
        assert!(data_dir.join(&key).join("spool.dat").exists());
        assert_eq!(persisted_metadata(&manager, &key).await.page_size, 4);
    }

    #[tokio::test]
    async fn test_recovery_migrates_old_main_one_page_and_empty_complete_spools() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let one_page_key = uuid::Uuid::new_v4().to_string();
        let empty_key = uuid::Uuid::new_v4().to_string();
        write_old_main_sidecar_fixture(&data_dir, &one_page_key, "Complete", b"abc", 1, Some(3))
            .await;
        write_old_main_sidecar_fixture(&data_dir, &empty_key, "Complete", b"", 0, None).await;

        let manager =
            SpoolManager::<TokioFileIO>::new(&data_dir, 8192, 65536, 256).expect("restart manager");
        manager.recover().await.expect("migrate terminal spools");

        let one_page = manager.get_spool(&one_page_key).expect("one-page spool");
        assert_eq!(one_page.page_size, 3);
        assert_eq!(
            one_page
                .read_page_for_test(0)
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            b"abc"
        );
        let empty = manager.get_spool(&empty_key).expect("empty spool");
        assert_eq!(empty.page_size, 1);
        assert!(empty.read_page_for_test(0).await.unwrap().is_none());
        assert!(data_dir.join(&one_page_key).join("spool.dat").exists());
        assert!(data_dir.join(&empty_key).join("spool.dat").exists());
    }

    #[tokio::test]
    async fn test_recovery_migrates_old_main_in_progress_spool_without_config_stride() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let paged_key = uuid::Uuid::new_v4().to_string();
        let zero_page_key = uuid::Uuid::new_v4().to_string();
        write_old_main_sidecar_fixture(&data_dir, &paged_key, "Writing", b"abcdefgh", 2, None)
            .await;
        write_old_main_sidecar_fixture(&data_dir, &zero_page_key, "Writing", b"abc", 0, None).await;

        let manager =
            SpoolManager::<TokioFileIO>::new(&data_dir, 8192, 65536, 256).expect("restart manager");
        manager.recover().await.expect("migrate active spools");

        let paged = manager.get_spool(&paged_key).expect("paged active spool");
        assert_eq!(paged.page_size, 4);
        assert_eq!(paged.metadata.lock().await.state, SpoolState::Writing);
        assert_eq!(
            paged.read_page_for_test(0).await.unwrap().unwrap().as_ref(),
            b"abcd"
        );
        assert_eq!(
            paged.read_page_for_test(1).await.unwrap().unwrap().as_ref(),
            b"efgh"
        );
        assert_eq!(persisted_metadata(&manager, &paged_key).await.page_size, 4);

        let zero_page = manager
            .get_spool(&zero_page_key)
            .expect("zero-page active spool");
        assert_eq!(zero_page.page_size, 4);
        assert_eq!(zero_page.write_buffer.lock().await.as_ref(), b"abc");
        zero_page
            .write(3, bytes::Bytes::from_static(b"d"))
            .await
            .expect("resume write at durable offset");
        assert_eq!(
            zero_page
                .read_page_for_test(0)
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            b"abcd"
        );
        assert!(data_dir.join(&paged_key).join("spool.dat").exists());
        assert!(data_dir.join(&zero_page_key).join("spool.dat").exists());
    }

    #[tokio::test]
    async fn test_recovery_resegments_legacy_write_locked_for_terminal_salvage() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let partial_key = uuid::Uuid::new_v4().to_string();
        let exact_key = uuid::Uuid::new_v4().to_string();
        let empty_key = uuid::Uuid::new_v4().to_string();
        let partial_data = b"abcdefghi"; // two old 4-byte pages plus one durable partial byte
        let exact_data = b"abcdefghijkl";
        write_old_main_sidecar_fixture(
            &data_dir,
            &partial_key,
            "WriteLocked",
            partial_data,
            2,
            None,
        )
        .await;
        write_old_main_sidecar_fixture(&data_dir, &exact_key, "WriteLocked", exact_data, 3, None)
            .await;
        write_old_main_sidecar_fixture(&data_dir, &empty_key, "WriteLocked", b"", 0, None).await;

        // The old files used 4-byte pages; migration deliberately uses the new
        // configured stride without changing the contiguous bytes on disk.
        let manager =
            SpoolManager::<TokioFileIO>::new(&data_dir, 6, 65536, 256).expect("restart manager");
        manager
            .recover()
            .await
            .expect("recover WriteLocked salvage spools");

        let partial = manager
            .get_spool(&partial_key)
            .expect("partial WriteLocked salvage spool");
        let partial_meta = partial.metadata.lock().await.clone();
        assert_eq!(partial.page_size, 6);
        assert_eq!(partial_meta.state, SpoolState::Complete);
        assert!(partial_meta.write_locked, "preserve write-lock provenance");
        assert!(partial.is_readable().await);
        assert_eq!(partial_meta.total_bytes_written, 9);
        assert_eq!(partial_meta.total_pages, 2);
        assert_eq!(partial_meta.final_page_size, Some(3));
        assert!(matches!(
            partial
                .write(9, bytes::Bytes::from_static(b"must-not-append"))
                .await,
            Err(BobsError::SpoolClosed)
        ));
        assert!(matches!(
            partial.complete(Some(8)).await,
            Err(BobsError::SizeMismatch {
                expected: 8,
                actual: 9
            })
        ));
        partial
            .complete(Some(9))
            .await
            .expect("idempotent completion validates recovered size");
        let first = partial.read_page_for_test(0).await.unwrap().unwrap();
        let second = partial.read_page_for_test(1).await.unwrap().unwrap();
        assert_eq!([first.as_ref(), second.as_ref()].concat(), partial_data);
        assert_eq!(
            read_exact_at::<TokioFileIO>(
                &partial.file_handle,
                2,
                6,
                "reading migrated WriteLocked range",
            )
            .await
            .expect("read range across migrated page boundary")
            .as_ref(),
            b"cdefgh"
        );

        let exact = manager
            .get_spool(&exact_key)
            .expect("exact WriteLocked salvage spool");
        let exact_meta = exact.metadata.lock().await.clone();
        assert_eq!(exact.page_size, 6);
        assert_eq!(exact_meta.state, SpoolState::Complete);
        assert_eq!(exact_meta.total_bytes_written, 12);
        assert_eq!(exact_meta.total_pages, 2);
        assert_eq!(exact_meta.final_page_size, None);
        exact
            .complete(Some(12))
            .await
            .expect("complete exact salvage");
        let first = exact.read_page_for_test(0).await.unwrap().unwrap();
        let second = exact.read_page_for_test(1).await.unwrap().unwrap();
        assert_eq!([first.as_ref(), second.as_ref()].concat(), exact_data);

        let empty = manager
            .get_spool(&empty_key)
            .expect("empty WriteLocked salvage spool");
        let empty_meta = empty.metadata.lock().await.clone();
        assert_eq!(empty.page_size, 6);
        assert_eq!(empty_meta.state, SpoolState::Complete);
        assert_eq!(empty_meta.total_bytes_written, 0);
        assert_eq!(empty_meta.total_pages, 0);
        assert_eq!(empty_meta.final_page_size, None);
        empty
            .complete(Some(0))
            .await
            .expect("complete empty salvage");
        assert!(empty.read_page_for_test(0).await.unwrap().is_none());

        for (key, expected_bytes) in [
            (&partial_key, partial_data.as_slice()),
            (&exact_key, exact_data.as_slice()),
            (&empty_key, b"".as_slice()),
        ] {
            let sidecar = persisted_metadata(&manager, key).await;
            assert_eq!(sidecar.state, SpoolState::Complete);
            assert_eq!(sidecar.page_size, 6);
            assert_eq!(sidecar.total_bytes_written, expected_bytes.len() as u64);
            assert_eq!(
                tokio::fs::read(data_dir.join(key).join("spool.dat"))
                    .await
                    .expect("read unchanged durable bytes"),
                expected_bytes
            );
            assert!(!data_dir.join(key).join("meta.json.tmp").exists());
        }

        drop(partial);
        drop(exact);
        drop(empty);
        drop(manager);

        let restarted =
            SpoolManager::<TokioFileIO>::new(&data_dir, 7, 65536, 256).expect("second restart");
        restarted
            .recover()
            .await
            .expect("recover atomically migrated sidecars");
        let partial = restarted
            .get_spool(&partial_key)
            .expect("persisted salvage spool");
        assert_eq!(
            partial.page_size, 6,
            "later config changes must not reinterpret the migrated layout"
        );
        let first = partial.read_page_for_test(0).await.unwrap().unwrap();
        let second = partial.read_page_for_test(1).await.unwrap().unwrap();
        assert_eq!([first.as_ref(), second.as_ref()].concat(), partial_data);
    }

    #[tokio::test]
    async fn test_recovery_quarantines_ambiguous_legacy_writing_partial_without_mutation() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let key = uuid::Uuid::new_v4().to_string();
        let data = b"abcdefghi"; // old page size 4, two full pages and one partial
        write_old_main_sidecar_fixture(&data_dir, &key, "Writing", data, 2, None).await;
        let spool_dir = data_dir.join(&key);
        let meta_path = spool_dir.join("meta.json");
        let original_sidecar = tokio::fs::read(&meta_path)
            .await
            .expect("read original sidecar");

        let manager =
            SpoolManager::<TokioFileIO>::new(&data_dir, 6, 65536, 256).expect("restart manager");
        manager
            .recover()
            .await
            .expect("ambiguous writer is quarantined without blocking startup");

        assert!(
            manager.get_spool(&key).is_none(),
            "quarantined spool is unavailable to read, write, or complete APIs"
        );
        assert_eq!(
            tokio::fs::read(spool_dir.join("spool.dat")).await.unwrap(),
            data
        );
        assert_eq!(tokio::fs::read(&meta_path).await.unwrap(), original_sidecar);
        assert!(spool_dir.exists(), "orphan sweep must preserve quarantine");
    }

    #[tokio::test]
    async fn test_recovery_quarantines_malformed_legacy_write_locked_without_mutation() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let key = uuid::Uuid::new_v4().to_string();
        let data = b"abcdefghi";
        write_old_main_sidecar_fixture(&data_dir, &key, "WriteLocked", data, 2, Some(1)).await;
        let spool_dir = data_dir.join(&key);
        let meta_path = spool_dir.join("meta.json");
        let original_sidecar = tokio::fs::read(&meta_path)
            .await
            .expect("read malformed sidecar");

        let manager =
            SpoolManager::<TokioFileIO>::new(&data_dir, 6, 65536, 256).expect("restart manager");
        manager
            .recover()
            .await
            .expect("malformed active legacy spool is quarantined");

        assert!(manager.get_spool(&key).is_none());
        assert_eq!(
            tokio::fs::read(spool_dir.join("spool.dat")).await.unwrap(),
            data
        );
        assert_eq!(tokio::fs::read(&meta_path).await.unwrap(), original_sidecar);
        assert!(spool_dir.exists());
    }

    #[tokio::test]
    async fn test_recovery_resegments_old_main_readable_sidecars() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let partial_key = uuid::Uuid::new_v4().to_string();
        let exact_key = uuid::Uuid::new_v4().to_string();
        let empty_key = uuid::Uuid::new_v4().to_string();
        let partial_data = b"abcdefghi"; // two old 4-byte pages plus one trailing byte
        let exact_data = b"abcdefghijkl"; // three old 4-byte pages
        write_old_main_sidecar_fixture(&data_dir, &partial_key, "Readable", partial_data, 2, None)
            .await;
        write_old_main_sidecar_fixture(&data_dir, &exact_key, "Readable", exact_data, 3, None)
            .await;
        write_old_main_sidecar_fixture(&data_dir, &empty_key, "Readable", b"", 0, None).await;

        // The old sidecars were written with 4-byte pages. Recovery must not
        // infer that active layout: Readable is terminal and can use this new
        // configured 6-byte segmentation without changing any durable bytes.
        let manager =
            SpoolManager::<TokioFileIO>::new(&data_dir, 6, 65536, 256).expect("restart manager");
        manager.recover().await.expect("migrate Readable spools");

        let partial = manager
            .get_spool(&partial_key)
            .expect("full-plus-partial Readable spool");
        assert_eq!(partial.page_size, 6);
        let partial_meta = partial.metadata.lock().await.clone();
        assert_eq!(partial_meta.state, SpoolState::Complete);
        assert_eq!(partial_meta.total_bytes_written, 9);
        assert_eq!(partial_meta.total_pages, 2);
        assert_eq!(partial_meta.final_page_size, Some(3));
        let first = partial.read_page_for_test(0).await.unwrap().unwrap();
        let second = partial.read_page_for_test(1).await.unwrap().unwrap();
        assert_eq!([first.as_ref(), second.as_ref()].concat(), partial_data);
        assert_eq!(
            read_exact_at::<TokioFileIO>(&partial.file_handle, 2, 6, "reading migrated range",)
                .await
                .expect("read range across new page boundary")
                .as_ref(),
            b"cdefgh"
        );

        let exact = manager
            .get_spool(&exact_key)
            .expect("exact-multiple Readable spool");
        assert_eq!(exact.page_size, 6);
        let exact_meta = exact.metadata.lock().await.clone();
        assert_eq!(exact_meta.total_bytes_written, 12);
        assert_eq!(exact_meta.total_pages, 2);
        assert_eq!(exact_meta.final_page_size, None);
        let first = exact.read_page_for_test(0).await.unwrap().unwrap();
        let second = exact.read_page_for_test(1).await.unwrap().unwrap();
        assert_eq!([first.as_ref(), second.as_ref()].concat(), exact_data);

        let empty = manager.get_spool(&empty_key).expect("empty Readable spool");
        assert_eq!(empty.page_size, 6);
        let empty_meta = empty.metadata.lock().await.clone();
        assert_eq!(empty_meta.total_bytes_written, 0);
        assert_eq!(empty_meta.total_pages, 0);
        assert_eq!(empty_meta.final_page_size, None);
        assert!(empty.read_page_for_test(0).await.unwrap().is_none());

        for key in [&partial_key, &exact_key, &empty_key] {
            let sidecar = persisted_metadata(&manager, key).await;
            assert_eq!(sidecar.state, SpoolState::Complete);
            assert_eq!(sidecar.page_size, 6);
            assert!(data_dir.join(key).join("spool.dat").exists());
            assert!(!data_dir.join(key).join("meta.json.tmp").exists());
        }
    }

    #[tokio::test]
    async fn test_recovery_rejects_ambiguous_old_main_stride_without_deleting_data() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let key = uuid::Uuid::new_v4().to_string();
        let data = b"abcdefghij";
        write_old_main_sidecar_fixture(&data_dir, &key, "Complete", data, 4, Some(3)).await;
        let spool_dir = data_dir.join(&key);
        let meta_path = spool_dir.join("meta.json");
        let original_sidecar = tokio::fs::read(&meta_path)
            .await
            .expect("read fixture sidecar");

        let manager =
            SpoolManager::<TokioFileIO>::new(&data_dir, 8192, 65536, 256).expect("restart manager");
        let counts = manager
            .recover_with_counts()
            .await
            .expect("ambiguous candidate must not block recovery");
        assert_eq!(counts, (0, 0));
        assert!(manager.get_spool(&key).is_none());
        assert_eq!(
            tokio::fs::read(spool_dir.join("spool.dat")).await.unwrap(),
            data
        );
        assert_eq!(tokio::fs::read(&meta_path).await.unwrap(), original_sidecar);
    }

    #[tokio::test]
    async fn test_failed_delete_stays_accounted_and_can_be_retried() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let key = uuid::Uuid::new_v4().to_string();
        let manager =
            SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 65536, 1).expect("manager init");
        manager
            .create_spool(key.clone(), None, None, false, HashMap::new())
            .await
            .expect("create spool");
        let meta_path = data_dir.join(&key).join("meta.json");
        tokio::fs::remove_file(&meta_path)
            .await
            .expect("remove metadata file");
        tokio::fs::create_dir(&meta_path)
            .await
            .expect("replace metadata with directory");

        assert!(manager.delete_spool(&key).await.is_err());
        let spool = manager
            .get_spool(&key)
            .expect("failed delete remains managed");
        assert_eq!(spool.metadata.lock().await.state, SpoolState::Deleting);
        assert_eq!(manager.admission.available_permits(), 0);

        tokio::fs::remove_dir(&meta_path)
            .await
            .expect("repair metadata path");
        manager
            .delete_spool(&key)
            .await
            .expect("retry delete succeeds");
        assert!(manager.get_spool(&key).is_none());
        assert_eq!(manager.admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn test_delete_wins_complete_race_without_resurrecting_directory() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let key = uuid::Uuid::new_v4().to_string();
        let manager = Arc::new(
            SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 65536, 256).expect("manager init"),
        );
        manager
            .create_spool(key.clone(), None, None, false, HashMap::new())
            .await
            .expect("create spool");
        let spool = manager.get_spool(&key).expect("spool exists");
        spool
            .write(0, bytes::Bytes::from_static(b"race"))
            .await
            .expect("write data");
        let lifecycle_guard = spool.lifecycle_lock.lock().await;

        let delete_manager = Arc::clone(&manager);
        let delete_key = key.clone();
        let delete = tokio::spawn(async move { delete_manager.delete_spool(&delete_key).await });
        tokio::task::yield_now().await;
        let complete_spool = Arc::clone(&spool);
        let complete = tokio::spawn(async move { complete_spool.complete(Some(4)).await });
        drop(lifecycle_guard);

        delete.await.expect("delete task").expect("delete wins");
        assert!(matches!(
            complete.await.expect("complete task"),
            Err(BobsError::SpoolNotFound { .. })
        ));
        assert!(!data_dir.join(&key).exists());
        assert!(manager.metadata_store.read(&key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_recovery_finishes_legacy_deleting_spool_without_page_size() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let key = uuid::Uuid::new_v4().to_string();
        let manager =
            SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 65536, 256).expect("manager init");
        write_sidecar_fixture(
            &manager,
            sidecar_fixture_metadata(&data_dir, &key, SpoolState::Deleting, 0),
            b"",
        )
        .await;
        let meta_path = data_dir.join(&key).join("meta.json");
        let mut json: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&meta_path).await.expect("read metadata"))
                .expect("parse metadata");
        json.as_object_mut()
            .expect("metadata object")
            .remove("page_size");
        tokio::fs::write(&meta_path, serde_json::to_vec(&json).unwrap())
            .await
            .expect("write legacy metadata");
        drop(manager);

        let recovered =
            SpoolManager::<TokioFileIO>::new(&data_dir, 8192, 65536, 256).expect("restart manager");
        recovered.recover().await.expect("finish legacy deletion");
        assert!(!data_dir.join(&key).exists());
    }

    #[tokio::test]
    async fn test_corrupt_sidecar_does_not_discard_valid_spool() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let valid_key = uuid::Uuid::new_v4().to_string();
        let corrupt_key = uuid::Uuid::new_v4().to_string();
        let manager =
            SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 65536, 256).expect("manager init");
        write_sidecar_fixture(
            &manager,
            sidecar_fixture_metadata(&data_dir, &valid_key, SpoolState::Complete, 4),
            b"data",
        )
        .await;
        let corrupt_dir = data_dir.join(&corrupt_key);
        tokio::fs::create_dir_all(&corrupt_dir)
            .await
            .expect("create corrupt spool dir");
        tokio::fs::write(corrupt_dir.join("meta.json"), b"{not json")
            .await
            .expect("write corrupt metadata");
        tokio::fs::write(corrupt_dir.join("spool.dat"), b"bad")
            .await
            .expect("write corrupt spool data");
        drop(manager);

        let recovered =
            SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 65536, 256).expect("restart manager");
        recovered.recover().await.expect("recover valid spool");
        assert!(recovered.get_spool(&valid_key).is_some());
        assert!(data_dir.join(&valid_key).exists());
        assert!(!corrupt_dir.exists());
    }

    #[tokio::test]
    async fn test_delete_wins_write_race_without_repopulating_cache() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let key = uuid::Uuid::new_v4().to_string();
        let manager = Arc::new(
            SpoolManager::<TokioFileIO>::new(&data_dir, 4, 64, 256).expect("manager init"),
        );
        manager
            .create_spool(key.clone(), None, None, false, HashMap::new())
            .await
            .expect("create spool");
        let spool = manager.get_spool(&key).expect("spool exists");
        let lifecycle_guard = spool.lifecycle_lock.lock().await;

        let delete_manager = Arc::clone(&manager);
        let delete_key = key.clone();
        let delete = tokio::spawn(async move { delete_manager.delete_spool(&delete_key).await });
        tokio::task::yield_now().await;
        let write_spool = Arc::clone(&spool);
        let write = tokio::spawn(async move {
            write_spool
                .write(0, bytes::Bytes::from_static(b"data"))
                .await
        });
        drop(lifecycle_guard);

        delete.await.expect("delete task").expect("delete wins");
        assert!(matches!(
            write.await.expect("write task"),
            Err(BobsError::InvalidState { .. })
        ));
        assert!(!data_dir.join(&key).exists());
        assert!(!manager.page_cache.lock().await.contains(&key, 0));
    }
}
