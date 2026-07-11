// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use crate::error::{BobsError, Result};
use crate::io::{read_exact_at, FileIO};
use crate::metadata::{MetadataStore, SyncSidecarMetadataStore};
use crate::metrics::BobsMetrics;
use crate::spool::{PageCache, Spool, SpoolMetadata, SpoolState};
use crate::time::now_secs;
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, Semaphore};

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

fn metric_state_label(state: &SpoolState) -> &'static str {
    match state {
        SpoolState::Creating | SpoolState::Writing => crate::metrics::state::WRITING,
        SpoolState::WriteLocked => crate::metrics::state::WRITE_LOCKED,
        SpoolState::Complete | SpoolState::Deleting => crate::metrics::state::COMPLETE,
    }
}

pub struct SpoolManager<F: FileIO, M: MetadataStore = SyncSidecarMetadataStore> {
    pub spools: DashMap<String, Arc<Spool<F, M>>>,
    pub metadata_store: M,
    pub data_dir: PathBuf,
    pub page_size: usize,
    pub max_cache_bytes: usize,
    pub page_cache: Arc<Mutex<PageCache>>,
    pub metrics: Arc<BobsMetrics>,
    /// Bounds spools concurrently holding first-read cache memory.
    pub admission: Arc<Semaphore>,
    pub max_live_spools: usize,
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

        Ok(Self {
            spools: DashMap::new(),
            metadata_store,
            data_dir: data_dir.as_ref().to_path_buf(),
            page_size,
            max_cache_bytes,
            page_cache,
            metrics: Arc::new(BobsMetrics::new(false)),
            admission: Arc::new(Semaphore::new(max_live_spools)),
            max_live_spools,
        })
    }

    /// Set the metrics handle (replaces the default no-op).
    pub fn set_metrics(&mut self, metrics: Arc<BobsMetrics>) {
        self.metrics = metrics;
    }

    pub async fn create_spool(
        &self,
        key: String,
        content_type: Option<String>,
        content_encoding: Option<String>,
        write_locked: bool,
        labels: HashMap<String, String>,
    ) -> Result<()> {
        let permit = Arc::clone(&self.admission)
            .acquire_owned()
            .await
            .map_err(|_| BobsError::IoError(std::io::Error::other("admission semaphore closed")))?;

        let spool_dir = self.data_dir.join(&key);
        let data_path = spool_dir.join("spool.dat");

        tokio::fs::create_dir_all(&spool_dir)
            .await
            .map_err(BobsError::IoError)?;

        let handle = F::create(&data_path).await.map_err(BobsError::IoError)?;

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

        self.metadata_store.write(&metadata).await?;

        let spool = Arc::new(
            Spool::new_with_admission(
                metadata,
                handle,
                self.page_size,
                Arc::clone(&self.page_cache),
                self.metadata_store.clone(),
                Arc::clone(&self.metrics),
                Some(permit),
            )
            .await,
        );
        self.spools.insert(key.clone(), spool);

        // Record initial state for the active spool gauge.
        let initial_state = if write_locked {
            crate::metrics::state::WRITE_LOCKED
        } else {
            crate::metrics::state::WRITING
        };
        self.metrics.record_state_transition(None, initial_state);

        Ok(())
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
            Ok(()) => tracing::info!(
                "event.name" = "bobs.spool.deleted",
                "bobs.spool.key" = %key,
                "request.id" = job_id.unwrap_or_default(),
                reason = reason.as_str(),
                outcome = "success",
                "spool deleted"
            ),
            Err(error) => tracing::error!(
                "event.name" = "bobs.spool.deleted",
                "bobs.spool.key" = %key,
                "request.id" = job_id.unwrap_or_default(),
                reason = reason.as_str(),
                outcome = "error",
                error = %error,
                "spool deletion failed"
            ),
        }
        result
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

        let old_label = {
            let mut meta = spool.metadata.lock().await;
            if meta.state == SpoolState::Deleting {
                None
            } else {
                let old_label = metric_state_label(&meta.state);
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
        let handle = spool.file_handle.lock().await.take();
        if let Some(handle) = handle {
            F::close(handle).await.map_err(BobsError::IoError)?;
        }

        // No durable Deleting tombstone is needed: before sidecar removal a crash
        // safely recovers the original spool, and afterwards the orphan sweep removes
        // any directory left behind.
        self.metadata_store.delete(key).await?;
        let spool_dir = self.data_dir.join(key);
        match tokio::fs::remove_dir_all(&spool_dir).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(BobsError::IoError(error)),
        }

        self.page_cache.lock().await.remove_spool(key);
        spool.release_admission();
        if self
            .spools
            .remove_if(key, |_, current| Arc::ptr_eq(current, &spool))
            .is_some()
        {
            self.metrics
                .record_spool_removed(crate::metrics::state::COMPLETE);
        }
        Ok(())
    }

    pub async fn recover(&self) -> Result<()> {
        let started = Instant::now();
        let mut recovered: Vec<(String, SpoolMetadata)> = Vec::new();
        let mut stale_keys: Vec<String> = Vec::new();
        let mut corrupt_deleted = 0_u64;
        let mut orphan_deleted = 0_u64;

        for (key, result) in self.metadata_store.list()? {
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

        for (key, mut meta) in recovered {
            if !meta.data_path.exists() {
                tracing::warn!(key = %key, "recovery: data file missing, discarding");
                stale_keys.push(key);
                continue;
            }

            match meta.state {
                SpoolState::Creating => {
                    tracing::debug!(key = %key, "recovery removing incomplete spool (Creating)");
                    stale_keys.push(key.clone());
                    let spool_dir = self.data_dir.join(&key);
                    let _ = tokio::fs::remove_dir_all(&spool_dir).await;
                    continue;
                }
                SpoolState::Deleting => {
                    tracing::debug!(key = %key, "recovery removing incomplete spool (Deleting)");
                    stale_keys.push(key.clone());
                    let spool_dir = self.data_dir.join(&key);
                    let _ = tokio::fs::remove_dir_all(&spool_dir).await;
                    continue;
                }
                SpoolState::Writing | SpoolState::WriteLocked => {
                    meta.last_write_at = now_secs();
                }
                SpoolState::Complete => {}
            }

            let spool_page_size = usize::try_from(meta.page_size)
                .ok()
                .filter(|size| *size > 0)
                .ok_or_else(|| {
                    let detail = if meta.page_size == 0 {
                        "metadata predates persisted page sizes"
                    } else {
                        "persisted page size is unsupported on this platform"
                    };
                    BobsError::ConfigurationError(format!(
                        "cannot recover spool {key}: {detail}; leave the spool directory intact and restart with a compatible BOBS version to drain or delete it"
                    ))
                })?;

            let handle = match F::open(&meta.data_path).await {
                Ok(h) => h,
                Err(e) => {
                    tracing::warn!(key = %key, error = %e, "recovery: failed to open data file, discarding");
                    stale_keys.push(key);
                    continue;
                }
            };

            let mut metadata_corrected = false;
            let file_size = std::fs::metadata(&meta.data_path)
                .map(|m| m.len())
                .unwrap_or(0);
            let mut trailing_partial_len = 0;

            // Backfill readable_at for spools that were persisted before this
            // field existed. Grants a full idle-TTL grace period after upgrade.
            if meta.state == SpoolState::Complete && meta.readable_at.is_none() {
                meta.readable_at = Some(now_secs());
                metadata_corrected = true;
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
                let expected_full_pages_bytes = match meta.final_page_size {
                    Some(final_page_size) if meta.total_pages > 0 => {
                        (meta.total_pages - 1) * spool_page_size as u64 + final_page_size
                    }
                    _ => meta.total_pages * spool_page_size as u64,
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

            if metadata_corrected {
                self.metadata_store.write(&meta).await?;
            }

            // Capture fields needed for post-init before meta is moved.
            let meta_state_for_init = meta.state.clone();
            let meta_total_bytes_for_init = meta.total_bytes_written;
            let permit = Arc::clone(&self.admission).try_acquire_owned().ok();
            if permit.is_none() {
                tracing::warn!(
                    key = %key,
                    max_live_spools = self.max_live_spools,
                    recovered_spools = self.spools.len() + 1,
                    "recovery: spool exceeds admission capacity and has no cache admission permit"
                );
            }

            let spool = Arc::new(
                Spool::new_with_admission(
                    meta,
                    handle,
                    spool_page_size,
                    Arc::clone(&self.page_cache),
                    self.metadata_store.clone(),
                    Arc::clone(&self.metrics),
                    permit,
                )
                .await,
            );

            if trailing_partial_len > 0 {
                let partial = read_exact_logical_range::<F>(
                    &spool.file_handle,
                    meta_total_bytes_for_init - trailing_partial_len,
                    trailing_partial_len as usize,
                )
                .await?;
                spool.write_buffer.lock().await.extend_from_slice(&partial);
            }

            // Seed last_read_activity_at so recovered spools get a full
            // read_idle_ttl_secs grace period before cleanup can fire.
            spool
                .last_read_activity_at
                .store(now_secs(), Ordering::SeqCst);

            // Re-initialize missing ranges for complete spools. No served
            // ranges are known after restart, so coverage resets to
            // [0, total_size). full_read_complete_ttl_secs won't trigger
            // until the object is re-served — safe by design.
            if matches!(meta_state_for_init, SpoolState::Complete) {
                spool
                    .missing_ranges
                    .lock()
                    .await
                    .initialize(meta_total_bytes_for_init);
            }

            self.spools.insert(key, spool);

            // Count recovered spool in the active gauge.
            let recovered_label = metric_state_label(&meta_state_for_init);
            self.metrics.record_state_transition(None, recovered_label);
        }

        stale_keys.sort();
        stale_keys.dedup();

        for key in &stale_keys {
            self.metadata_store.delete(key).await?;
            let spool_dir = self.data_dir.join(key);
            match tokio::fs::remove_dir_all(&spool_dir).await {
                Ok(()) => {
                    corrupt_deleted += 1;
                    tracing::info!("event.name" = "bobs.spool.deleted", "bobs.spool.key" = %key, reason = DeleteReason::Corrupt.as_str(), outcome = "success", "spool deleted during recovery");
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(BobsError::IoError(error)),
            }
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
            // Recognised-key directories without spool markers are preserved:
            // they may be unrelated operator data, or a future spool shape, and
            // are not safe orphans unless they contain a known spool marker.
            let shaped_like_spool =
                entry_path.join("spool.dat").exists() || entry_path.join("meta.json").exists();
            if !shaped_like_spool {
                tracing::warn!(orphan = %name, "recovery: skipping spool-key directory without spool markers during orphan sweep");
                continue;
            }

            tracing::warn!(orphan = %name, "removing orphan spool directory");
            if tokio::fs::remove_dir_all(entry_path).await.is_ok() {
                orphan_deleted += 1;
                tracing::info!("event.name" = "bobs.spool.deleted", "bobs.spool.key" = %name, reason = DeleteReason::Orphan.as_str(), outcome = "success", "orphan spool deleted during recovery");
            }
        }

        tracing::info!(
            "event.name" = "bobs.recovery.completed",
            recovered = self.spools.len(),
            stale = stale_keys.len(),
            orphan_deleted = orphan_deleted,
            corrupt_deleted = corrupt_deleted,
            duration_ms = started.elapsed().as_millis() as u64,
            outcome = "success",
            "recovery completed"
        );
        Ok(())
    }
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

async fn read_exact_logical_range<F: FileIO>(
    file_handle: &Arc<tokio::sync::Mutex<Option<F::Handle>>>,
    offset: u64,
    len: usize,
) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(len);
    let handle_guard = file_handle.lock().await;
    let Some(handle) = handle_guard.as_ref() else {
        return Err(BobsError::WriterInactive);
    };

    let buf = read_exact_at::<F>(handle, offset, len, "loading trailing partial page")
        .await
        .map_err(BobsError::IoError)?;
    out.extend_from_slice(&buf);

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::TokioFileIO;
    use std::sync::Arc;
    use tempfile::tempdir;

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

    #[tokio::test]
    async fn test_new_accepts_cache_smaller_than_page_size() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");

        let manager =
            SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 1024, 256).expect("manager init");

        assert_eq!(manager.page_size, 4096);
        assert_eq!(manager.max_cache_bytes, 1024);
        assert_eq!(manager.page_cache.lock().await.max_bytes(), 1024);
    }

    #[tokio::test]
    async fn test_new_accepts_zero_cache_bytes() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");

        let manager =
            SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 0, 256).expect("manager init");

        assert_eq!(manager.max_cache_bytes, 0);
        assert_eq!(manager.page_cache.lock().await.max_bytes(), 0);
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
                .read_page(0)
                .await
                .expect("read page 0")
                .unwrap()
                .as_ref(),
            &data[..page_size]
        );
        assert_eq!(
            spool
                .read_page(1)
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
            spool.read_page(0).await.expect("page 0").unwrap().as_ref(),
            &data[..page_size]
        );
        assert_eq!(
            spool.read_page(1).await.expect("page 1").unwrap().as_ref(),
            &data[page_size..page_size * 2]
        );
        assert_eq!(
            spool.read_page(2).await.expect("page 2").unwrap().as_ref(),
            &data[page_size * 2..]
        );
        assert!(spool.read_page(3).await.expect("end of spool").is_none());
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

    /// After recovery, `last_read_activity_at` must be nonzero so that the
    /// recovered spool gets a full `read_idle_ttl_secs` grace period before
    /// the cleanup loop can fire.
    #[tokio::test]
    async fn test_recovery_seeds_last_read_activity_at() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");

        let key = {
            let manager = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
                .expect("manager init");
            let key = uuid::Uuid::new_v4().to_string();
            manager
                .create_spool(key.clone(), None, None, true, HashMap::new()) // WriteLocked
                .await
                .expect("create spool");
            key
        };

        let manager2 = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
            .expect("manager2 init");
        manager2.recover().await.expect("recover should succeed");

        let spool = manager2.get_spool(&key).expect("recovered spool exists");
        let activity = spool.last_read_activity_at.load(Ordering::SeqCst);
        assert!(
            activity > 0,
            "last_read_activity_at must be seeded to nonzero on recovery, got {activity}"
        );
    }

    /// After recovering a Complete spool, `missing_ranges` must be initialised
    /// so that its `total_size` matches what was written and `gap_count() == 1`
    /// (the entire object is an unserved gap until re-served after restart).
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

        let manager2 = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 16 * 4096, 256)
            .expect("manager2 init");
        manager2.recover().await.expect("recover should succeed");

        let spool = manager2.get_spool(&key).expect("recovered spool exists");
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
    async fn test_recovery_orphan_sweep_preserves_uuid_empty_and_uuid_symlink_dirs() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        tokio::fs::create_dir_all(&data_dir)
            .await
            .expect("create data dir");

        let empty_uuid_key = uuid::Uuid::new_v4().to_string();
        let empty_uuid_dir = data_dir.join(&empty_uuid_key);
        tokio::fs::create_dir_all(&empty_uuid_dir)
            .await
            .expect("create empty UUID dir");

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
            empty_uuid_dir.exists(),
            "UUID-named empty directories must be preserved"
        );
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
        assert_eq!(spool.read_page(0).await.unwrap().unwrap().as_ref(), b"abcd");
        assert_eq!(spool.read_page(1).await.unwrap().unwrap().as_ref(), b"ef");
    }

    #[tokio::test]
    async fn test_recovery_rejects_ambiguous_legacy_page_size_without_deleting_data() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let key = uuid::Uuid::new_v4().to_string();
        let manager =
            SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 65536, 256).expect("manager init");
        manager
            .create_spool(key.clone(), None, None, false, HashMap::new())
            .await
            .expect("create spool");
        let spool_dir = data_dir.join(&key);
        let meta_path = spool_dir.join("meta.json");
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
        let error = recovered
            .recover()
            .await
            .expect_err("legacy metadata is ambiguous");
        assert!(
            matches!(error, BobsError::ConfigurationError(message) if message.contains(&key) && message.contains("compatible BOBS version"))
        );
        assert!(
            spool_dir.exists(),
            "rejection must leave legacy data intact"
        );
        assert!(
            meta_path.exists(),
            "rejection must leave legacy metadata intact"
        );
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
