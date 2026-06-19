use crate::error::{BobsError, Result};
use crate::io::FileIO;
use crate::metadata::{legacy_redb, MetadataStore, SyncSidecarMetadataStore};
use crate::metrics::BobsMetrics;
use crate::spool::{PageCache, Spool, SpoolMetadata, SpoolState};
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

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

pub struct SpoolManager<F: FileIO, M: MetadataStore = SyncSidecarMetadataStore> {
    pub spools: DashMap<String, Arc<Spool<F, M>>>,
    pub metadata_store: M,
    pub data_dir: PathBuf,
    pub page_size: usize,
    pub max_cache_bytes: usize,
    pub page_cache: Arc<Mutex<PageCache>>,
    pub metrics: Arc<BobsMetrics>,
}

impl<F: FileIO> SpoolManager<F, SyncSidecarMetadataStore> {
    pub fn new(
        db_path: impl AsRef<Path>,
        data_dir: impl AsRef<Path>,
        page_size: usize,
        max_cache_bytes: usize,
    ) -> Result<Self> {
        legacy_redb::migrate_from_redb(db_path, data_dir.as_ref())?;
        Self::with_metadata_store(
            SyncSidecarMetadataStore::new(data_dir.as_ref()),
            data_dir,
            page_size,
            max_cache_bytes,
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
    ) -> Result<Self> {
        if page_size == 0 {
            return Err(BobsError::IoError(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "page_size must be greater than 0",
            )));
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
            metrics: Arc::new(BobsMetrics::new(false, vec![], 128)),
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
            total_bytes_written: 0,
            checksum_crc32c: None,
            total_pages: 0,
            final_page_size: None,
            data_path,
            labels,
        };

        self.metadata_store.write(&metadata).await?;

        let spool = Arc::new(
            Spool::new(
                metadata,
                handle,
                self.page_size,
                Arc::clone(&self.page_cache),
                self.metadata_store.clone(),
                Arc::clone(&self.metrics),
            )
            .await,
        );
        self.spools.insert(key.clone(), spool);

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
            Ok(()) => {
                if let Some(job_id) = job_id {
                    tracing::info!("event.name" = "bobs.spool.deleted", "bobs.spool.key" = %key, "job.id" = %job_id, reason = reason.as_str(), outcome = "success", "spool deleted");
                } else {
                    tracing::info!("event.name" = "bobs.spool.deleted", "bobs.spool.key" = %key, reason = reason.as_str(), outcome = "success", "spool deleted");
                }
            }
            Err(error) => {
                if let Some(job_id) = job_id {
                    tracing::error!("event.name" = "bobs.spool.deleted", "bobs.spool.key" = %key, "job.id" = %job_id, reason = reason.as_str(), outcome = "error", error = %error, "spool deletion failed");
                } else {
                    tracing::error!("event.name" = "bobs.spool.deleted", "bobs.spool.key" = %key, reason = reason.as_str(), outcome = "error", error = %error, "spool deletion failed");
                }
            }
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

        {
            let mut meta = spool.metadata.lock().await;
            meta.state = SpoolState::Deleting;
        }
        spool.cancel.cancel();

        self.spools.remove(key);
        self.page_cache.lock().await.remove_spool(key);

        let deleting_meta = { spool.metadata.lock().await.clone() };
        self.metadata_store.write(&deleting_meta).await?;
        self.metadata_store.delete(key).await?;

        let spool_dir = self.data_dir.join(key);
        match tokio::fs::remove_dir_all(&spool_dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(BobsError::IoError(e)),
        }
    }

    pub async fn recover(&self) -> Result<()> {
        let started = Instant::now();
        let mut recovered: Vec<(String, SpoolMetadata)> = Vec::new();
        let mut stale_keys: Vec<String> = Vec::new();
        let mut corrupt_deleted = 0_u64;
        let mut orphan_deleted = 0_u64;

        for meta in self.metadata_store.list()? {
            match meta {
                Ok(meta) => recovered.push((meta.key.clone(), meta)),
                Err(error) => {
                    tracing::warn!(error = %error, "recovery: corrupt sidecar metadata, discarding affected spool directories");
                    stale_keys.extend(self.corrupt_sidecar_keys().await?);
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
                SpoolState::Complete | SpoolState::Readable => {}
            }

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
            if matches!(meta.state, SpoolState::Complete | SpoolState::Readable)
                && meta.readable_at.is_none()
            {
                meta.readable_at = Some(now_secs());
                metadata_corrected = true;
            }

            if matches!(meta.state, SpoolState::Writing | SpoolState::WriteLocked) {
                let progress = in_progress_progress_from_file(file_size, self.page_size);
                trailing_partial_len = progress.trailing_partial_len;

                if meta.total_bytes_written != progress.total_bytes_written
                    || meta.total_pages != progress.total_pages
                    || meta.final_page_size.is_some()
                    || meta.checksum_crc32c.is_some()
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
                    meta.checksum_crc32c = None;
                }
            } else {
                let expected_full_pages_bytes = match meta.final_page_size {
                    Some(final_page_size) if meta.total_pages > 0 => {
                        (meta.total_pages - 1) * self.page_size as u64 + final_page_size
                    }
                    _ => meta.total_pages * self.page_size as u64,
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

            let crc = if matches!(meta.state, SpoolState::Complete | SpoolState::Readable) {
                if let Some(checksum) = meta.checksum_crc32c {
                    checksum
                } else {
                    crc32c_for_logical_size::<F>(&handle, meta.total_bytes_written).await?
                }
            } else {
                crc32c_for_logical_size::<F>(&handle, file_size).await?
            };

            // Capture fields needed for post-init before meta is moved.
            let meta_state_for_init = meta.state.clone();
            let meta_total_bytes_for_init = meta.total_bytes_written;

            let spool = Arc::new(
                Spool::new(
                    meta,
                    handle,
                    self.page_size,
                    Arc::clone(&self.page_cache),
                    self.metadata_store.clone(),
                    Arc::clone(&self.metrics),
                )
                .await,
            );
            *spool.running_crc32c.lock().await = crc;

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
            if name == "spools.redb" {
                continue;
            }
            if !entry
                .file_type()
                .await
                .map_err(BobsError::IoError)?
                .is_dir()
            {
                continue;
            }
            if !recovered_keys.contains(&name)
                && !self.spools.contains_key(&name)
                && !stale_key_set.contains(&name)
            {
                tracing::warn!(orphan = %name, "removing orphan spool directory");
                if tokio::fs::remove_dir_all(entry.path()).await.is_ok() {
                    orphan_deleted += 1;
                    tracing::info!("event.name" = "bobs.spool.deleted", "bobs.spool.key" = %name, reason = DeleteReason::Orphan.as_str(), outcome = "success", "orphan spool deleted during recovery");
                }
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

    async fn corrupt_sidecar_keys(&self) -> Result<Vec<String>> {
        let mut corrupt = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.data_dir)
            .await
            .map_err(BobsError::IoError)?;
        while let Some(entry) = entries.next_entry().await.map_err(BobsError::IoError)? {
            if !entry
                .file_type()
                .await
                .map_err(BobsError::IoError)?
                .is_dir()
            {
                continue;
            }
            let key = entry.file_name().to_string_lossy().to_string();
            if !entry.path().join("meta.json").exists() {
                continue;
            }
            if let Err(error) = self.metadata_store.read(&key).await {
                tracing::warn!(key = %key, error = %error, "recovery: discarding corrupt metadata sidecar");
                corrupt.push(key);
            }
        }
        Ok(corrupt)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InProgressProgress {
    total_bytes_written: u64,
    total_pages: u64,
    trailing_partial_len: u64,
}

fn in_progress_progress_from_file(file_size: u64, page_size: usize) -> InProgressProgress {
    let page_size = page_size as u64;
    InProgressProgress {
        total_bytes_written: file_size,
        total_pages: file_size / page_size,
        trailing_partial_len: file_size % page_size,
    }
}

async fn crc32c_for_logical_size<F: FileIO>(handle: &F::Handle, logical_size: u64) -> Result<u32> {
    let mut crc = 0u32;
    let mut offset = 0u64;
    let mut remaining = logical_size;
    const CHUNK_LEN: u64 = 64 * 1024;

    while remaining > 0 {
        let want = remaining.min(CHUNK_LEN) as usize;
        let buf = F::read_at(handle, offset, want)
            .await
            .map_err(BobsError::IoError)?;
        if buf.is_empty() {
            return Err(BobsError::IoError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("expected {logical_size} bytes while reconstructing CRC, got {offset}"),
            )));
        }
        crc = crc32c::crc32c_append(crc, &buf);
        offset += buf.len() as u64;
        remaining -= buf.len() as u64;
    }

    Ok(crc)
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

    while out.len() < len {
        let buf = F::read_at(handle, offset + out.len() as u64, len - out.len())
            .await
            .map_err(BobsError::IoError)?;
        if buf.is_empty() {
            return Err(BobsError::IoError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "expected {len} bytes while loading trailing partial page, got {}",
                    out.len()
                ),
            )));
        }
        out.extend_from_slice(&buf);
    }

    Ok(out)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::TokioFileIO;
    use crate::metadata::legacy_redb;
    use std::fs;
    use tempfile::tempdir;

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
        let final_page_size = if total_pages == 0 || logical_size % page_size == 0 {
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
            total_bytes_written: logical_size,
            checksum_crc32c: None,
            total_pages,
            final_page_size,
            data_path: data_dir.join(key).join("spool.dat"),
            labels: HashMap::new(),
        }
    }

    async fn write_sidecar_fixture(
        manager: &SpoolManager<TokioFileIO>,
        mut metadata: SpoolMetadata,
        data: &[u8],
    ) {
        let spool_dir = manager.data_dir.join(&metadata.key);
        tokio::fs::create_dir_all(&spool_dir)
            .await
            .expect("create spool dir");
        tokio::fs::write(&metadata.data_path, data)
            .await
            .expect("write spool data");
        if matches!(metadata.state, SpoolState::Complete | SpoolState::Readable) {
            metadata.checksum_crc32c = Some(crc32c::crc32c(data));
        }
        manager
            .metadata_store
            .write(&metadata)
            .await
            .expect("write sidecar metadata");
    }

    fn migration_fixture_metadata(key: &str, generation: u64, data_dir: &Path) -> SpoolMetadata {
        SpoolMetadata {
            key: key.to_string(),
            content_type: Some("application/octet-stream".to_string()),
            content_encoding: None,
            state: SpoolState::Complete,
            write_locked: false,
            created_at: 100 + generation,
            last_write_at: 200 + generation,
            last_read_at: Some(250 + generation),
            readable_at: Some(300 + generation),
            total_bytes_written: generation * 8192,
            checksum_crc32c: Some((0xabc0 + generation) as u32),
            total_pages: generation,
            final_page_size: Some(4096),
            data_path: data_dir.join(key).join("spool.dat"),
            labels: HashMap::new(),
        }
    }

    fn assert_migrated_sidecars(
        data_dir: &Path,
        rows: &[legacy_redb::LegacyRedbRow],
        expected_count: usize,
    ) {
        let mut actual_keys = Vec::new();
        for entry in fs::read_dir(data_dir).expect("read data dir") {
            let entry = entry.expect("read data dir entry");
            if entry.file_type().expect("entry file type").is_dir() {
                actual_keys.push(entry.file_name().to_string_lossy().to_string());
            }
        }
        actual_keys.sort();
        actual_keys.dedup();
        assert_eq!(
            actual_keys.len(),
            expected_count,
            "migration must not create duplicate or missing sidecar directories"
        );

        for row in rows {
            let meta_path = data_dir.join(&row.key).join("meta.json");
            let bytes = fs::read(&meta_path).expect("read migrated meta.json");
            assert_eq!(
                bytes, row.bytes,
                "migrated meta.json bytes must match legacy redb row for {}",
                row.key
            );
            let recovered: SpoolMetadata =
                serde_json::from_slice(&bytes).expect("deserialize migrated sidecar");
            let legacy: SpoolMetadata =
                serde_json::from_slice(&row.bytes).expect("deserialize legacy row");
            assert_eq!(
                serde_json::to_value(recovered).expect("serialize recovered"),
                serde_json::to_value(legacy).expect("serialize legacy")
            );
        }
    }

    #[test]
    fn migration_from_redb_is_idempotent_with_existing_sidecars_and_tmp_files() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");
        let metadata = vec![
            migration_fixture_metadata("existing", 1, &data_dir),
            migration_fixture_metadata("tmp-leftover", 2, &data_dir),
            migration_fixture_metadata("new", 3, &data_dir),
        ];
        let rows = legacy_redb::create_legacy_db(&db_path, &metadata).expect("create legacy db");

        let existing_row = rows
            .iter()
            .find(|row| row.key == "existing")
            .expect("existing row");
        let existing_dir = data_dir.join("existing");
        fs::create_dir_all(&existing_dir).expect("create existing sidecar dir");
        fs::write(existing_dir.join("meta.json"), &existing_row.bytes)
            .expect("write existing sidecar");

        let tmp_row = rows
            .iter()
            .find(|row| row.key == "tmp-leftover")
            .expect("tmp row");
        let tmp_dir = data_dir.join("tmp-leftover");
        fs::create_dir_all(&tmp_dir).expect("create tmp sidecar dir");
        fs::write(tmp_dir.join("meta.json.tmp"), &tmp_row.bytes).expect("write leftover tmp");

        legacy_redb::migrate_from_redb(&db_path, &data_dir).expect("first migration");
        assert_migrated_sidecars(&data_dir, &rows, rows.len());
        assert!(
            !tmp_dir.join("meta.json.tmp").exists(),
            "leftover tmp must be removed"
        );
        assert!(
            !db_path.exists(),
            "legacy db removed only after sidecars are durable"
        );

        legacy_redb::migrate_from_redb(&db_path, &data_dir).expect("repeat migration no-op");
        assert_migrated_sidecars(&data_dir, &rows, rows.len());
    }

    #[tokio::test]
    async fn test_new_accepts_cache_smaller_than_page_size() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");

        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 1024)
            .expect("manager init");

        assert_eq!(manager.page_size, 4096);
        assert_eq!(manager.max_cache_bytes, 1024);
        assert_eq!(manager.page_cache.lock().await.max_bytes(), 1024);
    }

    #[tokio::test]
    async fn test_new_accepts_zero_cache_bytes() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");

        let manager =
            SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 0).expect("manager init");

        assert_eq!(manager.max_cache_bytes, 0);
        assert_eq!(manager.page_cache.lock().await.max_bytes(), 0);
    }

    #[tokio::test]
    async fn test_manager_shares_one_global_page_cache_and_delete_drops_entries() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");
        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 8192)
            .expect("manager init");

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
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");
        let manager =
            SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4, 8).expect("manager init");

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
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");

        let result = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 0, 1024);

        assert!(matches!(result, Err(BobsError::IoError(_))));
    }

    #[tokio::test]
    async fn test_create_and_get() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");

        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
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
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");

        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
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
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");

        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
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
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");

        let key = {
            let manager1 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
                .expect("manager1 init");

            let key = uuid::Uuid::new_v4().to_string();
            manager1
                .create_spool(key.clone(), None, None, true, HashMap::new())
                .await
                .expect("create spool");
            key
        };

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
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
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");

        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
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
        assert_eq!(in_memory.checksum_crc32c, None);

        let persisted = persisted_metadata(&manager, &key).await;
        assert_eq!(persisted.total_bytes_written, 0);
        assert_eq!(persisted.total_pages, 0);
        assert_eq!(persisted.final_page_size, None);
        assert_eq!(persisted.checksum_crc32c, None);
        assert_eq!(persisted.state, SpoolState::Writing);

        spool.complete(None).await.expect("complete succeeds");

        let persisted = persisted_metadata(&manager, &key).await;
        assert_eq!(persisted.total_bytes_written, data.len() as u64);
        assert_eq!(persisted.total_pages, 3);
        assert_eq!(persisted.final_page_size, Some(123));
        assert_eq!(persisted.checksum_crc32c, Some(crc32c::crc32c(&data)));
        assert_eq!(persisted.state, SpoolState::Complete);
    }

    #[tokio::test]
    async fn test_ack_then_restart_recovers_consistent_in_progress_state_without_sidecar_update() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");
        let page_size = 4096usize;
        let data = vec![0xA5u8; page_size * 2 + 777];

        let key = {
            let manager =
                SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, page_size, 16 * 4096)
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

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, page_size, 16 * 4096)
            .expect("manager2 init");
        manager2.recover().await.expect("recover succeeds");

        let spool = manager2.get_spool(&key).expect("recovered spool exists");
        let meta = spool.metadata.lock().await.clone();
        assert_eq!(meta.state, SpoolState::Writing);
        assert_eq!(meta.total_bytes_written, data.len() as u64);
        assert_eq!(meta.total_pages, 2);
        assert_eq!(meta.final_page_size, None);
        assert_eq!(meta.checksum_crc32c, None);

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
        assert_eq!(persisted.checksum_crc32c, None);
    }

    #[tokio::test]
    async fn test_crc_equivalence_across_restart_and_append() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");
        let page_size = 4096usize;
        let first = vec![0x10u8; page_size + 321];
        let second = vec![0x20u8; page_size + 17];
        let mut all = first.clone();
        all.extend_from_slice(&second);

        let (key, crc_before_restart) = {
            let manager =
                SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, page_size, 16 * 4096)
                    .expect("manager init");
            let key = uuid::Uuid::new_v4().to_string();
            manager
                .create_spool(key.clone(), None, None, false, HashMap::new())
                .await
                .expect("create spool");
            let spool = manager.get_spool(&key).expect("spool exists");
            spool
                .write(0, bytes::Bytes::copy_from_slice(&first))
                .await
                .expect("write succeeds");
            let crc = *spool.running_crc32c.lock().await;
            assert_eq!(crc, crc32c::crc32c(&first));
            (key, crc)
        };

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, page_size, 16 * 4096)
            .expect("manager2 init");
        manager2.recover().await.expect("recover succeeds");
        let spool = manager2.get_spool(&key).expect("recovered spool exists");
        assert_eq!(*spool.running_crc32c.lock().await, crc_before_restart);

        spool
            .write(first.len() as u64, bytes::Bytes::copy_from_slice(&second))
            .await
            .expect("append after restart succeeds");
        assert_eq!(*spool.running_crc32c.lock().await, crc32c::crc32c(&all));

        spool.complete(None).await.expect("complete succeeds");
        let persisted = persisted_metadata(&manager2, &key).await;
        assert_eq!(persisted.checksum_crc32c, Some(crc32c::crc32c(&all)));
    }

    #[tokio::test]
    async fn test_complete_durability_survives_restart_with_final_metadata_and_pages() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");
        let page_size = 4096usize;
        let data = vec![0x3Cu8; page_size * 2 + 19];

        let key = {
            let manager =
                SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, page_size, 16 * 4096)
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
            assert_eq!(persisted.checksum_crc32c, Some(crc32c::crc32c(&data)));
            key
        };

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, page_size, 16 * 4096)
            .expect("manager2 init");
        manager2.recover().await.expect("recover succeeds");
        let spool = manager2.get_spool(&key).expect("recovered spool exists");

        let meta = spool.metadata.lock().await.clone();
        assert_eq!(meta.state, SpoolState::Complete);
        assert_eq!(meta.total_bytes_written, data.len() as u64);
        assert_eq!(meta.total_pages, 3);
        assert_eq!(meta.final_page_size, Some(19));
        assert_eq!(meta.checksum_crc32c, Some(crc32c::crc32c(&data)));

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
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");

        let key = {
            let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
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

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
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
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");

        let key = {
            let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
                .expect("manager init");
            let key = uuid::Uuid::new_v4().to_string();
            manager
                .create_spool(key.clone(), None, None, true, HashMap::new()) // WriteLocked
                .await
                .expect("create spool");
            key
        };

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
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
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");

        let key = {
            let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
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

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
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
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");
        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
            .expect("manager init");
        let key = uuid::Uuid::new_v4().to_string();
        let data = vec![0x44; 4096];
        let mut metadata =
            sidecar_fixture_metadata(&data_dir, &key, SpoolState::Complete, data.len() as u64);
        metadata.readable_at = None;
        write_sidecar_fixture(&manager, metadata, &data).await;

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
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
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");
        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
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
        let orphan_dir = data_dir.join("orphan-no-sidecar");
        tokio::fs::create_dir_all(&orphan_dir)
            .await
            .expect("create orphan dir");
        tokio::fs::write(orphan_dir.join("spool.dat"), b"orphan")
            .await
            .expect("write orphan data");

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
            .expect("manager2 init");
        manager2.recover().await.expect("recover succeeds");

        assert!(!data_dir.join(creating).exists());
        assert!(!data_dir.join(deleting).exists());
        assert!(!orphan_dir.exists());
        assert!(manager2.spool_keys().is_empty());
    }

    #[tokio::test]
    async fn test_recovery_discards_complete_sidecar_when_data_file_too_short() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");
        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
            .expect("manager init");
        let key = uuid::Uuid::new_v4().to_string();
        write_sidecar_fixture(
            &manager,
            sidecar_fixture_metadata(&data_dir, &key, SpoolState::Complete, 8192),
            &[0x55; 4096],
        )
        .await;

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
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
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");
        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
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

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
            .expect("manager2 init");
        manager2.recover().await.expect("recover succeeds");

        assert!(manager2.get_spool(&key).is_none());
        assert!(!data_dir.join(&key).exists());
    }

    #[tokio::test]
    async fn test_recovery_discards_corrupt_metadata_sidecar() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("legacy-metadata.db");
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

        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
            .expect("manager init");
        manager.recover().await.expect("recover succeeds");

        assert!(manager.get_spool(&key).is_none());
        assert!(!spool_dir.exists());
    }

    #[tokio::test]
    async fn test_recovery_does_not_prepopulate_page_cache() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");
        let page_size = 4096usize;
        let data = vec![0x7Bu8; page_size * 2];

        let key = {
            let manager =
                SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, page_size, 16 * page_size)
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

        let manager2 =
            SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, page_size, 16 * page_size)
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
        let db_path = dir.path().join("legacy-metadata.db");
        let data_dir = dir.path().join("data");
        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 8192)
            .expect("manager init");

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
}
