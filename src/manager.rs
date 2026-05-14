use crate::error::{BobsError, Result};
use crate::io::FileIO;
use crate::spool::{Spool, SpoolMetadata, SpoolState};
use dashmap::DashMap;
use redb::{Database, ReadableTable, TableDefinition};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub const SPOOL_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("spools");

fn storage<E: Into<redb::Error>>(e: E) -> BobsError {
    BobsError::from(e.into())
}

pub struct SpoolManager<F: FileIO> {
    pub spools: DashMap<String, Arc<Spool<F>>>,
    pub db: Arc<Database>,
    pub data_dir: PathBuf,
    pub page_size: usize,
    pub page_cache_capacity: usize,
}

impl<F: FileIO> SpoolManager<F> {
    pub fn new(
        db_path: impl AsRef<Path>,
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
        if max_cache_bytes < page_size {
            return Err(BobsError::IoError(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "max_cache_bytes must be at least page_size",
            )));
        }
        std::fs::create_dir_all(data_dir.as_ref()).map_err(BobsError::IoError)?;
        let page_cache_capacity = max_cache_bytes / page_size;

        let db = Arc::new(Database::create(db_path).map_err(storage)?);
        {
            let write_txn = db.begin_write().map_err(storage)?;
            {
                let _ = write_txn.open_table(SPOOL_TABLE).map_err(storage)?;
            }
            write_txn.commit().map_err(storage)?;
        }

        Ok(Self {
            spools: DashMap::new(),
            db,
            data_dir: data_dir.as_ref().to_path_buf(),
            page_size,
            page_cache_capacity,
        })
    }

    pub async fn create_spool(
        &self,
        key: String,
        content_type: Option<String>,
        content_encoding: Option<String>,
        write_locked: bool,
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
        };

        let payload = serde_json::to_vec(&metadata)
            .map_err(|e| BobsError::SerializationError(e.to_string()))?;

        {
            let write_txn = self.db.begin_write().map_err(storage)?;
            {
                let mut table = write_txn.open_table(SPOOL_TABLE).map_err(storage)?;
                table
                    .insert(key.as_str(), payload.as_slice())
                    .map_err(storage)?;
            }
            write_txn.commit().map_err(storage)?;
        }

        let spool = Arc::new(
            Spool::new(
                metadata,
                handle,
                self.page_size,
                self.page_cache_capacity,
                Arc::clone(&self.db),
            )
            .await,
        );
        self.spools.insert(key.clone(), spool);

        Ok(())
    }

    pub fn get_spool(&self, key: &str) -> Option<Arc<Spool<F>>> {
        self.spools.get(key).map(|entry| Arc::clone(entry.value()))
    }

    pub fn spool_keys(&self) -> Vec<String> {
        self.spools
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    pub async fn delete_spool(&self, key: &str) -> Result<()> {
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

        {
            let write_txn = self.db.begin_write().map_err(storage)?;
            {
                let mut table = write_txn.open_table(SPOOL_TABLE).map_err(storage)?;
                table.remove(key).map_err(storage)?;
            }
            write_txn.commit().map_err(storage)?;
        }

        let spool_dir = self.data_dir.join(key);
        match tokio::fs::remove_dir_all(&spool_dir).await {
            Ok(()) => {
                tracing::info!(key = %key, "spool deleted");
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!(key = %key, "spool deleted (data already gone)");
                Ok(())
            }
            Err(e) => {
                tracing::error!(key = %key, error = %e, "failed to remove spool directory");
                Err(BobsError::IoError(e))
            }
        }
    }

    pub async fn recover(&self) -> Result<()> {
        let mut recovered: Vec<(String, SpoolMetadata)> = Vec::new();
        let mut stale_keys: Vec<String> = Vec::new();

        {
            let read_txn = self.db.begin_read().map_err(storage)?;
            let table = read_txn.open_table(SPOOL_TABLE).map_err(storage)?;

            for entry in table.iter().map_err(storage)? {
                let (k, v) = entry.map_err(storage)?;
                let key = k.value().to_string();
                let raw = v.value();
                match serde_json::from_slice::<SpoolMetadata>(raw) {
                    Ok(meta) => recovered.push((key, meta)),
                    Err(e) => {
                        tracing::warn!(key = %key, error = %e, "discarding spool with corrupt metadata");
                        stale_keys.push(key);
                    }
                }
            }
        }

        let recovered_keys: HashSet<String> =
            recovered.iter().map(|(key, _)| key.clone()).collect();

        tracing::info!(
            total = recovered.len(),
            stale = stale_keys.len(),
            "recovery: scanning spools"
        );

        for (key, mut meta) in recovered {
            if !meta.data_path.exists() {
                tracing::warn!(key = %key, "recovery: data file missing, discarding");
                stale_keys.push(key);
                continue;
            }

            match meta.state {
                SpoolState::Creating => {
                    tracing::info!(key = %key, "recovery: removing incomplete spool (Creating)");
                    stale_keys.push(key.clone());
                    let spool_dir = self.data_dir.join(&key);
                    let _ = tokio::fs::remove_dir_all(&spool_dir).await;
                    continue;
                }
                SpoolState::Deleting => {
                    tracing::info!(key = %key, "recovery: removing incomplete spool (Deleting)");
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
                let payload = serde_json::to_vec(&meta)
                    .map_err(|e| BobsError::SerializationError(e.to_string()))?;
                let write_txn = self.db.begin_write().map_err(storage)?;
                {
                    let mut table = write_txn.open_table(SPOOL_TABLE).map_err(storage)?;
                    table
                        .insert(key.as_str(), payload.as_slice())
                        .map_err(storage)?;
                }
                write_txn.commit().map_err(storage)?;
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
                    self.page_cache_capacity,
                    Arc::clone(&self.db),
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

        tracing::info!(
            recovered = self.spools.len(),
            stale = stale_keys.len(),
            "recovery: spools loaded"
        );

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
                let _ = tokio::fs::remove_dir_all(entry.path()).await;
            }
        }

        if !stale_keys.is_empty() {
            let write_txn = self.db.begin_write().map_err(storage)?;
            {
                let mut table = write_txn.open_table(SPOOL_TABLE).map_err(storage)?;
                for key in stale_keys {
                    table.remove(key.as_str()).map_err(storage)?;
                }
            }
            write_txn.commit().map_err(storage)?;
        }

        Ok(())
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
    let mut buf = vec![0u8; 64 * 1024];

    while remaining > 0 {
        let want = remaining.min(buf.len() as u64) as usize;
        let n = F::read_at(handle, offset, &mut buf[..want])
            .await
            .map_err(BobsError::IoError)?;
        if n == 0 {
            return Err(BobsError::IoError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("expected {logical_size} bytes while reconstructing CRC, got {offset}"),
            )));
        }
        crc = crc32c::crc32c_append(crc, &buf[..n]);
        offset += n as u64;
        remaining -= n as u64;
    }

    Ok(crc)
}

async fn read_exact_logical_range<F: FileIO>(
    file_handle: &Arc<tokio::sync::Mutex<Option<F::Handle>>>,
    offset: u64,
    len: usize,
) -> Result<Vec<u8>> {
    let mut out = vec![0u8; len];
    let mut filled = 0usize;
    let handle_guard = file_handle.lock().await;
    let Some(handle) = handle_guard.as_ref() else {
        return Err(BobsError::WriterInactive);
    };

    while filled < len {
        let n = F::read_at(handle, offset + filled as u64, &mut out[filled..])
            .await
            .map_err(BobsError::IoError)?;
        if n == 0 {
            return Err(BobsError::IoError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("expected {len} bytes while loading trailing partial page, got {filled}"),
            )));
        }
        filled += n;
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
    use tempfile::tempdir;

    fn persisted_metadata(manager: &SpoolManager<TokioFileIO>, key: &str) -> SpoolMetadata {
        let read_txn = manager.db.begin_read().expect("begin read");
        let table = read_txn.open_table(SPOOL_TABLE).expect("open table");
        let entry = table.get(key).expect("table get").expect("entry exists");
        serde_json::from_slice(entry.value()).expect("deserialize metadata")
    }

    #[tokio::test]
    async fn test_create_and_get() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
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
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");

        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
            .expect("manager init");

        let key = uuid::Uuid::new_v4().to_string();
        manager
            .create_spool(key.clone(), None, None, false)
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
        let db_path = dir.path().join("spools.redb");
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
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");

        let key = {
            let manager1 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
                .expect("manager1 init");

            let key = uuid::Uuid::new_v4().to_string();
            manager1
                .create_spool(key.clone(), None, None, true)
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
    async fn test_write_ack_leaves_redb_offsets_stale_until_complete() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");

        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
            .expect("manager init");

        let key = uuid::Uuid::new_v4().to_string();
        manager
            .create_spool(key.clone(), None, None, false)
            .await
            .expect("create spool");

        let spool = manager.get_spool(&key).expect("spool exists");
        let data = vec![0x5Au8; 8192 + 123];
        spool.write(0, &data).await.expect("write succeeds");

        let in_memory = spool.metadata.lock().await.clone();
        assert_eq!(in_memory.total_bytes_written, data.len() as u64);
        assert_eq!(in_memory.total_pages, 2);
        assert_eq!(in_memory.checksum_crc32c, None);

        let persisted = persisted_metadata(&manager, &key);
        assert_eq!(persisted.total_bytes_written, 0);
        assert_eq!(persisted.total_pages, 0);
        assert_eq!(persisted.final_page_size, None);
        assert_eq!(persisted.checksum_crc32c, None);
        assert_eq!(persisted.state, SpoolState::Writing);

        spool.complete(None).await.expect("complete succeeds");

        let persisted = persisted_metadata(&manager, &key);
        assert_eq!(persisted.total_bytes_written, data.len() as u64);
        assert_eq!(persisted.total_pages, 3);
        assert_eq!(persisted.final_page_size, Some(123));
        assert_eq!(persisted.checksum_crc32c, Some(crc32c::crc32c(&data)));
        assert_eq!(persisted.state, SpoolState::Complete);
    }

    #[tokio::test]
    async fn test_ack_then_restart_recovers_consistent_in_progress_state_without_redb_update() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");
        let page_size = 4096usize;
        let data = vec![0xA5u8; page_size * 2 + 777];

        let key = {
            let manager =
                SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, page_size, 16 * 4096)
                    .expect("manager init");
            let key = uuid::Uuid::new_v4().to_string();
            manager
                .create_spool(key.clone(), None, None, false)
                .await
                .expect("create spool");
            let spool = manager.get_spool(&key).expect("spool exists");
            spool.write(0, &data).await.expect("write ack succeeds");

            let persisted = persisted_metadata(&manager, &key);
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

        let persisted = persisted_metadata(&manager2, &key);
        assert_eq!(persisted.total_bytes_written, 0);
        assert_eq!(persisted.total_pages, 0);
        assert_eq!(persisted.final_page_size, None);
        assert_eq!(persisted.checksum_crc32c, None);
    }

    #[tokio::test]
    async fn test_crc_equivalence_across_restart_and_append() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
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
                .create_spool(key.clone(), None, None, false)
                .await
                .expect("create spool");
            let spool = manager.get_spool(&key).expect("spool exists");
            spool.write(0, &first).await.expect("write succeeds");
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
            .write(first.len() as u64, &second)
            .await
            .expect("append after restart succeeds");
        assert_eq!(*spool.running_crc32c.lock().await, crc32c::crc32c(&all));

        spool.complete(None).await.expect("complete succeeds");
        let persisted = persisted_metadata(&manager2, &key);
        assert_eq!(persisted.checksum_crc32c, Some(crc32c::crc32c(&all)));
    }

    #[tokio::test]
    async fn test_complete_durability_survives_restart_with_final_metadata_and_pages() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");
        let page_size = 4096usize;
        let data = vec![0x3Cu8; page_size * 2 + 19];

        let key = {
            let manager =
                SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, page_size, 16 * 4096)
                    .expect("manager init");
            let key = uuid::Uuid::new_v4().to_string();
            manager
                .create_spool(key.clone(), None, None, false)
                .await
                .expect("create spool");
            let spool = manager.get_spool(&key).expect("spool exists");
            spool.write(0, &data).await.expect("write succeeds");
            spool
                .complete(Some(data.len() as u64))
                .await
                .expect("complete succeeds");

            let persisted = persisted_metadata(&manager, &key);
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
    async fn test_recovery_corrects_metadata_on_disk_mismatch() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");

        let key = {
            let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
                .expect("manager init");

            let key = uuid::Uuid::new_v4().to_string();
            manager
                .create_spool(key.clone(), None, None, false)
                .await
                .expect("create spool");

            let spool = manager.get_spool(&key).expect("spool exists");
            spool
                .write(0, &vec![0x11u8; 8192])
                .await
                .expect("write succeeds");

            let read_txn = manager.db.begin_read().expect("begin read");
            let table = read_txn.open_table(SPOOL_TABLE).expect("open table");
            let entry = table
                .get(key.as_str())
                .expect("table get")
                .expect("entry exists");
            let mut meta: SpoolMetadata =
                serde_json::from_slice(entry.value()).expect("deserialize metadata");
            drop(table);
            drop(read_txn);

            meta.total_pages = 5;
            meta.total_bytes_written = 5 * 4096;
            meta.final_page_size = None;

            let payload = serde_json::to_vec(&meta).expect("serialize metadata");
            let write_txn = manager.db.begin_write().expect("begin write");
            {
                let mut table = write_txn.open_table(SPOOL_TABLE).expect("open table");
                table
                    .insert(key.as_str(), payload.as_slice())
                    .expect("insert corrected metadata");
            }
            write_txn.commit().expect("commit");

            key
        };

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
            .expect("manager2 init");
        manager2.recover().await.expect("recover succeeds");

        let spool = manager2.get_spool(&key).expect("recovered spool exists");
        let meta = spool.metadata.lock().await;
        assert_eq!(meta.total_pages, 2);
        assert_eq!(meta.total_bytes_written, 8192);
        assert!(meta.final_page_size.is_none());
        drop(meta);

        let persisted = persisted_metadata(&manager2, &key);
        assert_eq!(persisted.total_pages, 5);
        assert_eq!(persisted.total_bytes_written, 5 * 4096);
        assert_eq!(persisted.final_page_size, None);
    }

    #[tokio::test]
    async fn test_recovery_preserves_writing_state() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");

        let key = {
            let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
                .expect("manager init");

            let key = uuid::Uuid::new_v4().to_string();
            manager
                .create_spool(key.clone(), None, None, false)
                .await
                .expect("create spool");

            let spool = manager.get_spool(&key).expect("spool exists");
            spool
                .write(0, &vec![0x22u8; 4096])
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

    #[tokio::test]
    async fn test_recovery_cleans_up_creating_state() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");

        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
            .expect("manager init");

        let key = "fake-creating-key".to_string();
        let spool_dir = data_dir.join(&key);
        let data_path = spool_dir.join("spool.dat");
        std::fs::create_dir_all(&spool_dir).expect("create spool dir");
        std::fs::write(&data_path, vec![0x33u8; 16]).expect("create spool file");

        let meta = SpoolMetadata {
            key: key.clone(),
            content_type: None,
            content_encoding: None,
            state: SpoolState::Creating,
            write_locked: false,
            created_at: 0,
            last_write_at: 0,
            last_read_at: None,
            readable_at: None,
            total_bytes_written: 0,
            checksum_crc32c: None,
            total_pages: 0,
            final_page_size: None,
            data_path,
        };
        let payload = serde_json::to_vec(&meta).expect("serialize metadata");
        let write_txn = manager.db.begin_write().expect("begin write");
        {
            let mut table = write_txn.open_table(SPOOL_TABLE).expect("open table");
            table
                .insert(key.as_str(), payload.as_slice())
                .expect("insert creating metadata");
        }
        write_txn.commit().expect("commit");

        manager.recover().await.expect("recover succeeds");

        assert!(manager.get_spool(&key).is_none());
        assert!(!spool_dir.exists());

        let read_txn = manager.db.begin_read().expect("begin read");
        let table = read_txn.open_table(SPOOL_TABLE).expect("open table");
        assert!(table.get(key.as_str()).expect("table get").is_none());
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
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");

        let key = {
            let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
                .expect("manager init");
            let key = uuid::Uuid::new_v4().to_string();
            manager
                .create_spool(key.clone(), None, None, true) // WriteLocked
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
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");

        let key = {
            let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
                .expect("manager init");
            let key = uuid::Uuid::new_v4().to_string();
            manager
                .create_spool(key.clone(), None, None, false)
                .await
                .expect("create spool");
            let spool = manager.get_spool(&key).expect("spool exists");
            spool
                .write(0, &vec![0xABu8; 8192])
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
    async fn test_recovery_sets_readable_at_for_old_metadata() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");

        // Persist metadata for a completed spool with the current readable_at field.
        let key = {
            let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
                .expect("manager init");
            let key = uuid::Uuid::new_v4().to_string();
            manager
                .create_spool(key.clone(), None, None, false)
                .await
                .expect("create spool");
            let spool = manager.get_spool(&key).expect("spool exists");
            spool
                .write(0, &vec![0xCDu8; 4096])
                .await
                .expect("write succeeds");
            spool.complete(None).await.expect("complete succeeds");
            key
        };

        // Rewrite persisted metadata to match records created before readable_at existed.
        {
            let db = Arc::new(redb::Database::create(&db_path).expect("open db"));
            let read_txn = db.begin_read().expect("begin read");
            let table = read_txn.open_table(SPOOL_TABLE).expect("open table");
            let entry = table
                .get(key.as_str())
                .expect("table get")
                .expect("entry exists");
            let mut meta: SpoolMetadata =
                serde_json::from_slice(entry.value()).expect("deserialize");
            drop(table);
            drop(read_txn);

            meta.readable_at = None; // simulate old format
            let payload = serde_json::to_vec(&meta).expect("serialize");
            let write_txn = db.begin_write().expect("begin write");
            {
                let mut table = write_txn.open_table(SPOOL_TABLE).expect("open table");
                table
                    .insert(key.as_str(), payload.as_slice())
                    .expect("insert");
            }
            write_txn.commit().expect("commit");
        }

        // Recovery backfills readable_at for both memory state and persisted metadata.
        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
            .expect("manager2 init");
        manager2.recover().await.expect("recover should succeed");

        // In-memory check.
        let spool = manager2.get_spool(&key).expect("recovered spool exists");
        let meta = spool.metadata.lock().await;
        assert!(
            meta.readable_at.is_some(),
            "readable_at must be backfilled to Some(_) during recovery"
        );
        drop(meta);

        // Persisted check: the corrected value must be written back to redb.
        let read_txn = manager2.db.begin_read().expect("begin read");
        let table = read_txn.open_table(SPOOL_TABLE).expect("open table");
        let entry = table
            .get(key.as_str())
            .expect("table get")
            .expect("entry exists");
        let persisted: SpoolMetadata =
            serde_json::from_slice(entry.value()).expect("deserialize persisted");
        assert!(
            persisted.readable_at.is_some(),
            "backfilled readable_at must be persisted to redb"
        );
    }

    #[tokio::test]
    async fn test_recovery_discards_complete_spool_when_file_shorter_than_metadata() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");

        let key = {
            let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
                .expect("manager init");

            let key = uuid::Uuid::new_v4().to_string();
            manager
                .create_spool(key.clone(), None, None, false)
                .await
                .expect("create spool");

            let spool = manager.get_spool(&key).expect("spool exists");
            spool
                .write(0, &vec![0x44u8; 5000])
                .await
                .expect("write succeeds");
            spool.complete(None).await.expect("complete succeeds");

            let read_txn = manager.db.begin_read().expect("begin read");
            let table = read_txn.open_table(SPOOL_TABLE).expect("open table");
            let entry = table
                .get(key.as_str())
                .expect("table get")
                .expect("entry exists");
            let mut meta: SpoolMetadata =
                serde_json::from_slice(entry.value()).expect("deserialize metadata");
            drop(table);
            drop(read_txn);

            meta.total_pages = 3;
            meta.total_bytes_written = 3 * 4096;
            meta.final_page_size = None;

            let payload = serde_json::to_vec(&meta).expect("serialize metadata");
            let write_txn = manager.db.begin_write().expect("begin write");
            {
                let mut table = write_txn.open_table(SPOOL_TABLE).expect("open table");
                table
                    .insert(key.as_str(), payload.as_slice())
                    .expect("insert corrected metadata");
            }
            write_txn.commit().expect("commit");

            key
        };

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096)
            .expect("manager2 init");
        manager2.recover().await.expect("recover succeeds");

        assert!(
            manager2.get_spool(&key).is_none(),
            "complete spool whose file is shorter than persisted logical size must not be recovered"
        );

        let read_txn = manager2.db.begin_read().expect("begin read");
        let table = read_txn.open_table(SPOOL_TABLE).expect("open table");
        assert!(
            table.get(key.as_str()).expect("table get").is_none(),
            "discarded corrupt spool metadata must be removed from redb"
        );
    }

    #[tokio::test]
    async fn test_recovery_reconstructs_in_progress_partial_from_disk() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");
        let page_size = 4096usize;

        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, page_size, 16 * 4096)
            .expect("manager init");
        let key = uuid::Uuid::new_v4().to_string();
        let spool_dir = data_dir.join(&key);
        let data_path = spool_dir.join("spool.dat");
        std::fs::create_dir_all(&spool_dir).expect("create spool dir");

        let mut disk_bytes = vec![0x11u8; page_size];
        let trailing = vec![0x22u8; 904];
        disk_bytes.extend_from_slice(&trailing);
        std::fs::write(&data_path, &disk_bytes).expect("write spool file");

        let meta = SpoolMetadata {
            key: key.clone(),
            content_type: None,
            content_encoding: None,
            state: SpoolState::Writing,
            write_locked: false,
            created_at: 0,
            last_write_at: 0,
            last_read_at: None,
            readable_at: None,
            total_bytes_written: 0,
            checksum_crc32c: None,
            total_pages: 0,
            final_page_size: None,
            data_path: data_path.clone(),
        };
        let payload = serde_json::to_vec(&meta).expect("serialize metadata");
        let write_txn = manager.db.begin_write().expect("begin write");
        {
            let mut table = write_txn.open_table(SPOOL_TABLE).expect("open table");
            table
                .insert(key.as_str(), payload.as_slice())
                .expect("insert metadata");
        }
        write_txn.commit().expect("commit");
        drop(manager);

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, page_size, 16 * 4096)
            .expect("manager2 init");
        manager2.recover().await.expect("recover succeeds");

        let spool = manager2.get_spool(&key).expect("recovered spool exists");
        let meta = spool.metadata.lock().await;
        assert_eq!(meta.total_bytes_written, disk_bytes.len() as u64);
        assert_eq!(meta.total_pages, 1);
        assert_eq!(meta.final_page_size, None);
        assert_eq!(meta.checksum_crc32c, None);
        drop(meta);

        let buffer = spool.write_buffer.lock().await;
        assert_eq!(buffer.len(), trailing.len());
        assert_eq!(buffer.as_ref(), trailing.as_slice());
        drop(buffer);

        let crc = *spool.running_crc32c.lock().await;
        assert_eq!(crc, crc32c::crc32c(&disk_bytes));

        let read_txn = manager2.db.begin_read().expect("begin read");
        let table = read_txn.open_table(SPOOL_TABLE).expect("open table");
        let entry = table
            .get(key.as_str())
            .expect("table get")
            .expect("entry exists");
        let persisted: SpoolMetadata =
            serde_json::from_slice(entry.value()).expect("deserialize persisted metadata");
        assert_eq!(persisted.total_bytes_written, 0);
        assert_eq!(persisted.total_pages, 0);
        assert_eq!(persisted.final_page_size, None);
        assert_eq!(persisted.last_write_at, 0);
        drop(table);
        drop(read_txn);

        let completion = vec![0x33u8; page_size - trailing.len()];
        spool
            .write(disk_bytes.len() as u64, &completion)
            .await
            .expect("write completing recovered page succeeds");

        let mut completed_page = vec![0u8; page_size];
        completed_page[..trailing.len()].copy_from_slice(&trailing);
        completed_page[trailing.len()..].copy_from_slice(&completion);
        let on_disk = std::fs::read(&data_path).expect("read spool file");
        assert_eq!(
            &on_disk[page_size..page_size * 2],
            completed_page.as_slice()
        );
    }
}
