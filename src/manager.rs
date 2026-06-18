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
use tokio::sync::Semaphore;

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
    /// Admission gate: bounds the number of spools concurrently holding an
    /// in-memory page cache. `create_spool` awaits a permit, so writers block
    /// (backpressure) when the limit is reached rather than growing memory
    /// without bound.
    pub admission: Arc<Semaphore>,
}

impl<F: FileIO> SpoolManager<F> {
    pub fn new(
        db_path: impl AsRef<Path>,
        data_dir: impl AsRef<Path>,
        page_size: usize,
        max_cache_bytes: usize,
        max_live_spools: usize,
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
        if max_live_spools == 0 {
            return Err(BobsError::IoError(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "max_live_spools must be greater than 0",
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
            admission: Arc::new(Semaphore::new(max_live_spools)),
        })
    }

    pub async fn create_spool(
        &self,
        key: String,
        content_type: Option<String>,
        content_encoding: Option<String>,
        write_locked: bool,
    ) -> Result<()> {
        // Admission backpressure: block until a slot frees rather than letting
        // in-memory spools (and their page caches) grow without bound. The
        // permit is moved into the Spool and released when the object is fully
        // read or the spool is deleted.
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
            total_bytes_written: 0,
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
                Some(permit),
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

            let expected_full_pages_bytes = match meta.final_page_size {
                Some(final_page_size) if meta.total_pages > 0 => {
                    (meta.total_pages - 1) * self.page_size as u64 + final_page_size
                }
                _ => meta.total_pages * self.page_size as u64,
            };

            // Backfill readable_at for spools that were persisted before this
            // field existed. Grants a full idle-TTL grace period after upgrade.
            if matches!(meta.state, SpoolState::Complete | SpoolState::Readable)
                && meta.readable_at.is_none()
            {
                meta.readable_at = Some(now_secs());
                metadata_corrected = true;
            }

            if file_size < expected_full_pages_bytes {
                let actual_full_pages = file_size / self.page_size as u64;
                let trailing_bytes = file_size % self.page_size as u64;
                tracing::warn!(
                    key = %key,
                    expected_pages = meta.total_pages,
                    actual_pages = actual_full_pages,
                    file_size = file_size,
                    "disk file shorter than metadata, correcting"
                );
                meta.total_pages = actual_full_pages + u64::from(trailing_bytes > 0);
                meta.total_bytes_written = file_size;
                meta.final_page_size = if trailing_bytes > 0 {
                    Some(trailing_bytes)
                } else {
                    None
                };
                metadata_corrected = true;
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

            // Capture fields needed for post-init before meta is moved.
            let meta_state_for_init = meta.state.clone();
            let meta_total_bytes_for_init = meta.total_bytes_written;

            // Recovered spools occupy memory too; take a permit if available.
            // Over-limit recoveries (rare) load without one and drain via TTL.
            let permit = Arc::clone(&self.admission).try_acquire_owned().ok();
            let spool = Arc::new(
                Spool::new(
                    meta,
                    handle,
                    self.page_size,
                    self.page_cache_capacity,
                    Arc::clone(&self.db),
                    permit,
                )
                .await,
            );

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

    #[tokio::test]
    async fn test_create_and_get() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");

        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096, 256)
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

        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096, 256)
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
    async fn test_admission_blocks_at_capacity_and_releases_on_delete() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");
        // Admission limit of 1: only one live spool permitted at a time.
        let manager = Arc::new(
            SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096, 1)
                .expect("manager init"),
        );

        manager
            .create_spool("a".into(), None, None, false)
            .await
            .expect("first create succeeds");

        // Second create must block while at capacity.
        let m2 = Arc::clone(&manager);
        let mut create2 =
            tokio::spawn(async move { m2.create_spool("b".into(), None, None, false).await });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(
            !create2.is_finished(),
            "second create must block while admission is at capacity"
        );

        // Freeing a slot (delete) must unblock the waiting create.
        manager.delete_spool("a").await.expect("delete first");
        tokio::time::timeout(std::time::Duration::from_secs(5), &mut create2)
            .await
            .expect("second create should unblock once a slot frees")
            .expect("join")
            .expect("second create succeeds");
        assert!(manager.get_spool("b").is_some());
    }

    #[tokio::test]
    async fn test_release_admission_frees_a_slot_before_delete() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");
        let manager = Arc::new(
            SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096, 1)
                .expect("manager init"),
        );

        manager
            .create_spool("a".into(), None, None, false)
            .await
            .expect("first create succeeds");
        // Simulate the full-read transition releasing the permit while the spool
        // remains live (cache freed, slot returned).
        manager.get_spool("a").unwrap().release_admission();

        // A second create now proceeds without blocking even though "a" is alive.
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            manager.create_spool("b".into(), None, None, false),
        )
        .await
        .expect("create must not block after admission released")
        .expect("second create succeeds");
        assert!(manager.get_spool("a").is_some(), "a remains live");
        assert!(manager.get_spool("b").is_some());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_key() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");

        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096, 256)
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
            let manager1 =
                SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096, 256)
                    .expect("manager1 init");

            let key = uuid::Uuid::new_v4().to_string();
            manager1
                .create_spool(key.clone(), None, None, true)
                .await
                .expect("create spool");
            key
        };

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096, 256)
            .expect("manager2 init");

        manager2.recover().await.expect("recover should succeed");
        let spool = manager2.get_spool(&key).expect("recovered spool exists");
        let meta = spool.metadata.lock().await;
        assert_eq!(meta.key, key);
        assert_eq!(meta.state, SpoolState::WriteLocked);
    }

    #[tokio::test]
    async fn test_metadata_persists_total_pages_after_write() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");

        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096, 256)
            .expect("manager init");

        let key = uuid::Uuid::new_v4().to_string();
        manager
            .create_spool(key.clone(), None, None, false)
            .await
            .expect("create spool");

        let spool = manager.get_spool(&key).expect("spool exists");
        let data = vec![0x5Au8; 8192];
        spool.write(0, &data).await.expect("write succeeds");

        let read_txn = manager.db.begin_read().expect("begin read");
        let table = read_txn.open_table(SPOOL_TABLE).expect("open table");
        let entry = table
            .get(key.as_str())
            .expect("table get")
            .expect("entry exists");
        let raw = entry.value();

        let persisted: SpoolMetadata = serde_json::from_slice(raw).expect("deserialize metadata");
        assert_eq!(persisted.total_pages, 2);
    }

    #[tokio::test]
    async fn test_recovery_corrects_metadata_on_disk_mismatch() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");

        let key = {
            let manager =
                SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096, 256)
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

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096, 256)
            .expect("manager2 init");
        manager2.recover().await.expect("recover succeeds");

        let spool = manager2.get_spool(&key).expect("recovered spool exists");
        let meta = spool.metadata.lock().await;
        assert_eq!(meta.total_pages, 2);
        assert_eq!(meta.total_bytes_written, 8192);
        assert!(meta.final_page_size.is_none());
    }

    #[tokio::test]
    async fn test_recovery_preserves_writing_state() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");

        let key = {
            let manager =
                SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096, 256)
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

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096, 256)
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

        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096, 256)
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
            let manager =
                SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096, 256)
                    .expect("manager init");
            let key = uuid::Uuid::new_v4().to_string();
            manager
                .create_spool(key.clone(), None, None, true) // WriteLocked
                .await
                .expect("create spool");
            key
        };

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096, 256)
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
            let manager =
                SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096, 256)
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

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096, 256)
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

        // Step 1: create and complete a spool normally (sets readable_at).
        let key = {
            let manager =
                SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096, 256)
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

        // Step 2: overwrite the persisted metadata with readable_at = None,
        // simulating an old metadata record written before this field existed.
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

        // Step 3: recover and verify readable_at is backfilled.
        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096, 256)
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
    async fn test_recovery_preserves_partial_final_page() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");

        let key = {
            let manager =
                SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096, 256)
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

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 16 * 4096, 256)
            .expect("manager2 init");
        manager2.recover().await.expect("recover succeeds");

        let spool = manager2.get_spool(&key).expect("recovered spool exists");
        let meta = spool.metadata.lock().await;
        assert_eq!(meta.total_pages, 2);
        assert_eq!(meta.total_bytes_written, 5000);
        assert_eq!(meta.final_page_size, Some(904));
    }
}
