use crate::error::{BobsError, Result};
use crate::io::FileIO;
use crate::spool::{Spool, SpoolMetadata, SpoolState};
use dashmap::DashMap;
use redb::{Database, ReadableTable, TableDefinition};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

pub const SPOOL_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("spools");

fn storage<E: Into<redb::Error>>(e: E) -> BobsError {
    BobsError::StorageError(e.into())
}

pub struct SpoolManager<F: FileIO> {
    pub spools: DashMap<String, Arc<Spool<F>>>,
    pub db: Arc<Database>,
    pub data_dir: PathBuf,
    pub bob_id: String,
    pub page_size: usize,
    pub page_cache_capacity: usize,
}

impl<F: FileIO> SpoolManager<F> {
    pub fn new(
        db_path: impl AsRef<Path>,
        data_dir: impl AsRef<Path>,
        bob_id: String,
        page_size: usize,
        max_cache_bytes: usize,
    ) -> Result<Self> {
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
            bob_id,
            page_size,
            page_cache_capacity,
        })
    }

    pub async fn create_spool(
        &self,
        content_type: Option<String>,
        content_encoding: Option<String>,
        write_locked: bool,
    ) -> Result<String> {
        let key = format!("{}-{}", self.bob_id, Uuid::new_v4());
        let spool_dir = self.data_dir.join(&key);
        let data_path = spool_dir.join("spool.dat");

        tokio::fs::create_dir_all(&spool_dir)
            .await
            .map_err(BobsError::IoError)?;

        let handle = F::create(&data_path).await.map_err(BobsError::IoError)?;

        let now = now_secs();
        let metadata = SpoolMetadata {
            key: key.clone(),
            bob_id: self.bob_id.clone(),
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

        Ok(key)
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
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(BobsError::IoError(e)),
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
                    Err(_) => stale_keys.push(key),
                }
            }
        }

        let recovered_keys: HashSet<String> = recovered.iter().map(|(key, _)| key.clone()).collect();

        for (key, mut meta) in recovered {
            if !meta.data_path.exists() {
                stale_keys.push(key);
                continue;
            }

            match meta.state {
                SpoolState::Creating => {
                    stale_keys.push(key.clone());
                    let spool_dir = self.data_dir.join(&key);
                    let _ = tokio::fs::remove_dir_all(&spool_dir).await;
                    continue;
                }
                SpoolState::Deleting => {
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
                Err(_) => {
                    stale_keys.push(key);
                    continue;
                }
            };

            let mut metadata_corrected = false;
            let file_size = std::fs::metadata(&meta.data_path)
                .map(|m| m.len())
                .unwrap_or(0);

            let expected_full_pages_bytes = if meta.total_pages > 0 && meta.final_page_size.is_some() {
                (meta.total_pages - 1) * self.page_size as u64 + meta.final_page_size.unwrap()
            } else {
                meta.total_pages * self.page_size as u64
            };

            if file_size < expected_full_pages_bytes {
                let actual_full_pages = file_size / self.page_size as u64;
                tracing::warn!(
                    key = %key,
                    expected_pages = meta.total_pages,
                    actual_pages = actual_full_pages,
                    file_size = file_size,
                    "disk file shorter than metadata, correcting"
                );
                meta.total_pages = actual_full_pages;
                meta.total_bytes_written = actual_full_pages * self.page_size as u64;
                meta.final_page_size = None;
                meta.checksum_crc32c = None;
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

            let crc = if meta.state == SpoolState::Complete {
                if let Some(checksum) = meta.checksum_crc32c {
                    checksum
                } else {
                    let mut crc: u32 = 0;
                    if meta.total_pages > 0 {
                        for page_idx in 0..meta.total_pages {
                            let offset = page_idx * self.page_size as u64;
                            let read_size = if page_idx == meta.total_pages - 1 {
                                meta.final_page_size
                                    .map(|size| size as usize)
                                    .unwrap_or(self.page_size)
                            } else {
                                self.page_size
                            };
                            let mut buf = vec![0u8; read_size];
                            let n = F::read_at(&handle, offset, &mut buf)
                                .await
                                .map_err(BobsError::IoError)?;
                            buf.truncate(n);
                            crc = crc32c::crc32c_append(crc, &buf);
                        }
                    }
                    crc
                }
            } else {
                let mut crc: u32 = 0;
                if meta.total_pages > 0 {
                    for page_idx in 0..meta.total_pages {
                        let offset = page_idx * self.page_size as u64;
                        let read_size = if page_idx == meta.total_pages - 1 {
                            meta.final_page_size
                                .map(|size| size as usize)
                                .unwrap_or(self.page_size)
                        } else {
                            self.page_size
                        };
                        let mut buf = vec![0u8; read_size];
                        let n = F::read_at(&handle, offset, &mut buf)
                            .await
                            .map_err(BobsError::IoError)?;
                        buf.truncate(n);
                        crc = crc32c::crc32c_append(crc, &buf);
                    }
                }
                crc
            };

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
            self.spools.insert(key, spool);
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

        let manager =
            SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, "bob-a".into(), 4096, 16 * 4096)
                .expect("manager init");

        let key = manager
            .create_spool(Some("application/octet-stream".into()), None, false)
            .await
            .expect("create spool");
        assert!(key.starts_with("bob-a-"));

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

        let manager =
            SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, "bob-b".into(), 4096, 16 * 4096)
                .expect("manager init");

        let key = manager
            .create_spool(None, None, false)
            .await
            .expect("create spool");
        let spool_dir = data_dir.join(&key);
        assert!(spool_dir.exists());

        manager.delete_spool(&key).await.expect("delete spool");
        assert!(manager.get_spool(&key).is_none());
        assert!(!spool_dir.exists());
    }

    #[tokio::test]
    async fn test_recovery() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");

        let key = {
            let manager1 = SpoolManager::<TokioFileIO>::new(
                &db_path,
                &data_dir,
                "bob-c".into(),
                4096,
                16 * 4096,
            )
            .expect("manager1 init");

            manager1
                .create_spool(None, None, true)
                .await
                .expect("create spool")
        };

        let manager2 =
            SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, "bob-c".into(), 4096, 16 * 4096)
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

        let manager =
            SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, "bob-d".into(), 4096, 16 * 4096)
                .expect("manager init");

        let key = manager
            .create_spool(None, None, false)
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
            let manager = SpoolManager::<TokioFileIO>::new(
                &db_path,
                &data_dir,
                "bob-e".into(),
                4096,
                16 * 4096,
            )
            .expect("manager init");

            let key = manager
                .create_spool(None, None, false)
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

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, "bob-e".into(), 4096, 16 * 4096)
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
            let manager = SpoolManager::<TokioFileIO>::new(
                &db_path,
                &data_dir,
                "bob-f".into(),
                4096,
                16 * 4096,
            )
            .expect("manager init");

            let key = manager
                .create_spool(None, None, false)
                .await
                .expect("create spool");

            let spool = manager.get_spool(&key).expect("spool exists");
            spool
                .write(0, &vec![0x22u8; 4096])
                .await
                .expect("write succeeds");

            key
        };

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, "bob-f".into(), 4096, 16 * 4096)
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

        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, "bob-g".into(), 4096, 16 * 4096)
            .expect("manager init");

        let key = "bob-g-creating".to_string();
        let spool_dir = data_dir.join(&key);
        let data_path = spool_dir.join("spool.dat");
        std::fs::create_dir_all(&spool_dir).expect("create spool dir");
        std::fs::write(&data_path, vec![0x33u8; 16]).expect("create spool file");

        let meta = SpoolMetadata {
            key: key.clone(),
            bob_id: "bob-g".to_string(),
            content_type: None,
            content_encoding: None,
            state: SpoolState::Creating,
            write_locked: false,
            created_at: 0,
            last_write_at: 0,
            last_read_at: None,
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
}
