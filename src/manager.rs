use crate::error::{BobsError, Result};
use crate::io::FileIO;
use crate::spool::{Spool, SpoolMetadata, SpoolState};
use dashmap::DashMap;
use redb::{Database, ReadableTable, TableDefinition};
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
    pub db: Database,
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
        page_cache_capacity: usize,
    ) -> Result<Self> {
        std::fs::create_dir_all(data_dir.as_ref()).map_err(BobsError::IoError)?;

        let db = Database::create(db_path).map_err(storage)?;
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
                table.insert(key.as_str(), payload.as_slice()).map_err(storage)?;
            }
            write_txn.commit().map_err(storage)?;
        }

        let spool = Arc::new(
            Spool::new(
                metadata,
                handle,
                self.page_size,
                self.page_cache_capacity,
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
        self.spools.iter().map(|entry| entry.key().clone()).collect()
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

        for (key, mut meta) in recovered {
            if !meta.data_path.exists() {
                stale_keys.push(key);
                continue;
            }

            let handle = match F::open(&meta.data_path).await {
                Ok(h) => h,
                Err(_) => {
                    stale_keys.push(key);
                    continue;
                }
            };

            if matches!(meta.state, SpoolState::Creating | SpoolState::Deleting) {
                meta.state = SpoolState::Closed;
            }

            let spool = Arc::new(
                Spool::new(meta, handle, self.page_size, self.page_cache_capacity).await,
            );
            self.spools.insert(key, spool);
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

        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, "bob-a".into(), 4096, 16)
            .expect("manager init");

        let key = manager
            .create_spool(Some("application/octet-stream".into()), false)
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

        let manager = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, "bob-b".into(), 4096, 16)
            .expect("manager init");

        let key = manager
            .create_spool(None, false)
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
                16,
            )
            .expect("manager1 init");

            manager1
                .create_spool(None, true)
                .await
                .expect("create spool")
        };

        let manager2 = SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, "bob-c".into(), 4096, 16)
            .expect("manager2 init");

        manager2.recover().await.expect("recover should succeed");
        let spool = manager2.get_spool(&key).expect("recovered spool exists");
        let meta = spool.metadata.lock().await;
        assert_eq!(meta.key, key);
        assert_eq!(meta.state, SpoolState::WriteLocked);
    }
}
