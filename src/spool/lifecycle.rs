use crate::error::{BobsError, Result};
use crate::io::FileIO;
use crate::spool::types::SpoolState;
use bytes::BytesMut;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use super::Spool;

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

impl<F: FileIO> Spool<F> {
    /// Finalize the spool: flush any partial page in the write buffer to disk + cache,
    /// fsync, and transition to Complete. Notifies all waiting readers so they can see
    /// the final data and detect end-of-stream.
    pub async fn complete(&self, expected_size: Option<u64>) -> Result<()> {
        {
            let meta = self.metadata.lock().await;
            if matches!(meta.state, SpoolState::Complete | SpoolState::Deleting) {
                return Ok(());
            }
        }

        // Drain the write buffer — this is the final (possibly partial) page.
        let partial_page = {
            let mut buf = self.write_buffer.lock().await;
            if buf.is_empty() {
                None
            } else {
                Some(std::mem::replace(&mut *buf, BytesMut::new()).freeze())
            }
        };

        if let Some(page_data) = partial_page {
            let partial_size = page_data.len() as u64;

            let page_idx = {
                let meta = self.metadata.lock().await;
                meta.total_pages
            };
            let file_offset = page_idx * self.page_size as u64;

            {
                let handle_guard = self.file_handle.lock().await;
                let Some(handle) = handle_guard.as_ref() else {
                    return Err(BobsError::WriterInactive);
                };
                F::write_at(handle, file_offset, &page_data)
                    .await
                    .map_err(BobsError::IoError)?;
            }

            {
                let mut cache = self.page_cache.lock().await;
                cache.insert(page_idx, page_data);
            }

            {
                let mut meta = self.metadata.lock().await;
                meta.total_pages += 1;
                meta.final_page_size = Some(partial_size);
            }
        }

        {
            let handle_guard = self.file_handle.lock().await;
            if let Some(handle) = handle_guard.as_ref() {
                F::sync_data(handle).await.map_err(BobsError::IoError)?;
            }
        }

        {
            let mut meta = self.metadata.lock().await;
            if let Some(expected) = expected_size {
                if meta.total_bytes_written != expected {
                    return Err(BobsError::SizeMismatch {
                        expected,
                        actual: meta.total_bytes_written,
                    });
                }
            }
            meta.state = SpoolState::Complete;
            meta.readable_at.get_or_insert_with(now_secs);
        }

        let (meta, total_size) = {
            let m = self.metadata.lock().await.clone();
            let sz = m.total_bytes_written;
            (m, sz)
        };
        self.persist_metadata(&meta)?;

        // Initialize coverage tracking and detect immediate full-coverage
        // (zero-byte objects or objects whose bytes were all served pre-complete).
        let became_fully_read = {
            let mut mr = self.missing_ranges.lock().await;
            mr.initialize(total_size);
            mr.is_complete()
                && self
                    .full_object_read_at
                    .compare_exchange(0, now_secs(), Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
        };
        // If every byte was already served before completion (or this is a
        // zero-byte object), the page cache is redundant — free it now.
        if became_fully_read {
            self.page_cache.lock().await.clear();
        }

        self.notify.notify_waiters();

        Ok(())
    }

    pub async fn set_write_locked(&self, locked: bool) -> Result<()> {
        let updated = {
            let mut meta = self.metadata.lock().await;
            let old_state = meta.state.clone();
            let old_locked = meta.write_locked;

            meta.write_locked = locked;
            if locked && meta.state == SpoolState::Writing {
                meta.state = SpoolState::WriteLocked;
            } else if !locked && meta.state == SpoolState::WriteLocked {
                meta.state = SpoolState::Readable;
                meta.readable_at.get_or_insert_with(now_secs);
            }

            if meta.state != old_state || meta.write_locked != old_locked {
                Some(meta.clone())
            } else {
                None
            }
        };

        if let Some(meta) = updated {
            self.persist_metadata(&meta)?;
        }
        Ok(())
    }

    pub async fn is_readable(&self) -> bool {
        let meta = self.metadata.lock().await;
        match meta.state {
            SpoolState::Writing => !meta.write_locked,
            SpoolState::Complete | SpoolState::Readable => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{FileIO, TokioFileIO};
    use crate::spool::types::SpoolMetadata;
    use std::sync::Arc;
    use tempfile::tempdir;

    async fn make_spool(dir: &std::path::Path, page_size: usize) -> Spool<TokioFileIO> {
        let path = dir.join("spool.dat");
        let db_path = dir.join("test.redb");
        let db = Arc::new(redb::Database::create(&db_path).expect("create test db"));
        {
            let write_txn = db.begin_write().expect("begin write");
            {
                let _ = write_txn
                    .open_table(crate::manager::SPOOL_TABLE)
                    .expect("open table");
            }
            write_txn.commit().expect("commit");
        }
        let handle = TokioFileIO::create(&path).await.expect("create spool file");
        let meta = SpoolMetadata {
            key: "test-key".to_string(),
            content_type: None,
            content_encoding: None,
            state: SpoolState::Writing,
            write_locked: false,
            created_at: 0,
            last_write_at: 0,
            last_read_at: None,
            readable_at: None,
            total_bytes_written: 0,
            total_pages: 0,
            final_page_size: None,
            data_path: path,
        };

        Spool::new(meta, handle, page_size, 256, db).await
    }

    #[tokio::test]
    async fn test_close_flushes_partial_page() {
        let dir = tempdir().expect("create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        let partial_data = vec![0xABu8; 1000];
        {
            let mut buf = spool.write_buffer.lock().await;
            buf.extend_from_slice(&partial_data);
        }
        {
            let mut meta = spool.metadata.lock().await;
            meta.total_bytes_written = partial_data.len() as u64;
        }

        spool.complete(None).await.expect("complete succeeds");

        let meta = spool.metadata.lock().await;
        assert_eq!(meta.state, SpoolState::Complete);
        assert_eq!(meta.total_pages, 1);
        assert_eq!(meta.final_page_size, Some(1000));
        drop(meta);

        let cache = spool.page_cache.lock().await;
        let page = cache.get(0).expect("partial page cached");
        assert_eq!(page.len(), 1000);
        assert_eq!(page.as_ref(), partial_data.as_slice());
    }

    #[tokio::test]
    async fn test_write_lock_blocks_reads_until_close() {
        let dir = tempdir().expect("create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        spool
            .set_write_locked(true)
            .await
            .expect("set write locked");
        assert!(!spool.is_readable().await);

        spool.complete(None).await.expect("complete succeeds");
        assert!(spool.is_readable().await);
    }

    #[tokio::test]
    async fn test_complete_with_wrong_expected_size() {
        let dir = tempdir().expect("create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        let data = vec![0xBBu8; 2000];
        spool.write(0, &data).await.expect("write succeeds");

        let result = spool.complete(Some(9999)).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            BobsError::SizeMismatch { expected, actual } => {
                assert_eq!(expected, 9999);
                assert_eq!(actual, 2000);
            }
            other => panic!("expected SizeMismatch, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_complete_with_correct_expected_size() {
        let dir = tempdir().expect("create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        let data = vec![0xCCu8; 500];
        spool.write(0, &data).await.expect("write succeeds");

        spool
            .complete(Some(500))
            .await
            .expect("complete with correct size should succeed");

        let meta = spool.metadata.lock().await;
        assert_eq!(meta.state, SpoolState::Complete);
    }

    #[tokio::test]
    async fn test_set_write_locked_toggle() {
        let dir = tempdir().expect("create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        // Initially Writing + unlocked
        {
            let meta = spool.metadata.lock().await;
            assert_eq!(meta.state, SpoolState::Writing);
            assert!(!meta.write_locked);
        }

        // Lock it
        spool
            .set_write_locked(true)
            .await
            .expect("set write locked");
        {
            let meta = spool.metadata.lock().await;
            assert_eq!(meta.state, SpoolState::WriteLocked);
            assert!(meta.write_locked);
        }
        assert!(!spool.is_readable().await);

        // Unlock it — this releases the spool for reading.
        spool
            .set_write_locked(false)
            .await
            .expect("release write lock");
        {
            let meta = spool.metadata.lock().await;
            assert_eq!(meta.state, SpoolState::Readable);
            assert!(!meta.write_locked);
            assert!(meta.readable_at.is_some());
        }
        assert!(spool.is_readable().await);

        let read_txn = spool.db.begin_read().expect("begin read");
        let table = read_txn
            .open_table(crate::manager::SPOOL_TABLE)
            .expect("open table");
        let entry = table
            .get("test-key")
            .expect("table get")
            .expect("metadata entry");
        let persisted: SpoolMetadata =
            serde_json::from_slice(entry.value()).expect("deserialize metadata");
        assert_eq!(persisted.state, SpoolState::Readable);
        assert!(!persisted.write_locked);
        assert!(persisted.readable_at.is_some());
    }

    #[tokio::test]
    async fn test_double_close_is_idempotent() {
        let dir = tempdir().expect("create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        spool.complete(None).await.expect("first close succeeds");
        spool.complete(None).await.expect("second close succeeds");

        let meta = spool.metadata.lock().await;
        assert_eq!(meta.state, SpoolState::Complete);
    }
}
