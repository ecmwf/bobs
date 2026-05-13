use crate::error::{BobsError, Result};
use crate::io::FileIO;
use crate::spool::{Spool, SpoolState};

impl<F: FileIO> Spool<F> {
    /// Append data at the given offset. Writes are strictly sequential — the offset
    /// must match total_bytes_written exactly. Data accumulates in the write buffer
    /// and is flushed to disk + cache whenever a full page is ready. Each completed
    /// page notifies waiting readers.
    pub async fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        let mut buf = self.write_buffer.lock().await;

        {
            let meta = self.metadata.lock().await;
            match meta.state {
                SpoolState::Writing | SpoolState::WriteLocked => {}
                SpoolState::Complete => return Err(BobsError::SpoolClosed),
                ref other => {
                    return Err(BobsError::InvalidState {
                        current: format!("{other:?}"),
                        attempted_action: "write".to_string(),
                    });
                }
            }

            // Enforce sequential appends — no gaps or overwrites.
            if offset != meta.total_bytes_written {
                return Err(BobsError::OffsetMismatch {
                    expected: meta.total_bytes_written,
                    got: offset,
                });
            }
        }

        if data.is_empty() {
            return Ok(());
        }

        {
            let mut crc = self.running_crc32c.lock().await;
            *crc = crc32c::crc32c_append(*crc, data);
        }

        buf.extend_from_slice(data);

        // Flush complete pages: write to disk, cache, and notify readers.
        // Partial remainder stays in the buffer until more data arrives (or complete).
        while buf.len() >= self.page_size {
            let page_bytes = buf.split_to(self.page_size).freeze();

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
                F::write_at(handle, file_offset, &page_bytes)
                    .await
                    .map_err(BobsError::IoError)?;
            }

            {
                let mut cache = self.page_cache.lock().await;
                cache.insert(page_idx, page_bytes);
            }

            {
                let mut meta = self.metadata.lock().await;
                meta.total_pages += 1;
            }

            self.notify.notify_waiters();
        }

        {
            let mut meta = self.metadata.lock().await;
            meta.total_bytes_written = meta.total_pages * self.page_size as u64 + buf.len() as u64;
            meta.last_write_at = now_secs();
        }

        let meta = self.metadata.lock().await.clone();
        self.persist_metadata(&meta)?;

        Ok(())
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{FileIO, TokioFileIO};
    use crate::spool::SpoolMetadata;
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
        let handle = TokioFileIO::create(&path)
            .await
            .expect("failed to create spool file");
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
            checksum_crc32c: None,
            total_pages: 0,
            final_page_size: None,
            data_path: path,
        };

        Spool::new(meta, handle, page_size, 256, db).await
    }

    #[tokio::test]
    async fn test_write_single_page() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;
        let data = vec![0xABu8; 4096];

        spool.write(0, &data).await.expect("write should succeed");

        let meta = spool.metadata.lock().await;
        assert_eq!(meta.total_pages, 1);
        assert_eq!(meta.total_bytes_written, 4096);
        drop(meta);

        let cache = spool.page_cache.lock().await;
        assert!(cache.contains(0));
    }

    #[tokio::test]
    async fn test_write_multiple_pages() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;
        let data = vec![0xCDu8; 16384];

        spool.write(0, &data).await.expect("write should succeed");

        let meta = spool.metadata.lock().await;
        assert_eq!(meta.total_pages, 4);
        assert_eq!(meta.total_bytes_written, 16384);
    }

    #[tokio::test]
    async fn test_write_partial_page() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;
        let data = vec![0xEFu8; 1000];

        spool.write(0, &data).await.expect("write should succeed");

        let meta = spool.metadata.lock().await;
        assert_eq!(meta.total_pages, 0);
        assert_eq!(meta.total_bytes_written, 1000);
    }

    #[tokio::test]
    async fn test_write_offset_mismatch() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        let result = spool.write(100, &[0u8; 100]).await;
        assert!(matches!(
            result,
            Err(BobsError::OffsetMismatch {
                expected: 0,
                got: 100
            })
        ));
    }

    #[tokio::test]
    async fn test_write_empty_is_noop() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        spool
            .write(0, &[])
            .await
            .expect("empty write should succeed");

        let meta = spool.metadata.lock().await;
        assert_eq!(meta.total_pages, 0);
        assert_eq!(meta.total_bytes_written, 0);
    }

    #[tokio::test]
    async fn test_write_when_file_handle_none() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        {
            let mut handle = spool.file_handle.lock().await;
            *handle = None;
        }

        let data = vec![0xFFu8; 4096];
        let result = spool.write(0, &data).await;
        assert!(matches!(result, Err(BobsError::WriterInactive)));
    }

    #[tokio::test]
    async fn test_write_after_close_fails() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        {
            let mut meta = spool.metadata.lock().await;
            meta.state = SpoolState::Complete;
        }

        let result = spool.write(0, &[1, 2, 3]).await;
        assert!(matches!(result, Err(BobsError::SpoolClosed)));
    }
}
