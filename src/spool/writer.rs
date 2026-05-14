use crate::error::{BobsError, Result};
use crate::io::FileIO;
use crate::spool::{Spool, SpoolState};

impl<F: FileIO> Spool<F> {
    /// Append data at the given offset. Writes are strictly sequential — the offset
    /// must match total_bytes_written exactly. Every accepted non-empty body is
    /// appended to spool.dat before any in-memory state advances. Data also
    /// accumulates in the write buffer and full pages are published to the cache;
    /// each completed page notifies waiting readers.
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
            let handle_guard = self.file_handle.lock().await;
            let Some(handle) = handle_guard.as_ref() else {
                return Err(BobsError::WriterInactive);
            };
            let written = F::write_at(handle, offset, data)
                .await
                .map_err(BobsError::IoError)?;
            if written != data.len() {
                return Err(BobsError::IoError(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    format!("short write: wrote {written} of {} bytes", data.len()),
                )));
            }
        }

        {
            let mut crc = self.running_crc32c.lock().await;
            *crc = crc32c::crc32c_append(*crc, data);
        }

        buf.extend_from_slice(data);

        // Publish complete pages to the cache and notify readers. The bytes have
        // already been appended to disk at the caller's requested offset above.
        // Partial remainder stays in the buffer until more data arrives (or complete).
        while buf.len() >= self.page_size {
            let page_bytes = buf.split_to(self.page_size).freeze();

            let page_idx = {
                let meta = self.metadata.lock().await;
                meta.total_pages
            };

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
            meta.total_bytes_written = offset + data.len() as u64;
            meta.last_write_at = now_secs();
        }

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
        let payload = serde_json::to_vec(&meta).expect("serialize initial metadata");
        {
            let write_txn = db.begin_write().expect("begin write");
            {
                let mut table = write_txn
                    .open_table(crate::manager::SPOOL_TABLE)
                    .expect("open table");
                table
                    .insert(meta.key.as_str(), payload.as_slice())
                    .expect("insert initial metadata");
            }
            write_txn.commit().expect("commit");
        }

        Spool::new(meta, handle, page_size, 256, db).await
    }

    fn persisted_metadata(spool: &Spool<TokioFileIO>) -> SpoolMetadata {
        let read_txn = spool.db.begin_read().expect("begin read");
        let table = read_txn
            .open_table(crate::manager::SPOOL_TABLE)
            .expect("open table");
        let raw = table
            .get("test-key")
            .expect("read metadata")
            .expect("metadata exists")
            .value()
            .to_vec();
        serde_json::from_slice(&raw).expect("deserialize metadata")
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
    async fn test_write_persists_partial_without_metadata_then_publishes_completed_page() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;
        let first = vec![0x11u8; 1000];
        let second = vec![0x22u8; 3096];

        spool
            .write(0, &first)
            .await
            .expect("partial write should succeed");

        let file_len = tokio::fs::metadata(&spool.data_path)
            .await
            .expect("stat spool data file")
            .len();
        assert_eq!(file_len, first.len() as u64);

        let persisted = persisted_metadata(&spool);
        assert_eq!(persisted.total_bytes_written, 0);
        assert_eq!(persisted.total_pages, 0);
        assert_eq!(persisted.last_write_at, 0);

        spool
            .write(first.len() as u64, &second)
            .await
            .expect("remainder write should succeed");

        let mut expected = first;
        expected.extend_from_slice(&second);
        let got = spool.read_page(0).await.expect("page read should succeed");
        assert_eq!(got, Some(bytes::Bytes::from(expected)));
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
