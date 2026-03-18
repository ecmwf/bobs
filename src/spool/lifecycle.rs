use crate::error::{BobsError, Result};
use crate::io::FileIO;
use crate::spool::types::SpoolState;
use bytes::BytesMut;

use super::Spool;

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

        let crc32c = *self.running_crc32c.lock().await;

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
            meta.checksum_crc32c = Some(crc32c);
        }

        {
            let mut meta = self.metadata.lock().await;
            meta.state = SpoolState::Complete;
        }

        self.notify.notify_waiters();

        Ok(())
    }

    pub async fn set_write_locked(&self, locked: bool) {
        let mut meta = self.metadata.lock().await;
        meta.write_locked = locked;

        if locked && meta.state == SpoolState::Writing {
            meta.state = SpoolState::WriteLocked;
        } else if !locked && meta.state == SpoolState::WriteLocked {
            meta.state = SpoolState::Writing;
        }
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
    use tempfile::tempdir;

    async fn make_spool(dir: &std::path::Path, page_size: usize) -> Spool<TokioFileIO> {
        let path = dir.join("spool.dat");
        let handle = TokioFileIO::create(&path).await.expect("create spool file");
        let meta = SpoolMetadata {
            key: "test-key".to_string(),
            bob_id: "test-bob".to_string(),
            content_type: None,
            content_encoding: None,
            state: SpoolState::Writing,
            write_locked: false,
            created_at: 0,
            last_write_at: 0,
            last_read_at: None,
            total_bytes_written: 0,
            checksum_crc32c: None,
            total_pages: 0,
            final_page_size: None,
            data_path: path,
        };

        Spool::new(meta, handle, page_size, 256).await
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

        spool.set_write_locked(true).await;
        assert!(!spool.is_readable().await);

        spool.complete(None).await.expect("complete succeeds");
        assert!(spool.is_readable().await);
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
