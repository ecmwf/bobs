use std::sync::atomic::Ordering;

use bytes::Bytes;

use crate::error::{BobsError, Result};
use crate::io::FileIO;
use crate::spool::{Spool, SpoolState};

impl<F: FileIO> Spool<F> {
    pub fn acquire_reader(&self) -> Result<()> {
        if self
            .reader_active
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(BobsError::ReaderAlreadyActive);
        }
        Ok(())
    }

    pub fn release_reader(&self) {
        self.reader_active.store(false, Ordering::SeqCst);
    }

    pub async fn read_page(&self, page_idx: u64) -> Result<Option<Bytes>> {
        loop {
            {
                let cache = self.page_cache.lock().await;
                if let Some(page) = cache.get(page_idx) {
                    return Ok(Some(page));
                }
            }

            {
                let meta = self.metadata.lock().await;
                let buf = self.write_buffer.lock().await;
                if page_idx == meta.total_pages && !buf.is_empty() {
                    return Ok(Some(Bytes::copy_from_slice(&buf)));
                }
            }

            {
                let meta = self.metadata.lock().await;
                if page_idx < meta.total_pages {
                    let file_offset = page_idx * self.page_size as u64;
                    let mut disk_buf = vec![0u8; self.page_size];
                    let handle_guard = self.file_handle.lock().await;
                    let Some(handle) = handle_guard.as_ref() else {
                        return Err(BobsError::WriterInactive);
                    };

                    let n = F::read_at(handle, file_offset, &mut disk_buf)
                        .await
                        .map_err(BobsError::IoError)?;
                    disk_buf.truncate(n);
                    return Ok(Some(Bytes::from(disk_buf)));
                }
            }

            {
                let meta = self.metadata.lock().await;
                if matches!(meta.state, SpoolState::Closed | SpoolState::Deleting)
                    && page_idx >= meta.total_pages
                {
                    return Ok(None);
                }
            }

            let notified = self.notify.notified();
            tokio::select! {
                _ = notified => {
                    continue;
                }
                _ = self.cancel.cancelled() => {
                    return Err(BobsError::SpoolNotFound { key: self.data_path.to_string_lossy().to_string() });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use tempfile::tempdir;

    use super::*;
    use crate::io::{FileIO, TokioFileIO};
    use crate::spool::SpoolMetadata;

    async fn make_spool(dir: &std::path::Path, page_size: usize) -> Spool<TokioFileIO> {
        let path = dir.join("spool.dat");
        let handle = TokioFileIO::create(&path)
            .await
            .expect("failed to create spool file");
        let meta = SpoolMetadata {
            key: "test-key".to_string(),
            bob_id: "test-bob".to_string(),
            content_type: None,
            state: SpoolState::Writing,
            write_locked: false,
            created_at: 0,
            last_write_at: 0,
            last_read_at: None,
            total_bytes_written: 0,
            total_pages: 0,
            final_page_size: None,
            data_path: path,
        };

        Spool::new(meta, handle, page_size, 256).await
    }

    #[tokio::test]
    async fn test_read_cached_page() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        let data = Bytes::from(vec![0xABu8; 4096]);
        {
            let mut cache = spool.page_cache.lock().await;
            cache.insert(0, data.clone());
        }
        {
            let mut meta = spool.metadata.lock().await;
            meta.total_pages = 1;
        }

        let got = spool.read_page(0).await.expect("read should succeed");
        assert_eq!(got, Some(data));
    }

    #[tokio::test]
    async fn test_read_disk_page() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        let data = vec![0x7Au8; 4096];
        {
            let handle_guard = spool.file_handle.lock().await;
            let handle = handle_guard
                .as_ref()
                .expect("file handle should be active for disk read test");
            TokioFileIO::write_at(handle, 0, &data)
                .await
                .expect("failed to write test data to disk");
            TokioFileIO::sync_data(handle)
                .await
                .expect("failed to sync test data");
        }
        {
            let mut meta = spool.metadata.lock().await;
            meta.total_pages = 1;
        }

        let got = spool.read_page(0).await.expect("read should succeed");
        assert_eq!(got, Some(Bytes::from(data)));
    }

    #[tokio::test]
    async fn test_long_poll_unblocks_on_notify() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = Arc::new(make_spool(dir.path(), 4096).await);
        let reader_spool = Arc::clone(&spool);

        let reader = tokio::spawn(async move { reader_spool.read_page(0).await });

        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

        let data = Bytes::from(vec![0xCDu8; 4096]);
        {
            let mut cache = spool.page_cache.lock().await;
            cache.insert(0, data.clone());
        }
        {
            let mut meta = spool.metadata.lock().await;
            meta.total_pages = 1;
        }
        spool.notify.notify_waiters();

        let got = tokio::time::timeout(tokio::time::Duration::from_secs(1), reader)
            .await
            .expect("reader task timed out")
            .expect("join should succeed")
            .expect("read should succeed");
        assert_eq!(got, Some(data));
    }

    #[tokio::test]
    async fn test_long_poll_cancelled() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = Arc::new(make_spool(dir.path(), 4096).await);
        let reader_spool = Arc::clone(&spool);

        let reader = tokio::spawn(async move { reader_spool.read_page(0).await });

        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        spool.cancel.cancel();

        let result = tokio::time::timeout(tokio::time::Duration::from_secs(1), reader)
            .await
            .expect("reader task timed out")
            .expect("join should succeed");

        assert!(result.is_err());
        assert!(matches!(result, Err(BobsError::SpoolNotFound { .. })));
    }

    #[tokio::test]
    async fn test_single_reader_enforcement() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        spool.acquire_reader().expect("first acquire should succeed");
        let second = spool.acquire_reader();
        assert!(matches!(second, Err(BobsError::ReaderAlreadyActive)));

        spool.release_reader();
        spool
            .acquire_reader()
            .expect("acquire after release should succeed");
    }
}
