// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::Ordering;

use bytes::Bytes;

use crate::error::{BobsError, Result};
use crate::io::{read_exact_at, FileIO};
use crate::spool::{Spool, SpoolState};

impl<F, M> Spool<F, M>
where
    F: FileIO,
    M: crate::metadata::MetadataStore + Clone + Send + Sync + 'static,
{
    pub fn acquire_reader(&self) {
        self.reader_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn release_reader(&self) {
        self.reader_count.fetch_sub(1, Ordering::SeqCst);
    }

    /// Returns a completed page by index, or None if the spool is complete and
    /// no such page exists. Only returns full pages (or the final partial page
    /// after complete) — never the in-progress write buffer.
    ///
    /// Resolution order: page cache → disk → long-poll (wait for writer).
    pub async fn read_page(&self, page_idx: u64) -> Result<Option<Bytes>> {
        loop {
            if self.metadata.lock().await.state == SpoolState::Deleting {
                return Err(BobsError::SpoolNotFound {
                    key: self.key.clone(),
                });
            }
            // 1. Check in-memory page cache (recently written pages).
            {
                let cache = self.page_cache.lock().await;
                if let Some(page) = cache.get(&self.key, page_idx) {
                    self.metrics.record_cache_hit();
                    return Ok(Some(page));
                }
            }

            // 2. Page was flushed to disk but evicted from cache.
            {
                let meta = self.metadata.lock().await;
                if page_idx < meta.total_pages {
                    let is_final_partial =
                        meta.state == SpoolState::Complete && page_idx + 1 == meta.total_pages;
                    let page_len = if is_final_partial {
                        meta.final_page_size.unwrap_or(self.page_size as u64) as usize
                    } else {
                        self.page_size
                    };
                    let file_offset = page_idx * self.page_size as u64;
                    let handle_guard = self.file_handle.lock().await;
                    let Some(handle) = handle_guard.as_ref() else {
                        return Err(BobsError::WriterInactive);
                    };

                    let disk_buf = read_exact_at::<F>(
                        handle,
                        file_offset,
                        page_len,
                        "reading spool page from disk",
                    )
                    .await
                    .map_err(BobsError::IoError)?;
                    self.metrics.record_cache_miss();
                    return Ok(Some(disk_buf));
                }
            }

            // 3. No more pages to read and writer is done.
            {
                let meta = self.metadata.lock().await;
                if meta.state == SpoolState::Complete && page_idx >= meta.total_pages {
                    return Ok(None);
                }
            }

            // 4. Page doesn't exist yet — wait for the writer to complete it.
            let notified = self.notify.notified();
            tokio::select! {
                _ = notified => {
                    continue;
                }
                _ = self.cancel.cancelled() => {
                    return Err(BobsError::SpoolNotFound { key: self.key.clone() });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;

    use bytes::Bytes;
    use tempfile::tempdir;

    use super::*;
    use crate::io::{FileIO, TokioFileIO};
    use crate::metadata::{MetadataStore, SyncSidecarMetadataStore};
    use crate::spool::SpoolMetadata;

    #[derive(Clone)]
    struct ShortReadFileIO;

    struct ShortReadHandle {
        data: Bytes,
        max_chunk: usize,
    }

    impl FileIO for ShortReadFileIO {
        type Handle = ShortReadHandle;

        async fn create(_path: &Path) -> std::io::Result<Self::Handle> {
            unreachable!("short-read tests construct handles directly")
        }

        async fn open(_path: &Path) -> std::io::Result<Self::Handle> {
            unreachable!("short-read tests construct handles directly")
        }

        async fn write_at(
            _handle: &Self::Handle,
            _offset: u64,
            _data: Bytes,
        ) -> std::io::Result<usize> {
            unreachable!("short-read tests do not write through mock FileIO")
        }

        async fn read_at(handle: &Self::Handle, offset: u64, len: usize) -> std::io::Result<Bytes> {
            let start = offset as usize;
            if start >= handle.data.len() {
                return Ok(Bytes::new());
            }
            let end = handle.data.len().min(start + len.min(handle.max_chunk));
            Ok(handle.data.slice(start..end))
        }

        async fn sync_data(_handle: &Self::Handle) -> std::io::Result<()> {
            Ok(())
        }

        async fn close(_handle: Self::Handle) -> std::io::Result<()> {
            Ok(())
        }

        async fn remove(_path: &Path) -> std::io::Result<()> {
            Ok(())
        }
    }

    async fn make_short_read_spool(
        dir: &std::path::Path,
        data: Bytes,
        max_chunk: usize,
        page_size: usize,
    ) -> Spool<ShortReadFileIO> {
        let spool_dir = dir.join("short-read-key");
        tokio::fs::create_dir_all(&spool_dir)
            .await
            .expect("create spool dir");
        let path = spool_dir.join("spool.dat");
        let metadata_store = SyncSidecarMetadataStore::new(dir);
        let meta = SpoolMetadata {
            key: "short-read-key".to_string(),
            content_type: None,
            content_encoding: None,
            state: SpoolState::Writing,
            write_locked: false,
            created_at: 0,
            last_write_at: 0,
            last_read_at: None,
            readable_at: None,
            page_size: page_size as u64,
            total_bytes_written: page_size as u64,
            total_pages: 1,
            final_page_size: None,
            data_path: path,
            labels: HashMap::new(),
        };

        Spool::new(
            meta,
            ShortReadHandle { data, max_chunk },
            page_size,
            Arc::new(tokio::sync::Mutex::new(crate::spool::PageCache::new(0))),
            metadata_store,
            Arc::new(crate::metrics::BobsMetrics::new(false)),
        )
        .await
    }

    async fn make_spool(dir: &std::path::Path, page_size: usize) -> Spool<TokioFileIO> {
        make_spool_with_cache_bytes(dir, page_size, page_size * 256).await
    }

    async fn make_spool_with_cache_bytes(
        dir: &std::path::Path,
        page_size: usize,
        cache_bytes: usize,
    ) -> Spool<TokioFileIO> {
        let spool_dir = dir.join("test-key");
        tokio::fs::create_dir_all(&spool_dir)
            .await
            .expect("create spool dir");
        let path = spool_dir.join("spool.dat");
        let metadata_store = SyncSidecarMetadataStore::new(dir);
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
            page_size: page_size as u64,
            total_bytes_written: 0,
            total_pages: 0,
            final_page_size: None,
            data_path: path,
            labels: HashMap::new(),
        };

        metadata_store
            .write(&meta)
            .await
            .expect("insert initial metadata");

        Spool::new(
            meta,
            handle,
            page_size,
            Arc::new(tokio::sync::Mutex::new(crate::spool::PageCache::new(
                cache_bytes,
            ))),
            metadata_store,
            Arc::new(crate::metrics::BobsMetrics::new(false)),
        )
        .await
    }

    #[tokio::test]
    async fn test_read_cached_page() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        let data = Bytes::from(vec![0xABu8; 4096]);
        let cached_ptr = data.as_ptr();
        {
            let mut cache = spool.page_cache.lock().await;
            cache.insert(&spool.key, 0, data.clone());
        }
        {
            let mut meta = spool.metadata.lock().await;
            meta.total_pages = 1;
        }

        let got = spool.read_page(0).await.expect("read should succeed");
        let got = got.expect("cached page should exist");
        assert_eq!(got, data);
        assert_eq!(
            got.as_ptr(),
            cached_ptr,
            "cached full pages must be returned as shared Bytes, not copied"
        );
    }

    #[tokio::test]
    async fn test_read_disk_page_after_cache_eviction() {
        let dir = tempdir().expect("failed to create tempdir");
        let page_size = 4096;
        let spool = make_spool_with_cache_bytes(dir.path(), page_size, page_size).await;
        let page0 = Bytes::from(vec![0xA0u8; page_size]);
        let page1 = Bytes::from(vec![0xB1u8; page_size]);

        spool
            .write(0, page0.clone())
            .await
            .expect("first page write should succeed");
        spool
            .write(page_size as u64, page1)
            .await
            .expect("second page write should succeed");

        {
            let cache = spool.page_cache.lock().await;
            assert!(
                !cache.contains(&spool.key, 0),
                "first page should have been evicted by the byte-capped FIFO cache"
            );
            assert!(cache.contains(&spool.key, 1));
        }

        let got = spool.read_page(0).await.expect("read should succeed");
        assert_eq!(got, Some(page0), "evicted page must be read from disk");
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
            TokioFileIO::write_at(handle, 0, Bytes::copy_from_slice(&data))
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
    async fn test_read_disk_page_loops_over_short_reads_until_full_page() {
        let dir = tempdir().expect("failed to create tempdir");
        let page_size = 4096;
        let data = Bytes::from((0..page_size).map(|n| (n % 251) as u8).collect::<Vec<_>>());
        let spool = make_short_read_spool(dir.path(), data.clone(), 997, page_size).await;

        let got = spool.read_page(0).await.expect("read should succeed");
        assert_eq!(got, Some(data));
    }

    #[tokio::test]
    async fn test_read_disk_page_returns_unexpected_eof_for_short_logical_page() {
        let dir = tempdir().expect("failed to create tempdir");
        let page_size = 4096;
        let spool = make_short_read_spool(
            dir.path(),
            Bytes::from(vec![0x11u8; page_size - 17]),
            512,
            page_size,
        )
        .await;

        let err = spool.read_page(0).await.expect_err("read should fail");
        let BobsError::IoError(err) = err else {
            panic!("expected IoError, got {err:?}");
        };
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn test_read_recovered_full_page_from_disk() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        let data = vec![0x42u8; 4096];
        {
            let handle_guard = spool.file_handle.lock().await;
            let handle = handle_guard
                .as_ref()
                .expect("file handle should be active for disk read test");
            TokioFileIO::write_at(handle, 0, Bytes::copy_from_slice(&data))
                .await
                .expect("failed to write recovered full page to disk");
            TokioFileIO::sync_data(handle)
                .await
                .expect("failed to sync test data");
        }
        {
            let mut meta = spool.metadata.lock().await;
            meta.total_bytes_written = data.len() as u64;
            meta.total_pages = 1;
            meta.final_page_size = None;
            meta.state = SpoolState::Writing;
        }

        let got = spool.read_page(0).await.expect("read should succeed");
        assert_eq!(got, Some(Bytes::from(data)));
    }

    #[tokio::test]
    async fn test_recovered_trailing_partial_not_returned_before_complete() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        let partial = vec![0x55u8; 1000];
        {
            let handle_guard = spool.file_handle.lock().await;
            let handle = handle_guard
                .as_ref()
                .expect("file handle should be active for disk read test");
            TokioFileIO::write_at(handle, 0, Bytes::copy_from_slice(&partial))
                .await
                .expect("failed to write recovered partial to disk");
            TokioFileIO::sync_data(handle)
                .await
                .expect("failed to sync test data");
        }
        {
            let mut meta = spool.metadata.lock().await;
            meta.total_bytes_written = partial.len() as u64;
            meta.total_pages = 0;
            meta.final_page_size = None;
            meta.state = SpoolState::Writing;
        }

        let result =
            tokio::time::timeout(tokio::time::Duration::from_millis(50), spool.read_page(0)).await;
        assert!(
            result.is_err(),
            "recovered trailing partial must long-poll before complete"
        );
    }

    #[tokio::test]
    async fn test_recovered_trailing_partial_returned_after_complete() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        let partial = vec![0x66u8; 1000];
        {
            let handle_guard = spool.file_handle.lock().await;
            let handle = handle_guard
                .as_ref()
                .expect("file handle should be active for disk read test");
            TokioFileIO::write_at(handle, 0, Bytes::copy_from_slice(&partial))
                .await
                .expect("failed to write recovered partial to disk");
            TokioFileIO::sync_data(handle)
                .await
                .expect("failed to sync test data");
        }
        {
            let mut meta = spool.metadata.lock().await;
            meta.total_bytes_written = partial.len() as u64;
            meta.total_pages = 1;
            meta.final_page_size = Some(partial.len() as u64);
            meta.state = SpoolState::Complete;
        }

        let got = spool.read_page(0).await.expect("read should succeed");
        assert_eq!(got, Some(Bytes::from(partial)));
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
            cache.insert(&spool.key, 0, data.clone());
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

        let Err(BobsError::SpoolNotFound { key }) = result else {
            panic!("expected SpoolNotFound, got {result:?}");
        };
        assert_eq!(key, spool.key);
        assert!(!key.contains("spool.dat"));
        assert!(!key.contains(dir.path().to_string_lossy().as_ref()));
    }

    #[tokio::test]
    async fn test_partial_buffer_not_returned_until_complete() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = Arc::new(make_spool(dir.path(), 4096).await);
        let reader_spool = Arc::clone(&spool);

        {
            let mut buf = spool.write_buffer.lock().await;
            buf.extend_from_slice(&[0xABu8; 1000]);
        }
        {
            let mut meta = spool.metadata.lock().await;
            meta.total_bytes_written = 1000;
        }

        let reader = tokio::spawn(async move { reader_spool.read_page(0).await });

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        assert!(!reader.is_finished());

        let full_page = Bytes::from(vec![0xABu8; 4096]);
        {
            let mut cache = spool.page_cache.lock().await;
            cache.insert(&spool.key, 0, full_page.clone());
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
        assert_eq!(got, Some(full_page));
    }

    #[tokio::test]
    async fn test_multiple_readers_allowed() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        spool.acquire_reader();
        spool.acquire_reader();
        assert_eq!(spool.reader_count.load(Ordering::SeqCst), 2);

        spool.release_reader();
        assert_eq!(spool.reader_count.load(Ordering::SeqCst), 1);

        spool.release_reader();
        assert_eq!(spool.reader_count.load(Ordering::SeqCst), 0);
    }
}
