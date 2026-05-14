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
    /// Finalize the spool: publish any trailing partial page already appended to
    /// disk, fsync, and transition to Complete. Notifies all waiting readers so
    /// they can see the final data and detect end-of-stream.
    pub async fn complete(&self, expected_size: Option<u64>) -> Result<()> {
        {
            let meta = self.metadata.lock().await;
            if matches!(meta.state, SpoolState::Complete | SpoolState::Deleting) {
                return Ok(());
            }
        }

        // Drain the write buffer — this is the final (possibly partial) page.
        // Spool::write has already appended these bytes to spool.dat; completion
        // only publishes page metadata/cache so readers can observe the partial
        // final page. Do not rewrite the page here: the disk append is the source
        // of persistence.
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
                let mut meta = self.metadata.lock().await;
                let page_idx = meta.total_pages;
                meta.total_pages += 1;
                meta.final_page_size = Some(partial_size);
                page_idx
            };

            {
                let mut cache = self.page_cache.lock().await;
                cache.insert(&self.key, page_idx, page_data);
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
        {
            let mut mr = self.missing_ranges.lock().await;
            mr.initialize(total_size);
            if mr.is_complete() {
                self.full_object_read_at
                    .compare_exchange(0, now_secs(), Ordering::SeqCst, Ordering::SeqCst)
                    .ok();
            }
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
    use crate::manager::SpoolManager;
    use crate::spool::types::SpoolMetadata;
    use bytes::Bytes;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;
    use tempfile::tempdir;

    #[derive(Clone)]
    struct CountingFileIO;

    static COUNTING_WRITE_AT_CALLS: AtomicUsize = AtomicUsize::new(0);

    impl FileIO for CountingFileIO {
        type Handle = <TokioFileIO as FileIO>::Handle;

        fn create(
            path: &Path,
        ) -> impl std::future::Future<Output = std::io::Result<Self::Handle>> + Send {
            TokioFileIO::create(path)
        }

        fn open(
            path: &Path,
        ) -> impl std::future::Future<Output = std::io::Result<Self::Handle>> + Send {
            TokioFileIO::open(path)
        }

        fn write_at(
            handle: &Self::Handle,
            offset: u64,
            data: Bytes,
        ) -> impl std::future::Future<Output = std::io::Result<usize>> + Send {
            COUNTING_WRITE_AT_CALLS.fetch_add(1, AtomicOrdering::SeqCst);
            TokioFileIO::write_at(handle, offset, data)
        }

        fn read_at(
            handle: &Self::Handle,
            offset: u64,
            len: usize,
        ) -> impl std::future::Future<Output = std::io::Result<Bytes>> + Send {
            TokioFileIO::read_at(handle, offset, len)
        }

        fn sync_data(
            handle: &Self::Handle,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            TokioFileIO::sync_data(handle)
        }

        fn close(
            handle: Self::Handle,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            TokioFileIO::close(handle)
        }

        fn remove(path: &Path) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            TokioFileIO::remove(path)
        }
    }

    #[derive(Clone)]
    struct SyncFailingFileIO;

    impl FileIO for SyncFailingFileIO {
        type Handle = <TokioFileIO as FileIO>::Handle;

        fn create(
            path: &Path,
        ) -> impl std::future::Future<Output = std::io::Result<Self::Handle>> + Send {
            TokioFileIO::create(path)
        }

        fn open(
            path: &Path,
        ) -> impl std::future::Future<Output = std::io::Result<Self::Handle>> + Send {
            TokioFileIO::open(path)
        }

        fn write_at(
            handle: &Self::Handle,
            offset: u64,
            data: Bytes,
        ) -> impl std::future::Future<Output = std::io::Result<usize>> + Send {
            TokioFileIO::write_at(handle, offset, data)
        }

        fn read_at(
            handle: &Self::Handle,
            offset: u64,
            len: usize,
        ) -> impl std::future::Future<Output = std::io::Result<Bytes>> + Send {
            TokioFileIO::read_at(handle, offset, len)
        }

        fn sync_data(
            _handle: &Self::Handle,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            async move {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "injected sync failure",
                ))
            }
        }

        fn close(
            handle: Self::Handle,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            TokioFileIO::close(handle)
        }

        fn remove(path: &Path) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            TokioFileIO::remove(path)
        }
    }

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

        Spool::new(
            meta,
            handle,
            page_size,
            Arc::new(tokio::sync::Mutex::new(crate::spool::PageCache::new(
                page_size * 256,
            ))),
            db,
        )
        .await
    }

    fn persisted_metadata<F: FileIO>(spool: &Spool<F>) -> SpoolMetadata {
        let read_txn = spool.db.begin_read().expect("begin read");
        let table = read_txn
            .open_table(crate::manager::SPOOL_TABLE)
            .expect("open table");
        let raw = table
            .get(spool.key.as_str())
            .expect("read metadata")
            .expect("metadata exists")
            .value()
            .to_vec();
        serde_json::from_slice(&raw).expect("deserialize metadata")
    }

    async fn make_sync_failing_spool(
        dir: &std::path::Path,
        page_size: usize,
    ) -> Spool<SyncFailingFileIO> {
        let path = dir.join("spool.dat");
        let db_path = dir.join("test-sync-failing.redb");
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
        let handle = SyncFailingFileIO::create(&path)
            .await
            .expect("create spool file");
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

        Spool::new(
            meta,
            handle,
            page_size,
            Arc::new(tokio::sync::Mutex::new(crate::spool::PageCache::new(
                page_size * 256,
            ))),
            db,
        )
        .await
    }

    async fn make_counting_spool(dir: &std::path::Path, page_size: usize) -> Spool<CountingFileIO> {
        let path = dir.join("spool.dat");
        let db_path = dir.join("test-counting.redb");
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
        let handle = CountingFileIO::create(&path)
            .await
            .expect("create spool file");
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

        Spool::new(
            meta,
            handle,
            page_size,
            Arc::new(tokio::sync::Mutex::new(crate::spool::PageCache::new(
                page_size * 256,
            ))),
            db,
        )
        .await
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
        let page = cache.get(&spool.key, 0).expect("partial page cached");
        assert_eq!(page.len(), 1000);
        assert_eq!(page.as_ref(), partial_data.as_slice());
    }

    #[tokio::test]
    async fn test_complete_publishes_partial_page_without_rewriting_it() {
        let dir = tempdir().expect("create tempdir");
        let spool = make_counting_spool(dir.path(), 4096).await;

        COUNTING_WRITE_AT_CALLS.store(0, AtomicOrdering::SeqCst);
        let partial_data = vec![0xABu8; 1000];
        spool
            .write(0, bytes::Bytes::copy_from_slice(&partial_data))
            .await
            .expect("write succeeds");
        assert_eq!(
            COUNTING_WRITE_AT_CALLS.load(AtomicOrdering::SeqCst),
            1,
            "initial write should append trailing partial bytes to disk"
        );

        spool.complete(None).await.expect("complete succeeds");

        assert_eq!(
            COUNTING_WRITE_AT_CALLS.load(AtomicOrdering::SeqCst),
            1,
            "complete must not rewrite the trailing partial page"
        );

        let meta = spool.metadata.lock().await;
        assert_eq!(meta.state, SpoolState::Complete);
        assert_eq!(meta.total_pages, 1);
        assert_eq!(meta.final_page_size, Some(1000));
        drop(meta);

        let got = spool.read_page(0).await.expect("read partial final page");
        assert_eq!(got.as_deref(), Some(partial_data.as_slice()));
    }

    #[tokio::test]
    async fn test_complete_syncs_then_persists_final_partial_metadata() {
        let dir = tempdir().expect("create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        let partial_data = vec![0xD5u8; 777];
        spool
            .write(0, bytes::Bytes::copy_from_slice(&partial_data))
            .await
            .expect("write succeeds");

        let before = persisted_metadata(&spool);
        assert_eq!(before.state, SpoolState::Writing);
        assert_eq!(before.total_pages, 0);
        assert_eq!(before.final_page_size, None);

        spool
            .complete(Some(partial_data.len() as u64))
            .await
            .expect("complete succeeds");

        let cached = {
            let cache = spool.page_cache.lock().await;
            cache.get(&spool.key, 0).expect("final partial page cached")
        };
        assert_eq!(cached.as_ref(), partial_data.as_slice());

        let persisted = persisted_metadata(&spool);
        assert_eq!(persisted.state, SpoolState::Complete);
        assert_eq!(persisted.total_bytes_written, partial_data.len() as u64);
        assert_eq!(persisted.total_pages, 1);
        assert_eq!(persisted.final_page_size, Some(partial_data.len() as u64));
        assert_eq!(
            persisted.checksum_crc32c,
            Some(crc32c::crc32c(&partial_data))
        );
    }

    #[tokio::test]
    async fn test_complete_does_not_persist_final_metadata_if_sync_data_fails() {
        let dir = tempdir().expect("create tempdir");
        let spool = make_sync_failing_spool(dir.path(), 4096).await;
        let partial_data = vec![0xE6u8; 333];

        spool
            .write(0, bytes::Bytes::copy_from_slice(&partial_data))
            .await
            .expect("write succeeds");

        let result = spool.complete(Some(partial_data.len() as u64)).await;
        assert!(matches!(result, Err(BobsError::IoError(_))));

        let cached = {
            let cache = spool.page_cache.lock().await;
            cache
                .get(&spool.key, 0)
                .expect("final partial page is published before sync")
        };
        assert_eq!(cached.as_ref(), partial_data.as_slice());

        let persisted = persisted_metadata(&spool);
        assert_eq!(persisted.state, SpoolState::Writing);
        assert_eq!(persisted.total_pages, 0);
        assert_eq!(persisted.final_page_size, None);
        assert_eq!(persisted.checksum_crc32c, None);
    }

    #[tokio::test]
    async fn test_complete_with_partial_page_survives_restart() {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");
        let page_size = 4096;
        let key = "partial-restart".to_string();
        let data = vec![0x5Au8; page_size + 904];

        {
            let manager =
                SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, page_size, 16 * page_size)
                    .expect("manager init");
            manager
                .create_spool(key.clone(), None, None, false)
                .await
                .expect("create spool");
            let spool = manager.get_spool(&key).expect("spool exists");
            spool
                .write(0, bytes::Bytes::copy_from_slice(&data))
                .await
                .expect("write succeeds");
            spool
                .complete(Some(data.len() as u64))
                .await
                .expect("complete succeeds");
        }

        let manager2 =
            SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, page_size, 16 * page_size)
                .expect("manager2 init");
        manager2.recover().await.expect("recover succeeds");
        let spool = manager2.get_spool(&key).expect("recovered spool exists");

        let meta = spool.metadata.lock().await;
        assert_eq!(meta.state, SpoolState::Complete);
        assert_eq!(meta.total_bytes_written, data.len() as u64);
        assert_eq!(meta.total_pages, 2);
        assert_eq!(meta.final_page_size, Some(904));
        assert_eq!(meta.checksum_crc32c, Some(crc32c::crc32c(&data)));
        drop(meta);

        let first = spool
            .read_page(0)
            .await
            .expect("read first page")
            .expect("first page present");
        let final_partial = spool
            .read_page(1)
            .await
            .expect("read final page")
            .expect("final page present");
        let end = spool.read_page(2).await.expect("read end marker");
        assert!(end.is_none());

        let mut read_back = Vec::new();
        read_back.extend_from_slice(&first);
        read_back.extend_from_slice(&final_partial);
        assert_eq!(read_back, data);
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
        spool
            .write(0, bytes::Bytes::copy_from_slice(&data))
            .await
            .expect("write succeeds");

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
        spool
            .write(0, bytes::Bytes::copy_from_slice(&data))
            .await
            .expect("write succeeds");

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
