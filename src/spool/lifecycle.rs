use crate::error::{BobsError, Result};
use crate::io::FileIO;
use crate::metrics;
use crate::spool::types::SpoolState;
use crate::time::now_secs;
use std::sync::atomic::Ordering;

use super::Spool;

fn state_label(state: &SpoolState) -> &'static str {
    match state {
        SpoolState::Writing | SpoolState::Creating => metrics::state::WRITING,
        SpoolState::WriteLocked => metrics::state::WRITE_LOCKED,
        SpoolState::Complete | SpoolState::Deleting => metrics::state::COMPLETE,
        SpoolState::Readable => metrics::state::READABLE,
    }
}

impl<F, M> Spool<F, M>
where
    F: FileIO,
    M: crate::metadata::MetadataStore + Clone + Send + Sync + 'static,
{
    /// Finalize the spool: publish any trailing partial page already appended to
    /// disk, fsync, and transition to Complete. Notifies all waiting readers so
    /// they can see the final data and detect end-of-stream.
    pub async fn complete(&self, expected_size: Option<u64>) -> Result<()> {
        let mut buf = self.write_buffer.lock().await;

        let terminal_metadata = {
            let meta = self.metadata.lock().await;
            if matches!(meta.state, SpoolState::Complete | SpoolState::Deleting) {
                Some(meta.clone())
            } else {
                if let Some(expected) = expected_size {
                    if meta.total_bytes_written != expected {
                        return Err(BobsError::SizeMismatch {
                            expected,
                            actual: meta.total_bytes_written,
                        });
                    }
                }
                None
            }
        };

        if let Some(meta) = terminal_metadata {
            let durable = self.metadata_store.read(&self.key).await?;
            let durable_is_terminal = durable.as_ref().is_some_and(|metadata| {
                matches!(metadata.state, SpoolState::Complete | SpoolState::Deleting)
            });
            if durable_is_terminal {
                return Ok(());
            }
            self.persist_metadata(&meta).await?;
            return Ok(());
        }

        // The write buffer is the final (possibly partial) page. Spool::write has
        // already appended these bytes to spool.dat; completion only publishes
        // page metadata/cache after the data file and sidecar metadata are durable.
        let partial_page = (!buf.is_empty()).then(|| buf.clone().freeze());
        let partial_page_idx = {
            let meta = self.metadata.lock().await;
            meta.total_pages
        };

        let (candidate, total_size) = {
            let meta = self.metadata.lock().await;
            let mut candidate = meta.clone();
            if let Some(page_data) = partial_page.as_ref() {
                candidate.total_pages += 1;
                candidate.final_page_size = Some(page_data.len() as u64);
            }
            candidate.state = SpoolState::Complete;
            candidate.readable_at.get_or_insert_with(now_secs);
            let total_size = candidate.total_bytes_written;
            (candidate, total_size)
        };

        {
            let handle_guard = self.file_handle.lock().await;
            if let Some(handle) = handle_guard.as_ref() {
                F::sync_data(handle).await.map_err(BobsError::IoError)?;
            }
        }

        self.persist_metadata(&candidate).await?;

        if let Some(page_data) = partial_page {
            buf.clear();
            let mut cache = self.page_cache.lock().await;
            cache.insert(&self.key, partial_page_idx, page_data);
        }

        {
            let mut meta = self.metadata.lock().await;
            if let Some(expected) = expected_size {
                if candidate.total_bytes_written != expected {
                    return Err(BobsError::SizeMismatch {
                        expected,
                        actual: candidate.total_bytes_written,
                    });
                }
            }
            let old_state = meta.state.clone();
            *meta = candidate;
            self.metrics.record_state_transition(
                Some(state_label(&old_state)),
                crate::metrics::state::COMPLETE,
            );
        }

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
        if became_fully_read {
            self.on_fully_read().await;
        }

        self.notify.notify_waiters();

        Ok(())
    }

    /// Record that a byte range has been served and run the first full-read
    /// transition exactly once when coverage reaches the complete object.
    pub async fn mark_served_and_maybe_fully_read(&self, start: u64, end: u64, now: u64) {
        let became_fully_read = {
            let mut mr = self.missing_ranges.lock().await;
            mr.mark_served(start, end);
            mr.is_complete()
                && self.full_object_read_at.load(Ordering::Relaxed) == 0
                && self
                    .full_object_read_at
                    .compare_exchange(0, now, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
        };

        if became_fully_read {
            self.on_fully_read().await;
        }
    }

    /// First full-read transition hook. At this point every byte has been served
    /// at least once, so the first-read page cache for this spool is redundant.
    pub async fn on_fully_read(&self) {
        self.page_cache.lock().await.free_spool(&self.key);
        self.release_admission();
    }

    pub async fn set_write_locked(&self, locked: bool) -> Result<()> {
        let updated = {
            let mut meta = self.metadata.lock().await;
            let old_state = meta.state.clone();
            let old_locked = meta.write_locked;

            meta.write_locked = locked;
            if locked && meta.state == SpoolState::Writing {
                meta.state = SpoolState::WriteLocked;
                self.metrics.record_state_transition(
                    Some(crate::metrics::state::WRITING),
                    crate::metrics::state::WRITE_LOCKED,
                );
            } else if !locked && meta.state == SpoolState::WriteLocked {
                meta.state = SpoolState::Readable;
                meta.readable_at.get_or_insert_with(now_secs);
                self.metrics.record_state_transition(
                    Some(crate::metrics::state::WRITE_LOCKED),
                    crate::metrics::state::READABLE,
                );
            }

            if meta.state != old_state || meta.write_locked != old_locked {
                Some(meta.clone())
            } else {
                None
            }
        };

        if let Some(meta) = updated {
            self.persist_metadata(&meta).await?;
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
    #[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
    use crate::io::{
        ring_pool::{
            scoped_test_ring_pool_override, RingPool, RingPoolOperationKind, RingPoolOptions,
        },
        UringFileIO,
    };
    use crate::io::{FileIO, TokioFileIO};
    use crate::manager::SpoolManager;
    #[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
    use crate::metadata::UringSidecarMetadataStore;
    use crate::metadata::{MetadataStore, SyncSidecarMetadataStore};
    use crate::spool::types::SpoolMetadata;
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::fs::{self, File};
    use std::future::Future;
    use std::io::{self, Write};
    use std::path::{Path, PathBuf};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::{Arc, Mutex as StdMutex, OnceLock};
    use tempfile::tempdir;

    #[derive(Clone)]
    struct CountingFileIO;

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum CompletionEvent {
        DataFileSyncData,
        MetadataWrite,
        MetadataTmpFdatasync,
        MetadataRename,
        MetadataDirectoryFsync,
    }

    type CompletionEventLog = Arc<StdMutex<Vec<CompletionEvent>>>;

    static COUNTING_WRITE_AT_CALLS: AtomicUsize = AtomicUsize::new(0);
    static COMPLETION_EVENT_LOG: OnceLock<StdMutex<Option<CompletionEventLog>>> = OnceLock::new();

    fn completion_event_slot() -> &'static StdMutex<Option<CompletionEventLog>> {
        COMPLETION_EVENT_LOG.get_or_init(|| StdMutex::new(None))
    }

    fn set_completion_event_log(log: Option<CompletionEventLog>) {
        *completion_event_slot()
            .lock()
            .expect("completion event log mutex poisoned") = log;
    }

    fn record_completion_event(event: CompletionEvent) {
        let log = completion_event_slot()
            .lock()
            .expect("completion event log mutex poisoned")
            .clone();
        if let Some(log) = log {
            log.lock()
                .expect("completion event log mutex poisoned")
                .push(event);
        }
    }

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
    struct CompletionCountingFileIO;

    impl FileIO for CompletionCountingFileIO {
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
            handle: &Self::Handle,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            record_completion_event(CompletionEvent::DataFileSyncData);
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
    struct CountingMetadataStore {
        data_dir: PathBuf,
        events: CompletionEventLog,
    }

    impl CountingMetadataStore {
        fn new(data_dir: impl Into<PathBuf>, events: CompletionEventLog) -> Self {
            Self {
                data_dir: data_dir.into(),
                events,
            }
        }

        fn spool_dir(&self, key: &str) -> PathBuf {
            self.data_dir.join(key)
        }

        fn meta_path(&self, key: &str) -> PathBuf {
            self.spool_dir(key).join("meta.json")
        }

        fn tmp_path(&self, key: &str) -> PathBuf {
            self.spool_dir(key).join("meta.json.tmp")
        }

        fn record(&self, event: CompletionEvent) {
            self.events
                .lock()
                .expect("completion event log mutex poisoned")
                .push(event);
        }

        fn write_sync(&self, metadata: &SpoolMetadata) -> Result<()> {
            let spool_dir = self.spool_dir(&metadata.key);
            fs::create_dir_all(&spool_dir).map_err(storage_error)?;

            self.record(CompletionEvent::MetadataWrite);
            let payload = serde_json::to_vec(metadata)
                .map_err(|error| BobsError::SerializationError(error.to_string()))?;
            let tmp_path = self.tmp_path(&metadata.key);
            let meta_path = self.meta_path(&metadata.key);

            {
                let mut tmp = File::create(&tmp_path).map_err(storage_error)?;
                tmp.write_all(&payload).map_err(storage_error)?;
                self.record(CompletionEvent::MetadataTmpFdatasync);
                tmp.sync_data().map_err(storage_error)?;
            }

            self.record(CompletionEvent::MetadataRename);
            fs::rename(&tmp_path, &meta_path).map_err(storage_error)?;
            self.record(CompletionEvent::MetadataDirectoryFsync);
            File::open(&spool_dir)
                .and_then(|dir| dir.sync_all())
                .map_err(storage_error)?;
            Ok(())
        }

        fn read_sync(&self, key: &str) -> Result<Option<SpoolMetadata>> {
            match fs::read(self.meta_path(key)) {
                Ok(bytes) => serde_json::from_slice(&bytes)
                    .map(Some)
                    .map_err(|error| BobsError::SerializationError(error.to_string())),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(storage_error(error)),
            }
        }
    }

    impl MetadataStore for CountingMetadataStore {
        type WriteFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
        type ReadFuture<'a> =
            Pin<Box<dyn Future<Output = Result<Option<SpoolMetadata>>> + Send + 'a>>;
        type DeleteFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
        type ListIter = std::vec::IntoIter<Result<SpoolMetadata>>;

        fn write<'a>(&'a self, metadata: &'a SpoolMetadata) -> Self::WriteFuture<'a> {
            Box::pin(async move { self.write_sync(metadata) })
        }

        fn read<'a>(&'a self, key: &'a str) -> Self::ReadFuture<'a> {
            Box::pin(async move { self.read_sync(key) })
        }

        fn delete<'a>(&'a self, key: &'a str) -> Self::DeleteFuture<'a> {
            Box::pin(async move {
                let meta_path = self.meta_path(key);
                let tmp_path = self.tmp_path(key);
                match fs::remove_file(meta_path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(storage_error(error)),
                }
                match fs::remove_file(tmp_path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(storage_error(error)),
                }
                Ok(())
            })
        }

        fn list(&self) -> Result<Self::ListIter> {
            Ok(Vec::new().into_iter())
        }
    }

    fn storage_error(error: io::Error) -> BobsError {
        BobsError::StorageError(Box::new(error))
    }

    #[derive(Clone)]
    struct FailFirstCompleteMetadataStore {
        inner: SyncSidecarMetadataStore,
        failed_once: Arc<AtomicBool>,
    }

    impl FailFirstCompleteMetadataStore {
        fn new(data_dir: impl Into<PathBuf>) -> Self {
            Self {
                inner: SyncSidecarMetadataStore::new(data_dir),
                failed_once: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    impl MetadataStore for FailFirstCompleteMetadataStore {
        type WriteFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
        type ReadFuture<'a> =
            Pin<Box<dyn Future<Output = Result<Option<SpoolMetadata>>> + Send + 'a>>;
        type DeleteFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
        type ListIter = std::vec::IntoIter<Result<SpoolMetadata>>;

        fn write<'a>(&'a self, metadata: &'a SpoolMetadata) -> Self::WriteFuture<'a> {
            Box::pin(async move {
                if metadata.state == SpoolState::Complete
                    && !self.failed_once.swap(true, AtomicOrdering::SeqCst)
                {
                    return Err(storage_error(io::Error::other(
                        "injected complete metadata write failure",
                    )));
                }
                self.inner.write(metadata).await
            })
        }

        fn read<'a>(&'a self, key: &'a str) -> Self::ReadFuture<'a> {
            Box::pin(async move { self.inner.read(key).await })
        }

        fn delete<'a>(&'a self, key: &'a str) -> Self::DeleteFuture<'a> {
            Box::pin(async move { self.inner.delete(key).await })
        }

        fn list(&self) -> Result<Self::ListIter> {
            Ok(Vec::new().into_iter())
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

        async fn sync_data(_handle: &Self::Handle) -> std::io::Result<()> {
            Err(std::io::Error::other("injected sync failure"))
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
        let spool_dir = dir.join("test-key");
        tokio::fs::create_dir_all(&spool_dir)
            .await
            .expect("create spool dir");
        let path = spool_dir.join("spool.dat");
        let metadata_store = SyncSidecarMetadataStore::new(dir);
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
                page_size * 256,
            ))),
            metadata_store,
            Arc::new(crate::metrics::BobsMetrics::new(false, vec![], 128)),
        )
        .await
    }

    async fn persisted_metadata<F, M>(spool: &Spool<F, M>) -> SpoolMetadata
    where
        F: FileIO,
        M: MetadataStore + Clone + Send + Sync + 'static,
    {
        spool
            .metadata_store
            .read(spool.key.as_str())
            .await
            .expect("read metadata")
            .expect("metadata exists")
    }

    async fn make_sync_failing_spool(
        dir: &std::path::Path,
        page_size: usize,
    ) -> Spool<SyncFailingFileIO> {
        let spool_dir = dir.join("test-key");
        tokio::fs::create_dir_all(&spool_dir)
            .await
            .expect("create spool dir");
        let path = spool_dir.join("spool.dat");
        let metadata_store = SyncSidecarMetadataStore::new(dir);
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
                page_size * 256,
            ))),
            metadata_store,
            Arc::new(crate::metrics::BobsMetrics::new(false, vec![], 128)),
        )
        .await
    }

    async fn make_fail_first_metadata_spool(
        dir: &std::path::Path,
        page_size: usize,
    ) -> Spool<TokioFileIO, FailFirstCompleteMetadataStore> {
        let spool_dir = dir.join("test-key");
        tokio::fs::create_dir_all(&spool_dir)
            .await
            .expect("create spool dir");
        let path = spool_dir.join("spool.dat");
        let metadata_store = FailFirstCompleteMetadataStore::new(dir);
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
                page_size * 256,
            ))),
            metadata_store,
            Arc::new(crate::metrics::BobsMetrics::new(false, vec![], 128)),
        )
        .await
    }

    async fn make_counting_spool(dir: &std::path::Path, page_size: usize) -> Spool<CountingFileIO> {
        let spool_dir = dir.join("test-key");
        tokio::fs::create_dir_all(&spool_dir)
            .await
            .expect("create spool dir");
        let path = spool_dir.join("spool.dat");
        let metadata_store = SyncSidecarMetadataStore::new(dir);
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
                page_size * 256,
            ))),
            metadata_store,
            Arc::new(crate::metrics::BobsMetrics::new(false, vec![], 128)),
        )
        .await
    }

    #[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
    fn explicit_test_ring_pool_options(shard_count: usize) -> RingPoolOptions {
        RingPoolOptions {
            shard_count,
            queue_capacity: 1024,
            driver_name_prefix: "bobs-complete-routing-test".to_owned(),
        }
    }

    async fn make_counting_completion_spool(
        dir: &std::path::Path,
        page_size: usize,
        events: CompletionEventLog,
    ) -> Spool<CompletionCountingFileIO, CountingMetadataStore> {
        let spool_dir = dir.join("test-key");
        tokio::fs::create_dir_all(&spool_dir)
            .await
            .expect("create spool dir");
        let path = spool_dir.join("spool.dat");
        let metadata_store = CountingMetadataStore::new(dir, Arc::clone(&events));
        let handle = CompletionCountingFileIO::create(&path)
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
            total_pages: 0,
            final_page_size: None,
            data_path: path,
            labels: HashMap::new(),
        };
        metadata_store
            .write(&meta)
            .await
            .expect("insert initial metadata");
        events
            .lock()
            .expect("completion event log mutex poisoned")
            .clear();

        Spool::new(
            meta,
            handle,
            page_size,
            Arc::new(tokio::sync::Mutex::new(crate::spool::PageCache::new(
                page_size * 256,
            ))),
            metadata_store,
            Arc::new(crate::metrics::BobsMetrics::new(false, vec![], 128)),
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

        let before = persisted_metadata(&spool).await;
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

        let persisted = persisted_metadata(&spool).await;
        assert_eq!(persisted.state, SpoolState::Complete);
        assert_eq!(persisted.total_bytes_written, partial_data.len() as u64);
        assert_eq!(persisted.total_pages, 1);
        assert_eq!(persisted.final_page_size, Some(partial_data.len() as u64));
    }

    #[tokio::test]
    async fn complete_does_one_data_sync_and_one_metadata_commit() {
        let dir = tempdir().expect("create tempdir");
        let events = Arc::new(StdMutex::new(Vec::new()));
        let spool = make_counting_completion_spool(dir.path(), 4096, Arc::clone(&events)).await;
        set_completion_event_log(Some(Arc::clone(&events)));

        let data = vec![0xA5u8; 6000];
        spool
            .write(0, bytes::Bytes::copy_from_slice(&data))
            .await
            .expect("write succeeds");

        spool
            .complete(Some(data.len() as u64))
            .await
            .expect("complete succeeds");
        spool
            .complete(Some(data.len() as u64))
            .await
            .expect("second complete is idempotent");
        set_completion_event_log(None);

        let got = events
            .lock()
            .expect("completion event log mutex poisoned")
            .clone();
        let expected = vec![
            CompletionEvent::DataFileSyncData,
            CompletionEvent::MetadataWrite,
            CompletionEvent::MetadataTmpFdatasync,
            CompletionEvent::MetadataRename,
            CompletionEvent::MetadataDirectoryFsync,
        ];
        assert_eq!(
            got, expected,
            "completion must sync spool.dat once before exactly one durable metadata commit"
        );
        assert_eq!(
            got.iter()
                .filter(|event| matches!(event, CompletionEvent::DataFileSyncData))
                .count(),
            1,
            "complete must not issue extra data-file sync_data calls"
        );
        assert_eq!(
            got.iter()
                .filter(|event| matches!(event, CompletionEvent::MetadataWrite))
                .count(),
            1,
            "complete must issue exactly one metadata write/commit"
        );

        #[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
        {
            let routed_dir = tempdir().expect("create routed tempdir");
            let pool = Arc::new(
                RingPool::new_for_test(explicit_test_ring_pool_options(4))
                    .expect("complete routing test ring pool should start"),
            );
            let _override = scoped_test_ring_pool_override(Arc::clone(&pool));
            let metadata_store = UringSidecarMetadataStore::new(routed_dir.path());
            let key = "test-key".to_string();
            let spool_dir = routed_dir.path().join(&key);
            tokio::fs::create_dir_all(&spool_dir)
                .await
                .expect("create routed spool dir");
            let path = spool_dir.join("spool.dat");
            let handle = UringFileIO::create(&path)
                .await
                .expect("create routed spool file");
            let meta = SpoolMetadata {
                key: key.clone(),
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
                labels: HashMap::new(),
            };
            metadata_store
                .write(&meta)
                .await
                .expect("insert routed initial metadata");
            let routed_spool = Spool::<UringFileIO, UringSidecarMetadataStore>::new(
                meta,
                handle,
                4096,
                Arc::new(tokio::sync::Mutex::new(crate::spool::PageCache::new(
                    4096 * 256,
                ))),
                metadata_store,
                Arc::new(crate::metrics::BobsMetrics::new(false, vec![], 128)),
            )
            .await;
            routed_spool
                .write(0, bytes::Bytes::copy_from_slice(&data))
                .await
                .expect("routed write succeeds");

            pool.clear_routing_events();
            routed_spool
                .complete(Some(data.len() as u64))
                .await
                .expect("routed complete succeeds");
            routed_spool
                .complete(Some(data.len() as u64))
                .await
                .expect("routed second complete is idempotent");

            let complete_events: Vec<_> = pool
                .routing_events()
                .into_iter()
                .filter(|event| {
                    event.routed_key == key
                        && matches!(
                            event.operation_kind,
                            RingPoolOperationKind::DataSync | RingPoolOperationKind::MetadataCommit
                        )
                })
                .collect();
            assert_eq!(
                complete_events.len(),
                2,
                "routed complete must enqueue exactly one data sync and one metadata commit"
            );
            assert_eq!(
                complete_events[0].operation_kind,
                RingPoolOperationKind::DataSync
            );
            assert_eq!(
                complete_events[1].operation_kind,
                RingPoolOperationKind::MetadataCommit
            );
            assert_eq!(
                complete_events[0].ring_index, complete_events[1].ring_index,
                "complete data sync and metadata commit for the same spool key must use the same shard"
            );
        }
    }

    #[tokio::test]
    async fn test_complete_does_not_publish_final_partial_page_if_sync_data_fails() {
        let dir = tempdir().expect("create tempdir");
        let spool = make_sync_failing_spool(dir.path(), 4096).await;
        let partial_data = vec![0xE6u8; 333];

        spool
            .write(0, bytes::Bytes::copy_from_slice(&partial_data))
            .await
            .expect("write succeeds");

        let result = spool.complete(Some(partial_data.len() as u64)).await;
        assert!(matches!(result, Err(BobsError::IoError(_))));

        {
            let cache = spool.page_cache.lock().await;
            assert!(
                cache.get(&spool.key, 0).is_none(),
                "final partial page must not be published before sync succeeds"
            );
        }

        let meta = spool.metadata.lock().await;
        assert_eq!(meta.state, SpoolState::Writing);
        assert_eq!(meta.total_pages, 0);
        assert_eq!(meta.final_page_size, None);
        drop(meta);

        let persisted = persisted_metadata(&spool).await;
        assert_eq!(persisted.state, SpoolState::Writing);
        assert_eq!(persisted.total_pages, 0);
        assert_eq!(persisted.final_page_size, None);
    }

    #[tokio::test]
    async fn test_complete_with_partial_page_survives_restart() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let page_size = 4096;
        let key = "partial-restart".to_string();
        let data = vec![0x5Au8; page_size + 904];

        {
            let manager =
                SpoolManager::<TokioFileIO>::new(&data_dir, page_size, 16 * page_size, 256)
                    .expect("manager init");
            manager
                .create_spool(key.clone(), None, None, false, HashMap::new())
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

        let manager2 = SpoolManager::<TokioFileIO>::new(&data_dir, page_size, 16 * page_size, 256)
            .expect("manager2 init");
        manager2.recover().await.expect("recover succeeds");
        let spool = manager2.get_spool(&key).expect("recovered spool exists");

        let meta = spool.metadata.lock().await;
        assert_eq!(meta.state, SpoolState::Complete);
        assert_eq!(meta.total_bytes_written, data.len() as u64);
        assert_eq!(meta.total_pages, 2);
        assert_eq!(meta.final_page_size, Some(904));
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
    async fn test_complete_size_mismatch_preserves_partial_page_for_retry() {
        let dir = tempdir().expect("create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        let first = vec![0xBBu8; 2000];
        spool
            .write(0, bytes::Bytes::copy_from_slice(&first))
            .await
            .expect("first write succeeds");

        let result = spool.complete(Some(400)).await;
        assert!(matches!(
            result,
            Err(BobsError::SizeMismatch {
                expected: 400,
                actual: 2000
            })
        ));

        {
            let meta = spool.metadata.lock().await;
            assert_eq!(meta.state, SpoolState::Writing);
            assert_eq!(meta.total_pages, 0);
            assert_eq!(meta.final_page_size, None);
            assert_eq!(meta.total_bytes_written, first.len() as u64);
        }
        {
            let buf = spool.write_buffer.lock().await;
            assert_eq!(buf.as_ref(), first.as_slice());
        }
        {
            let cache = spool.page_cache.lock().await;
            assert!(cache.get(&spool.key, 0).is_none());
        }

        let second = vec![0xCCu8; 1234];
        spool
            .write(first.len() as u64, bytes::Bytes::copy_from_slice(&second))
            .await
            .expect("write after size mismatch succeeds");

        let mut all = first;
        all.extend_from_slice(&second);
        spool
            .complete(Some(all.len() as u64))
            .await
            .expect("complete with true size succeeds");

        let got = spool
            .read_page(0)
            .await
            .expect("read final partial page")
            .expect("page present");
        assert_eq!(got.as_ref(), all.as_slice());
    }

    #[tokio::test]
    async fn test_complete_metadata_write_failure_does_not_publish_complete_until_retry() {
        let dir = tempdir().expect("create tempdir");
        let spool = make_fail_first_metadata_spool(dir.path(), 4096).await;
        let data = vec![0xADu8; 700];

        spool
            .write(0, bytes::Bytes::copy_from_slice(&data))
            .await
            .expect("write succeeds");

        let result = spool.complete(Some(data.len() as u64)).await;
        assert!(matches!(result, Err(BobsError::StorageError(_))));

        {
            let meta = spool.metadata.lock().await;
            assert_eq!(meta.state, SpoolState::Writing);
            assert_eq!(meta.total_pages, 0);
            assert_eq!(meta.final_page_size, None);
        }
        {
            let cache = spool.page_cache.lock().await;
            assert!(cache.get(&spool.key, 0).is_none());
        }
        let persisted = persisted_metadata(&spool).await;
        assert_eq!(persisted.state, SpoolState::Writing);
        assert_eq!(persisted.total_pages, 0);
        assert_eq!(persisted.final_page_size, None);

        spool
            .complete(Some(data.len() as u64))
            .await
            .expect("retry complete succeeds");

        let persisted = persisted_metadata(&spool).await;
        assert_eq!(persisted.state, SpoolState::Complete);
        assert_eq!(persisted.total_pages, 1);
        assert_eq!(persisted.final_page_size, Some(data.len() as u64));
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

        let persisted = spool
            .metadata_store
            .read("test-key")
            .await
            .expect("read metadata")
            .expect("metadata entry");
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
