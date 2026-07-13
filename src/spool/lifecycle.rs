// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use crate::error::{BobsError, Result};
use crate::io::FileIO;
use crate::metrics;
use crate::spool::types::SpoolState;
use crate::time::now_secs;
use std::sync::Arc;

use super::Spool;

fn active_state_label(write_locked: bool) -> &'static str {
    if write_locked {
        metrics::state::WRITE_LOCKED
    } else {
        metrics::state::WRITING
    }
}

impl<F, M> Spool<F, M>
where
    F: FileIO,
    M: crate::metadata::MetadataStore + Clone + Send + Sync + 'static,
{
    /// Finalize the spool in a detached, owned transaction. Waiting on the bounded
    /// per-spool gate is cancellation-safe and does not spawn a task. Once admitted,
    /// dropping the caller cannot cancel or overlap the active completion transaction.
    pub async fn complete(self: &Arc<Self>, expected_size: Option<u64>) -> Result<()> {
        let permit = Arc::clone(&self.operation_gate)
            .acquire_owned()
            .await
            .map_err(|_| {
                BobsError::IoError(std::io::Error::other("spool operation gate closed"))
            })?;
        let spool = Arc::clone(self);
        tokio::spawn(async move { spool.complete_transaction(expected_size, permit).await })
            .await
            .map_err(|error| BobsError::StorageError(Box::new(error)))?
    }

    /// Durability order: data fdatasync -> Completing marker -> Complete commit ->
    /// in-memory Complete -> optional cache/buffer housekeeping. The marker carries
    /// the final page layout, so recovery never needs the volatile write buffer.
    async fn complete_transaction(
        &self,
        expected_size: Option<u64>,
        _permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<()> {
        // Shared mutation lock order: operation gate -> lifecycle -> write buffer.
        let _lifecycle_guard = self.lifecycle_lock.lock().await;
        let mut buf = self.write_buffer.lock().await;

        let terminal_metadata = {
            let meta = self.metadata.lock().await;
            if meta.state == SpoolState::Deleting {
                return Err(BobsError::SpoolNotFound {
                    key: self.key.clone(),
                });
            }
            if let Some(expected) = expected_size {
                if meta.total_bytes_written != expected {
                    return Err(BobsError::SizeMismatch {
                        expected,
                        actual: meta.total_bytes_written,
                    });
                }
            }
            (meta.state == SpoolState::Complete).then(|| meta.clone())
        };

        if let Some(meta) = terminal_metadata {
            let durable_is_complete = self
                .metadata_store
                .read(&self.key)
                .await?
                .is_some_and(|metadata| metadata.state == SpoolState::Complete);
            if !durable_is_complete {
                self.persist_metadata(&meta).await?;
            }
            return Ok(());
        }

        // Spool::write has already appended the final bytes to spool.dat. Retain
        // this copy until Complete is both durable and reflected in memory; it is
        // useful for retry but is never the recovery source of truth.
        let partial_page = (!buf.is_empty()).then(|| buf.clone().freeze());
        let (marker, candidate, total_size, partial_page_idx, previous_state_label) = {
            let mut meta = self.metadata.lock().await;
            if !matches!(
                meta.state,
                SpoolState::Writing | SpoolState::WriteLocked | SpoolState::Completing
            ) {
                return Err(BobsError::InvalidState {
                    current: format!("{:?}", meta.state),
                    attempted_action: "complete".to_string(),
                });
            }

            // This in-memory fail-stop boundary precedes I/O. Cancellation cannot
            // roll it back because this owned transaction outlives its caller.
            meta.state = SpoolState::Completing;
            let partial_page_idx = meta.total_pages;
            let previous_state_label = active_state_label(meta.write_locked);
            let mut marker = meta.clone();
            if let Some(page_data) = partial_page.as_ref() {
                marker.total_pages += 1;
                marker.final_page_size = Some(page_data.len() as u64);
            }
            marker.readable_at.get_or_insert_with(now_secs);
            let mut candidate = marker.clone();
            candidate.state = SpoolState::Complete;
            let total_size = candidate.total_bytes_written;
            (
                marker,
                candidate,
                total_size,
                partial_page_idx,
                previous_state_label,
            )
        };

        {
            let handle_guard = self.file_handle.lock().await;
            if let Some(handle) = handle_guard.as_ref() {
                F::sync_data(handle).await.map_err(BobsError::IoError)?;
            }
        }

        // A durable marker certifies that spool.dat was synced and records the
        // complete candidate layout. Recovery either commits this exact candidate
        // or quarantines it; it never turns Completing back into a writable state.
        self.persist_metadata(&marker).await?;
        self.persist_metadata(&candidate).await?;

        {
            let mut meta = self.metadata.lock().await;
            *meta = candidate;
            self.metrics.record_state_transition(
                Some(previous_state_label),
                crate::metrics::state::COMPLETE,
            );
        }
        self.record_readable();

        // From here the sidecar and in-memory metadata are authoritative. Cache
        // population is only an optimisation; skip it rather than wait on a busy
        // cache. Disk reads reconstruct the page exactly.
        if let Some(page_data) = partial_page {
            buf.clear();
            if let Ok(mut cache) = self.page_cache.try_lock() {
                cache.insert(&self.key, partial_page_idx, page_data);
            }
        }

        // Initialize coverage tracking and detect immediate full-coverage
        // (zero-byte objects or objects whose bytes were all served pre-complete).
        let became_fully_read = {
            let mut mr = self.missing_ranges.lock().await;
            mr.initialize(total_size);
            mr.is_complete() && self.record_fully_read()
        };
        if became_fully_read {
            self.on_fully_read().await;
        }

        self.notify.notify_waiters();

        Ok(())
    }

    /// Record byte-serving activity and coverage under the lifecycle lock used
    /// by cleanup revalidation. A stale cleanup candidate therefore orders either
    /// before this activity or after it; it cannot delete from an old snapshot.
    pub async fn mark_served_and_maybe_fully_read(&self, start: u64, end: u64) {
        let _lifecycle_guard = self.lifecycle_lock.lock().await;
        {
            let mut meta = self.metadata.lock().await;
            if meta.state == SpoolState::Deleting {
                return;
            }
            self.record_read_activity();
            let now = now_secs();
            meta.last_read_at = Some(meta.last_read_at.unwrap_or(0).max(now));
        }

        let became_fully_read = {
            let mut mr = self.missing_ranges.lock().await;
            mr.mark_served(start, end);
            mr.is_complete() && self.record_fully_read()
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

    pub async fn is_readable(&self) -> bool {
        let meta = self.metadata.lock().await;
        match meta.state {
            SpoolState::Writing => !meta.write_locked,
            SpoolState::Complete => true,
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
    use std::io::{self, Write};
    use std::path::{Path, PathBuf};
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

        fn sync_directory(
            path: &Path,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            TokioFileIO::sync_directory(path)
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

        fn sync_directory(
            path: &Path,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            TokioFileIO::sync_directory(path)
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
        async fn write(&self, metadata: &SpoolMetadata) -> Result<()> {
            let store = self.clone();
            let metadata = metadata.clone();
            tokio::task::spawn_blocking(move || store.write_sync(&metadata))
                .await
                .map_err(|error| storage_error(io::Error::other(error)))?
        }

        async fn read(&self, key: &str) -> Result<Option<SpoolMetadata>> {
            let store = self.clone();
            let key = key.to_owned();
            tokio::task::spawn_blocking(move || store.read_sync(&key))
                .await
                .map_err(|error| storage_error(io::Error::other(error)))?
        }

        async fn delete(&self, key: &str) -> Result<()> {
            let meta_path = self.meta_path(key);
            let tmp_path = self.tmp_path(key);
            tokio::task::spawn_blocking(move || {
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
            .await
            .map_err(|error| storage_error(io::Error::other(error)))?
        }

        async fn list(&self) -> Result<Vec<(String, Result<SpoolMetadata>)>> {
            Ok(Vec::new())
        }
    }

    #[derive(Clone, Copy)]
    enum MetadataGatePoint {
        BeforeMarker,
        AfterMarker,
        AfterComplete,
    }

    #[derive(Clone)]
    struct GatedMetadataStore {
        inner: SyncSidecarMetadataStore,
        point: MetadataGatePoint,
        reached: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        read_calls: Arc<AtomicUsize>,
        write_calls: Arc<AtomicUsize>,
    }

    impl GatedMetadataStore {
        fn new(
            data_dir: impl Into<PathBuf>,
            point: MetadataGatePoint,
        ) -> (Self, Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
            let reached = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            (
                Self {
                    inner: SyncSidecarMetadataStore::new(data_dir),
                    point,
                    reached: Arc::clone(&reached),
                    release: Arc::clone(&release),
                    read_calls: Arc::new(AtomicUsize::new(0)),
                    write_calls: Arc::new(AtomicUsize::new(0)),
                },
                reached,
                release,
            )
        }

        async fn gate(&self) {
            self.reached.notify_one();
            self.release.notified().await;
        }
    }

    impl MetadataStore for GatedMetadataStore {
        async fn write(&self, metadata: &SpoolMetadata) -> Result<()> {
            self.write_calls.fetch_add(1, AtomicOrdering::SeqCst);
            let gate_before = metadata.state == SpoolState::Completing
                && matches!(self.point, MetadataGatePoint::BeforeMarker);
            let gate_after = matches!(
                (metadata.state.clone(), self.point),
                (SpoolState::Completing, MetadataGatePoint::AfterMarker)
                    | (SpoolState::Complete, MetadataGatePoint::AfterComplete)
            );
            if gate_before {
                self.gate().await;
            }
            self.inner.write(metadata).await?;
            if gate_after {
                self.gate().await;
            }
            Ok(())
        }

        async fn read(&self, key: &str) -> Result<Option<SpoolMetadata>> {
            self.read_calls.fetch_add(1, AtomicOrdering::SeqCst);
            self.inner.read(key).await
        }

        async fn delete(&self, key: &str) -> Result<()> {
            self.inner.delete(key).await
        }

        async fn list(&self) -> Result<Vec<(String, Result<SpoolMetadata>)>> {
            self.inner.list().await
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
        async fn write(&self, metadata: &SpoolMetadata) -> Result<()> {
            if metadata.state == SpoolState::Complete
                && !self.failed_once.swap(true, AtomicOrdering::SeqCst)
            {
                return Err(storage_error(io::Error::other(
                    "injected complete metadata write failure",
                )));
            }
            self.inner.write(metadata).await
        }

        async fn read(&self, key: &str) -> Result<Option<SpoolMetadata>> {
            self.inner.read(key).await
        }

        async fn delete(&self, key: &str) -> Result<()> {
            self.inner.delete(key).await
        }

        async fn list(&self) -> Result<Vec<(String, Result<SpoolMetadata>)>> {
            Ok(Vec::new())
        }
    }

    #[derive(Clone)]
    struct PostRenameFailOnceMetadataStore {
        inner: SyncSidecarMetadataStore,
        data_dir: PathBuf,
        failed_once: Arc<AtomicBool>,
    }

    impl PostRenameFailOnceMetadataStore {
        fn new(data_dir: impl Into<PathBuf>) -> Self {
            let data_dir = data_dir.into();
            Self {
                inner: SyncSidecarMetadataStore::new(&data_dir),
                data_dir,
                failed_once: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    impl MetadataStore for PostRenameFailOnceMetadataStore {
        async fn write(&self, metadata: &SpoolMetadata) -> Result<()> {
            if metadata.state == SpoolState::Complete
                && !self.failed_once.swap(true, AtomicOrdering::SeqCst)
            {
                let metadata = metadata.clone();
                let data_dir = self.data_dir.clone();
                return tokio::task::spawn_blocking(move || {
                    let spool_dir = data_dir.join(&metadata.key);
                    let tmp_path = spool_dir.join("meta.json.tmp");
                    let meta_path = spool_dir.join("meta.json");
                    let payload = serde_json::to_vec(&metadata)
                        .map_err(|error| BobsError::SerializationError(error.to_string()))?;
                    {
                        let mut tmp = File::create(&tmp_path).map_err(storage_error)?;
                        tmp.write_all(&payload).map_err(storage_error)?;
                        tmp.sync_data().map_err(storage_error)?;
                    }
                    fs::rename(&tmp_path, &meta_path).map_err(storage_error)?;
                    Err(storage_error(io::Error::other(
                        "injected directory fsync failure after metadata rename",
                    )))
                })
                .await
                .map_err(|error| storage_error(io::Error::other(error)))?;
            }
            self.inner.write(metadata).await
        }

        async fn read(&self, key: &str) -> Result<Option<SpoolMetadata>> {
            self.inner.read(key).await
        }

        async fn delete(&self, key: &str) -> Result<()> {
            self.inner.delete(key).await
        }

        async fn list(&self) -> Result<Vec<(String, Result<SpoolMetadata>)>> {
            self.inner.list().await
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

        fn sync_directory(
            path: &Path,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            TokioFileIO::sync_directory(path)
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

    async fn make_spool(dir: &std::path::Path, page_size: usize) -> Arc<Spool<TokioFileIO>> {
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

        Arc::new(
            Spool::new(
                meta,
                handle,
                page_size,
                Arc::new(tokio::sync::Mutex::new(crate::spool::PageCache::new(
                    page_size * 256,
                ))),
                metadata_store,
                Arc::new(crate::metrics::BobsMetrics::new(false)),
            )
            .await,
        )
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
    ) -> Arc<Spool<SyncFailingFileIO>> {
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

        Arc::new(
            Spool::new(
                meta,
                handle,
                page_size,
                Arc::new(tokio::sync::Mutex::new(crate::spool::PageCache::new(
                    page_size * 256,
                ))),
                metadata_store,
                Arc::new(crate::metrics::BobsMetrics::new(false)),
            )
            .await,
        )
    }

    async fn make_fail_first_metadata_spool(
        dir: &std::path::Path,
        page_size: usize,
    ) -> Arc<Spool<TokioFileIO, FailFirstCompleteMetadataStore>> {
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

        Arc::new(
            Spool::new(
                meta,
                handle,
                page_size,
                Arc::new(tokio::sync::Mutex::new(crate::spool::PageCache::new(
                    page_size * 256,
                ))),
                metadata_store,
                Arc::new(crate::metrics::BobsMetrics::new(false)),
            )
            .await,
        )
    }

    async fn make_post_rename_failing_spool(
        dir: &std::path::Path,
        page_size: usize,
    ) -> Arc<Spool<TokioFileIO, PostRenameFailOnceMetadataStore>> {
        let spool_dir = dir.join("test-key");
        tokio::fs::create_dir_all(&spool_dir)
            .await
            .expect("create spool dir");
        let path = spool_dir.join("spool.dat");
        let initial_store = SyncSidecarMetadataStore::new(dir);
        let metadata_store = PostRenameFailOnceMetadataStore::new(dir);
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
            page_size: page_size as u64,
            total_bytes_written: 0,
            total_pages: 0,
            final_page_size: None,
            data_path: path,
            labels: HashMap::new(),
        };
        initial_store
            .write(&meta)
            .await
            .expect("insert initial metadata");

        Arc::new(
            Spool::new(
                meta,
                handle,
                page_size,
                Arc::new(tokio::sync::Mutex::new(crate::spool::PageCache::new(
                    page_size * 256,
                ))),
                metadata_store,
                Arc::new(crate::metrics::BobsMetrics::new(false)),
            )
            .await,
        )
    }

    async fn make_gated_spool(
        dir: &std::path::Path,
        page_size: usize,
        point: MetadataGatePoint,
    ) -> (
        Arc<Spool<TokioFileIO, GatedMetadataStore>>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    ) {
        let spool_dir = dir.join("test-key");
        tokio::fs::create_dir_all(&spool_dir)
            .await
            .expect("create spool dir");
        let path = spool_dir.join("spool.dat");
        let (metadata_store, reached, release) = GatedMetadataStore::new(dir, point);
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

        let spool = Arc::new(
            Spool::new(
                meta,
                handle,
                page_size,
                Arc::new(tokio::sync::Mutex::new(crate::spool::PageCache::new(
                    page_size * 256,
                ))),
                metadata_store,
                Arc::new(crate::metrics::BobsMetrics::new(false)),
            )
            .await,
        );
        (spool, reached, release)
    }

    async fn make_counting_spool(
        dir: &std::path::Path,
        page_size: usize,
    ) -> Arc<Spool<CountingFileIO>> {
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

        Arc::new(
            Spool::new(
                meta,
                handle,
                page_size,
                Arc::new(tokio::sync::Mutex::new(crate::spool::PageCache::new(
                    page_size * 256,
                ))),
                metadata_store,
                Arc::new(crate::metrics::BobsMetrics::new(false)),
            )
            .await,
        )
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
    ) -> Arc<Spool<CompletionCountingFileIO, CountingMetadataStore>> {
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
        events
            .lock()
            .expect("completion event log mutex poisoned")
            .clear();

        Arc::new(
            Spool::new(
                meta,
                handle,
                page_size,
                Arc::new(tokio::sync::Mutex::new(crate::spool::PageCache::new(
                    page_size * 256,
                ))),
                metadata_store,
                Arc::new(crate::metrics::BobsMetrics::new(false)),
            )
            .await,
        )
    }

    async fn wait_for_detached_completion(spool: &Arc<Spool<TokioFileIO, GatedMetadataStore>>) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let _guard = spool.lifecycle_lock.lock().await;
        })
        .await
        .expect("detached completion transaction should finish");
    }

    async fn assert_gated_completion_bytes(
        spool: &Arc<Spool<TokioFileIO, GatedMetadataStore>>,
        data: &[u8],
    ) {
        assert_eq!(spool.metadata.lock().await.state, SpoolState::Complete);
        assert!(spool.write_buffer.lock().await.is_empty());
        let durable = persisted_metadata(spool).await;
        assert_eq!(durable.state, SpoolState::Complete);
        assert_eq!(durable.total_bytes_written, data.len() as u64);
        assert_eq!(durable.total_pages, 1);
        assert_eq!(durable.final_page_size, Some(data.len() as u64));
        assert_eq!(
            tokio::fs::read(&spool.data_path)
                .await
                .expect("read completed data"),
            data
        );
        let page = spool
            .read_page(0)
            .await
            .expect("read final page")
            .expect("final page exists");
        assert_eq!(page.as_ref(), data);
        assert!(spool.read_page(1).await.expect("read exact EOF").is_none());
    }

    #[tokio::test]
    async fn caller_abort_before_and_while_marker_persistence_cannot_cancel_completion() {
        for (point, expected_durable_state) in [
            (MetadataGatePoint::BeforeMarker, SpoolState::Writing),
            (MetadataGatePoint::AfterMarker, SpoolState::Completing),
        ] {
            let dir = tempdir().expect("create tempdir");
            let (spool, reached, release) = make_gated_spool(dir.path(), 4096, point).await;
            let data = Bytes::from_static(b"detached trailing bytes");
            spool.write(0, data.clone()).await.expect("write succeeds");

            let caller_spool = Arc::clone(&spool);
            let caller =
                tokio::spawn(async move { caller_spool.complete(Some(data.len() as u64)).await });
            tokio::time::timeout(std::time::Duration::from_secs(5), reached.notified())
                .await
                .expect("completion reached metadata gate");
            caller.abort();
            assert!(caller
                .await
                .expect_err("caller should be aborted")
                .is_cancelled());
            assert_eq!(
                persisted_metadata(&spool).await.state,
                expected_durable_state
            );

            release.notify_one();
            wait_for_detached_completion(&spool).await;
            assert_gated_completion_bytes(&spool, b"detached trailing bytes").await;
        }
    }

    #[tokio::test]
    async fn completion_burst_is_backpressured_before_spawning_owned_tasks() {
        let dir = tempdir().expect("create tempdir");
        let (spool, reached, release) =
            make_gated_spool(dir.path(), 4096, MetadataGatePoint::BeforeMarker).await;
        let data = Bytes::from_static(b"single-flight-completion");
        spool.write(0, data.clone()).await.expect("write succeeds");

        let first_spool = Arc::clone(&spool);
        let expected_size = data.len() as u64;
        let first = tokio::spawn(async move { first_spool.complete(Some(expected_size)).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), reached.notified())
            .await
            .expect("first completion reached marker gate");

        const BURST: usize = 64;
        let mut waiters = Vec::with_capacity(BURST);
        for _ in 0..BURST {
            let waiter_spool = Arc::clone(&spool);
            waiters.push(tokio::spawn(async move {
                waiter_spool.complete(Some(expected_size)).await
            }));
        }
        tokio::task::yield_now().await;

        assert_eq!(spool.operation_gate.available_permits(), 0);
        assert_eq!(
            spool
                .metadata_store
                .write_calls
                .load(AtomicOrdering::SeqCst),
            2,
            "only the initial sidecar and active marker attempt may start"
        );
        assert_eq!(
            spool.metadata_store.read_calls.load(AtomicOrdering::SeqCst),
            0,
            "backpressured retries must not execute idempotency reads"
        );

        for waiter in &waiters {
            waiter.abort();
        }
        for waiter in waiters {
            assert!(waiter
                .await
                .expect_err("cancelled completion waiter should stop")
                .is_cancelled());
        }
        tokio::task::yield_now().await;
        assert_eq!(
            Arc::strong_count(&spool),
            3,
            "only main, active caller, and its one owned transaction retain the spool"
        );
        assert_eq!(
            spool
                .metadata_store
                .write_calls
                .load(AtomicOrdering::SeqCst),
            2
        );
        assert_eq!(
            spool.metadata_store.read_calls.load(AtomicOrdering::SeqCst),
            0
        );

        release.notify_one();
        first
            .await
            .expect("first completion task joins")
            .expect("first completion succeeds");
        assert_eq!(spool.operation_gate.available_permits(), 1);
        assert_eq!(
            spool
                .metadata_store
                .write_calls
                .load(AtomicOrdering::SeqCst),
            3,
            "one marker and one Complete commit follow the initial sidecar"
        );

        spool
            .complete(Some(expected_size))
            .await
            .expect("idempotent completion succeeds");
        assert_eq!(
            spool.metadata_store.read_calls.load(AtomicOrdering::SeqCst),
            1,
            "one admitted idempotent retry performs one durable-state read"
        );
    }

    #[tokio::test]
    async fn abort_after_durable_complete_survives_metadata_and_cache_contention() {
        // Durable Complete followed by metadata-lock contention: the owned task
        // retains the buffer and lifecycle lock until memory catches up.
        let metadata_dir = tempdir().expect("create metadata tempdir");
        let (metadata_spool, reached, release) =
            make_gated_spool(metadata_dir.path(), 4096, MetadataGatePoint::AfterComplete).await;
        let metadata_bytes = Bytes::from_static(b"metadata-contention-tail");
        metadata_spool
            .write(0, metadata_bytes.clone())
            .await
            .expect("write metadata contention bytes");
        let caller_spool = Arc::clone(&metadata_spool);
        let caller = tokio::spawn(async move {
            caller_spool
                .complete(Some(metadata_bytes.len() as u64))
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), reached.notified())
            .await
            .expect("Complete sidecar became durable");
        assert_eq!(
            persisted_metadata(&metadata_spool).await.state,
            SpoolState::Complete
        );
        let metadata_guard = metadata_spool.metadata.lock().await;
        caller.abort();
        assert!(caller
            .await
            .expect_err("caller should be aborted")
            .is_cancelled());
        release.notify_one();
        tokio::task::yield_now().await;
        assert!(
            metadata_spool.lifecycle_lock.try_lock().is_err(),
            "transaction must remain fail-stop while updating in-memory metadata"
        );
        drop(metadata_guard);
        wait_for_detached_completion(&metadata_spool).await;
        assert_gated_completion_bytes(&metadata_spool, b"metadata-contention-tail").await;

        // A held cache lock is irrelevant to correctness and must not delay the
        // transaction: final-page cache population is best-effort.
        let cache_dir = tempdir().expect("create cache tempdir");
        let (cache_spool, reached, release) =
            make_gated_spool(cache_dir.path(), 4096, MetadataGatePoint::AfterComplete).await;
        let cache_bytes = Bytes::from_static(b"cache-contention-tail");
        cache_spool
            .write(0, cache_bytes.clone())
            .await
            .expect("write cache contention bytes");
        let cache_guard = cache_spool.page_cache.lock().await;
        let caller_spool = Arc::clone(&cache_spool);
        let caller =
            tokio::spawn(
                async move { caller_spool.complete(Some(cache_bytes.len() as u64)).await },
            );
        tokio::time::timeout(std::time::Duration::from_secs(5), reached.notified())
            .await
            .expect("Complete sidecar became durable");
        caller.abort();
        assert!(caller
            .await
            .expect_err("caller should be aborted")
            .is_cancelled());
        release.notify_one();
        wait_for_detached_completion(&cache_spool).await;
        drop(cache_guard);
        assert_gated_completion_bytes(&cache_spool, b"cache-contention-tail").await;

        // Retry is idempotent and restart reconstructs the uncached final page
        // from spool.dat rather than from the cleared volatile buffer.
        cache_spool
            .complete(Some(b"cache-contention-tail".len() as u64))
            .await
            .expect("completion retry succeeds");
        let recovered = SpoolManager::<TokioFileIO>::new(cache_dir.path(), 4096, 16 * 4096, 8)
            .expect("recovery manager init");
        recovered.recover().await.expect("recovery succeeds");
        let recovered_spool = recovered
            .get_spool("test-key")
            .expect("completed spool recovers");
        let recovered_page = recovered_spool
            .read_page(0)
            .await
            .expect("read recovered final page")
            .expect("recovered final page exists");
        assert_eq!(recovered_page.as_ref(), b"cache-contention-tail");
        assert!(recovered_spool
            .read_page(1)
            .await
            .expect("read recovered EOF")
            .is_none());
    }

    #[tokio::test]
    async fn test_close_flushes_partial_page() {
        let dir = tempdir().expect("create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        let partial_data = vec![0xABu8; 1000];
        spool
            .write(0, Bytes::copy_from_slice(&partial_data))
            .await
            .expect("write partial page");

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
    async fn complete_orders_one_data_sync_and_two_metadata_commits() {
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
            CompletionEvent::MetadataWrite,
            CompletionEvent::MetadataTmpFdatasync,
            CompletionEvent::MetadataRename,
            CompletionEvent::MetadataDirectoryFsync,
        ];
        assert_eq!(
            got, expected,
            "completion must sync spool.dat, then commit Completing and Complete metadata"
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
            2,
            "complete must commit one recovery marker and one final sidecar"
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
                page_size: 4096,
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
            let routed_spool = Arc::new(
                Spool::<UringFileIO, UringSidecarMetadataStore>::new(
                    meta,
                    handle,
                    4096,
                    Arc::new(tokio::sync::Mutex::new(crate::spool::PageCache::new(
                        4096 * 256,
                    ))),
                    metadata_store,
                    Arc::new(crate::metrics::BobsMetrics::new(false)),
                )
                .await,
            );
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
                3,
                "routed complete must enqueue one data sync and two metadata commits"
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
                complete_events[2].operation_kind,
                RingPoolOperationKind::MetadataCommit
            );
            assert!(
                complete_events
                    .windows(2)
                    .all(|pair| pair[0].ring_index == pair[1].ring_index),
                "complete data sync and both metadata commits for one spool must use the same shard"
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
        assert_eq!(meta.state, SpoolState::Completing);
        assert_eq!(meta.total_pages, 0);
        assert_eq!(meta.final_page_size, None);
        drop(meta);
        assert!(matches!(
            spool
                .write(partial_data.len() as u64, Bytes::from_static(b"tail"))
                .await,
            Err(BobsError::InvalidState { .. })
        ));

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
            assert_eq!(meta.state, SpoolState::Completing);
            assert_eq!(meta.total_pages, 0);
            assert_eq!(meta.final_page_size, None);
        }
        assert!(matches!(
            spool
                .write(data.len() as u64, Bytes::from_static(b"tail"))
                .await,
            Err(BobsError::InvalidState { .. })
        ));
        {
            let cache = spool.page_cache.lock().await;
            assert!(cache.get(&spool.key, 0).is_none());
        }
        let persisted = persisted_metadata(&spool).await;
        assert_eq!(persisted.state, SpoolState::Completing);
        assert_eq!(persisted.total_pages, 1);
        assert_eq!(persisted.final_page_size, Some(data.len() as u64));

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
    async fn post_rename_complete_error_is_fail_stop_and_retryable_without_hidden_tail() {
        let dir = tempdir().expect("create tempdir");
        let spool = make_post_rename_failing_spool(dir.path(), 4096).await;
        let data = Bytes::from_static(b"exact acknowledged bytes");

        spool.write(0, data.clone()).await.expect("write succeeds");
        let error = spool
            .complete(Some(data.len() as u64))
            .await
            .expect_err("post-rename directory fsync error is reported");
        assert!(matches!(error, BobsError::StorageError(_)));
        assert_eq!(
            spool.metadata.lock().await.state,
            SpoolState::Completing,
            "indeterminate completion must reject subsequent writes"
        );
        assert!(matches!(
            spool
                .write(
                    data.len() as u64,
                    Bytes::from_static(b"unacknowledged-tail")
                )
                .await,
            Err(BobsError::InvalidState { .. })
        ));

        let published = persisted_metadata(&spool).await;
        assert_eq!(published.state, SpoolState::Complete);
        assert_eq!(published.total_bytes_written, data.len() as u64);
        assert_eq!(
            tokio::fs::read(dir.path().join("test-key/spool.dat"))
                .await
                .expect("read data after failed complete"),
            data.as_ref()
        );

        spool
            .complete(Some(data.len() as u64))
            .await
            .expect("same-process complete retry succeeds");
        assert_eq!(spool.metadata.lock().await.state, SpoolState::Complete);

        let recovered = SpoolManager::<TokioFileIO>::new(dir.path(), 4096, 16 * 4096, 8)
            .expect("recovery manager init");
        recovered.recover().await.expect("recovery succeeds");
        let recovered_spool = recovered
            .get_spool("test-key")
            .expect("published complete spool recovers");
        let recovered_page = recovered_spool
            .read_page(0)
            .await
            .expect("read recovered page")
            .expect("recovered page exists");
        assert_eq!(recovered_page.as_ref(), data.as_ref());
        assert!(recovered_spool
            .read_page(1)
            .await
            .expect("read recovered EOF")
            .is_none());
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
    async fn test_idempotent_complete_validates_expected_size() {
        let dir = tempdir().expect("create tempdir");
        let spool = make_spool(dir.path(), 4096).await;
        let data = bytes::Bytes::from_static(b"complete once");

        spool.write(0, data.clone()).await.expect("write succeeds");
        spool
            .complete(Some(data.len() as u64))
            .await
            .expect("first complete succeeds");

        let result = spool.complete(Some(data.len() as u64 + 1)).await;
        assert!(matches!(
            result,
            Err(BobsError::SizeMismatch {
                expected,
                actual
            }) if expected == data.len() as u64 + 1 && actual == data.len() as u64
        ));
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
