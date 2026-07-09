// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use crate::io::FileIO;
use crate::metadata::{MetadataStore, SyncSidecarMetadataStore};
use crate::metrics::BobsMetrics;
use bytes::BytesMut;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit};
use tokio_util::sync::CancellationToken;

pub mod coverage;
pub mod lifecycle;
pub mod page_cache;
pub mod reader;
pub mod types;
pub mod writer;

pub use coverage::MissingRanges;

/// A spool buffers a single streaming response: one writer appends pages sequentially,
/// multiple readers can consume byte ranges in parallel. Pages are flushed to disk when
/// full (page_size bytes) and cached in memory for fast reads. The writer signals readers
/// via `notify` after each completed page; readers long-poll until data is available.
pub struct Spool<F: FileIO, M: MetadataStore = SyncSidecarMetadataStore> {
    pub key: String,
    pub metadata: Arc<Mutex<SpoolMetadata>>,
    pub page_cache: Arc<Mutex<PageCache>>,
    /// Accumulates incoming bytes until a full page is ready for flush.
    pub write_buffer: Arc<Mutex<BytesMut>>,
    pub file_handle: Arc<Mutex<Option<F::Handle>>>,
    pub metadata_store: M,
    /// Writer notifies after each completed page; readers long-poll on this.
    pub notify: Arc<Notify>,
    /// Fired on spool deletion to unblock any waiting readers.
    pub cancel: CancellationToken,
    pub page_size: usize,
    pub data_path: PathBuf,
    /// Number of active reader connections.
    pub reader_count: Arc<AtomicUsize>,
    /// Tracks which byte ranges have not yet been served to any client.
    /// Never persisted — reset to `[0, total_size)` on every restart.
    pub missing_ranges: Arc<Mutex<MissingRanges>>,
    /// Unix secs of the last byte-served event. 0 = never served since last restart.
    /// Updated on the read hot-path with `Ordering::Relaxed`.
    pub last_read_activity_at: Arc<AtomicU64>,
    /// Unix secs when full-object coverage was first detected. 0 = not yet.
    pub full_object_read_at: Arc<AtomicU64>,
    /// Metrics handle for cache hit/miss recording.
    pub metrics: Arc<BobsMetrics>,
    /// Admission permit held while this spool can retain first-read cache memory.
    /// Released at the full-read transition, or on deletion/drop if that happens first.
    admission_permit: std::sync::Mutex<Option<OwnedSemaphorePermit>>,
    pub(crate) _phantom: PhantomData<F>,
}

impl<F, M> Spool<F, M>
where
    F: FileIO,
    M: MetadataStore + Clone + Send + Sync + 'static,
{
    pub async fn new(
        metadata: SpoolMetadata,
        file_handle: F::Handle,
        page_size: usize,
        page_cache: Arc<Mutex<PageCache>>,
        metadata_store: M,
        metrics: Arc<BobsMetrics>,
    ) -> Self {
        Self::new_with_admission(
            metadata,
            file_handle,
            page_size,
            page_cache,
            metadata_store,
            metrics,
            None,
        )
        .await
    }

    pub async fn new_with_admission(
        metadata: SpoolMetadata,
        file_handle: F::Handle,
        page_size: usize,
        page_cache: Arc<Mutex<PageCache>>,
        metadata_store: M,
        metrics: Arc<BobsMetrics>,
        admission_permit: Option<OwnedSemaphorePermit>,
    ) -> Self {
        let data_path = metadata.data_path.clone();
        let key = metadata.key.clone();

        Self {
            key,
            metadata: Arc::new(Mutex::new(metadata)),
            page_cache,
            write_buffer: Arc::new(Mutex::new(BytesMut::new())),
            file_handle: Arc::new(Mutex::new(Some(file_handle))),
            metadata_store,
            notify: Arc::new(Notify::new()),
            cancel: CancellationToken::new(),
            page_size,
            data_path,
            reader_count: Arc::new(AtomicUsize::new(0)),
            missing_ranges: Arc::new(Mutex::new(MissingRanges::new(1024))),
            last_read_activity_at: Arc::new(AtomicU64::new(0)),
            full_object_read_at: Arc::new(AtomicU64::new(0)),
            metrics,
            admission_permit: std::sync::Mutex::new(admission_permit),
            _phantom: PhantomData,
        }
    }

    pub fn release_admission(&self) {
        if let Ok(mut permit) = self.admission_permit.lock() {
            permit.take();
        }
    }

    pub async fn persist_metadata(&self, metadata: &SpoolMetadata) -> crate::error::Result<()> {
        self.metadata_store.write(metadata).await
    }
}

pub use page_cache::PageCache;
pub use types::{SpoolMetadata, SpoolState};
