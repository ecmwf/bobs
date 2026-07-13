// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use crate::io::FileIO;
use crate::metadata::{MetadataStore, SyncSidecarMetadataStore};
use crate::metrics::BobsMetrics;
use bytes::BytesMut;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore};
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
#[derive(Clone, Copy, Debug)]
pub(crate) struct CleanupAnchors {
    pub last_write_at: Instant,
    pub readable_at: Option<Instant>,
    pub last_read_activity_at: Option<Instant>,
    pub full_object_read_at: Option<Instant>,
}

/// Weighted manager-wide admission held for the lifetime of one read response.
///
/// The semaphore units represent configured pages. Recovered spools with wider
/// persisted pages acquire multiple units, so old layouts cannot bypass the current
/// response-memory budget. Dropping the lease releases both admission and metrics.
pub struct ReadResponsePermit {
    _permit: OwnedSemaphorePermit,
    active_responses: Arc<AtomicUsize>,
    active_permits: Arc<AtomicUsize>,
    permit_units: usize,
    metrics: Arc<BobsMetrics>,
}

impl Drop for ReadResponsePermit {
    fn drop(&mut self) {
        let previous_responses = self.active_responses.fetch_sub(1, Ordering::AcqRel);
        let previous_permits = self
            .active_permits
            .fetch_sub(self.permit_units, Ordering::AcqRel);
        debug_assert!(previous_responses > 0);
        debug_assert!(previous_permits >= self.permit_units);
        self.metrics
            .record_read_response_permit_released(self.permit_units);
    }
}

pub struct Spool<F: FileIO, M: MetadataStore = SyncSidecarMetadataStore> {
    pub key: String,
    pub metadata: Arc<Mutex<SpoolMetadata>>,
    pub page_cache: Arc<Mutex<PageCache>>,
    /// Accumulates incoming bytes until a full page is ready for flush.
    pub write_buffer: Arc<Mutex<BytesMut>>,
    /// Shared positional-I/O handle. Cloning this `Arc` keeps the handle alive for
    /// each in-flight operation; it closes naturally when the final spool/operation
    /// reference is dropped.
    pub file_handle: Arc<F::Handle>,
    pub metadata_store: M,
    /// Writer notifies after each completed page; readers long-poll on this.
    pub notify: Arc<Notify>,
    /// Fired on spool deletion to unblock any waiting readers.
    pub cancel: CancellationToken,
    /// Orders mutating transactions before they start owned work. Callers wait on
    /// this bounded gate before spawning, so cancellation leaves no detached waiter
    /// and at most one write/completion transaction can be active per spool.
    pub(crate) operation_gate: Arc<Semaphore>,
    /// Serializes mutation with terminal lifecycle operations and cleanup activity.
    /// Operation transactions always acquire `operation_gate` before this lock.
    pub(crate) lifecycle_lock: Mutex<()>,
    /// Manager-wide response-buffer admission. A response acquires its weighted
    /// share before any page read and retains it until its body/reader lease drops.
    read_response_admission: Arc<Semaphore>,
    read_response_active: Arc<AtomicUsize>,
    read_response_permits_active: Arc<AtomicUsize>,
    read_response_permits_per_reader: u32,
    pub page_size: usize,
    pub data_path: PathBuf,
    /// Number of active reader connections.
    pub reader_count: Arc<AtomicUsize>,
    /// Tracks which byte ranges have not yet been served to any client.
    /// Never persisted — reset to `[0, total_size)` on every restart.
    pub missing_ranges: Arc<Mutex<MissingRanges>>,
    /// In-memory monotonic anchors used by cleanup TTL rules.
    /// Persisted wall-clock timestamps remain metadata/observability only.
    pub(crate) cleanup_anchors: Arc<StdMutex<CleanupAnchors>>,
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
    pub fn new(
        metadata: SpoolMetadata,
        file_handle: F::Handle,
        page_size: usize,
        page_cache: Arc<Mutex<PageCache>>,
        metadata_store: M,
        metrics: Arc<BobsMetrics>,
    ) -> Self {
        let read_response_admission = Arc::new(Semaphore::new(1));
        Self::new_with_admission(
            metadata,
            file_handle,
            page_size,
            page_cache,
            metadata_store,
            metrics,
            None,
            read_response_admission,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            1,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_admission(
        metadata: SpoolMetadata,
        file_handle: F::Handle,
        page_size: usize,
        page_cache: Arc<Mutex<PageCache>>,
        metadata_store: M,
        metrics: Arc<BobsMetrics>,
        admission_permit: Option<OwnedSemaphorePermit>,
        read_response_admission: Arc<Semaphore>,
        read_response_active: Arc<AtomicUsize>,
        read_response_permits_active: Arc<AtomicUsize>,
        read_response_permits_per_reader: u32,
    ) -> Self {
        let data_path = metadata.data_path.clone();
        let key = metadata.key.clone();
        let now = Instant::now();
        let readable_at =
            matches!(metadata.state, SpoolState::Complete | SpoolState::Deleting).then_some(now);
        Self {
            key,
            metadata: Arc::new(Mutex::new(metadata)),
            page_cache,
            write_buffer: Arc::new(Mutex::new(BytesMut::new())),
            file_handle: Arc::new(file_handle),
            metadata_store,
            notify: Arc::new(Notify::new()),
            cancel: CancellationToken::new(),
            operation_gate: Arc::new(Semaphore::new(1)),
            lifecycle_lock: Mutex::new(()),
            read_response_admission,
            read_response_active,
            read_response_permits_active,
            read_response_permits_per_reader,
            page_size,
            data_path,
            reader_count: Arc::new(AtomicUsize::new(0)),
            missing_ranges: Arc::new(Mutex::new(MissingRanges::new(1024))),
            cleanup_anchors: Arc::new(StdMutex::new(CleanupAnchors {
                last_write_at: now,
                readable_at,
                last_read_activity_at: None,
                full_object_read_at: None,
            })),
            metrics,
            admission_permit: std::sync::Mutex::new(admission_permit),
            _phantom: PhantomData,
        }
    }

    pub(crate) fn cleanup_anchors(&self) -> CleanupAnchors {
        *self
            .cleanup_anchors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn record_write_activity(&self) {
        self.cleanup_anchors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .last_write_at = Instant::now();
    }

    pub(crate) fn record_readable(&self) {
        self.cleanup_anchors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .readable_at = Some(Instant::now());
    }

    pub(crate) fn record_read_activity(&self) {
        self.cleanup_anchors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .last_read_activity_at = Some(Instant::now());
    }

    pub(crate) fn record_fully_read(&self) -> bool {
        let mut anchors = self
            .cleanup_anchors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if anchors.full_object_read_at.is_some() {
            false
        } else {
            anchors.full_object_read_at = Some(Instant::now());
            true
        }
    }

    pub fn release_admission(&self) {
        if let Ok(mut permit) = self.admission_permit.lock() {
            permit.take();
        }
    }

    /// Acquire the manager-wide response-buffer budget without holding lifecycle,
    /// metadata, cache, or spool-admission locks. Deletion cancels queued readers
    /// immediately rather than leaving them behind a slow client.
    pub async fn acquire_read_response_permit(&self) -> crate::error::Result<ReadResponsePermit> {
        if self.cancel.is_cancelled() {
            return Err(crate::error::BobsError::SpoolNotFound {
                key: self.key.clone(),
            });
        }

        let units = self.read_response_permits_per_reader;
        let acquire = Arc::clone(&self.read_response_admission).acquire_many_owned(units);
        let permit = tokio::select! {
            result = acquire => result.map_err(|_| {
                crate::error::BobsError::IoError(std::io::Error::other(
                    "read response semaphore closed",
                ))
            })?,
            _ = self.cancel.cancelled() => {
                return Err(crate::error::BobsError::SpoolNotFound {
                    key: self.key.clone(),
                });
            }
        };

        // Deletion can race the semaphore wake. Do not publish a lease after the
        // spool has become terminal; dropping the raw permit restores capacity.
        if self.cancel.is_cancelled() {
            return Err(crate::error::BobsError::SpoolNotFound {
                key: self.key.clone(),
            });
        }

        self.read_response_active.fetch_add(1, Ordering::AcqRel);
        self.read_response_permits_active
            .fetch_add(units as usize, Ordering::AcqRel);
        self.metrics
            .record_read_response_permit_acquired(units as usize);
        Ok(ReadResponsePermit {
            _permit: permit,
            active_responses: Arc::clone(&self.read_response_active),
            active_permits: Arc::clone(&self.read_response_permits_active),
            permit_units: units as usize,
            metrics: Arc::clone(&self.metrics),
        })
    }

    pub async fn persist_metadata(&self, metadata: &SpoolMetadata) -> crate::error::Result<()> {
        self.metadata_store.write(metadata).await
    }
}

pub use page_cache::PageCache;
pub use types::{SpoolMetadata, SpoolState};
