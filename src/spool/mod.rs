use crate::io::FileIO;
use bytes::BytesMut;
use redb::Database;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

pub mod lifecycle;
pub mod page_cache;
pub mod reader;
pub mod types;
pub mod writer;

/// A spool buffers a single streaming response: one writer appends pages sequentially,
/// multiple readers can consume byte ranges in parallel. Pages are flushed to disk when
/// full (page_size bytes) and cached in memory for fast reads. The writer signals readers
/// via `notify` after each completed page; readers long-poll until data is available.
pub struct Spool<F: FileIO> {
    pub metadata: Arc<Mutex<SpoolMetadata>>,
    pub page_cache: Arc<Mutex<PageCache>>,
    /// Accumulates incoming bytes until a full page is ready for flush.
    pub write_buffer: Arc<Mutex<BytesMut>>,
    pub file_handle: Arc<Mutex<Option<F::Handle>>>,
    pub db: Arc<Database>,
    pub running_crc32c: Arc<Mutex<u32>>,
    /// Writer notifies after each completed page; readers long-poll on this.
    pub notify: Arc<Notify>,
    /// Fired on spool deletion to unblock any waiting readers.
    pub cancel: CancellationToken,
    pub page_size: usize,
    pub data_path: PathBuf,
    /// Number of active reader connections. Cleanup skips spools with readers > 0.
    pub reader_count: Arc<AtomicUsize>,
    pub(crate) _phantom: PhantomData<F>,
}

impl<F: FileIO> Spool<F> {
    pub async fn new(
        metadata: SpoolMetadata,
        file_handle: F::Handle,
        page_size: usize,
        cache_capacity: usize,
        db: Arc<Database>,
    ) -> Self {
        let data_path = metadata.data_path.clone();

        Self {
            metadata: Arc::new(Mutex::new(metadata)),
            page_cache: Arc::new(Mutex::new(PageCache::new(cache_capacity))),
            write_buffer: Arc::new(Mutex::new(BytesMut::new())),
            file_handle: Arc::new(Mutex::new(Some(file_handle))),
            db,
            running_crc32c: Arc::new(Mutex::new(0)),
            notify: Arc::new(Notify::new()),
            cancel: CancellationToken::new(),
            page_size,
            data_path,
            reader_count: Arc::new(AtomicUsize::new(0)),
            _phantom: PhantomData,
        }
    }

    pub fn persist_metadata(&self, metadata: &SpoolMetadata) -> crate::error::Result<()> {
        use crate::error::BobsError;
        use crate::manager::SPOOL_TABLE;

        let payload = serde_json::to_vec(metadata)
            .map_err(|e| BobsError::SerializationError(e.to_string()))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| BobsError::StorageError(e.into()))?;
        {
            let mut table = write_txn
                .open_table(SPOOL_TABLE)
                .map_err(|e| BobsError::StorageError(e.into()))?;
            table
                .insert(metadata.key.as_str(), payload.as_slice())
                .map_err(|e| BobsError::StorageError(e.into()))?;
        }
        write_txn
            .commit()
            .map_err(|e| BobsError::StorageError(e.into()))?;
        Ok(())
    }
}

pub use page_cache::PageCache;
pub use types::{SpoolMetadata, SpoolState};
