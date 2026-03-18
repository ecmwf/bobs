use crate::io::FileIO;
use bytes::BytesMut;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

pub mod page_cache;
pub mod lifecycle;
pub mod reader;
pub mod types;
pub mod writer;

pub struct Spool<F: FileIO> {
    pub metadata: Arc<Mutex<SpoolMetadata>>,
    pub page_cache: Arc<Mutex<PageCache>>,
    pub write_buffer: Arc<Mutex<BytesMut>>,
    pub file_handle: Arc<Mutex<Option<F::Handle>>>,
    pub notify: Arc<Notify>,
    pub cancel: CancellationToken,
    pub page_size: usize,
    pub data_path: PathBuf,
    pub reader_active: Arc<AtomicBool>,
    pub(crate) _phantom: PhantomData<F>,
}

impl<F: FileIO> Spool<F> {
    pub async fn new(
        metadata: SpoolMetadata,
        file_handle: F::Handle,
        page_size: usize,
        cache_capacity: usize,
    ) -> Self {
        let data_path = metadata.data_path.clone();

        Self {
            metadata: Arc::new(Mutex::new(metadata)),
            page_cache: Arc::new(Mutex::new(PageCache::new(cache_capacity))),
            write_buffer: Arc::new(Mutex::new(BytesMut::new())),
            file_handle: Arc::new(Mutex::new(Some(file_handle))),
            notify: Arc::new(Notify::new()),
            cancel: CancellationToken::new(),
            page_size,
            data_path,
            reader_active: Arc::new(AtomicBool::new(false)),
            _phantom: PhantomData,
        }
    }
}

pub use page_cache::PageCache;
pub use types::{SpoolMetadata, SpoolState};
