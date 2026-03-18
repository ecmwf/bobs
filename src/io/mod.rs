pub mod tokio_fs;
pub use tokio_fs::TokioFileIO;

use std::future::Future;
use std::path::Path;

/// FileIO trait for async file operations.
/// Implementations must be Send + Sync + Clone for use in concurrent contexts.
pub trait FileIO: Send + Sync + Clone + 'static {
    /// Associated type for file handles. Must be Send + Sync.
    type Handle: Send + Sync + 'static;

    /// Create a new file at the given path.
    fn create(path: &Path) -> impl Future<Output = std::io::Result<Self::Handle>> + Send;

    /// Open an existing file at the given path.
    fn open(path: &Path) -> impl Future<Output = std::io::Result<Self::Handle>> + Send;

    /// Write data at a specific offset in the file.
    fn write_at(
        handle: &Self::Handle,
        offset: u64,
        data: &[u8],
    ) -> impl Future<Output = std::io::Result<usize>> + Send;

    /// Read data from a specific offset in the file.
    fn read_at(
        handle: &Self::Handle,
        offset: u64,
        buf: &mut [u8],
    ) -> impl Future<Output = std::io::Result<usize>> + Send;

    /// Sync file data to disk.
    fn sync_data(handle: &Self::Handle) -> impl Future<Output = std::io::Result<()>> + Send;

    /// Close the file handle.
    fn close(handle: Self::Handle) -> impl Future<Output = std::io::Result<()>> + Send;

    /// Remove a file at the given path.
    fn remove(path: &Path) -> impl Future<Output = std::io::Result<()>> + Send;
}
