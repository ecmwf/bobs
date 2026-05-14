pub mod tokio_fs;
pub use tokio_fs::TokioFileIO;

use bytes::Bytes;
use std::future::Future;
use std::path::Path;

/// FileIO trait for async positional file operations.
///
/// Reads and writes are addressed by explicit byte offsets. Implementations must
/// not depend on, expose, or mutate a shared file cursor for `read_at` or
/// `write_at`; concurrent operations on the same handle must behave as
/// independent positional I/O. Implementations must be Send + Sync + Clone for
/// use in concurrent contexts.
pub trait FileIO: Send + Sync + Clone + 'static {
    /// Associated type for file handles. Must be Send + Sync.
    type Handle: Send + Sync + 'static;

    /// Create a new file at the given path.
    fn create(path: &Path) -> impl Future<Output = std::io::Result<Self::Handle>> + Send;

    /// Open an existing file at the given path.
    fn open(path: &Path) -> impl Future<Output = std::io::Result<Self::Handle>> + Send;

    /// Write owned data at a specific byte offset in the file.
    fn write_at(
        handle: &Self::Handle,
        offset: u64,
        data: Bytes,
    ) -> impl Future<Output = std::io::Result<usize>> + Send;

    /// Read up to `len` bytes from a specific byte offset in the file.
    fn read_at(
        handle: &Self::Handle,
        offset: u64,
        len: usize,
    ) -> impl Future<Output = std::io::Result<Bytes>> + Send;

    /// Sync file data to disk.
    fn sync_data(handle: &Self::Handle) -> impl Future<Output = std::io::Result<()>> + Send;

    /// Close the file handle.
    fn close(handle: Self::Handle) -> impl Future<Output = std::io::Result<()>> + Send;

    /// Remove a file at the given path.
    fn remove(path: &Path) -> impl Future<Output = std::io::Result<()>> + Send;
}
