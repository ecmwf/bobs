// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

/// Maximum explicit number of `io_uring` rings and driver threads.
///
/// The automatic default remains `(num_cpus / 4).max(1)`. This limit prevents a
/// malformed or hostile configuration from attempting pathological allocation and
/// thread creation.
pub const MAX_IO_URING_SHARDS: usize = 256;

#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
pub mod ring_pool;
pub mod tokio_fs;
#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
pub mod uring_fs;

#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
pub use ring_pool::{RingPool, RingPoolOptions, RingPoolShutdown, RingPoolStartup};
pub use tokio_fs::TokioFileIO;
#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
pub use uring_fs::{initialize_production_ring_pool, UringFileIO};

#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
pub type DefaultFileIO = UringFileIO;

#[cfg(any(not(target_os = "linux"), feature = "tokio-fileio-fallback"))]
pub type DefaultFileIO = TokioFileIO;

use bytes::{Bytes, BytesMut};
use std::future::Future;
use std::io;
use std::path::Path;

/// FileIO trait for async positional file operations.
///
/// Reads and writes are addressed by explicit byte offsets. Implementations must
/// not depend on, expose, or mutate a shared file cursor for `read_at` or
/// `write_at`; concurrent operations on the same handle must behave as
/// independent positional I/O. Implementations must be Send + Sync + Clone for
/// use in concurrent contexts.
pub async fn read_exact_at<F: FileIO>(
    handle: &F::Handle,
    offset: u64,
    len: usize,
    context: &str,
) -> io::Result<Bytes> {
    if len == 0 {
        return Ok(Bytes::new());
    }

    let first = F::read_at(handle, offset, len).await?;
    if first.len() == len {
        return Ok(first);
    }
    if first.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("expected {len} bytes while {context}, got 0"),
        ));
    }

    let mut out = BytesMut::with_capacity(len);
    out.extend_from_slice(&first);

    while out.len() < len {
        let read_offset = offset + out.len() as u64;
        let remaining = len - out.len();
        let buf = F::read_at(handle, read_offset, remaining).await?;
        if buf.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("expected {len} bytes while {context}, got {}", out.len()),
            ));
        }
        out.extend_from_slice(&buf);
    }

    Ok(out.freeze())
}

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

    /// Sync a directory so entry creation, rename, and removal are durable.
    fn sync_directory(path: &Path) -> impl Future<Output = std::io::Result<()>> + Send;

    /// Close the file handle.
    fn close(handle: Self::Handle) -> impl Future<Output = std::io::Result<()>> + Send;

    /// Remove a file at the given path.
    fn remove(path: &Path) -> impl Future<Output = std::io::Result<()>> + Send;
}

#[cfg(test)]
pub(crate) mod fileio_test_cases {
    use super::FileIO;
    use bytes::Bytes;
    use std::marker::PhantomData;
    use std::sync::Arc;
    use tempfile::tempdir;

    pub(crate) struct FileIOTestSuite<I>(PhantomData<I>);

    impl<I> FileIOTestSuite<I>
    where
        I: FileIO,
    {
        pub(crate) async fn write_read_at_offset() {
            let dir = tempdir().expect("failed to create temp dir");
            let file_path = dir.path().join("test.bin");
            let handle = I::create(&file_path).await.expect("failed to create file");

            let data = b"hello world";
            let written = I::write_at(&handle, 0, Bytes::copy_from_slice(data))
                .await
                .expect("failed to write");
            assert_eq!(written, data.len());
            I::sync_data(&handle).await.expect("failed to sync");

            let buf = I::read_at(&handle, 0, data.len())
                .await
                .expect("failed to read");
            assert_eq!(buf.as_ref(), data);

            let offset_data = b"WORLD";
            let written = I::write_at(&handle, 5, Bytes::copy_from_slice(offset_data))
                .await
                .expect("failed to write at offset");
            assert_eq!(written, offset_data.len());
            I::sync_data(&handle).await.expect("failed to sync");

            let buf = I::read_at(&handle, 0, 10).await.expect("failed to read");
            assert_eq!(buf.as_ref(), b"helloWORLD");
            I::close(handle).await.expect("failed to close");
        }

        pub(crate) async fn parallel_disjoint_writes() {
            let dir = tempdir().expect("failed to create temp dir");
            let file_path = dir.path().join("test_positional.bin");
            let handle = Arc::new(I::create(&file_path).await.expect("failed to create file"));

            const BLOCKS: usize = 128;
            const BLOCK_LEN: usize = 64;
            I::write_at(&handle, 0, Bytes::from(vec![b'.'; BLOCKS * BLOCK_LEN]))
                .await
                .expect("failed to initialize file");

            let tasks: Vec<_> = (0..BLOCKS)
                .map(|block| {
                    let handle = Arc::clone(&handle);
                    let offset = (block * BLOCK_LEN) as u64;
                    let byte = b'A' + (block % 26) as u8;
                    let data = Bytes::from(vec![byte; BLOCK_LEN]);
                    tokio::spawn(async move { I::write_at(&handle, offset, data).await })
                })
                .collect();

            for task in tasks {
                let written = task
                    .await
                    .expect("write task panicked")
                    .expect("positional write failed");
                assert_eq!(written, BLOCK_LEN);
            }

            I::sync_data(&handle).await.expect("failed to sync");
            let whole = I::read_at(&handle, 0, BLOCKS * BLOCK_LEN)
                .await
                .expect("failed to read whole file");
            assert_eq!(whole.len(), BLOCKS * BLOCK_LEN);
            for block in 0..BLOCKS {
                let byte = b'A' + (block % 26) as u8;
                let start = block * BLOCK_LEN;
                assert_eq!(
                    &whole[start..start + BLOCK_LEN],
                    vec![byte; BLOCK_LEN].as_slice()
                );
            }

            let handle = Arc::try_unwrap(handle)
                .ok()
                .expect("test still holds handle refs");
            I::close(handle).await.expect("failed to close");
        }

        pub(crate) async fn parallel_reads_from_same_file_return_owned_bytes() {
            let dir = tempdir().expect("failed to create temp dir");
            let file_path = dir.path().join("test_parallel_same_page.bin");
            let handle = I::create(&file_path).await.expect("failed to create file");
            let page = Bytes::from((0..4096).map(|n| (n % 251) as u8).collect::<Vec<_>>());
            I::write_at(&handle, 8192, page.clone())
                .await
                .expect("failed to write page");
            I::sync_data(&handle).await.expect("failed to sync");
            I::close(handle).await.expect("failed to close");

            let handle = Arc::new(I::open(&file_path).await.expect("failed to reopen file"));
            let tasks: Vec<_> = (0..64)
                .map(|_| {
                    let handle = Arc::clone(&handle);
                    tokio::spawn(async move { I::read_at(&handle, 8192, 4096).await })
                })
                .collect();

            let mut reads = Vec::with_capacity(tasks.len());
            for task in tasks {
                let read = task
                    .await
                    .expect("read task panicked")
                    .expect("positional read failed");
                assert_eq!(read.as_ref(), page.as_ref());
                reads.push(read);
            }

            I::write_at(&handle, 8192, Bytes::from(vec![0xEE; 4096]))
                .await
                .expect("failed to overwrite page");
            for read in reads {
                assert_eq!(read.as_ref(), page.as_ref());
            }

            let handle = Arc::try_unwrap(handle)
                .ok()
                .expect("test still holds handle refs");
            I::close(handle).await.expect("failed to close");
        }

        pub(crate) async fn parallel_reads_while_writes_append_later_offsets() {
            let dir = tempdir().expect("failed to create temp dir");
            let file_path = dir.path().join("test_read_while_append.bin");
            let handle = Arc::new(I::create(&file_path).await.expect("failed to create file"));
            let base_page = Bytes::from(vec![0x5A; 4096]);
            I::write_at(&handle, 0, base_page.clone())
                .await
                .expect("failed to write base page");
            I::sync_data(&handle)
                .await
                .expect("failed to sync base page");

            let readers: Vec<_> = (0..64)
                .map(|_| {
                    let handle = Arc::clone(&handle);
                    let expected = base_page.clone();
                    tokio::spawn(async move {
                        for _ in 0..16 {
                            let read = I::read_at(&handle, 0, expected.len()).await?;
                            if read != expected {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "read of base page was affected by append at a later offset",
                                ));
                            }
                        }
                        Ok::<_, std::io::Error>(())
                    })
                })
                .collect();

            let writers: Vec<_> = (0..64)
                .map(|block| {
                    let handle = Arc::clone(&handle);
                    let offset = 4096 + (block * 257) as u64;
                    let data = Bytes::from(vec![block as u8; 257]);
                    tokio::spawn(async move { I::write_at(&handle, offset, data).await })
                })
                .collect();

            for writer in writers {
                assert_eq!(
                    writer
                        .await
                        .expect("append writer task panicked")
                        .expect("append writer failed"),
                    257
                );
            }
            for reader in readers {
                reader
                    .await
                    .expect("reader task panicked")
                    .expect("reader observed data from the wrong offset");
            }

            let base = I::read_at(&handle, 0, 4096)
                .await
                .expect("failed to reread base page");
            assert_eq!(base.as_ref(), base_page.as_ref());
            let handle = Arc::try_unwrap(handle)
                .ok()
                .expect("test still holds handle refs");
            I::close(handle).await.expect("failed to close");
        }

        pub(crate) async fn read_beyond_eof_returns_available_then_empty_bytes() {
            let dir = tempdir().expect("failed to create temp dir");
            let file_path = dir.path().join("test_eof.bin");
            let handle = I::create(&file_path).await.expect("failed to create file");
            let data = b"short";
            I::write_at(&handle, 0, Bytes::copy_from_slice(data))
                .await
                .expect("failed to write");
            I::sync_data(&handle).await.expect("failed to sync");

            let buf = I::read_at(&handle, 0, 100).await.expect("failed to read");
            assert_eq!(buf.as_ref(), data);
            I::write_at(&handle, 0, Bytes::from_static(b"later"))
                .await
                .expect("failed to overwrite data");
            assert_eq!(buf.as_ref(), data);

            let buf = I::read_at(&handle, 100, 10)
                .await
                .expect("failed to read beyond eof");
            assert!(buf.is_empty(), "reading beyond EOF should return no bytes");
            I::close(handle).await.expect("failed to close");
        }

        pub(crate) async fn remove_unlinks_file() {
            let dir = tempdir().expect("failed to create temp dir");
            let file_path = dir.path().join("test_remove.bin");
            let handle = I::create(&file_path).await.expect("failed to create file");
            I::write_at(&handle, 0, Bytes::from_static(b"data"))
                .await
                .expect("failed to write");
            I::close(handle).await.expect("failed to close");
            assert!(file_path.exists(), "file should exist after creation");
            I::remove(&file_path).await.expect("failed to remove file");
            assert!(!file_path.exists(), "file should not exist after removal");
        }

        pub(crate) async fn sync_directory_succeeds() {
            let dir = tempdir().expect("failed to create temp dir");
            let file_path = dir.path().join("directory-entry");
            let handle = I::create(&file_path).await.expect("failed to create file");
            I::sync_data(&handle).await.expect("failed to sync file");
            I::close(handle).await.expect("failed to close");
            I::sync_directory(dir.path())
                .await
                .expect("failed to sync directory");
        }

        pub(crate) async fn close_and_drop_are_safe() {
            let dir = tempdir().expect("failed to create temp dir");
            let close_path = dir.path().join("test_close.bin");
            let handle = I::create(&close_path).await.expect("failed to create file");
            I::write_at(&handle, 0, Bytes::from_static(b"close"))
                .await
                .expect("failed to write before close");
            I::close(handle).await.expect("close should be safe");
            I::remove(&close_path)
                .await
                .expect("remove after close should work");

            let drop_path = dir.path().join("test_drop.bin");
            {
                let handle = I::create(&drop_path).await.expect("failed to create file");
                I::write_at(&handle, 0, Bytes::from_static(b"drop"))
                    .await
                    .expect("failed to write before drop");
            }
            I::remove(&drop_path)
                .await
                .expect("remove after drop should work");
        }
    }
}
