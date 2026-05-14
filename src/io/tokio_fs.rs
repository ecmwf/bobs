#[cfg(not(unix))]
compile_error!(
    "TokioFileIO requires Unix positional file APIs; add an explicit non-Unix backend before building on this platform"
);

use super::FileIO;
use bytes::Bytes;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use tokio::task;

/// TokioFileIO: FileIO implementation backed by standard file handles and
/// Tokio blocking tasks for positional file operations. Reads and writes do not
/// rely on or mutate a shared file cursor.
#[derive(Clone)]
pub struct TokioFileIO;

impl FileIO for TokioFileIO {
    type Handle = Arc<std::fs::File>;

    fn create(
        path: &Path,
    ) -> impl std::future::Future<Output = std::io::Result<Self::Handle>> + Send {
        let path = path.to_path_buf();
        async move {
            let file = task::spawn_blocking(move || {
                std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .read(true)
                    .open(path)
            })
            .await
            .map_err(join_error_to_io)??;
            Ok(Arc::new(file))
        }
    }

    fn open(
        path: &Path,
    ) -> impl std::future::Future<Output = std::io::Result<Self::Handle>> + Send {
        let path = path.to_path_buf();
        async move {
            let file = task::spawn_blocking(move || {
                std::fs::OpenOptions::new()
                    .write(true)
                    .read(true)
                    .open(path)
            })
            .await
            .map_err(join_error_to_io)??;
            Ok(Arc::new(file))
        }
    }

    fn write_at(
        handle: &Self::Handle,
        offset: u64,
        data: Bytes,
    ) -> impl std::future::Future<Output = std::io::Result<usize>> + Send {
        let file = Arc::clone(handle);
        async move {
            task::spawn_blocking(move || {
                let mut written = 0usize;
                while written < data.len() {
                    let n = file.write_at(&data[written..], offset + written as u64)?;
                    if n == 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::WriteZero,
                            format!("short write: wrote {written} of {} bytes", data.len()),
                        ));
                    }
                    written += n;
                }
                Ok(written)
            })
            .await
            .map_err(join_error_to_io)?
        }
    }

    fn read_at(
        handle: &Self::Handle,
        offset: u64,
        len: usize,
    ) -> impl std::future::Future<Output = std::io::Result<Bytes>> + Send {
        let file = Arc::clone(handle);
        async move {
            task::spawn_blocking(move || {
                let mut buf = vec![0u8; len];
                let n = file.read_at(&mut buf, offset)?;
                buf.truncate(n);
                Ok(Bytes::from(buf))
            })
            .await
            .map_err(join_error_to_io)?
        }
    }

    fn sync_data(
        handle: &Self::Handle,
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        let file = Arc::clone(handle);
        async move {
            task::spawn_blocking(move || file.sync_data())
                .await
                .map_err(join_error_to_io)?
        }
    }

    async fn close(handle: Self::Handle) -> std::io::Result<()> {
        drop(handle);
        Ok(())
    }

    fn remove(path: &Path) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        let path = path.to_path_buf();
        async move { tokio::fs::remove_file(path).await }
    }
}

fn join_error_to_io(error: task::JoinError) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_write_read_at_offset() {
        let dir = tempdir().expect("failed to create temp dir");
        let file_path = dir.path().join("test.bin");

        // Create file and write data at offset 0
        let handle = TokioFileIO::create(&file_path)
            .await
            .expect("failed to create file");

        let data = b"hello world";
        let written = TokioFileIO::write_at(&handle, 0, Bytes::copy_from_slice(data))
            .await
            .expect("failed to write");
        assert_eq!(written, data.len());

        // Sync to ensure data is written
        TokioFileIO::sync_data(&handle)
            .await
            .expect("failed to sync");

        // Read back the data
        let buf = TokioFileIO::read_at(&handle, 0, data.len())
            .await
            .expect("failed to read");
        assert_eq!(buf.len(), data.len());
        assert_eq!(buf.as_ref(), data);

        // Write at offset 5
        let offset_data = b"WORLD";
        let written = TokioFileIO::write_at(&handle, 5, Bytes::copy_from_slice(offset_data))
            .await
            .expect("failed to write at offset");
        assert_eq!(written, offset_data.len());

        TokioFileIO::sync_data(&handle)
            .await
            .expect("failed to sync");

        // Read back and verify offset write
        let buf = TokioFileIO::read_at(&handle, 0, 10)
            .await
            .expect("failed to read");
        assert_eq!(buf.len(), 10);
        assert_eq!(buf.as_ref(), b"helloWORLD");

        TokioFileIO::close(handle).await.expect("failed to close");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_positional_writes_do_not_share_cursor() {
        let dir = tempdir().expect("failed to create temp dir");
        let file_path = dir.path().join("test_positional.bin");

        let handle = TokioFileIO::create(&file_path)
            .await
            .expect("failed to create file");

        const BLOCKS: usize = 128;
        const BLOCK_LEN: usize = 64;
        TokioFileIO::write_at(&handle, 0, Bytes::from(vec![b'.'; BLOCKS * BLOCK_LEN]))
            .await
            .expect("failed to initialize file");

        let tasks: Vec<_> = (0..BLOCKS)
            .map(|block| {
                let handle = Arc::clone(&handle);
                let offset = (block * BLOCK_LEN) as u64;
                let byte = b'A' + (block % 26) as u8;
                let data = Bytes::from(vec![byte; BLOCK_LEN]);
                tokio::spawn(async move { TokioFileIO::write_at(&handle, offset, data).await })
            })
            .collect();

        for task in tasks {
            let written = task
                .await
                .expect("write task panicked")
                .expect("positional write failed");
            assert_eq!(written, BLOCK_LEN);
        }

        TokioFileIO::sync_data(&handle)
            .await
            .expect("failed to sync");

        let whole = TokioFileIO::read_at(&handle, 0, BLOCKS * BLOCK_LEN)
            .await
            .expect("failed to read whole file");
        assert_eq!(whole.len(), BLOCKS * BLOCK_LEN);
        for block in 0..BLOCKS {
            let byte = b'A' + (block % 26) as u8;
            let start = block * BLOCK_LEN;
            let end = start + BLOCK_LEN;
            assert_eq!(
                &whole[start..end],
                vec![byte; BLOCK_LEN].as_slice(),
                "block {block} was not written at its requested offset"
            );
        }

        TokioFileIO::close(handle).await.expect("failed to close");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_parallel_reads_of_same_page_from_disk_return_owned_bytes() {
        let dir = tempdir().expect("failed to create temp dir");
        let file_path = dir.path().join("test_parallel_same_page.bin");

        let handle = TokioFileIO::create(&file_path)
            .await
            .expect("failed to create file");
        let page = Bytes::from((0..4096).map(|n| (n % 251) as u8).collect::<Vec<_>>());
        TokioFileIO::write_at(&handle, 8192, page.clone())
            .await
            .expect("failed to write page");
        TokioFileIO::sync_data(&handle)
            .await
            .expect("failed to sync");
        TokioFileIO::close(handle).await.expect("failed to close");

        let handle = TokioFileIO::open(&file_path)
            .await
            .expect("failed to reopen file");
        let tasks: Vec<_> = (0..64)
            .map(|_| {
                let handle = Arc::clone(&handle);
                tokio::spawn(async move { TokioFileIO::read_at(&handle, 8192, 4096).await })
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

        TokioFileIO::write_at(&handle, 8192, Bytes::from(vec![0xEE; 4096]))
            .await
            .expect("failed to overwrite page");
        for read in reads {
            assert_eq!(
                read.as_ref(),
                page.as_ref(),
                "previous reads must remain owned buffers after later writes"
            );
        }

        TokioFileIO::close(handle).await.expect("failed to close");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_parallel_reads_while_writes_append_later_offsets() {
        let dir = tempdir().expect("failed to create temp dir");
        let file_path = dir.path().join("test_read_while_append.bin");

        let handle = TokioFileIO::create(&file_path)
            .await
            .expect("failed to create file");
        let base_page = Bytes::from(vec![0x5A; 4096]);
        TokioFileIO::write_at(&handle, 0, base_page.clone())
            .await
            .expect("failed to write base page");
        TokioFileIO::sync_data(&handle)
            .await
            .expect("failed to sync base page");

        let readers: Vec<_> = (0..64)
            .map(|_| {
                let handle = Arc::clone(&handle);
                let expected = base_page.clone();
                tokio::spawn(async move {
                    for _ in 0..16 {
                        let read = TokioFileIO::read_at(&handle, 0, expected.len()).await?;
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
                tokio::spawn(async move { TokioFileIO::write_at(&handle, offset, data).await })
            })
            .collect();

        for writer in writers {
            let written = writer
                .await
                .expect("append writer task panicked")
                .expect("append writer failed");
            assert_eq!(written, 257);
        }
        for reader in readers {
            reader
                .await
                .expect("reader task panicked")
                .expect("reader observed data from the wrong offset");
        }

        let base = TokioFileIO::read_at(&handle, 0, 4096)
            .await
            .expect("failed to reread base page");
        assert_eq!(base.as_ref(), base_page.as_ref());

        TokioFileIO::close(handle).await.expect("failed to close");
    }

    #[tokio::test]
    async fn test_read_beyond_eof() {
        let dir = tempdir().expect("failed to create temp dir");
        let file_path = dir.path().join("test_eof.bin");

        let handle = TokioFileIO::create(&file_path)
            .await
            .expect("failed to create file");

        let data = b"short";
        TokioFileIO::write_at(&handle, 0, Bytes::copy_from_slice(data))
            .await
            .expect("failed to write");

        TokioFileIO::sync_data(&handle)
            .await
            .expect("failed to sync");

        // Reading a range that extends beyond EOF returns the available bytes in
        // an owned buffer, not an error and not a zero-padded buffer.
        let buf = TokioFileIO::read_at(&handle, 0, 100)
            .await
            .expect("failed to read");
        assert_eq!(buf.as_ref(), data);

        TokioFileIO::write_at(&handle, 0, Bytes::from_static(b"later"))
            .await
            .expect("failed to overwrite data");
        assert_eq!(
            buf.as_ref(),
            data,
            "read-beyond-EOF result must remain owned after later writes"
        );

        // Reading from an offset beyond EOF returns an empty owned buffer.
        let buf = TokioFileIO::read_at(&handle, 100, 10)
            .await
            .expect("failed to read beyond eof");
        assert!(buf.is_empty(), "reading beyond EOF should return no bytes");

        TokioFileIO::close(handle).await.expect("failed to close");
    }

    #[tokio::test]
    async fn test_remove() {
        let dir = tempdir().expect("failed to create temp dir");
        let file_path = dir.path().join("test_remove.bin");

        let handle = TokioFileIO::create(&file_path)
            .await
            .expect("failed to create file");

        TokioFileIO::write_at(&handle, 0, Bytes::from_static(b"data"))
            .await
            .expect("failed to write");

        TokioFileIO::close(handle).await.expect("failed to close");

        // File should exist
        assert!(file_path.exists(), "file should exist after creation");

        // Remove the file
        TokioFileIO::remove(&file_path)
            .await
            .expect("failed to remove file");

        // File should not exist
        assert!(!file_path.exists(), "file should not exist after removal");
    }
}
