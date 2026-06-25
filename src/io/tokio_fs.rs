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
                // A single pread may return fewer bytes than requested (a short
                // read), which is more likely the larger the page. Loop to fill
                // the whole buffer (mirroring write_at), stopping only at EOF, so
                // callers never receive a truncated mid-file page -- a truncated
                // page stalls the streaming follow-reader on large objects.
                let mut total = 0usize;
                while total < len {
                    let n = file.read_at(&mut buf[total..], offset + total as u64)?;
                    if n == 0 {
                        break; // EOF
                    }
                    total += n;
                }
                buf.truncate(total);
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
    std::io::Error::other(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::fileio_test_cases::FileIOTestSuite;

    type Suite = FileIOTestSuite<TokioFileIO>;

    #[tokio::test]
    async fn tokio_fileio_write_read_at_offset() {
        Suite::write_read_at_offset().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tokio_fileio_parallel_disjoint_writes() {
        Suite::parallel_disjoint_writes().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tokio_fileio_parallel_reads_from_same_file_return_owned_bytes() {
        Suite::parallel_reads_from_same_file_return_owned_bytes().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tokio_fileio_parallel_reads_while_writes_append_later_offsets() {
        Suite::parallel_reads_while_writes_append_later_offsets().await;
    }

    #[tokio::test]
    async fn tokio_fileio_read_beyond_eof_returns_available_then_empty_bytes() {
        Suite::read_beyond_eof_returns_available_then_empty_bytes().await;
    }

    #[tokio::test]
    async fn tokio_fileio_remove_unlinks_file() {
        Suite::remove_unlinks_file().await;
    }

    #[tokio::test]
    async fn tokio_fileio_close_and_drop_are_safe() {
        Suite::close_and_drop_are_safe().await;
    }

    #[tokio::test]
    async fn tokio_fileio_read_at_fills_large_buffer_not_short() {
        // read_at must return the whole requested span (until EOF), never a
        // truncated mid-file page -- the invariant that keeps the streaming
        // follow-reader from spinning on large (e.g. 16 MiB page) objects.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big");
        let handle = TokioFileIO::create(&path).await.unwrap();
        let size = 8 * 1024 * 1024;
        let data = Bytes::from((0..size).map(|i| i as u8).collect::<Vec<u8>>());
        let written = TokioFileIO::write_at(&handle, 0, data.clone())
            .await
            .unwrap();
        assert_eq!(written, size);
        TokioFileIO::sync_data(&handle).await.unwrap();
        let read = TokioFileIO::read_at(&handle, 0, size).await.unwrap();
        assert_eq!(
            read.len(),
            size,
            "read_at must fill the entire buffer, not short-read"
        );
        assert_eq!(&read[..], &data[..]);
    }
}
