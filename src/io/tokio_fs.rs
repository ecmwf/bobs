use super::FileIO;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

/// TokioFileIO: FileIO implementation using tokio::fs::File.
/// Uses Arc<Mutex<File>> to allow shared access with seek operations.
#[derive(Clone)]
pub struct TokioFileIO;

impl FileIO for TokioFileIO {
    type Handle = Arc<Mutex<File>>;

    fn create(
        path: &Path,
    ) -> impl std::future::Future<Output = std::io::Result<Self::Handle>> + Send {
        let path = path.to_path_buf();
        async move {
            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .read(true)
                .open(path)
                .await?;
            Ok(Arc::new(Mutex::new(file)))
        }
    }

    fn open(
        path: &Path,
    ) -> impl std::future::Future<Output = std::io::Result<Self::Handle>> + Send {
        let path = path.to_path_buf();
        async move {
            let file = tokio::fs::OpenOptions::new()
                .write(true)
                .read(true)
                .open(path)
                .await?;
            Ok(Arc::new(Mutex::new(file)))
        }
    }

    fn write_at(
        handle: &Self::Handle,
        offset: u64,
        data: &[u8],
    ) -> impl std::future::Future<Output = std::io::Result<usize>> + Send {
        let handle = Arc::clone(handle);
        let data = data.to_vec();
        async move {
            let mut file = handle.lock().await;
            file.seek(std::io::SeekFrom::Start(offset)).await?;
            file.write_all(&data).await?;
            Ok(data.len())
        }
    }

    fn read_at(
        handle: &Self::Handle,
        offset: u64,
        buf: &mut [u8],
    ) -> impl std::future::Future<Output = std::io::Result<usize>> + Send {
        let handle = Arc::clone(handle);
        async move {
            let mut file = handle.lock().await;
            file.seek(std::io::SeekFrom::Start(offset)).await?;
            file.read(buf).await
        }
    }

    fn sync_data(
        handle: &Self::Handle,
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        let handle = Arc::clone(handle);
        async move {
            let file = handle.lock().await;
            file.sync_data().await
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
        let written = TokioFileIO::write_at(&handle, 0, data)
            .await
            .expect("failed to write");
        assert_eq!(written, data.len());

        // Sync to ensure data is written
        TokioFileIO::sync_data(&handle)
            .await
            .expect("failed to sync");

        // Read back the data
        let mut buf = vec![0u8; data.len()];
        let read = TokioFileIO::read_at(&handle, 0, &mut buf)
            .await
            .expect("failed to read");
        assert_eq!(read, data.len());
        assert_eq!(&buf, data);

        // Write at offset 5
        let offset_data = b"WORLD";
        let written = TokioFileIO::write_at(&handle, 5, offset_data)
            .await
            .expect("failed to write at offset");
        assert_eq!(written, offset_data.len());

        TokioFileIO::sync_data(&handle)
            .await
            .expect("failed to sync");

        // Read back and verify offset write
        let mut buf = vec![0u8; 10];
        let read = TokioFileIO::read_at(&handle, 0, &mut buf)
            .await
            .expect("failed to read");
        assert_eq!(read, 10);
        assert_eq!(&buf, b"helloWORLD");

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
        TokioFileIO::write_at(&handle, 0, data)
            .await
            .expect("failed to write");

        TokioFileIO::sync_data(&handle)
            .await
            .expect("failed to sync");

        // Try to read beyond EOF
        let mut buf = vec![0u8; 100];
        let read = TokioFileIO::read_at(&handle, 0, &mut buf)
            .await
            .expect("failed to read");
        assert_eq!(read, data.len(), "should read only available data");

        // Read at offset beyond EOF
        let mut buf = vec![0u8; 10];
        let read = TokioFileIO::read_at(&handle, 100, &mut buf)
            .await
            .expect("failed to read beyond eof");
        assert_eq!(read, 0, "reading beyond EOF should return 0");

        TokioFileIO::close(handle).await.expect("failed to close");
    }

    #[tokio::test]
    async fn test_remove() {
        let dir = tempdir().expect("failed to create temp dir");
        let file_path = dir.path().join("test_remove.bin");

        let handle = TokioFileIO::create(&file_path)
            .await
            .expect("failed to create file");

        TokioFileIO::write_at(&handle, 0, b"data")
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
