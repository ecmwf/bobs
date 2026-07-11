// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

#[cfg(not(target_os = "linux"))]
compile_error!("UringFileIO is only available on Linux; use the tokio-fileio-fallback feature on this platform");

use super::{ring_pool, FileIO};
use bytes::Bytes;
use std::ffi::CString;
use std::io::{Error, ErrorKind, Result};
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::oneshot;

const O_CREAT: i32 = 0o100;
const O_TRUNC: i32 = 0o1000;
const O_RDWR: i32 = 0o2;
const CREATE_MODE: u32 = 0o666;

#[derive(Clone)]
pub struct UringFileIO;

#[derive(Clone)]
#[cfg_attr(not(test), allow(dead_code))]
pub struct UringFileHandle {
    fd: Arc<OwnedFd>,
    pool: Arc<ring_pool::RingPool>,
    ring_index: usize,
    routed_key: String,
}

impl UringFileHandle {
    fn new(
        fd: Arc<OwnedFd>,
        pool: Arc<ring_pool::RingPool>,
        ring_index: usize,
        routed_key: String,
    ) -> Self {
        Self {
            fd,
            pool,
            ring_index,
            routed_key,
        }
    }
}

impl AsRawFd for UringFileHandle {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.fd.as_raw_fd()
    }
}

pub fn initialize_production_ring_pool(
    configured_shards: Option<usize>,
    queue_capacity: usize,
) -> Result<ring_pool::RingPoolStartup> {
    ring_pool::initialize_production_ring_pool(configured_shards, queue_capacity)
}

pub fn init_global_ring_pool(
    configured_shards: Option<usize>,
    queue_capacity: usize,
) -> Result<ring_pool::RingPoolStartup> {
    ring_pool::init_global_ring_pool(configured_shards, queue_capacity)
}

#[allow(dead_code)]
pub(crate) fn global_ring_pool() -> Result<Arc<ring_pool::RingPool>> {
    ring_pool::global_ring_pool()
}

pub fn shutdown_global_ring_pool_for_exit() -> Result<Option<ring_pool::RingPoolShutdown>> {
    ring_pool::shutdown_global_ring_pool_for_exit()
}

impl FileIO for UringFileIO {
    type Handle = UringFileHandle;

    async fn create(path: &Path) -> Result<Self::Handle> {
        submit_open(path, O_RDWR | O_CREAT | O_TRUNC, CREATE_MODE).await
    }

    async fn open(path: &Path) -> Result<Self::Handle> {
        submit_open(path, O_RDWR, 0).await
    }

    async fn write_at(handle: &Self::Handle, offset: u64, data: Bytes) -> Result<usize> {
        let (tx, rx) = oneshot::channel();
        let pool = Arc::clone(&handle.pool);
        #[cfg(test)]
        record_handle_routing(
            pool.as_ref(),
            ring_pool::RingPoolOperationKind::DataWrite,
            handle,
        );
        pool.submit_to_ring(
            handle.ring_index,
            ring_pool::Request::Write {
                fd: Arc::clone(&handle.fd),
                offset,
                data,
                tx,
            },
        )
        .await?;
        recv_result(rx).await
    }

    async fn read_at(handle: &Self::Handle, offset: u64, len: usize) -> Result<Bytes> {
        if len > u32::MAX as usize {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "io_uring reads are limited to u32::MAX bytes",
            ));
        }
        let (tx, rx) = oneshot::channel();
        let pool = Arc::clone(&handle.pool);
        #[cfg(test)]
        record_handle_routing(
            pool.as_ref(),
            ring_pool::RingPoolOperationKind::DataRead,
            handle,
        );
        pool.submit_to_ring(
            handle.ring_index,
            ring_pool::Request::Read {
                fd: Arc::clone(&handle.fd),
                offset,
                len,
                tx,
            },
        )
        .await?;
        recv_result(rx).await
    }

    async fn sync_data(handle: &Self::Handle) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let pool = Arc::clone(&handle.pool);
        #[cfg(test)]
        record_handle_routing(
            pool.as_ref(),
            ring_pool::RingPoolOperationKind::DataSync,
            handle,
        );
        pool.submit_to_ring(
            handle.ring_index,
            ring_pool::Request::SyncData {
                fd: Arc::clone(&handle.fd),
                tx,
            },
        )
        .await?;
        recv_result(rx).await
    }

    async fn close(handle: Self::Handle) -> Result<()> {
        drop(handle);
        Ok(())
    }

    async fn remove(path: &Path) -> Result<()> {
        let (routed_key, routing_bytes) = data_path_routing_key(path);
        #[cfg(not(test))]
        let _ = &routed_key;
        let path = path_to_cstring(path)?;
        let pool = ring_pool::global_or_default_ring_pool()?;
        let ring_index = ring_pool::ring_index_for_key_bytes(&routing_bytes, pool.shard_count());
        #[cfg(test)]
        pool.record_routing(
            ring_pool::RingPoolOperationKind::DataOpen,
            routed_key,
            ring_index,
        );
        let (tx, rx) = oneshot::channel();
        pool.submit_to_ring(ring_index, ring_pool::Request::Remove { path, tx })
            .await?;
        recv_result(rx).await
    }
}

async fn recv_result<T>(rx: oneshot::Receiver<Result<T>>) -> Result<T> {
    rx.await.map_err(|_| {
        Error::new(
            ErrorKind::BrokenPipe,
            "io_uring FileIO driver dropped request",
        )
    })?
}

fn path_to_cstring(path: &Path) -> Result<CString> {
    CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        Error::new(
            ErrorKind::InvalidInput,
            format!("path contains an interior NUL byte: {}", path.display()),
        )
    })
}

async fn submit_open(path: &Path, flags: i32, mode: u32) -> Result<UringFileHandle> {
    let (routed_key, routing_bytes) = data_path_routing_key(path);
    let c_path = path_to_cstring(path)?;
    let pool = ring_pool::global_or_default_ring_pool()?;
    let ring_index = ring_pool::ring_index_for_key_bytes(&routing_bytes, pool.shard_count());
    #[cfg(test)]
    pool.record_routing(
        if flags & O_CREAT != 0 {
            ring_pool::RingPoolOperationKind::DataCreate
        } else {
            ring_pool::RingPoolOperationKind::DataOpen
        },
        routed_key.clone(),
        ring_index,
    );

    let (tx, rx) = oneshot::channel();
    pool.submit_to_ring(
        ring_index,
        ring_pool::Request::Open {
            path: c_path,
            flags,
            mode,
            tx,
        },
    )
    .await?;
    let fd = recv_result(rx).await?;
    Ok(UringFileHandle::new(fd, pool, ring_index, routed_key))
}

fn data_path_routing_key(path: &Path) -> (String, Vec<u8>) {
    if path.file_name().map(|name| name.as_bytes()) == Some(b"spool.dat") {
        if let Some(key) = path.parent().and_then(Path::file_name) {
            let key_bytes = key.as_bytes().to_vec();
            return (String::from_utf8_lossy(&key_bytes).into_owned(), key_bytes);
        }
    }

    let path_bytes = path.as_os_str().as_bytes().to_vec();
    (
        String::from_utf8_lossy(&path_bytes).into_owned(),
        path_bytes,
    )
}

#[cfg(test)]
fn record_handle_routing(
    pool: &ring_pool::RingPool,
    operation_kind: ring_pool::RingPoolOperationKind,
    handle: &UringFileHandle,
) {
    pool.record_routing(operation_kind, handle.routed_key.clone(), handle.ring_index);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::fileio_test_cases::FileIOTestSuite;
    use crate::io::ring_pool::{
        ring_index_for_key, ring_index_for_key_bytes, scoped_test_ring_pool_override, RingPool,
        RingPoolOperationKind, RingPoolOptions,
    };
    use bytes::Bytes;
    use std::collections::VecDeque;
    use std::os::unix::ffi::OsStrExt;
    use std::sync::Arc;
    use tempfile::tempdir;

    type Suite = FileIOTestSuite<UringFileIO>;

    fn explicit_test_options(shard_count: usize) -> RingPoolOptions {
        RingPoolOptions {
            shard_count,
            queue_capacity: 1024,
            driver_name_prefix: "bobs-uring-routing-test".to_owned(),
        }
    }

    #[tokio::test]
    async fn ring_pool_per_spool_routing() {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let key = "routing-key-a";
        let spool_dir = data_dir.join(key);
        std::fs::create_dir_all(&spool_dir).expect("create spool dir");
        let spool_path = spool_dir.join("spool.dat");

        let fallback_path = dir.path().join("standalone-file.bin");
        let (fallback_key, fallback_bytes) = data_path_routing_key(&fallback_path);
        assert_eq!(
            fallback_key.as_bytes(),
            fallback_path.as_os_str().as_bytes()
        );
        assert_eq!(
            ring_index_for_key_bytes(&fallback_bytes, 4),
            ring_index_for_key_bytes(fallback_path.as_os_str().as_bytes(), 4),
            "non-spool paths must fall back to hashing the full path bytes"
        );

        let pool = Arc::new(
            RingPool::new_for_test(explicit_test_options(4))
                .expect("routing test ring pool should start"),
        );
        let _override = scoped_test_ring_pool_override(Arc::clone(&pool));

        let handle = UringFileIO::create(&spool_path)
            .await
            .expect("create spool data file");
        UringFileIO::write_at(&handle, 0, Bytes::from_static(b"abc"))
            .await
            .expect("first write");
        UringFileIO::write_at(&handle, 3, Bytes::from_static(b"def"))
            .await
            .expect("second write");
        assert_eq!(
            UringFileIO::read_at(&handle, 0, 6)
                .await
                .expect("read spool data"),
            Bytes::from_static(b"abcdef")
        );
        UringFileIO::sync_data(&handle)
            .await
            .expect("sync spool data");

        let metadata_ring_index = ring_index_for_key(key, pool.shard_count());
        pool.record_routing(
            RingPoolOperationKind::MetadataCommit,
            key,
            metadata_ring_index,
        );

        let expected_ring_index = ring_index_for_key(key, pool.shard_count());
        let events = pool.routing_events();
        let relevant: Vec<_> = events
            .iter()
            .filter(|event| {
                matches!(
                    event.operation_kind,
                    RingPoolOperationKind::DataCreate
                        | RingPoolOperationKind::DataWrite
                        | RingPoolOperationKind::DataRead
                        | RingPoolOperationKind::DataSync
                        | RingPoolOperationKind::MetadataCommit
                )
            })
            .collect();

        assert_eq!(
            relevant
                .iter()
                .map(|event| event.operation_kind)
                .collect::<Vec<_>>(),
            vec![
                RingPoolOperationKind::DataCreate,
                RingPoolOperationKind::DataWrite,
                RingPoolOperationKind::DataWrite,
                RingPoolOperationKind::DataRead,
                RingPoolOperationKind::DataSync,
                RingPoolOperationKind::MetadataCommit,
            ]
        );
        for event in relevant {
            assert_eq!(
                event.routed_key, key,
                "{event:?} used the wrong routing key"
            );
            assert_eq!(
                event.ring_index, expected_ring_index,
                "{event:?} routed to the wrong ring index"
            );
        }
    }

    #[tokio::test]
    async fn io_uring_fileio_write_read_at_offset() {
        Suite::write_read_at_offset().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn io_uring_fileio_parallel_disjoint_writes() {
        Suite::parallel_disjoint_writes().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn io_uring_fileio_parallel_reads_from_same_file_return_owned_bytes() {
        Suite::parallel_reads_from_same_file_return_owned_bytes().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn io_uring_fileio_parallel_reads_while_writes_append_later_offsets() {
        Suite::parallel_reads_while_writes_append_later_offsets().await;
    }

    #[tokio::test]
    async fn io_uring_fileio_read_beyond_eof_returns_available_then_empty_bytes() {
        Suite::read_beyond_eof_returns_available_then_empty_bytes().await;
    }

    #[tokio::test]
    async fn io_uring_fileio_remove_unlinks_file() {
        Suite::remove_unlinks_file().await;
    }

    #[tokio::test]
    async fn io_uring_fileio_close_and_drop_are_safe() {
        Suite::close_and_drop_are_safe().await;
    }

    #[derive(Debug)]
    enum FakeCompletion {
        Write(usize),
        Fsync(Result<()>),
    }

    #[derive(Debug)]
    struct FakeSubmitter {
        completions: VecDeque<FakeCompletion>,
    }

    impl FakeSubmitter {
        fn short_writes(chunks: impl IntoIterator<Item = usize>) -> Self {
            Self {
                completions: chunks.into_iter().map(FakeCompletion::Write).collect(),
            }
        }

        fn fsync_error(error: Error) -> Self {
            Self {
                completions: VecDeque::from([FakeCompletion::Fsync(Err(error))]),
            }
        }

        fn write_all(&mut self, requested: Bytes) -> Result<usize> {
            let mut accepted = 0usize;
            while accepted < requested.len() {
                let completed = match self.completions.pop_front() {
                    Some(FakeCompletion::Write(n)) => n,
                    Some(FakeCompletion::Fsync(_)) => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "fake submitter returned fsync completion for write request",
                        ));
                    }
                    None => {
                        return Err(Error::new(
                            ErrorKind::UnexpectedEof,
                            "fake submitter ran out of write completions",
                        ));
                    }
                };
                if completed == 0 {
                    return Err(Error::new(
                        ErrorKind::WriteZero,
                        "zero-length write completion",
                    ));
                }
                accepted += completed;
            }
            Ok(accepted)
        }

        fn next_fsync(&mut self) -> Result<()> {
            match self.completions.pop_front() {
                Some(FakeCompletion::Fsync(result)) => result,
                Some(FakeCompletion::Write(_)) => Err(Error::new(
                    ErrorKind::InvalidData,
                    "fake submitter returned write completion for fsync request",
                )),
                None => Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "fake submitter ran out of fsync completions",
                )),
            }
        }
    }

    #[tokio::test]
    async fn io_uring_fileio_retries_short_write_completions_until_full_buffer_accepted() {
        let mut fake = FakeSubmitter::short_writes([2, 1, 4]);
        let requested = Bytes::from_static(b"abcdefg");
        let accepted = fake
            .write_all(requested.clone())
            .expect("fake write completion failed");
        assert_eq!(accepted, requested.len());
    }

    #[tokio::test]
    async fn io_uring_fileio_propagates_fsync_completion_errors() {
        let error = Error::other("injected fsync failure");
        let mut fake = FakeSubmitter::fsync_error(error);
        let observed = fake.next_fsync().expect_err("fake fsync should fail");
        assert_eq!(observed.kind(), ErrorKind::Other);
    }
}
