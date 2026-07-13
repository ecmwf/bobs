// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use bytes::Bytes;

use crate::error::{BobsError, Result};
use crate::io::FileIO;
use crate::spool::{Spool, SpoolState};
use crate::time::now_secs;

impl<F, M> Spool<F, M>
where
    F: FileIO,
    M: crate::metadata::MetadataStore + Clone + Send + Sync + 'static,
{
    /// Refresh writer activity for an accepted HTTP body frame, including frames
    /// that are too small to flush through [`Self::write`]. The lifecycle lock
    /// makes the state check and monotonic timestamp update atomic with cleanup
    /// revalidation and deletion.
    pub async fn refresh_write_activity(&self, now: u64) -> Result<()> {
        let _lifecycle_guard = self.lifecycle_lock.lock().await;
        let mut meta = self.metadata.lock().await;
        match meta.state {
            SpoolState::Writing | SpoolState::WriteLocked => {
                meta.last_write_at = meta.last_write_at.max(now);
                self.record_write_activity();
                Ok(())
            }
            SpoolState::Complete => Err(BobsError::SpoolClosed),
            SpoolState::Deleting => Err(BobsError::SpoolNotFound {
                key: self.key.clone(),
            }),
            ref other => Err(BobsError::InvalidState {
                current: format!("{other:?}"),
                attempted_action: "refresh write activity".to_string(),
            }),
        }
    }

    /// Append data at the given offset. Waiting for the per-spool operation gate is
    /// cancellation-safe and does not spawn work. After admission, an owned task keeps
    /// the gate and lifecycle lock until backend I/O and all publication have reached
    /// a stable success or failure, even if the caller disappears.
    pub async fn write(self: &std::sync::Arc<Self>, offset: u64, data: Bytes) -> Result<()> {
        let permit = std::sync::Arc::clone(&self.operation_gate)
            .acquire_owned()
            .await
            .map_err(|_| {
                BobsError::IoError(std::io::Error::other("spool operation gate closed"))
            })?;
        let spool = std::sync::Arc::clone(self);
        tokio::spawn(async move { spool.write_transaction(offset, data, permit).await })
            .await
            .map_err(|error| BobsError::StorageError(Box::new(error)))?
    }

    async fn write_transaction(
        &self,
        offset: u64,
        data: Bytes,
        _permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<()> {
        // Lock order for mutation is operation gate -> lifecycle -> write buffer.
        // Cleanup/read activity take lifecycle only; no path takes these in reverse.
        let _lifecycle_guard = self.lifecycle_lock.lock().await;
        // This lock is the spool's write/delete linearization gate. A write is
        // either fully published before deletion starts or observes Deleting.
        let mut buf = self.write_buffer.lock().await;

        {
            let meta = self.metadata.lock().await;
            match meta.state {
                SpoolState::Writing | SpoolState::WriteLocked => {}
                SpoolState::Complete => return Err(BobsError::SpoolClosed),
                ref other => {
                    return Err(BobsError::InvalidState {
                        current: format!("{other:?}"),
                        attempted_action: "write".to_string(),
                    });
                }
            }

            // Enforce sequential appends — no gaps or overwrites.
            if offset != meta.total_bytes_written {
                return Err(BobsError::OffsetMismatch {
                    expected: meta.total_bytes_written,
                    got: offset,
                });
            }
        }

        if data.is_empty() {
            return Ok(());
        }

        let written = F::write_at(&self.file_handle, offset, data.clone())
            .await
            .map_err(BobsError::IoError)?;
        if written != data.len() {
            return Err(BobsError::IoError(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                format!("short write: wrote {written} of {} bytes", data.len()),
            )));
        }

        let mut cursor = 0;
        let mut completed_pages = Vec::new();

        // If a previous call left a partial page, copy only enough incoming bytes
        // to finish that page. Complete pages wholly contained in `data` are
        // published below as zero-copy Bytes slices.
        if !buf.is_empty() {
            let needed = self.page_size - buf.len();
            let take = needed.min(data.len());
            buf.extend_from_slice(&data[..take]);
            cursor += take;

            if buf.len() == self.page_size {
                completed_pages.push(buf.split_to(self.page_size).freeze());
            }
        }

        // Publish full pages from the incoming owned buffer without cloning their
        // contents. Only cross-call partial pages use `write_buffer` assembly.
        while cursor + self.page_size <= data.len() {
            completed_pages.push(data.slice(cursor..cursor + self.page_size));
            cursor += self.page_size;
        }

        if cursor < data.len() {
            buf.extend_from_slice(&data[cursor..]);
        }

        self.publish_write(completed_pages, offset + data.len() as u64, now_secs())
            .await;
        self.record_write_activity();

        Ok(())
    }

    /// Publish all metadata for one accepted disk append under one metadata lock.
    /// Readers can therefore never observe new pages with a stale byte count.
    async fn publish_write(&self, pages: Vec<Bytes>, total_bytes_written: u64, now: u64) {
        let published_pages = !pages.is_empty();
        let mut meta = self.metadata.lock().await;

        if published_pages {
            let mut cache = self.page_cache.lock().await;
            for page_bytes in pages {
                let page_idx = meta.total_pages;
                // Admission makes one cache-only page-sized copy. The disk append
                // above consumed the transport-backed Bytes directly; zero-capacity
                // or too-small caches return here without copying.
                cache.insert(&self.key, page_idx, page_bytes);
                meta.total_pages += 1;
            }
        }

        meta.total_bytes_written = total_bytes_written;
        meta.last_write_at = meta.last_write_at.max(now);
        drop(meta);

        if published_pages {
            self.notify.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{FileIO, TokioFileIO};
    use crate::metadata::{MetadataStore, SyncSidecarMetadataStore};
    use crate::spool::SpoolMetadata;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex, OnceLock};
    use tempfile::tempdir;

    #[derive(Default)]
    struct DelayedWriteControl {
        delay_next: AtomicBool,
        write_calls: AtomicUsize,
        active_owned_writes: AtomicUsize,
        max_active_owned_writes: AtomicUsize,
        started: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    static DELAYED_WRITE_CONTROL: OnceLock<StdMutex<Option<Arc<DelayedWriteControl>>>> =
        OnceLock::new();

    fn delayed_write_slot() -> &'static StdMutex<Option<Arc<DelayedWriteControl>>> {
        DELAYED_WRITE_CONTROL.get_or_init(|| StdMutex::new(None))
    }

    fn install_delayed_write_control() -> Arc<DelayedWriteControl> {
        let control = Arc::new(DelayedWriteControl {
            delay_next: AtomicBool::new(true),
            ..DelayedWriteControl::default()
        });
        *delayed_write_slot()
            .lock()
            .expect("delayed write control mutex poisoned") = Some(Arc::clone(&control));
        control
    }

    #[derive(Clone)]
    struct DelayedOwnedWriteFileIO;

    impl FileIO for DelayedOwnedWriteFileIO {
        type Handle = <TokioFileIO as FileIO>::Handle;

        fn create(
            path: &std::path::Path,
        ) -> impl std::future::Future<Output = std::io::Result<Self::Handle>> + Send {
            TokioFileIO::create(path)
        }

        fn open(
            path: &std::path::Path,
        ) -> impl std::future::Future<Output = std::io::Result<Self::Handle>> + Send {
            TokioFileIO::open(path)
        }

        fn write_at(
            handle: &Self::Handle,
            offset: u64,
            data: Bytes,
        ) -> impl std::future::Future<Output = std::io::Result<usize>> + Send {
            let handle = Arc::clone(handle);
            let control = delayed_write_slot()
                .lock()
                .expect("delayed write control mutex poisoned")
                .clone();
            async move {
                let Some(control) = control else {
                    return TokioFileIO::write_at(&handle, offset, data).await;
                };
                control.write_calls.fetch_add(1, Ordering::SeqCst);
                if !control.delay_next.swap(false, Ordering::SeqCst) {
                    return TokioFileIO::write_at(&handle, offset, data).await;
                }

                // Model an owned io_uring operation: dropping this returned future
                // detaches, rather than cancels, the submitted backend write.
                let owned_control = Arc::clone(&control);
                tokio::spawn(async move {
                    let active = owned_control
                        .active_owned_writes
                        .fetch_add(1, Ordering::SeqCst)
                        + 1;
                    owned_control
                        .max_active_owned_writes
                        .fetch_max(active, Ordering::SeqCst);
                    owned_control.started.notify_one();
                    owned_control.release.notified().await;
                    let result = TokioFileIO::write_at(&handle, offset, data).await;
                    owned_control
                        .active_owned_writes
                        .fetch_sub(1, Ordering::SeqCst);
                    result
                })
                .await
                .map_err(std::io::Error::other)?
            }
        }

        fn read_at(
            handle: &Self::Handle,
            offset: u64,
            len: usize,
        ) -> impl std::future::Future<Output = std::io::Result<Bytes>> + Send {
            TokioFileIO::read_at(handle, offset, len)
        }

        fn sync_data(
            handle: &Self::Handle,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            TokioFileIO::sync_data(handle)
        }

        fn sync_directory(
            path: &std::path::Path,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            TokioFileIO::sync_directory(path)
        }

        fn close(
            handle: Self::Handle,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            TokioFileIO::close(handle)
        }

        fn remove(
            path: &std::path::Path,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            TokioFileIO::remove(path)
        }
    }

    async fn make_spool(dir: &std::path::Path, page_size: usize) -> Arc<Spool<TokioFileIO>> {
        let spool_dir = dir.join("test-key");
        tokio::fs::create_dir_all(&spool_dir)
            .await
            .expect("create spool dir");
        let path = spool_dir.join("spool.dat");
        let metadata_store = SyncSidecarMetadataStore::new(dir);
        let handle = TokioFileIO::create(&path)
            .await
            .expect("failed to create spool file");
        let meta = SpoolMetadata {
            key: "test-key".to_string(),
            content_type: None,
            content_encoding: None,
            state: SpoolState::Writing,
            write_locked: false,
            created_at: 0,
            last_write_at: 0,
            last_read_at: None,
            readable_at: None,
            page_size: page_size as u64,
            total_bytes_written: 0,
            total_pages: 0,
            final_page_size: None,
            data_path: path,
            labels: HashMap::new(),
        };
        metadata_store
            .write(&meta)
            .await
            .expect("insert initial metadata");

        Arc::new(Spool::new(
            meta,
            handle,
            page_size,
            Arc::new(tokio::sync::Mutex::new(crate::spool::PageCache::new(
                page_size * 256,
            ))),
            metadata_store,
            Arc::new(crate::metrics::BobsMetrics::new(false)),
        ))
    }

    async fn persisted_metadata(spool: &Spool<TokioFileIO>) -> SpoolMetadata {
        spool
            .metadata_store
            .read("test-key")
            .await
            .expect("read metadata")
            .expect("metadata exists")
    }

    #[tokio::test]
    async fn test_write_single_page() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;
        let data = vec![0xABu8; 4096];

        spool
            .write(0, bytes::Bytes::copy_from_slice(&data))
            .await
            .expect("write should succeed");

        let meta = spool.metadata.lock().await;
        assert_eq!(meta.total_pages, 1);
        assert_eq!(meta.total_bytes_written, 4096);
        drop(meta);

        let cache = spool.page_cache.lock().await;
        assert!(cache.contains(&spool.key, 0));
    }

    #[tokio::test]
    async fn test_write_multiple_pages() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;
        let data = vec![0xCDu8; 16384];

        spool
            .write(0, bytes::Bytes::copy_from_slice(&data))
            .await
            .expect("write should succeed");

        let meta = spool.metadata.lock().await;
        assert_eq!(meta.total_pages, 4);
        assert_eq!(meta.total_bytes_written, 16384);
    }

    #[tokio::test]
    async fn test_page_and_byte_publication_share_one_metadata_lock() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = Arc::new(make_spool(dir.path(), 4096).await);
        let cache_guard = spool.page_cache.lock().await;
        let writer_spool = Arc::clone(&spool);
        let write_task =
            tokio::spawn(async move { writer_spool.write(0, Bytes::from(vec![0xCD; 4096])).await });

        // The disk append precedes publication. Once it is visible, give the writer
        // time to reach the deliberately blocked cache insertion.
        loop {
            let len = tokio::fs::metadata(&spool.data_path)
                .await
                .expect("stat spool data")
                .len();
            if len == 4096 {
                break;
            }
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), spool.metadata.lock())
                .await
                .is_err(),
            "metadata must stay locked while page cache publication is pending"
        );

        drop(cache_guard);
        write_task
            .await
            .expect("write task join")
            .expect("write succeeds");
        let metadata = spool.metadata.lock().await;
        assert_eq!(metadata.total_pages, 1);
        assert_eq!(metadata.total_bytes_written, 4096);
    }

    #[tokio::test]
    async fn test_write_partial_page() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;
        let data = vec![0xEFu8; 1000];

        spool
            .write(0, bytes::Bytes::copy_from_slice(&data))
            .await
            .expect("write should succeed");

        let meta = spool.metadata.lock().await;
        assert_eq!(meta.total_pages, 0);
        assert_eq!(meta.total_bytes_written, 1000);
    }

    #[tokio::test]
    async fn test_write_persists_partial_without_metadata_then_publishes_completed_page() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;
        let first = vec![0x11u8; 1000];
        let second = vec![0x22u8; 3096];

        spool
            .write(0, bytes::Bytes::copy_from_slice(&first))
            .await
            .expect("partial write should succeed");

        let file_len = tokio::fs::metadata(&spool.data_path)
            .await
            .expect("stat spool data file")
            .len();
        assert_eq!(file_len, first.len() as u64);

        let persisted = persisted_metadata(&spool).await;
        assert_eq!(persisted.total_bytes_written, 0);
        assert_eq!(persisted.total_pages, 0);
        assert_eq!(persisted.last_write_at, 0);

        spool
            .write(first.len() as u64, bytes::Bytes::copy_from_slice(&second))
            .await
            .expect("remainder write should succeed");

        let mut expected = first;
        expected.extend_from_slice(&second);
        let got = spool
            .read_page_for_test(0)
            .await
            .expect("page read should succeed");
        assert_eq!(got, Some(bytes::Bytes::from(expected)));
    }

    #[tokio::test]
    async fn test_write_offset_mismatch() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        let result = spool
            .write(100, bytes::Bytes::copy_from_slice(&[0u8; 100]))
            .await;
        assert!(matches!(
            result,
            Err(BobsError::OffsetMismatch {
                expected: 0,
                got: 100
            })
        ));
    }

    #[tokio::test]
    async fn test_producer_cannot_rewind_after_accepted_partial_write() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;
        let first = bytes::Bytes::from_static(b"abc");

        spool
            .write(0, first)
            .await
            .expect("initial partial write should be accepted");

        let result = spool.write(0, bytes::Bytes::from_static(b"rewind")).await;
        assert!(matches!(
            result,
            Err(BobsError::OffsetMismatch {
                expected: 3,
                got: 0
            })
        ));

        let meta = spool.metadata.lock().await;
        assert_eq!(meta.total_bytes_written, 3);
        assert_eq!(meta.total_pages, 0, "partial page remains unpublished");
    }

    #[tokio::test]
    async fn test_write_empty_is_noop() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        spool
            .write(0, bytes::Bytes::new())
            .await
            .expect("empty write should succeed");

        let meta = spool.metadata.lock().await;
        assert_eq!(meta.total_pages, 0);
        assert_eq!(meta.total_bytes_written, 0);
    }

    #[tokio::test]
    async fn test_write_after_close_fails() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;

        {
            let mut meta = spool.metadata.lock().await;
            meta.state = SpoolState::Complete;
        }

        let result = spool
            .write(0, bytes::Bytes::copy_from_slice(&[1, 2, 3]))
            .await;
        assert!(matches!(result, Err(BobsError::SpoolClosed)));
    }

    #[tokio::test]
    async fn cancelled_owned_backend_write_blocks_retry_and_completion_until_stable() {
        let dir = tempdir().expect("failed to create tempdir");
        let control = install_delayed_write_control();
        let manager =
            crate::manager::SpoolManager::<DelayedOwnedWriteFileIO>::new(dir.path(), 4, 1024, 8)
                .expect("create manager");
        manager
            .create_spool("cancel-safe".to_string(), None, None, false, HashMap::new())
            .await
            .expect("create spool");
        let spool = manager.get_spool("cancel-safe").expect("spool exists");

        let old_spool = Arc::clone(&spool);
        let old_caller =
            tokio::spawn(async move { old_spool.write(0, Bytes::from_static(b"AAAA")).await });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            control.started.notified(),
        )
        .await
        .expect("old owned backend write started");
        old_caller.abort();
        assert!(old_caller
            .await
            .expect_err("old caller should be cancelled")
            .is_cancelled());
        assert_eq!(control.active_owned_writes.load(Ordering::SeqCst), 1);

        let retry_spool = Arc::clone(&spool);
        let retry =
            tokio::spawn(async move { retry_spool.write(0, Bytes::from_static(b"BBBB")).await });
        tokio::task::yield_now().await;
        assert!(
            !retry.is_finished(),
            "retry must backpressure behind old I/O"
        );

        let complete_spool = Arc::clone(&spool);
        let complete = tokio::spawn(async move { complete_spool.complete(Some(4)).await });
        tokio::task::yield_now().await;
        assert!(
            !complete.is_finished(),
            "completion must backpressure behind old I/O and queued retry"
        );
        assert_eq!(spool.operation_gate.available_permits(), 0);
        assert_eq!(control.write_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            control.max_active_owned_writes.load(Ordering::SeqCst),
            1,
            "only one owned backend write may be active"
        );

        control.release.notify_one();
        let retry_result = tokio::time::timeout(std::time::Duration::from_secs(5), retry)
            .await
            .expect("retry should finish after old I/O")
            .expect("retry task joins");
        assert!(matches!(
            retry_result,
            Err(BobsError::OffsetMismatch {
                expected: 4,
                got: 0
            })
        ));
        tokio::time::timeout(std::time::Duration::from_secs(5), complete)
            .await
            .expect("completion should finish after retry")
            .expect("completion task joins")
            .expect("completion succeeds");
        assert_eq!(control.write_calls.load(Ordering::SeqCst), 1);
        assert_eq!(control.active_owned_writes.load(Ordering::SeqCst), 0);

        assert_eq!(
            tokio::fs::read(dir.path().join("cancel-safe/spool.dat"))
                .await
                .expect("read disk bytes"),
            b"AAAA"
        );
        assert_eq!(
            spool
                .read_page_for_test(0)
                .await
                .expect("read cached page")
                .expect("cached page exists")
                .as_ref(),
            b"AAAA"
        );
        assert_eq!(spool.metadata.lock().await.state, SpoolState::Complete);

        drop(spool);
        drop(manager);
        let restarted = crate::manager::SpoolManager::<TokioFileIO>::new(dir.path(), 4, 1024, 8)
            .expect("create restarted manager");
        restarted.recover().await.expect("recover completed spool");
        let recovered = restarted.get_spool("cancel-safe").expect("recover spool");
        assert_eq!(recovered.metadata.lock().await.state, SpoolState::Complete);
        assert_eq!(
            recovered
                .read_page_for_test(0)
                .await
                .expect("read recovered page")
                .expect("recovered page exists")
                .as_ref(),
            b"AAAA"
        );
        assert_eq!(
            tokio::fs::read(dir.path().join("cancel-safe/spool.dat"))
                .await
                .expect("read restarted disk bytes"),
            b"AAAA"
        );

        *delayed_write_slot()
            .lock()
            .expect("delayed write control mutex poisoned") = None;
    }

    #[tokio::test]
    async fn frame_refresh_is_monotonic_and_rejects_terminal_states() {
        let dir = tempdir().expect("failed to create tempdir");
        let spool = make_spool(dir.path(), 4096).await;
        spool.metadata.lock().await.last_write_at = 10;

        spool
            .refresh_write_activity(9)
            .await
            .expect("writable spool accepts frame refresh");
        assert_eq!(spool.metadata.lock().await.last_write_at, 10);
        spool
            .refresh_write_activity(11)
            .await
            .expect("newer frame advances activity");
        assert_eq!(spool.metadata.lock().await.last_write_at, 11);

        spool.metadata.lock().await.state = SpoolState::Completing;
        assert!(matches!(
            spool.refresh_write_activity(12).await,
            Err(BobsError::InvalidState { .. })
        ));

        spool.metadata.lock().await.state = SpoolState::Complete;
        assert!(matches!(
            spool.refresh_write_activity(12).await,
            Err(BobsError::SpoolClosed)
        ));

        spool.metadata.lock().await.state = SpoolState::Deleting;
        assert!(matches!(
            spool.refresh_write_activity(12).await,
            Err(BobsError::SpoolNotFound { .. })
        ));
        assert_eq!(
            spool.metadata.lock().await.last_write_at,
            11,
            "rejected frames must not refresh cleanup activity"
        );
    }
}
