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
    /// Append data at the given offset. Writes are strictly sequential — the offset
    /// must match total_bytes_written exactly. Every accepted non-empty body is
    /// appended to spool.dat before any in-memory state advances. Full pages are
    /// published to the cache from the owned input bytes where possible; the write
    /// buffer is only used to assemble pages that span multiple write calls.
    pub async fn write(&self, offset: u64, data: Bytes) -> Result<()> {
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

    /// Refresh the writer-inactivity anchor when HTTP receives a body frame, even
    /// when the frame is too small to flush the request's pending write batch.
    pub async fn refresh_write_activity(&self) {
        let active = {
            let mut meta = self.metadata.lock().await;
            if matches!(meta.state, SpoolState::Writing | SpoolState::WriteLocked) {
                meta.last_write_at = now_secs();
                true
            } else {
                false
            }
        };
        if active {
            self.record_write_activity();
        }
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
                cache.insert(&self.key, page_idx, page_bytes);
                meta.total_pages += 1;
            }
        }

        meta.total_bytes_written = total_bytes_written;
        meta.last_write_at = now;
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
    use std::sync::Arc;
    use tempfile::tempdir;

    async fn make_spool(dir: &std::path::Path, page_size: usize) -> Spool<TokioFileIO> {
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

        Spool::new(
            meta,
            handle,
            page_size,
            Arc::new(tokio::sync::Mutex::new(crate::spool::PageCache::new(
                page_size * 256,
            ))),
            metadata_store,
            Arc::new(crate::metrics::BobsMetrics::new(false)),
        )
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
        let got = spool.read_page(0).await.expect("page read should succeed");
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
}
