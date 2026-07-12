// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use crate::config::Config;
use crate::io::FileIO;
use crate::manager::{DeleteReason, SpoolManager};
use crate::metadata::MetadataStore;
use crate::spool::{SpoolMetadata, SpoolState};
use crate::time::now_secs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};

/// Periodic sweep that reclaims spools which are no longer needed.
///
/// Three independent triggers:
/// - **Writer inactive**: writer has not written for `writer_inactivity_timeout_secs` while
///   the spool is still in Writing or WriteLocked state.
/// - **Full-read TTL**: every byte of the object has been served at least once and
///   `full_read_complete_ttl_secs` has elapsed since the most recent read activity.
/// - **Idle TTL**: spool is Complete and no bytes have been served for
///   `read_idle_ttl_secs`. The idle timer is anchored on the last byte-served activity
///   (`last_read_activity_at`), falling back to `readable_at` from metadata, or `now`
///   (safe: won't delete on this sweep) if neither is known.
///
/// Note: `reader_count` is NOT used as an absolute guard. Stalled connections that serve
/// no bytes will expire via the idle TTL like any other unserved spool.
#[derive(Clone)]
struct CleanupCandidate {
    key: String,
    state: SpoolState,
    last_write_at: u64,
    readable_at: Option<u64>,
    last_read_activity_at: u64,
    full_object_read_at: u64,
    reason: &'static str,
}

fn cleanup_reason(
    metadata: &SpoolMetadata,
    last_read_activity_at: u64,
    full_object_read_at: u64,
    now: u64,
    config: &Config,
) -> Option<&'static str> {
    if metadata.state == SpoolState::Deleting {
        return Some(crate::metrics::reason::DELETE_RETRY);
    }

    let writer_inactive = matches!(
        metadata.state,
        SpoolState::Writing | SpoolState::WriteLocked
    ) && now.saturating_sub(metadata.last_write_at)
        > config.writer_inactivity_timeout_secs;
    if writer_inactive {
        return Some(crate::metrics::reason::WRITER_TIMEOUT);
    }

    if full_object_read_at > 0 {
        let anchor = full_object_read_at.max(last_read_activity_at);
        if now.saturating_sub(anchor) > config.full_read_complete_ttl_secs {
            return Some(crate::metrics::reason::FULL_READ_TTL);
        }
    }

    if metadata.state == SpoolState::Complete {
        let anchor = if last_read_activity_at > 0 {
            last_read_activity_at
        } else {
            metadata.readable_at.unwrap_or(now)
        };
        if now.saturating_sub(anchor) > config.read_idle_ttl_secs {
            return Some(crate::metrics::reason::IDLE_TTL);
        }
    }

    None
}

pub async fn run_cleanup_loop<F, M>(manager: Arc<SpoolManager<F, M>>, config: Arc<Config>)
where
    F: FileIO,
    M: MetadataStore + Clone + Send + Sync + 'static,
{
    let mut interval = time::interval(Duration::from_secs(config.cleanup_sweep_interval_secs));
    // Guards against overlapping disk-usage measurements: at most one walk is
    // ever in flight. Set to true while a spawned measurement task is running;
    // the task clears it when done.
    let disk_measuring = Arc::new(AtomicBool::new(false));

    loop {
        interval.tick().await;
        let started = std::time::Instant::now();
        let now = now_secs();
        let mut to_delete = Vec::new();
        let mut keys = manager.spool_keys();
        keys.sort();
        let mut inspected = 0_u64;
        tracing::debug!(
            "event.name" = "bobs.cleanup.run.started",
            spool_count = keys.len() as u64,
            outcome = "success",
            "cleanup run started"
        );

        for key in keys {
            inspected += 1;
            let Some(spool) = manager.get_spool(&key) else {
                continue;
            };
            let _lifecycle_guard = spool.lifecycle_lock.lock().await;
            let metadata = spool.metadata.lock().await.clone();
            let last_read_activity_at = spool.last_read_activity_at.load(Ordering::SeqCst);
            let full_object_read_at = spool.full_object_read_at.load(Ordering::SeqCst);
            if let Some(reason) = cleanup_reason(
                &metadata,
                last_read_activity_at,
                full_object_read_at,
                now,
                &config,
            ) {
                to_delete.push(CleanupCandidate {
                    key,
                    state: metadata.state,
                    last_write_at: metadata.last_write_at,
                    readable_at: metadata.readable_at,
                    last_read_activity_at,
                    full_object_read_at,
                    reason,
                });
            }
        }

        let mut deleted = 0_u64;
        let mut failed_delete = 0_u64;
        for candidate in to_delete {
            let expected = candidate.clone();
            let key = candidate.key.clone();
            let candidate_config = Arc::clone(&config);
            match manager
                .delete_spool_with_reason_if(
                    &key,
                    DeleteReason::Ttl,
                    move |meta, last_read, full_read| {
                        meta.state == expected.state
                            && meta.last_write_at == expected.last_write_at
                            && meta.readable_at == expected.readable_at
                            && last_read == expected.last_read_activity_at
                            && full_read == expected.full_object_read_at
                            && cleanup_reason(meta, last_read, full_read, now, &candidate_config)
                                == Some(expected.reason)
                    },
                )
                .await
            {
                Ok(Some(labels)) => {
                    deleted += 1;
                    manager
                        .metrics
                        .record_spool_deleted(&labels, candidate.reason);
                }
                Ok(None) => {
                    tracing::debug!(%key, "cleanup candidate invalidated by lifecycle activity");
                }
                Err(err) => {
                    failed_delete += 1;
                    tracing::debug!(%key, error = %err, "cleanup delete failed");
                }
            }
        }
        tracing::info!(
            "event.name" = "bobs.cleanup.run.completed",
            inspected = inspected,
            deleted = deleted,
            failed_delete = failed_delete,
            duration_ms = started.elapsed().as_millis() as u64,
            outcome = if failed_delete == 0 {
                "success"
            } else {
                "error"
            },
            "cleanup run completed"
        );

        // Measure disk usage after cleanup. Spawned so the loop is not
        // blocked on spawn_blocking I/O between sweeps. The AtomicBool gate
        // ensures at most one walk is in flight at a time: if the previous
        // measurement is still running we skip rather than queue another task
        // (which would contend for I/O and cause unbounded task growth on a
        // slow or busy filesystem).
        let m = Arc::clone(&disk_measuring);
        if !m.swap(true, Ordering::AcqRel) {
            let metrics_ref = Arc::clone(&manager.metrics);
            let data_dir_ref = config.data_dir.clone();
            tokio::spawn(async move {
                if let Ok(usage) = measure_disk_usage(&data_dir_ref).await {
                    metrics_ref.record_disk_usage(usage);
                }
                m.store(false, Ordering::Release);
            });
        }
    }
}

pub fn start_cleanup_task<F, M>(
    manager: Arc<SpoolManager<F, M>>,
    config: Arc<Config>,
) -> JoinHandle<()>
where
    F: FileIO,
    M: MetadataStore + Clone + Send + Sync + 'static,
{
    tokio::spawn(run_cleanup_loop(manager, config))
}

/// Walk the data directory and sum file sizes to estimate disk usage.
async fn measure_disk_usage(data_dir: &std::path::Path) -> std::io::Result<u64> {
    let mut total = 0u64;
    let mut read_dir = tokio::fs::read_dir(data_dir).await?;
    while let Some(entry) = read_dir.next_entry().await? {
        if entry.file_type().await?.is_dir() {
            let mut sub_dir = tokio::fs::read_dir(entry.path()).await?;
            while let Some(sub_entry) = sub_dir.next_entry().await? {
                if let Ok(meta) = sub_entry.metadata().await {
                    total += meta.len();
                }
            }
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::TokioFileIO;
    use crate::metadata::MetadataStore;
    use crate::spool::SpoolMetadata;
    use std::collections::HashMap;
    use tempfile::tempdir;

    static BLOCK_NEXT_CLOSE: AtomicBool = AtomicBool::new(false);
    static CLOSE_STARTED: tokio::sync::Notify = tokio::sync::Notify::const_new();
    static CLOSE_RELEASE: tokio::sync::Notify = tokio::sync::Notify::const_new();

    #[derive(Clone)]
    struct BlockingCloseFileIO;

    impl FileIO for BlockingCloseFileIO {
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
            data: bytes::Bytes,
        ) -> impl std::future::Future<Output = std::io::Result<usize>> + Send {
            TokioFileIO::write_at(handle, offset, data)
        }

        fn read_at(
            handle: &Self::Handle,
            offset: u64,
            len: usize,
        ) -> impl std::future::Future<Output = std::io::Result<bytes::Bytes>> + Send {
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

        async fn close(handle: Self::Handle) -> std::io::Result<()> {
            if BLOCK_NEXT_CLOSE.swap(false, Ordering::SeqCst) {
                CLOSE_STARTED.notify_waiters();
                CLOSE_RELEASE.notified().await;
            }
            TokioFileIO::close(handle).await
        }

        fn remove(
            path: &std::path::Path,
        ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
            TokioFileIO::remove(path)
        }
    }

    fn test_config() -> Arc<Config> {
        Arc::new(Config {
            host: "127.0.0.1".into(),
            port: 0,
            data_dir: std::path::PathBuf::from("./data"),
            page_size: 4096,
            max_cache_bytes: 65536,
            max_live_spools: 256,
            writer_inactivity_timeout_secs: 1,
            read_idle_ttl_secs: 1,
            full_read_complete_ttl_secs: 1,
            reader_done_ttl_secs: 1,
            unread_ttl_secs: 1,
            cleanup_sweep_interval_secs: 1,
            long_poll_timeout_ms: 25000,
            io_uring_shards: None,
            io_uring_queue_capacity: 1024,
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
            ..Config::default()
        })
    }

    async fn test_manager() -> (Arc<SpoolManager<TokioFileIO>>, tempfile::TempDir) {
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let manager = Arc::new(
            SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 65536, 256).expect("manager init"),
        );
        (manager, dir)
    }

    async fn wait_for_spool_removal(manager: &SpoolManager<TokioFileIO>, key: &str) {
        for _ in 0..2_000 {
            if manager.get_spool(key).is_none() {
                return;
            }
            tokio::task::spawn_blocking(|| std::thread::sleep(Duration::from_millis(1)))
                .await
                .expect("removal wait task panicked");
        }
    }

    async fn rewrite_persisted_metadata<F: FileIO>(
        manager: &SpoolManager<F>,
        key: &str,
        mutate: impl FnOnce(&mut SpoolMetadata),
    ) {
        let mut meta = manager
            .metadata_store
            .read(key)
            .await
            .expect("read metadata")
            .expect("metadata exists");
        mutate(&mut meta);
        manager
            .metadata_store
            .write(&meta)
            .await
            .expect("rewrite metadata");
    }

    // -----------------------------------------------------------------------
    // Rule 1: writer inactivity (unchanged semantics)
    // -----------------------------------------------------------------------

    /// A Writing spool whose writer has been silent for > writer_inactivity_timeout_secs
    /// must be deleted by the cleanup sweep.
    #[tokio::test]
    async fn test_writer_inactivity_cleanup() {
        tokio::time::pause();
        let (manager, _dir) = test_manager().await;
        let config = test_config();

        let key = uuid::Uuid::new_v4().to_string();
        manager
            .create_spool(key.clone(), None, None, false, HashMap::new())
            .await
            .expect("create spool");
        let spool = manager.get_spool(&key).expect("spool exists");
        {
            let mut meta = spool.metadata.lock().await;
            meta.state = SpoolState::Writing;
            meta.last_write_at = 0; // epoch — long since stale
        }

        let task = start_cleanup_task(manager.clone(), config);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        wait_for_spool_removal(&manager, &key).await;
        assert!(
            manager.get_spool(&key).is_none(),
            "inactive writer should be cleaned up"
        );
        task.abort();
    }

    /// Recovery must seed in-progress spools with a fresh in-memory last_write_at.
    /// Persisted metadata is only final lifecycle state; its write timestamp may be
    /// stale, so cleanup must not delete a recovered writer on the first sweep solely
    /// because the sidecar timestamp is old.
    #[tokio::test]
    async fn test_recovered_in_progress_stale_last_write_survives_first_cleanup() {
        tokio::time::pause();
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let config = test_config();
        let key = uuid::Uuid::new_v4().to_string();

        {
            let manager = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 65536, 256)
                .expect("manager init");
            manager
                .create_spool(key.clone(), None, None, false, HashMap::new())
                .await
                .expect("create spool");
            let spool = manager.get_spool(&key).expect("spool exists");
            spool
                .write(0, bytes::Bytes::copy_from_slice(&[0xAA; 4096]))
                .await
                .expect("write page");

            rewrite_persisted_metadata(&manager, &key, |meta| {
                meta.state = SpoolState::Writing;
                meta.last_write_at = 0;
            })
            .await;
        }

        let manager = Arc::new(
            SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 65536, 256)
                .expect("manager init after restart"),
        );
        manager.recover().await.expect("recover");
        let spool = manager.get_spool(&key).expect("recovered spool exists");
        assert!(
            spool.metadata.lock().await.last_write_at > 0,
            "recovery must replace stale persisted last_write_at with a fresh in-memory value"
        );

        let task = start_cleanup_task(manager.clone(), config);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        assert!(
            manager.get_spool(&key).is_some(),
            "freshly recovered in-progress spool must survive the first cleanup sweep"
        );
        task.abort();
    }

    /// Accepted post-recovery writes must refresh the same in-memory last_write_at
    /// anchor used by writer-inactivity cleanup, independent of stale persisted
    /// metadata.
    #[tokio::test]
    async fn test_post_recovery_write_refreshes_last_write_anchor() {
        tokio::time::pause();
        let dir = tempdir().expect("create tempdir");
        let data_dir = dir.path().join("data");
        let config = test_config();
        let key = uuid::Uuid::new_v4().to_string();

        {
            let manager = SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 65536, 256)
                .expect("manager init");
            manager
                .create_spool(key.clone(), None, None, false, HashMap::new())
                .await
                .expect("create spool");
            let spool = manager.get_spool(&key).expect("spool exists");
            spool
                .write(0, bytes::Bytes::copy_from_slice(&[0xBB; 4096]))
                .await
                .expect("write page");
            rewrite_persisted_metadata(&manager, &key, |meta| meta.last_write_at = 0).await;
        }

        let manager = Arc::new(
            SpoolManager::<TokioFileIO>::new(&data_dir, 4096, 65536, 256)
                .expect("manager init after restart"),
        );
        manager.recover().await.expect("recover");
        let spool = manager.get_spool(&key).expect("recovered spool exists");

        {
            let mut meta = spool.metadata.lock().await;
            meta.last_write_at = 1;
        }
        spool
            .write(4096, bytes::Bytes::copy_from_slice(&[0xCC; 4096]))
            .await
            .expect("post-recovery write succeeds");
        assert!(
            spool.metadata.lock().await.last_write_at > 1,
            "accepted write must refresh in-memory last_write_at"
        );

        let task = start_cleanup_task(manager.clone(), config);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        assert!(
            manager.get_spool(&key).is_some(),
            "recent post-recovery write must prevent writer-inactivity cleanup"
        );
        task.abort();
    }

    // -----------------------------------------------------------------------
    // Rule 3: idle TTL — renamed/updated tests
    // -----------------------------------------------------------------------

    /// Idle TTL fires when `last_read_activity_at = 0` (never served) and
    /// `readable_at` is an old timestamp. Renamed from test_reader_done_ttl_cleanup.
    #[tokio::test]
    async fn test_idle_ttl_after_read_activity() {
        tokio::time::pause();
        let (manager, _dir) = test_manager().await;
        let config = test_config();

        let key = uuid::Uuid::new_v4().to_string();
        manager
            .create_spool(key.clone(), None, None, false, HashMap::new())
            .await
            .expect("create spool");
        let spool = manager.get_spool(&key).expect("spool exists");
        spool.complete(None).await.expect("complete spool");
        {
            let mut meta = spool.metadata.lock().await;
            // Anchor idle TTL on a very old readable_at so the spool expires.
            // last_read_activity_at stays 0 (never served), so readable_at is the anchor.
            meta.readable_at = Some(0);
        }
        // last_read_activity_at stays 0 — cleanup uses readable_at as anchor

        let task = start_cleanup_task(manager.clone(), config);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        wait_for_spool_removal(&manager, &key).await;
        assert!(
            manager.get_spool(&key).is_none(),
            "spool with expired readable_at and no activity should be cleaned up"
        );
        task.abort();
    }

    /// Idle TTL is anchored on `readable_at`, NOT `created_at`. A spool with a
    /// recent `created_at` but an old `readable_at` must still be deleted.
    /// Renamed/updated from test_unread_ttl_cleanup.
    #[tokio::test]
    async fn test_idle_ttl_anchors_on_readable_at() {
        tokio::time::pause();
        let (manager, _dir) = test_manager().await;
        let config = test_config();

        let key = uuid::Uuid::new_v4().to_string();
        manager
            .create_spool(key.clone(), None, None, false, HashMap::new())
            .await
            .expect("create spool");
        let spool = manager.get_spool(&key).expect("spool exists");
        spool.complete(None).await.expect("complete spool");
        {
            let mut meta = spool.metadata.lock().await;
            // readable_at is old (epoch), but created_at is recent.
            // This proves the idle timer anchors on readable_at, not created_at.
            meta.readable_at = Some(0); // very old — idle TTL has expired
            meta.created_at = now_secs(); // recent — would NOT expire if this were the anchor
        }
        // last_read_activity_at stays 0 (never served) — anchor falls back to readable_at

        let task = start_cleanup_task(manager.clone(), config);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        wait_for_spool_removal(&manager, &key).await;
        assert!(
            manager.get_spool(&key).is_none(),
            "idle TTL should anchor on readable_at (old), not created_at (recent)"
        );
        task.abort();
    }

    /// Recent byte-serving activity (last_read_activity_at = now) prevents idle cleanup.
    /// Renamed from test_active_reader_not_deleted.
    #[tokio::test]
    async fn test_recent_activity_prevents_idle_cleanup() {
        tokio::time::pause();
        let (manager, _dir) = test_manager().await;
        let config = test_config();

        let key = uuid::Uuid::new_v4().to_string();
        manager
            .create_spool(key.clone(), None, None, false, HashMap::new())
            .await
            .expect("create spool");
        let spool = manager.get_spool(&key).expect("spool exists");
        spool.complete(None).await.expect("complete spool");
        // Simulate recent byte-serving activity — this refreshes the idle TTL anchor.
        // With last_read_activity_at = now_secs(), the idle check: now - now ≈ 0 > 1 → false.
        spool
            .last_read_activity_at
            .store(now_secs(), Ordering::SeqCst);

        let task = start_cleanup_task(manager.clone(), config);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        assert!(
            manager.get_spool(&key).is_some(),
            "spool with recent byte activity should not be deleted"
        );
        task.abort();
    }

    // -----------------------------------------------------------------------
    // New tests — Rule 3: idle TTL edge cases
    // -----------------------------------------------------------------------

    /// When `readable_at = None` (old metadata without the field) and no activity has
    /// been recorded, the cleanup loop must use `now` as the anchor — a conservative
    /// choice that prevents premature deletion on first sweep after upgrade.
    #[tokio::test]
    async fn test_readable_at_none_uses_now_as_safe_anchor() {
        tokio::time::pause();
        let (manager, _dir) = test_manager().await;
        let config = test_config();

        let key = uuid::Uuid::new_v4().to_string();
        manager
            .create_spool(key.clone(), None, None, false, HashMap::new())
            .await
            .expect("create spool");
        let spool = manager.get_spool(&key).expect("spool exists");
        spool.complete(None).await.expect("complete spool");
        {
            let mut meta = spool.metadata.lock().await;
            // Simulate old metadata that predates the readable_at field.
            meta.readable_at = None;
        }
        // last_read_activity_at stays 0 — cleanup falls back to readable_at.unwrap_or(now).
        // Since readable_at is None, anchor = now → delta ≈ 0 → spool NOT deleted.

        let task = start_cleanup_task(manager.clone(), config);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        assert!(
            manager.get_spool(&key).is_some(),
            "spool with readable_at=None must not be deleted (anchor falls back to now)"
        );
        task.abort();
    }

    /// A stalled reader (reader_count > 0 but zero bytes served) must NOT protect the
    /// spool forever. With an old readable_at and no activity, it should be deleted.
    #[tokio::test]
    async fn test_stalled_reader_no_activity_idle_expires() {
        tokio::time::pause();
        let (manager, _dir) = test_manager().await;
        let config = test_config();

        let key = uuid::Uuid::new_v4().to_string();
        manager
            .create_spool(key.clone(), None, None, false, HashMap::new())
            .await
            .expect("create spool");
        let spool = manager.get_spool(&key).expect("spool exists");
        spool.complete(None).await.expect("complete spool");
        {
            let mut meta = spool.metadata.lock().await;
            meta.readable_at = Some(0); // very old
        }
        // Simulate an open connection with no bytes served.
        spool.reader_count.fetch_add(1, Ordering::SeqCst);
        // last_read_activity_at stays 0 — no bytes were actually served.

        let task = start_cleanup_task(manager.clone(), config);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        wait_for_spool_removal(&manager, &key).await;
        assert!(
            manager.get_spool(&key).is_none(),
            "stalled reader with no served bytes must not prevent idle cleanup"
        );
        task.abort();
    }

    /// When byte activity is recorded (last_read_activity_at = now), the idle anchor
    /// shifts to the activity timestamp, preventing deletion even if readable_at is old.
    #[tokio::test]
    async fn test_active_reader_with_recent_bytes_refreshes_idle() {
        tokio::time::pause();
        let (manager, _dir) = test_manager().await;
        let config = test_config();

        let key = uuid::Uuid::new_v4().to_string();
        manager
            .create_spool(key.clone(), None, None, false, HashMap::new())
            .await
            .expect("create spool");
        let spool = manager.get_spool(&key).expect("spool exists");
        spool.complete(None).await.expect("complete spool");
        {
            let mut meta = spool.metadata.lock().await;
            // readable_at is old — would normally trigger idle expiry.
            meta.readable_at = Some(0);
        }
        // But last_read_activity_at is fresh — this refreshes the idle anchor.
        // Anchor = last_read_activity_at = now_secs(); delta = now - now ≈ 0 < 1 → not deleted.
        spool
            .last_read_activity_at
            .store(now_secs(), Ordering::SeqCst);

        // Advance tokio time so the cleanup loop ticks multiple times.
        let task = start_cleanup_task(manager.clone(), config);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        assert!(
            manager.get_spool(&key).is_some(),
            "recent byte activity must refresh the idle anchor and prevent deletion"
        );

        // Now simulate that activity stopped (reset to 0) while readable_at stays old.
        // The spool should eventually be cleaned up.
        spool.last_read_activity_at.store(0, Ordering::SeqCst);

        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        wait_for_spool_removal(&manager, &key).await;
        assert!(
            manager.get_spool(&key).is_none(),
            "once activity stops and readable_at is old, idle TTL should fire"
        );
        task.abort();
    }

    /// A Writing spool must not be deleted by the idle TTL rule, which only applies
    /// to Complete state.
    #[tokio::test]
    async fn test_writing_spool_not_deleted_by_idle() {
        tokio::time::pause();
        let (manager, _dir) = test_manager().await;
        let config = test_config();

        let key = uuid::Uuid::new_v4().to_string();
        manager
            .create_spool(key.clone(), None, None, false, HashMap::new())
            .await
            .expect("create spool");
        let spool = manager.get_spool(&key).expect("spool exists");
        {
            let mut meta = spool.metadata.lock().await;
            meta.state = SpoolState::Writing;
            // Keep last_write_at fresh so writer_inactive doesn't fire.
            meta.last_write_at = now_secs();
            // Make readable_at old and activity zero — idle *would* fire if the rule
            // applied to Writing state, but it must not.
            meta.readable_at = Some(0);
        }
        // last_read_activity_at stays 0

        let task = start_cleanup_task(manager.clone(), config);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        assert!(
            manager.get_spool(&key).is_some(),
            "Writing spool must not be deleted by the idle TTL rule"
        );
        task.abort();
    }

    // -----------------------------------------------------------------------
    // New tests — Rule 2: full-read short TTL
    // -----------------------------------------------------------------------

    /// Once every byte has been served (`full_object_read_at > 0`) and
    /// `full_read_complete_ttl_secs` has elapsed, the spool must be deleted.
    #[tokio::test]
    async fn test_full_object_read_triggers_short_ttl() {
        tokio::time::pause();
        let (manager, _dir) = test_manager().await;
        let config = test_config();

        let key = uuid::Uuid::new_v4().to_string();
        manager
            .create_spool(key.clone(), None, None, false, HashMap::new())
            .await
            .expect("create spool");
        let spool = manager.get_spool(&key).expect("spool exists");
        spool.complete(None).await.expect("complete spool");
        // Simulate that full-object read and the last byte-serving activity happened
        // at a very old timestamp. The full-read short TTL should delete the spool.
        spool.full_object_read_at.store(1, Ordering::SeqCst);
        spool.last_read_activity_at.store(1, Ordering::SeqCst);

        let task = start_cleanup_task(manager.clone(), config);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        wait_for_spool_removal(&manager, &key).await;
        assert!(
            manager.get_spool(&key).is_none(),
            "spool should be deleted after full-read TTL expires"
        );
        task.abort();
    }

    /// Later read activity refreshes the full-read short TTL anchor.
    #[tokio::test]
    async fn test_full_object_read_short_ttl_refreshed_by_activity() {
        tokio::time::pause();
        let (manager, _dir) = test_manager().await;
        let config = test_config();

        let key = uuid::Uuid::new_v4().to_string();
        manager
            .create_spool(key.clone(), None, None, false, HashMap::new())
            .await
            .expect("create spool");
        let spool = manager.get_spool(&key).expect("spool exists");
        spool.complete(None).await.expect("complete spool");
        // Full coverage was detected long ago, but bytes were served recently.
        // The spool must survive until the short TTL elapses after the latest activity.
        spool.full_object_read_at.store(1, Ordering::SeqCst);
        spool
            .last_read_activity_at
            .store(now_secs(), Ordering::SeqCst);

        let task = start_cleanup_task(manager.clone(), config);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        assert!(
            manager.get_spool(&key).is_some(),
            "recent byte activity must refresh the full-read short TTL anchor"
        );
        task.abort();
    }

    /// The full-read short TTL must NOT fire before `full_read_complete_ttl_secs`
    /// has elapsed since full coverage was first detected.
    #[tokio::test]
    async fn test_full_object_read_short_ttl_not_triggered_before_expiry() {
        tokio::time::pause();
        let (manager, _dir) = test_manager().await;
        let config = test_config();

        let key = uuid::Uuid::new_v4().to_string();
        manager
            .create_spool(key.clone(), None, None, false, HashMap::new())
            .await
            .expect("create spool");
        let spool = manager.get_spool(&key).expect("spool exists");
        spool.complete(None).await.expect("complete spool");
        // full_object_read_at = now → delta from wall-clock now is ≈ 0 < 1s TTL → not expired.
        spool
            .full_object_read_at
            .store(now_secs(), Ordering::SeqCst);
        // Keep idle anchor fresh too.
        spool
            .last_read_activity_at
            .store(now_secs(), Ordering::SeqCst);

        let task = start_cleanup_task(manager.clone(), config);
        tokio::task::yield_now().await;
        // Advance only a fraction of the TTL (in tokio time; wall clock barely moves).
        tokio::time::advance(Duration::from_millis(400)).await;
        tokio::task::yield_now().await;

        assert!(
            manager.get_spool(&key).is_some(),
            "full-read TTL must not fire before expiry"
        );
        task.abort();
    }

    #[derive(Clone, Copy, Debug)]
    enum SnapshotRefresh {
        Complete,
        Write,
        Read,
    }

    #[tokio::test]
    async fn stale_cleanup_candidates_are_revalidated_after_completion_write_and_read() {
        for refresh in [
            SnapshotRefresh::Complete,
            SnapshotRefresh::Write,
            SnapshotRefresh::Read,
        ] {
            let dir = tempdir().expect("create tempdir");
            let data_dir = dir.path().join("data");
            let manager = Arc::new(
                SpoolManager::<BlockingCloseFileIO>::new(&data_dir, 4096, 65536, 8)
                    .expect("manager init"),
            );
            for key in ["a-first-delete", "b-stale-candidate"] {
                manager
                    .create_spool(key.into(), None, None, false, HashMap::new())
                    .await
                    .expect("create candidate");
            }
            let first = manager
                .get_spool("a-first-delete")
                .expect("first candidate exists");
            first.metadata.lock().await.last_write_at = 1;
            let second = manager
                .get_spool("b-stale-candidate")
                .expect("second candidate exists");

            if matches!(refresh, SnapshotRefresh::Read) {
                second
                    .write(0, bytes::Bytes::from_static(b"served"))
                    .await
                    .expect("write reader fixture");
                second
                    .complete(None)
                    .await
                    .expect("complete reader fixture");
                let mut meta = second.metadata.lock().await;
                meta.readable_at = Some(1);
                second.last_read_activity_at.store(0, Ordering::SeqCst);
                second.full_object_read_at.store(0, Ordering::SeqCst);
            } else {
                second.metadata.lock().await.last_write_at = 1;
            }

            let mut config = (*test_config()).clone();
            config.data_dir = data_dir.clone();
            config.cleanup_sweep_interval_secs = 3600;
            let config = Arc::new(config);
            let close_started = CLOSE_STARTED.notified();
            BLOCK_NEXT_CLOSE.store(true, Ordering::SeqCst);
            let task = start_cleanup_task(Arc::clone(&manager), config);
            tokio::time::timeout(Duration::from_secs(5), close_started)
                .await
                .expect("first sorted delete reaches blocked close");

            match refresh {
                SnapshotRefresh::Complete => {
                    second
                        .complete(None)
                        .await
                        .expect("complete after snapshot");
                }
                SnapshotRefresh::Write => {
                    second
                        .write(0, bytes::Bytes::from_static(b"fresh"))
                        .await
                        .expect("write after snapshot");
                }
                SnapshotRefresh::Read => {
                    second
                        .mark_served_and_maybe_fully_read(0, 6, now_secs())
                        .await;
                }
            }
            CLOSE_RELEASE.notify_one();

            tokio::time::timeout(Duration::from_secs(5), async {
                while manager.get_spool("a-first-delete").is_some() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("first candidate is deleted");
            assert!(
                manager.get_spool("b-stale-candidate").is_some(),
                "{refresh:?} after snapshot must invalidate the second deletion candidate"
            );
            assert_ne!(
                second.metadata.lock().await.state,
                SpoolState::Deleting,
                "invalidated candidate must retain its live lifecycle state"
            );
            task.abort();
        }
    }
}
