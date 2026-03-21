use crate::config::Config;
use crate::io::FileIO;
use crate::manager::SpoolManager;
use crate::spool::SpoolState;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};

/// Periodic sweep that reclaims spools which are no longer needed.
/// Deletion triggers: writer abandoned the spool, reader finished and TTL expired,
/// or the spool was never read within the unread TTL. Spools with active readers
/// are always protected.
pub async fn run_cleanup_loop<F: FileIO>(manager: Arc<SpoolManager<F>>, config: Arc<Config>) {
    let mut interval = time::interval(Duration::from_secs(config.cleanup_sweep_interval_secs));

    loop {
        interval.tick().await;
        let now = now_secs();
        let mut to_delete = Vec::new();

        for key in manager.spool_keys() {
            let Some(spool) = manager.get_spool(&key) else {
                continue;
            };

            let (state, last_write_at, last_read_at, created_at) = {
                let meta = spool.metadata.lock().await;
                (
                    meta.state.clone(),
                    meta.last_write_at,
                    meta.last_read_at,
                    meta.created_at,
                )
            };
            let readers_active = spool.reader_count.load(Ordering::SeqCst) > 0;

            // Writer stopped sending data — probably crashed or disconnected.
            let writer_inactive = matches!(state, SpoolState::Writing | SpoolState::WriteLocked)
                && now.saturating_sub(last_write_at) > config.writer_inactivity_timeout_secs;

            // Spool is done (or readable), no active readers, and last read was long enough ago.
            let reader_done_expired = matches!(state, SpoolState::Complete | SpoolState::Readable)
                && !readers_active
                && last_read_at
                    .map(|last| now.saturating_sub(last) > config.reader_done_ttl_secs)
                    .unwrap_or(false);

            // Spool is done (or readable) but nobody ever read it.
            let unread_expired = matches!(state, SpoolState::Complete | SpoolState::Readable)
                && last_read_at.is_none()
                && now.saturating_sub(created_at) > config.unread_ttl_secs;

            if writer_inactive || reader_done_expired || unread_expired {
                to_delete.push(key);
            }
        }

        for key in to_delete {
            if let Err(err) = manager.delete_spool(&key).await {
                tracing::debug!(%key, error = %err, "cleanup delete failed");
            }
        }
    }
}

pub fn start_cleanup_task<F: FileIO>(
    manager: Arc<SpoolManager<F>>,
    config: Arc<Config>,
) -> JoinHandle<()> {
    tokio::spawn(run_cleanup_loop(manager, config))
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::TokioFileIO;
    use tempfile::tempdir;

    fn test_config() -> Arc<Config> {
        Arc::new(Config {
            host: "127.0.0.1".into(),
            port: 0,
            data_dir: std::path::PathBuf::from("./data"),
            page_size: 4096,
            max_cache_bytes: 65536,
            writer_inactivity_timeout_secs: 1,
            reader_done_ttl_secs: 1,
            unread_ttl_secs: 1,
            cleanup_sweep_interval_secs: 1,
            long_poll_timeout_ms: 25000,
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
        })
    }

    async fn test_manager() -> Arc<SpoolManager<TokioFileIO>> {
        let dir = tempdir().expect("create tempdir");
        let db_path = dir.path().join("spools.redb");
        let data_dir = dir.path().join("data");
        Arc::new(
            SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 65536)
                .expect("manager init"),
        )
    }

    #[tokio::test]
    async fn test_writer_inactivity_cleanup() {
        tokio::time::pause();
        let manager = test_manager().await;
        let config = test_config();

        let key = manager
            .create_spool(None, None, false)
            .await
            .expect("create spool");
        let spool = manager.get_spool(&key).expect("spool exists");
        {
            let mut meta = spool.metadata.lock().await;
            meta.state = SpoolState::Writing;
            meta.last_write_at = 0;
        }

        let task = start_cleanup_task(manager.clone(), config);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        assert!(manager.get_spool(&key).is_none());
        task.abort();
    }

    #[tokio::test]
    async fn test_reader_done_ttl_cleanup() {
        tokio::time::pause();
        let manager = test_manager().await;
        let config = test_config();

        let key = manager
            .create_spool(None, None, false)
            .await
            .expect("create spool");
        let spool = manager.get_spool(&key).expect("spool exists");
        spool.complete(None).await.expect("complete spool");
        {
            let mut meta = spool.metadata.lock().await;
            meta.last_read_at = Some(0);
        }

        let task = start_cleanup_task(manager.clone(), config);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        assert!(manager.get_spool(&key).is_none());
        task.abort();
    }

    #[tokio::test]
    async fn test_unread_ttl_cleanup() {
        tokio::time::pause();
        let manager = test_manager().await;
        let config = test_config();

        let key = manager
            .create_spool(None, None, false)
            .await
            .expect("create spool");
        let spool = manager.get_spool(&key).expect("spool exists");
        spool.complete(None).await.expect("complete spool");
        {
            let mut meta = spool.metadata.lock().await;
            meta.created_at = 0;
            meta.last_read_at = None;
        }

        let task = start_cleanup_task(manager.clone(), config);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        assert!(manager.get_spool(&key).is_none());
        task.abort();
    }

    #[tokio::test]
    async fn test_active_reader_not_deleted() {
        tokio::time::pause();
        let manager = test_manager().await;
        let config = test_config();

        let key = manager
            .create_spool(None, None, false)
            .await
            .expect("create spool");
        let spool = manager.get_spool(&key).expect("spool exists");
        spool.complete(None).await.expect("complete spool");
        {
            let mut meta = spool.metadata.lock().await;
            meta.last_read_at = Some(0);
        }
        spool.reader_count.fetch_add(1, Ordering::SeqCst);

        let task = start_cleanup_task(manager.clone(), config);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        assert!(manager.get_spool(&key).is_some());
        task.abort();
    }
}
