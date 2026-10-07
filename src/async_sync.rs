// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

/// Coalesces delayed best-effort durability requests into one filesystem flush.
///
/// A per-spool sequence of fdatasync/fsync calls creates a barrier storm on shared
/// storage and can stall unrelated foreground writes. `syncfs` provides the same
/// best-effort data-plus-metadata durability for every completion included in a
/// batch, with one barrier per delay window instead of several per object.
pub struct AsyncSyncCoordinator {
    data_dir: PathBuf,
    delay: Duration,
    scheduled_generation: AtomicU64,
    notify: Notify,
    #[cfg(test)]
    completed_batches: AtomicU64,
}

impl AsyncSyncCoordinator {
    pub fn start(data_dir: PathBuf, delay: Duration) -> Arc<Self> {
        let coordinator = Arc::new(Self {
            data_dir,
            delay,
            scheduled_generation: AtomicU64::new(0),
            notify: Notify::new(),
            #[cfg(test)]
            completed_batches: AtomicU64::new(0),
        });
        tokio::spawn(Arc::clone(&coordinator).run());
        coordinator
    }

    /// Record a completed object without waiting for the delayed flush.
    pub fn schedule(&self) {
        self.scheduled_generation.fetch_add(1, Ordering::Release);
        self.notify.notify_one();
    }

    async fn run(self: Arc<Self>) {
        let mut flushed_generation = 0;
        loop {
            while self.scheduled_generation.load(Ordering::Acquire) == flushed_generation {
                self.notify.notified().await;
            }

            tokio::time::sleep(self.delay).await;
            let target_generation = self.scheduled_generation.load(Ordering::Acquire);
            let data_dir = self.data_dir.clone();
            let result = tokio::task::spawn_blocking(move || sync_filesystem(&data_dir)).await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(
                    "event.name" = "bobs.spool.async_sync_failed",
                    error = %error,
                    "best-effort delayed filesystem sync failed"
                ),
                Err(error) => tracing::warn!(
                    "event.name" = "bobs.spool.async_sync_failed",
                    error = %error,
                    "best-effort delayed filesystem sync task failed"
                ),
            }
            #[cfg(test)]
            self.completed_batches.fetch_add(1, Ordering::Release);
            flushed_generation = target_generation;
        }
    }

    #[cfg(test)]
    pub(crate) fn scheduled_count(&self) -> u64 {
        self.scheduled_generation.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn completed_batch_count(&self) -> u64 {
        self.completed_batches.load(Ordering::Acquire)
    }
}

#[cfg(target_os = "linux")]
fn sync_filesystem(path: &Path) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    let directory = std::fs::File::open(path)?;
    // SAFETY: `directory` owns a valid descriptor for the duration of the call.
    if unsafe { libc::syncfs(directory.as_raw_fd()) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn sync_filesystem(path: &Path) -> std::io::Result<()> {
    std::fs::File::open(path)?.sync_all()
}
