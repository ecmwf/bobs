// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use crate::error::{BobsError, Result};
use crate::spool::SpoolMetadata;
use std::fs::{self, File};
use std::future::Future;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use tokio::task;

const META_FILE: &str = "meta.json";
const TMP_FILE: &str = "meta.json.tmp";

/// Metadata persistence backend.
///
/// All operations are asynchronous and return `Send` futures for generic
/// Axum/Tokio callers. Implementations must keep blocking filesystem work off
/// Tokio worker threads while preserving the sidecar durability protocol.
pub trait MetadataStore: Sync {
    // Native `async fn` cannot express the `Send` guarantee required by generic
    // Tokio/Axum callers; RPITIT is its allocation-free, stable equivalent.
    fn write(&self, metadata: &SpoolMetadata) -> impl Future<Output = Result<()>> + Send;
    fn read(&self, key: &str) -> impl Future<Output = Result<Option<SpoolMetadata>>> + Send;
    fn delete(&self, key: &str) -> impl Future<Output = Result<()>> + Send;
    fn list(&self) -> impl Future<Output = Result<Vec<(String, Result<SpoolMetadata>)>>> + Send;
}

/// Synchronous sidecar metadata backend selected for fallback benchmarking and
/// non-Linux builds.
///
/// Metadata is stored as `<data_dir>/<key>/meta.json`. Updates are committed by
/// writing `<data_dir>/<key>/meta.json.tmp`, syncing that file's data, renaming
/// it over the final sidecar, and syncing the spool directory.
#[derive(Clone, Debug)]
pub struct SyncSidecarMetadataStore {
    data_dir: PathBuf,
    sync_directory: fn(&Path) -> io::Result<()>,
    #[cfg(test)]
    operation_hook: Option<fn()>,
}

impl Default for SyncSidecarMetadataStore {
    fn default() -> Self {
        Self::new(PathBuf::new())
    }
}

impl SyncSidecarMetadataStore {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            sync_directory,
            #[cfg(test)]
            operation_hook: None,
        }
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    #[cfg(test)]
    fn with_directory_sync_error(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            sync_directory: |_| Err(io::Error::other("injected directory fsync failure")),
            operation_hook: None,
        }
    }

    #[cfg(test)]
    fn with_operation_hook(data_dir: impl Into<PathBuf>, operation_hook: fn()) -> Self {
        Self {
            data_dir: data_dir.into(),
            sync_directory,
            operation_hook: Some(operation_hook),
        }
    }

    fn spool_dir(&self, key: &str) -> PathBuf {
        self.data_dir.join(key)
    }

    fn meta_path(&self, key: &str) -> PathBuf {
        self.spool_dir(key).join(META_FILE)
    }

    fn tmp_path(&self, key: &str) -> PathBuf {
        self.spool_dir(key).join(TMP_FILE)
    }

    #[cfg(test)]
    fn invoke_operation_hook(&self) {
        if let Some(hook) = self.operation_hook {
            hook();
        }
    }

    #[cfg(not(test))]
    fn invoke_operation_hook(&self) {}

    fn write_sync(&self, metadata: &SpoolMetadata) -> Result<()> {
        self.invoke_operation_hook();
        let spool_dir = self.spool_dir(&metadata.key);
        fs::create_dir_all(&spool_dir).map_err(storage_error)?;

        let payload = serde_json::to_vec(metadata)
            .map_err(|error| BobsError::SerializationError(error.to_string()))?;
        let tmp_path = spool_dir.join(TMP_FILE);
        let meta_path = spool_dir.join(META_FILE);

        {
            let mut tmp = File::create(&tmp_path).map_err(storage_error)?;
            tmp.write_all(&payload).map_err(storage_error)?;
            tmp.sync_data().map_err(storage_error)?;
        }

        fs::rename(&tmp_path, &meta_path).map_err(storage_error)?;
        (self.sync_directory)(&spool_dir).map_err(storage_error)?;
        Ok(())
    }

    fn read_sync(&self, key: &str) -> Result<Option<SpoolMetadata>> {
        self.invoke_operation_hook();
        match fs::read(self.meta_path(key)) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|error| BobsError::SerializationError(error.to_string())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(storage_error(error)),
        }
    }

    fn delete_sync(&self, key: &str) -> Result<()> {
        self.invoke_operation_hook();
        let meta_path = self.meta_path(key);
        let tmp_path = self.tmp_path(key);
        let mut removed_any = false;

        removed_any |= remove_file_if_present(&meta_path)?;
        removed_any |= remove_file_if_present(&tmp_path)?;

        if removed_any {
            let spool_dir = self.spool_dir(key);
            if spool_dir.exists() {
                (self.sync_directory)(&spool_dir).map_err(storage_error)?;
            }
        }

        Ok(())
    }

    fn list_sync(&self) -> Result<Vec<(String, Result<SpoolMetadata>)>> {
        self.invoke_operation_hook();
        let entries = match fs::read_dir(&self.data_dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(storage_error(error)),
        };

        let mut metadata = Vec::new();
        for entry in entries {
            let entry = entry.map_err(storage_error)?;
            let file_type = entry.file_type().map_err(storage_error)?;
            if !file_type.is_dir() {
                continue;
            }

            let key = entry.file_name().to_string_lossy().to_string();
            let meta_path = entry.path().join(META_FILE);
            match fs::read(meta_path) {
                Ok(bytes) => metadata.push((
                    key,
                    serde_json::from_slice(&bytes)
                        .map_err(|error| BobsError::SerializationError(error.to_string())),
                )),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => metadata.push((key, Err(storage_error(error)))),
            }
        }

        Ok(metadata)
    }
}

impl MetadataStore for SyncSidecarMetadataStore {
    async fn write(&self, metadata: &SpoolMetadata) -> Result<()> {
        let store = self.clone();
        let metadata = metadata.clone();
        run_blocking(move || store.write_sync(&metadata)).await
    }

    async fn read(&self, key: &str) -> Result<Option<SpoolMetadata>> {
        let store = self.clone();
        let key = key.to_owned();
        run_blocking(move || store.read_sync(&key)).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let store = self.clone();
        let key = key.to_owned();
        run_blocking(move || store.delete_sync(&key)).await
    }

    async fn list(&self) -> Result<Vec<(String, Result<SpoolMetadata>)>> {
        let store = self.clone();
        run_blocking(move || store.list_sync()).await
    }
}

async fn run_blocking<T, F>(operation: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    task::spawn_blocking(operation)
        .await
        .map_err(|error| BobsError::StorageError(Box::new(error)))?
}

fn remove_file_if_present(path: &Path) -> Result<bool> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(storage_error(error)),
    }
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn storage_error(error: io::Error) -> BobsError {
    BobsError::StorageError(Box::new(error))
}

/// Linux default sidecar metadata backend selected for the io_uring filesystem
/// path.
///
/// Its commit path is designed around linked write, fdatasync, rename, and
/// directory fsync operations using the low-level `io-uring` crate.
#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
#[derive(Clone, Debug)]
pub struct UringSidecarMetadataStore {
    data_dir: PathBuf,
    #[cfg(test)]
    operation_hook: Option<fn()>,
}

#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
pub type DefaultMetadataStore = UringSidecarMetadataStore;

#[cfg(any(not(target_os = "linux"), feature = "tokio-fileio-fallback"))]
pub type DefaultMetadataStore = SyncSidecarMetadataStore;

#[cfg(any(test, all(target_os = "linux", not(feature = "tokio-fileio-fallback"))))]
#[path = "metadata/uring.rs"]
mod uring;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spool::{SpoolMetadata, SpoolState};
    use std::collections::HashMap;
    use std::fs::{self, File};
    use std::io::{self, Write};
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum CrashPoint {
        AfterWriteBeforeTmpFdatasync,
        AfterTmpFdatasyncBeforeRename,
        AfterRenameBeforeDirectoryFsync { rename_represented: bool },
        Clean,
    }

    #[derive(Debug)]
    enum RecoveredMetadata {
        Missing,
        Complete(SpoolMetadata),
    }

    #[derive(Debug)]
    struct FaultHarness {
        root: PathBuf,
    }

    impl FaultHarness {
        fn new(root: PathBuf) -> Self {
            Self { root }
        }

        fn meta_path(&self) -> PathBuf {
            self.root.join(META_FILE)
        }

        fn tmp_path(&self) -> PathBuf {
            self.root.join(TMP_FILE)
        }

        fn seed_durable_meta(&self, meta: &SpoolMetadata) -> io::Result<()> {
            fs::create_dir_all(&self.root)?;
            write_complete_json(&self.meta_path(), meta)?;
            sync_parent_dir(&self.root)
        }

        /// Model the sidecar commit protocol:
        ///
        /// 1. create/write `meta.json.tmp`
        /// 2. fdatasync the temporary file
        /// 3. atomically rename it to `meta.json`
        /// 4. fsync the parent directory
        ///
        /// The crash point decides which on-disk names are represented in the
        /// recovery fixture. This deliberately does not share implementation
        /// with the production sidecar store; it is a small oracle for
        /// the protocol invariants the store must satisfy.
        fn commit_with_crash(&self, new_meta: &SpoolMetadata, crash: CrashPoint) -> io::Result<()> {
            fs::create_dir_all(&self.root)?;
            write_complete_json(&self.tmp_path(), new_meta)?;

            match crash {
                CrashPoint::AfterWriteBeforeTmpFdatasync => Ok(()),
                CrashPoint::AfterTmpFdatasyncBeforeRename => {
                    File::options().read(true).open(self.tmp_path())?.sync_all()
                }
                CrashPoint::AfterRenameBeforeDirectoryFsync { rename_represented } => {
                    File::options()
                        .read(true)
                        .open(self.tmp_path())?
                        .sync_all()?;
                    if rename_represented {
                        fs::rename(self.tmp_path(), self.meta_path())?;
                    }
                    Ok(())
                }
                CrashPoint::Clean => {
                    File::options()
                        .read(true)
                        .open(self.tmp_path())?
                        .sync_all()?;
                    fs::rename(self.tmp_path(), self.meta_path())?;
                    sync_parent_dir(&self.root)
                }
            }
        }

        fn leave_torn_tmp(&self, bytes: &[u8]) -> io::Result<()> {
            fs::write(self.tmp_path(), bytes)
        }

        fn leave_torn_meta(&self, bytes: &[u8]) -> io::Result<()> {
            fs::write(self.meta_path(), bytes)
        }

        /// Recovery for sidecar metadata is name-based: ignore
        /// `meta.json.tmp`, and parse only a complete `meta.json`. A malformed
        /// `meta.json` is treated as corrupt rather than being accepted as a
        /// partial/torn metadata record.
        fn recover(&self) -> Result<RecoveredMetadata> {
            let path = self.meta_path();
            if !path.exists() {
                return Ok(RecoveredMetadata::Missing);
            }
            let bytes = fs::read(path).expect("read meta.json fixture");
            serde_json::from_slice::<SpoolMetadata>(&bytes)
                .map(RecoveredMetadata::Complete)
                .map_err(|error| BobsError::SerializationError(error.to_string()))
        }
    }

    fn write_complete_json(path: &Path, meta: &SpoolMetadata) -> io::Result<()> {
        let payload = serde_json::to_vec(meta).expect("serialize metadata fixture");
        let mut file = File::create(path)?;
        file.write_all(&payload)?;
        Ok(())
    }

    fn sync_parent_dir(path: &Path) -> io::Result<()> {
        File::open(path)?.sync_all()
    }

    fn assert_recovered_complete(actual: RecoveredMetadata, expected: &SpoolMetadata) {
        let RecoveredMetadata::Complete(actual) = actual else {
            panic!("expected complete metadata, got {actual:?}");
        };
        assert_metadata_eq(actual, expected);
    }

    fn assert_metadata_eq(actual: SpoolMetadata, expected: &SpoolMetadata) {
        assert_eq!(
            serde_json::to_value(&actual).expect("serialize actual metadata"),
            serde_json::to_value(expected).expect("serialize expected metadata")
        );
    }

    fn metadata_with_generation(generation: u64) -> SpoolMetadata {
        SpoolMetadata {
            key: "sidecar-test-key".to_string(),
            content_type: Some("application/octet-stream".to_string()),
            content_encoding: None,
            state: if generation == 0 {
                SpoolState::Writing
            } else {
                SpoolState::Complete
            },
            write_locked: false,
            created_at: 10,
            last_write_at: 20 + generation,
            last_read_at: None,
            readable_at: Some(30 + generation),
            page_size: 4096,
            total_bytes_written: generation * 4096,
            total_pages: generation,
            final_page_size: if generation == 0 { None } else { Some(4096) },
            data_path: PathBuf::from(format!("/tmp/sidecar-test-key.{generation}.data")),
            labels: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn sync_store_write_read_delete_and_list() {
        let dir = tempdir().expect("create tempdir");
        let store = SyncSidecarMetadataStore::new(dir.path());
        let meta = metadata_with_generation(1);

        assert!(store.read(&meta.key).await.expect("read missing").is_none());
        store.write(&meta).await.expect("write metadata");
        assert_metadata_eq(
            store
                .read(&meta.key)
                .await
                .expect("read metadata")
                .expect("metadata present"),
            &meta,
        );

        let listed: Vec<_> = store
            .list()
            .await
            .expect("list metadata")
            .into_iter()
            .map(|(key, metadata)| (key, metadata.expect("listed metadata parses")))
            .collect();
        assert_eq!(listed.len(), 1);
        let (key, listed_metadata) = listed.into_iter().next().expect("listed metadata");
        assert_eq!(key, meta.key);
        assert_metadata_eq(listed_metadata, &meta);

        store.delete(&meta.key).await.expect("delete metadata");
        assert!(store
            .read(&meta.key)
            .await
            .expect("read after delete")
            .is_none());
        assert!(store.list().await.expect("list after delete").is_empty());
    }

    #[tokio::test]
    async fn sync_store_replacement_keeps_old_final_until_rename() {
        let dir = tempdir().expect("create tempdir");
        let store = SyncSidecarMetadataStore::new(dir.path());
        let old = metadata_with_generation(1);
        let new = metadata_with_generation(2);

        store.write(&old).await.expect("write old metadata");
        let spool_dir = dir.path().join(&old.key);
        write_complete_json(&spool_dir.join(TMP_FILE), &new).expect("write tmp replacement");
        File::options()
            .read(true)
            .open(spool_dir.join(TMP_FILE))
            .expect("open tmp")
            .sync_data()
            .expect("sync tmp");

        assert_metadata_eq(
            store
                .read(&old.key)
                .await
                .expect("read old")
                .expect("old metadata present"),
            &old,
        );
        fs::rename(spool_dir.join(TMP_FILE), spool_dir.join(META_FILE)).expect("rename tmp");
        assert_metadata_eq(
            store
                .read(&new.key)
                .await
                .expect("read new")
                .expect("new metadata present"),
            &new,
        );
    }

    #[tokio::test]
    async fn sync_store_delete_tolerates_missing_and_removes_tmp() {
        let dir = tempdir().expect("create tempdir");
        let store = SyncSidecarMetadataStore::new(dir.path());
        let meta = metadata_with_generation(1);
        let spool_dir = dir.path().join(&meta.key);
        fs::create_dir_all(&spool_dir).expect("create spool dir");
        fs::write(spool_dir.join(TMP_FILE), b"torn").expect("write tmp");

        store.delete(&meta.key).await.expect("delete tmp only");
        assert!(!spool_dir.join(TMP_FILE).exists());
        store
            .delete(&meta.key)
            .await
            .expect("delete absent metadata");
    }

    #[tokio::test]
    async fn sync_store_corrupt_json_is_serialization_error() {
        let dir = tempdir().expect("create tempdir");
        let store = SyncSidecarMetadataStore::new(dir.path());
        let meta = metadata_with_generation(1);
        let spool_dir = dir.path().join(&meta.key);
        fs::create_dir_all(&spool_dir).expect("create spool dir");
        fs::write(spool_dir.join(META_FILE), b"not json").expect("write corrupt meta");

        assert!(matches!(
            store.read(&meta.key).await,
            Err(BobsError::SerializationError(_))
        ));
        let (key, result) = store
            .list()
            .await
            .expect("start list")
            .into_iter()
            .next()
            .expect("entry");
        assert_eq!(key, meta.key);
        assert!(matches!(result, Err(BobsError::SerializationError(_))));
    }

    #[tokio::test]
    async fn sync_store_directory_fsync_errors_are_storage_errors() {
        let dir = tempdir().expect("create tempdir");
        let store = SyncSidecarMetadataStore::with_directory_sync_error(dir.path());
        let meta = metadata_with_generation(1);

        assert!(matches!(
            store.write(&meta).await,
            Err(BobsError::StorageError(_))
        ));
    }

    #[tokio::test]
    async fn sync_store_operations_leave_tokio_worker_thread() {
        use std::sync::OnceLock;
        use std::thread::{self, ThreadId};

        static ASYNC_WORKER_THREAD: OnceLock<ThreadId> = OnceLock::new();

        fn assert_on_blocking_thread() {
            assert_ne!(
                thread::current().id(),
                *ASYNC_WORKER_THREAD
                    .get()
                    .expect("async worker thread recorded"),
                "blocking metadata operation ran on the Tokio worker thread"
            );
        }

        ASYNC_WORKER_THREAD
            .set(thread::current().id())
            .expect("worker thread is recorded once");
        let dir = tempdir().expect("create tempdir");
        let store =
            SyncSidecarMetadataStore::with_operation_hook(dir.path(), assert_on_blocking_thread);
        let meta = metadata_with_generation(1);

        store.write(&meta).await.expect("write metadata");
        store.read(&meta.key).await.expect("read metadata");
        store.list().await.expect("list metadata");
        store.delete(&meta.key).await.expect("delete metadata");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_store_blocking_work_does_not_stall_worker_progress() {
        use std::time::{Duration, Instant};

        fn slow_blocking_operation() {
            std::thread::sleep(Duration::from_millis(300));
        }

        let dir = tempdir().expect("create tempdir");
        let store =
            SyncSidecarMetadataStore::with_operation_hook(dir.path(), slow_blocking_operation);
        let meta = metadata_with_generation(1);
        let started = Instant::now();

        let (heartbeat_elapsed, write_result) = tokio::join!(
            async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                started.elapsed()
            },
            store.write(&meta),
        );

        write_result.expect("write metadata");
        assert!(
            heartbeat_elapsed < Duration::from_millis(150),
            "Tokio worker heartbeat was delayed by blocking metadata I/O: {heartbeat_elapsed:?}"
        );
    }

    #[test]
    fn sync_store_is_available_on_all_targets() {
        fn assert_store<T: MetadataStore>() {}
        assert_store::<SyncSidecarMetadataStore>();
    }

    #[test]
    fn metadata_store_rpitit_futures_preserve_send_contract() {
        fn assert_send<T: Send>(_: T) {}
        fn assert_store_futures_are_send<M: MetadataStore>(store: &M, metadata: &SpoolMetadata) {
            assert_send(store.write(metadata));
            assert_send(store.read(&metadata.key));
            assert_send(store.delete(&metadata.key));
            assert_send(store.list());
        }

        let dir = tempdir().expect("create tempdir");
        let store = SyncSidecarMetadataStore::new(dir.path());
        assert_store_futures_are_send(&store, &metadata_with_generation(1));
    }

    #[test]
    fn sync_store_writes_current_serde_json_field_order() {
        let dir = tempdir().expect("create tempdir");
        let store = SyncSidecarMetadataStore::new(dir.path());
        let meta = metadata_with_generation(1);
        store.write_sync(&meta).expect("write metadata");

        let bytes = fs::read(dir.path().join(&meta.key).join(META_FILE)).expect("read sidecar");
        assert_eq!(
            bytes,
            serde_json::to_vec(&meta).expect("serialize expected")
        );
    }

    #[test]
    fn sidecar_ignores_tmp_on_recovery() {
        let dir = tempdir().expect("create tempdir");
        let harness = FaultHarness::new(dir.path().to_path_buf());
        let old = metadata_with_generation(1);
        harness.seed_durable_meta(&old).expect("seed old meta");
        harness
            .leave_torn_tmp(b"{\"key\":\"sidecar-test-key\",\"total_bytes_written\":")
            .expect("leave torn tmp");

        assert_recovered_complete(harness.recover().expect("recover complete meta"), &old);
    }

    #[test]
    fn metadata_store_atomic_rename() {
        let old = metadata_with_generation(1);
        let new = metadata_with_generation(2);

        for crash in [
            CrashPoint::AfterWriteBeforeTmpFdatasync,
            CrashPoint::AfterTmpFdatasyncBeforeRename,
            CrashPoint::AfterRenameBeforeDirectoryFsync {
                rename_represented: false,
            },
        ] {
            let dir = tempdir().expect("create tempdir");
            let harness = FaultHarness::new(dir.path().to_path_buf());
            harness.seed_durable_meta(&old).expect("seed old meta");
            harness
                .commit_with_crash(&new, crash)
                .expect("commit fixture with crash");

            let recovered = harness.recover().expect("recover old meta");
            assert_recovered_complete(recovered, &old);
        }

        for crash in [
            CrashPoint::AfterRenameBeforeDirectoryFsync {
                rename_represented: true,
            },
            CrashPoint::Clean,
        ] {
            let dir = tempdir().expect("create tempdir");
            let harness = FaultHarness::new(dir.path().to_path_buf());
            harness.seed_durable_meta(&old).expect("seed old meta");
            harness
                .commit_with_crash(&new, crash)
                .expect("commit fixture with crash");

            let recovered = harness.recover().expect("recover new meta");
            assert_recovered_complete(recovered, &new);
        }
    }

    #[test]
    fn sidecar_recovery_rejects_torn_meta_json() {
        let dir = tempdir().expect("create tempdir");
        let harness = FaultHarness::new(dir.path().to_path_buf());
        harness
            .leave_torn_meta(b"{\"key\":\"sidecar-test-key\",\"state\":\"Complete\"")
            .expect("leave torn meta");

        assert!(
            matches!(harness.recover(), Err(BobsError::SerializationError(_))),
            "recovery must reject a torn meta.json rather than parse partial metadata"
        );
    }
}
