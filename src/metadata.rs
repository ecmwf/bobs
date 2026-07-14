// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use crate::error::{BobsError, Result};
use crate::spool::SpoolMetadata;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use tokio::task;

const META_FILE: &str = "meta.json";
const TMP_FILE: &str = "meta.json.tmp";

const TEMP_CREATE_ATTEMPTS: usize = 16;

#[derive(Clone, Copy, Debug)]
pub(crate) struct MetadataFileIdentity {
    #[cfg(unix)]
    pub(crate) device: u64,
    #[cfg(unix)]
    pub(crate) inode: u64,
}

#[derive(Debug)]
struct CreatedMetadataTemp {
    file: File,
    path: PathBuf,
    identity: MetadataFileIdentity,
}

#[derive(Debug)]
struct MetadataTempCleanup {
    path: Option<PathBuf>,
}

impl MetadataTempCleanup {
    fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    fn disarm(&mut self) {
        self.path = None;
    }
}

impl Drop for MetadataTempCleanup {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = remove_file_if_present(&path);
        }
    }
}

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
    fn list(&self) -> impl Future<Output = Result<Vec<Result<SpoolMetadata>>>> + Send;
}

/// Synchronous sidecar metadata backend selected for fallback benchmarking and
/// non-Linux builds.
///
/// Metadata is stored as `<data_dir>/<key>/meta.json`. Updates are committed through
/// a create-new, non-following, transaction-private temporary file, which is synced,
/// atomically renamed over the final sidecar, and followed by a spool-directory sync.
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

    #[cfg(test)]
    fn invoke_operation_hook(&self) {
        if let Some(hook) = self.operation_hook {
            hook();
        }
    }

    #[cfg(not(test))]
    fn invoke_operation_hook(&self) {}

    fn write_sync(&self, metadata: &SpoolMetadata) -> Result<()> {
        let spool_dir = self.spool_dir(&metadata.key);
        fs::create_dir_all(&spool_dir).map_err(storage_error)?;

        let payload = serde_json::to_vec(metadata)
            .map_err(|error| BobsError::SerializationError(error.to_string()))?;
        let CreatedMetadataTemp {
            mut file,
            path: tmp_path,
            identity,
        } = create_metadata_temp(&spool_dir)?;
        let mut tmp_cleanup = MetadataTempCleanup::new(tmp_path.clone());
        let meta_path = spool_dir.join(META_FILE);
        self.invoke_operation_hook();

        file.write_all(&payload).map_err(storage_error)?;
        file.sync_data().map_err(storage_error)?;
        validate_metadata_temp_path(&tmp_path, identity).map_err(storage_error)?;

        fs::rename(&tmp_path, &meta_path).map_err(storage_error)?;
        tmp_cleanup.disarm();
        (self.sync_directory)(&spool_dir).map_err(storage_error)?;
        Ok(())
    }

    fn read_sync(&self, key: &str) -> Result<Option<SpoolMetadata>> {
        self.invoke_operation_hook();
        match read_file_no_follow(&self.meta_path(key)) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|error| BobsError::SerializationError(error.to_string())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(storage_error(error)),
        }
    }

    fn delete_sync(&self, key: &str) -> Result<()> {
        self.invoke_operation_hook();
        let spool_dir = self.spool_dir(key);
        let mut removed_any = remove_file_if_present(&self.meta_path(key))?;
        removed_any |= remove_metadata_temp_entries(&spool_dir)?;

        if removed_any {
            match fs::symlink_metadata(&spool_dir) {
                Ok(metadata) if metadata.file_type().is_dir() => {
                    (self.sync_directory)(&spool_dir).map_err(storage_error)?;
                }
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(storage_error(error)),
            }
        }

        Ok(())
    }

    fn list_sync(&self) -> Result<Vec<Result<SpoolMetadata>>> {
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

            let meta_path = entry.path().join(META_FILE);
            match read_file_no_follow(&meta_path) {
                Ok(bytes) => metadata.push(
                    serde_json::from_slice(&bytes)
                        .map_err(|error| BobsError::SerializationError(error.to_string())),
                ),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => metadata.push(Err(storage_error(error))),
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

    async fn list(&self) -> Result<Vec<Result<SpoolMetadata>>> {
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

fn create_metadata_temp(spool_dir: &Path) -> Result<CreatedMetadataTemp> {
    // `meta.json.tmp` was the historical fixed temporary name. Unlink only that
    // directory entry, whether it is a regular file or a symlink; never open it.
    remove_file_if_present(&spool_dir.join(TMP_FILE))?;

    for _ in 0..TEMP_CREATE_ATTEMPTS {
        let name = format!("{TMP_FILE}.{}", uuid::Uuid::new_v4());
        let path = spool_dir.join(name);
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        configure_no_follow(&mut options);

        match options.open(&path) {
            Ok(file) => {
                let identity = match metadata_file_identity(&file) {
                    Ok(identity) => identity,
                    Err(error) => {
                        let _ = remove_file_if_present(&path);
                        return Err(storage_error(error));
                    }
                };
                return Ok(CreatedMetadataTemp {
                    file,
                    path,
                    identity,
                });
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                // Never unlink a transaction-private collision: it may belong to a
                // concurrent writer. A fresh random name avoids interfering with it.
            }
            Err(error) => return Err(storage_error(error)),
        }
    }

    Err(storage_error(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate a private metadata temporary file",
    )))
}

fn configure_no_follow(options: &mut OpenOptions) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
}

fn metadata_file_identity(file: &File) -> io::Result<MetadataFileIdentity> {
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "metadata temporary path is not a regular file",
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(MetadataFileIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
    #[cfg(not(unix))]
    {
        Ok(MetadataFileIdentity {})
    }
}

pub(crate) fn validate_metadata_temp_path(
    path: &Path,
    expected: MetadataFileIdentity,
) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "metadata temporary path was replaced before publication",
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.dev() != expected.device || metadata.ino() != expected.inode {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "metadata temporary inode was replaced before publication",
            ));
        }
    }

    Ok(())
}

fn read_file_no_follow(path: &Path) -> io::Result<Vec<u8>> {
    let mut options = OpenOptions::new();
    options.read(true);
    configure_no_follow(&mut options);
    let mut file = options.open(path)?;
    if !file.metadata()?.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "metadata sidecar is not a regular file",
        ));
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn remove_metadata_temp_entries(spool_dir: &Path) -> Result<bool> {
    let entries = match fs::read_dir(spool_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(storage_error(error)),
    };
    let mut removed_any = false;

    for entry in entries {
        let entry = entry.map_err(storage_error)?;
        if !is_metadata_temp_name(&entry.file_name()) {
            continue;
        }
        let file_type = entry.file_type().map_err(storage_error)?;
        if file_type.is_dir() && !file_type.is_symlink() {
            continue;
        }
        removed_any |= remove_file_if_present(&entry.path())?;
    }

    Ok(removed_any)
}

fn is_metadata_temp_name(name: &OsStr) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    name == TMP_FILE
        || name
            .strip_prefix(TMP_FILE)
            .is_some_and(|suffix| suffix.starts_with('.'))
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

    fn metadata_temp_paths(spool_dir: &Path) -> Vec<PathBuf> {
        let mut paths = fs::read_dir(spool_dir)
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.ok())
            .filter(|entry| is_metadata_temp_name(&entry.file_name()))
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        paths.sort();
        paths
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

        let listed = store
            .list()
            .await
            .expect("list metadata")
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .expect("listed metadata parses");
        assert_eq!(listed.len(), 1);
        assert_metadata_eq(listed.into_iter().next().expect("listed metadata"), &meta);

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
    #[cfg(unix)]
    async fn sync_store_unlinks_stale_tmp_symlink_without_touching_target() {
        use std::os::unix::fs::symlink;

        let dir = tempdir().expect("create tempdir");
        let store = SyncSidecarMetadataStore::new(dir.path());
        let mut meta = metadata_with_generation(1);
        meta.key = "A".to_owned();
        let spool_dir = dir.path().join(&meta.key);
        let target_dir = dir.path().join("B");
        fs::create_dir_all(&spool_dir).expect("create metadata directory");
        fs::create_dir_all(&target_dir).expect("create target directory");
        let target = target_dir.join("spool.dat");
        let sentinel = b"must not be truncated";
        fs::write(&target, sentinel).expect("seed symlink target");
        symlink(&target, spool_dir.join(TMP_FILE)).expect("craft stale temporary symlink");

        store.write(&meta).await.expect("write metadata safely");

        assert_eq!(fs::read(&target).expect("read target"), sentinel);
        assert!(fs::symlink_metadata(spool_dir.join(TMP_FILE)).is_err());
        assert!(metadata_temp_paths(&spool_dir).is_empty());
        assert_metadata_eq(
            store.read(&meta.key).await.expect("read metadata").unwrap(),
            &meta,
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn sync_store_read_ignores_and_delete_unlinks_tmp_symlink() {
        use std::os::unix::fs::symlink;

        let dir = tempdir().expect("create tempdir");
        let store = SyncSidecarMetadataStore::new(dir.path());
        let mut meta = metadata_with_generation(1);
        meta.key = "A".to_owned();
        let spool_dir = dir.path().join(&meta.key);
        let target_dir = dir.path().join("B");
        fs::create_dir_all(&spool_dir).expect("create metadata directory");
        fs::create_dir_all(&target_dir).expect("create target directory");
        let target = target_dir.join("spool.dat");
        let sentinel = b"temporary symlink target";
        fs::write(&target, sentinel).expect("seed symlink target");
        let tmp_path = spool_dir.join(TMP_FILE);
        symlink(&target, &tmp_path).expect("craft temporary symlink");

        assert!(store
            .read(&meta.key)
            .await
            .expect("read metadata")
            .is_none());
        assert_eq!(
            fs::read(&target).expect("read target after metadata read"),
            sentinel
        );
        store
            .delete(&meta.key)
            .await
            .expect("delete temporary symlink");
        assert!(fs::symlink_metadata(&tmp_path).is_err());
        assert_eq!(
            fs::read(&target).expect("read target after metadata delete"),
            sentinel
        );
    }

    #[tokio::test]
    async fn sync_store_recovers_stale_regular_tmp() {
        let dir = tempdir().expect("create tempdir");
        let store = SyncSidecarMetadataStore::new(dir.path());
        let meta = metadata_with_generation(1);
        let spool_dir = dir.path().join(&meta.key);
        fs::create_dir_all(&spool_dir).expect("create spool directory");
        fs::write(spool_dir.join(TMP_FILE), b"stale partial metadata")
            .expect("seed stale regular temporary file");

        store
            .write(&meta)
            .await
            .expect("recover stale temporary file");

        assert!(metadata_temp_paths(&spool_dir).is_empty());
        assert_metadata_eq(
            store.read(&meta.key).await.expect("read metadata").unwrap(),
            &meta,
        );
    }

    #[tokio::test]
    async fn sync_store_concurrent_writes_use_independent_temps() {
        let dir = tempdir().expect("create tempdir");
        let store = SyncSidecarMetadataStore::new(dir.path());
        let first = metadata_with_generation(1);
        let second = metadata_with_generation(2);

        let (first_result, second_result) = tokio::join!(store.write(&first), store.write(&second));
        first_result.expect("first concurrent write");
        second_result.expect("second concurrent write");

        let actual = store
            .read(&first.key)
            .await
            .expect("read final metadata")
            .unwrap();
        assert!(
            actual.total_pages == first.total_pages || actual.total_pages == second.total_pages
        );
        assert!(metadata_temp_paths(&dir.path().join(&first.key)).is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_sync_write_leaves_no_orphaned_temp() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::Duration;

        static WRITE_STARTED: AtomicBool = AtomicBool::new(false);

        fn slow_write() {
            WRITE_STARTED.store(true, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(300));
        }

        WRITE_STARTED.store(false, Ordering::SeqCst);
        let dir = tempdir().expect("create tempdir");
        let store = SyncSidecarMetadataStore::with_operation_hook(dir.path(), slow_write);
        let meta = metadata_with_generation(1);
        let spool_dir = dir.path().join(&meta.key);
        let write_store = store.clone();
        let write_meta = meta.clone();
        let write = tokio::spawn(async move { write_store.write(&write_meta).await });

        tokio::time::timeout(Duration::from_secs(2), async {
            while !WRITE_STARTED.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocking write should start");
        assert_eq!(metadata_temp_paths(&spool_dir).len(), 1);

        write.abort();
        assert!(write
            .await
            .expect_err("write should be cancelled")
            .is_cancelled());
        tokio::time::timeout(Duration::from_secs(2), async {
            while !metadata_temp_paths(&spool_dir).is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("detached blocking transaction should finish without an orphaned temp");
        assert!(spool_dir.join(META_FILE).is_file());
    }

    #[tokio::test]
    async fn sync_store_rejects_replaced_transaction_temp() {
        use std::sync::OnceLock;

        static SPOOL_DIR: OnceLock<PathBuf> = OnceLock::new();

        fn replace_temp() {
            let paths = metadata_temp_paths(SPOOL_DIR.get().expect("test spool path set"));
            assert_eq!(paths.len(), 1);
            fs::remove_file(&paths[0]).expect("unlink opened transaction temp");
            fs::write(&paths[0], b"attacker replacement").expect("replace transaction temp");
        }

        let dir = tempdir().expect("create tempdir");
        let meta = metadata_with_generation(1);
        let spool_dir = dir.path().join(&meta.key);
        SPOOL_DIR
            .set(spool_dir.clone())
            .expect("set test spool path");
        let store = SyncSidecarMetadataStore::with_operation_hook(dir.path(), replace_temp);

        assert!(matches!(
            store.write(&meta).await,
            Err(BobsError::StorageError(_))
        ));
        assert!(!spool_dir.join(META_FILE).exists());
        assert!(metadata_temp_paths(&spool_dir).is_empty());
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
        assert!(matches!(
            store.list().await.expect("start list").into_iter().next(),
            Some(Err(BobsError::SerializationError(_)))
        ));
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
