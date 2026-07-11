// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
use super::{
    remove_file_if_present, storage_error, MetadataStore, SyncSidecarMetadataStore,
    UringSidecarMetadataStore, META_FILE, TMP_FILE,
};
#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
use crate::error::{BobsError, Result};
#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
use crate::spool::SpoolMetadata;
#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
use bytes::Bytes;
#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
use std::ffi::CString;
#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
use std::fs::{self, File, OpenOptions};
#[cfg(all(test, target_os = "linux", not(feature = "tokio-fileio-fallback")))]
use std::io;
#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
use std::os::fd::OwnedFd;
#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
use std::path::{Path, PathBuf};

#[cfg(all(test, any(not(target_os = "linux"), feature = "tokio-fileio-fallback")))]
use super::{storage_error, META_FILE, TMP_FILE};
#[cfg(all(test, any(not(target_os = "linux"), feature = "tokio-fileio-fallback")))]
use crate::error::{BobsError, Result};
#[cfg(all(test, any(not(target_os = "linux"), feature = "tokio-fileio-fallback")))]
use crate::spool::SpoolMetadata;
#[cfg(test)]
use std::collections::HashMap;
#[cfg(all(test, any(not(target_os = "linux"), feature = "tokio-fileio-fallback")))]
use std::fs;
#[cfg(all(test, any(not(target_os = "linux"), feature = "tokio-fileio-fallback")))]
use std::io;
#[cfg(all(test, any(not(target_os = "linux"), feature = "tokio-fileio-fallback")))]
use std::path::{Path, PathBuf};

#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
impl Default for UringSidecarMetadataStore {
    fn default() -> Self {
        Self::new(PathBuf::new())
    }
}

#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
impl UringSidecarMetadataStore {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
        }
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    fn sync_store(&self) -> SyncSidecarMetadataStore {
        SyncSidecarMetadataStore::new(&self.data_dir)
    }

    fn spool_dir(&self, key: &str) -> PathBuf {
        self.data_dir.join(key)
    }

    async fn write_uring(&self, metadata: &SpoolMetadata) -> Result<()> {
        let payload = serde_json::to_vec(metadata)
            .map_err(|error| BobsError::SerializationError(error.to_string()))?;

        let spool_dir = self.spool_dir(&metadata.key);
        fs::create_dir_all(&spool_dir).map_err(storage_error)?;
        let tmp_path = spool_dir.join(TMP_FILE);

        let tmp = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&tmp_path)
            .map_err(storage_error)?;
        let parent = File::open(&spool_dir).map_err(storage_error)?;

        let tmp_fd: OwnedFd = tmp.into();
        let parent_fd: OwnedFd = parent.into();
        let tmp_name = CString::new(TMP_FILE).expect("metadata tmp filename contains no NUL");
        let final_name = CString::new(META_FILE).expect("metadata filename contains no NUL");

        let pool = crate::io::ring_pool::global_or_default_ring_pool().map_err(storage_error)?;
        match pool
            .submit_metadata_commit(
                metadata.key.clone(),
                tmp_fd,
                parent_fd,
                tmp_name,
                final_name,
                Bytes::from(payload),
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => {
                let _ = remove_file_if_present(&tmp_path);
                Err(storage_error(error))
            }
        }
    }
}

#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
impl MetadataStore for UringSidecarMetadataStore {
    async fn write(&self, metadata: &SpoolMetadata) -> Result<()> {
        self.write_uring(metadata).await
    }

    async fn read(&self, key: &str) -> Result<Option<SpoolMetadata>> {
        let store = self.sync_store();
        store.read(key).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let store = self.sync_store();
        store.delete(key).await
    }

    async fn list(&self) -> Result<Vec<Result<SpoolMetadata>>> {
        let store = self.sync_store();
        store.list().await
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MetadataOpKind {
    Write,
    Fdatasync,
    Rename,
    DirectoryFsync,
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) enum MetadataSqe {
    Write { path: PathBuf, payload: Vec<u8> },
    Fdatasync { path: PathBuf },
    Rename { from: PathBuf, to: PathBuf },
    DirectoryFsync { path: PathBuf },
}

#[cfg(test)]
impl MetadataSqe {
    fn kind(&self) -> MetadataOpKind {
        match self {
            Self::Write { .. } => MetadataOpKind::Write,
            Self::Fdatasync { .. } => MetadataOpKind::Fdatasync,
            Self::Rename { .. } => MetadataOpKind::Rename,
            Self::DirectoryFsync { .. } => MetadataOpKind::DirectoryFsync,
        }
    }
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
struct RecordedSqe {
    kind: MetadataOpKind,
    linked: bool,
    path: Option<PathBuf>,
    second_path: Option<PathBuf>,
    fd: Option<i32>,
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecordedCompletion {
    kind: MetadataOpKind,
    performed: bool,
    cancelled: bool,
    result: Option<io::ErrorKind>,
    bytes_written: Option<usize>,
}

#[cfg(test)]
pub(crate) trait MetadataCommitSubmitter {
    fn begin_commit_on_shard(&mut self, _shard_index: usize) -> io::Result<()> {
        Ok(())
    }

    fn push(&mut self, sqe: MetadataSqe, linked: bool) -> io::Result<()>;
    fn submit_and_wait(&mut self) -> io::Result<Vec<RecordedCompletion>>;
}

#[cfg(all(test, target_os = "linux", not(feature = "tokio-fileio-fallback")))]
#[allow(dead_code)]
pub(crate) fn record_metadata_commit_routing_for_test(
    pool: &crate::io::ring_pool::RingPool,
    key: &str,
) {
    let ring_index = crate::io::ring_pool::ring_index_for_key(key, pool.shard_count());
    pool.record_routing(
        crate::io::ring_pool::RingPoolOperationKind::MetadataCommit,
        key,
        ring_index,
    );
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn commit_hot_metadata<S: MetadataCommitSubmitter>(
    submitter: &mut S,
    data_dir: &Path,
    metadata: &SpoolMetadata,
) -> Result<()> {
    commit_hot_metadata_on_shard(submitter, data_dir, metadata, 0)
}

#[cfg(test)]
pub(crate) fn commit_hot_metadata_on_shard<S: MetadataCommitSubmitter>(
    submitter: &mut S,
    data_dir: &Path,
    metadata: &SpoolMetadata,
    shard_index: usize,
) -> Result<()> {
    submitter
        .begin_commit_on_shard(shard_index)
        .map_err(storage_error)?;
    let spool_dir = data_dir.join(&metadata.key);
    fs::create_dir_all(&spool_dir).map_err(storage_error)?;

    let payload = serde_json::to_vec(metadata)
        .map_err(|error| BobsError::SerializationError(error.to_string()))?;
    let payload_len = payload.len();
    let tmp_path = spool_dir.join(TMP_FILE);
    let meta_path = spool_dir.join(META_FILE);

    // Phase one makes the complete temporary file durable. A positive short
    // write does not break an IO_LINK chain, so rename must not be queued yet.
    submitter
        .push(
            MetadataSqe::Write {
                path: tmp_path.clone(),
                payload,
            },
            true,
        )
        .map_err(storage_error)?;
    submitter
        .push(
            MetadataSqe::Fdatasync {
                path: tmp_path.clone(),
            },
            false,
        )
        .map_err(storage_error)?;
    let write_completions = submitter.submit_and_wait().map_err(storage_error)?;
    check_completion_errors(&write_completions)?;
    let bytes_written = write_completions
        .iter()
        .find(|completion| completion.kind == MetadataOpKind::Write)
        .and_then(|completion| completion.bytes_written)
        .ok_or_else(|| storage_error(io::Error::other("metadata write completion missing")))?;
    if bytes_written != payload_len {
        return Err(storage_error(io::Error::new(
            io::ErrorKind::WriteZero,
            format!("metadata io_uring short write: wrote {bytes_written} of {payload_len} bytes"),
        )));
    }

    // Phase two can replace the durable old sidecar only after phase one was
    // checked in userspace. The directory fsync then makes that rename durable.
    submitter
        .push(
            MetadataSqe::Rename {
                from: tmp_path,
                to: meta_path,
            },
            true,
        )
        .map_err(storage_error)?;
    submitter
        .push(MetadataSqe::DirectoryFsync { path: spool_dir }, false)
        .map_err(storage_error)?;
    let rename_completions = submitter.submit_and_wait().map_err(storage_error)?;
    check_completion_errors(&rename_completions)
}

#[cfg(test)]
fn check_completion_errors(completions: &[RecordedCompletion]) -> Result<()> {
    for completion in completions {
        if let Some(kind) = completion.result {
            if !completion.cancelled {
                return Err(storage_error(io::Error::new(
                    kind,
                    format!("metadata {:?} completion failed", completion.kind),
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[derive(Debug)]
struct FakeLinkedSubmitter {
    queued: Vec<(MetadataSqe, bool)>,
    recorded: Vec<RecordedSqe>,
    completions: Vec<RecordedCompletion>,
    submit_batches: Vec<Vec<MetadataOpKind>>,
    fail: Option<MetadataOpKind>,
    short_write_len: Option<usize>,
    fds: HashMap<PathBuf, i32>,
    next_fd: i32,
    invoked_shards: Vec<usize>,
}

#[cfg(test)]
impl FakeLinkedSubmitter {
    fn successful() -> Self {
        Self::new(None, None)
    }

    fn fail_on(kind: MetadataOpKind) -> Self {
        Self::new(Some(kind), None)
    }

    fn short_write(len: usize) -> Self {
        Self::new(None, Some(len))
    }

    fn new(fail: Option<MetadataOpKind>, short_write_len: Option<usize>) -> Self {
        Self {
            queued: Vec::new(),
            recorded: Vec::new(),
            completions: Vec::new(),
            submit_batches: Vec::new(),
            fail,
            short_write_len,
            fds: HashMap::new(),
            next_fd: 10,
            invoked_shards: Vec::new(),
        }
    }

    fn fd_for(&mut self, path: &Path) -> i32 {
        if let Some(fd) = self.fds.get(path) {
            return *fd;
        }
        let fd = self.next_fd;
        self.next_fd += 1;
        self.fds.insert(path.to_path_buf(), fd);
        fd
    }

    fn record(&mut self, sqe: &MetadataSqe, linked: bool) {
        let recorded = match sqe {
            MetadataSqe::Write { path, .. } => RecordedSqe {
                kind: MetadataOpKind::Write,
                linked,
                path: Some(path.clone()),
                second_path: None,
                fd: Some(self.fd_for(path)),
            },
            MetadataSqe::Fdatasync { path } => RecordedSqe {
                kind: MetadataOpKind::Fdatasync,
                linked,
                path: Some(path.clone()),
                second_path: None,
                fd: Some(self.fd_for(path)),
            },
            MetadataSqe::Rename { from, to } => RecordedSqe {
                kind: MetadataOpKind::Rename,
                linked,
                path: Some(from.clone()),
                second_path: Some(to.clone()),
                fd: None,
            },
            MetadataSqe::DirectoryFsync { path } => RecordedSqe {
                kind: MetadataOpKind::DirectoryFsync,
                linked,
                path: Some(path.clone()),
                second_path: None,
                fd: Some(self.fd_for(path)),
            },
        };
        self.recorded.push(recorded);
    }

    fn apply(&mut self, sqe: &MetadataSqe) -> io::Result<Option<usize>> {
        match sqe {
            MetadataSqe::Write { path, payload } => {
                let len = self.short_write_len.take().unwrap_or(payload.len());
                let len = len.min(payload.len());
                fs::write(path, &payload[..len])?;
                Ok(Some(len))
            }
            MetadataSqe::Fdatasync { .. } => Ok(None),
            MetadataSqe::Rename { from, to } => {
                fs::rename(from, to)?;
                Ok(None)
            }
            MetadataSqe::DirectoryFsync { .. } => Ok(None),
        }
    }
}

#[cfg(test)]
impl MetadataCommitSubmitter for FakeLinkedSubmitter {
    fn begin_commit_on_shard(&mut self, shard_index: usize) -> io::Result<()> {
        self.invoked_shards.push(shard_index);
        Ok(())
    }

    fn push(&mut self, sqe: MetadataSqe, linked: bool) -> io::Result<()> {
        self.record(&sqe, linked);
        self.queued.push((sqe, linked));
        Ok(())
    }

    fn submit_and_wait(&mut self) -> io::Result<Vec<RecordedCompletion>> {
        let batch_kinds = self.queued.iter().map(|(sqe, _)| sqe.kind()).collect();
        self.submit_batches.push(batch_kinds);

        let mut batch_completions = Vec::new();
        let mut cancel_rest = false;
        let queued = std::mem::take(&mut self.queued);
        for (sqe, linked) in queued {
            let kind = sqe.kind();
            let completion = if cancel_rest {
                RecordedCompletion {
                    kind,
                    performed: false,
                    cancelled: true,
                    result: Some(io::ErrorKind::Interrupted),
                    bytes_written: None,
                }
            } else if self.fail == Some(kind) {
                if linked {
                    cancel_rest = true;
                }
                RecordedCompletion {
                    kind,
                    performed: true,
                    cancelled: false,
                    result: Some(io::ErrorKind::Other),
                    bytes_written: None,
                }
            } else {
                let bytes_written = self.apply(&sqe)?;
                RecordedCompletion {
                    kind,
                    performed: true,
                    cancelled: false,
                    result: None,
                    bytes_written,
                }
            };
            batch_completions.push(completion.clone());
            self.completions.push(completion);
        }
        Ok(batch_completions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spool::SpoolState;
    use std::collections::HashMap;
    use tempfile::tempdir;

    const LINKED_CHAIN_TEST_SHARDS: usize = 4;

    fn metadata_for_key_generation(key: String, generation: u64) -> SpoolMetadata {
        SpoolMetadata {
            key,
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

    #[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
    fn metadata_for_routed_key(key: &str) -> SpoolMetadata {
        let mut metadata = metadata_for_key_generation(key.to_owned(), 2);
        metadata.data_path = PathBuf::from(format!("/tmp/{key}.data"));
        metadata
    }

    fn metadata_for_shard(shard_index: usize, num_shards: usize, generation: u64) -> SpoolMetadata {
        for candidate in 0..10_000 {
            let key = format!("linked-chain-shard-{shard_index}-{candidate}");
            #[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
            let routed = crate::io::ring_pool::ring_index_for_key(&key, num_shards);
            #[cfg(any(not(target_os = "linux"), feature = "tokio-fileio-fallback"))]
            let routed = candidate as usize % num_shards;

            if routed == shard_index {
                let mut metadata = metadata_for_key_generation(key, generation);
                metadata.data_path = PathBuf::from(format!(
                    "/tmp/linked-chain-shard-{shard_index}.{generation}.data"
                ));
                return metadata;
            }
        }
        panic!("test could not find metadata key for shard {shard_index} of {num_shards}");
    }

    fn assert_metadata_eq(actual: SpoolMetadata, expected: &SpoolMetadata) {
        assert_eq!(
            serde_json::to_value(&actual).expect("serialize actual metadata"),
            serde_json::to_value(expected).expect("serialize expected metadata")
        );
    }

    #[test]
    fn io_uring_linked_chain_emits_hot_commit_sqes_in_order() {
        for shard_index in [0, LINKED_CHAIN_TEST_SHARDS - 1] {
            let dir = tempdir().expect("create tempdir");
            let new = metadata_for_shard(shard_index, LINKED_CHAIN_TEST_SHARDS, 2);
            let spool_dir = dir.path().join(&new.key);
            let tmp_path = spool_dir.join(TMP_FILE);
            let meta_path = spool_dir.join(META_FILE);
            let mut fake = FakeLinkedSubmitter::successful();

            commit_hot_metadata_on_shard(&mut fake, dir.path(), &new, shard_index)
                .expect("commit metadata");

            assert_eq!(fake.invoked_shards, vec![shard_index]);
            assert_eq!(
                fake.recorded.iter().map(|sqe| sqe.kind).collect::<Vec<_>>(),
                vec![
                    MetadataOpKind::Write,
                    MetadataOpKind::Fdatasync,
                    MetadataOpKind::Rename,
                    MetadataOpKind::DirectoryFsync,
                ],
                "unexpected SQE order for shard {shard_index}"
            );
            assert_eq!(
                fake.recorded
                    .iter()
                    .map(|sqe| sqe.linked)
                    .collect::<Vec<_>>(),
                vec![true, false, true, false],
                "each durability phase must be a separate linked pair for shard {shard_index}"
            );
            assert_eq!(
                fake.submit_batches,
                vec![
                    vec![MetadataOpKind::Write, MetadataOpKind::Fdatasync],
                    vec![MetadataOpKind::Rename, MetadataOpKind::DirectoryFsync],
                ],
                "rename must be submitted only after write completion is verified"
            );
            assert_eq!(fake.recorded[0].path.as_deref(), Some(tmp_path.as_path()));
            assert_eq!(fake.recorded[1].path.as_deref(), Some(tmp_path.as_path()));
            assert_eq!(fake.recorded[0].fd, fake.recorded[1].fd);
            assert_eq!(fake.recorded[2].path.as_deref(), Some(tmp_path.as_path()));
            assert_eq!(
                fake.recorded[2].second_path.as_deref(),
                Some(meta_path.as_path())
            );
            assert_eq!(fake.recorded[3].path.as_deref(), Some(spool_dir.as_path()));
            assert!(fake
                .completions
                .iter()
                .all(|completion| completion.performed));
            assert!(fake
                .completions
                .iter()
                .all(|completion| !completion.cancelled));

            let persisted: SpoolMetadata = serde_json::from_slice(
                &fs::read(meta_path).expect("meta.json written by fake commit"),
            )
            .expect("deserialize committed metadata");
            assert_metadata_eq(persisted, &new);
        }
    }

    #[tokio::test]
    #[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
    async fn uring_store_write_read_delete_and_list() {
        let dir = tempdir().expect("create tempdir");
        let store = UringSidecarMetadataStore::new(dir.path());
        let meta = metadata_for_key_generation("sidecar-test-key".to_string(), 1);

        assert!(store.read(&meta.key).await.expect("read missing").is_none());
        store
            .write(&meta)
            .await
            .expect("write metadata with io_uring");
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
    }

    #[tokio::test]
    #[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
    async fn ring_pool_metadata_commit_routes_by_key() {
        use crate::io::ring_pool::{scoped_test_ring_pool_override, RingPool, RingPoolOptions};
        use std::sync::Arc;

        let dir = tempdir().expect("create tempdir");
        let pool = Arc::new(
            RingPool::new_for_test(RingPoolOptions {
                shard_count: 4,
                queue_capacity: 1024,
                driver_name_prefix: "bobs-metadata-routing-test".to_owned(),
            })
            .expect("metadata routing test ring pool should start"),
        );
        let _override = scoped_test_ring_pool_override(Arc::clone(&pool));
        let store = UringSidecarMetadataStore::new(dir.path());
        let metadata = metadata_for_routed_key("metadata-routing-key");
        let expected_event = RingPool::metadata_commit_routing_event_for_key_for_test(
            &metadata.key,
            pool.shard_count(),
        );

        store
            .write(&metadata)
            .await
            .expect("metadata write should commit before routing assertion");

        assert_eq!(pool.routing_events(), vec![expected_event]);
    }

    #[test]
    fn io_uring_fdatasync_error_never_submits_rename_and_preserves_old_metadata() {
        for shard_index in [0, LINKED_CHAIN_TEST_SHARDS - 1] {
            let dir = tempdir().expect("create tempdir");
            let old = metadata_for_shard(shard_index, LINKED_CHAIN_TEST_SHARDS, 1);
            let mut new = old.clone();
            new.last_write_at = 22;
            new.readable_at = Some(32);
            new.total_bytes_written = 8192;
            new.total_pages = 2;
            new.data_path = PathBuf::from(format!("/tmp/linked-chain-shard-{shard_index}.2.data"));
            let sync_store = crate::metadata::SyncSidecarMetadataStore::new(dir.path());
            sync_store.write_sync(&old).expect("seed old metadata");
            let meta_path = dir.path().join(&old.key).join(META_FILE);
            let old_bytes = fs::read(&meta_path).expect("read old meta.json");
            let mut fake = FakeLinkedSubmitter::fail_on(MetadataOpKind::Fdatasync);

            let err = commit_hot_metadata_on_shard(&mut fake, dir.path(), &new, shard_index)
                .expect_err("fdatasync failure must propagate");

            assert!(matches!(err, BobsError::StorageError(_)));
            assert_eq!(fake.invoked_shards, vec![shard_index]);
            assert_eq!(
                fake.recorded.iter().map(|sqe| sqe.kind).collect::<Vec<_>>(),
                vec![MetadataOpKind::Write, MetadataOpKind::Fdatasync],
                "rename must not be queued before fdatasync succeeds"
            );
            assert_eq!(
                fake.submit_batches,
                vec![vec![MetadataOpKind::Write, MetadataOpKind::Fdatasync]],
            );
            assert_eq!(
                fs::read(&meta_path).expect("read meta.json after failed commit"),
                old_bytes,
                "fdatasync failure must leave final meta.json at its old durable value"
            );
            let persisted: SpoolMetadata =
                serde_json::from_slice(&fs::read(meta_path).expect("read persisted old metadata"))
                    .expect("deserialize old metadata");
            assert_metadata_eq(persisted, &old);
        }
    }

    #[test]
    fn io_uring_short_metadata_write_never_submits_rename_and_preserves_old_metadata() {
        let dir = tempdir().expect("create tempdir");
        let old = metadata_for_key_generation("short-write-key".to_owned(), 1);
        let mut new = metadata_for_key_generation(old.key.clone(), 2);
        new.data_path = old.data_path.with_extension("new.data");
        let sync_store = crate::metadata::SyncSidecarMetadataStore::new(dir.path());
        sync_store.write_sync(&old).expect("seed old metadata");
        let spool_dir = dir.path().join(&old.key);
        let meta_path = spool_dir.join(META_FILE);
        let old_bytes = fs::read(&meta_path).expect("read old meta.json");
        let mut fake = FakeLinkedSubmitter::short_write(7);

        let err = commit_hot_metadata(&mut fake, dir.path(), &new)
            .expect_err("short metadata write must fail before rename");

        assert!(matches!(err, BobsError::StorageError(_)));
        assert_eq!(
            fake.recorded.iter().map(|sqe| sqe.kind).collect::<Vec<_>>(),
            vec![MetadataOpKind::Write, MetadataOpKind::Fdatasync],
        );
        assert_eq!(
            fake.submit_batches,
            vec![vec![MetadataOpKind::Write, MetadataOpKind::Fdatasync]],
        );
        assert_eq!(
            fs::read(&meta_path).expect("read meta.json after short write"),
            old_bytes,
            "a partial temporary file must never replace durable metadata"
        );
        assert_eq!(
            fs::read(spool_dir.join(TMP_FILE)).expect("read partial tmp"),
            serde_json::to_vec(&new).expect("serialize new metadata")[..7],
        );
    }
}
