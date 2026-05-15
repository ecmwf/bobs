use crate::error::{BobsError, Result};
use crate::spool::SpoolMetadata;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;

const META_FILE: &str = "meta.json";
const TMP_FILE: &str = "meta.json.tmp";
const EXPORT_MAGIC: &str = "bobs-legacy-redb-export-v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyRedbRow {
    pub key: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MigrationReport {
    pub rows_seen: usize,
    pub sidecars_written: usize,
}

/// Build a legacy migration fixture using the same row payloads as the former
/// redb table. The project no longer links redb at runtime; tests exercise the
/// supported no-redb import path with a line-delimited export file.
#[cfg(test)]
pub fn create_legacy_db(
    db_path: impl AsRef<Path>,
    metadata: &[SpoolMetadata],
) -> Result<Vec<LegacyRedbRow>> {
    let db_path = db_path.as_ref();
    if let Some(parent) = db_path.parent() {
        fs::create_dir_all(parent).map_err(storage_io)?;
    }

    let mut rows = Vec::with_capacity(metadata.len());
    let mut file = File::create(db_path).map_err(storage_io)?;
    writeln!(file, "{EXPORT_MAGIC}").map_err(storage_io)?;

    for meta in metadata {
        let bytes = serde_json::to_vec(meta)
            .map_err(|error| BobsError::SerializationError(error.to_string()))?;
        let record = serde_json::json!({
            "key": meta.key,
            "metadata": bytes,
        });
        serde_json::to_writer(&mut file, &record)
            .map_err(|error| BobsError::SerializationError(error.to_string()))?;
        writeln!(file).map_err(storage_io)?;
        rows.push(LegacyRedbRow {
            key: meta.key.clone(),
            bytes,
        });
    }

    file.sync_data().map_err(storage_io)?;
    if let Some(parent) = db_path.parent() {
        sync_directory(parent).map_err(storage_io)?;
    }

    Ok(rows)
}

#[cfg(test)]
pub fn read_legacy_rows(db_path: impl AsRef<Path>) -> Result<Vec<LegacyRedbRow>> {
    read_rows(db_path.as_ref())
}

/// Migrate legacy redb metadata into durable sidecar `meta.json` files.
///
/// The redb crate has been removed from the runtime dependency graph. To keep a
/// final no-redb migration path, this importer accepts a read-only export file
/// whose rows contain the exact JSON bytes previously stored as redb values.
/// Native redb database files are left untouched and return a storage error so
/// operators can run an older build to export or migrate them before upgrading.
///
/// This is retry-safe for supported exports:
///
/// * missing legacy databases are a no-op;
/// * every row is read and validated as `SpoolMetadata` before any sidecar is
///   written, so corrupt rows leave the legacy file in place for operator
///   review;
/// * sidecars are written from the exact legacy value bytes through
///   `meta.json.tmp`, fdatasynced, atomically renamed, then the spool directory
///   is fsynced;
/// * only after all final sidecars are durable is the legacy file removed and
///   its parent/data directories fsynced.
pub fn migrate_legacy_candidates(data_dir: impl AsRef<Path>) -> Result<MigrationReport> {
    let data_dir = data_dir.as_ref();
    let mut aggregate = MigrationReport::default();

    for file_name in ["spools.redb", "bobs.redb"] {
        let report = migrate_from_redb(data_dir.join(file_name), data_dir)?;
        aggregate.rows_seen += report.rows_seen;
        aggregate.sidecars_written += report.sidecars_written;
    }

    Ok(aggregate)
}

pub fn migrate_from_redb(
    db_path: impl AsRef<Path>,
    data_dir: impl AsRef<Path>,
) -> Result<MigrationReport> {
    let db_path = db_path.as_ref();
    let data_dir = data_dir.as_ref();

    if !db_path.exists() {
        return Ok(MigrationReport::default());
    }

    let rows = read_rows(db_path)?;
    let mut validated = Vec::with_capacity(rows.len());
    for row in rows {
        let metadata = match serde_json::from_slice::<SpoolMetadata>(&row.bytes) {
            Ok(metadata) => metadata,
            Err(error) => {
                tracing::error!(
                    db = %db_path.display(),
                    key = %row.key,
                    error = %error,
                    "legacy redb export row contains corrupt metadata; leaving file for operator review"
                );
                return Err(BobsError::SerializationError(error.to_string()));
            }
        };

        if metadata.key != row.key {
            let message = format!(
                "legacy redb export row key mismatch: table key {:?}, metadata key {:?}",
                row.key, metadata.key
            );
            tracing::error!(
                db = %db_path.display(),
                key = %row.key,
                metadata_key = %metadata.key,
                "legacy redb export row key mismatch; leaving file for operator review"
            );
            return Err(BobsError::SerializationError(message));
        }

        validated.push(row);
    }

    fs::create_dir_all(data_dir).map_err(storage_io)?;

    let mut sidecars_written = 0;
    for row in &validated {
        if write_sidecar_bytes(data_dir, row)? {
            sidecars_written += 1;
        }
    }

    sync_directory(data_dir).map_err(storage_io)?;

    fs::remove_file(db_path).map_err(storage_io)?;
    sync_directory(data_dir).map_err(storage_io)?;
    if let Some(parent) = db_path.parent().filter(|parent| *parent != data_dir) {
        sync_directory(parent).map_err(storage_io)?;
    }

    Ok(MigrationReport {
        rows_seen: validated.len(),
        sidecars_written,
    })
}

fn read_rows(db_path: &Path) -> Result<Vec<LegacyRedbRow>> {
    let file = File::open(db_path).map_err(storage_io)?;
    let mut lines = BufReader::new(file).lines();
    match lines.next() {
        Some(Ok(header)) if header == EXPORT_MAGIC => {}
        Some(Ok(_)) => {
            return Err(BobsError::StorageError(Box::new(io::Error::new(
                io::ErrorKind::InvalidData,
                "native redb migration requires an exported legacy metadata file",
            ))));
        }
        Some(Err(error)) => return Err(storage_io(error)),
        None => return Ok(Vec::new()),
    }

    let mut rows = Vec::new();
    for line in lines {
        let line = line.map_err(storage_io)?;
        if line.trim().is_empty() {
            continue;
        }
        let row: LegacyExportRow = serde_json::from_str(&line)
            .map_err(|error| BobsError::SerializationError(error.to_string()))?;
        rows.push(LegacyRedbRow {
            key: row.key,
            bytes: row.metadata,
        });
    }

    rows.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(rows)
}

#[derive(serde::Deserialize)]
struct LegacyExportRow {
    key: String,
    metadata: Vec<u8>,
}

fn write_sidecar_bytes(data_dir: &Path, row: &LegacyRedbRow) -> Result<bool> {
    let spool_dir = data_dir.join(&row.key);
    let meta_path = spool_dir.join(META_FILE);
    let tmp_path = spool_dir.join(TMP_FILE);

    fs::create_dir_all(&spool_dir).map_err(storage_io)?;

    if matches!(fs::read(&meta_path), Ok(existing) if existing == row.bytes) {
        let removed_tmp = remove_file_if_present(&tmp_path)?;
        if removed_tmp {
            sync_directory(&spool_dir).map_err(storage_io)?;
        }
        return Ok(false);
    }

    {
        let mut tmp = File::create(&tmp_path).map_err(storage_io)?;
        tmp.write_all(&row.bytes).map_err(storage_io)?;
        tmp.sync_data().map_err(storage_io)?;
    }

    fs::rename(&tmp_path, &meta_path).map_err(storage_io)?;
    sync_directory(&spool_dir).map_err(storage_io)?;
    Ok(true)
}

fn remove_file_if_present(path: &Path) -> Result<bool> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(storage_io(error)),
    }
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn storage_io(error: io::Error) -> BobsError {
    BobsError::StorageError(Box::new(error))
}
