// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpoolState {
    Creating,
    Writing,
    WriteLocked, // write-lock mode: writes OK, reads blocked until Complete
    /// Completion has started but its metadata commit is not known to be durable.
    /// Writes are fail-stop; completion remains retryable.
    Completing,
    Complete, // writer finished, all data readable
    Deleting,
}

impl SpoolState {
    pub fn is_readable(&self) -> bool {
        matches!(self, SpoolState::Complete | SpoolState::Writing)
    }
}

/// Integrity record committed atomically with terminal spool metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrityMetadata {
    pub algorithm: String,
    pub length: u64,
    pub checksum: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpoolMetadata {
    pub key: String,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub state: SpoolState,
    pub write_locked: bool,
    pub created_at: u64,    // unix timestamp secs
    pub last_write_at: u64, // unix timestamp secs
    pub last_read_at: Option<u64>,
    #[serde(default)]
    pub readable_at: Option<u64>, // unix secs; set when spool first becomes readable
    /// Page size fixed when this spool was created. Zero identifies a legacy
    /// sidecar whose stride must be derived and durably migrated during recovery.
    #[serde(default)]
    pub page_size: u64,
    pub total_bytes_written: u64,
    pub total_pages: u64,
    pub final_page_size: Option<u64>, // size of last (partial) page after complete
    pub data_path: PathBuf,
    /// Caller-provided labels propagated to metrics as OTel attributes.
    /// Bobs does not interpret these — they are pass-through dimensions.
    #[serde(default)]
    pub labels: HashMap<String, String>,
    /// Present for spools created by integrity-aware BOBS versions.
    #[serde(default)]
    pub integrity: Option<IntegrityMetadata>,
    /// Persisted quarantine marker. A quarantined result is never served again.
    #[serde(default)]
    pub integrity_failure: Option<String>,
}

#[derive(Deserialize)]
enum PersistedSpoolState {
    Creating,
    Writing,
    WriteLocked,
    Completing,
    Readable,
    Complete,
    Deleting,
}

#[derive(Deserialize)]
struct PersistedSpoolMetadata {
    key: String,
    content_type: Option<String>,
    content_encoding: Option<String>,
    state: PersistedSpoolState,
    write_locked: bool,
    created_at: u64,
    last_write_at: u64,
    last_read_at: Option<u64>,
    #[serde(default)]
    readable_at: Option<u64>,
    #[serde(default)]
    page_size: u64,
    total_bytes_written: u64,
    total_pages: u64,
    final_page_size: Option<u64>,
    data_path: PathBuf,
    #[serde(default)]
    labels: HashMap<String, String>,
    #[serde(default)]
    integrity: Option<IntegrityMetadata>,
    #[serde(default)]
    integrity_failure: Option<String>,
}

impl<'de> Deserialize<'de> for SpoolMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let persisted = PersistedSpoolMetadata::deserialize(deserializer)?;
        let legacy_readable = matches!(persisted.state, PersistedSpoolState::Readable);
        let state = match persisted.state {
            PersistedSpoolState::Creating => SpoolState::Creating,
            PersistedSpoolState::Writing => SpoolState::Writing,
            PersistedSpoolState::WriteLocked => SpoolState::WriteLocked,
            PersistedSpoolState::Completing => SpoolState::Completing,
            PersistedSpoolState::Readable | PersistedSpoolState::Complete => SpoolState::Complete,
            PersistedSpoolState::Deleting => SpoolState::Deleting,
        };

        Ok(Self {
            key: persisted.key,
            content_type: persisted.content_type,
            content_encoding: persisted.content_encoding,
            state,
            write_locked: persisted.write_locked,
            created_at: persisted.created_at,
            last_write_at: persisted.last_write_at,
            last_read_at: persisted.last_read_at,
            readable_at: persisted.readable_at,
            page_size: persisted.page_size,
            total_bytes_written: persisted.total_bytes_written,
            total_pages: persisted.total_pages,
            // `Some(0)` is impossible for valid terminal metadata and acts only
            // as a transient recovery marker for the removed Readable state.
            final_page_size: if legacy_readable {
                Some(0)
            } else {
                persisted.final_page_size
            },
            data_path: persisted.data_path,
            labels: persisted.labels,
            integrity: persisted.integrity,
            integrity_failure: persisted.integrity_failure,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_is_readable_per_state() {
        assert!(!SpoolState::Creating.is_readable());
        assert!(SpoolState::Writing.is_readable());
        assert!(!SpoolState::WriteLocked.is_readable());
        assert!(!SpoolState::Completing.is_readable());
        assert!(SpoolState::Complete.is_readable());
        assert!(!SpoolState::Deleting.is_readable());
    }
}
