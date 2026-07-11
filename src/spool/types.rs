// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpoolState {
    Creating,
    Writing,
    WriteLocked, // write-lock mode: writes OK, reads blocked until Complete
    Complete,    // writer finished, all data readable
    Deleting,
}

impl SpoolState {
    pub fn is_readable(&self) -> bool {
        matches!(self, SpoolState::Complete | SpoolState::Writing)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Page size fixed when this spool was created. Zero means legacy metadata
    /// written before page sizes were persisted and is rejected during recovery.
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
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_is_readable_per_state() {
        assert!(!SpoolState::Creating.is_readable());
        assert!(SpoolState::Writing.is_readable());
        assert!(!SpoolState::WriteLocked.is_readable());
        assert!(SpoolState::Complete.is_readable());
        assert!(!SpoolState::Deleting.is_readable());
    }
}
