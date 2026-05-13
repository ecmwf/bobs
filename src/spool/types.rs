use crate::error::{BobsError, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpoolState {
    Creating,
    Writing,
    WriteLocked, // write-lock mode: writes OK, reads blocked until Complete
    Readable,    // after write-lock released
    Complete,    // writer finished, all data readable
    Deleting,
}

impl SpoolState {
    pub fn can_transition_to(&self, target: &SpoolState) -> bool {
        use SpoolState::*;
        matches!(
            (self, target),
            (Creating, Writing)
            | (Creating, WriteLocked)
            | (Writing, WriteLocked)
            | (Writing, Complete)
            | (Writing, Deleting)
            | (WriteLocked, Readable)
            | (WriteLocked, Complete)   // complete releases write-lock
            | (WriteLocked, Deleting)
            | (Readable, WriteLocked)
            | (Readable, Complete)
            | (Complete, Deleting)
            | (Readable, Deleting)
        )
    }

    pub fn transition_to(&self, target: SpoolState) -> Result<SpoolState> {
        if self.can_transition_to(&target) {
            Ok(target)
        } else {
            Err(BobsError::InvalidState {
                current: format!("{:?}", self),
                attempted_action: format!("transition to {:?}", target),
            })
        }
    }

    pub fn is_readable(&self) -> bool {
        matches!(
            self,
            SpoolState::Complete | SpoolState::Readable | SpoolState::Writing
        )
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
    pub total_bytes_written: u64,
    pub checksum_crc32c: Option<u32>,
    pub total_pages: u64,
    pub final_page_size: Option<u64>, // size of last (partial) page after complete
    pub data_path: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_transitions() {
        assert!(SpoolState::Creating.can_transition_to(&SpoolState::Writing));
        assert!(SpoolState::Writing.can_transition_to(&SpoolState::Complete));
        assert!(SpoolState::WriteLocked.can_transition_to(&SpoolState::Complete));
        assert!(SpoolState::Complete.can_transition_to(&SpoolState::Deleting));
    }

    #[test]
    fn test_invalid_transitions() {
        assert!(!SpoolState::Complete.can_transition_to(&SpoolState::Writing));
        assert!(!SpoolState::Deleting.can_transition_to(&SpoolState::Writing));
        assert!(!SpoolState::Writing.can_transition_to(&SpoolState::Creating));
    }

    #[test]
    fn test_transition_to_returns_error_on_invalid() {
        let result = SpoolState::Complete.transition_to(SpoolState::Writing);
        assert!(result.is_err());
    }

    #[test]
    fn test_transition_to_success() {
        let result = SpoolState::Creating.transition_to(SpoolState::Writing);
        assert_eq!(result.unwrap(), SpoolState::Writing);

        let result = SpoolState::Writing.transition_to(SpoolState::Complete);
        assert_eq!(result.unwrap(), SpoolState::Complete);

        let result = SpoolState::WriteLocked.transition_to(SpoolState::Complete);
        assert_eq!(result.unwrap(), SpoolState::Complete);

        let result = SpoolState::Complete.transition_to(SpoolState::Deleting);
        assert_eq!(result.unwrap(), SpoolState::Deleting);
    }

    #[test]
    fn test_is_readable_per_state() {
        assert!(!SpoolState::Creating.is_readable());
        assert!(SpoolState::Writing.is_readable());
        assert!(!SpoolState::WriteLocked.is_readable());
        assert!(SpoolState::Readable.is_readable());
        assert!(SpoolState::Complete.is_readable());
        assert!(!SpoolState::Deleting.is_readable());
    }
}
