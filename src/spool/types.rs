use crate::error::{BobsError, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpoolState {
    Creating,
    Writing,
    WriteLocked, // write-lock mode: writes OK, reads blocked until Closed
    Readable,    // closed and readable (after write-lock released)
    Closed,      // writer closed, readable
    Deleting,
}

impl SpoolState {
    pub fn can_transition_to(&self, target: &SpoolState) -> bool {
        use SpoolState::*;
        matches!(
            (self, target),
            (Creating, Writing)
            | (Creating, WriteLocked)
            | (Writing, Closed)
            | (Writing, Deleting)
            | (WriteLocked, Closed)   // close releases write-lock
            | (WriteLocked, Deleting)
            | (Closed, Deleting)
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
            SpoolState::Closed | SpoolState::Readable | SpoolState::Writing
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpoolMetadata {
    pub key: String,
    pub bob_id: String,
    pub content_type: Option<String>,
    pub state: SpoolState,
    pub write_locked: bool,
    pub created_at: u64,    // unix timestamp secs
    pub last_write_at: u64, // unix timestamp secs
    pub last_read_at: Option<u64>,
    pub total_bytes_written: u64,
    pub total_pages: u64,
    pub final_page_size: Option<u64>, // size of last (partial) page after close
    pub data_path: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_transitions() {
        assert!(SpoolState::Creating.can_transition_to(&SpoolState::Writing));
        assert!(SpoolState::Writing.can_transition_to(&SpoolState::Closed));
        assert!(SpoolState::WriteLocked.can_transition_to(&SpoolState::Closed));
        assert!(SpoolState::Closed.can_transition_to(&SpoolState::Deleting));
    }

    #[test]
    fn test_invalid_transitions() {
        assert!(!SpoolState::Closed.can_transition_to(&SpoolState::Writing));
        assert!(!SpoolState::Deleting.can_transition_to(&SpoolState::Writing));
        assert!(!SpoolState::Writing.can_transition_to(&SpoolState::Creating));
    }

    #[test]
    fn test_transition_to_returns_error_on_invalid() {
        let result = SpoolState::Closed.transition_to(SpoolState::Writing);
        assert!(result.is_err());
    }
}
