use thiserror::Error;

#[derive(Debug, Error)]
pub enum BobsError {
    #[error("spool not found: {key}")]
    SpoolNotFound { key: String },

    #[error("spool is closed")]
    SpoolClosed,

    #[error("spool is write-locked (not yet closed by writer)")]
    SpoolLocked,

    #[error("offset mismatch: expected {expected}, got {got}")]
    OffsetMismatch { expected: u64, got: u64 },

    #[error("reader already active on this spool")]
    ReaderAlreadyActive,

    #[error("writer is inactive")]
    WriterInactive,

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("storage error: {0}")]
    StorageError(#[from] redb::Error),

    #[error("serialization error: {0}")]
    SerializationError(String),

    #[error("invalid state transition: cannot {attempted_action} when in {current} state")]
    InvalidState {
        current: String,
        attempted_action: String,
    },
}

pub type Result<T> = std::result::Result<T, BobsError>;
