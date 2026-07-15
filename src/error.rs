// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use thiserror::Error;

#[derive(Debug, Error)]
pub enum BobsError {
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("spool exceeds configured maximum of {max_bytes} bytes")]
    SpoolTooLarge { max_bytes: u64 },

    #[error("timed out waiting for create admission capacity")]
    AdmissionTimeout,

    #[error("spool not found: {key}")]
    SpoolNotFound { key: String },

    #[error("spool already exists: {key}")]
    SpoolAlreadyExists { key: String },

    #[error("spool is closed")]
    SpoolClosed,

    #[error("spool is write-locked (not yet closed by writer)")]
    SpoolLocked,

    #[error("offset mismatch: expected {expected}, got {got}")]
    OffsetMismatch { expected: u64, got: u64 },

    #[error("size mismatch: expected {expected}, got {actual}")]
    SizeMismatch { expected: u64, actual: u64 },

    #[error("invalid range header: {0}")]
    InvalidRange(String),

    #[error("range not satisfiable: {reason}")]
    RangeNotSatisfiable { total: Option<u64>, reason: String },

    #[error("configuration error: {0}")]
    ConfigurationError(String),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("storage error: {0}")]
    StorageError(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("serialization error: {0}")]
    SerializationError(String),

    #[error("metadata sidecar is too large: {actual} bytes exceeds {maximum}-byte limit")]
    MetadataTooLarge { actual: u64, maximum: u64 },

    #[error("metadata sidecar contains unsupported field: {field}")]
    MetadataUnknownField { field: String },

    #[error("invalid state transition: cannot {attempted_action} when in {current} state")]
    InvalidState {
        current: String,
        attempted_action: String,
    },
}

pub type Result<T> = std::result::Result<T, BobsError>;
