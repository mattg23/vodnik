use serde::Deserialize;
use thiserror::Error;
use tracing::warn;

use crate::{api::ApiError, meta::SeriesId};

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("timestamp and value length mismatch")]
    LengthMismatch,

    #[error("invalid timestamp: {0}")]
    InvalidTimestamp(String),

    #[error("value type does not match series type")]
    TypeMismatch,
}

impl From<IngestError> for ApiError {
    fn from(err: IngestError) -> Self {
        match err {
            IngestError::LengthMismatch => ApiError::BadRequest(err.to_string()),
            IngestError::InvalidTimestamp(_) => ApiError::Unprocessable(err.to_string()),
            IngestError::TypeMismatch => ApiError::BadRequest(err.to_string()),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct BatchIngest {
    pub series: SeriesId,
    // assume UNIX TS in ms (aka ms after UNIX EPOCH) for now
    // once we have ICU support, we'll also support parsing ts.
    pub ts: Vec<u64>,
    #[serde(flatten)]
    pub vals: ValueVec,
}

impl BatchIngest {
    pub fn validate(&self) -> Result<(), IngestError> {
        if self.ts.len() != self.vals.len() {
            warn!("length mismatch");
            return Err(IngestError::LengthMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", content = "values")]
pub enum ValueVec {
    #[serde(alias = "f32")]
    F32(Vec<f32>),
    #[serde(alias = "f64")]
    F64(Vec<f64>),
    #[serde(alias = "i32")]
    I32(Vec<i32>),
    #[serde(alias = "i64")]
    I64(Vec<i64>),
    #[serde(alias = "u32")]
    U32(Vec<u32>),
    #[serde(alias = "u64")]
    U64(Vec<u64>),
    #[serde(alias = "enum")]
    Enum(Vec<u8>),
}

impl ValueVec {
    pub fn len(&self) -> usize {
        match self {
            ValueVec::F32(v) => v.len(),
            ValueVec::F64(v) => v.len(),
            ValueVec::I32(v) => v.len(),
            ValueVec::I64(v) => v.len(),
            ValueVec::U32(v) => v.len(),
            ValueVec::U64(v) => v.len(),
            ValueVec::Enum(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
