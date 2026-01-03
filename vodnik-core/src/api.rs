use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

use crate::meta::{Quality, SeriesId, StorageType};

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("timestamp and value length mismatch")]
    LengthMismatch,

    #[error("invalid timestamp: {0}")]
    InvalidTimestamp(String),

    #[error("value type does not match series type")]
    TypeMismatch,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BatchIngest {
    pub series: SeriesId,
    // assume UNIX TS in ms (aka ms after UNIX EPOCH) for now
    // once we have ICU support, we'll also support parsing ts.
    pub ts: Vec<u64>,
    pub qs: Vec<Quality>,
    #[serde(flatten)]
    pub vals: ValueVec,
}

impl BatchIngest {
    pub fn validate(&self) -> Result<(), IngestError> {
        if self.ts.len() != self.vals.len() && self.vals.len() == self.qs.len() {
            warn!("length mismatch");
            return Err(IngestError::LengthMismatch);
        }

        if self.ts.len() == 0 {
            return Err(IngestError::InvalidTimestamp(
                "no timestamps given".to_string(),
            ));
        }

        for window in self.ts.windows(2) {
            let t1 = window[0];
            let t2 = window[1];
            if t1 > t2 {
                return Err(IngestError::InvalidTimestamp(
                    "timestamps must be sorted in ascending order".to_string(),
                ));
            }
        }

        // TODO: for float values we need to reject NaN

        Ok(())
    }

    pub fn check_type(&self, stype: StorageType) -> Result<(), IngestError> {
        match (&self.vals, stype) {
            (ValueVec::F32(_), StorageType::Float32) => Ok(()),
            (ValueVec::F64(_), StorageType::Float64) => Ok(()),
            (ValueVec::I32(_), StorageType::Int32) => Ok(()),
            (ValueVec::I64(_), StorageType::Int64) => Ok(()),
            (ValueVec::U32(_), StorageType::UInt32) => Ok(()),
            (ValueVec::U64(_), StorageType::UInt64) => Ok(()),
            (ValueVec::Enum(_), StorageType::Enumeration) => Ok(()),
            _ => Err(IngestError::TypeMismatch),
        }
    }
}

#[derive(Deserialize, Serialize)]
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

impl std::fmt::Debug for ValueVec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::F32(v) => write!(f, "F32(len={})", v.len()),
            Self::F64(v) => write!(f, "F64(len={})", v.len()),
            Self::I32(v) => write!(f, "I32(len={})", v.len()),
            Self::I64(v) => write!(f, "I64(len={})", v.len()),
            Self::U32(v) => write!(f, "U32(len={})", v.len()),
            Self::U64(v) => write!(f, "U64(len={})", v.len()),
            Self::Enum(v) => write!(f, "Enum(len={})", v.len()),
        }
    }
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
