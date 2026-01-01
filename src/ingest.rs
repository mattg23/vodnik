use std::ops::Range;

use axum::{Json, extract::State};
use serde::Deserialize;
use thiserror::Error;
use tracing::warn;

use crate::{
    AppState,
    api::ApiError,
    meta::{BlockNumber, MetaStore, Quality, SeriesId, SeriesMeta},
};

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

        Ok(())
    }
}

#[derive(Deserialize)]
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

pub(crate) async fn batch_ingest(
    State(state): State<AppState>,
    Json(req): Json<BatchIngest>,
) -> Result<(), ApiError> {
    // TODO: limit req size + add streaming endpoint
    req.validate()?;
    let series = state
        .meta_store
        .get(req.series)
        .await
        .map_err(crate::meta::into_api_error)?;
    let mut start_index = 0;
    let mut current_block = crate::helpers::get_block_id(&series, req.ts[0]) as usize;

    for i in 1..req.ts.len() {
        let next_block = crate::helpers::get_block_id(&series, req.ts[i]) as usize;

        if next_block != current_block {
            write_chunk(
                &state,
                &series,
                BlockNumber(current_block as u64),
                &req,
                start_index..i,
            )
            .await?;

            start_index = i;
            current_block = next_block;
        }
    }

    write_chunk(
        &state,
        &series,
        BlockNumber(current_block as u64),
        &req,
        start_index..req.ts.len(),
    )
    .await
}

async fn write_chunk(
    state: &AppState,
    series: &SeriesMeta,
    block_id: BlockNumber,
    req: &BatchIngest,
    range: Range<usize>,
) -> Result<(), ApiError> {
    const MAX_RETRIES: u32 = 3; // TODO: settings!
    let mut attempt = 0;

    loop {
        let res = state.hot.write(
            &series,
            block_id,
            &req.ts[range.clone()],
            &req.vals,
            &req.qs[range.clone()],
            range.clone(),
        );

        match res {
            crate::hot::WriteResult::Ok { .. } => return Ok(()),
            crate::hot::WriteResult::Busy => {
                attempt += 1;
                warn!("WriteResult::Busy");
                if attempt >= MAX_RETRIES {
                    return Err(ApiError::ResourceLocked);
                }
                // Yield the async task to let the lock holder finish
                tokio::task::yield_now().await;
            }
            crate::hot::WriteResult::NeedsColdStore => todo!(),
        }
    }
}
