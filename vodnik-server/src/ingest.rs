use std::ops::Range;

use crate::{
    AppState,
    api::ApiError,
    persistence::{self, write_cold},
};
use axum::{Json, extract::State};
use serde::Deserialize;
use thiserror::Error;
use tracing::{info, warn};
use vodnik_core::meta::{BlockNumber, Quality, SeriesId, SeriesMeta, StorageType};

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

    req.check_type(series.storage_type)?;

    let mut start_index = 0;
    let mut current_block = vodnik_core::helpers::get_block_id(&series, req.ts[0]) as usize;

    for i in 1..req.ts.len() {
        let next_block = vodnik_core::helpers::get_block_id(&series, req.ts[i]) as usize;

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
            crate::hot::WriteResult::Ok { flushing, .. } => {
                if !flushing.is_empty() {
                    let s = state.clone();
                    let sid = series.id;
                    tokio::spawn(async move {
                        flush_background(&s, sid, flushing).await;
                    });
                }
                return Ok(());
            }
            crate::hot::WriteResult::Busy => {
                attempt += 1;
                warn!("WriteResult::Busy");
                if attempt >= MAX_RETRIES {
                    return Err(ApiError::ResourceLocked);
                }
                // Yield the async task to let the lock holder finish
                tokio::task::yield_now().await;
            }
            crate::hot::WriteResult::NeedsColdStore => {
                let cold_write_result = match &req.vals {
                    ValueVec::F32(items) => {
                        write_cold(
                            &state.storage,
                            &state.block_meta,
                            series,
                            block_id,
                            &req.ts[range.clone()],
                            &req.qs[range.clone()],
                            &items[range.clone()],
                        )
                        .await
                    }

                    ValueVec::F64(items) => {
                        write_cold(
                            &state.storage,
                            &state.block_meta,
                            series,
                            block_id,
                            &req.ts[range.clone()],
                            &req.qs[range.clone()],
                            &items[range.clone()],
                        )
                        .await
                    }
                    ValueVec::I32(items) => {
                        write_cold(
                            &state.storage,
                            &state.block_meta,
                            series,
                            block_id,
                            &req.ts[range.clone()],
                            &req.qs[range.clone()],
                            &items[range.clone()],
                        )
                        .await
                    }
                    ValueVec::I64(items) => {
                        write_cold(
                            &state.storage,
                            &state.block_meta,
                            series,
                            block_id,
                            &req.ts[range.clone()],
                            &req.qs[range.clone()],
                            &items[range.clone()],
                        )
                        .await
                    }
                    ValueVec::U32(items) => {
                        write_cold(
                            &state.storage,
                            &state.block_meta,
                            series,
                            block_id,
                            &req.ts[range.clone()],
                            &req.qs[range.clone()],
                            &items[range.clone()],
                        )
                        .await
                    }
                    ValueVec::U64(items) => {
                        write_cold(
                            &state.storage,
                            &state.block_meta,
                            series,
                            block_id,
                            &req.ts[range.clone()],
                            &req.qs[range.clone()],
                            &items[range.clone()],
                        )
                        .await
                    }
                    ValueVec::Enum(items) => {
                        write_cold(
                            &state.storage,
                            &state.block_meta,
                            series,
                            block_id,
                            &req.ts[range.clone()],
                            &req.qs[range.clone()],
                            &items[range.clone()],
                        )
                        .await
                    }
                };
                return cold_write_result;
            }
        }
    }
}

async fn flush_background(state: &AppState, series: SeriesId, blocks_to_flush: Vec<BlockNumber>) {
    for block_id in blocks_to_flush.iter() {
        if let Some(block) = state.hot.take_flushing_block(series, *block_id) {
            let r = persistence::flush_block(
                &state.storage,
                &state.block_meta,
                series,
                *block_id,
                &block,
            )
            .await;
            if r.is_ok() {
                info!("flushed block {block_id:?} for series {series}");
            }
        }
    }
}
