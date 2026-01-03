use std::ops::Range;

use crate::{
    AppState,
    api::ApiError,
    persistence::{self, write_cold},
};
use axum::{Json, extract::State};
use tracing::{info, warn};
use vodnik_core::{
    api::{BatchIngest, IngestError, ValueVec},
    meta::{BlockNumber, SeriesId, SeriesMeta},
};

impl From<IngestError> for ApiError {
    fn from(err: IngestError) -> Self {
        match err {
            IngestError::LengthMismatch => ApiError::BadRequest(err.to_string()),
            IngestError::InvalidTimestamp(_) => ApiError::Unprocessable(err.to_string()),
            IngestError::TypeMismatch => ApiError::BadRequest(err.to_string()),
        }
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
