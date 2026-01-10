use crate::{
    AppState,
    api::ApiError,
    persistence::{self, write_cold},
    wal::next_txid,
};
use axum::{Json, extract::State};
use tracing::{info, warn};
use vodnik_core::{
    api::{BatchIngest, IngestError, ValueVec},
    meta::{BlockNumber, BlockWritable, Quality, SeriesId, SeriesMeta, StorableNum, WriteBatch},
    wal::{TxId, from_write_batch},
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
    match req.vals {
        ValueVec::F32(items) => batch_writes(&state, &series, req.ts, items, req.qs).await,
        ValueVec::F64(items) => batch_writes(&state, &series, req.ts, items, req.qs).await,
        ValueVec::I32(items) => batch_writes(&state, &series, req.ts, items, req.qs).await,
        ValueVec::I64(items) => batch_writes(&state, &series, req.ts, items, req.qs).await,
        ValueVec::U32(items) => batch_writes(&state, &series, req.ts, items, req.qs).await,
        ValueVec::U64(items) => batch_writes(&state, &series, req.ts, items, req.qs).await,
        ValueVec::Enum(items) => batch_writes(&state, &series, req.ts, items, req.qs).await,
    }
}

async fn batch_writes<T: BlockWritable>(
    state: &AppState,
    series: &SeriesMeta,
    ts: Vec<u64>,
    vals: Vec<T>,
    qs: Vec<Quality>,
) -> Result<(), ApiError> {
    let mut start_index = 0;
    let mut current_block = vodnik_core::helpers::get_block_id(&series, ts[0]) as usize;

    for i in 1..ts.len() {
        let next_block = vodnik_core::helpers::get_block_id(&series, ts[i]) as usize;

        if next_block != current_block {
            let batch = WriteBatch::new(
                series,
                BlockNumber(current_block as u64),
                &ts[start_index..i],
                &vals[start_index..i],
                &qs[start_index..i],
            );

            write_to_wal(state, &batch)?;

            write_chunk(&state, &batch).await?;

            start_index = i;
            current_block = next_block;
        }
    }

    let batch = WriteBatch::new(
        series,
        BlockNumber(current_block as u64),
        &ts[start_index..ts.len()],
        &vals[start_index..ts.len()],
        &qs[start_index..ts.len()],
    );

    write_chunk(&state, &batch).await
}

fn write_to_wal<T: StorableNum>(
    state: &AppState,
    batch: &WriteBatch<'_, T>,
) -> Result<(), ApiError> {
    let tx = TxId(next_txid());
    let mut w_entry = from_write_batch(tx, batch);
    state
        .wal
        .lock()
        .map_err(|e| ApiError::ResourceLocked)?
        .write_entry(&mut w_entry)?;
    Ok(())
}

async fn write_chunk<'a, T: BlockWritable>(
    state: &AppState,
    batch: &'a WriteBatch<'a, T>,
) -> Result<(), ApiError> {
    const MAX_RETRIES: u32 = 3; // TODO: settings!
    let mut attempt = 0;

    loop {
        let res = state.hot.write(batch);

        match res {
            crate::hot::WriteResult::Ok { flushing, .. } => {
                if !flushing.is_empty() {
                    let s = state.clone();
                    let sid = batch.series.id;
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
                let cold_write_result = write_cold(&state.storage, &state.block_meta, batch).await;
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
