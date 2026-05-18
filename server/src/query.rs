use axum::{
    Json,
    extract::{Path, State},
};
use serde::{Deserialize, Serialize};

use crate::{AppState, api::ApiError, persistence};
use vodnik_core::{
    api::{IntoValueVec, ValueVec},
    helpers,
    meta::{
        BinaryAccumulator, BlockNumber, BlockReadable, Quality, SeriesId, SeriesMeta, SizedBlock,
        StorableNum, StorageType,
    },
};

pub(crate) async fn read_single_block(
    State(state): State<AppState>,
    Path((series_id, block_id)): Path<(SeriesId, BlockNumber)>,
) -> Result<Json<SizedBlock>, ApiError> {
    let b = persistence::read_block_from_storage(
        &state.storage,
        &state.block_meta,
        series_id,
        block_id,
    )
    .await?;

    Ok(Json(b))
}

#[derive(Debug, Deserialize)]
pub struct QuerySingleSeries {
    pub ts_left: String,
    pub ts_right: String,
}

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub ts: Vec<u64>,
    pub qs: Vec<Quality>,
    pub vals: ValueVec,
}

pub(crate) async fn read_single_series_simple(
    State(state): State<AppState>,
    Path(series_id): Path<SeriesId>,
    Json(query): Json<QuerySingleSeries>,
) -> Result<Json<QueryResponse>, ApiError> {
    // parse timestamps

    let series_meta = state.meta_store.get(series_id).await?;
    let tz = chrono_tz::Tz::from_str_insensitive(series_meta.tz.as_str())
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    let left = parse_to_epoch_millis(&query.ts_left, "%Y-%m-%d %H:%M:%S", tz)?;
    let right = parse_to_epoch_millis(&query.ts_right, "%Y-%m-%d %H:%M:%S", tz)?;

    if left > right {
        return Err(ApiError::BadRequest(
            "left timestamp is later than right timestamp".to_owned(),
        ));
    }
    //let data= query_raw(&state, left, right, &series_meta).await?; //
    let data = match series_meta.storage_type {
        StorageType::Float32 => query_raw::<f32>(&state, left, right, &series_meta).await?,
        StorageType::Float64 => query_raw::<f64>(&state, left, right, &series_meta).await?,
        StorageType::Int32 => query_raw::<i32>(&state, left, right, &series_meta).await?,
        StorageType::Int64 => query_raw::<i64>(&state, left, right, &series_meta).await?,
        StorageType::UInt32 => query_raw::<u32>(&state, left, right, &series_meta).await?,
        StorageType::UInt64 => query_raw::<i64>(&state, left, right, &series_meta).await?,
        StorageType::Enumeration => query_raw::<u8>(&state, left, right, &series_meta).await?,
    };

    Ok(Json(data))
}

async fn query_raw<T>(
    state: &AppState,
    left: u64,
    right: u64,
    series: &SeriesMeta,
) -> Result<QueryResponse, ApiError>
where
    T: StorableNum + BlockReadable + IntoValueVec,
    T::Accumulator: BinaryAccumulator,
{
    let left_block = BlockNumber(helpers::get_block_id(series, left));
    let right_block = BlockNumber(helpers::get_block_id(series, right));

    let blocks_to_fetch = state
        .block_meta
        .list_in_range::<T>(series.id, left_block, right_block)
        .await?;

    let blocks_found = blocks_to_fetch.len();
    let samples_per_block = helpers::get_block_length(series) as usize;

    // TODO: for small result sizes we can alloc & return
    //       for larger results sizes we prob want a return streaming results path
    let guess = blocks_found * samples_per_block;
    let mut ret_vals = Vec::<T>::with_capacity(guess);
    let mut ret_qs = Vec::<Quality>::with_capacity(guess);
    let mut ret_ts = Vec::<u64>::with_capacity(guess);

    if blocks_to_fetch.len() == 0 {
        return Ok(QueryResponse {
            ts: Vec::new(),
            qs: Vec::new(),
            vals: ValueVec::create::<T>(Vec::new()),
        });
    }

    // TODO: query hot blocks as well
    let last_block_idx = blocks_to_fetch.len() - 1;
    for (i, (b_num, _)) in blocks_to_fetch.iter().enumerate() {
        let block = persistence::read_block_from_storage(
            &state.storage,
            &state.block_meta,
            series.id,
            *b_num,
        )
        .await?;

        match i {
            x if x == 0 && x == last_block_idx => SizedBlock::read_from_block_range::<T>(
                &block,
                &mut ret_vals,
                &mut ret_qs,
                &mut ret_ts,
                b_num,
                series,
                Some(left),
                Some(right),
            ),
            0 => SizedBlock::read_from_block_range::<T>(
                &block,
                &mut ret_vals,
                &mut ret_qs,
                &mut ret_ts,
                b_num,
                series,
                Some(left),
                None,
            ),
            last_block_idx => SizedBlock::read_from_block_range::<T>(
                &block,
                &mut ret_vals,
                &mut ret_qs,
                &mut ret_ts,
                b_num,
                series,
                None,
                Some(right),
            ),
            _ => SizedBlock::read::<T>(
                &block,
                &mut ret_vals,
                &mut ret_qs,
                &mut ret_ts,
                b_num,
                series,
            ),
        };
    }

    Ok(QueryResponse {
        ts: ret_ts,
        qs: ret_qs,
        vals: ValueVec::create::<T>(ret_vals),
    })
}

fn parse_to_epoch_millis(
    input: &str,
    format: &str,
    tz: impl chrono::TimeZone,
) -> Result<u64, ApiError> {
    let naive = chrono::NaiveDateTime::parse_from_str(input, format)
        .map_err(|e| ApiError::BadRequest(format!("parse error: {e}")))?;
    // 2) Interpret naive time in the given timezone
    let local = tz.from_local_datetime(&naive);

    let zoned = match local {
        chrono::LocalResult::Single(dt) => dt,

        chrono::LocalResult::Ambiguous(early, late) => {
            return Err(ApiError::BadRequest(
                (format!("ambiguous local time: {:?} or {:?}", early, late)),
            ));
            // OR pick a policy:
            // early
            // late
        }

        chrono::LocalResult::None => {
            return Err(ApiError::BadRequest(
                "nonexistent local time (DST gap)".into(),
            ));
        }
    };

    let utc = zoned.with_timezone(&chrono_tz::Tz::UTC);

    Ok(utc.timestamp_millis() as u64)
}
