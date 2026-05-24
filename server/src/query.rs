use std::num::NonZero;

use axum::{
    Json,
    extract::{Path, State},
};
use chrono::{DateTime, TimeZone};
use chrono_tz::Tz;
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

pub(crate) async fn validate_query(
    State(state): State<AppState>,
    Json(query): Json<GridQuery>,
) -> Result<Json<ValidatedGridQuery>, ApiError> {
    let series_meta = state.meta_store.get(query.series_id).await?;
    match query.validate(&series_meta) {
        Ok(validated_query) => Ok(Json(validated_query)),
        Err(errs) => {
            let msg = errs
                .into_iter()
                .map(|e| format!("{e:?}"))
                .collect::<Vec<_>>()
                .join("\n");

            Err(ApiError::BadRequest(msg))
        }
    }
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

#[derive(Debug, Deserialize)]
pub enum FixedUnit {
    Millisecond,
    Second,
    Minute,
    Hour,
}

#[derive(Debug, Deserialize)]
pub enum CalendarUnit {
    Day,
    Month,
    Year,
}

#[derive(Debug, Deserialize)]
pub enum GridDuration {
    Fixed(NonZero<u32>, FixedUnit),
    Calendar(NonZero<u32>, CalendarUnit),
}

#[derive(Debug, Deserialize)]
pub enum TimezoneStyle {
    TimezoneId(String),
    SeriesLocal,
    UTC,
}

#[derive(Debug, Deserialize)]
pub struct TimestampTxt(pub String); // newtype for validation

#[derive(Debug, Deserialize)]
pub enum SlotLayout {
    FixedCount(NonZero<u32>), // equally spaced up to ms -> gridduration / count
    Calendar(NonZero<u32>, CalendarUnit), // grid duration / slot duration (where / has to "somewhat" match)
    FixedDuration(NonZero<u32>, FixedUnit), // Fixed XX ms intervals as much as we can fit
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum Accumulation {
    Sum,
    Min,
    Max,
    Count,
    Avg,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum QualityFilter {
    GoodOnly,
    GoodOrUncertain,
    NoFilter, // We add more logic here later, like min good > 70% ...
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SlotAccumulator {
    pub function: Accumulation,
    pub qual_filter: QualityFilter,
}

#[derive(Debug, Deserialize)]
pub struct GridAnchor {
    pub timezone_style: TimezoneStyle,
    pub timestamp: TimestampTxt,
}

#[derive(Debug, Deserialize)]
pub enum AmbiguousTimeResolution {
    Reject,
    Earlier,
    Later,
    KeepBoth,
}

#[derive(Debug, Deserialize)]
pub enum NonexistentTimeResolution {
    Reject,
    ShiftForward,
    ShiftBackward,
}

#[derive(Debug, Deserialize)]
pub struct DstResolution {
    pub ambiguous: AmbiguousTimeResolution,
    pub nonexistent: NonexistentTimeResolution,
}

#[derive(Debug, Deserialize)]
pub struct GridQuery {
    pub series_id: SeriesId,         // pub struct SeriesId(pub NonZero<u64>)
    pub ref_time: GridAnchor,        // aka: seriesLocal + "2026-05-11 11:30" basically grid start
    pub grid_duration: GridDuration, // end = ref_time + grid_duration
    pub slot_layout: SlotLayout,     // how to divide [grid_start;grid_end]
    pub slot_accumulation: SlotAccumulator, // currently only one for all, we add more strategies later
    pub dst_resolution: DstResolution,      // caller needs to think about this
}

#[derive(Debug, Serialize)]
pub struct ResolvedSlot {
    pub start: DateTime<Tz>,
    pub end: DateTime<Tz>,
}

#[derive(Debug, Serialize)]
pub struct ValidatedGridQuery {
    pub start: DateTime<Tz>,
    pub end: DateTime<Tz>,
    pub slots: Vec<ResolvedSlot>,
    pub accumulation: SlotAccumulator,
}

#[derive(Debug, Deserialize)]
pub enum ValidationError {
    InvalidTimestamp(String),
    InvalidTimezone(String),
    UnsupportedUnitCombination,
    GridTooLarge,
    SlotDoesNotDivideGrid,
    AmbiguousLocalTime(String),
    NonexistentLocalTime(String),
    Other(String),
    CalendarEndCouldNotBeResolved,
}

impl GridQuery {
    const MAX_SLOTS: u64 = 1_000_000; // TODO: config

    pub fn validate(
        &self,
        series: &SeriesMeta,
    ) -> Result<ValidatedGridQuery, Vec<ValidationError>> {
        let mut errs: Vec<ValidationError> = vec![];

        let start = match self.get_start(series, &mut errs) {
            Some(start) => start,
            None => return Err(errs),
        };

        let end = match self.get_end(&start) {
            Some(end) => end,
            None => {
                errs.push(ValidationError::CalendarEndCouldNotBeResolved);
                return Err(errs);
            }
        };

        if let Some(slot_count) = self.estimate_slot_count(&start, &end) {
            if slot_count > Self::MAX_SLOTS {
                errs.push(ValidationError::GridTooLarge);
                return Err(errs);
            }
        }

        let slots = match self.slottify(&start, &end, &mut errs) {
            Some(slots) => slots,
            None => return Err(errs),
        };

        Ok(ValidatedGridQuery {
            start,
            end,
            slots,
            accumulation: self.slot_accumulation.clone(),
        })
    }

    fn estimate_slot_count(&self, start: &DateTime<Tz>, end: &DateTime<Tz>) -> Option<u64> {
        let duration = *end - *start;

        match &self.slot_layout {
            SlotLayout::FixedCount(n) => Some(n.get() as u64),

            SlotLayout::FixedDuration(n, unit) => {
                let slot_duration = match unit {
                    FixedUnit::Millisecond => chrono::TimeDelta::milliseconds(n.get() as i64),
                    FixedUnit::Second => chrono::TimeDelta::seconds(n.get() as i64),
                    FixedUnit::Minute => chrono::TimeDelta::minutes(n.get() as i64),
                    FixedUnit::Hour => chrono::TimeDelta::hours(n.get() as i64),
                };

                let total_ms = duration.num_milliseconds();
                let slot_ms = slot_duration.num_milliseconds();

                if total_ms <= 0 || slot_ms <= 0 {
                    return None;
                }

                Some(Self::div_ceil_u64(total_ms as u64, slot_ms as u64))
            }

            SlotLayout::Calendar(n, unit) => {
                let slot_duration_approx = match unit {
                    CalendarUnit::Day => chrono::TimeDelta::days(n.get() as i64),
                    CalendarUnit::Month => chrono::TimeDelta::days(28 * n.get() as i64),
                    CalendarUnit::Year => chrono::TimeDelta::days(365 * n.get() as i64),
                };

                let total_ms = duration.num_milliseconds();
                let slot_ms = slot_duration_approx.num_milliseconds();

                if total_ms <= 0 || slot_ms <= 0 {
                    return None;
                }

                Some(Self::div_ceil_u64(total_ms as u64, slot_ms as u64))
            }
        }
    }

    fn div_ceil_u64(a: u64, b: u64) -> u64 {
        if a == 0 { 0 } else { 1 + ((a - 1) / b) }
    }

    fn slottify(
        &self,
        start: &DateTime<Tz>,
        end: &DateTime<Tz>,
        errs: &mut Vec<ValidationError>,
    ) -> Option<Vec<ResolvedSlot>> {
        let naive_duration = *end - start;
        if naive_duration.is_zero() {
            // should not happen, bc all params are NonZero<_>
            errs.push(ValidationError::Other("grid duration is zero".to_string()));
            return None;
        }
        let mut slots = vec![];
        match &self.slot_layout {
            // fixed count is grid_duration / N -> resulting in arbitrary slot length
            SlotLayout::FixedCount(non_zero) => {
                // checked_div only returns None if we divide by 0, so unwrap is okay
                let slot_duration = naive_duration.checked_div(non_zero.get() as i32).unwrap();
                let mut slot_left = start.clone();
                let mut slot_right = start.clone();
                let mut i = 0;
                while slot_right < *end {
                    slot_right = match slot_left.checked_add_signed(slot_duration) {
                        Some(slot_right) => slot_right,
                        None => {
                            errs.push(ValidationError::Other(format!(
                            "cannot compute slot end for slot {i}; slot_left={slot_left:?}; slot_duration={slot_duration:}"
                            ))); // TODO: bail for now, lets capture the error cases, maybe we can resolve some of them later
                            return None;
                        }
                    };
                    slots.push(ResolvedSlot {
                        start: slot_left.clone(),
                        end: slot_right.clone(),
                    });

                    slot_left = slot_right;

                    i += 1;
                }
            }
            SlotLayout::Calendar(non_zero, calendar_unit) => {
                // checked_div only returns None if we divide by 0, so unwrap is okay
                let mut slot_left = start.clone();
                let mut slot_right = start.clone();
                let mut i = 0;
                while slot_right < *end {
                    let maybe_right = match calendar_unit {
                        CalendarUnit::Day => {
                            slot_left.checked_add_days(chrono::Days::new(non_zero.get() as u64))
                        }
                        CalendarUnit::Month => {
                            slot_left.checked_add_months(chrono::Months::new(non_zero.get()))
                        }
                        CalendarUnit::Year => {
                            slot_left.checked_add_months(chrono::Months::new(12 * non_zero.get()))
                        }
                    };

                    slot_right = match maybe_right {
                        Some(slot_right) => slot_right,
                        None => {
                            errs.push(ValidationError::Other(format!(
                            "cannot compute slot end for slot {i}; slot_left={slot_left:?}; slot boundary lands on ambiguous or nonexistent timestamp"
                            ))); // TODO: bail for now, lets capture the error cases, maybe we can resolve some of them later
                            return None;
                        }
                    };

                    slots.push(ResolvedSlot {
                        start: slot_left.clone(),
                        end: slot_right.clone(),
                    });

                    slot_left = slot_right;

                    i += 1;
                }
            }
            SlotLayout::FixedDuration(non_zero, fixed_unit) => {
                let slot_duration = match fixed_unit {
                    FixedUnit::Millisecond => {
                        chrono::TimeDelta::milliseconds(non_zero.get() as i64)
                    }
                    FixedUnit::Second => chrono::TimeDelta::seconds(non_zero.get() as i64),
                    FixedUnit::Minute => chrono::TimeDelta::minutes(non_zero.get() as i64),
                    FixedUnit::Hour => chrono::TimeDelta::hours(non_zero.get() as i64),
                };
                let mut slot_left = start.clone();
                let mut slot_right = start.clone();
                let mut i = 0;
                while slot_right < *end {
                    slot_right = match slot_left.checked_add_signed(slot_duration) {
                        Some(slot_right) => slot_right,
                        None => {
                            errs.push(ValidationError::Other(format!(
                            "cannot compute slot end for slot {i}; slot_left={slot_left:?}; slot_duration={slot_duration:}"
                            ))); // TODO: bail for now, lets capture the error cases, maybe we can resolve some of them later
                            return None;
                        }
                    };
                    slots.push(ResolvedSlot {
                        start: slot_left.clone(),
                        end: slot_right.clone(),
                    });

                    slot_left = slot_right;

                    i += 1;
                }
            }
        }
        Some(slots)
    }

    fn get_end(&self, start: &DateTime<Tz>) -> Option<DateTime<Tz>> {
        let start_copy = start.clone();
        match &self.grid_duration {
            GridDuration::Fixed(non_zero, fixed_unit) => {
                let time_delta = match fixed_unit {
                    FixedUnit::Millisecond => {
                        chrono::TimeDelta::milliseconds(non_zero.get() as i64)
                    }
                    FixedUnit::Second => chrono::TimeDelta::seconds(non_zero.get() as i64),
                    FixedUnit::Minute => chrono::TimeDelta::minutes(non_zero.get() as i64),
                    FixedUnit::Hour => chrono::TimeDelta::hours(non_zero.get() as i64),
                };
                start_copy.checked_add_signed(time_delta)
            }
            // TODO: chrono returns None for ambiguous or non existant
            //       we wanna handle that ourselves later according to the users
            //       resolver preference.
            //       maybe chrono would like some try_add|sub_XXX functions?
            GridDuration::Calendar(non_zero, calendar_unit) => match calendar_unit {
                CalendarUnit::Day => {
                    start_copy.checked_add_days(chrono::Days::new(non_zero.get() as u64))
                }
                CalendarUnit::Month => {
                    start_copy.checked_add_months(chrono::Months::new(non_zero.get() as u32))
                }
                CalendarUnit::Year => {
                    let months = non_zero.get().checked_mul(12)?;
                    start_copy.checked_add_months(chrono::Months::new(months))
                }
            },
        }
    }

    fn get_start(
        &self,
        series: &SeriesMeta,
        errs: &mut Vec<ValidationError>,
    ) -> Option<DateTime<Tz>> {
        // just parse a timestamp .. cant be that hard

        let tz_str: &str = match &self.ref_time.timezone_style {
            TimezoneStyle::TimezoneId(tz) => tz.as_str(),
            TimezoneStyle::SeriesLocal => series.tz.as_str(),
            TimezoneStyle::UTC => "UTC",
        };

        let tz = match chrono_tz::Tz::from_str_insensitive(tz_str) {
            Ok(tz) => tz,
            Err(e) => {
                errs.push(ValidationError::InvalidTimezone(e.to_string()));
                return None;
            }
        };

        let naive = match chrono::NaiveDateTime::parse_from_str(
            &self.ref_time.timestamp.0,
            "%Y-%m-%d %H:%M:%S", // TODO: config/param
        ) {
            Ok(naive) => naive,
            Err(e) => {
                errs.push(ValidationError::InvalidTimestamp(e.to_string()));
                return None;
            }
        };

        match tz.from_local_datetime(&naive) {
            chrono::LocalResult::Single(dt) => Some(dt),
            chrono::LocalResult::Ambiguous(early, late) => match self.dst_resolution.ambiguous {
                AmbiguousTimeResolution::Reject => {
                    errs.push(ValidationError::AmbiguousLocalTime("ref_time".to_string()));
                    None
                }
                AmbiguousTimeResolution::Earlier => Some(early),
                AmbiguousTimeResolution::Later => Some(late),
                AmbiguousTimeResolution::KeepBoth => {
                    errs.push(ValidationError::Other(
                        "ref_time cannot use KeepBoth because the query needs one concrete start instant".to_string(),
                    ));
                    None
                }
            },
            chrono::LocalResult::None => {
                errs.push(ValidationError::NonexistentLocalTime(
                    "ref_time".to_string(),
                ));
                None
            }
        }
    }
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

    let data = match series_meta.storage_type {
        StorageType::Float32 => query_raw::<f32>(&state, left, right, &series_meta).await?,
        StorageType::Float64 => query_raw::<f64>(&state, left, right, &series_meta).await?,
        StorageType::Int32 => query_raw::<i32>(&state, left, right, &series_meta).await?,
        StorageType::Int64 => query_raw::<i64>(&state, left, right, &series_meta).await?,
        StorageType::UInt32 => query_raw::<u32>(&state, left, right, &series_meta).await?,
        StorageType::UInt64 => query_raw::<u64>(&state, left, right, &series_meta).await?,
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
            x if x == last_block_idx => SizedBlock::read_from_block_range::<T>(
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
