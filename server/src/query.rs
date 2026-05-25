use std::num::NonZero;

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
    time::{
        AmbiguousTimeResolution, DstResolution, VodnikCalendarPeriod, VodnikFixedPeriod,
        VodnikLocalDateTime, VodnikTimezoneId, VodnikZonedDateTime, ZoningResult,
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
    Calendar(NonZero<u16>, CalendarUnit),
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
    Calendar(NonZero<u16>, CalendarUnit), // grid duration / slot duration (where / has to "somewhat" match)
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
    pub start: VodnikZonedDateTime,
    pub end: VodnikZonedDateTime,
}

#[derive(Debug, Serialize)]
pub struct ValidatedGridQuery {
    pub start: VodnikZonedDateTime,
    pub end: VodnikZonedDateTime,
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

        let end = match self.get_end(&start, &mut errs) {
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

    fn estimate_slot_count(
        &self,
        start: &VodnikZonedDateTime,
        end: &VodnikZonedDateTime,
    ) -> Option<u64> {
        let duration = end.checked_duration_since(start)?;

        match &self.slot_layout {
            SlotLayout::FixedCount(n) => Some(n.get() as u64),

            SlotLayout::FixedDuration(n, unit) => {
                let slot_duration = match unit {
                    FixedUnit::Millisecond => VodnikFixedPeriod::from_ms(n.get() as u64),
                    FixedUnit::Second => VodnikFixedPeriod::from_secs(n.get()),
                    FixedUnit::Minute => VodnikFixedPeriod::from_mins(n.get()),
                    FixedUnit::Hour => VodnikFixedPeriod::from_hours(n.get()),
                };

                let total_ms = duration.ms();
                let slot_ms = slot_duration.ms();

                if total_ms <= 0 || slot_ms <= 0 {
                    return None;
                }

                Some(Self::div_ceil_u64(total_ms as u64, slot_ms as u64))
            }

            SlotLayout::Calendar(n, unit) => {
                let slot_ms_approx = match unit {
                    CalendarUnit::Day => VodnikFixedPeriod::from_hours(24).ms(),
                    CalendarUnit::Month => {
                        VodnikFixedPeriod::from_hours(24).ms() * (28 * n.get() as u64)
                    }
                    CalendarUnit::Year => {
                        VodnikFixedPeriod::from_hours(24).ms() * (365 * n.get() as u64)
                    }
                };

                let total_ms = duration.ms();

                if total_ms <= 0 || slot_ms_approx <= 0 {
                    return None;
                }

                Some(Self::div_ceil_u64(total_ms as u64, slot_ms_approx as u64))
            }
        }
    }

    fn div_ceil_u64(a: u64, b: u64) -> u64 {
        if a == 0 { 0 } else { 1 + ((a - 1) / b) }
    }

    fn slottify(
        &self,
        start: &VodnikZonedDateTime,
        end: &VodnikZonedDateTime,
        errs: &mut Vec<ValidationError>,
    ) -> Option<Vec<ResolvedSlot>> {
        let duration = end.checked_duration_since(start)?;
        if duration.is_zero() {
            // should not happen, bc all params are NonZero<_>
            errs.push(ValidationError::Other("grid duration is zero".to_string()));
            return None;
        }
        let mut slots = vec![];
        match &self.slot_layout {
            // fixed count is grid_duration / N -> resulting in arbitrary slot length
            SlotLayout::FixedCount(non_zero) => {
                // checked_div only returns None if we divide by 0, so unwrap is okay
                let slot_duration_ms = duration.ms().checked_div(non_zero.get() as u64).unwrap();
                let slot_duration = VodnikFixedPeriod::from_ms(slot_duration_ms);
                let mut slot_left = start.clone();
                let mut slot_right = start.clone();
                let mut i = 0;
                while &slot_right < end {
                    slot_right = match slot_left.fixed_add(slot_duration) {
                        Some(slot_right) => slot_right,
                        None => {
                            errs.push(ValidationError::Other(format!(
                            "cannot compute slot end for slot {i}; slot_left={slot_left:?}; slot_duration={slot_duration:?}"
                            ))); // TODO: bail for now, lets capture the error cases, maybe we can resolve some of them later
                            return None;
                        }
                    };
                    slots.push(ResolvedSlot {
                        start: slot_left.clone(),
                        end: slot_right.clone(),
                    });

                    slot_left = slot_right.clone();

                    i += 1;
                }
            }
            SlotLayout::Calendar(non_zero, calendar_unit) => {
                // checked_div only returns None if we divide by 0, so unwrap is okay
                let mut slot_left = start.clone();
                let mut slot_right = start.clone();
                let mut i = 0;

                let slot_duration = match calendar_unit {
                    CalendarUnit::Day => VodnikCalendarPeriod {
                        years: 0,
                        months: 0,
                        days: non_zero.get(),
                    },
                    CalendarUnit::Month => match u8::try_from(non_zero.get()) {
                        Ok(m) => VodnikCalendarPeriod {
                            years: 0,
                            months: m,
                            days: 0,
                        },
                        Err(_) => {
                            errs.push(ValidationError::Other("cannot compute slot duration. Month calendar unit must be between 1 and 255".to_string()));
                            return None;
                        }
                    },
                    CalendarUnit::Year => match u8::try_from(non_zero.get()) {
                        Ok(y) => VodnikCalendarPeriod {
                            years: y,
                            months: 0,
                            days: 0,
                        },
                        Err(_) => {
                            errs.push(ValidationError::Other("cannot compute slot duration. Year calendar unit must be between 1 and 255".to_string()));
                            return None;
                        }
                    },
                };

                while &slot_right < end {
                    slot_right = match slot_left
                        .calendar_add_with_resolver(slot_duration, &self.dst_resolution)
                    {
                        Some(slot_right) => match self.apply_dst_resolution(slot_right, errs) {
                            Some(slot_right) => slot_right,
                            None => return None,
                        },
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

                    slot_left = slot_right.clone();

                    i += 1;
                }
            }
            SlotLayout::FixedDuration(non_zero, fixed_unit) => {
                let slot_duration = match fixed_unit {
                    FixedUnit::Millisecond => VodnikFixedPeriod::from_ms(non_zero.get() as u64),
                    FixedUnit::Second => VodnikFixedPeriod::from_secs(non_zero.get()),
                    FixedUnit::Minute => VodnikFixedPeriod::from_mins(non_zero.get()),
                    FixedUnit::Hour => VodnikFixedPeriod::from_hours(non_zero.get()),
                };
                let mut slot_left = start.clone();
                let mut slot_right = start.clone();
                let mut i = 0;
                while &slot_right < end {
                    slot_right = match slot_left.fixed_add(slot_duration) {
                        Some(slot_right) => slot_right,
                        None => {
                            errs.push(ValidationError::Other(format!(
                            "cannot compute slot end for slot {i}; slot_left={slot_left:?}; slot_duration={slot_duration:?}"
                            ))); // TODO: bail for now, lets capture the error cases, maybe we can resolve some of them later
                            return None;
                        }
                    };
                    slots.push(ResolvedSlot {
                        start: slot_left.clone(),
                        end: slot_right.clone(),
                    });

                    slot_left = slot_right.clone();

                    i += 1;
                }
            }
        }
        Some(slots)
    }

    fn get_end(
        &self,
        start: &VodnikZonedDateTime,
        errs: &mut Vec<ValidationError>,
    ) -> Option<VodnikZonedDateTime> {
        match &self.grid_duration {
            GridDuration::Fixed(non_zero, fixed_unit) => {
                let fixed_period = match fixed_unit {
                    FixedUnit::Millisecond => VodnikFixedPeriod::from_ms(non_zero.get() as u64),
                    FixedUnit::Second => VodnikFixedPeriod::from_secs(non_zero.get()),
                    FixedUnit::Minute => VodnikFixedPeriod::from_mins(non_zero.get()),
                    FixedUnit::Hour => VodnikFixedPeriod::from_hours(non_zero.get()),
                };
                start.fixed_add(fixed_period)
            }
            GridDuration::Calendar(non_zero, calendar_unit) => {
                let zoning_result = match calendar_unit {
                    CalendarUnit::Day => start.calendar_add_with_resolver(
                        VodnikCalendarPeriod {
                            years: 0,
                            months: 0,
                            days: non_zero.get(),
                        },
                        &self.dst_resolution,
                    )?,
                    CalendarUnit::Month => match u8::try_from(non_zero.get()) {
                        Ok(m) => start.calendar_add_with_resolver(
                            VodnikCalendarPeriod {
                                years: 0,
                                months: m,
                                days: 0,
                            },
                            &self.dst_resolution,
                        )?,
                        Err(e) => {
                            errs.push(ValidationError::Other("cannot compute end date. Month calendar unit must be between 1 and 255".to_string()));
                            return None;
                        }
                    },
                    CalendarUnit::Year => match u8::try_from(non_zero.get()) {
                        Ok(y) => start.calendar_add_with_resolver(
                            VodnikCalendarPeriod {
                                years: y,
                                months: 0,
                                days: 0,
                            },
                            &self.dst_resolution,
                        )?,
                        Err(e) => {
                            errs.push(ValidationError::Other("cannot compute end date. Year calendar unit must be between 1 and 255".to_string()));
                            return None;
                        }
                    },
                };

                self.apply_dst_resolution(zoning_result, errs)
            }
        }
    }

    fn apply_dst_resolution(
        &self,
        zoning_result: ZoningResult,
        errs: &mut Vec<ValidationError>,
    ) -> Option<VodnikZonedDateTime> {
        match zoning_result {
            ZoningResult::Exact(vodnik_zoned) => Some(vodnik_zoned),
            ZoningResult::Ambiguous { early, late } => match self.dst_resolution.ambiguous {
                AmbiguousTimeResolution::Reject => None,
                AmbiguousTimeResolution::Earlier => Some(early),
                AmbiguousTimeResolution::Later => Some(late),
                AmbiguousTimeResolution::KeepBoth => {
                    errs.push(ValidationError::Other(
                            "ref_time cannot use KeepBoth because the query needs one concrete end instant"
                                .to_string(),
                        ));
                    return None;
                }
            },
            ZoningResult::Nonexistent => None,
        }
    }

    fn get_start(
        &self,
        series: &SeriesMeta,
        errs: &mut Vec<ValidationError>,
    ) -> Option<VodnikZonedDateTime> {
        // just parse a timestamp .. cant be that hard

        let tz_str: &str = match &self.ref_time.timezone_style {
            TimezoneStyle::TimezoneId(tz) => tz.as_str(),
            TimezoneStyle::SeriesLocal => series.tz.as_str(),
            TimezoneStyle::UTC => "UTC",
        };

        let tz = match VodnikTimezoneId::from_str_insensitive(tz_str) {
            Ok(tz) => tz,
            Err(e) => {
                errs.push(ValidationError::InvalidTimezone(e.to_string()));
                return None;
            }
        };

        let local = match VodnikLocalDateTime::parse_from_str(&self.ref_time.timestamp.0) {
            Ok(local) => local,
            Err(e) => {
                errs.push(ValidationError::InvalidTimestamp(e.to_string()));
                return None;
            }
        };

        match local.in_zone(&tz, &self.dst_resolution).ok()? {
            ZoningResult::Exact(dt) => Some(dt),
            ZoningResult::Ambiguous { early, late } => match self.dst_resolution.ambiguous {
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
            ZoningResult::Nonexistent => {
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
