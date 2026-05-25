use crate::time::*;
use jiff::civil::DateTime;
use jiff::tz::{self, AmbiguousOffset};

impl VodnikTimezoneId {
    pub fn from_str_insensitive(s: &str) -> Result<Self, TimeError> {
        match tz::db().get(s) {
            Ok(jiff_tz) => match jiff_tz.iana_name() {
                Some(name) => Ok(Self::new(name.to_string())),
                None => Err(TimeError::InvalidTimezone(s.to_string())),
            },
            Err(_) => Err(TimeError::InvalidTimezone(s.to_string())),
        }
    }
}

impl VodnikZonedDateTime {
    pub fn fixed_add(&self, p: VodnikFixedPeriod) -> Option<VodnikZonedDateTime> {
        let duration = jiff::SignedDuration::from_millis(p.ms() as i64);
        let jiff_zoned = self.to_jiff_zoned()?;
        // this errors if outside of bounds > year 9999, no recovery path
        let res = jiff_zoned.checked_add(duration).ok()?;
        // safe because VodnikInstant and VodnikFixedPeriod are both non-negative,
        // and this method only adds positive elapsed time.
        let v_instant = VodnikInstant(res.timestamp().as_millisecond() as u64);
        Some(VodnikZonedDateTime(v_instant, self.1.clone()))
    }

    fn to_jiff_zoned(&self) -> Option<jiff::Zoned> {
        let jiff_tz = tz::db().get(self.1.id()).unwrap();
        let jiff_zoned = jiff::Timestamp::from_millisecond(self.0.0 as i64)
            .ok()?
            .to_zoned(jiff_tz);
        Some(jiff_zoned)
    }

    pub fn calendar_add_with_resolver(
        &self,
        p: VodnikCalendarPeriod,
        dst: &DstResolution,
    ) -> Option<ZoningResult> {
        let span = jiff::Span::new()
            .years(p.years)
            .months(p.months)
            .days(p.days);

        let jiff_zoned = self.to_jiff_zoned()?;

        let jiff_dt = jiff_zoned.datetime().checked_add(span).ok()?;
        let vodnik_local_adjusted = jiff_dt_to_vodniklocaltime(jiff_dt).ok()?;
        vodnik_local_adjusted.in_zone(&self.1, dst).ok()
    }
}

impl VodnikLocalDateTime {
    pub fn parse_from_str(s: &str) -> Result<Self, TimeError> {
        match s.parse::<DateTime>() {
            Ok(dt) => jiff_dt_to_vodniklocaltime(dt),
            Err(_) => Err(TimeError::ParseLocalDateTime(s.to_string())),
        }
    }

    pub fn in_zone(
        &self,
        tz: &VodnikTimezoneId,
        dst: &DstResolution,
    ) -> Result<ZoningResult, TimeError> {
        // unwrap okay, its validated in from_str_insensitive and new is crate only
        let jiff_tz = tz::db().get(tz.id()).unwrap();
        let jiff_datetime = jiff::civil::date(
            self.date.year as i16,
            self.date.month as i8,
            self.date.day as i8,
        )
        .at(
            self.time.hour as i8,
            self.time.minute as i8,
            self.time.second as i8,
            self.time.millisecond as i32 * 1_000_000,
        );

        let ambiguous = jiff_tz.to_ambiguous_zoned(jiff_datetime);
        match ambiguous.offset() {
            AmbiguousOffset::Unambiguous { offset } => match offset.to_timestamp(jiff_datetime) {
                Ok(ts) => {
                    let ts = ts.as_millisecond();
                    if ts < 0 {
                        return Err(TimeError::Unsupported(
                            "Timestamps earlier than 1970-01-01 are not supported".to_string(),
                        ));
                    }
                    Ok(ZoningResult::Exact(VodnikZonedDateTime(
                        VodnikInstant(ts as u64),
                        tz.clone(),
                    )))
                }
                Err(_) => Ok(ZoningResult::Nonexistent),
            },
            AmbiguousOffset::Gap { .. } => Ok(ZoningResult::Nonexistent),
            AmbiguousOffset::Fold { before, after } => match (
                before.to_timestamp(jiff_datetime),
                after.to_timestamp(jiff_datetime),
            ) {
                (Ok(bf_ts), Ok(af_ts)) => {
                    let bf_ts = bf_ts.as_millisecond();
                    let af_ts = af_ts.as_millisecond();

                    let (early_ts, late_ts) = if bf_ts <= af_ts {
                        (bf_ts, af_ts)
                    } else {
                        (af_ts, bf_ts)
                    };

                    if bf_ts < 0 || af_ts < 0 {
                        return Err(TimeError::Unsupported(
                            "Timestamps earlier than 1970-01-01 are not supported".to_string(),
                        ));
                    }
                    Ok(ZoningResult::Ambiguous {
                        early: VodnikZonedDateTime(VodnikInstant(early_ts as u64), tz.clone()),
                        late: VodnikZonedDateTime(VodnikInstant(late_ts as u64), tz.clone()),
                    })
                }
                _ => Ok(ZoningResult::Nonexistent),
            },
        }
    }
}

fn jiff_dt_to_vodniklocaltime(dt: DateTime) -> Result<VodnikLocalDateTime, TimeError> {
    if dt.year() < 1970 {
        return Err(TimeError::Unsupported(
            "Timestamps earlier than 1970-01-01 are not supported".to_string(),
        ));
    }

    Ok(VodnikLocalDateTime {
        date: VodnikLocalDate {
            year: i16::max(0i16, dt.year()) as u16,
            month: dt.month() as u8,
            day: dt.day() as u8,
        },
        time: VodnikLocalTime {
            hour: dt.hour() as u8,
            minute: dt.minute() as u8,
            second: dt.second() as u8,
            millisecond: dt.millisecond() as u16,
        },
    })
}
