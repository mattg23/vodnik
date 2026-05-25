use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TimeError {
    #[error("Unknown timezone: {0}")]
    InvalidTimezone(String),

    #[error("local datetime text '{0}' - wrong format use: 'yyyy-MM-dd HH:mm:ss'")]
    ParseLocalDateTime(String),

    #[error("Unsupported: {0}")]
    Unsupported(String),
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct VodnikInstant(pub(crate) u64); // epoch ms

impl VodnikInstant {
    pub fn epoch_ms(&self) -> u64 {
        self.0
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct VodnikTimezoneId(String); // IANA ID

impl VodnikTimezoneId {
    pub(crate) fn new(s: String) -> Self {
        Self(s)
    }

    pub fn id(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct VodnikZonedDateTime(pub(crate) VodnikInstant, pub(crate) VodnikTimezoneId);

impl VodnikZonedDateTime {
    pub fn instant(&self) -> VodnikInstant {
        self.0
    }
    pub fn tz_id(&self) -> &VodnikTimezoneId {
        &self.1
    }

    pub fn abs_delta(&self, other: &Self) -> VodnikFixedPeriod {
        VodnikFixedPeriod(self.0.epoch_ms().abs_diff(other.0.epoch_ms()))
    }

    pub fn checked_duration_since(&self, earlier: &Self) -> Option<VodnikFixedPeriod> {
        if earlier.0.epoch_ms() > self.0.epoch_ms() {
            None
        } else {
            Some(self.abs_delta(earlier))
        }
    }
}

impl PartialOrd for VodnikZonedDateTime {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.0.cmp(&other.0))
    }
}

impl Ord for VodnikZonedDateTime {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VodnikLocalDateTime {
    pub date: VodnikLocalDate,
    pub time: VodnikLocalTime,
}

pub enum ZoningResult {
    Exact(VodnikZonedDateTime),
    Ambiguous {
        early: VodnikZonedDateTime,
        late: VodnikZonedDateTime,
    },
    Nonexistent,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct VodnikLocalDate {
    pub year: u16,
    pub month: u8,
    pub day: u8,
}
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct VodnikLocalTime {
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub millisecond: u16,
}
#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub struct VodnikFixedPeriod(u64); // ms

impl VodnikFixedPeriod {
    pub fn from_ms(ms: u64) -> Self {
        Self(ms)
    }
    pub fn from_secs(s: u32) -> Self {
        Self(1000 * s as u64)
    }
    pub fn from_mins(m: u32) -> Self {
        Self(1000 * 60 * m as u64)
    }
    pub fn from_hours(h: u32) -> Self {
        Self(1000 * 60 * 60 * h as u64)
    }

    pub fn ms(&self) -> u64 {
        self.0
    }

    pub fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub struct VodnikCalendarPeriod {
    pub years: u8,
    pub months: u8,
    pub days: u16,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum AmbiguousTimeResolution {
    Reject,
    Earlier,
    Later,
    KeepBoth,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum NonexistentTimeResolution {
    Reject,
    ShiftForward,
    ShiftBackward,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct DstResolution {
    pub ambiguous: AmbiguousTimeResolution,
    pub nonexistent: NonexistentTimeResolution,
}
