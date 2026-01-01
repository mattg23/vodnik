pub mod store;

use num_traits::{Bounded, Num, NumAssign, NumCast};
use serde::{Deserialize, Serialize};
use std::{fmt, num::NonZero};
use thiserror::Error;

use crate::api::ApiError;

// for f64/f32 NaN is not allowed. this should be checked at the boundary
// at ingestion time. StorableNum assumes a non-NaN value for floating point types
pub trait StorableNum: Num + NumCast + NumAssign + Bounded + PartialOrd + Copy {}

impl StorableNum for f64 {}
impl StorableNum for f32 {}
impl StorableNum for i64 {}
impl StorableNum for i32 {}
impl StorableNum for u32 {}
impl StorableNum for u64 {}
impl StorableNum for u8 {}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct BlockMeta<T: StorableNum> {
    pub count_non_missing: u32,
    pub count_valid: u32, // aka good | uncertain

    // stats for valid values
    pub sum: T,
    pub min: T,
    pub max: T,

    // fst/lst valid (good | uncertain)
    pub fst_valid: T,
    pub fst_valid_q: Quality,

    pub lst_valid: T,
    pub lst_valid_q: Quality,

    pub fst_valid_offset: u32,
    pub lst_valid_offset: u32,

    // fst/lst any qual
    pub fst: T,
    pub fst_q: Quality,

    pub lst: T,
    pub lst_q: Quality,

    pub fst_offset: u32,
    pub lst_offset: u32,

    pub qual_acc_or: u32,
    pub qual_acc_and: u32,
}

impl<T: StorableNum> BlockMeta<T> {
    pub fn new() -> Self {
        Self {
            count_non_missing: 0,
            count_valid: 0,
            fst_offset: u32::MAX,
            lst_offset: u32::MIN,
            sum: T::zero(),
            min: T::max_value(),
            max: T::min_value(),
            fst: T::zero(),
            lst: T::zero(),
            qual_acc_or: 0,
            qual_acc_and: 0xFFFFFFFF,
            fst_valid: T::zero(),
            fst_valid_q: Quality::MISSING,
            lst_valid: T::zero(),
            lst_valid_q: Quality::MISSING,
            fst_valid_offset: u32::MAX,
            lst_valid_offset: u32::MIN,
            fst_q: Quality::MISSING,
            lst_q: Quality::MISSING,
        }
    }

    // quality acc FLAGS
    pub const ACC_GOOD: u32 = 1;
    pub const ACC_BAD: u32 = 1 << 1;
    pub const ACC_UNCERTAIN: u32 = 1 << 2;
    pub const ACC_NODATA: u32 = 1 << 3;

    // does a full scan
    pub fn recalc_block_data_full(
        &mut self,
        updated_block_data: &[T],
        updated_quality_data: &[Quality],
    ) {
        // TODO: for simplicity we do a full scan here. once we have the ingest/storage path built out more,
        // we add running statistics. but lets focus on making progress for now
        // also our blocks are kinda small, so it will take some time, until we notice this perf wise (famous last words)

        assert_eq!(updated_block_data.len(), updated_quality_data.len());
        assert_ne!(updated_block_data.len(), 0);

        // reset state
        self.count_non_missing = 0;
        self.count_valid = 0;

        self.sum = T::zero();
        self.min = T::max_value();
        self.max = T::min_value();

        self.fst_offset = u32::MAX;
        self.lst_offset = u32::MIN;
        self.fst = T::zero();
        self.fst_q = Quality::MISSING;

        self.fst_valid_offset = u32::MAX;
        self.lst_valid_offset = u32::MIN;
        self.fst_valid = T::zero();
        self.fst_valid_q = Quality::MISSING;

        // Reset masks
        self.qual_acc_or = 0;
        // Start AND mask with all 1s so the intersection works.
        // If block is empty, this remains MAX (or you can handle empty case specifically).
        self.qual_acc_and = u32::MAX;

        for i in 0..updated_block_data.len() {
            let v = updated_block_data[i];
            let q = updated_quality_data[i];
            let idx = i as u32;

            let current_qual_flag = if q.is_missing() {
                Self::ACC_NODATA
            } else if q.is_bad() {
                Self::ACC_BAD
            } else if q.is_uncertain() {
                Self::ACC_UNCERTAIN
            } else {
                Self::ACC_GOOD
            };

            self.qual_acc_or |= current_qual_flag;
            self.qual_acc_and &= current_qual_flag;

            if q.is_missing() {
                continue;
            }

            // if we are here, we have non missing data
            self.count_non_missing += 1;

            // fst/lst any qual
            if idx < self.fst_offset {
                self.fst_offset = idx;
                self.fst = v;
                self.fst_q = q;
            }

            if idx >= self.lst_offset {
                self.lst_offset = idx;
                self.lst = v;
                self.lst_q = q;
            }

            // calculate stats for valid (good | uncertain) data
            if q.is_good() || q.is_uncertain() {
                self.count_valid += 1;
                self.sum += v;

                if v < self.min {
                    self.min = v;
                }
                if v > self.max {
                    self.max = v;
                }

                // fst / lst valid
                if idx < self.fst_valid_offset {
                    self.fst_valid_offset = idx;
                    self.fst_valid = v;
                    self.fst_valid_q = q;
                }
                if idx >= self.lst_valid_offset {
                    self.lst_valid_offset = idx;
                    self.lst_valid = v;
                    self.lst_valid_q = q;
                }
            }
        }

        // no data -> no min/max
        if self.count_valid == 0 {
            self.min = T::zero();
            self.max = T::zero();
        }
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Quality(u8);

impl Quality {
    // LAYOUT (OPC)
    // QQ_SSSS_LL
    // QQ -> Major Quality
    // SSSS -> SubStatus
    // LL -> Limit

    const GOOD: Self = Self(0b11_0000_00); // 192 (0xC0)
    const BAD: Self = Self(0b00_0000_00); // 1 (0x00)
    const UNCERTAIN: Self = Self(0b01_0000_00); // 64 (0x40)

    pub const MISSING: Self = Self(0b10_0000_00); // opc doesnt use 10_SSSS_LL

    // masks
    pub const MASK_MAJOR: u8 = 0b11_0000_00;
    pub const MASK_SUB: u8 = 0b00_1110_00;
    pub const MASK_LIMIT: u8 = 0b00_0001_11;

    pub fn is_good(self) -> bool {
        (self.0 & Self::MASK_MAJOR) == Self::GOOD.0
    }

    pub fn is_bad(self) -> bool {
        (self.0 & Self::MASK_MAJOR) == Self::BAD.0
    }

    pub fn is_uncertain(self) -> bool {
        (self.0 & Self::MASK_MAJOR) == Self::UNCERTAIN.0
    }

    pub fn is_missing(self) -> bool {
        self.0 == Self::MISSING.0
    }
}

impl Default for Quality {
    fn default() -> Self {
        Self::GOOD
    }
}

impl fmt::Debug for Quality {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_good() {
            write!(f, "Quality(Good)")
        } else if self.is_bad() {
            write!(f, "Quality(Bad bits={:08b})", self.0)
        } else if self.is_missing() {
            write!(f, "MISSING")
        } else {
            write!(f, "Quality(Uncertain)")
        }
    }
}

impl TryFrom<u8> for Quality {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        // TODO: Add validation
        Ok(Self(value))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SizedBlock {
    F32Block(BlockMeta<f32>, Vec<f32>, Vec<Quality>),
    F64Block(BlockMeta<f64>, Vec<f64>, Vec<Quality>),
    I32Block(BlockMeta<i32>, Vec<i32>, Vec<Quality>),
    I64Block(BlockMeta<i64>, Vec<i64>, Vec<Quality>),
    U32Block(BlockMeta<u32>, Vec<u32>, Vec<Quality>),
    U64Block(BlockMeta<u64>, Vec<u64>, Vec<Quality>),
    U8Block(BlockMeta<u8>, Vec<u8>, Vec<Quality>),
}

#[repr(u64)]
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum TimeResolution {
    Millisecond = 1,
    Second = 1000,
    Minute = 1000 * 60,
    Hour = 1000 * 60 * 60,
}

impl From<TimeResolution> for u64 {
    fn from(value: TimeResolution) -> Self {
        value as u64
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum StorageType {
    Float32,
    Float64,
    Int32,
    Int64,
    UInt32,
    UInt64,
    Enumeration,
}

impl StorageType {
    pub const fn sample_bytes(self) -> u64 {
        match self {
            StorageType::Float32 => 4,
            StorageType::Float64 => 8,
            StorageType::Int32 => 4,
            StorageType::Int64 => 8,
            StorageType::UInt32 => 4,
            StorageType::UInt64 => 8,
            StorageType::Enumeration => 1,
        }
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub struct BlockNumber(pub u64);
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct BlockLength(pub NonZero<u64>);
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct SampleLength(pub NonZero<u64>);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Label {
    pub name: String,
    pub value: String,
}
#[derive(Copy, Clone, Debug, Deserialize, Serialize, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub struct SeriesId(pub NonZero<u64>);

impl std::fmt::Display for SeriesId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeriesMeta {
    pub id: SeriesId,
    pub name: String,
    pub storage_type: StorageType,
    pub block_length: BlockLength,
    pub block_resolution: TimeResolution,
    pub sample_length: SampleLength,
    pub sample_resolution: TimeResolution,
    pub first_block: BlockNumber,
    pub last_block: BlockNumber,
    pub labels: Vec<Label>,
}

#[derive(Error, Debug)]
pub enum MetaStoreError {
    #[error("series {0} already exists.")]
    Duplicate(SeriesId),
    #[error("series {0} not found")]
    NotFound(SeriesId),
    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}

pub(crate) fn into_api_error(e: MetaStoreError) -> ApiError {
    e.into()
}

#[derive(Debug)]
pub struct NonEmptySlice<'a, T>(&'a [T]);

impl<'a, T> NonEmptySlice<'a, T> {
    pub fn as_slice(&self) -> &'a [T] {
        self.0
    }
}

impl<'a, T> TryFrom<&'a [T]> for NonEmptySlice<'a, T> {
    type Error = ();

    fn try_from(slice: &'a [T]) -> Result<Self, Self::Error> {
        if slice.is_empty() {
            Err(())
        } else {
            Ok(NonEmptySlice(slice))
        }
    }
}

pub trait MetaStore {
    async fn create(&self, series: &SeriesMeta) -> Result<SeriesId, MetaStoreError>;
    async fn update(&self, series: &SeriesMeta) -> Result<(), MetaStoreError>;
    async fn delete(&self, id: SeriesId) -> Result<(), MetaStoreError>;
    async fn get(&self, id: SeriesId) -> Result<SeriesMeta, MetaStoreError>;
    async fn get_all(&self) -> Result<Vec<SeriesMeta>, MetaStoreError>;
    async fn match_any(
        &self,
        labels: NonEmptySlice<Label>,
    ) -> Result<Vec<SeriesMeta>, MetaStoreError>;
    async fn match_all(
        &self,
        labels: NonEmptySlice<Label>,
    ) -> Result<Vec<SeriesMeta>, MetaStoreError>;
}
