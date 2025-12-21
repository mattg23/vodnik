pub mod store;

use num_traits::{Bounded, Num, NumAssign, NumCast};
use serde::{Deserialize, Serialize};
use std::num::NonZero;
use thiserror::Error;

// for f64/f32 NaN is not allowed. this should be checked at the boundary
// at ingestion time. StorableNum assumes a non-NaN value for floating point types
pub trait StorableNum: Num + NumCast + NumAssign + Bounded + PartialOrd + Copy {
    // TODO: add quality
}

impl StorableNum for f64 {}
impl StorableNum for f32 {}
impl StorableNum for i64 {}
impl StorableNum for i32 {}
impl StorableNum for u32 {}
impl StorableNum for u64 {}
impl StorableNum for u8 {}

#[derive(Debug, Copy, Clone)]
pub struct Block<T: StorableNum> {
    // BLOCK stats need to be adjusted for quality later
    // for v1 we prob want a fixed set of allowed "qualities"
    // something like GOOD, BAD, MISSING, DELETED, MANUAL
    pub count: u32,
    pub fst_offset: u32,
    pub lst_offset: u32,

    pub sum: T,
    pub min: T,
    pub max: T,
    pub fst: T,
    pub lst: T,
}

impl<T: StorableNum> Block<T> {
    pub fn new() -> Self {
        Self {
            count: 0,
            fst_offset: u32::MAX,
            lst_offset: u32::MIN,
            sum: T::zero(),
            min: T::max_value(),
            max: T::min_value(),
            fst: T::zero(),
            lst: T::zero(),
        }
    }

    pub fn update_block_meta(
        &mut self,
        value: T,
        offset: u32,
        old_value: &Option<T>,
        updated_block_data: &[Option<T>],
    ) {
        if let Some(old) = old_value {
            self.sum -= *old;
        } else {
            self.count += 1;
        }

        self.sum += value;

        let is_append = offset > self.lst_offset;

        if self.fst_offset >= offset {
            self.fst_offset = offset;
            self.fst = value;
        }

        if self.lst_offset <= offset {
            self.lst_offset = offset;
            self.lst = value;
        }

        if is_append || old_value.is_none() {
            if self.min > value {
                self.min = value;
            }

            if self.max < value {
                self.max = value;
            }
        } else {
            //if not append && is_overwrite we need to calc min / max again
            self.min = T::max_value();
            self.max = T::min_value();

            for i in updated_block_data.iter().filter_map(|x| *x) {
                if self.min > i {
                    self.min = i;
                }

                if self.max < i {
                    self.max = i;
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum SizedBlock {
    F32Block(Block<f32>, Vec<Option<f32>>),
    F64Block(Block<f64>, Vec<Option<f64>>),
    I32Block(Block<i32>, Vec<Option<i32>>),
    I64Block(Block<i64>, Vec<Option<i64>>),
    U32Block(Block<u32>, Vec<Option<u32>>),
    U64Block(Block<u64>, Vec<Option<u64>>),
    U8Block(Block<u8>, Vec<Option<u8>>),
}

impl SizedBlock {
    pub fn get_size(&self) -> usize {
        match self {
            SizedBlock::F32Block(_, d) => {
                4 + 4 + 4 + std::mem::size_of::<f32>() * 5 + std::mem::size_of::<f32>() * d.len()
            }
            SizedBlock::F64Block(_, d) => {
                4 + 4 + 4 + std::mem::size_of::<f64>() * 5 + std::mem::size_of::<f64>() * d.len()
            }
            SizedBlock::I32Block(_, d) => {
                4 + 4 + 4 + std::mem::size_of::<i32>() * 5 + std::mem::size_of::<i32>() * d.len()
            }
            SizedBlock::I64Block(_, d) => {
                4 + 4 + 4 + std::mem::size_of::<i64>() * 5 + std::mem::size_of::<i64>() * d.len()
            }
            SizedBlock::U32Block(_, d) => {
                4 + 4 + 4 + std::mem::size_of::<u32>() * 5 + std::mem::size_of::<u32>() * d.len()
            }
            SizedBlock::U64Block(_, d) => {
                4 + 4 + 4 + std::mem::size_of::<u64>() * 5 + std::mem::size_of::<u64>() * d.len()
            }
            SizedBlock::U8Block(_, d) => {
                4 + 4 + 4 + std::mem::size_of::<u8>() * 5 + std::mem::size_of::<u8>() * d.len()
            }
        }
    }

    pub fn get_count_written(&self) -> u32 {
        match self {
            SizedBlock::F32Block(b, _) => b.count,
            SizedBlock::F64Block(b, _) => b.count,
            SizedBlock::I32Block(b, _) => b.count,
            SizedBlock::I64Block(b, _) => b.count,
            SizedBlock::U32Block(b, _) => b.count,
            SizedBlock::U64Block(b, _) => b.count,
            SizedBlock::U8Block(b, _) => b.count,
        }
    }
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

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
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
#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
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
