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

    pub fn update(
        &mut self,
        value: T,
        offset: u32,
        old_value: &Option<T>,
        block_data: &[Option<T>],
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

            for i in block_data.iter().filter_map(|x| *x) {
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

#[repr(u64)]
#[derive(Debug, Copy, Clone)]
pub enum TimeResolution {
    Millisecond = 1,
    Second = 1000,
    Minute = 1000 * 60,
    Hour = 1000 * 60 * 60,
}

#[derive(Debug, Copy, Clone)]
pub enum StorageType {
    Float32,
    Float64,
    Int32,
    Int64,
    UInt32,
    UInt64,
    Enumeration,
}

#[derive(Copy, Clone, Debug)]
pub struct BlockNumber(pub NonZero<u64>);
#[derive(Copy, Clone, Debug)]
pub struct BlockLength(pub NonZero<u64>);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Label {
    pub name: String,
    pub value: String,
}
#[derive(Copy, Clone, Debug)]
pub struct SeriesId(pub NonZero<u64>);

impl std::fmt::Display for SeriesId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug)]
pub struct SeriesMeta {
    pub id: SeriesId,
    pub name: String,
    pub storage_type: StorageType,
    pub block_length: BlockLength,
    pub block_resolution: TimeResolution,
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
