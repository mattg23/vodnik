use crate::helpers;
use num_traits::{Bounded, Num, NumAssign, NumCast};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::{fmt, num::NonZero};

pub trait SafeAdd: Copy {
    fn safe_add(self, other: Self) -> Self;
}

macro_rules! impl_safe_add_int {
    ($($t:ty),*) => {
        $(
            impl SafeAdd for $t {
                #[inline]
                fn safe_add(self, other: Self) -> Self {
                    self.saturating_add(other)
                }
            }
        )*
    };
}

impl_safe_add_int!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128);

pub trait ByteStorable {
    fn write_le_bytes(&self, dst: &mut [u8]);
    fn read_le_bytes(src: &[u8]) -> Self;
}

macro_rules! impl_storable_bytes {
    ($($t:ty),*) => {
        $(
            impl ByteStorable for $t {
                #[inline]
                fn write_le_bytes(&self, dst: &mut [u8]) {
                    let bytes = self.to_le_bytes();
                    dst[..bytes.len()].copy_from_slice(&bytes);
                }

                #[inline]
                fn read_le_bytes(src: &[u8]) -> Self {
                    // TODO: zero copy?
                    let mut bytes = [0u8; std::mem::size_of::<$t>()];
                    bytes.copy_from_slice(&src[..std::mem::size_of::<$t>()]);
                    <$t>::from_le_bytes(bytes)
                }
            }
        )*
    };
}

impl_storable_bytes!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64);

// Helper to handle the BLOB storage for Sums (i128, u128, f64)
pub trait BinaryAccumulator: Sized {
    fn to_blob(&self) -> Vec<u8>;
    fn from_blob(bytes: &[u8]) -> Result<Self, String>;
}

macro_rules! impl_binary_accumulator {
    // Matches: i128, 16; f64, 8; ...
    ($($t:ty, $size:literal),* $(,)?) => {
        $(
            impl BinaryAccumulator for $t {
                fn to_blob(&self) -> Vec<u8> {
                    self.to_le_bytes().to_vec()
                }

                fn from_blob(bytes: &[u8]) -> Result<Self, String> {
                    let b: [u8; $size] = bytes.try_into().map_err(|_| {
                        format!(
                            "Invalid blob len for {}: expected {}, got {}",
                            stringify!($t),
                            $size,
                            bytes.len()
                        )
                    })?;
                    Ok(Self::from_le_bytes(b))
                }
            }
        )*
    };
}

impl_binary_accumulator!(f64, 8, i64, 8, u64, 8, i128, 16, u128, 16);

// for f64/f32 NaN is not allowed. this should be checked at the boundary
// at ingestion time. StorableNum assumes a non-NaN value for floating point types
pub trait StorableNum:
    Num + NumCast + NumAssign + Bounded + PartialOrd + Copy + Debug + ByteStorable
{
    type Accumulator: Num
        + NumCast
        + PartialOrd
        + Copy
        + NumAssign
        + SafeAdd
        + Debug
        + Default
        + BinaryAccumulator;

    fn to_acc(self) -> Self::Accumulator;
}

impl SafeAdd for f32 {
    #[inline]
    fn safe_add(self, other: Self) -> Self {
        self + other
    }
}

impl SafeAdd for f64 {
    #[inline]
    fn safe_add(self, other: Self) -> Self {
        self + other
    }
}
impl StorableNum for f32 {
    type Accumulator = f64;
    fn to_acc(self) -> f64 {
        self as f64
    }
}

impl StorableNum for f64 {
    type Accumulator = f64;
    fn to_acc(self) -> f64 {
        self
    }
}

impl StorableNum for i32 {
    type Accumulator = i64;
    fn to_acc(self) -> i64 {
        self as i64
    }
}

impl StorableNum for u32 {
    type Accumulator = u64;
    fn to_acc(self) -> u64 {
        self as u64
    }
}

impl StorableNum for i64 {
    type Accumulator = i128;
    fn to_acc(self) -> i128 {
        self as i128
    }
}

impl StorableNum for u64 {
    type Accumulator = u128;
    fn to_acc(self) -> u128 {
        self as u128
    }
}
impl StorableNum for u8 {
    type Accumulator = u64;
    fn to_acc(self) -> u64 {
        self as u64
    }
}

#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct BlockMeta<T: StorableNum> {
    pub count_non_missing: u32,
    pub count_valid: u32, // aka good | uncertain

    // stats for valid values
    pub sum: T::Accumulator,
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

    pub object_key: String,
}

impl<T: StorableNum> BlockMeta<T> {
    pub fn new() -> Self {
        Self {
            count_non_missing: 0,
            count_valid: 0,
            fst_offset: u32::MAX,
            lst_offset: u32::MIN,
            sum: T::Accumulator::default(),
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
            object_key: String::default(),
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

        self.sum = T::Accumulator::default();
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
                self.sum = self.sum.safe_add(v.to_acc());

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
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[serde(transparent)]
pub struct Quality(pub u8);

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
        (self.0 & Self::MASK_MAJOR) == Self::MISSING.0
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

#[derive(Debug, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum SizedBlock {
    F32Block(BlockMeta<f32>, Vec<f32>, Vec<Quality>),
    F64Block(BlockMeta<f64>, Vec<f64>, Vec<Quality>),
    I32Block(BlockMeta<i32>, Vec<i32>, Vec<Quality>),
    I64Block(BlockMeta<i64>, Vec<i64>, Vec<Quality>),
    U32Block(BlockMeta<u32>, Vec<u32>, Vec<Quality>),
    U64Block(BlockMeta<u64>, Vec<u64>, Vec<Quality>),
    U8Block(BlockMeta<u8>, Vec<u8>, Vec<Quality>),
}

pub struct WriteBatch<'a, T: StorableNum> {
    pub series: &'a SeriesMeta,
    pub block_id: BlockNumber,
    pub ts: &'a [u64], // ms after UNIX epoch
    pub vals: &'a [T],
    pub qs: &'a [Quality],
}

impl<'a, T: StorableNum> WriteBatch<'a, T> {
    pub fn new(
        series: &'a SeriesMeta,
        block_id: BlockNumber,
        ts: &'a [u64],
        vals: &'a [T],
        qs: &'a [Quality],
    ) -> Self {
        assert!(
            ts.len() == vals.len() && vals.len() == qs.len(),
            "WriteBatch length mismatch: ts={}, vals={}, qs={}.",
            ts.len(),
            vals.len(),
            qs.len()
        );

        Self {
            series,
            block_id,
            ts,
            vals,
            qs,
        }
    }
}

pub trait BlockWritable: StorableNum {
    fn write_to_block(block: &mut SizedBlock, batch: &WriteBatch<Self>);
    fn new_sized_block(len: usize) -> SizedBlock;
}

macro_rules! impl_block_data_type {
    ($type:ty, $variant:ident) => {
        impl BlockWritable for $type {
            fn write_to_block(block: &mut SizedBlock, batch: &WriteBatch<Self>) {
                match block {
                    SizedBlock::$variant(block_meta, vals, qs) => {
                        let bl_start =
                            helpers::get_block_start_as_offset(batch.series, batch.block_id.0);

                        for i in 0..batch.ts.len() {
                            let idx =
                                helpers::get_sample_offset(batch.series, batch.ts[i] - bl_start)
                                    as usize;

                            vals[idx] = batch.vals[i];
                            qs[idx] = batch.qs[i];
                        }
                        // TODO: do running stats instead of full recalc
                        block_meta.recalc_block_data_full(vals, qs);
                    }
                    other => {
                        unreachable!(
                            "Type Mismatch in HotSet: Expected {}, got {}",
                            stringify!($variant),
                            std::any::type_name_of_val(&other)
                        );
                    }
                }
            }

            fn new_sized_block(len: usize) -> SizedBlock {
                SizedBlock::$variant(
                    BlockMeta::new(),
                    vec![Default::default(); len],
                    vec![Quality::MISSING; len],
                )
            }
        }
    };
}

impl_block_data_type!(f32, F32Block);
impl_block_data_type!(f64, F64Block);
impl_block_data_type!(i32, I32Block);
impl_block_data_type!(i64, I64Block);
impl_block_data_type!(u32, U32Block);
impl_block_data_type!(u64, U64Block);
impl_block_data_type!(u8, U8Block);

impl SizedBlock {
    pub fn write<T: BlockWritable>(&mut self, batch: &WriteBatch<T>) {
        T::write_to_block(self, batch);
    }

    pub fn new<T: BlockWritable>(len: usize) -> SizedBlock {
        T::new_sized_block(len)
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

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Serialize, Deserialize, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub struct BlockNumber(pub u64);

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct BlockLength(pub NonZero<u64>);

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct SampleLength(pub NonZero<u64>);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Label {
    pub name: String,
    pub value: String,
}

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Deserialize, Serialize, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub struct SeriesId(pub NonZero<u64>);

impl std::fmt::Display for SeriesId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
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

#[derive(Debug)]
#[repr(transparent)]
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
