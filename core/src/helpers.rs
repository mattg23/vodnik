use std::num::NonZero;

use tracing::info;

use crate::meta::{BlockLength, SampleLength, SeriesMeta, StorageType, TimeResolution};

pub fn get_block_start_as_offset(meta: &SeriesMeta, block_id: u64) -> u64 {
    let res: u64 = meta.block_resolution.into();
    res * block_id * meta.block_length.0.get()
}

pub fn get_block_id(meta: &SeriesMeta, unix_ms: u64) -> u64 {
    let res: u64 = meta.block_resolution.into();
    unix_ms / (meta.block_length.0.get() * res)
}

pub fn get_sample_offset(meta: &SeriesMeta, delta_from_block_start: u64) -> u64 {
    let res: u64 = meta.sample_resolution.into();
    delta_from_block_start / (meta.sample_length.0.get() * res)
}

pub fn get_block_length(meta: &SeriesMeta) -> u64 {
    let block_duration: u64 = duration(meta.block_resolution, meta.block_length.0);
    let sample_duration: u64 = duration(meta.sample_resolution, meta.sample_length.0);
    block_duration / sample_duration
}

pub fn duration(res: TimeResolution, len: NonZero<u64>) -> u64 {
    let res: u64 = res.into();
    len.get() * res
}

pub fn derive_block_size(
    storage_type: StorageType,
    sample_res: TimeResolution,
    sample_len: SampleLength,
) -> (BlockLength, TimeResolution) {
    const MIN_BLOCK_LENGTH: u64 = 1024; // TODO: settings? we need to test this

    let ms_per_sample: u64 = sample_len.0.get() * (sample_res as u64);

    let (block_len, block_res) = match ms_per_sample * MIN_BLOCK_LENGTH {
        ms if ms < TimeResolution::Second.into() => (ms, TimeResolution::Millisecond),
        ms if ms < TimeResolution::Minute.into() => {
            (ms / TimeResolution::Second as u64, TimeResolution::Second)
        }
        ms if ms < TimeResolution::Hour.into() => {
            (ms / TimeResolution::Minute as u64, TimeResolution::Minute)
        }
        ms => (ms / TimeResolution::Hour as u64, TimeResolution::Hour),
    };

    let bucket = TimeDuration::find_bucket(
        NonZero::new(block_len).expect("zero block length"),
        block_res,
    );

    let res = (BlockLength(bucket.len), bucket.res);
    info!("derived block size: Input={storage_type:?},{sample_len:?},{sample_res:?} => {res:?}");
    res
}

#[derive(Copy, Clone)]
struct TimeDuration {
    pub len: NonZero<u64>,
    pub res: TimeResolution,
}

impl PartialEq for TimeDuration {
    fn eq(&self, other: &Self) -> bool {
        let self_ms = self.len.get() * self.res as u64;
        let other_ms = other.len.get() * other.res as u64;
        self_ms == other_ms
    }
}

impl PartialOrd for TimeDuration {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let self_ms = self.len.get() * self.res as u64;
        let other_ms = other.len.get() * other.res as u64;
        self_ms.partial_cmp(&other_ms)
    }
}

const fn td(len: u64, res: TimeResolution) -> TimeDuration {
    TimeDuration {
        len: unsafe { NonZero::new_unchecked(len) },
        res,
    }
}

impl TimeDuration {
    const BUCKETS: [TimeDuration; 20] = [
        td(1, TimeResolution::Second),
        td(5, TimeResolution::Second),
        td(15, TimeResolution::Second),
        td(30, TimeResolution::Second),
        td(1, TimeResolution::Minute),
        td(5, TimeResolution::Minute),
        td(15, TimeResolution::Minute),
        td(30, TimeResolution::Minute),
        td(1, TimeResolution::Hour),
        td(2, TimeResolution::Hour),
        td(6, TimeResolution::Hour),
        td(12, TimeResolution::Hour),
        td(24, TimeResolution::Hour),
        td(36, TimeResolution::Hour),
        td(48, TimeResolution::Hour),
        td(72, TimeResolution::Hour),
        td(144, TimeResolution::Hour),
        td(288, TimeResolution::Hour),
        td(576, TimeResolution::Hour),
        td(1152, TimeResolution::Hour),
    ];

    pub(crate) fn find_bucket(len: NonZero<u64>, res: TimeResolution) -> TimeDuration {
        TimeDuration { len, res }.clamp()
    }

    fn clamp(&self) -> Self {
        debug_assert!(Self::BUCKETS.windows(2).all(|w| w[0] < w[1]));

        for b in Self::BUCKETS.iter() {
            if self <= b {
                return *b;
            }
        }

        *self
    }
}
