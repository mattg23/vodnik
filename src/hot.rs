use std::{collections::HashMap, ops::Range};

use dashmap::DashMap;
use tracing::{debug, info};

use crate::{
    helpers,
    ingest::ValueVec,
    meta::{BlockMeta, BlockNumber, Quality, SeriesId, SeriesMeta, SizedBlock, StorageType},
};

#[derive(Default, Debug)]
struct HotData {
    live: Option<SizedBlock>,
    flushing: HashMap<BlockNumber, SizedBlock>,
    live_id: Option<BlockNumber>,
}

#[derive(Debug)]
pub(crate) enum WriteResult {
    Ok {
        live: BlockNumber,
        flushing: Vec<BlockNumber>,
    },
    Busy,
    NeedsColdStore,
}

macro_rules! write_hot_variant {
    (
        $self:expr,       // The 'self' instance
        $series:expr,     // SeriesMeta
        $block:expr,      // BlockNumber
        $ts:expr,         // Timestamp slice
        $qs:expr,         // Quality slice
        $items:expr,      // Values slice (e.g., &[f32])
        $Variant:path,    // The Enum Variant (e.g., SizedBlock::F32Block)
        $Type:ty,         // The Native Type (e.g., f32)
        $def:expr         // Default Value of the Native type
    ) => {{
        // 1. Lifecycle Management: Rotate, Backfill, or Existing
        let mut current = if $self.live.is_some() {
            let live_block = $self
                .live_id
                .expect("self.live == Some(..), but live_id is None?");

            if live_block < $block {
                // Case: Rotation (Newer block arrived)
                let len = helpers::get_block_length(&$series) as usize;
                $self.flush_live();
                $self.live_id = Some($block);
                $Variant(
                    BlockMeta::new(),
                    vec![$def; len],
                    vec![Quality::MISSING; len],
                )
            } else if live_block > $block {
                // Case: Backfill (Older block arrived) -> Send to Cold Store
                return WriteResult::NeedsColdStore;
            } else {
                // Case: Current (Append to existing live block)
                $self.live.take().unwrap()
            }
        } else {
            // Case: Cold Start (First block)
            $self.live_id = Some($block);
            let len = helpers::get_block_length(&$series) as usize;
            $Variant(
                BlockMeta::new(),
                vec![$def; len],
                vec![Quality::MISSING; len],
            )
        };

        let bl_start = helpers::get_block_start_as_offset(&$series, $block.0);

        assert!($ts.len() == $items.len() && $items.len() == $qs.len());

        // 2. The Write Loop
        match &mut current {
            &mut $Variant(ref mut block_meta, ref mut vals, ref mut qs) => {
                for i in 0..$ts.len() {
                    let v = $items[i];
                    let t = $ts[i];
                    let q = $qs[i];

                    let idx = helpers::get_sample_offset(&$series, t - bl_start) as usize;
                    vals[idx] = v;
                    qs[idx] = q;
                }
                block_meta.recalc_block_data_full(vals, qs);
            }
            other => {
                // Safety Panic: This should never happen if SeriesMeta aligns with Data
                unreachable!(
                    "Type Mismatch in HotSet: Expected {}, got {}",
                    stringify!($Variant),
                    std::any::type_name_of_val(&other)
                );
            }
        };

        // 3. State Restore
        $self.live = Some(current);
        $self.live_id = Some($block);

        WriteResult::Ok {
            live: $self.live_id.expect("No live_id after write"),
            flushing: $self.flushing.keys().copied().collect(),
        }
    }};
}

impl HotData {
    fn flushed(&mut self, block: BlockNumber) {
        if let Some(_) = self.flushing.remove(&block) {
            debug!("removed {block:?} from flushing");
        } else {
            info!("tried to remove {block:?} from flushing list, which didn't exist");
        }
    }
    fn write_into_block(
        &mut self,
        series: &SeriesMeta,
        block: BlockNumber,
        ts: &[u64],
        qs: &[Quality],
        vals: &ValueVec,
        val_range: Range<usize>,
    ) -> WriteResult {
        debug!(
            "write_into_block:: Series={}, Block={:?}, #samples={}",
            series.id,
            &block,
            ts.len()
        );
        match (series.storage_type, vals) {
            (StorageType::Float32, ValueVec::F32(items)) => {
                write_hot_variant!(
                    self,
                    series,
                    block,
                    ts,
                    qs,
                    &items[val_range.clone()],
                    SizedBlock::F32Block,
                    f32,
                    f32::default()
                )
            }
            (StorageType::Float64, ValueVec::F64(items)) => {
                write_hot_variant!(
                    self,
                    series,
                    block,
                    ts,
                    qs,
                    &items[val_range.clone()],
                    SizedBlock::F64Block,
                    f64,
                    f64::default()
                )
            }
            (StorageType::Int32, ValueVec::I32(items)) => {
                write_hot_variant!(
                    self,
                    series,
                    block,
                    ts,
                    qs,
                    &items[val_range.clone()],
                    SizedBlock::I32Block,
                    i32,
                    i32::default()
                )
            }
            (StorageType::Int64, ValueVec::I64(items)) => {
                write_hot_variant!(
                    self,
                    series,
                    block,
                    ts,
                    qs,
                    &items[val_range.clone()],
                    SizedBlock::I64Block,
                    i64,
                    i64::default()
                )
            }
            (StorageType::UInt32, ValueVec::U32(items)) => {
                write_hot_variant!(
                    self,
                    series,
                    block,
                    ts,
                    qs,
                    &items[val_range.clone()],
                    SizedBlock::U32Block,
                    u32,
                    u32::default()
                )
            }
            (StorageType::UInt64, ValueVec::U64(items)) => {
                write_hot_variant!(
                    self,
                    series,
                    block,
                    ts,
                    qs,
                    &items[val_range.clone()],
                    SizedBlock::U64Block,
                    u64,
                    u64::default()
                )
            }
            (StorageType::Enumeration, ValueVec::Enum(items)) => {
                write_hot_variant!(
                    self,
                    series,
                    block,
                    ts,
                    qs,
                    &items[val_range.clone()],
                    SizedBlock::U8Block,
                    u8,
                    u8::default()
                )
            }
            (st, val) => {
                panic!(
                    "Type Mismatch in Ingestion: Series expected {:?}, but payload contained {:?}",
                    st, val
                )
            }
        }
    }

    fn flush_live(&mut self) {
        let live = self.live.take().unwrap();
        self.flushing.insert(self.live_id.unwrap(), live);
        // TODO: Notify external system to store block & update meta data
    }
}

pub(crate) struct HotSet {
    data: DashMap<SeriesId, HotData>,
}

impl std::fmt::Debug for HotSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HotSet")
            .field("data", &self.data.len())
            .finish()
    }
}

impl HotSet {
    pub(crate) fn new() -> Self {
        Self {
            data: DashMap::new(),
        }
    }

    // returns (live, flushing)
    pub(crate) fn get_live_blocks(&self, id: SeriesId) -> (Option<BlockNumber>, Vec<BlockNumber>) {
        if let Some(hd) = self.data.get(&id) {
            (hd.live_id, hd.flushing.keys().copied().collect())
        } else {
            (None, vec![])
        }
    }

    pub(crate) fn write(
        &self,
        series: &SeriesMeta,
        block: BlockNumber,
        ts: &[u64],
        vals: &ValueVec,
        qs: &[Quality],
        val_range: Range<usize>,
    ) -> WriteResult {
        match self.data.try_get_mut(&series.id) {
            dashmap::try_result::TryResult::Present(mut hd) => {
                let wr = hd
                    .value_mut()
                    .write_into_block(series, block, ts, qs, vals, val_range);
                debug!("case Present: {:?}", hd.value());
                wr
            }
            dashmap::try_result::TryResult::Absent => {
                let mut hd = HotData::default();
                let wr = hd.write_into_block(series, block, ts, qs, vals, val_range);
                debug!("case Absent: {hd:?}");
                self.data.insert(series.id, hd);
                wr
            }
            dashmap::try_result::TryResult::Locked => WriteResult::Busy,
        }
    }
}
