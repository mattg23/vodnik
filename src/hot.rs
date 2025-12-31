use std::{collections::HashMap, ops::Range};

use dashmap::DashMap;
use tracing::{debug, info};

use crate::{
    helpers,
    ingest::ValueVec,
    meta::{Block, BlockNumber, SeriesId, SeriesMeta, SizedBlock, StorageType},
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
        $items:expr,      // Values slice (e.g., &[f32])
        $Variant:path,    // The Enum Variant (e.g., SizedBlock::F32Block)
        $Type:ty          // The Native Type (e.g., f32)
    ) => {{
        // 1. Lifecycle Management: Rotate, Backfill, or Existing
        let mut current = if $self.live.is_some() {
            let live_block = $self
                .live_id
                .expect("self.live == Some(..), but live_id is None?");

            if live_block < $block {
                // Case: Rotation (Newer block arrived)
                $self.flush_live();
                $self.live_id = Some($block);
                $Variant(
                    Block::new(),
                    vec![None; helpers::get_block_length(&$series) as usize],
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
            $Variant(
                Block::new(),
                vec![None; helpers::get_block_length(&$series) as usize],
            )
        };

        let bl_start = helpers::get_block_start_as_offset(&$series, $block.0);

        // 2. The Write Loop
        match &mut current {
            &mut $Variant(ref mut block_meta, ref mut vals) => {
                for (t, v) in $ts.iter().zip($items.iter()) {
                    // Calculate index inside the Fixed Grid
                    let idx = helpers::get_sample_offset(&$series, *t - bl_start) as usize;

                    // Update Value and Metadata (Min/Max/Count)
                    let old = vals[idx];
                    vals[idx] = Some(*v);
                    block_meta.update_block_meta(*v, idx as u32, &old, vals.as_slice());
                }
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
                    &items[val_range],
                    SizedBlock::F32Block,
                    f32
                )
            }
            (StorageType::Float64, ValueVec::F64(items)) => {
                write_hot_variant!(
                    self,
                    series,
                    block,
                    ts,
                    &items[val_range],
                    SizedBlock::F64Block,
                    f64
                )
            }
            (StorageType::Int32, ValueVec::I32(items)) => {
                write_hot_variant!(
                    self,
                    series,
                    block,
                    ts,
                    &items[val_range],
                    SizedBlock::I32Block,
                    i32
                )
            }
            (StorageType::Int64, ValueVec::I64(items)) => {
                write_hot_variant!(
                    self,
                    series,
                    block,
                    ts,
                    &items[val_range],
                    SizedBlock::I64Block,
                    i64
                )
            }
            (StorageType::UInt32, ValueVec::U32(items)) => {
                write_hot_variant!(
                    self,
                    series,
                    block,
                    ts,
                    &items[val_range],
                    SizedBlock::U32Block,
                    u32
                )
            }
            (StorageType::UInt64, ValueVec::U64(items)) => {
                write_hot_variant!(
                    self,
                    series,
                    block,
                    ts,
                    &items[val_range],
                    SizedBlock::U64Block,
                    u64
                )
            }
            (StorageType::Enumeration, ValueVec::Enum(items)) => {
                write_hot_variant!(
                    self,
                    series,
                    block,
                    ts,
                    &items[val_range],
                    SizedBlock::U8Block,
                    u8
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
        val_range: Range<usize>,
    ) -> WriteResult {
        match self.data.try_get_mut(&series.id) {
            dashmap::try_result::TryResult::Present(mut hd) => {
                let wr = hd
                    .value_mut()
                    .write_into_block(series, block, ts, vals, val_range);
                debug!("case Present: {:?}", hd.value());
                wr
            }
            dashmap::try_result::TryResult::Absent => {
                let mut hd = HotData::default();
                let wr = hd.write_into_block(series, block, ts, vals, val_range);
                debug!("case Absent: {hd:?}");
                self.data.insert(series.id, hd);
                wr
            }
            dashmap::try_result::TryResult::Locked => WriteResult::Busy,
        }
    }
}
