use std::{collections::HashMap, ops::Range};

use dashmap::DashMap;
use tracing::{debug, info, trace};
use vodnik_core::api::ValueVec;
use vodnik_core::helpers;
use vodnik_core::meta::{
    BlockMeta, BlockNumber, BlockWritable, Quality, SeriesId, SeriesMeta, SizedBlock, StorageType,
    WriteBatch,
};
use vodnik_core::wal::TxId;

#[derive(Default, Debug)]
struct HotData {
    live: Option<(TxId, SizedBlock)>,
    flushing: HashMap<BlockNumber, (TxId, SizedBlock)>,
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

impl HotData {
    fn write_into_block<T: BlockWritable>(&mut self, batch: &WriteBatch<T>) -> WriteResult {
        debug!(
            "write_into_block:: Series={}, Block={:?}, #samples={}, TX={:?}",
            batch.series.id,
            batch.block_id,
            batch.ts.len(),
            batch.tx
        );

        let (tx, mut current) = if self.live.is_some() {
            let live_block = self
                .live_id
                .expect("self.live == Some(..), but live_id is None?");

            if live_block < batch.block_id {
                // Case: Rotation (Newer block arrived)
                let len = helpers::get_block_length(batch.series) as usize;
                self.flush_live();
                self.live_id = Some(batch.block_id);
                info!("rotated live block for series {}", batch.series.id);
                (batch.tx, SizedBlock::new::<T>(len))
            } else if live_block > batch.block_id {
                // Case: Backfill (Older block arrived) -> Send to Cold Store
                return WriteResult::NeedsColdStore;
            } else {
                // Case: Current (Append to existing live block)
                self.live.take().unwrap()
            }
        } else {
            // Case: Cold Start (First block)
            self.live_id = Some(batch.block_id);
            let len = helpers::get_block_length(&batch.series) as usize;
            (batch.tx, SizedBlock::new::<T>(len))
        };

        // write to the block
        current.write::<T>(batch);
        // TODO: handle out of order better
        let tx = TxId(batch.tx.0.max(tx.0));
        // State Restore
        self.live = Some((tx, current));
        self.live_id = Some(batch.block_id);

        WriteResult::Ok {
            live: self.live_id.expect("No live_id after write"),
            flushing: self.flushing.keys().copied().collect(),
        }
    }

    fn flush_live(&mut self) {
        let live = self.live.take().unwrap();
        self.flushing.insert(self.live_id.unwrap(), live);
    }

    fn take_flushing_block(&mut self, block: BlockNumber) -> Option<(TxId, SizedBlock)> {
        self.flushing.remove(&block)
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

    pub(crate) fn take_all_blocks(
        &self,
        buff: &mut Vec<(SeriesId, TxId, BlockNumber, SizedBlock)>,
    ) {
        for mut k in self.data.iter_mut() {
            let series = k.key().clone();
            k.flush_live();
            let blocks: Vec<BlockNumber> = k.flushing.keys().copied().collect();
            for b in blocks {
                if let Some((tx, block)) = k.value_mut().take_flushing_block(b) {
                    buff.push((series, tx, b, block));
                }
            }
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

    pub(crate) fn take_flushing_block(
        &self,
        series: SeriesId,
        block: BlockNumber,
    ) -> Option<(TxId, SizedBlock)> {
        match self.data.try_get_mut(&series) {
            dashmap::try_result::TryResult::Present(mut hd) => {
                hd.value_mut().take_flushing_block(block)
            }
            dashmap::try_result::TryResult::Absent => None,
            dashmap::try_result::TryResult::Locked => None,
        }
    }

    pub(crate) fn write<T: BlockWritable>(&self, batch: &WriteBatch<T>) -> WriteResult {
        match self.data.try_get_mut(&batch.series.id) {
            dashmap::try_result::TryResult::Present(mut hd) => {
                let wr = hd.value_mut().write_into_block(batch);
                trace!("case Present: {:?}", hd.value());
                wr
            }
            dashmap::try_result::TryResult::Absent => {
                let mut hd = HotData::default();
                let wr = hd.write_into_block(batch);
                trace!("case Absent: {hd:?}");
                self.data.insert(batch.series.id, hd);
                wr
            }
            dashmap::try_result::TryResult::Locked => WriteResult::Busy,
        }
    }
}
