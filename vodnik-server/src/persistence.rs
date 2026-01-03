use crate::api::ApiError;
use crate::meta::block::BlockMetaStore;
use opendal::Operator;
use rkyv::{deserialize, rancor};
use tracing::{debug, error};
use ulid::Ulid;
use vodnik_core::helpers;
use vodnik_core::meta::{
    ArchivedSizedBlock, BlockNumber, BlockWritable, Quality, SeriesId, SeriesMeta, SizedBlock,
};

pub async fn flush_block(
    op: &Operator,
    db: &BlockMetaStore,
    series_id: SeriesId,
    block_id: BlockNumber,
    block: &SizedBlock,
) -> Result<(), ApiError> {
    let bytes = rkyv::to_bytes::<rancor::Error>(block).map_err(|e| {
        error!("Rkyv serialization error: {:?}", e);
        ApiError::Internal
    })?;

    // Format: data/{series_id % 100}/{series_id}/{block_id}_{uuid}.blk
    let path_pref = series_id.0.get() % 100u64;
    let write_id = Ulid::new();
    let object_key = format!(
        "data/{}/{}/{}_{}.blk",
        path_pref, series_id.0, block_id.0, write_id
    );

    // Write to Storage (OpenDAL)
    // TODO: this creates a copy, fine for now. we prob write our own serializer later
    //       but atm we are experimenting with the internal structure
    let bytes = bytes.to_vec();

    // TODO: On S3 we need to know when the flushed block is available for read (research).
    //       maybe we need to postpone updating the metadata a bit
    op.write(&object_key, bytes).await.map_err(|e| {
        error!("error writing to storage: {:?}", e);
        ApiError::Internal
    })?;

    // update metadata
    let result = match block {
        SizedBlock::F32Block(meta, ..) => db.upsert(series_id, block_id, object_key, meta).await,
        SizedBlock::F64Block(meta, ..) => db.upsert(series_id, block_id, object_key, meta).await,
        SizedBlock::I32Block(meta, ..) => db.upsert(series_id, block_id, object_key, meta).await,
        SizedBlock::I64Block(meta, ..) => db.upsert(series_id, block_id, object_key, meta).await,
        SizedBlock::U32Block(meta, ..) => db.upsert(series_id, block_id, object_key, meta).await,
        SizedBlock::U64Block(meta, ..) => db.upsert(series_id, block_id, object_key, meta).await,
        SizedBlock::U8Block(meta, ..) => db.upsert(series_id, block_id, object_key, meta).await,
    };

    result.map_err(ApiError::from)
}

pub async fn read_block_from_storage(
    op: &Operator,
    db: &BlockMetaStore,
    series_id: SeriesId,
    block_id: BlockNumber,
) -> Result<SizedBlock, ApiError> {
    let key = db
        .get_object_key(series_id, block_id)
        .await
        .map_err(ApiError::from)?;

    let bytes = op
        .read(&key)
        .await
        .map_err(|e| {
            error!("Failed to read block raw: {:?}", e);
            ApiError::Internal
        })
        .map(|bs| bs.to_vec())?;

    let archived = rkyv::access::<ArchivedSizedBlock, rancor::Error>(&bytes).unwrap();

    let mut block = deserialize::<SizedBlock, rancor::Error>(archived).map_err(|e| {
        error!("Rkyv serialization error: {:?}", e);
        ApiError::Internal
    })?;

    match &mut block {
        SizedBlock::F32Block(meta, ..) => meta.object_key = key,
        SizedBlock::F64Block(meta, ..) => meta.object_key = key,
        SizedBlock::I32Block(meta, ..) => meta.object_key = key,
        SizedBlock::I64Block(meta, ..) => meta.object_key = key,
        SizedBlock::U32Block(meta, ..) => meta.object_key = key,
        SizedBlock::U64Block(meta, ..) => meta.object_key = key,
        SizedBlock::U8Block(meta, ..) => meta.object_key = key,
    }

    Ok(block)
}

pub(crate) async fn write_cold<T: BlockWritable>(
    op: &Operator,
    db: &BlockMetaStore,
    series: &SeriesMeta,
    block: BlockNumber,
    ts: &[u64],
    qs: &[Quality],
    vals: &[T],
) -> Result<(), ApiError> {
    debug!(
        "write_bold:: Series={}, Block={:?}, #samples={}",
        series.id,
        &block,
        ts.len()
    );

    let mut block_to_write = match read_block_from_storage(op, db, series.id, block).await {
        Ok(b) => b,
        Err(ApiError::NotFound(_)) => {
            let len = helpers::get_block_length(&series) as usize;
            T::new_sized_block(len)
        }
        Err(e) => {
            error!("{e:?}");
            return Err(e);
        }
    };

    block_to_write.write(series, block, ts, vals, qs);
    flush_block(op, db, series.id, block, &block_to_write).await
}
