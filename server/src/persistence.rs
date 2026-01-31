use crate::api::ApiError;
use crate::meta::block::BlockMetaStore;
use opendal::Operator;
use tracing::{debug, error};
use ulid::Ulid;
use vodnik_core::helpers;
use vodnik_core::meta::{BlockNumber, BlockWritable, SeriesId, SizedBlock, WriteBatch};

pub async fn flush_block(
    op: &Operator,
    db: &BlockMetaStore,
    series_id: SeriesId,
    block_id: BlockNumber,
    block: &SizedBlock,
) -> Result<(), ApiError> {
    // Format: data/{series_id % 100}/{series_id}/{block_id}_{uuid}.blk
    let path_pref = series_id.0.get() % 100u64;
    let write_id = Ulid::new();
    let object_key = format!(
        "data/{}/{}/{}_{}.blk",
        path_pref, series_id.0, block_id.0, write_id
    );

    // Write to Storage (OpenDAL)
    let bytes = vodnik_core::codec::encode_block(&block).map_err(|_| ApiError::Internal)?;

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

    let mut block = vodnik_core::codec::decode_block(&bytes).map_err(|_| ApiError::Internal)?;

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

pub(crate) async fn write_cold<'a, T: BlockWritable>(
    op: &Operator,
    db: &BlockMetaStore,
    batch: &'a WriteBatch<'a, T>,
) -> Result<(), ApiError> {
    debug!(
        "write_bold:: Series={}, Block={:?}, #samples={}",
        batch.series.id,
        batch.block_id,
        batch.ts.len()
    );

    let mut block_to_write =
        match read_block_from_storage(op, db, batch.series.id, batch.block_id).await {
            Ok(b) => b,
            Err(ApiError::NotFound(_)) => {
                let len = helpers::get_block_length(batch.series) as usize;
                T::new_sized_block(len)
            }
            Err(e) => {
                error!("{e:?}");
                return Err(e);
            }
        };

    block_to_write.write(batch);
    flush_block(op, db, batch.series.id, batch.block_id, &block_to_write).await
}
