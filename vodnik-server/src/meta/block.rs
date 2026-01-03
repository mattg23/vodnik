use sea_orm::ActiveValue::NotSet;
use sea_orm::entity::prelude::*;
use sea_orm::{QueryOrder, QuerySelect, Set};
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use thiserror::Error;

use vodnik_core::meta::{
    BinaryAccumulator, BlockMeta, BlockNumber, Quality, SeriesId, StorableNum,
};

#[derive(Error, Debug)]
pub enum BlockMetaStoreError {
    #[error("Database error: {0}")]
    DbError(#[from] DbErr),
    #[error("Block not found: Series {0}, Block {1}")]
    BlockNotFound(i64, i64),
    #[error("Serialization error: {0}")]
    SerializationError(String),
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "blocks")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub series_id: i64,
    #[sea_orm(primary_key, auto_increment = false)]
    pub block_id: i64,

    pub count_non_missing: i64,
    pub count_valid: i64,

    // We store the sum as raw bytes (BLOB) to handle u128/i128 safely
    #[sea_orm(column_type = "Blob")]
    pub sum_val: Vec<u8>,

    #[sea_orm(column_type = "Double", nullable)]
    pub min_val: Option<f64>,
    #[sea_orm(column_type = "Double", nullable)]
    pub max_val: Option<f64>,

    #[sea_orm(column_type = "Double", nullable)]
    pub fst_valid_val: Option<f64>,
    pub fst_valid_q: Option<i32>,
    pub fst_valid_offset: Option<i64>,

    #[sea_orm(column_type = "Double", nullable)]
    pub lst_valid_val: Option<f64>,
    pub lst_valid_q: Option<i32>,
    pub lst_valid_offset: Option<i64>,

    #[sea_orm(column_type = "Double", nullable)]
    pub fst_val: Option<f64>,
    pub fst_q: Option<i32>,
    pub fst_offset: Option<i64>,

    #[sea_orm(column_type = "Double", nullable)]
    pub lst_val: Option<f64>,
    pub lst_q: Option<i32>,
    pub lst_offset: Option<i64>,

    pub qual_acc_or: i64,
    pub qual_acc_and: i64,

    pub object_key: String,
    pub created_at: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Clone, Debug)]
pub struct BlockMetaStore {
    db: DatabaseConnection,
}

impl BlockMetaStore {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

impl BlockMetaStore {
    /// Upsert metadata for a specific block.
    pub async fn upsert<T>(
        &self,
        series_id: SeriesId,
        block_id: BlockNumber,
        object_key: String,
        meta: &BlockMeta<T>,
    ) -> Result<(), BlockMetaStoreError>
    where
        T: StorableNum,
    {
        let db_series_id = series_id.0.get() as i64;
        let db_block_id = block_id.0 as i64;
        let model = ActiveModel {
            series_id: Set(db_series_id),
            block_id: Set(db_block_id),

            count_non_missing: Set(meta.count_non_missing as i64),
            count_valid: Set(meta.count_valid as i64),

            sum_val: Set(meta.sum.to_blob()),

            // TODO: for postgres we can use the correct types
            min_val: Set(num_traits::cast(meta.min).map(|v: f64| v)),
            max_val: Set(num_traits::cast(meta.max).map(|v: f64| v)),

            fst_valid_val: Set(num_traits::cast(meta.fst_valid).map(|v: f64| v)),
            fst_valid_q: Set(Some(meta.fst_valid_q.0 as i32)),
            fst_valid_offset: Set(Some(meta.fst_valid_offset as i64)),

            lst_valid_val: Set(num_traits::cast(meta.lst_valid).map(|v: f64| v)),
            lst_valid_q: Set(Some(meta.lst_valid_q.0 as i32)),
            lst_valid_offset: Set(Some(meta.lst_valid_offset as i64)),

            fst_val: Set(num_traits::cast(meta.fst).map(|v: f64| v)),
            fst_q: Set(Some(meta.fst_q.0 as i32)),
            fst_offset: Set(Some(meta.fst_offset as i64)),

            lst_val: Set(num_traits::cast(meta.lst).map(|v: f64| v)),
            lst_q: Set(Some(meta.lst_q.0 as i32)),
            lst_offset: Set(Some(meta.lst_offset as i64)),

            qual_acc_or: Set(meta.qual_acc_or as i64),
            qual_acc_and: Set(meta.qual_acc_and as i64),

            object_key: Set(object_key),
            created_at: NotSet, // let the DB handle that
        };

        Entity::insert(model)
            .on_conflict(
                sea_orm::sea_query::OnConflict::columns([Column::SeriesId, Column::BlockId])
                    .update_columns([
                        Column::CountNonMissing,
                        Column::CountValid,
                        Column::SumVal,
                        Column::MinVal,
                        Column::MaxVal,
                        Column::ObjectKey,
                        Column::CreatedAt,
                        Column::QualAccOr,
                        Column::QualAccAnd,
                        Column::FstValidVal,
                        Column::FstValidQ,
                        Column::FstValidOffset,
                        Column::LstValidVal,
                        Column::LstValidQ,
                        Column::LstValidOffset,
                        Column::FstVal,
                        Column::FstQ,
                        Column::FstOffset,
                        Column::LstVal,
                        Column::LstQ,
                        Column::LstOffset,
                    ])
                    .to_owned(),
            )
            .exec_without_returning(&self.db)
            .await?;

        Ok(())
    }

    /// Returns (BlockId, BlockMeta<T>) tuples.
    pub async fn list_in_range<T>(
        &self,
        series_id: SeriesId,
        min_block_id: BlockNumber,
        max_block_id: BlockNumber,
    ) -> Result<Vec<(BlockNumber, BlockMeta<T>)>, BlockMetaStoreError>
    where
        T: StorableNum,
        T::Accumulator: BinaryAccumulator,
    {
        let db_series_id = series_id.0.get() as i64;
        let db_min = min_block_id.0 as i64;
        let db_max = max_block_id.0 as i64;

        let models = Entity::find()
            .filter(Column::SeriesId.eq(db_series_id))
            .filter(Column::BlockId.gte(db_min))
            .filter(Column::BlockId.lte(db_max))
            .order_by_asc(Column::BlockId)
            .all(&self.db)
            .await?;

        let mut results = Vec::with_capacity(models.len());
        for m in models {
            let meta = Self::model_to_meta(&m)?;
            results.push((BlockNumber(m.block_id as u64), meta));
        }

        Ok(results)
    }

    /// Returns (BlockId, BlockMeta<T>) tuples.
    pub async fn get<T>(
        &self,
        series_id: SeriesId,
        block_id: BlockNumber,
    ) -> Result<BlockMeta<T>, BlockMetaStoreError>
    where
        T: StorableNum,
        T::Accumulator: BinaryAccumulator,
    {
        let db_series_id = series_id.0.get() as i64;
        let db_block_id = block_id.0 as i64;

        let model = Entity::find()
            .filter(Column::SeriesId.eq(db_series_id))
            .filter(Column::BlockId.eq(db_block_id))
            .one(&self.db)
            .await?;

        if let Some(b) = model {
            Self::model_to_meta(&b)
        } else {
            Err(BlockMetaStoreError::BlockNotFound(
                db_series_id,
                db_block_id,
            ))
        }
    }

    pub async fn get_object_key(
        &self,
        series_id: SeriesId,
        block_id: BlockNumber,
    ) -> Result<String, BlockMetaStoreError> {
        let db_series_id = series_id.0.get() as i64;
        let db_block_id = block_id.0 as i64;

        let result: Option<String> = Entity::find()
            .select_only()
            .column(Column::ObjectKey)
            .filter(Column::SeriesId.eq(db_series_id))
            .filter(Column::BlockId.eq(db_block_id))
            .into_tuple() // Maps the single column result directly to String
            .one(&self.db)
            .await?;

        result.ok_or(BlockMetaStoreError::BlockNotFound(
            db_series_id,
            db_block_id,
        ))
    }

    // Internal mapping function
    fn model_to_meta<T>(m: &Model) -> Result<BlockMeta<T>, BlockMetaStoreError>
    where
        T: StorableNum,
        T::Accumulator: BinaryAccumulator,
    {
        let cast =
            |opt: Option<f64>| -> T { opt.and_then(|v| num_traits::cast(v)).unwrap_or(T::zero()) };

        let cast_min = |opt: Option<f64>| -> T {
            opt.and_then(|v| num_traits::cast(v))
                .unwrap_or(T::max_value())
        };
        let cast_max = |opt: Option<f64>| -> T {
            opt.and_then(|v| num_traits::cast(v))
                .unwrap_or(T::min_value())
        };

        let q = |opt: Option<i32>| -> Quality {
            opt.map(|v| Quality(v as u8)).unwrap_or(Quality::MISSING)
        };

        Ok(BlockMeta {
            count_non_missing: m.count_non_missing as u32,
            count_valid: m.count_valid as u32,

            sum: T::Accumulator::from_blob(&m.sum_val)
                .map_err(|e| BlockMetaStoreError::SerializationError(e))?,

            min: cast_min(m.min_val),
            max: cast_max(m.max_val),

            fst_valid: cast(m.fst_valid_val),
            fst_valid_q: q(m.fst_valid_q),
            fst_valid_offset: m.fst_valid_offset.unwrap_or(0) as u32,

            lst_valid: cast(m.lst_valid_val),
            lst_valid_q: q(m.lst_valid_q),
            lst_valid_offset: m.lst_valid_offset.unwrap_or(0) as u32,

            fst: cast(m.fst_val),
            fst_q: q(m.fst_q),
            fst_offset: m.fst_offset.unwrap_or(0) as u32,

            lst: cast(m.lst_val),
            lst_q: q(m.lst_q),
            lst_offset: m.lst_offset.unwrap_or(0) as u32,

            qual_acc_or: m.qual_acc_or as u32,
            qual_acc_and: m.qual_acc_and as u32,

            object_key: m.object_key.clone(),
        })
    }
}
