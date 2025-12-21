use std::{collections::BTreeMap, num::NonZero};

use crate::meta::*;

use sea_orm::{
    ActiveValue::Set, Database, FromJsonQueryResult, IntoActiveModel, entity::prelude::*,
};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use tracing::info;

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "Text")]
pub enum DbStorageType {
    #[sea_orm(string_value = "float32")]
    Float32,
    #[sea_orm(string_value = "float64")]
    Float64,
    #[sea_orm(string_value = "int32")]
    Int32,
    #[sea_orm(string_value = "int64")]
    Int64,
    #[sea_orm(string_value = "uint32")]
    UInt32,
    #[sea_orm(string_value = "uint64")]
    UInt64,
    #[sea_orm(string_value = "enumeration")]
    Enumeration,
}

impl From<StorageType> for DbStorageType {
    fn from(v: StorageType) -> Self {
        match v {
            StorageType::Float32 => Self::Float32,
            StorageType::Float64 => Self::Float64,
            StorageType::Int32 => Self::Int32,
            StorageType::Int64 => Self::Int64,
            StorageType::UInt32 => Self::UInt32,
            StorageType::UInt64 => Self::UInt64,
            StorageType::Enumeration => Self::Enumeration,
        }
    }
}

impl From<DbStorageType> for StorageType {
    fn from(v: DbStorageType) -> Self {
        match v {
            DbStorageType::Float32 => Self::Float32,
            DbStorageType::Float64 => Self::Float64,
            DbStorageType::Int32 => Self::Int32,
            DbStorageType::Int64 => Self::Int64,
            DbStorageType::UInt32 => Self::UInt32,
            DbStorageType::UInt64 => Self::UInt64,
            DbStorageType::Enumeration => Self::Enumeration,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "Text")]
pub enum DbTimeResolution {
    #[sea_orm(string_value = "ms")]
    Millisecond,
    #[sea_orm(string_value = "s")]
    Second,
    #[sea_orm(string_value = "min")]
    Minute,
    #[sea_orm(string_value = "h")]
    Hour,
}

impl From<TimeResolution> for DbTimeResolution {
    fn from(v: TimeResolution) -> Self {
        match v {
            TimeResolution::Millisecond => Self::Millisecond,
            TimeResolution::Second => Self::Second,
            TimeResolution::Minute => Self::Minute,
            TimeResolution::Hour => Self::Hour,
        }
    }
}

impl From<DbTimeResolution> for TimeResolution {
    fn from(v: DbTimeResolution) -> Self {
        match v {
            DbTimeResolution::Millisecond => Self::Millisecond,
            DbTimeResolution::Second => Self::Second,
            DbTimeResolution::Minute => Self::Minute,
            DbTimeResolution::Hour => Self::Hour,
        }
    }
}

#[derive(Clone, Debug, PartialEq, FromJsonQueryResult)]
pub struct DbLabels(pub Vec<Label>);

impl Serialize for DbLabels {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for label in &self.0 {
            map.serialize_entry(&label.name, &label.value)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for DbLabels {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let map = BTreeMap::<String, String>::deserialize(deserializer)?;
        Ok(DbLabels(
            map.into_iter()
                .map(|(name, value)| Label { name, value })
                .collect(),
        ))
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "series")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = true)]
    pub id: i64,
    pub name: String,
    pub storage_type: DbStorageType,
    pub block_len: i64,
    pub block_res: DbTimeResolution,
    pub sample_len: i64,
    pub sample_res: DbTimeResolution,
    pub first: i64,
    pub last: i64,
    pub labels: DbLabels,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Clone, Debug)]
pub struct SqlMetaStore {
    db: DatabaseConnection,
}

impl SqlMetaStore {
    fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub async fn create(db_url: &str) -> Result<Self, MetaStoreError> {
        match Database::connect(db_url).await {
            Ok(_db) => {
                info!("Connected to metadata database at {}", db_url);
                Ok(Self::new(_db))
            }
            Err(e) => Err(orm_err(e)),
        }
    }
}

fn model_to_meta(m: Model) -> SeriesMeta {
    SeriesMeta {
        id: SeriesId(NonZero::new(m.id as u64).unwrap()),
        name: m.name,
        storage_type: m.storage_type.into(),
        block_length: BlockLength(NonZero::new(m.block_len as u64).unwrap()),
        block_resolution: m.block_res.into(),
        sample_length: SampleLength(NonZero::new(m.sample_len as u64).unwrap()),
        sample_resolution: m.sample_res.into(),
        first_block: BlockNumber(m.first as u64),
        last_block: BlockNumber(m.last as u64),
        labels: m.labels.0,
    }
}

fn orm_err(e: sea_orm::DbErr) -> MetaStoreError {
    // TODO: maybe we have more special cases here later
    MetaStoreError::Unknown(e.into())
}

impl MetaStore for SqlMetaStore {
    async fn create(&self, series: &SeriesMeta) -> Result<SeriesId, MetaStoreError> {
        let active = ActiveModel {
            name: Set(series.name.clone()),
            storage_type: Set(series.storage_type.into()),
            block_len: Set(series.block_length.0.get() as i64),
            block_res: Set(series.block_resolution.into()),
            sample_len: Set(series.sample_length.0.get() as i64),
            sample_res: Set(series.sample_resolution.into()),
            first: Set(series.first_block.0 as i64),
            last: Set(series.last_block.0 as i64),
            labels: Set(DbLabels(series.labels.clone())),
            ..Default::default()
        };

        let res = Entity::insert(active)
            .exec(&self.db)
            .await
            .map_err(orm_err)?;

        Ok(SeriesId(NonZero::new(res.last_insert_id as u64).unwrap()))
    }

    async fn get(&self, id: SeriesId) -> Result<SeriesMeta, MetaStoreError> {
        let model = Entity::find_by_id(id.0.get() as i64)
            .one(&self.db)
            .await
            .map_err(orm_err)?
            .ok_or(MetaStoreError::NotFound(id))?;

        Ok(model_to_meta(model))
    }
    async fn get_all(&self) -> Result<Vec<SeriesMeta>, MetaStoreError> {
        let models = Entity::find().all(&self.db).await.map_err(orm_err)?;

        Ok(models.into_iter().map(model_to_meta).collect())
    }

    async fn update(&self, series: &SeriesMeta) -> Result<(), MetaStoreError> {
        let mut model = Entity::find_by_id(series.id.0.get() as i64)
            .one(&self.db)
            .await
            .map_err(orm_err)?
            .ok_or(MetaStoreError::NotFound(series.id))?
            .into_active_model();

        model.name = Set(series.name.clone());
        model.storage_type = Set(series.storage_type.into());
        model.block_len = Set(series.block_length.0.get() as i64);
        model.block_res = Set(series.block_resolution.into());
        model.sample_len = Set(series.sample_length.0.get() as i64);
        model.sample_res = Set(series.sample_resolution.into());
        model.first = Set(series.first_block.0 as i64);
        model.last = Set(series.last_block.0 as i64);
        model.labels = Set(DbLabels(series.labels.clone()));

        model.update(&self.db).await.map_err(orm_err)?;

        Ok(())
    }
    async fn delete(&self, id: SeriesId) -> Result<(), MetaStoreError> {
        let res = Entity::delete_by_id(id.0.get() as i64)
            .exec(&self.db)
            .await
            .map_err(orm_err)?;

        if res.rows_affected == 0 {
            return Err(MetaStoreError::NotFound(id));
        }

        Ok(())
    }
    async fn match_any(
        &self,
        labels: NonEmptySlice<'_, Label>,
    ) -> Result<Vec<SeriesMeta>, MetaStoreError> {
        let wanted = labels.as_slice();

        let models = Entity::find().all(&self.db).await.map_err(orm_err)?;

        Ok(models
            .into_iter()
            .filter(|m| wanted.iter().any(|l| m.labels.0.iter().any(|x| x == l)))
            .map(model_to_meta)
            .collect())
    }

    async fn match_all(
        &self,
        labels: NonEmptySlice<'_, Label>,
    ) -> Result<Vec<SeriesMeta>, MetaStoreError> {
        let wanted = labels.as_slice();

        let models = Entity::find().all(&self.db).await.map_err(orm_err)?;

        Ok(models
            .into_iter()
            .filter(|m| wanted.iter().all(|l| m.labels.0.iter().any(|x| x == l)))
            .map(model_to_meta)
            .collect())
    }
}
