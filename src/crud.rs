use std::num::NonZero;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use regex::Regex;
use serde::Deserialize;
use thiserror::Error;

use crate::{
    AppState,
    api::{ApiError, as_internal_err},
    helpers::{derive_block_size, duration},
    meta::{
        self, BlockLength, BlockNumber, Label, MetaStore, MetaStoreError, SampleLength, SeriesId,
        SeriesMeta, StorageType, TimeResolution,
    },
};

#[derive(Debug, Error)]
pub enum CrudError {
    #[error(
        "sample duration (length * resolution) muste be strictly smaller than block duration (length * resolution)"
    )]
    SampleBlockDurationMismatch,
    #[error("invalid series name: '{0}'. validity: /^[a-zA-Z][a-zA-Z0-9_]*$/")]
    InvalidSeriesName(String),
}

impl From<CrudError> for ApiError {
    fn from(err: CrudError) -> Self {
        match err {
            CrudError::SampleBlockDurationMismatch => ApiError::BadRequest(err.to_string()),
            CrudError::InvalidSeriesName(_) => ApiError::BadRequest(err.to_string()),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateSeries {
    pub name: String,
    pub storage_type: StorageType,
    pub block_length: Option<BlockLength>,
    pub block_resolution: Option<TimeResolution>,
    pub sample_length: SampleLength,
    pub sample_resolution: TimeResolution,
    pub labels: Vec<Label>,
}

impl From<&CreateSeries> for SeriesMeta {
    fn from(value: &CreateSeries) -> Self {
        let (block_len, block_res) = match (value.block_resolution, value.block_length) {
            (Some(block_res), Some(block_len)) => (block_len, block_res),
            _ => derive_block_size(
                value.storage_type,
                value.sample_resolution,
                value.sample_length,
            ),
        };

        SeriesMeta {
            id: SeriesId(NonZero::new(1).unwrap()),
            name: value.name.clone(),
            storage_type: value.storage_type,
            block_length: block_len,
            block_resolution: block_res,
            sample_length: value.sample_length,
            sample_resolution: value.sample_resolution,
            first_block: BlockNumber(0),
            last_block: BlockNumber(0),
            labels: value.labels.clone(),
        }
    }
}

const RE_NAME: &str = "^[a-zA-Z][a-zA-Z0-9_]*$";
impl CreateSeries {
    pub fn validate(&self) -> Result<(), ApiError> {
        if let (Some(block_resolution), Some(block_length)) =
            (self.block_resolution, self.block_length)
        {
            let block_duration = duration(block_resolution, block_length.0);
            let sample_duration = duration(self.sample_resolution, self.sample_length.0);

            if block_duration <= sample_duration {
                return Err(CrudError::SampleBlockDurationMismatch.into());
            }
        }

        validate_series_name(self.name.as_str())?;

        Ok(())
    }
}

fn validate_series_name(name: &str) -> Result<(), ApiError> {
    let re = Regex::new(RE_NAME).map_err(|e| as_internal_err(e))?;
    Ok(if !re.is_match(name) {
        return Err(CrudError::InvalidSeriesName(name.to_string()).into());
    })
}

fn into_api_error(e: MetaStoreError) -> ApiError {
    e.into()
}

pub(crate) async fn create_series(
    State(state): State<AppState>,
    Json(series): Json<CreateSeries>,
) -> Result<(StatusCode, Json<SeriesId>), ApiError> {
    series.validate()?;
    let id = state
        .meta_store
        .create(&(&series).into())
        .await
        .map_err(into_api_error)?;

    Ok((StatusCode::CREATED, Json(id)))
}

pub(crate) async fn read_series(
    State(state): State<AppState>,
    Path(id): Path<SeriesId>,
) -> Result<Json<SeriesMeta>, ApiError> {
    let series = state.meta_store.get(id).await.map_err(into_api_error)?;
    Ok(Json(series))
}

#[derive(Debug, Deserialize)]
pub struct UpdateSeries {
    pub name: Option<String>,
    pub labels: Option<Vec<Label>>,
}

impl UpdateSeries {
    fn validate(&self) -> Result<(), ApiError> {
        if self.name.is_none() && self.labels.is_none() {
            return Err(ApiError::BadRequest("No changes to apply".to_string()));
        }

        if let Some(name) = &self.name {
            validate_series_name(name)?;
        }

        Ok(())
    }
}

pub(crate) async fn update_series(
    State(state): State<AppState>,
    Path(id): Path<SeriesId>,
    Json(update): Json<UpdateSeries>,
) -> Result<Json<SeriesMeta>, ApiError> {
    update.validate()?;
    let mut series = state.meta_store.get(id).await.map_err(into_api_error)?;

    if let Some(name) = update.name {
        series.name = name;
    }

    if let Some(labels) = update.labels {
        series.labels = labels;
    }

    state
        .meta_store
        .update(&series)
        .await
        .map_err(into_api_error)?;

    let series = state.meta_store.get(id).await.map_err(into_api_error)?;
    Ok(Json(series))
}

pub(crate) async fn delete_series(
    State(state): State<AppState>,
    Path(id): Path<SeriesId>,
) -> Result<StatusCode, ApiError> {
    state.meta_store.delete(id).await.map_err(into_api_error)?;
    // TODO: we need to queue background batch deletion here for blocks
    Ok(StatusCode::NO_CONTENT)
}
