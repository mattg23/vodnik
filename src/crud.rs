use std::num::NonZero;

use axum::{Json, extract::State, http::StatusCode};
use regex::Regex;
use serde::Deserialize;
use thiserror::Error;

use crate::{
    AppState,
    api::{ApiError, as_internal_err},
    helpers::{derive_block_size, duration},
    meta::{
        BlockLength, BlockNumber, Label, MetaStore, SampleLength, SeriesId, SeriesMeta,
        StorageType, TimeResolution,
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

        let re = Regex::new("^[a-zA-Z][a-zA-Z0-9_]*$").map_err(|e| as_internal_err(e))?;

        if !re.is_match(self.name.as_str()) {
            return Err(CrudError::InvalidSeriesName(self.name.clone()).into());
        }

        Ok(())
    }
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
        .map_err(as_internal_err)?;

    Ok((StatusCode::CREATED, Json(id)))
}
