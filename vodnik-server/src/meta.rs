use thiserror::Error;
use vodnik_core::meta::SeriesId;

use crate::api::ApiError;

pub mod block;
pub mod store;

#[derive(Error, Debug)]
pub enum MetaStoreError {
    #[error("series {0} already exists.")]
    Duplicate(SeriesId),
    #[error("series {0} not found")]
    NotFound(SeriesId),
    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}

pub(crate) fn into_api_error(e: MetaStoreError) -> ApiError {
    e.into()
}
