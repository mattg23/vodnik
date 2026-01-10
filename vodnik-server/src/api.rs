use std::fmt::Display;

use axum::{
    Router,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, patch, post},
};
use thiserror::Error;
use tracing::{error, warn};
use vodnik_core::wal::WalError;

use crate::{
    AppState,
    crud::{create_series, delete_series, read_series, update_series},
    ingest::batch_ingest,
    meta::{MetaStoreError, block::BlockMetaStoreError},
    query::read_single_block,
};

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/batch", post(batch_ingest))
        .route("/series", post(create_series))
        .route("/series/{id}", get(read_series))
        .route("/series/{id}", patch(update_series))
        .route("/series/{id}", delete(delete_series))
        .route(
            "/series/{series_id}/block/{block_id}",
            get(read_single_block),
        )
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Unprocessable(String),
    #[error("internal server error")]
    Internal,
    #[error("server busy")]
    ResourceLocked,
}

pub(crate) fn as_internal_err<E: Display>(err: E) -> ApiError {
    warn!("internal error: {}", err);
    ApiError::Internal
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            ApiError::Conflict(msg) => (StatusCode::CONFLICT, msg),
            ApiError::Unprocessable(msg) => (StatusCode::UNPROCESSABLE_ENTITY, msg),
            ApiError::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal server error".into(),
            ),
            ApiError::ResourceLocked => {
                (StatusCode::SERVICE_UNAVAILABLE, "server busy".to_string())
            }
        }
        .into_response()
    }
}

impl From<MetaStoreError> for ApiError {
    fn from(err: MetaStoreError) -> Self {
        match err {
            MetaStoreError::Duplicate(_) => ApiError::Conflict(err.to_string()),
            MetaStoreError::NotFound(_) => ApiError::NotFound(err.to_string()),
            MetaStoreError::Unknown(_) => as_internal_err(err),
        }
    }
}

impl From<opendal::Error> for ApiError {
    fn from(err: opendal::Error) -> Self {
        error!("opendal::Error: {err:?}");
        as_internal_err(err)
    }
}

impl From<BlockMetaStoreError> for ApiError {
    fn from(err: BlockMetaStoreError) -> Self {
        match err {
            BlockMetaStoreError::BlockNotFound(series_id, block_id) => ApiError::NotFound(format!(
                "Block not found: series_id={}, block_id={}",
                series_id, block_id
            )),

            BlockMetaStoreError::DbError(db_err) => {
                error!("Internal DB Error: {:?}", db_err);
                ApiError::Internal
            }

            BlockMetaStoreError::SerializationError(msg) => {
                error!("Block Serialization Error: {}", msg);
                ApiError::Internal
            }
        }
    }
}

impl From<WalError> for ApiError {
    fn from(err: WalError) -> Self {
        error!("WAL Critical Failure: {:?}", err);
        ApiError::Internal
    }
}
