use std::fmt::Display;

use axum::{
    Json, Router,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, patch, post},
};
use thiserror::Error;
use tracing::warn;

use crate::{
    AppState,
    crud::{create_series, delete_series, read_series, update_series},
    ingest::BatchIngest,
    meta::MetaStoreError,
};

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/batch", post(batch_ingest))
        .route("/series", post(create_series))
        .route("/series/{id}", get(read_series))
        .route("/series/{id}", patch(update_series))
        .route("/series/{id}", delete(delete_series))
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

async fn batch_ingest(Json(req): Json<BatchIngest>) -> Result<(), ApiError> {
    // TODO: limit req size + add streaming endpoint
    req.validate()?;
    Ok(())
}
