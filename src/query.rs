use axum::{
    Json,
    extract::{Path, State},
};

use crate::{
    AppState,
    api::ApiError,
    meta::{BlockNumber, SeriesId, SizedBlock},
    persistence,
};

pub(crate) async fn read_single_block(
    State(state): State<AppState>,
    Path((series_id, block_id)): Path<(SeriesId, BlockNumber)>,
) -> Result<Json<SizedBlock>, ApiError> {
    let b = persistence::read_block_from_storage(
        &state.storage,
        &state.block_meta,
        series_id,
        block_id,
    )
    .await?;

    Ok(Json(b))
}
