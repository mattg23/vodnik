use rkyv::{deserialize, rancor};
use thiserror::Error;
use tracing::error;

use crate::meta::{ArchivedSizedBlock, SizedBlock};

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("serialization failed with: {0}")]
    SerializationFailed(String),
    #[error("deserialization failed with: {0}")]
    DeserializationFailed(String),
    #[error("invalid/corrupted data ({0})")]
    InvalidData(String),
}

pub fn encode_block(block: &SizedBlock) -> Result<Vec<u8>, CodecError> {
    let bytes = rkyv::to_bytes::<rancor::Error>(block).map_err(|e| {
        error!("Rkyv serialization error: {:?}", e);
        CodecError::SerializationFailed(e.to_string())
    })?;

    // TODO: this creates a copy, fine for now. we prob write our own serializer later
    //       but atm we are experimenting with the internal structure

    Ok(bytes.to_vec())
}

pub fn decode_block(bs: &[u8]) -> Result<SizedBlock, CodecError> {
    let archived = rkyv::access::<ArchivedSizedBlock, rancor::Error>(&bs).map_err(|e| {
        error!("Rkyv access error: {:?}", e);
        CodecError::InvalidData(e.to_string())
    })?;

    let block = deserialize::<SizedBlock, rancor::Error>(archived).map_err(|e| {
        error!("Rkyv deserialization error: {:?}", e);
        CodecError::DeserializationFailed(e.to_string())
    })?;

    Ok(block)
}
