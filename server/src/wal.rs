use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use tracing::info;
use vodnik_core::{
    meta::{BlockWritable, SeriesMeta, StorableNum, WriteBatch},
    wal::{
        TAG_FLUSH, TAG_WRITE, TxId, WalEntry, WalEntryHeader, WalError, WalFrame, WalFrameIterator,
        WalSync,
    },
};

use crate::{AppState, persistence};

static TXID: AtomicU64 = AtomicU64::new(0);
pub fn next_txid() -> u64 {
    TXID.fetch_add(1, Ordering::SeqCst)
}

#[derive(Debug)]
pub struct WalConfig {
    pub dir: PathBuf,
    pub max_file_size: u64,
    pub sync_mode: WalSync,
}

#[derive(Debug)]
pub struct Wal {
    config: WalConfig,
    current_file: Option<File>,
    next_file_idx: u32,
    current_size: u64,
    write_buffer: Vec<u8>,
}

impl Wal {
    pub fn new(config: WalConfig) -> Result<Self, WalError> {
        if !config.dir.exists() {
            fs::create_dir_all(&config.dir).map_err(|e| WalError::Config(e.to_string()))?;
        }

        let mut _self = Self {
            config,
            current_file: None,
            next_file_idx: 0,
            current_size: 0,
            write_buffer: Vec::with_capacity(4 * 1024 * 1024),
        };

        Ok(_self)
    }

    pub fn write_entry<T: StorableNum>(&mut self, entry: &mut WalEntry<T>) -> Result<(), WalError> {
        if self.current_file.is_none() {
            // uninitialized after start
            self.open_next_log()?;
        }

        let result = if let Some(file) = &mut self.current_file {
            let req_size = entry.storage_size_bytes();
            if self.write_buffer.len() < req_size {
                self.write_buffer.resize(req_size, 0);
            }

            let used_bytes = entry.write(&mut self.write_buffer[..req_size])?;
            let payload_slice = &self.write_buffer[..used_bytes];

            let mut frame = WalFrame {
                len: payload_slice.len() as u32,
                crc: 0,
                payload: payload_slice.to_vec(), // TODO: cpy?
            };
            frame.set_crc();
            let frame_size = frame.get_storage_size();

            frame.write(&mut *file)?;
            self.current_size += frame_size as u64;

            match self.config.sync_mode {
                WalSync::Immediate => file.sync_data().map_err(WalError::SyncFailed)?,
            };

            Ok(())
        } else {
            Err(WalError::Config("Not initialized yet".to_string()))
        };

        if self.current_size > self.config.max_file_size {
            self.rotate()?;
        }

        result
    }

    fn open_next_log(&mut self) -> Result<(), WalError> {
        let path = self
            .config
            .dir
            .join(format!("wal_{:03}.log", self.next_file_idx));

        self.next_file_idx += 1;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        self.current_file = Some(file);
        self.current_size = 0;

        Ok(())
    }

    fn rotate(&mut self) -> Result<(), WalError> {
        self.open_next_log()
    }
}

pub fn find_wal_to_recover(wal_dir: PathBuf) -> Result<BTreeMap<TxId, WalFrame>, WalError> {
    let mut todo = BTreeMap::new();

    let files = std::fs::read_dir(wal_dir)?;

    for f in files {
        let f = f?;

        let iter = WalFrameIterator::new(f.path())?;
        for frame_res in iter {
            let mut frame = frame_res?;

            let header = WalEntryHeader::peek(frame.payload.as_mut_slice())?;
            match header.tag {
                TAG_WRITE => {
                    _ = todo.insert(header.tx, frame);
                }
                TAG_FLUSH => {
                    _ = todo.remove(&header.tx);
                }
                _ => unreachable!(),
            }
        }
    }

    Ok(todo)
}

pub async fn replay(todo: BTreeMap<TxId, WalFrame>, state: &AppState) -> anyhow::Result<()> {
    for (_, mut frame) in todo {
        let header = WalEntryHeader::peek(frame.payload.as_mut_slice())?;
        let meta = state.meta_store.get(header.series).await?;

        match meta.storage_type {
            vodnik_core::meta::StorageType::Float32 => {
                replay_entry::<f32>(
                    &state,
                    &meta,
                    WalEntry::<f32>::read(frame.payload.as_mut_slice())?,
                )
                .await?;
            }
            vodnik_core::meta::StorageType::Float64 => {
                replay_entry::<f64>(
                    &state,
                    &meta,
                    WalEntry::<f64>::read(frame.payload.as_mut_slice())?,
                )
                .await?;
            }
            vodnik_core::meta::StorageType::Int32 => {
                replay_entry::<i32>(
                    &state,
                    &meta,
                    WalEntry::<i32>::read(frame.payload.as_mut_slice())?,
                )
                .await?;
            }
            vodnik_core::meta::StorageType::Int64 => {
                replay_entry::<i64>(
                    &state,
                    &meta,
                    WalEntry::<i64>::read(frame.payload.as_mut_slice())?,
                )
                .await?;
            }
            vodnik_core::meta::StorageType::UInt32 => {
                replay_entry::<u32>(
                    &state,
                    &meta,
                    WalEntry::<u32>::read(frame.payload.as_mut_slice())?,
                )
                .await?;
            }
            vodnik_core::meta::StorageType::UInt64 => {
                replay_entry::<u64>(
                    &state,
                    &meta,
                    WalEntry::<u64>::read(frame.payload.as_mut_slice())?,
                )
                .await?;
            }
            vodnik_core::meta::StorageType::Enumeration => {
                replay_entry::<u8>(
                    &state,
                    &meta,
                    WalEntry::<u8>::read(frame.payload.as_mut_slice())?,
                )
                .await?;
            }
        }
    }

    Ok(())
}

async fn replay_entry<T: BlockWritable>(
    state: &AppState,
    series_meta: &SeriesMeta,
    entry: WalEntry<T>,
) -> anyhow::Result<()> {
    match entry {
        WalEntry::Write {
            tx,
            block,
            ts,
            vals,
            qs,
            ..
        } => {
            let batch = WriteBatch::new(
                series_meta,
                block,
                ts.as_slice(),
                vals.as_slice(),
                qs.as_slice(),
                tx,
            );
            crate::ingest::write_chunk(state, &batch, true).await?;
        }
        WalEntry::Flush { .. } => {
            unreachable!("");
        }
    }
    Ok(())
}

pub async fn force_flush(state: &AppState) -> anyhow::Result<()> {
    let mut blocks = vec![];
    state.hot.take_all_blocks(&mut blocks);

    let len = blocks.len();
    for (s, _, bn, sb) in blocks {
        persistence::flush_block(&state.storage, &state.block_meta, s, bn, &sb).await?;
    }

    info!("force flushed {len} blocks");

    Ok(())
}

pub fn cleanup_wal_files(wal_dir: PathBuf) -> std::io::Result<()> {
    let entries = fs::read_dir(wal_dir)?;

    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if path.extension().map_or(false, |ext| ext == "log") {
            info!("Removing processed WAL file: {:?}", path.file_name());
            fs::remove_file(path)?;
        }
    }
    Ok(())
}
