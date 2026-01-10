use std::{
    fs::{self, File, OpenOptions},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use vodnik_core::{
    meta::StorableNum,
    wal::{WalEntry, WalError, WalFrame, WalSync},
};

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

        _self.open_next_log()?;

        Ok(_self)
    }

    pub fn write_entry<T: StorableNum>(&mut self, entry: &mut WalEntry<T>) -> Result<(), WalError> {
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

    fn rotate(&self) -> Result<(), WalError> {
        todo!()
    }
}
