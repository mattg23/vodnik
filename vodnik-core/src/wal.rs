use crate::meta::{BlockNumber, Quality, SeriesId, StorableNum, WriteBatch};
use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WalError {
    #[error("WAL I/O operation failed: {0}")]
    Io(#[from] io::Error),

    #[error("Failed to sync WAL to disk: {0}")]
    SyncFailed(io::Error),

    #[error("Failed to serialize WAL entry: {0}")]
    Serialization(String),

    #[error("Buffer too small, expected: {0}")]
    BufferTooSmall(usize),

    #[error(
        "WAL Corruption detected: CRC mismatch (expected {expected:#010x}, found {found:#010x})"
    )]
    ChecksumMismatch { expected: u32, found: u32 },

    #[error("Invalid WAL frame length: {0}")]
    InvalidFrameLength(u32),

    #[error("Unexpected end of WAL file (incomplete frame)")]
    UnexpectedEof,

    #[error("WAL configuration error: {0}")]
    Config(String),
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TxId(pub u64);

#[derive(Debug)]
pub enum WalSync {
    Immediate,
}

pub enum WalEntry<T: StorableNum> {
    Write {
        block: BlockNumber,
        qs: Vec<Quality>,
        series: SeriesId,
        ts: Vec<u64>,
        tx: TxId,
        vals: Vec<T>,
    },
    Flush {
        tx: TxId,
        series: SeriesId,
        block: BlockNumber,
    },
}

const TAG_WRITE: u8 = 1;
const TAG_FLUSH: u8 = 2;

impl<T: StorableNum> WalEntry<T> {
    pub fn write(&self, bytes: &mut [u8]) -> Result<usize, WalError> {
        let required = self.storage_size_bytes();
        if required > bytes.len() {
            return Err(WalError::BufferTooSmall(required));
        }

        let mut cursor = Cursor::new(bytes);

        match self {
            WalEntry::Write {
                tx,
                series,
                block,
                ts,
                vals,
                qs,
            } => {
                cursor.write_u8(TAG_WRITE);

                cursor.write_u64(tx.0);
                cursor.write_u64(series.0.get());
                cursor.write_u64(block.0);

                let count = ts.len() as u32;
                cursor.write_u32(count);

                for ts in ts {
                    cursor.write_u64(*ts);
                }
                for val in vals {
                    cursor.write_val(*val);
                }
                for q in qs {
                    cursor.write_u8(q.0);
                }
            }
            WalEntry::Flush { tx, series, block } => {
                cursor.write_u8(TAG_FLUSH);
                cursor.write_u64(tx.0);
                cursor.write_u64(series.0.get());
                cursor.write_u64(block.0);
            }
        }

        Ok(cursor.pos)
    }

    pub fn storage_size_bytes(&self) -> usize {
        match self {
            WalEntry::Write { qs, ts, vals, .. } => {
                size_of::<u8>() // Tag 
                    + size_of::<u64>() // block
                    + size_of::<u64>() // series
                    + size_of::<u64>() // txid
                    + size_of::<u32>() // len of the data vecs
                    + (size_of::<T>() * vals.len())
                    + (size_of::<u64>() * ts.len())
                    + (size_of::<u8>() * qs.len())
            }
            WalEntry::Flush { .. } => {
                // tx + series + block
                size_of::<u8>() + size_of::<u64>() + size_of::<u64>() + size_of::<u64>()
            }
        }
    }
}

pub fn from_write_batch<'a, T: StorableNum>(tx: TxId, batch: &WriteBatch<'a, T>) -> WalEntry<T> {
    WalEntry::Write {
        block: batch.block_id,
        series: batch.series.id,
        ts: Vec::from(batch.ts),     // TODO: no cpy
        vals: Vec::from(batch.vals), // TODO: no cpy
        qs: Vec::from(batch.qs),     // TODO: no cpy
        tx,
    }
}

pub struct WalFrame {
    pub len: u32,
    pub crc: u32,
    pub payload: Vec<u8>,
}

const ALGO: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);

impl WalFrame {
    pub fn set_crc(&mut self) {
        self.crc = self.calc_crc();
    }

    pub fn calc_crc(&self) -> u32 {
        ALGO.checksum(&self.payload.as_slice())
    }

    pub fn get_storage_size(&self) -> usize {
        // [LEN][CRC][PAYLOAD]
        size_of::<u32>() + size_of::<u32>() + self.payload.len()
    }

    pub fn write(&self, mut w: impl io::Write) -> Result<(), io::Error> {
        w.write(&self.len.to_le_bytes())?;
        w.write(&self.crc.to_le_bytes())?;
        w.write_all(&self.payload.as_slice())?;
        Ok(())
    }
}

struct Cursor<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn write_u8(&mut self, v: u8) {
        self.buf[self.pos] = v;
        self.pos += 1;
    }

    fn write_u32(&mut self, v: u32) {
        let s = size_of::<u32>();
        self.buf[self.pos..self.pos + s].copy_from_slice(&v.to_le_bytes());
        self.pos += s;
    }

    fn write_u64(&mut self, v: u64) {
        let s = size_of::<u64>();
        self.buf[self.pos..self.pos + s].copy_from_slice(&v.to_le_bytes());
        self.pos += s;
    }

    fn write_val<T: StorableNum>(&mut self, v: T) {
        let s = size_of::<T>();
        v.write_le_bytes(&mut self.buf[self.pos..self.pos + s]);
        self.pos += s;
    }

    // TODO: read stuff
}
