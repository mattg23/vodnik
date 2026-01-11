use crate::meta::{BlockNumber, Quality, SeriesId, StorableNum, WriteBatch};
use std::{
    fs::File,
    io::{self, BufReader, Read},
    num::NonZero,
    path::PathBuf,
};
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
#[derive(Clone, Copy, Debug, PartialEq, Hash, Eq, Ord, PartialOrd)]
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

pub const TAG_WRITE: u8 = 1;
pub const TAG_FLUSH: u8 = 2;

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

    pub fn read(bytes: &mut [u8]) -> Result<Self, WalError> {
        if bytes.is_empty() {
            return Err(WalError::Serialization("Empty payload".into()));
        }

        let mut cursor = Cursor { buf: bytes, pos: 0 };

        let tag = cursor.read_u8();

        match tag {
            TAG_WRITE => {
                let tx = TxId(cursor.read_u64());
                let series = SeriesId(
                    NonZero::new(cursor.read_u64())
                        .ok_or(WalError::Serialization("0 series id".into()))?,
                );
                let block = BlockNumber(cursor.read_u64());

                let count = cursor.read_u32() as usize;

                let mut ts = Vec::with_capacity(count);
                for _ in 0..count {
                    ts.push(cursor.read_u64());
                }

                let mut vals = Vec::with_capacity(count);
                for _ in 0..count {
                    vals.push(cursor.read_val::<T>());
                }

                let mut qs = Vec::with_capacity(count);
                for _ in 0..count {
                    qs.push(Quality(cursor.read_u8()));
                }

                Ok(WalEntry::Write {
                    tx,
                    series,
                    block,
                    ts,
                    vals,
                    qs,
                })
            }
            TAG_FLUSH => {
                let tx = TxId(cursor.read_u64());
                let series = SeriesId(
                    NonZero::new(cursor.read_u64())
                        .ok_or(WalError::Serialization("0 series id".into()))?,
                );
                let block = BlockNumber(cursor.read_u64());

                Ok(WalEntry::Flush { tx, series, block })
            }
            _ => Err(WalError::Serialization(format!("Unknown tag: {}", tag))),
        }
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

pub fn from_write_batch<'a, T: StorableNum>(batch: &WriteBatch<'a, T>) -> WalEntry<T> {
    WalEntry::Write {
        block: batch.block_id,
        series: batch.series.id,
        ts: Vec::from(batch.ts),     // TODO: no cpy
        vals: Vec::from(batch.vals), // TODO: no cpy
        qs: Vec::from(batch.qs),     // TODO: no cpy
        tx: batch.tx,
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

    fn read_u8(&mut self) -> u8 {
        let v = self.buf[self.pos];
        self.pos += 1;
        v
    }

    fn read_u32(&mut self) -> u32 {
        let s = size_of::<u32>();
        // TODO: make this better
        let bytes = self.buf[self.pos..self.pos + s].try_into().unwrap();
        self.pos += s;
        u32::from_le_bytes(bytes)
    }

    fn read_u64(&mut self) -> u64 {
        let s = size_of::<u64>();
        // TODO: make this better
        let bytes = self.buf[self.pos..self.pos + s].try_into().unwrap();
        self.pos += s;
        u64::from_le_bytes(bytes)
    }

    fn read_val<T: StorableNum>(&mut self) -> T {
        let s = size_of::<T>();
        let v = T::read_le_bytes(&self.buf[self.pos..self.pos + s]);
        self.pos += s;
        v
    }
}

pub struct WalFrameIterator {
    wal_file: PathBuf,
    buffer: Vec<u8>,
    reader: BufReader<File>,
}

impl WalFrameIterator {
    pub fn new(wal_file: PathBuf) -> Result<Self, WalError> {
        Ok(Self {
            wal_file: wal_file.clone(),
            buffer: Vec::with_capacity(1 * 1024 * 1024),
            reader: BufReader::new(std::fs::File::open(wal_file)?),
        })
    }
}

impl Iterator for WalFrameIterator {
    type Item = Result<WalFrame, WalError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut header = [0u8; 8];
        match self.reader.read_exact(&mut header) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return None,
            Err(e) => return Some(Err(WalError::Io(e))),
        }

        let len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
        let expected_crc = u32::from_le_bytes(header[4..8].try_into().unwrap());

        if len == 0 || len > (100 * 1024 * 1024) {
            return Some(Err(WalError::InvalidFrameLength(len as u32)));
        }

        if self.buffer.len() < len {
            self.buffer.resize(len, 0);
        }

        let payload_buf = &mut self.buffer[..len];
        if let Err(e) = self.reader.read_exact(payload_buf) {
            return Some(Err(WalError::Io(e)));
        }

        let crc = ALGO.checksum(payload_buf);
        if crc != expected_crc {
            return Some(Err(WalError::ChecksumMismatch {
                expected: expected_crc,
                found: crc,
            }));
        }

        Some(Ok(WalFrame {
            len: len as u32,
            crc,
            payload: payload_buf.to_vec(),
        }))
    }
}

pub struct WalEntryHeader {
    pub tag: u8,
    pub tx: TxId,
    pub series: SeriesId,
    pub block: BlockNumber,
}

impl WalEntryHeader {
    pub fn peek(bytes: &mut [u8]) -> Result<Self, WalError> {
        // Use your Cursor helper to read just the first few fields
        let mut cursor = Cursor { buf: bytes, pos: 0 };
        let tag = cursor.read_u8();
        let tx = TxId(cursor.read_u64());
        let series = SeriesId(
            NonZero::new(cursor.read_u64()).ok_or(WalError::Serialization("0 series id".into()))?,
        );
        let block = BlockNumber(cursor.read_u64());

        Ok(Self {
            tag,
            tx,
            series,
            block,
        })
    }
}
