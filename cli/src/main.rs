use std::{
    num::NonZero,
    path::PathBuf,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use clap::{Parser, Subcommand, ValueEnum};
use reqwest::Client;
use vodnik_core::{
    api::{BatchIngest, ValueVec},
    codec,
    meta::{BlockMeta, Quality, SeriesId, SizedBlock, StorableNum},
    wal::{TAG_WRITE, WalEntryHeader, WalFrameIterator},
};

#[derive(Parser)]
#[command(name = "vodnik-cli")]
#[command(about = "CLI tool for vodnik", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Base URL of the Vodnik server
    #[arg(long, default_value = "http://localhost:8123")]
    url: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate synthetic data and push it to a series
    Generate {
        /// The Series ID to write to
        #[arg(long)]
        series_id: NonZero<u64>,

        /// Number of samples to generate
        #[arg(long, default_value_t = 100)]
        count: usize,

        /// Data Pattern
        #[arg(long, value_enum, default_value_t = Pattern::Sine)]
        pattern: Pattern,

        // data type of the series
        #[arg(long, value_enum, default_value_t = StorageType::Float32)]
        stype: StorageType,

        /// Start Timestamp (Unix Seconds).
        /// If not provided, defaults to NOW - count (Liveish data)
        #[arg(long)]
        start: Option<u64>,

        /// Quality flag to apply to all points
        #[arg(long, default_value_t = 192)]
        quality: u8,
    },

    /// inspect a local block file
    InspectBlock {
        /// Path to the .blk file
        path: PathBuf,

        /// Print the first N values (optional)
        #[arg(long, default_value_t = 10)]
        head: usize,
    },
    /// inspect a local WAL file
    InspectWal {
        /// Path to the wal file
        path: PathBuf,

        /// How to print entries
        #[arg(long, value_enum, default_value_t = WalInspectMode::Headers)]
        mode: WalInspectMode,
    },
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum WalInspectMode {
    Frames,
    Headers,
    Full,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum Pattern {
    Sine,
    Constant,
    Ramp,
}

#[derive(Debug, Copy, Clone, ValueEnum)]
pub enum StorageType {
    Float32,
    Float64,
    Int32,
    Int64,
    UInt32,
    UInt64,
    Enumeration,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    eprintln!("{}", vodnik_core::VODNIK_ASCII);
    let cli = Cli::parse();

    match cli.command {
        Commands::Generate {
            series_id,
            count,
            pattern,
            start,
            quality,
            stype,
        } => generate_data(&cli, series_id, count, pattern, start, quality, stype).await?,
        Commands::InspectBlock { path, head } => inspect_block(path, head)?,
        Commands::InspectWal { path, mode } => inspect_wal(path, mode)?,
    }
    Ok(())
}

fn inspect_wal(path: PathBuf, mode: WalInspectMode) -> anyhow::Result<()> {
    let iter = WalFrameIterator::new(path)?;
    for frame_res in iter {
        let mut frame = frame_res?;
        print_frame(&mut frame, mode)?;
    }
    Ok(())
}

fn print_frame(frame: &mut vodnik_core::wal::WalFrame, mode: WalInspectMode) -> anyhow::Result<()> {
    print!("[len:{:8}][crc:{:8x}]", frame.len, frame.crc);

    if mode == WalInspectMode::Headers || mode == WalInspectMode::Full {
        let header = WalEntryHeader::peek(frame.payload.as_mut_slice())?;
        let tag = if header.tag == TAG_WRITE {
            "WRITE"
        } else {
            "FLUSH"
        };
        print!(
            "[{},{:?},{:?},{:?}]",
            tag, header.tx, header.series, header.block
        );
    }

    if mode == WalInspectMode::Full {
        todo!();
    }

    println!("");

    Ok(())
}

async fn generate_data(
    cli: &Cli,
    series_id: NonZero<u64>,
    count: usize,
    pattern: Pattern,
    start: Option<u64>,
    quality: u8,
    stype: StorageType,
) -> anyhow::Result<()> {
    let start_ts = start.unwrap_or_else(|| {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        now.saturating_sub((count as u128) * 1000u128) as u64
    });

    println!(
        "--> Generating {} samples for Series {} starting at {} ({:?})",
        count, series_id, start_ts, pattern
    );

    let mut ts = Vec::with_capacity(count);
    let vals = generate_values(stype, count, pattern);
    let mut qs = Vec::with_capacity(count);

    for i in 0..count {
        let t = start_ts + ((i * 1000) as u64);

        ts.push(t);
        qs.push(Quality(quality));
    }

    // 3. Send Request
    let payload = BatchIngest {
        series: SeriesId(series_id),
        ts: ts,
        qs: qs,
        vals,
    };

    let client = Client::new();
    let target_url = format!("{}/batch", cli.url);

    let start = Instant::now();
    let resp = client.post(&target_url).json(&payload).send().await?;
    let duration = start.elapsed();

    println!("<-- Status: {} (took {:.2?})", resp.status(), duration);

    if !resp.status().is_success() {
        let error_text = resp.text().await?;
        println!("    Error Body: {}", error_text);
    } else {
        println!(
            "    Success! Written {} points in {}ms.",
            count,
            duration.as_millis()
        );
    }
    Ok(())
}

fn generate_values(stype: StorageType, len: usize, pattern: Pattern) -> ValueVec {
    // Helper closure to calculate the raw value as f64 first
    let get_raw_val = |i: usize| -> f64 {
        match pattern {
            Pattern::Sine => 10.0 * ((i as f64) * 0.1).sin(),
            Pattern::Constant => 42.0,
            Pattern::Ramp => i as f64,
        }
    };

    match stype {
        StorageType::Float32 => {
            let vals = (0..len).map(|i| get_raw_val(i) as f32).collect();
            ValueVec::F32(vals)
        }
        StorageType::Float64 => {
            let vals = (0..len).map(|i| get_raw_val(i)).collect();
            ValueVec::F64(vals)
        }
        StorageType::Int32 => {
            let vals = (0..len).map(|i| get_raw_val(i) as i32).collect();
            ValueVec::I32(vals)
        }
        StorageType::Int64 => {
            let vals = (0..len).map(|i| get_raw_val(i) as i64).collect();
            ValueVec::I64(vals)
        }
        StorageType::UInt32 => {
            let vals = (0..len).map(|i| get_raw_val(i).abs() as u32).collect();
            ValueVec::U32(vals)
        }
        StorageType::UInt64 => {
            let vals = (0..len).map(|i| get_raw_val(i).abs() as u64).collect();
            ValueVec::U64(vals)
        }
        // For enum/u8, we just modulo 255 to keep it safe
        StorageType::Enumeration => {
            let vals = (0..len)
                .map(|i| (get_raw_val(i).abs() as u64 % 255) as u8)
                .collect();
            ValueVec::Enum(vals)
        }
    }
}

fn inspect_block(path: PathBuf, head: usize) -> anyhow::Result<()> {
    println!("Inspecting file: {:?}", path);

    // TODO: BufReader
    let bytes = std::fs::read(&path).map_err(|e| anyhow::anyhow!("Failed to read file: {}", e))?;

    let file_size = bytes.len();
    let block = codec::decode_block(&bytes).map_err(|e| anyhow::anyhow!("Decode failed: {}", e))?;

    macro_rules! inspect {
        ($meta:expr, $vals:expr, $qs:expr, $type_name:literal) => {{
            print_block_stats($meta, $qs, file_size, $type_name);
            if head > 0 {
                print_head($vals, $qs, head);
            }
        }};
    }

    match &block {
        SizedBlock::F32Block(m, v, q) => inspect!(m, v, q, "F32"),
        SizedBlock::F64Block(m, v, q) => inspect!(m, v, q, "F64"),
        SizedBlock::I32Block(m, v, q) => inspect!(m, v, q, "I32"),
        SizedBlock::I64Block(m, v, q) => inspect!(m, v, q, "I64"),
        SizedBlock::U32Block(m, v, q) => inspect!(m, v, q, "U32"),
        SizedBlock::U64Block(m, v, q) => inspect!(m, v, q, "U64"),
        SizedBlock::U8Block(m, v, q) => inspect!(m, v, q, "U8"),
    }
    Ok(())
}

fn print_block_stats<T: StorableNum>(
    meta: &BlockMeta<T>,
    qs: &[Quality],
    file_size: usize,
    type_name: &str,
) {
    let total_samples = qs.len();

    println!("==================================================");
    println!("Previous Object Key:   {}", meta.object_key);
    println!("--------------------------------------------------");

    println!("Type:         {}", type_name);
    println!("File Size:    {} bytes", file_size);
    println!("Samples:      {} Total", total_samples);
    println!("   ├─ Non-Miss:  {}", meta.count_non_missing);
    println!("   └─ Valid:     {} (Good | Uncertain)", meta.count_valid);

    println!("--------------------------------------------------");
    println!("Valid Data Stats:");
    println!("   ├─ Min:       {:?}", meta.min);
    println!("   ├─ Max:       {:?}", meta.max);
    println!("   └─ Sum:       {:?}", meta.sum);

    println!("--------------------------------------------------");
    println!("Boundary Values:");

    let fmt_pt = |val: T, q: Quality, off: u32| -> String {
        format!(
            "{:?} (Q:{:?} | {}) @ +{}sample_t",
            val,
            q,
            quality_bits(q.0),
            off
        )
    };

    println!("   [Any Data]");
    println!(
        "   ├─ First:     {}",
        fmt_pt(meta.fst, meta.fst_q, meta.fst_offset)
    );
    println!(
        "   └─ Last:      {}",
        fmt_pt(meta.lst, meta.lst_q, meta.lst_offset)
    );

    println!("   [Valid Only]");
    println!(
        "   ├─ First:     {}",
        fmt_pt(meta.fst_valid, meta.fst_valid_q, meta.fst_valid_offset)
    );
    println!(
        "   └─ Last:      {}",
        fmt_pt(meta.lst_valid, meta.lst_valid_q, meta.lst_valid_offset)
    );

    println!("--------------------------------------------------");
    println!("Quality Internals:");
    println!(
        "   ├─ OR Mask:   0x{:08x} (bin: {})",
        meta.qual_acc_or,
        bin_grouped(meta.qual_acc_or, 4)
    );
    println!(
        "   └─ AND Mask:  0x{:08x} (bin: {})",
        meta.qual_acc_and,
        bin_grouped(meta.qual_acc_and, 4)
    );

    println!("==================================================");
}

fn quality_bits(q: u8) -> String {
    let qq = (q >> 6) & 0b11;
    let ssss = (q >> 2) & 0b1111;
    let ll = q & 0b11;

    format!("{:02b}_{:04b}_{:02b}", qq, ssss, ll)
}

fn bin_grouped(v: u32, group: usize) -> String {
    let s = format!("{:032b}", v);
    s.as_bytes()
        .chunks(group)
        .map(std::str::from_utf8)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join("_")
}

// Generic Worker: Prints Head
fn print_head<T: StorableNum>(vals: &[T], qs: &[Quality], n: usize) {
    let limit = n.min(vals.len());
    println!("First {} values:", limit);
    for i in 0..limit {
        println!(
            "   [{:04}] {:?} (Q: {:?} | {})",
            i,
            vals[i],
            qs[i],
            quality_bits(qs[i].0)
        );
    }
}
