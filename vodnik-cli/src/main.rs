use std::{
    num::NonZero,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use clap::{Parser, Subcommand, ValueEnum};
use reqwest::Client;
use vodnik_core::{
    api::{BatchIngest, ValueVec},
    meta::{Quality, SeriesId},
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
    }
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
