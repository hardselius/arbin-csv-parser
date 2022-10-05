use anyhow::Result;
use clap::Parser;
use polars::prelude::*;
use std::fs::File;
use std::path::PathBuf;

use data_playground::{arbin, keyence};

/// Simple program to stitch Arbin and Keyence data together in the time domain.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Keyence .csv file
    #[clap(short, long, value_parser)]
    keyence: Option<PathBuf>,

    /// Arbin .csv file
    #[clap(short, long, value_parser)]
    arbin: PathBuf,

    /// Channel number
    #[clap(short, long, value_parser)]
    channel: u32,

    /// Output .csv file
    #[clap(short, long, value_parser)]
    out: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let lf = if let Some(keyence) = args.keyence {
        arbin_keyence(keyence, args.arbin, args.channel)?
    } else {
        arbin::parse(args.arbin, args.channel)?
    };
    let mut df = lf.collect()?;

    let file = File::create(args.out).unwrap();
    let mut writer = CsvWriter::new(file);
    writer.finish(&mut df)?;
    Ok(())
}

fn arbin_keyence(keyence: PathBuf, arbin: PathBuf, channel: u32) -> Result<LazyFrame> {
    let klf = keyence::parse(keyence)?;
    let alf = arbin::parse(arbin, channel)?;
    Ok(alf.join(klf, [col("timestamp")], [col("timestamp")], JoinType::Inner))
}
