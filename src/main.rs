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
    keyence: PathBuf,

    /// Arbin .csv file
    #[clap(short, long, value_parser)]
    arbin: PathBuf,
    
    /// Output .csv file
    #[clap(short, long, value_parser)]
    out: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let klf = keyence::parse(args.keyence)?;
    let mut alf = arbin::parse(args.arbin)?;
    alf = alf.join(klf, [col("timestamp")], [col("timestamp")], JoinType::Inner);
    let mut df = alf.collect()?;

    // let temp_dir = env::temp_dir();
    // let temp_file = temp_dir.join("arbin_with_distance.csv");
    // println!("{:?}", temp_file);

    let file = File::create(args.out).unwrap();
    let mut writer = CsvWriter::new(file);
    writer.finish(&mut df)?;
    Ok(())
}
