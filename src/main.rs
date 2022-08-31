use polars::prelude::*;

// use std::env;
// use std::fs::File;

fn main() -> Result<()> {


    let id = "123-45-67-890";
    let path = "arbin2.csv";
    
    let lf = arbin_csv_parser::arbin::read(path, id)?;
    let df = lf.collect()?;
    println!("{}", df);
    Ok(())

    // let temp_dir = env::temp_dir();
    // let temp_file = temp_dir.join("arbin.out.csv");
    // println!("{:?}", temp_file);
    // let file = File::create(temp_file).unwrap();
    // let mut writer = CsvWriter::new(file);
    // writer.finish(&mut df)
}
