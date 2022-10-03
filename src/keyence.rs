use anyhow::Result;
use polars::prelude::*;
use std::path::Path;

pub fn parse(path: impl AsRef<Path>) -> Result<LazyFrame> {
    let fields = vec![
        Field::new("datetime", DataType::Utf8),
        Field::new("distance", DataType::Float64),
    ];
    let schema = Schema::from(fields);

    let mut lf = LazyCsvReader::new(path)
        .with_schema(Arc::new(schema))
        .finish()?
        .select(&[
            col("*").exclude(["datetime"]),
            col("datetime")
                .str()
                .strptime(StrpTimeOptions {
                    date_dtype: DataType::Datetime(
                        TimeUnit::Milliseconds,
                        Some("UTC".to_string()),
                    ),
                    fmt: Some("%+".to_string()),
                    strict: true,
                    exact: true,
                })
                // .cast(DataType::Uint64) / lit(1000) * lit(1000)
                .cast(DataType::Datetime(
                    TimeUnit::Milliseconds,
                    Some("UTC".to_string()),
                ))
                .alias("datetime"),
            lit("a").alias("group"),
            // col("*").exclude(["timestamp"]),
            // lit("a").alias("group"),
            // (col("timestamp") * lit(1000))
            //     .cast(DataType::Datetime(
            //         TimeUnit::Milliseconds,
            //         Some("UTC".to_string()),
            //     ))
            //     .alias("datetime"),
        ]);

    lf = lf.select(&[
        col("*").exclude(["datetime"]),
        (col("datetime").cast(DataType::Int64) / lit(1000) * lit(1000)).cast(DataType::Datetime(TimeUnit::Milliseconds, Some("UTC".to_string()))),
    ]);


    let mut df = lf.collect()?;
    println!("{}", df);
    df = df
        .upsample_stable(
            &["group"],
            "datetime",
            Duration::parse("1s"),
            Duration::parse("0s"),
        )?;

    df = df.fill_null(FillNullStrategy::Backward(None))?;
    println!("{}", df);

    lf = df.lazy().select(&[
        col("*").exclude(["group"]),
        (col("datetime").cast(DataType::Int64) / lit(1000)).alias("timestamp"),
    ]);
    Ok(lf)
}
