use anyhow::Result;
use polars::prelude::*;
use std::path::Path;

pub fn parse(path: impl AsRef<Path>, channel: u32) -> Result<LazyFrame> {
    let mut schema = Schema::new();
    schema.with_column("Discharge Capacity (Ah)".to_string(), DataType::Float64);

    let mut lf = LazyCsvReader::new(path)
        .with_schema(Arc::new(schema))
        .with_parse_dates(true)
        .with_schema_modify(|schema| {
            let mut schema = schema.clone();
            schema.with_column("Discharge Capacity (Ah)".to_string(), DataType::Float64);
            schema.with_column("Discharge Energy (Wh)".to_string(), DataType::Float64);
            Ok(schema)
        })?
        .finish()?;

    lf = lf
        .with_column(lit(channel).alias("channel_number"))
        .with_column(lit("arbin").alias("cycler_type"))
        .select(&[
            col("*").exclude(["Date Time"]),
            col("Date Time")
                .str()
                .strptime(StrpTimeOptions {
                    date_dtype: DataType::Datetime(
                        TimeUnit::Milliseconds,
                        Some("Europe/Stockholm".to_string()),
                    ),
                    fmt: Some("%m/%d/%Y %T%.3f".to_string()),
                    strict: true,
                    exact: true,
                })
                .cast(DataType::Datetime(
                    TimeUnit::Milliseconds,
                    Some("Europe/Stockholm".to_string()),
                ))
                .alias("observed_at"),
        ])
        .select(&[
            col("*"),
            (col("observed_at").cast(DataType::Int64) / lit(1000) - lit(7200)).alias("timestamp"),
        ]);

    lf = calculate_step_capacities_and_types(lf);
    lf = lf.select(&[
        col("sequence_number"),
        ((col("Test Time (s)") * lit(1000.0)).cast(DataType::Int64)).alias("total_time_millis"),
        ((col("Step Time (s)") * lit(1000.0)).cast(DataType::Int64)).alias("step_time_millis"),
        col("voltage"),
        col("current"),
        col("power"),
        col("step_amp_hours"),
        col("step_charged_amp_hours"),
        col("step_discharged_amp_hours"),
        col("step_watt_hours"),
        col("step_charged_watt_hours"),
        col("step_discharged_watt_hours"),
        col("total_cycle"),
        col("step_number"),
        col("step_type"),
        col("step_id"),
        col("channel_number"),
        col("cycler_type"),
        col("timestamp"),
        // col("observed_at"), // TODO: this thing crashes.
    ]);
    Ok(lf)
}

fn calculate_step_capacities_and_types(lf: LazyFrame) -> LazyFrame {
    let mut lf = lf;
    let reverse_cumsum = |lf: LazyFrame, name: &str| -> LazyFrame {
        lf.select(&[
            col("*").exclude([name]),
            col(name) - col(name).shift_and_fill(1, lit(0.0)),
        ])
    };

    lf = reverse_cumsum(lf, "Charge Capacity (Ah)");
    lf = reverse_cumsum(lf, "Discharge Capacity (Ah)");
    lf = reverse_cumsum(lf, "Charge Energy (Wh)");
    lf = reverse_cumsum(lf, "Discharge Energy (Wh)");

    lf.rename(&["Voltage (V)", "Current (A)"], &["voltage", "current"])
        .with_column((col("voltage") * col("current")).alias("power"))
        // the resulting groups become our step numbers
        .groupby_stable([col("Cycle Index"), col("Step Index")])
        .agg([
            col("*").exclude([
                "Data Point",
                "Charge Capacity (Ah)",
                "Discharge Capacity (Ah)",
                "Charge Energy (Wh)",
                "Discharge Energy (Wh)",
            ]),
            col("Data Point").alias("sequence_number"),
            // calculate cumulative energy and capacity per step
            col("Charge Capacity (Ah)")
                .cumsum(false)
                .alias("step_charged_amp_hours"),
            col("Discharge Capacity (Ah)")
                .cumsum(false)
                .alias("step_discharged_amp_hours"),
            col("Charge Energy (Wh)")
                .cumsum(false)
                .alias("step_charged_watt_hours"),
            col("Discharge Energy (Wh)")
                .cumsum(false)
                .alias("step_discharged_watt_hours"),
            // calculate step types
            // current == 0
            when(col("current").first().eq(lit(0.0)))
                .then(lit("Rest"))
                // discharge steps, current < 0
                .when(col("current").first().lt(lit(0.0)))
                .then(
                    // check if current at beginning of step is lt 95% of current at end of step
                    when((col("current").first() * lit(0.95)).lt(col("current").last()))
                        .then(lit("CCCV_DChg"))
                        .otherwise(lit("CC_DChg")),
                )
                // charge steps
                .otherwise(
                    // check if current at beginning of step is gt 95% of current at end of step
                    when((col("current").first() * lit(0.95)).gt(col("current").last()))
                        .then(lit("CCCV_Chg"))
                        .otherwise(lit("CC_Chg")),
                )
                .alias("step_type"),
        ])
        // give each step a new step number
        .with_row_count("step_number", Some(1))
        .explode([col("*").exclude(["Cycle Index", "Step Index", "step_number", "step_type"])])
        .rename(&["Cycle Index", "Step Index"], &["total_cycle", "step_id"])
        .with_columns(vec![
            col("*"),
            (col("step_charged_amp_hours") + col("step_discharged_amp_hours"))
                .alias("step_amp_hours"),
            col("*"),
            (col("step_charged_watt_hours") + col("step_discharged_watt_hours"))
                .alias("step_watt_hours"),
        ])
}
