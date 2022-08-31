use polars::prelude::*;

pub fn read(path: &str, measurement_id: &str) -> Result<LazyFrame> {
    // TODO: this should probably be validated somehow
    let measurement_id: Vec<&str> = measurement_id.split("-").collect();

    let mut lf = read_arbin_file(path)?;
    lf = lf
        .filter(col("Data Point").is_not_null())
        .filter(col("Data Type Flag").eq(lit(1)));
    lf = calculate_step_capacities_and_types(lf);
    lf = fix_durations(lf);
    lf = fix_datetime(lf);
    lf = lf.with_columns(vec![
        lit("arbin").alias("cycler_type"),
        lit(measurement_id[0]).alias("equipment_number"),
        lit(measurement_id[1]).alias("unit_number"),
        lit(measurement_id[2]).alias("channel_number"),
        lit(measurement_id[3]).alias("measurement_id"),
    ]);

    lf = lf.select(&[
        col("sequence_number"),
        col("total_time_millis"),
        col("step_time_millis"),
        col("voltage"),
        col("current"),
        col("power"),
        col("step_amp_hours"),
        col("step_charged_amp_hours"),
        col("step_discharged_amp_hours"),
        col("step_watt_hours"),
        col("step_charged_watt_hours"),
        col("step_discharged_watt_hours"),
        // col("cell_temperature1"), TODO: None
        // col("cell_temperature2"), TODO: None
        // col("cell_temperature3"), TODO: None
        // col("cell_auxiliary_voltage1"), TODO: None
        // col("cell_auxiliary_voltage2"), TODO: None
        // col("cell_auxiliary_voltage3"), TODO: None
        col("total_cycle"),
        col("step_number"),
        col("step_type"),
        col("step_id"),
        col("equipment_number"),
        col("unit_number"),
        col("channel_number"),
        col("measurement_id"),
        col("cycler_type"),
        // col("step_type_code"), TODO: None
        col("observed_at"),
        // col("recorded_at"), TODO: None
        // col("experiment_name"), TODO: None
    ]);

    Ok(lf)
}

fn read_arbin_file(path: &str) -> Result<LazyFrame> {
    let header_lines = header_lines(path)?;

    let fields = vec![
        Field::new("Byte Index", DataType::Utf8),
        Field::new("Line Number", DataType::Utf8),
        Field::new("Data Type Flag", DataType::UInt32),
        Field::new("Data Point", DataType::UInt32),
        Field::new("Test Time", DataType::Float64),
        Field::new("Step Time", DataType::Float64),
        Field::new("Cycle Index", DataType::UInt32),
        Field::new("Step Index", DataType::UInt32),
        Field::new("Current", DataType::Float64),
        Field::new("Voltage", DataType::Float64),
        Field::new("Charge Capacity", DataType::Float64),
        Field::new("Discharge Capacity", DataType::Float64),
        Field::new("Charge Energy", DataType::Float64),
        Field::new("Discharge Energy", DataType::Float64),
        Field::new("Data Flags", DataType::Utf8),
        Field::new("Data Time", DataType::Int64),
        Field::new("Other Information", DataType::Utf8),
    ];
    let schema = Schema::from(fields);

    LazyCsvReader::new(path.into())
        .with_skip_rows(header_lines - 2)
        .with_skip_rows_after_header(1)
        .with_parse_dates(true)
        .with_delimiter(b'\t')
        .with_schema(Arc::new(schema))
        .finish()
}

fn header_lines(path: &str) -> Result<usize> {
    let header = LazyCsvReader::new(path.into())
        .has_header(false)
        .with_n_rows(Some(8))
        .with_delimiter(b'\t')
        .finish()?;

    let mut df = header
        .filter(col("column_1").str().starts_with("Header Lines"))
        .collect()?;

    df.apply("column_2", |x| {
        x.utf8()
            .unwrap()
            .into_iter()
            .map(|l| l.map(|s| s.parse::<u32>().unwrap()))
            .collect::<UInt32Chunked>()
            .into_series()
    })?;

    let chunked_array = df.column("column_2")?.u32()?;
    let l = chunked_array.get(0).unwrap();
    Ok(l as usize)
}

fn calculate_step_capacities_and_types(lf: LazyFrame) -> LazyFrame {
    let mut lf = lf;
    let reverse_cumsum = |lf: LazyFrame, name: &str| -> LazyFrame {
        lf.select(&[
            col("*").exclude([name]),
            col(name) - col(name).shift_and_fill(1, lit(0.0)),
        ])
    };

    lf = reverse_cumsum(lf, "Charge Capacity");
    lf = reverse_cumsum(lf, "Discharge Capacity");
    lf = reverse_cumsum(lf, "Charge Energy");
    lf = reverse_cumsum(lf, "Discharge Energy");

    lf.rename(&["Voltage", "Current"], &["voltage", "current"])
        .with_column((col("voltage") * col("current")).alias("power"))
        // the resulting groups become our step numbers
        .groupby_stable([col("Cycle Index"), col("Step Index")])
        .agg([
            col("*").exclude([
                "Data Point",
                "Charge Capacity",
                "Discharge Capacity",
                "Charge Energy",
                "Discharge Energy",
            ]),
            col("Data Point").alias("sequence_number"),
            // calculate cumulative energy and capacity per step
            col("Charge Capacity")
                .cumsum(false)
                .alias("step_charged_amp_hours"),
            col("Discharge Capacity")
                .cumsum(false)
                .alias("step_discharged_amp_hours"),
            col("Charge Energy")
                .cumsum(false)
                .alias("step_charged_watt_hours"),
            col("Discharge Energy")
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

fn fix_durations(lf: LazyFrame) -> LazyFrame {
    lf.select(&[
        col("*").exclude(["Test Time", "Step Time"]),
        (col("Test Time") * lit(1000.0))
            .cast(DataType::Int32)
            .alias("total_time_millis"),
        (col("Step Time") * lit(1000.0))
            .cast(DataType::Int32)
            .alias("step_time_millis"),
    ])
}

/// Turn the Data Time column into a dtype datetime and change to column name to observed_at.
fn fix_datetime(lf: LazyFrame) -> LazyFrame {
    lf.select(&[
        col("*").exclude(["Data Time"]),
        // the timestamps has to be turned into proper nanosecond timestamps before
        // they can be converted to datetime
        (col("Data Time") * lit(100))
            .cast(DataType::Datetime(TimeUnit::Nanoseconds, None))
            .alias("observed_at"),
    ])
}
