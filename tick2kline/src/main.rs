use clap::Parser;
use polars::prelude::*;

/// tick2kline is the tool to convert tick data to kline data
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Path pattern to the tick data file
    #[arg(short, long)]
    scan_path: String,
    /// Path to the output kline data file
    #[arg(short, long, default_value = "data/kline.parquet")]
    output_path: String,
}

fn tick2kline(scan_path: &str, output_path: &str) {
    let load_plan = LazyFrame::scan_parquet(scan_path, Default::default()).unwrap();
    let query_plan = load_plan
        .filter(col("Simtrade").eq(lit(0)))
        .groupby([
            col("Code"),
            col("Date")
                .dt()
                .combine(col("Time"), TimeUnit::Milliseconds)
                .dt()
                .truncate(TruncateOptions {
                    every: "1m".to_owned(),
                    offset: "1m".to_owned(),
                    use_earliest: None,
                })
                .alias("Datetime"),
        ])
        .agg([
            col("Close").first().alias("Open"),
            col("Close").max().alias("High"),
            col("Close").min().alias("Low"),
            col("Close").last().alias("Close"),
            col("Volume").sum().alias("Volume"),
        ])
        .sort_by_exprs(
            vec![col("Code"), col("Datetime")],
            vec![false, false],
            false,
            false,
        );
    match query_plan.collect() {
        Ok(mut df) => {
            let mut f = std::fs::File::create(output_path).unwrap();
            ParquetWriter::new(&mut f)
                .finish(&mut df)
                .unwrap();
            println!("df: {:?}", df);
            println!("tick2kline output_path: {:?}", output_path);
        }
        Err(e) => println!("error: {:?}", e),
    }
}

fn main() {
    let args = Args::parse();
    println!("tick2kline scan_path: {:?}", args.scan_path);
    tick2kline(&args.scan_path, &args.output_path);
}
