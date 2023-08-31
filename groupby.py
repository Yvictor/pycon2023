import polars as pl

load_plan = pl.scan_parquet(
  "data/stkv2/stkv2_202301*.parquet"
)
query_plan = load_plan.filter(
  pl.col("Simtrade")==0
).with_columns(
    pl.col("Date").dt.combine(
      pl.col("Time")
    ).dt.truncate("1m").alias("Datetime")
).groupby("Code", "Datetime").agg([
    pl.col("Close").first().alias("Open"),
    pl.col("Close").max().alias("High"),
    pl.col("Close").min().alias("Low"),
    pl.col("Close").last().alias("Close"),
    pl.col("Volume").sum().alias("Volume")
]).sort("Code", "Datetime")

query_plan.collect()

