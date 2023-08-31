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


query_plan.with_columns(
    pl.col("Close").rolling_mean(window_size=5).over("Code").alias("MA5")
)

load_plan.with_columns(
    pl.col("AskPrice").list.get(0).alias("BestAskPrice"),
    pl.col("BidPrice").list.get(0).alias("BestBidPrice"),
    pl.col("AskVolume").list.sum().alias("AskTotalVolume"),
    pl.col("BidVolume").list.sum().alias("BidTotalVolume"),
)



