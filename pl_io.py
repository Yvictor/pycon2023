import polars as pl

query = pl.scan_csv(
    "data.csv"
).select(
    pl.col(["col1", "col2"])
)
df = query.collect()


query = pl.scan_parquet(
    "path*.parquet"
).groupby("Code").agg(
    pl.col("Volume").sum()
)
df = query.collect()

