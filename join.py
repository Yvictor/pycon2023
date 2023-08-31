import polars as pl

base = pl.scan_parquet("data/daily/base/*y.parquet")
ref_price = pl.scan_parquet("data/daily/refprice/all.parquet")
funds = pl.scan_parquet("data/daily/funds/all.parquet")
query = base.with_columns(
    pl.col("Code").str.strip(" ")
).filter(
    pl.col("Code").str.n_chars()==4
).join(ref_price.with_columns(
    pl.col("Code").str.strip(" ")
    ), on=["Code", "Date"], how="left"
).join(funds.with_columns(
    pl.col("Code").str.strip(" ")
    ), on=["Code", "Date"], how="left"
).with_columns(
    pl.col("Date").dt.month().alias("Month")
)
query.collect()

