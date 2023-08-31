## polars version
import polars as pl

# %%time
base = pl.scan_parquet("data/daily/base/all.parquet")
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


## pandas version
import pandas as pd

# %%time
df_base = pd.read_parquet("data/daily/base/all.parquet")
df_ref_price = pd.read_parquet("data/daily/refprice/all.parquet")
df_funds = pd.read_parquet("data/daily/funds/all.parquet")

df_base["Code"] = df_base["Code"].map(lambda x: x.strip(" "))
df_ref_price["Code"] = df_ref_price["Code"].map(lambda x: x.strip(" "))
df_funds["Code"] = df_funds["Code"].map(lambda x: x.strip(" "))

df_base = df_base[df_base["Code"].map(len) == 4]

df_base = df_base.set_index(["Date", "Code"])
df_ref_price = df_ref_price.set_index(["Date", "Code"])
df_funds = df_funds.set_index(["Date", "Code"])

df_base.join(
    df_ref_price, 
    on=["Date", "Code"], how="left"
).join(
    df_funds,
    on=["Date", "Code"], how="left"
)
