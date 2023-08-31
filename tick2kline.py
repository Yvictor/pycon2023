## polars version
import polars as pl

# %%time
load_plan = pl.scan_parquet(
    "data/stkv2/stkv2_202207*.parquet"
)
query_plan = load_plan.filter(
    pl.col("Simtrade")==0
).groupby("Code", 
    pl.col("Date").dt.combine(
        pl.col("Time")
    ).dt.truncate("1m").alias("Datetime")
).agg([
    pl.col("Close").first().alias("Open"),
    pl.col("Close").max().alias("High"),
    pl.col("Close").min().alias("Low"),
    pl.col("Close").last().alias("Close"),
    pl.col("Volume").sum().alias("Volume")
]).sort("Code", "Datetime")

# %%time
query_plan.collect()



## pandas version
import datetime
import pandas as pd

# %%time
df = pd.read_parquet("data/stkv2m/M202207")
df = df[df["Simtrade"]==0].copy()


# %%time
df["Datetime"] = pd.to_datetime(
    [datetime.datetime.combine(d, t) 
     for d, t in zip(df["Date"], df["Time"])]
)

# %%time
df["Datetime"] = df["Datetime"].dt.floor("1min")
df = df.set_index(["Code", "Datetime"]).sort_index()
df.groupby(["Code", "Datetime"]).agg(
    Open=pd.NamedAgg(column="Close", aggfunc="first"),
    High=pd.NamedAgg(column="Close", aggfunc="max"),
    Low=pd.NamedAgg(column="Close", aggfunc="min"),
    Close=pd.NamedAgg(column="Close", aggfunc="last"),
    Volume=pd.NamedAgg(column="Volume", aggfunc="sum"),
)


