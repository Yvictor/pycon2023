import io
import polars as pl

query_plan = pl.scan_parquet("data/daily/base/*y.parquet").select(
    pl.col(["Date", "Code", "Open", "High", "Low", "Close", "Volume"])
)
query_plan_json = query_plan.serialize()

query_plan = pl.LazyFrame.deserialize(io.StringIO(query_plan_json))

df_sample = query_plan.fetch()

df, df_profile = query_plan.profile()


