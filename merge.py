import polars as pl

load_plan_202301 = pl.scan_parquet("data/stkv2/stkv2_202301*.parquet")
load_plan_202302 = pl.scan_parquet("data/stkv2/stkv2_202302*.parquet")
load_plan = pl.concat([load_plan_202301, load_plan_202302], how="vertical")


