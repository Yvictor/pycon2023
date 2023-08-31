import pandas as pd

df = pd.read_csv(
    "data.csv"
)[["col1", "col2"]]





df = pd.read_parquet(
  "folder"
).groupby(
  "Code"
).sum("Volume")



