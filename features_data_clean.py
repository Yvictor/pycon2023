import polars as pl

dtypes = {
    "成交量": str,
    "到期月份(週別)": str,
    "結算價": str,
    "未沖銷契約數": str,
    "歷史最高價": str,
    "歷史最低價": str,
}

fill_back_cols = (
    pl.lit(None).cast(str).alias("是否因訊息面暫停交易"),
    pl.lit("一般").alias("交易時段"),
    pl.lit(None).cast(str).alias("價差對單式委託成交量"),
)

load_plan = pl.concat(
    [
        pl.scan_csv(
            "data/daily/futures/csv/200*.csv",
            dtypes=dtypes,
        ).with_columns(*fill_back_cols),
        pl.scan_csv(
            "data/daily/futures/csv/201[0-4]*.csv",
            dtypes=dtypes,
        ).with_columns(*fill_back_cols),
        pl.scan_csv(
            "data/daily/futures/csv/201[5-6]*.csv",
            dtypes=dtypes,
        ).with_columns(*fill_back_cols[1:]),
        pl.scan_csv("data/daily/futures/csv/201[7-9]*.csv", dtypes=dtypes),
        pl.scan_csv("data/daily/futures/csv/202*.csv", dtypes=dtypes),
        pl.scan_csv("data/daily/futures/csv/[M]*.csv", dtypes=dtypes),
    ]
)

float_convert_cols = [
    ("開盤價", "Open"),
    ("最高價", "High"),
    ("最低價", "Low"),
    ("收盤價", "Close"),
]
int_convert_cols = [("成交量", "Volume"), ("未沖銷契約數", "OpenInterest")]
float_strip_convert_cols = [
    ("漲跌價", "Change"),
    ("漲跌%", "ChangePct"),
    ("結算價", "SettlePrice"),
    ("最後最佳買價", "LastBid"),
    ("最後最佳賣價", "LastAsk"),
    ("歷史最高價", "HistHigh"),
    ("歷史最低價", "HistLow"),
]

query_plan = load_plan.filter(pl.col("契約").is_not_null()).select(
    pl.col("交易日期").str.strptime(pl.Date, format="%Y/%m/%d").alias("Date"),
    *[
        pl.when(pl.col(col) == "-")
        .then(None)
        .otherwise(pl.col(col))
        .cast(pl.Float64)
        .alias(alias_name)
        for col, alias_name in float_convert_cols
    ],
    *[
        pl.when(pl.col(col).cast(str) == "-")
        .then(None)
        .otherwise(pl.col(col))
        .cast(pl.Int64)
        .alias(alias_name)
        for col, alias_name in int_convert_cols
    ],
    pl.col("契約").cast(pl.Categorical).alias("Contract"),
    pl.col("到期月份(週別)").str.strip(" ").cast(pl.Categorical).alias("DeliveryCate"),
    *[
        pl.when(pl.col(col) == "-")
        .then(None)
        .otherwise(pl.col(col).str.strip("%"))
        .cast(pl.Float64)
        .alias(alias_name)
        for col, alias_name in float_strip_convert_cols
    ],
    pl.when(pl.col("交易時段") == "一般")
    .then(pl.lit("Regular"))
    .otherwise(pl.lit("AfterHours"))
    .cast(pl.Categorical)
    .alias("TradingSession"),
)
