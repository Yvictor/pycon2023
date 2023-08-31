# tick2kline with rust

## new project

```
cargo new tick2kline
```

## add dependency
for cli args parse
```
cargo add clap --features derive
```
polars
```
cargo add polars -F lazy -F parquet -F temporal
```

## cargo build

```
cargo build --release
```

## run
```
cp ./target/release/tick2kline .

./tick2kline -s "../data/stkv2/stkv2_202207*.parquet" -o "../d
ata/kline202207M.parquet"
```