-- Reads daily stock price Parquet from bronze, partitioned by full trading
-- date (date=YYYY-MM-DD), not year_month. Single source (yfinance), so no
-- source=* glob needed. Dedupes cross-run using row_hash, since bronze only
-- validates (null close, missing tickers, date bounds) and does not dedupe

with source as (
    select *
    from read_parquet(
        's3://{{ env_var("WEALTH_LAKEHOUSE_BUCKET") }}/bronze/stock_prices/year_month=*/*.parquet',
        hive_partitioning=true,
        union_by_name=true)
),

standardized as (
    select
        cast(price_date as date) as price_date,
        symbol,
        cast(open as decimal(18, 4)) as open,
        cast(high as decimal(18, 4)) as high,
        cast(low as decimal(18, 4)) as low,
        cast(close as decimal(18, 4)) as close,
        cast(adj_close as decimal(18, 4)) as adj_close,
        cast(volume as bigint) as volume,
        currency,
        row_hash,
        ingested_at
    from source
),

deduped as (
    select *
    from (
        select
            *,
            row_number() over (partition by row_hash order by ingested_at asc) as rn
        from standardized
    )
    where rn = 1
)

select
    price_date,
    symbol,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    currency,
    row_hash
from deduped
