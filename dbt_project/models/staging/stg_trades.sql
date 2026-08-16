-- Reads trade ledger Parquet from bronze (source=fidelity/, source=vanguard/),
-- Combined via source=* glob. Dedupes cross-file/cross-run using row_hash,

with source as (
    select *
    from read_parquet(
        's3://{{ env_var("WEALTH_LAKEHOUSE_BUCKET") }}/bronze/trades/source=*/year_month=*/*.parquet',
        hive_partitioning=true,
        union_by_name=true)
),

standardized as (
    select
        cast(trade_date as date) as trade_date,
        account,
        action,
        symbol,
        currency,
        cast(price as decimal(18, 4)) as price,
        cast(quantity as decimal(18, 6)) as quantity,
        cast(amount as decimal(18, 2)) as amount,
        source_account_hint,
        source,
        row_hash,
        ingested_at
    from source
),

deduped as (
    select *
    from (
        select *, row_number() over(partition by row_hash order by ingested_at asc) as rn
        from standardized
    )
    where rn = 1
)

select
    trade_date,
    account,
    action,
    symbol,
    currency,
    price,
    quantity,
    amount,
    source_account_hint,
    source,
    row_hash
from deduped