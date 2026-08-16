-- Reads bank transaction Parquet from bronze (source=amex/, source=chase/)
-- Combined via source=* glob. Dedupes cross-file/cross-run using row_hash

with source as (
    select *
    from read_parquet(
        's3://{{ env_var("WEALTH_LAKEHOUSE_BUCKET") }}/bronze/bank_transactions/source=*/year_month=*/*.parquet',
        hive_partitioning=true,
        union_by_name=true)
),

standardized as (
    select
        cast(transaction_date as date) as transaction_date,
        description,
        cast(amount as decimal(18, 2)) as amount,
        category_raw,
        source_account_hint,
        source,
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
    transaction_date,
    description,
    amount,
    category_raw,
    source_account_hint,
    source,
    row_hash
from deduped
