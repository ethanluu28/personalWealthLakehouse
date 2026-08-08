-- Reads raw daily-partitioned Parquet from bronze, standardizes types.
-- TODO(you): point the read_parquet glob at your real bucket name.

with source as (
    select *
    from read_parquet('s3://{{ env_var("WEALTH_LAKEHOUSE_BUCKET") }}/bronze/stock_prices/dt=*/*.parquet')
),

cleaned as (
    select
        ticker,
        cast(close_price as decimal(18, 4)) as close_price,
        cast(price_date as date) as price_date
    from source
)

select * from cleaned
