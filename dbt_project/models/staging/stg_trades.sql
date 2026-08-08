-- Reads the buy/sell trade ledger from bronze.
-- TODO(you): rename source columns to match your ledger's actual format.

with source as (
    select *
    from read_csv_auto('s3://{{ env_var("WEALTH_LAKEHOUSE_BUCKET") }}/bronze/trades/dt=*/*.csv', union_by_name=true)
)

select
    cast(trade_date as date) as trade_date,
    ticker,
    side,                               -- 'buy' or 'sell'
    cast(shares as decimal(18, 6)) as shares,
    cast(price_per_share as decimal(18, 4)) as price_per_share
from source
