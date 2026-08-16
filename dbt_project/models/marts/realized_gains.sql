-- Gold layer: realized gains from closed positions (sells only), filterable
-- by symbol/source/date range at query time.

select
    trade_date,
    symbol,
    source,          -- 'fidelity' or 'vanguard' — filter on this per-brokerage
    account,
    quantity as shares_sold,
    price as sale_price,
    amount as proceeds,
    realized_gain
from {{ ref('position_lots') }}
where action ilike '%sold%'
order by trade_date
