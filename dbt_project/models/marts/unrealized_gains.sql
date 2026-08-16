-- Gold layer: current unrealized gains — latest held position per
-- (symbol, source) valued at the most recent available price, vs. its
-- running average cost basis

with latest_position as (
    select *
    from (
        select
            *,
            row_number() over (partition by symbol, source order by trade_date desc, rn desc) as latest_rn
        from {{ ref('position_lots') }}
    )
    where latest_rn = 1
    and shares_held > 0    -- exclude fully-closed positions (nothing held = nothing unrealized)
),

latest_price as (
    select
        symbol,
        close as current_price,
        price_date
    from (
        select
            *,
            row_number() over (partition by symbol order by price_date desc) as rn
        from {{ ref('stg_stock_prices') }}
    )
    where rn = 1
)

select
    p.symbol,
    p.source,
    p.account,
    p.shares_held,
    round(p.cost_basis, 2) as cost_basis,
    round(p.cost_basis / nullif(p.shares_held, 0), 4) as avg_cost_per_share,
    lp.current_price,
    lp.price_date as priced_as_of,
    round(p.shares_held * lp.current_price, 2) as market_value,
    round((p.shares_held * lp.current_price) - p.cost_basis, 2) as unrealized_gain
from latest_position p
left join latest_price lp
    on lp.symbol = p.symbol
order by unrealized_gain desc
