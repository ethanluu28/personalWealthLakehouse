-- Gold layer: total investment growth per (symbol, source) — realized gains to date + current unrealized gains
-- Actual ROE signal, separate from cash contributions/deposits

with realized_by_position as (
    select
        symbol,
        source,
        sum(realized_gain) as total_realized_gain
    from {{ ref('realized_gains') }}
    group by 1, 2
),

unrealized_by_position as (
    select
        symbol,
        source,
        cost_basis as current_cost_basis,
        market_value,
        unrealized_gain
    from {{ ref('unrealized_gains') }}
)

select
    coalesce(r.symbol, u.symbol) as symbol,
    coalesce(r.source, u.source) as source,
    coalesce(r.total_realized_gain, 0) as total_realized_gain,
    coalesce(u.current_cost_basis, 0) as current_cost_basis,
    coalesce(u.market_value, 0) as current_market_value,
    coalesce(u.unrealized_gain, 0) as unrealized_gain,
    coalesce(r.total_realized_gain, 0) + coalesce(u.unrealized_gain, 0) as total_gain,
    round(
        (coalesce(r.total_realized_gain, 0) + coalesce(u.unrealized_gain, 0))
        / nullif(coalesce(u.current_cost_basis, 0), 0) * 100,
        2
    ) as roe_pct
from realized_by_position r
full outer join unrealized_by_position u
    on r.symbol = u.symbol
    and r.source = u.source
order by total_gain desc
