-- Gold layer: rolled-up ROE across your whole portfolio, plus one row per brokerage (source) for comparison
-- investment_growth.sql is for the symbol level, this aggregates on top of it

with by_source as (
    select
        source,
        sum(total_realized_gain) as total_realized_gain,
        sum(current_cost_basis) as current_cost_basis,
        sum(current_market_value) as current_market_value,
        sum(unrealized_gain) as unrealized_gain,
        sum(total_gain) as total_gain
    from {{ ref('investment_growth') }}
    group by 1
),

overall as (
    select
        'ALL' as source,
        sum(total_realized_gain) as total_realized_gain,
        sum(current_cost_basis) as current_cost_basis,
        sum(current_market_value) as current_market_value,
        sum(unrealized_gain) as unrealized_gain,
        sum(total_gain) as total_gain
    from {{ ref('investment_growth') }}
)

select
    source,
    round(total_realized_gain, 2) as total_realized_gain,
    round(current_cost_basis, 2) as current_cost_basis,
    round(current_market_value, 2) as current_market_value,
    round(unrealized_gain, 2) as unrealized_gain,
    round(total_gain, 2) as total_gain,
    round(total_gain / nullif(current_cost_basis, 0) * 100, 2) as roe_pct
from (select * from overall union all select * from by_source)
order by case when source = 'ALL' then 0 else 1 end, source
