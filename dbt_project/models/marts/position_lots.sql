-- Running cost basis + realized gain per trade, using the average cost method. 
-- Requires a recursive CTE because cost_basis after a sell depends on cost_basis before it
--
-- Average cost, not FIFO

with recursive trade_rows as (
    select
        row_number() over (partition by symbol, source order by trade_date, amount) as rn,
        trade_date,
        symbol,
        source,
        account,
        action,
        quantity,
        price,
        amount
    from {{ ref('stg_trades') }}
    where action ilike '%bought%' or action ilike '%sold%' or action ilike '%reinvestment%'
),
 
lots as (
    -- base case: first trade per (symbol, source)
    select
        t.rn, t.trade_date, t.symbol, t.source, t.account, t.action, t.quantity, t.price, t.amount,
        cast(case when t.action ilike '%bought%' or t.action ilike '%reinvestment%' then t.quantity else 0 end as decimal(18, 6)) as shares_held,
        cast(case when t.action ilike '%bought%' or t.action ilike '%reinvestment%' then t.quantity * t.price else 0 end as decimal(18, 2)) as cost_basis,
        cast(0.0 as decimal(18, 2)) as realized_gain
    from trade_rows t
    where t.rn = 1
 
    union all
 
    -- recursive step: each trade builds on the prior trade's running state
    select
        t.rn, t.trade_date, t.symbol, t.source, t.account, t.action, t.quantity, t.price, t.amount,
        cast(case
            when t.action ilike '%bought%' or t.action ilike '%reinvestment%' then l.shares_held + t.quantity
            when t.action ilike '%sold%' then l.shares_held - t.quantity
            else l.shares_held
        end as decimal(18, 6)) as shares_held,
        cast(case
            when t.action ilike '%bought%' or t.action ilike '%reinvestment%' then l.cost_basis + (t.quantity * t.price)
            when t.action ilike '%sold%' then l.cost_basis - (t.quantity * (l.cost_basis / nullif(l.shares_held, 0)))
            else l.cost_basis
        end as decimal(18, 2)) as cost_basis,
        cast(case
            when t.action ilike '%sold%' then t.amount - (t.quantity * (l.cost_basis / nullif(l.shares_held, 0)))
            else 0.0
        end as decimal(18, 2)) as realized_gain
    from trade_rows t
    join lots l
        on l.symbol = t.symbol
        and l.source = t.source
        and l.rn = t.rn - 1
)
 
select
    rn,
    trade_date,
    symbol,
    source,
    account,
    action,
    quantity,
    price,
    amount,
    round(shares_held, 6) as shares_held,
    round(cost_basis, 2) as cost_basis,
    round(realized_gain, 2) as realized_gain
from lots
order by symbol, source, trade_date