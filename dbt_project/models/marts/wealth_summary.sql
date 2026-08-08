-- Gold layer: one row per day with running cash balance, portfolio market
-- value, total net worth, and day-over-day ROI.
-- TODO(you): adjust the starting cash balance assumption to your reality.

with daily_cash_flow as (
    select
        transaction_date,
        sum(amount) as day_amount
    from {{ ref('stg_transactions') }}
    group by 1
),

running_cash_balance as (
    select
        transaction_date,
        sum(day_amount) over (
            order by transaction_date
            rows between unbounded preceding and current row
        ) as cash_balance
    from daily_cash_flow
),

running_holdings as (
    select
        trade_date,
        ticker,
        sum(case when side = 'buy' then shares else -shares end) over (
            partition by ticker
            order by trade_date
            rows between unbounded preceding and current row
        ) as shares_held
    from {{ ref('stg_trades') }}
),

-- one row per (date, ticker) with the running share count as of that date
holdings_by_day as (
    select
        p.price_date,
        h.ticker,
        h.shares_held
    from {{ ref('stg_stock_prices') }} p
    left join running_holdings h
        on h.ticker = p.ticker
        and h.trade_date <= p.price_date
    qualify row_number() over (
        partition by p.price_date, h.ticker
        order by h.trade_date desc
    ) = 1
),

portfolio_value_by_day as (
    select
        p.price_date,
        sum(h.shares_held * p.close_price) as portfolio_value
    from {{ ref('stg_stock_prices') }} p
    left join holdings_by_day h
        on h.ticker = p.ticker
        and h.price_date = p.price_date
    group by 1
),

combined as (
    select
        coalesce(c.transaction_date, v.price_date) as as_of_date,
        c.cash_balance,
        v.portfolio_value,
        coalesce(c.cash_balance, 0) + coalesce(v.portfolio_value, 0) as net_worth
    from running_cash_balance c
    full outer join portfolio_value_by_day v
        on c.transaction_date = v.price_date
),

with_roi as (
    select
        as_of_date,
        cash_balance,
        portfolio_value,
        net_worth,
        lag(net_worth) over (order by as_of_date) as prior_net_worth,
        round(
            (net_worth - lag(net_worth) over (order by as_of_date))
            / nullif(lag(net_worth) over (order by as_of_date), 0) * 100,
            2
        ) as day_over_day_roi_pct
    from combined
)

select * from with_roi
order by as_of_date
