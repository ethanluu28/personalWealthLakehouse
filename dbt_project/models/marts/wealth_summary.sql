-- Gold layer: daily net worth = forward-filled cash balance + forward-filled
-- portfolio market value, plus day-over-day ROI
--
-- Cash balance is a running SUM of stg_transactions only, not added in yet
--
-- Forward-fill is needed because portfolio prices only exist on trading
-- days, but transactions can post any day (weekends/holidays included) —
-- without filling, those days would have a null portfolio_value.

with calendar as (
    select distinct transaction_date as d from {{ ref('stg_transactions') }}
    union
    select distinct price_date as d from {{ ref('stg_stock_prices') }}
),

daily_cash_flow as (
    select
        transaction_date,
        sum(amount) as day_amount
    from {{ ref('stg_transactions') }}
    group by 1
),

cash_running as (
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
        symbol,
        sum(
            case
                when action ilike '%bought%' or action ilike '%reinvestment%' then quantity
                when action ilike '%sold%' then -quantity
                else 0
            end
        ) over (
            partition by symbol
            order by trade_date
            rows between unbounded preceding and current row
        ) as shares_held
    from {{ ref('stg_trades') }}
    where action ilike '%bought%' or action ilike '%sold%' or action ilike '%reinvestment%'
),

holdings_by_day as (
    select
        p.price_date,
        p.symbol,
        h.shares_held
    from {{ ref('stg_stock_prices') }} p
    left join running_holdings h
        on h.symbol = p.symbol
        and h.trade_date <= p.price_date
    qualify row_number() over (
        partition by p.price_date, p.symbol
        order by h.trade_date desc
    ) = 1
),

portfolio_value_by_day as (
    select
        p.price_date as as_of_date,
        sum(coalesce(h.shares_held, 0) * p.close) as portfolio_value
    from {{ ref('stg_stock_prices') }} p
    left join holdings_by_day h
        on h.symbol = p.symbol
        and h.price_date = p.price_date
    group by 1
),

-- forward-fill: every calendar date picks up the most recent known
-- cash_balance / portfolio_value as of that date, so weekends/holidays
-- (which have no new price data) still carry a real number forward.
calendar_filled as (
    select
        c.d as as_of_date,
        last_value(cr.cash_balance ignore nulls) over (
            order by c.d rows between unbounded preceding and current row
        ) as cash_balance,
        last_value(pv.portfolio_value ignore nulls) over (
            order by c.d rows between unbounded preceding and current row
        ) as portfolio_value
    from calendar c
    left join cash_running cr on cr.transaction_date = c.d
    left join portfolio_value_by_day pv on pv.as_of_date = c.d
),

combined as (
    select
        as_of_date,
        coalesce(cash_balance, 0) as cash_balance,
        coalesce(portfolio_value, 0) as portfolio_value,
        coalesce(cash_balance, 0) + coalesce(portfolio_value, 0) as net_worth
    from calendar_filled
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