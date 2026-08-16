-- Gold layer: monthly spend by category, across all bank sources (Amex + Chase).
-- Only counts spend (positive amount, per bronze's normalized sign convention:
-- positive = money out/charge, negative = credit/payment/refund). Payments
-- and refunds are excluded so this reflects actual spending, not net card activity.

with spend_only as (
    select
        transaction_date,
        date_trunc('month', transaction_date) as month,
        category_raw,
        source,
        amount
    from {{ ref('stg_transactions') }}
    where amount > 0   -- exclude payments/credits/refunds
),

by_category as (
    select
        month,
        coalesce(category_raw, 'Uncategorized') as category,
        sum(amount) as total_spent,
        count(*) as transaction_count
    from spend_only
    group by 1, 2
),

by_month_total as (
    select
        month,
        sum(amount) as total_spent_month,
        count(*) as transaction_count_month
    from spend_only
    group by 1
)

select
    c.month,
    c.category,
    c.total_spent,
    c.transaction_count,
    m.total_spent_month,
    round(c.total_spent / nullif(m.total_spent_month, 0) * 100, 1) as pct_of_month_spend
from by_category c
join by_month_total m
    on c.month = m.month
order by c.month, c.total_spent desc
