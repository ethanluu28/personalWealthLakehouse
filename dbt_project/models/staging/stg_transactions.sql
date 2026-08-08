-- Reads raw bank export CSVs from bronze, standardizes columns, and dedupes
-- using a hash of the natural key (date + amount + description).
-- TODO(you): rename source columns to match your bank's actual CSV headers.

with source as (
    select *
    from read_csv_auto('s3://{{ env_var("WEALTH_LAKEHOUSE_BUCKET") }}/bronze/expenses/dt=*/*.csv', union_by_name=true)
),

standardized as (
    select
        cast(transaction_date as date) as transaction_date,
        description,
        cast(amount as decimal(18, 2)) as amount,
        category,
        md5(concat_ws('|', transaction_date, amount, description)) as txn_hash
    from source
),

deduped as (
    select *
    from (
        select
            *,
            row_number() over (partition by txn_hash order by transaction_date) as rn
        from standardized
    )
    where rn = 1
)

select
    transaction_date,
    description,
    amount,
    category,
    txn_hash
from deduped
