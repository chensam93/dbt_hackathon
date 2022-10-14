select
    transaction_date,
    date_id,
    week_id,
    week_name,
    month_id,
    month_name,
    quarter_id,
    quarter_name,
    year_id
from {{ source('coffee_shop', 'date') }}