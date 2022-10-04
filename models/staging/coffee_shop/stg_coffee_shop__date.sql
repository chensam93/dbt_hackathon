select
    _file,
    _line,
    transaction_date,
    date_id,
    week_id,
    week_name,
    month_id,
    month_name,
    quarter_id,
    quarter_name,
    year_id,
    _fivetran_synced
from {{ source('coffee_shop', 'date') }}