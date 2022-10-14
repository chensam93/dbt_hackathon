select
    _line,
    sales_outlet_id as store_id,
    product_id,
    transaction_date,
    start_of_day,
    quantity_sold,
    waste,
    _waste as waste_pct,
    _fivetran_synced
from {{ source('coffee_shop', 'pastry_inventory') }}