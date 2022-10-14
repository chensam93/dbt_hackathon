select
    _line,
    transaction_id,
    transaction_date,
    transaction_time,
    store_id,
    staff_id,
    customer_id,
    product_id,
    instore_yn,
    "ORDER" as order_number,
    line_item_id,
    quantity,
    line_item_amount,
    unit_price,
    promo_item_yn,
    _fivetran_synced
from {{ source('coffee_shop', 'sales_receipts') }}