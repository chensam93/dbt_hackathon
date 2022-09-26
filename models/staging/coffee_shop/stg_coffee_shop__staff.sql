select
    staff_id,
    first_name,
    last_name,
    position,
    store_id,
    start_date,
    email,
    phone,
    street,
    city,
    state,
    zip_code,
    county
from {{ source('coffee_shop', 'staff') }}