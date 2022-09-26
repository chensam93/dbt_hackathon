select
    customer_id,
    home_store_id,
    full_name,
    first_name,
    last_name,
    email,
    loyalty_card_number,
    birthdate,
    gender,
    customer_since,
    phone,
    street,
    city,
    state,
    zip_code,
    county
from {{ source('coffee_shop', 'customer') }}