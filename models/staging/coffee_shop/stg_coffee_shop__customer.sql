select
    _line,
    customer_id,
    home_store_id,
    customer_name,
    customer_first_name,
    customer_last_name,
    customer_email,
    loyalty_card_number,
    customer_birthdate,
    customer_gender,
    customer_birth_year,
    customer_age,
    customer_since,
    customer_phone,
    customer_street,
    customer_city,
    customer_state,
    customer_zip_code,
    customer_county,
    cc_number,
    _fivetran_synced
from {{ source('coffee_shop', 'customer') }}