select
    _line,
    store_id,
    store_type,
    store_square_feet,
    store_address,
    store_city,
    store_state,
    store_telephone,
    store_postal_code,
    store_latitude,
    store_longitude,
    store_coordinates,
    store_manager_id,
    _fivetran_synced
from {{ source('coffee_shop', 'store') }}