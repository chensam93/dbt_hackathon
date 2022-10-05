select
    _file,
    _line,
    birth_year,
    generation,
    _fivetran_synced
from {{ source('coffee_shop', 'generation') }}