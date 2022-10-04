select
    _file,
    _line,
    store_id,
    year_month,
    beans_goal,
    beverage_goal,
    food_goal,
    merchandise_goal,
    total_goal,
    _fivetran_synced
from {{ source('coffee_shop', 'sales_targets') }}