select
    {{ dbt_utils.generate_surrogate_key(['bay_id', 'rule_number']) }} as restriction_key,
    bay_id,
    rule_number,
    description as rule_description,
    duration as max_minutes,
    start_time,
    end_time,
    day_range,
    on_public_hols,
    type_desc,
    current_timestamp() as load_timestamp
from {{ source('melpark', 'parking_restrictions') }}