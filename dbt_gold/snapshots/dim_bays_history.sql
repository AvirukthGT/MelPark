-- This model lets you travel back in time.
-- You can join this to your Fact table on:
-- bay_id AND fact.arrival_time BETWEEN valid_from AND valid_to

with snapshot_data as (
    select * from {{ ref('bays_snapshot') }}
)

select
    bay_id,
    description,
    latitude,
    longitude,
    dbt_valid_from as valid_from,
    -- If valid_to is null, it means it's the Current Row. 
    -- We replace NULL with a far-future date for easier SQL joins.
    coalesce(dbt_valid_to, cast('9999-12-31' as timestamp)) as valid_to,
    
    case 
        when dbt_valid_to is null then true 
        else false 
    end as is_current_record

from snapshot_data