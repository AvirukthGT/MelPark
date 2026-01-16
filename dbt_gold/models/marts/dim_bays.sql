with bays as (
    -- Updated to match the clean name in sources.yml
    select * from {{ source('melpark', 'parking_bays') }}
),

zones as (
    -- Updated to match the clean name in sources.yml
    select * from {{ source('melpark', 'parking_zones_plates') }}
)

select
    b.bay_id,
    b.description as bay_description,
    b.latitude,
    b.longitude,
    current_timestamp() as load_timestamp
from bays b
-- logic to join zones would go here eventually