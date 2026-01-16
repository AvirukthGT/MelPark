select
    meter_id,
    asset_id,
    has_credit_card,
    has_tap_and_go,
    description as meter_description,
    latitude,
    longitude,
    current_timestamp() as load_timestamp
from {{ source('melpark', 'parking_meters') }}