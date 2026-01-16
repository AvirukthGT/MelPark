with sensor_events as (
    select
        bay_id,
        status, 
        cast(event_time as timestamp) as event_time
    from {{ source('melpark', 'parking_sensors') }}
),

status_pairs as (
    select
        bay_id,
        status as start_status,
        event_time as start_time,
        -- Look ahead to find when the status changed
        lead(status) over (partition by bay_id order by event_time) as next_status,
        lead(event_time) over (partition by bay_id order by event_time) as end_time
    from sensor_events
)

select
    {{ dbt_utils.generate_surrogate_key(['bay_id', 'start_time']) }} as session_id,
    bay_id,
    start_time as arrival_time,
    end_time as departure_time,
    
    -- Calculate Duration in Minutes
    round(
        (cast(end_time as double) - cast(start_time as double)) / 60, 
    2) as duration_minutes,
    
    current_timestamp() as load_timestamp

from status_pairs
-- UPDATED LOGIC:
-- A "Session" starts when a car is 'Present' 
-- and ends when the next recorded status is 'Unoccupied'
where start_status = 'Present'  
  and next_status = 'Unoccupied'
  and end_time is not null