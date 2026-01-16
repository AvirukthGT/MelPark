-- This flattens your Star Schema into one wide table for easy analysis
with sessions as (
    select * from {{ ref('fact_parking_sessions') }}
),

bays as (
    select * from {{ ref('dim_bays') }}
)

select
    s.session_id,
    s.arrival_time,
    s.departure_time,
    s.duration_minutes,
    
    -- Bay Details
    b.bay_description,
    b.latitude,
    b.longitude,
    
    -- Derived Metrics (Good for charts)
    date_trunc('hour', s.arrival_time) as arrival_hour,
    case 
        when s.duration_minutes < 15 then 'Short Stay'
        when s.duration_minutes < 60 then 'Medium Stay'
        else 'Long Stay'
    end as stay_category

from sessions s
left join bays b on s.bay_id = b.bay_id