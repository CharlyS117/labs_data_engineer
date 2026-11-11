with staging as (
    select * from {{ ref('stg_station_status') }}
)

select
    station_id,
    bikes_available,
    docks_available,
    occupancy_rate,
    last_reported_at,
    extracted_at,
    case
        when not is_renting or not is_returning then 'OUT_OF_SERVICE'
        when occupancy_rate <= 10.0 then 'CRITICAL_LOW_STOCK'
        when occupancy_rate >= 90.0 then 'CRITICAL_HIGH_CONGESTION'
        else 'HEALTHY_STOCK'
    end as operational_status
from staging