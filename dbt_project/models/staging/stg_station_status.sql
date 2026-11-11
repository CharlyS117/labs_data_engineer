with source as (
    select * from {{ source('mobility_raw', 'raw_station_status') }}  
),
cleaned as (
    select 
        station_id,
        num_bikes_available::integer as bikes_available,
        coalesce(num_bikes_available::integer, 0) as ebikes_available,
        num_docks_available::integer as docks_available,
        (is_installed = 1) as is_installed,
        (is_renting = 1) as is_renting,
        (is_returning = 1) as is_returning,
        to_timestamp(last_reported) as last_reported_at,
        extracted_at::timestamp as extracted_at,
        coalesce(
            round(
                (num_bikes_available::numeric / nullif(num_bikes_available + num_docks_available, 0)) * 100,
                2
            ),
            0.0
        ) as occupancy_rate
    from source
)

select * from cleaned