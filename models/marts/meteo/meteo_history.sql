with meteo as (
    select * from {{ ref('fact_meteo_meteo') }}
)

, meteo_aggregation as(
    select
        capture_time
        , station_name
        , temperature_celsius
        , humidity
        , min_temperature_last_12_hours_celsius
        , max_temperature_last_12_hours_celsius
        , snow_cover_height
        , latitude
        , longitude
        , epci_name
        , month
    from
        meteo
) 

select * from meteo_aggregation