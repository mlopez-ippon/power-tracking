with meteo_meteo as (
    select * from {{ ref('base_meteo__meteo') }}
)

, meteo_structured as (
    select
        capture_time
        , station_name
        , cast(round(temperature_celsius, 1) as decimal(10, 1)) as temperature_celsius
        , humidity
        , cast(round(min_temperature_last_12_hours_celsius, 1) as decimal(10, 1)) as min_temperature_last_12_hours_celsius
        , cast(round(max_temperature_last_12_hours_celsius, 1) as decimal(10, 1)) as max_temperature_last_12_hours_celsius
        , snow_cover_height
        , latitude
        , longitude
        , epci_name
        , month
        , normalisation_timestamp
    from
        meteo_meteo
    qualify 
        row_number() over (partition by capture_time, station_name order by normalisation_timestamp desc) = 1
)

select * from meteo_structured