with meteo as (
    select * from {{ ref('fact_meteo_meteo') }}
)


, meteo_aggregation as(
    select
        date
        , capture_time
        , city
        , station_name
        , temperature_celsius
        , humidity
        , min_temperature_last_12_hours_celsius
        , max_temperature_last_12_hours_celsius
        , snow_cover_height
        , latitude
        , longitude
        , month
    from
        meteo
) 


select 
    date(capture_time) as date,
    city,
    avg(temperature_celsius) as average_temperature,
    avg(humidity) as average_humidity
from meteo_aggregation
group by date(capture_time), city