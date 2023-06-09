with semana_aggregation as (
    select * from {{ ref('semana_history') }}
)

, energy_aggregation as (
    select * from {{ ref('energy_history') }}
)

, meteo_aggregation as (
    select * from {{ ref('meteo_history') }}
)
, final_aggregation as (
    select
        s.reservation_date
        , s.day_of_the_week
        , s.agency as AGENCY
        , s.latitude
        , s.longitude
        , e.max_value_of_day
        , s.day_office_collaborators
        , s.day_remote_collaborators
        , s.day_off_collaborators  
        , s.day_unset_collaborators
        , m.average_temperature
        , m.average_humidity
    from 
        semana_aggregation s
    left join
        energy_aggregation e
    on 
        s.reservation_date=e.date and s.agency=e.agence
    left join
        meteo_aggregation m
    on
        s.reservation_date = m.date and s.agency=m.city
)

select * from final_aggregation where reservation_date > '2022-01-01' order by reservation_date