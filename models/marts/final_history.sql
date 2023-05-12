with semana_aggregation as (
    select * from {{ ref('semana_history') }}
)

, enedis_aggregation as (
    select * from {{ ref('enedis_history') }}
)

, final_aggregation as (
    select
        s.reservation_date
        , s.day_of_the_week
        , s.agency
        , s.latitude
        , s.longitude
        , e.h_offpeak_supplier
        , e.h_peak_supplier
        , s.day_office_collaborators
        , s.day_remote_collaborators
        , s.day_off_collaborators  
        , s.day_unset_collaborators
        , m.average_temperature
        , m.average_humidity
    from 
        semana_aggregation s
    left join
        enedis_aggregation e
    on 
        s.reservation_date=e.conso_date and s.agency=e.city_id
    left join
        meteo_history m
    on
        s.reservation_date = m.date and s.agency=m.city
)

select * from final_aggregation where reservation_date > '2022-01-01' order by reservation_date