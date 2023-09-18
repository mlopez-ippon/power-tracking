with predictions_semana as (
    select * from {{ ref('fact_predictions_semana') }}
)

, predictions_enedis as (
    select * from {{ ref('fact_predictions_enedis') }}
)

, predictions_aggregation as (
    select
        date(s.date) as reservation_date
        , s.agency
        , s.day_office as day_office_collaborators
        , e.conso_energy as max_value_of_day
        , case when s.date <= current_date() then 'no' 
            else s.predictions 
            end as predictions
    from 
        predictions_semana s
    join 
        predictions_enedis e
    on 
        s.date = e.date and s.agency = e.agency
)

select * from predictions_aggregation order by reservation_date