with predictions_aggregation as (
    select * from {{ ref('predictions_history') }}
)

, semana_aggregation as (
    select * from {{ ref('semana_history') }}
)

, predictsemana_aggregation as (
    select
        reservation_date
        , agency
        , day_office_collaborators
        , predictions
        , 1 as priority
    from 
        predictions_aggregation
    union all
    select
        reservation_date
        , agency
        , day_office_collaborators
        , 'no' as predictions
        , 2 as priority
    from 
        semana_aggregation
)



select * from predictsemana_aggregation order by reservation_date desc 