with predictions_predict as (
    select * from {{ ref('base_predictions__semana') }}
)

, predict as (
    select 
        date
        , agency
        , case when day_office_collaborators < 0 then 0 else 
            round(day_office_collaborators)
            end as day_office
    from
        predictions_predict
    qualify 
        row_number() over (partition by date, agency order by date desc) = 1
    order by 
        date
)

select * from predict