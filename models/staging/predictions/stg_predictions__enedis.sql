with predictions_predict as (
    select * from {{ ref('base_predictions__enedis') }}
)

, predict as (
    select 
        date
        , agency
        , case when max_value_of_day < 0 then 0 
            else round(max_value_of_day)
            end as conso_energy
    from
        predictions_predict
    qualify 
        row_number() over (partition by date, agency order by date desc) = 1
    order by 
        date
)

select * from predict