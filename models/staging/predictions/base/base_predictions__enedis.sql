with source as (
    {{ mockable_source('input_enedis','POWER_TRACKING_SCHEMA','PREDICT_ENEDIS') }}
) 

, renamed as (
    select 
        index::date as date
        , agency::string as agency
        ,  max_value_of_day::float as max_value_of_day
    from 
        source
    order by 
        date
)

select * from renamed 