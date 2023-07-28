with source as (
    {{ mockable_source('input_consommation','POWER_TRACKING_SCHEMA','TEST_PREDICT_SEMANA') }}
) 

, renamed as (
    select 
        index::date as date
        , agency::string as agency
        ,  day_office_collaborators::float as day_office_collaborators
    from 
        source
    order by 
        date
)

select * from renamed 