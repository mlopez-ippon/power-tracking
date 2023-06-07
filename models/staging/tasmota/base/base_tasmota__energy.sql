with source as (
    {{ mockable_source('input_meteo','FEATURE_SILVER','FCT_TASMOTA_ENERGY') }}
) 

select * from source