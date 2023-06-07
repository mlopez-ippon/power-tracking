with source as (
    {{ mockable_source('input_tasmota','FEATURE_SILVER','FCT_TASMOTA_ENERGY') }}
) 

select * from source