{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    cluster_by=['capture_time'],
    merge_exclude_columns=['capture_time','station_name','meteo_job_insert_at'],
    unique_key=['capture_time','station_name']
  )
}}

with meteo_structured as (
    select * from {{ ref('stg_meteo__meteo') }}
)

, fact_meteo_meteo as (
    select 
        capture_time
        , station_name
        , temperature_celsius
        , humidity
        , min_temperature_last_12_hours_celsius
        , max_temperature_last_12_hours_celsius
        , snow_cover_height
        , latitude
        , longitude
        , epci_name
        , month
        , sysdate()                                         as meteo_job_insert_at
        , sysdate()                                         as meteo_job_modify_at   
    from
        meteo_structured
)

select * from fact_meteo_meteo