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
        date(capture_time) as date
        , capture_time
        , case epci_name
            when 'Bordeaux Métropole' then 'Bordeaux'
            when 'Métropole du Grand Paris' then 'Paris'
            when 'Eurométropole de Strasbourg' then 'Strasbourg'
            when 'Métropole Européenne de Lille' then 'Lille'
            when 'Nantes Métropole' then 'Nantes'
            when 'Tours Métropole Val de Loire' then 'Tours'
            when 'Toulouse Métropole' then 'Toulouse'
            when 'Métropole d''Aix-Marseille-Provence' then 'Marseille'
            when 'CC de l''Est Lyonnais (CCEL)' then 'Lyon'
            else epci_name
        end as city
        , station_name
        , temperature_celsius
        , humidity
        , min_temperature_last_12_hours_celsius
        , max_temperature_last_12_hours_celsius
        , snow_cover_height
        , latitude
        , longitude
        , month
        , sysdate()                                         as meteo_job_insert_at
        , sysdate()                                         as meteo_job_modify_at   
    from
        meteo_structured
    where
        date_part('hour', capture_time) IN (8,10,11,13,14,16,17,19)
)

select * from fact_meteo_meteo