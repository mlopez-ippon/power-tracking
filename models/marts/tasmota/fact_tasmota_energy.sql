{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    cluster_by=['measure_time'],
    merge_exclude_columns=['measure_time','tasmota_job_insert_at'],
    unique_key=['measure_time']
  )
}}

with total_energy as (
    select * from {{ ref('stg_tasmota__energy') }}
)

, fact_tasmota_energy as (
    select
        date(measure_time) as date
        , dayname(date) as day_of_week
        , dayofweek(date) as day_week_number
        , measure_time
        , current_current
        , current_load
        , current_power
        , total_energy_today * 1000 as total_energy_today
        , sysdate()                                         as tasmota_job_insert_at
        , sysdate()                                         as tasmota_job_modify_at 
    from 
        total_energy
)

select * from fact_tasmota_energy