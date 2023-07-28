{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    cluster_by=['date'],
    merge_exclude_columns=['date','agency','predictions_job_insert_at'],
    unique_key=['date','agency']
  )
}}

with predict_structured as (
    select * from {{ ref('stg_predictions__semana') }}
)

, fact_prediction_semana as(
    select
        date
        , agency
        , day_office
        , 'yes'                                                                 as predictions
        , sysdate()                                                             as predictions_job_insert_at
        , sysdate()                                                             as predictions_job_modify_at   
    from
        predict_structured
)

select * from fact_prediction_semana