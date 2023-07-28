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
    select * from {{ ref('stg_predictions__enedis') }}
)

, fact_prediction_enedis as(
    select
        date
        , agency
        , conso_energy
        , 'yes'                                                                 as predictions
        , sysdate()                                                             as predictions_job_insert_at
        , sysdate()                                                             as predictions_job_modify_at   
    from
        predict_structured
)

select * from fact_prediction_enedis