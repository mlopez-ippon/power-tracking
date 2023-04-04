{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    cluster_by=['conso_date'],
    merge_exclude_columns=['conso_date','city','enedis_job_insert_at'],
    unique_key=['conso_date','city']
  )
}}

with enedis_consommation_structured as (
    select * from {{ ref('stg_enedis__consommation') }}
)

, fact_enedis_consommation as (
    select 
        conso_date
        , city
        , ifnull(h_offpeak_supplier,0)              as h_offpeak_supplier
        , ifnull(h_peak_supplier,0)                 as h_peak_supplier
        , ifnull(total_sum,0)                       as total_sum
        , sysdate()                                 as enedis_job_insert_at
        , sysdate()                                 as enedis_job_modify_at   
    from
        enedis_consommation_structured
)

select * from fact_enedis_consommation