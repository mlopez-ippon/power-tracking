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
        , lead(h_offpeak_supplier) over(partition by city order by conso_date)-h_offpeak_supplier      as h_offpeak_supplier
        , lead(h_peak_supplier) over(partition by city order by conso_date)-h_peak_supplier            as h_peak_supplier
        , lead(total_sum) over(partition by city order by conso_date)-total_sum                        as total_sum
        , sysdate()                                         as enedis_job_insert_at
        , sysdate()                                         as enedis_job_modify_at   
    from
        enedis_consommation_structured
)

select * from fact_enedis_consommation