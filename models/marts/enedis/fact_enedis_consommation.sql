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

, cte_date_changed as (
    select
        dateadd(day,1,conso_date)       as conso_date_1day
        , city
        , h_offpeak_supplier
        , h_peak_supplier
        , total_sum
    from
        enedis_consommation_structured
)

, fact_enedis_consommation as (
    select 
        c.conso_date
        , c.city
        , (c.h_offpeak_supplier-cte.h_offpeak_supplier)     as h_offpeak_supplier
        , (c.h_peak_supplier-cte.h_peak_supplier)           as h_peak_supplier
        , (c.total_sum-cte.total_sum)                       as total_sum
        , sysdate()                                         as enedis_job_insert_at
        , sysdate()                                         as enedis_job_modify_at   
    from
        enedis_consommation_structured c
    inner join 
        cte_date_changed cte
    on 
        c.conso_date=cte.conso_date_1day and c.city=cte.city
)

select * from fact_enedis_consommation