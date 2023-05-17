{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    cluster_by=['conso_date'],
    merge_exclude_columns=['conso_date','city_id','enedis_job_insert_at'],
    unique_key=['conso_date','city_id']
  )
}}

with enedis_consommation_structured as (
    select * from {{ ref('stg_enedis__consommation') }}
)

, fact_enedis_consommation as (
    select 
        conso_date
        , case city_id
            when '16182633862074' then 'Bordeaux'
            when '7449348688744' then 'Paris 8ème'
            when '7448769817520' then 'Paris 5ème'
            when '7334876946936' then 'Breteuil'
            else city_id
        end as city_id
        , lead(h_offpeak_supplier) over(partition by city_id order by conso_date)-h_offpeak_supplier      as h_offpeak_supplier
        , lead(h_peak_supplier) over(partition by city_id order by conso_date)-h_peak_supplier            as h_peak_supplier
        , lead(total_sum) over(partition by city_id order by conso_date)-total_sum                        as total_sum
        , sysdate()                                         as enedis_job_insert_at
        , sysdate()                                         as enedis_job_modify_at   
    from
        enedis_consommation_structured
)

select 
    conso_date
    , max(case when city_id like 'Paris%' then 'Paris' else city_id end) as city_id
    , sum(h_offpeak_supplier) as h_offpeak_supplier
    , sum(h_peak_supplier) as h_peak_supplier
    , sum(total_sum) as total_sum
    , max(enedis_job_insert_at) as enedis_job_insert_at
    , max(enedis_job_modify_at) as enedis_job_modify_at
from fact_enedis_consommation
group by conso_date, case when city_id like 'Paris%' then 'Paris' else city_id end
order by conso_date
