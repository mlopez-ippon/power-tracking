{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    cluster_by='city_parent_id',
    merge_exclude_columns=['community_id','semana_job_insert_at'],
    unique_key='community_id'
  )
}}

with semana_communities_structured as (
    select * from {{ ref('stg_semana__communities') }}
)

, semana_collaborators_structured as (
    select * from {{ ref('stg_semana__collaborators') }}
)

, dim_semana_practices as (
    select distinct 
        p.community_id
        , p.practice_name
        , a.latitude                                                                                                            
        , a.longitude                                               
        , ifnull(p.city_parent_id,p.community_id)                                 as city_parent_id
        , ifnull(p2.practice_name,p.practice_name)                                as agency
        , count_if(c.status like 'enabled') over(partition by c.community_id)     as nb_collaborators
        , convert_timezone('UTC','Europe/Paris',p.created_at::timestamp_ntz)      as created_at_France_practices
        , sysdate()                                                               as semana_job_insert_at
        , sysdate()                                                               as semana_job_modify_at 
    from 
        semana_communities_structured p
    left join 
        semana_communities_structured p2
    on 
        p.city_parent_id=p2.community_id
    left join
        semana_collaborators_structured c
    on
        p.community_id=c.community_id
    left join
        {{ ref('stg_semana__agencies') }} a
    on  
        p.practice_name=a.agency_city
    where
        agency not like 'Moscou' 
        and agency not like 'Melbourne'
        and agency not like 'IpponUSA'
        and agency not like 'Coworkers'
        and agency not like 'Guests'
    order by 
        agency
)

select * from dim_semana_practices