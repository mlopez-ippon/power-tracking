{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    cluster_by='city_parent_id',
    merge_exclude_columns='community_id',
    unique_key='community_id'
  )
}}

select 
    p.community_id
    , p.practice_name
    , p.city_parent_id
    , ifnull(p2.practice_name,split(p.practice_name,'-')[0])                  as parent_practice_name   --problème 2 practices Marseille
    , convert_timezone('UTC','Europe/Paris',p.created_at::timestamp_ntz)      as created_at_France_practices
from 
    {{ ref('stg_semana__communities') }} p
left join 
    {{ ref('stg_semana__communities') }} p2
on 
    p.city_parent_id=p2.community_id
where
    parent_practice_name not like 'Moscou' 
    and parent_practice_name not like 'Melbourne'
    and parent_practice_name not like 'IpponUSA'
    and parent_practice_name not like 'Coworkers'
    and parent_practice_name not like 'Guests'

