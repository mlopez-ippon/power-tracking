{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    cluster_by='city_parent_id',
    merge_exclude_columns='community_id',
    unique_key='community_id'
  )
}}

select distinct 
    p.community_id
    , p.practice_name
    , p.city_parent_id
    , ifnull(p2.practice_name,p.practice_name)                                as parent_practice_name
    , count_if(c.status like 'enabled') over(partition by c.community_id)     as nb_collaborators
    , convert_timezone('UTC','Europe/Paris',p.created_at::timestamp_ntz)      as created_at_France_practices
from 
    {{ ref('stg_semana__communities') }} p
left join 
    {{ ref('stg_semana__communities') }} p2
on 
    p.city_parent_id=p2.community_id
inner join
    {{ ref('stg_semana__collaborators') }} c
on
    p.community_id=c.community_id
where
    parent_practice_name not like 'Moscou' 
    and parent_practice_name not like 'Melbourne'
    and parent_practice_name not like 'IpponUSA'
    and parent_practice_name not like 'Coworkers'
    and parent_practice_name not like 'Guests'
order by 
    parent_practice_name
