{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    cluster_by='community_id',
    merge_exclude_columns='collaborator_id',
    unique_key='collaborator_id'
  )
}}

select 
    md5(concat(coalesce(cast(c.collaborator_id as string),'_this_used_to_be_null_'),coalesce(cast(p.community_id as string),'_this_used_to_be_null_'))) as _surrogate_key
    , c.collaborator_id
    , c.role
    , c.status
    , convert_timezone('UTC','Europe/Paris',c.created_at::timestamp_ntz)    as created_at_France_collaborators
    , c.community_id
from 
    {{ ref('stg_semana__collaborators') }} c
left join 
    {{ ref('dim_semana_practices') }} p
on 
    c.community_id=p.community_id