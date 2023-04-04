{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    cluster_by='community_id',
    merge_exclude_columns=['collaborator_id','semana_job_insert_at'],
    unique_key='collaborator_id'
  )
}}

with semana_communities_structured as (
    select * from {{ ref('stg_semana__communities') }}
)

, semana_collaborators_structured as (
    select * from {{ ref('stg_semana__collaborators') }}
)

, dim_semana_collaborators as (
    select 
        md5(concat(coalesce(cast(c.collaborator_id as string),'_this_used_to_be_null_'),coalesce(cast(p.community_id as string),'_this_used_to_be_null_'))) as _surrogate_key
        , c.collaborator_id
        , c.role
        , c.status
        , convert_timezone('UTC','Europe/Paris',c.created_at::timestamp_ntz)    as created_at_France_collaborators
        , c.community_id
        , sysdate()                                                             as semana_job_insert_at
        , sysdate()                                                             as semana_job_modify_at 
    from 
        semana_collaborators_structured c
    left join 
        semana_communities_structured p
    on 
        c.community_id=p.community_id
)

select * from dim_semana_collaborators