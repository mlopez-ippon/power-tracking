{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    cluster_by='community_id',
    merge_exclude_columns='collaborator_id',
    unique_key='colaborator_id'
  )
}}

select 
    collaborator_id
    , role
    , dateadd(hour, datediff(hour, getutcdate(), getdate()), created_at)    as created_at_France_collaborators
    , community_id
from 
    {{ ref('stg_semana__collaborators') }}