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
    collaborator_id
    , role
    , convert_timezone('UTC','Europe/Paris',created_at::timestamp_ntz)    as created_at_France_collaborators
    , community_id
from 
    {{ ref('stg_semana__collaborators') }}