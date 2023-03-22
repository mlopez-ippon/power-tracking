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
    community_id
    , name_practice
    , city_parent_id
    , dateadd(hour, datediff(hour, getutcdate(), getdate()), created_at)    as created_at_France_practices
from 
    {{ ref('stg_semana__communities') }}

