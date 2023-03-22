{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    cluster_by=['reservation_date','bookings_id'],
    merge_exclude_columns=['collaborator_id','reservation_date'],
    unique_key=['collaborator_id','reservation_date']
  )
}}

select
    bookings_id
    , reservation_date
    , period
    , dateadd(hour, datediff(hour, getutcdate(), getdate()), created_at)    as created_at_France_bookings
    , status_label
    , collaborator_id     
from
    {{ ref('stg_semana__bookings') }}