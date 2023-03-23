{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    cluster_by=['reservation_date','booking_id'],
    merge_exclude_columns=['collaborator_id','reservation_date'],
    unique_key=['collaborator_id','reservation_date']
  )
}}

select
    booking_id
    , reservation_date
    , period
    , convert_timezone('UTC','Europe/Paris',created_at::timestamp_ntz)    as created_at_France_bookings
    , status_label
    , collaborator_id     
from
    {{ ref('stg_semana__bookings') }}