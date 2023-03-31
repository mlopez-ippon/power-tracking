{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    cluster_by=['reservation_date'],
    merge_exclude_columns=['booking_id','semana_job_insert_at'],
    unique_key=['booking_id']
  )
}}

select
    md5(concat(coalesce(cast(b.booking_id as string),'_this_used_to_be_null_'),coalesce(cast(c.collaborator_id as string),'_this_used_to_be_null_'))) as _surrogate_key
    , b.booking_id
    , b.reservation_date
    , b.period
    , convert_timezone('UTC','Europe/Paris',b.created_at::timestamp_ntz)    as created_at_France_bookings
    , b.type_res
    , b.collaborator_id
    , sysdate()                                                             as semana_job_insert_at
    , sysdate()                                                             as semana_job_modify_at     
from
    {{ ref('stg_semana__bookings') }} b
left join 
    {{ ref('dim_semana_collaborators') }} c
on 
    b.collaborator_id=c.collaborator_id