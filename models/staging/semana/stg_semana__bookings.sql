with semana_bookings as (
    select * from {{ ref('base_semana__bookings') }}
)

, semana_bookings_structured as (
    select
        booking_id
        , reservation_date
        , period
        , created_at
        , floor_name
        , type_res
        , collaborator_id                                 
    from
        semana_bookings 
    qualify row_number() over (partition by reservation_date, collaborator_id order by created_at desc) = 1
)

select * from semana_bookings_structured