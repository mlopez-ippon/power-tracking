with source as (
    {{ mockable_source('input_bookings','POWER_TRACKING_SCHEMA','BOOKINGS') }}
    {% if var('is_backload') %} 
        where 
            _airbyte_normalized_at >= dateadd(day, {{ var('nb_backload_days') }} , sysdate())
    {% elif is_prod() %}
        where 
            _airbyte_normalized_at >= dateadd(day, -1, sysdate())
    {% endif %}
)

, renamed as (
    select
        id::number(38,0)                            as booking_id
        , date::date                                as reservation_date
        , type::string                              as type_res
        , period::string                            as period
        , statusid::string                          as status_id
        , createdat::timestamp                      as created_at
        , floorname::string                         as floor_name
        , statuslabel::string                       as status_label
        , collaboratorid::number(38,0)              as collaborator_id
	    , _airbyte_ab_id::string                    as ingestion_id
        , _airbyte_unique_key::string               as airbyte_key
        , _airbyte_emitted_at::timestamp            as inserted_timestamp
        , _airbyte_normalized_at::timestamp         as normalisation_timestamp
        , _airbyte_bookings_hashid::string          as hash_id
    from
        source
    order by 
        reservation_date
        , floor_name
)

select * from renamed