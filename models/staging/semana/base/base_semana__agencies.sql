with agencies as (
    select * from {{ ref('agencies') }}
)

, renamed as (
    select
        city::string                as agency_city
        , adress::string            as agency_adress
        , latitude::float           as latitude
        , longitude::float          as longitude
    from
        agencies
)

select * from renamed