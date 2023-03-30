with source as (
    select * from {{ ref('base_semana__agencies') }}
)

, semana_agencies_structured as (
    select
        agency_city
        , agency_adress
        , latitude
        , longitude
    from
        source
)

select * from semana_agencies_structured