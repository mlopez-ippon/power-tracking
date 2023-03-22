with source as (
    select * from {{ source('SEMANA_SCHEMA','COMMUNITIES') }}
)

, renamed as (
    select
        id::number(38,0)                            as community_id
        , name::string                              as name_practice
        , parentid::number(38,0)                    as city_parent_id
        , createdat::timestamp                      as created_at
        , _airbyte_ab_id::string                    as ingestion_id
        , _airbyte_emitted_at::timestamp            as inserted_timestamp
        , _airbyte_normalized_at::timestamp         as normalisation_timestamp
        , _airbyte_communities_hashid::string       as hash_id
    from 
        source
    order by
        name
) 

select * from renamed
