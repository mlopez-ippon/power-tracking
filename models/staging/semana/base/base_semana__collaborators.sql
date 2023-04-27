with source as (
    {{ mockable_source('input_collaborators','POWER_TRACKING_SCHEMA','COLLABORATORS') }}
)

, renamed as (
    select
        id::number(38,0)                                as collaborator_id
        --, name::string                                  as name
        , role::string                                  as role
        --, email::string                                 as email
        , status::string                                as status
        , createdat::timestamp                          as created_at
        , externalid::string                            as external_id
        , communityid::number(38,0)                     as community_id
        , lastloginat::timestamp                        as last_login_at
        , provisioned::boolean                          as provisioned
        , _airbyte_ab_id::string                        as ingestion_id
        , _airbyte_emitted_at::timestamp                as inserted_timestamp
        , _airbyte_normalized_at::timestamp             as normalisation_timestamp
        , _airbyte_collaborators_hashid::string         as hash_id
    from 
        source
    order by
        created_at
)

select * from renamed