with source as (
    select * from {{ ref('base_semana__communities') }}
)

, semana_communities_structured as (
    select
        community_id
        , practice_name
        , city_parent_id
        , created_at
    from
        source
)

select * from semana_communities_structured