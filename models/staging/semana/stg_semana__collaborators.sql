with source as (
    select * from {{ ref('base_semana__collaborators') }}
)

, semana_collaborators_structured as (
    select 
        collaborator_id
        , role
        , created_at
        , community_id
    from 
        source
)

select * from semana_collaborators_structured