with practices as (
    select * from {{ ref('dim_semana_practices') }}
)

, collaborators as (
    select * from {{ ref('dim_semana_collaborators') }}
)

, bookings as (
    select * from {{ ref('fact_semana_bookings') }}
)

, unset as (
    select 
        city_parent_id
        , sum(nb_collaborators) as nb
    from 
        practices 
    group by 
        city_parent_id
)

, coordinates as (
    select
        practice_name
        , latitude
        , longitude
    from
        practices
    where
        community_id=city_parent_id
)

{% set type_res = ["office","remote","off"] %}

, cte_group_by as (
    select
        b.reservation_date
        , p.agency
        , dayname(b.reservation_date)       as day_of_the_week
        {% for type in type_res %}
        , (sum(count_if(b.period like 'day' and b.type_res like '{{type}}')) over(partition by b.reservation_date, p.agency)
        + sum(count_if(b.period not like 'day' and b.type_res like '{{type}}')/2) over(partition by b.reservation_date, p.agency))
        as day_{{type}}_collaborators
        {% endfor%}
        , (u.nb-day_office_collaborators-day_remote_collaborators-day_off_collaborators)  
        as day_unset_collaborators
    from 
        bookings b
    inner join 
        collaborators c
    on 
        b.collaborator_id=c.collaborator_id
    inner join 
        practices p
    on 
        p.community_id=c.community_id
    inner join 
        unset u 
    on 
        p.city_parent_id=u.city_parent_id
    group by
        b.reservation_date, p.agency, u.nb
    order by 
        b.reservation_date desc, p.agency
)

, semana_aggregation as (
    select
        cte.reservation_date
        , cte.day_of_the_week
        , cte.agency
        , c.latitude
        , c.longitude
        , cte.day_office_collaborators
        , cte.day_remote_collaborators
        , cte.day_off_collaborators  
        , cte.day_unset_collaborators
    from 
        cte_group_by cte
    inner join 
        coordinates c
    on 
        cte.agency=c.practice_name
)

select * from semana_aggregation