with practices as (
    select * from {{ ref('dim_semana_practices') }}
)

, collaborators as (
    select * from {{ ref('dim_semana_collaborators') }}
)

, bookings as (
    select * from {{ ref('fact_semana_bookings') }}
)

{% set type_res = ["office","remote","off"] %}

, semana_aggregation as (
    select
        b.reservation_date
        , p.parent_practice_name
        , dayname(b.reservation_date)       as day_of_the_week
        {% for type in type_res %}
        , (sum(count_if(b.period like 'day' and b.type_res like '{{type}}')) over(partition by b.reservation_date, p.parent_practice_name)
        + sum(count_if(b.period not like 'day' and b.type_res like '{{type}}')/2) over(partition by b.reservation_date, p.parent_practice_name))
        as day_{{type}}_collaborators
        {% endfor%}
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
    group by
        b.reservation_date, p.parent_practice_name
    order by 
        b.reservation_date desc, p.parent_practice_name
)

select * from semana_aggregation