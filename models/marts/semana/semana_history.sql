with practices as (
    select * from {{ ref('dim_semana_practices') }}
)

, collaborators as (
    select * from {{ ref('dim_semana_collaborators') }}
)

, bookings as (
    select * from {{ ref('fact_semana_bookings') }}
)

{% set periods = ["am", "pm", "day"] %}
{% set type_res = ["office","remote","off"] %}

, semana_aggregation as (
    select
        b.reservation_date
        , p.parent_practice_name
        , dayname(b.reservation_date)       as day_of_the_week
        {% for period in periods %}
        {% for type in type_res %}
        , sum(count_if(b.period like '{{period}}' and b.type_res like '{{type}}')) over(partition by b.reservation_date, p.parent_practice_name) as {{period}}_{{type}}_collaborators
        {% endfor %}
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