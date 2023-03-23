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
{% set status_labels = ["Bureau","Absence"] %}

, semana_aggregation as (
    select
        b.reservation_date
        , p.parent_practice_name
        , dayname(b.reservation_date)       as day_of_the_week
        {% for period in periods %}
        {% for status_label in status_labels %}
        , sum(count_if(b.period like '{{period}}' and b.status_label like '{{status_label}}')) over(partition by b.reservation_date, p.parent_practice_name) as {{period}}_{{status_label}}_collaborators
        {% endfor %}
        {% endfor%}
        {% for period in periods %}
        , sum(count_if(b.period like '{{period}}' and b.status_label like 'Télétravail')) over(partition by b.reservation_date, p.parent_practice_name) as {{period}}_Teletravail_collaborators
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