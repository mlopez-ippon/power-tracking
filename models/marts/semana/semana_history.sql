with practices as (
    select * from {{ ref('dim_semana_practices') }}
)

, collaborators as (
    select * from {{ ref('dim_semana_collaborators') }}
)

, bookings as (
    select * from {{ ref('fact_semana_bookings') }}
)

, semana_aggregation as (
    select
        b.reservation_date
        , datename(weekday,b.reservation_date)      as day_of_the_week
        , count_if(b.period like 'am' and b.status_label like 'office') over(partition by b.reservation_date) as morning_office_collaborators
        , count_if(b.period like 'pm' and b.status_label like 'office') over(partition by b.reservation_date) as afternoon_office_collaborators
        , count_if(b.period like 'day' and b.status_label like 'office') over(partition by b.reservation_date) as full_day_office_collaborators
    from 
        bookings b
    inner join 
        collaborators c
    on 
        b.collaborator_id=c.collaborator_id
    inner join 
        pratices p
    on 
        p.community_id=c.community_id
    order by 
        b.reservation_date desc
)

select * from semana_aggregation