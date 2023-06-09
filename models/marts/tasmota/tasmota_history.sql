with tasmota as (
    select * from {{ ref('fact_tasmota_energy') }}
)

, tasmota_aggregation as (
    select 
        t1.date
        , t1.day_of_week
        , t1.day_week_number
        , t1.measure_time
        , t1.energy_per_hour
        , t2.max_value as max_value_of_day
        , extract(hour from t1.measure_time) as hour_of_measure_time
        , 'Nantes' as agency
    from
        (select 
            date,
            day_of_week,
            day_week_number,
            measure_time,
            case
                when lag(total_energy_today, 1) over (order by measure_time) is null then 0
                when total_energy_today - lag(total_energy_today, 1, 0) over (order by measure_time) < 0 then 0
                else total_energy_today - lag(total_energy_today, 1, 0) over (order by measure_time)
            end as energy_per_hour
    from   
        tasmota) t1
    join
        (select
            date,
            max(total_energy_today) as max_value
    from
        tasmota
    group by date) t2 on t1.date = t2.date
)

select * from tasmota_aggregation