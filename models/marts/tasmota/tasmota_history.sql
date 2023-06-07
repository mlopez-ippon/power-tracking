with tasmota as (
    select * from {{ ref('fact_tasmota_energy') }}
)

, tasmota_aggregation as (
    select 
        measure_time
        , case when lag(total_energy_today, 1) over (order by measure_time) is null then 0
               when total_energy_today - lag(total_energy_today, 1, 0) over (order by measure_time) < 0 then 0
               else total_energy_today - lag(total_energy_today, 1, 0) over (order by measure_time)
        end as energy_per_hour
    from 
        tasmota
)

select * from tasmota_aggregation