with tasmota_energy as (
    select * from {{ ref('base_tasmota__energy') }}
)

, total_energy as (
    select 
        id_meter
        , measure_time
        , current_current
        , current_load
        , current_power
        , total_energy_today
    from 
        tasmota_energy
    qualify
        row_number() over(partition by measure_time order by measure_time desc) = 1

)

select * from total_energy where substring(measure_time, 14, 3) = ':00' order by measure_time