with enedis_aggregation as (
    select * from {{ ref('enedis_history') }}
)

, tasmota_aggregation as (
    select * from {{ ref('tasmota_history') }}
)

, energy_aggregation as (
    select
    t1.date,
    t1.day_of_week,
    t1.agence,
    t1.max_value_of_day
FROM
    (SELECT
        tasmota_aggregation.date,
        tasmota_aggregation.day_of_week,
        'Nantes' AS agence,
        tasmota_aggregation.max_value_of_day
    FROM
        tasmota_aggregation
    UNION ALL
    SELECT
        enedis_aggregation.conso_date AS date,
        enedis_aggregation.day_of_week,
        'Paris' AS agence,
        enedis_aggregation.total_sum AS total_energy_today
    FROM
        enedis_aggregation
    WHERE
        enedis_aggregation.city_id = 'Paris'
    UNION ALL
    SELECT
        enedis_aggregation.conso_date AS date,
        enedis_aggregation.day_of_week,
        'Bordeaux' AS agence,
        enedis_aggregation.total_sum AS total_energy_today
    FROM
        enedis_aggregation
    WHERE
        enedis_aggregation.city_id = 'Bordeaux') t1
    qualify 
        row_number() over (partition by date, agence order by t1.date desc) = 1
ORDER BY
    t1.date
)

select * from energy_aggregation