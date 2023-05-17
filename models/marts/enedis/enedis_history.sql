with consommation as (
    select * from {{ ref('fact_enedis_consommation') }}
)

, enedis_aggregation as(
    select
        conso_date
        , dayname(conso_date) as day_of_week
        , city_id
        , h_offpeak_supplier
        , h_peak_supplier
    from
        consommation
) 

select * from enedis_aggregation
