with consommation as (
    select * from {{ ref('fact_enedis_consommation') }}
)

, enedis_aggregation as(
    select
        conso_date
        , city
        , h_offpeak_supplier
        , h_peak_supplier
    from
        consommation
) 

select * from enedis_aggregation