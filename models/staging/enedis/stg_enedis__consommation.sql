with enedis_consommation as (
    select * from {{ ref('base_enedis__consommation') }}
) 

, enedis_consommation_structured as (
    select 
        conso_date
        , city
        , h_offpeak_supplier
        , h_peak_supplier
        , h_offpeak_low_season_distributor
        , h_peak_low_season_distributor
        , h_offpeak_high_season_distributor
        , h_peak_high_season_distributor
        , total_sum
    from 
        enedis_consommation
)

select * from enedis_consommation_structured