with enedis_consommation as (
    select * from {{ ref('base_enedis__consommation') }}
) 

, enedis_consommation_structured as (
    select 
        conso_date
        , city_id
        , ifnull(h_offpeak_supplier,0)              as h_offpeak_supplier
        , ifnull(h_peak_supplier,0)                 as h_peak_supplier
        , h_offpeak_low_season_distributor
        , h_peak_low_season_distributor
        , h_offpeak_high_season_distributor
        , h_peak_high_season_distributor
        , ifnull(total_sum,0)                       as total_sum
    from 
        enedis_consommation
    qualify
        row_number() over(partition by conso_date, city_id order by conso_date desc) = 1
)

select * from enedis_consommation_structured WHERE city_id <> '7334876946936'
