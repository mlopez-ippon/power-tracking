-- The total energy must be equal to the sum of the ernegy supplied and to the sum of the energy distributed.
-- Therefore return records where this isn't true to make the test fail
select
    conso_date
    , city
    , (h_offpeak_supplier+h_peak_supplier)  as total_h_supplied
    , (h_offpeak_high_season_distributor+h_peak_high_season_distributor+h_offpeak_low_season_distributor+h_peak_low_season_distributor)
    as total_h_distributed
    , total_sum
from 
    {{ ref('stg_enedis__consommation' )}}
where
    total_h_supplied != total_sum or total_h_distributed != total_sum