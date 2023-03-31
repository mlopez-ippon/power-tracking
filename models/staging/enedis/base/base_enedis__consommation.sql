with source as (
    select * from {{ ref('simu_enedis') }}
) 

, renamed as (
    select
        Horodate::date         as conso_date
        , Type_de_releve::string    as statement_type
        , Ville::string             as city
        , EAS_F1::number(38,0)      as h_offpeak_supplier
        , EAS_F2::number(38,0)      as h_peak_supplier
        , EAS_F3::number(38,0) 
        , EAS_F4::number(38,0) 
        , EAS_F5::number(38,0) 
        , EAS_F6::number(38,0) 
        , EAS_F7::number(38,0) 
        , EAS_F8::number(38,0) 
        , EAS_F9::number(38,0) 
        , EAS_F10::number(38,0)
        , EAS_D1::number(38,0)      as h_offpeak_low_season_distributor
        , EAS_D2::number(38,0)      as h_peak_low_season_distributor
        , EAS_D3::number(38,0)      as h_offpeak_high_season_distributor
        , EAS_D4::number(38,0)      as h_peak_high_season_distributor
        , EAS_T::number(38,0)       as total_sum
    from
        source
)

select * from renamed