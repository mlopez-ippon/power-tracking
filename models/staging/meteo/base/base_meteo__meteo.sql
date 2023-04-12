with source as (
    {{ mockable_source('input_meteo','METEO_SCHEMA','METEO') }}
)

, renamed as (
    select
        t::float     as temperature_kelvin
        , tc::float    as temperature_celsius
        , u::number(38,0)     as humidity
        , date::timestamp     as capture_time
        , nom::string         as station_name
        , dd::number(38,0)     as wind_direction
        , ff::float     as wind_speed
        , td::float    as dew_point
        , vv::float    as horizontal_visibility
        , ww::string          as present_weather
        , per::float   as wind_gust_measurement_period
        , rr1::float   as rainfall_1_last_hour
        , rr3::float   as rainfall_3_last_hours
        , rr6::float   as rainfall_6_last_hours
        , hbas::float  as cloud_height
        , nbas::string        as cloudiness
        , pmer::number(38,0)  as sea_level_pressure
        , pres::float  as station_pressure
        , rr12::float   as rainfall_12_last_hours
        , rr24::float   as rainfall_24_last_hours
        , tend::float  as pressure_variation_last_3_hours
        , tn12::float  as min_temperature_last_12_hours_kelvin
        , tx12::float  as max_temperature_last_12_hours_kelvin
        , raf10::float as wind_gust_last_10_minutes
        , tn12c::float  as min_temperature_last_12_hours_celsius
        , tx12c::float  as max_temperature_last_12_hours_celsius
        , libgeo::string      as city
        , rafper::float as wind_gust_one_period
        , tend24::float as pressure_variation_last_24_hours
        , codegeo::string      as postal_code
        , hnuage1::float as base_height_1
        , nnuage1::float as base_cloudiness_1
        , nom_reg::string       as region_name
        , tminsol::float as min_ground_temperature_in_12_hours_kelvin
        , altitude::number(38,0) as altitude
        , cod_tend::string      as type_of_barometric_trend
        , code_dep::string      as code_department
        , code_reg::string      as code_region
        , etat_sol::string      as ground_state
        , ht_neige::float      as snow_cover_height
        , latitude::float as latitude
        , nom_dept::string      as department_name
        , nom_epci::string      as epci_name
        , tminsolc::float as min_ground_temperature_in_12_hours_celsius
        , code_epci::string      as epci_code
        , longitude::float as longitude
        , numer_sta::string       as numero_station
        , coordonnees             as coordinates
        , temps_present::string   as temps_present
        , mois_de_l_annee::number(38,0) as month
        , type_de_tendance_barometrique::string as barometric_trend_type
        , _airbyte_ab_id::string                    as ingestion_id
        , _airbyte_emitted_at::timestamp            as inserted_timestamp
        , _airbyte_normalized_at::timestamp         as normalisation_timestamp
        , _airbyte_meteo_hashid::string          as hash_id
        
    from 
        source
    order by
        capture_time
)

select * from renamed 