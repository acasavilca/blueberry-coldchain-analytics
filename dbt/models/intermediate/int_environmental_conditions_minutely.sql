{{ config(
    materialized='view',
) }}

with telemetry_minutely as (
    select
        plant_id,
        timezone_id,
        fruit_type,
        measured_at,
        timestamp_trunc(measured_at, hour) as measured_at_hour,
        loaded_at,
        temp_room_c,
        temp_pulp_c,
        rh_room_pct,
        co2_ppm,
        o2_pct,
        power_compressor_kw,
        compressor_on,
        humidifier_on,
        fruit_mass_stored_kg
    from {{ ref('stg_telemetry') }}
),

satellite_hourly as (
    select
        location_id,
        measured_at,
        temp_2m_c as temp_ambient_c,
        rh_2m_pct as rh_ambient_pct
    from {{ ref('stg_satellite') }}
)

select
    tel.plant_id,
    tel.timezone_id,
    tel.fruit_type,
    tel.measured_at,
    tel.loaded_at,
    tel.temp_room_c,
    tel.temp_pulp_c,
    tel.rh_room_pct,
    tel.co2_ppm,
    tel.o2_pct,
    tel.power_compressor_kw,
    tel.compressor_on,
    tel.humidifier_on,
    tel.fruit_mass_stored_kg,
    sat.temp_ambient_c,
    sat.rh_ambient_pct
from telemetry_minutely as tel
left join satellite_hourly as sat
    on tel.plant_id = sat.location_id
    and tel.measured_at_hour = sat.measured_at
    