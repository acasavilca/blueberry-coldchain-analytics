{{ config(
    materialized='table',
) }}

select
    plant_id,
    fruit_type,
    timestamp_trunc(datetime, hour) as date,

    avg(t_room) as avg_t_room,
    avg(t_pulp) as avg_t_pulp,
    avg(rh_room) as avg_rh_room,
    avg(co2_ppm) as avg_co2_ppm,
    avg(o2_pct) as avg_o2_pct,
    sum(w_compressor_kw/360) as compressor_energy_kwh,
    avg(cooling_call) as one_hour_duty_cycle_cooling,
    avg(humidifier_call) as one_hour_duty_cycle_humidifier,

from {{ ref('stg_telemetry') }}
group by 1, 2, 3
