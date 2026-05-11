{{ config(
    materialized='view',
) }}

select
    plant_id,
    fruit_type,
    timestamp_trunc(measured_at, hour) as measured_hour_at,
    timestamp_trunc(measured_at_localtime, hour) as measured_at_localtime,
    timestamp_trunc(loaded_at, hour) as loaded_at,

    avg(temp_room_c) as t_room_c,
    avg(temp_pulp_c) as t_pulp_c,
    avg(rh_room_pct) as rh_room_pct,
    avg(co2_ppm) as co2_ppm,
    avg(o2_pct) as o2_pct,
    sum(power_compressor_kw/360) as compressor_energy_kwh,
    avg(power_compressor_kw) as power_compressor_kw,
    avg(temp_evap_inlet_c) as t_evap_inlet_c,
    avg(temp_evap_outlet_c) as t_evap_outlet_c,
    avg(rh_evap_inlet_pct) as rh_evap_inlet_pct,
    avg(rh_evap_outlet_pct) as rh_evap_outlet_pct,
    avg(airflow_evap_kg_s) as airflow_evap_kg_s,
    avg(temp_coil_suction_c) as t_coil_suction_c,

    avg(compressor_on) as one_hour_duty_cycle_compressor,
    avg(humidifier_on) as one_hour_duty_cycle_humidifier,

from {{ ref('stg_telemetry') }}
group by 1, 2, 3, 4, 5
order by measured_hour_at