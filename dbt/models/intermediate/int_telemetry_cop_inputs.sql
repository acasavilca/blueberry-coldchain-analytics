{{ config(
    materialized='view',
) }}

with hourly_telemetry as (
    select
        plant_id,
        fruit_type,
        measured_hour_at,
        loaded_at,
        power_compressor_kw,
        compressor_energy_kwh,
        t_evap_inlet_c,
        t_evap_outlet_c,
        rh_evap_inlet_pct,
        rh_evap_outlet_pct,
        airflow_evap_kg_s,
        one_hour_duty_cycle_compressor,
        fruit_mass_stored_kg

    from {{ ref('int_telemetry_hourly') }}
),

hourly_satellite as (
    select
        location_id,
        measured_at,
        temp_2m_c,
        pressure_kpa
    from {{ ref('stg_satellite') }}
)

select
    tel.plant_id,
    tel.fruit_type,
    tel.measured_hour_at,
    tel.loaded_at,
    tel.one_hour_duty_cycle_compressor as compressor_duty_cycle,
    sat.temp_2m_c,
    tel.power_compressor_kw*1000 as power_compressor_w,
    tel.compressor_energy_kwh,
    tel.t_evap_inlet_c,
    tel.t_evap_outlet_c,
    tel.rh_evap_inlet_pct,
    tel.rh_evap_outlet_pct,
    tel.airflow_evap_kg_s,
    tel.fruit_mass_stored_kg,
    sat.pressure_kpa*1000 as pressure_pa
    
from hourly_telemetry tel
join hourly_satellite sat
on tel.plant_id = sat.location_id
and tel.measured_hour_at = sat.measured_at
