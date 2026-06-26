{{ config(
    materialized='view',
) }}

with telemetry_filtered as (
    select
        plant_id,
        fruit_type,
        measured_at,
        timestamp_trunc(measured_at, hour) as measured_at_hour,
        power_compressor_kw * 1000 as power_compressor_w,
        temp_evap_inlet_c,
        temp_evap_outlet_c,
        rh_evap_inlet_pct,
        rh_evap_outlet_pct,
        comp_modulation_pct/100 as comp_modulation_frac,
        evap_fan_speed_pct/100 as evap_fan_speed_frac,
        temp_coil_suction_c
    from {{ ref('stg_telemetry') }}
    -- where compressor_on = 1
    where comp_modulation_pct > 0.0
),

fruits_specs as (
    select
        fruit_type,
        q_rated_kw*1000 as q_rated_w,
        td_design,
        bf
    from {{ ref('fruit_specific_params') }}
),

satellite_hourly as (
    select
        location_id,
        measured_at,
        pressure_kpa*1000 as pressure_pa
    from {{ ref('stg_satellite') }}
),

telemetry_enriched as (
select
    tel.plant_id,
    tel.fruit_type,
    tel.measured_at,
    tel.power_compressor_w,
    tel.temp_evap_inlet_c,
    tel.temp_evap_outlet_c,
    tel.rh_evap_inlet_pct,
    tel.rh_evap_outlet_pct,
    tel.comp_modulation_frac,
    sat.pressure_pa,
    tel.evap_fan_speed_frac*{{ max_airflow_evap_kg_s('ft.q_rated_w', 'ft.td_design', 'ft.bf') }} as airflow_evap_kg_s,
    ft.q_rated_w,
    tel.temp_coil_suction_c
from telemetry_filtered as tel
join satellite_hourly as sat
    on tel.plant_id = sat.location_id
    and tel.measured_at_hour = sat.measured_at
join fruits_specs as ft
    on tel.fruit_type = ft.fruit_type
)

select
    plant_id,
    fruit_type,
    measured_at,
    power_compressor_w,
    temp_coil_suction_c,
    temp_evap_inlet_c,
    temp_evap_outlet_c,
    rh_evap_inlet_pct,
    rh_evap_outlet_pct,
    {{ q_total_w(
        'temp_evap_inlet_c',
        'temp_evap_outlet_c',
        'rh_evap_inlet_pct',
        'rh_evap_outlet_pct',
        'pressure_pa',
        'airflow_evap_kg_s',
        'comp_modulation_frac',
        'power_compressor_w',
        'q_rated_w'
    ) }} as q_total_w,
    {{ w_(
        'rh_evap_inlet_pct',
        p_sat_magnus('temp_evap_inlet_c'),
        'pressure_pa'
        ) }} as w_evap_inlet,
    {{ w_(
        'rh_evap_outlet_pct',
        p_sat_magnus('temp_evap_outlet_c'),
        'pressure_pa'
    ) }} as w_evap_outlet,
    airflow_evap_kg_s
from telemetry_enriched
